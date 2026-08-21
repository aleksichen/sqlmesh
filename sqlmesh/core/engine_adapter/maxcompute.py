from __future__ import annotations

import contextlib
import typing as t
from datetime import datetime, timezone

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from sqlmesh.core.engine_adapter.base import MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh.core.engine_adapter.mixins import RowDiffMixin
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.dialect import add_table
from sqlmesh.core.schema_diff import TableAlterOperation
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF
    from sqlmesh.core.model.kind import ModelKind


def _property_value_name(value: exp.Expr | str) -> str:
    if isinstance(value, exp.Boolean):
        return str(bool(value.this)).lower()
    if isinstance(value, exp.Literal):
        return str(value.this)
    return value.name if isinstance(value, exp.Expr) else str(value)


class _MaxComputeProperties(t.NamedTuple):
    lifecycle: t.Optional[str]
    transactional: t.Optional[bool]
    primary_key: t.List[str]
    write_bucket_num: t.Optional[int]
    cluster_bucket_num: t.Optional[int]
    remaining: t.Dict[str, exp.Expr]


@set_catalog()
class MaxComputeEngineAdapter(RowDiffMixin):
    DIALECT = "maxcompute"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_UNPARTITIONED_INSERT_OVERWRITE = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_METADATA_TABLE_LAST_MODIFIED_TS = True
    SUPPORTS_GRANTS = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.SINGLE_CATALOG_ONLY

    @property
    def odps(self) -> t.Any:
        return self.connection.odps

    def get_current_catalog(self) -> t.Optional[str]:
        return self.odps.project

    def _is_schema_namespace_enabled(self) -> bool:
        try:
            if vars(self.connection).get("_sqlmesh_schema_namespace_configured", False):
                return True
        except TypeError:
            pass

        try:
            return bool(self.odps.is_schema_namespace_enabled())
        except Exception:
            return True

    def _normalize_table(self, table_name: TableName) -> exp.Table:
        table = exp.to_table(table_name).copy()
        if self._is_schema_namespace_enabled():
            return table

        namespace = table.db
        if namespace and namespace != self._default_catalog:
            table.set("this", exp.to_identifier(f"{namespace}__{table.name}"))
        table.set("db", None)
        table.set("catalog", None)
        return table

    def _normalize_table_expression(self, node: t.Any) -> t.Any:
        return self._normalize_table(node) if isinstance(node, exp.Table) else node

    def _to_sql(self, expression: exp.Expr, quote: bool = True, **kwargs: t.Any) -> str:
        if not self._is_schema_namespace_enabled():
            expression = expression.transform(self._normalize_table_expression, copy=True)
        return super()._to_sql(expression, quote=quote, **kwargs)

    def _get_source_queries_and_columns_to_types(
        self,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        target_table: TableName,
        *,
        batch_size: t.Optional[int] = None,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> t.Tuple[t.List[SourceQuery], t.Optional[t.Dict[str, exp.DataType]]]:
        import pandas as pd

        if isinstance(query_or_df, pd.DataFrame):
            raise SQLMeshError("MaxCompute does not support DataFrame writes")
        return super()._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table,
            batch_size=batch_size,
            source_columns=source_columns,
        )

    def _df_to_source_queries(
        self,
        df: DF,
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> t.List[SourceQuery]:
        raise SQLMeshError("MaxCompute does not support DataFrame, Seed, or Python model writes")

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        if self._requires_two_step_ctas(
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        ):
            self._create_then_insert_source_queries(
                table_name,
                source_queries,
                target_columns_to_types or {},
                exists=exists,
                table_description=table_description,
                column_descriptions=column_descriptions,
                track_rows_processed=track_rows_processed,
                **kwargs,
            )
            return

        return super()._create_table_from_source_queries(
            table_name,
            source_queries,
            target_columns_to_types=target_columns_to_types,
            exists=exists,
            replace=replace,
            table_description=table_description,
            column_descriptions=column_descriptions,
            table_kind=table_kind,
            track_rows_processed=track_rows_processed,
            **kwargs,
        )

    def create_table(
        self,
        table_name: TableName,
        target_columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        if not columns_to_types_all_known(target_columns_to_types):
            if exists and self.table_exists(table_name):
                return
            raise SQLMeshError(
                "Cannot create a MaxCompute table without known column types. "
                f"Columns to types: {target_columns_to_types}"
            )

        properties = self._parse_maxcompute_properties(kwargs.get("table_properties"))
        property_primary_key = properties.primary_key
        if primary_key and property_primary_key and list(primary_key) != property_primary_key:
            raise SQLMeshError("Conflicting MaxCompute primary_key definitions")
        primary_key_columns = list(primary_key or property_primary_key)
        if primary_key_columns and properties.transactional is not True:
            raise SQLMeshError("MaxCompute primary_key requires transactional = true")

        partitioned_by = kwargs.get("partitioned_by") or []
        auto_partition = self._auto_partition_expression(
            partitioned_by,
            target_columns_to_types,
            kwargs.get("partition_interval_unit"),
        )
        data_columns, partition_columns = self._split_columns(
            target_columns_to_types, [] if auto_partition else partitioned_by
        )
        missing_primary_key_columns = [
            name for name in primary_key_columns if name not in data_columns
        ]
        if missing_primary_key_columns:
            raise SQLMeshError(
                f"MaxCompute primary key columns must be non-partition columns: {missing_primary_key_columns}"
            )

        clustered_by = self._column_names(kwargs.get("clustered_by"), "clustered_by")
        if clustered_by and properties.cluster_bucket_num is None:
            raise SQLMeshError("MaxCompute hash clustering requires cluster_bucket_num")
        if properties.cluster_bucket_num is not None and not clustered_by:
            raise SQLMeshError("MaxCompute cluster_bucket_num requires clustered_by")
        if clustered_by and properties.transactional:
            raise SQLMeshError("MaxCompute transactional tables cannot be clustered")
        if properties.write_bucket_num is not None and not properties.transactional:
            raise SQLMeshError("MaxCompute write_bucket_num requires transactional = true")

        table = exp.to_table(table_name)
        if_not_exists = " IF NOT EXISTS" if exists else ""
        data_schema = ", ".join(
            f"{self._quote_identifier(name)} {dtype.sql(dialect=self.dialect)}"
            f"{' NOT NULL' if name in primary_key_columns else ''}"
            f"{self._column_comment_sql(name, column_descriptions)}"
            for name, dtype in data_columns.items()
        )
        if primary_key_columns:
            data_schema += (
                ", PRIMARY KEY ("
                + ", ".join(self._quote_identifier(name) for name in primary_key_columns)
                + ")"
            )
        sql = f"CREATE TABLE{if_not_exists} {self._to_sql(table)} ({data_schema})"
        if table_description and self.comments_enabled:
            sql += f" COMMENT {self._string_literal(table_description)}"
        if partition_columns:
            partition_schema = ", ".join(
                f"{self._quote_identifier(name)} {dtype.sql(dialect=self.dialect)}"
                f"{self._column_comment_sql(name, column_descriptions)}"
                for name, dtype in partition_columns.items()
            )
            sql += f" PARTITIONED BY ({partition_schema})"
        elif auto_partition is not None:
            sql += f" AUTO PARTITIONED BY ({self._to_sql(auto_partition)})"
        if clustered_by:
            sql += (
                " CLUSTERED BY ("
                + ", ".join(self._quote_identifier(name) for name in clustered_by)
                + f") INTO {properties.cluster_bucket_num} BUCKETS"
            )
        properties_sql = self._render_maxcompute_properties(properties)
        if properties_sql:
            sql += f" {properties_sql}"
        self.execute(sql)
        self._clear_data_object_cache(table)

    def ctas(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=table_name,
            source_columns=source_columns,
        )
        target_columns_to_types = target_columns_to_types or {}

        if self._requires_two_step_ctas(
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        ):
            self._create_then_insert_source_queries(
                table_name,
                source_queries,
                target_columns_to_types,
                exists=exists,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )
            return

        table = exp.to_table(table_name)
        exists_sql = " IF NOT EXISTS" if exists else ""
        for source_query in source_queries:
            with source_query as query:
                self.execute(
                    f"CREATE TABLE{exists_sql} {self._to_sql(table)} AS {self._to_sql(query)}"
                )
                self._clear_data_object_cache(table)

    def _create_then_insert_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Dict[str, exp.DataType],
        exists: bool,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        self.create_table(
            table_name,
            target_columns_to_types=target_columns_to_types,
            exists=exists,
            **kwargs,
        )
        partitioned_by = kwargs.get("partitioned_by") or []
        if (
            partitioned_by
            and self._auto_partition_expression(
                partitioned_by,
                target_columns_to_types,
                kwargs.get("partition_interval_unit"),
            )
            is None
        ):
            self._insert_source_queries_by_partition(
                table_name,
                source_queries,
                partitioned_by=kwargs["partitioned_by"],
                target_columns_to_types=target_columns_to_types,
                overwrite=True,
                track_rows_processed=track_rows_processed,
            )
            return

        for source_query in source_queries:
            with source_query as query:
                self._insert_append_query(
                    table_name,
                    query,
                    target_columns_to_types,
                    track_rows_processed=track_rows_processed,
                )

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expr]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        materialized_properties = dict(materialized_properties or {})
        view_properties = dict(view_properties or {})
        if not materialized and (materialized_properties or view_properties):
            raise SQLMeshError(
                "MaxCompute storage properties are only supported for materialized views"
            )

        source_queries, _ = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=view_name,
            source_columns=source_columns,
        )
        view = exp.to_table(view_name)
        partitioned_by = materialized_properties.pop("partitioned_by", None)
        partition_names = self._column_names(partitioned_by, "partitioned_by")

        if materialized:
            prefix = "CREATE MATERIALIZED VIEW IF NOT EXISTS"
        else:
            prefix = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW"

        columns_sql = ""
        if column_descriptions and self.comments_enabled:
            columns = {
                name: data_type
                for name, data_type in (target_columns_to_types or {}).items()
                if name not in partition_names
            }
            columns_sql = (
                " ("
                + ", ".join(
                    f"{self._quote_identifier(name)}{self._column_comment_sql(name, column_descriptions)}"
                    for name in columns
                )
                + ")"
            )

        prefix_properties_sql: t.List[str] = []
        properties_sql: t.List[str] = []
        if table_description and self.comments_enabled:
            properties_sql.append(f"COMMENT {self._string_literal(table_description)}")

        clustered_by = self._column_names(
            materialized_properties.pop("clustered_by", None), "clustered_by"
        )
        parsed_properties = self._parse_maxcompute_properties(view_properties)
        table_only_properties = [
            name
            for name, configured in (
                ("transactional", parsed_properties.transactional is not None),
                ("primary_key", bool(parsed_properties.primary_key)),
                ("write_bucket_num", parsed_properties.write_bucket_num is not None),
            )
            if configured
        ]
        if table_only_properties:
            raise SQLMeshError(
                "MaxCompute table properties are not supported for materialized views: "
                + ", ".join(table_only_properties)
            )
        if parsed_properties.cluster_bucket_num is not None and not clustered_by:
            raise SQLMeshError("MaxCompute cluster_bucket_num requires clustered_by")
        if parsed_properties.lifecycle is not None:
            prefix_properties_sql.append(f"LIFECYCLE {parsed_properties.lifecycle}")
        disable_rewrite = parsed_properties.remaining.pop("disable_rewrite", None)
        if disable_rewrite is not None and self._property_bool(disable_rewrite, "disable_rewrite"):
            properties_sql.append("DISABLE REWRITE")
        if partitioned_by:
            properties_sql.append(
                "PARTITIONED ON ("
                + ", ".join(self._quote_identifier(name) for name in partition_names)
                + ")"
            )
        if clustered_by:
            if parsed_properties.cluster_bucket_num is None:
                raise SQLMeshError("MaxCompute hash clustering requires cluster_bucket_num")
            properties_sql.append(
                "CLUSTERED BY ("
                + ", ".join(self._quote_identifier(name) for name in clustered_by)
                + f") INTO {parsed_properties.cluster_bucket_num} BUCKETS"
            )
        rendered_properties = self._render_maxcompute_properties(
            parsed_properties._replace(lifecycle=None)
        )
        if rendered_properties:
            properties_sql.append(rendered_properties)

        if replace:
            self.drop_data_object_on_type_mismatch(
                self.get_data_object(view_name),
                DataObjectType.MATERIALIZED_VIEW if materialized else DataObjectType.VIEW,
            )
        if materialized and replace:
            self.drop_view(view_name, materialized=True)

        for source_query in source_queries:
            with source_query as query:
                self.execute(
                    f"{prefix} {self._to_sql(view)}"
                    f"{' ' if prefix_properties_sql else ''}{' '.join(prefix_properties_sql)}"
                    f"{columns_sql}"
                    f"{' ' if properties_sql else ''}{' '.join(properties_sql)} AS "
                    f"{self._to_sql(query)}"
                )
        self._clear_data_object_cache(view)

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expr]] = None,
    ) -> None:
        if not self._is_schema_namespace_enabled():
            return

        exists_sql = " IF NOT EXISTS" if ignore_if_exists else ""
        self.execute(
            f"CREATE SCHEMA{exists_sql} "
            f"{exp.to_table(schema_name).sql(dialect=self.dialect, identify=True)}"
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expr],
    ) -> None:
        if not self._is_schema_namespace_enabled():
            return

        exists_sql = " IF EXISTS" if ignore_if_not_exists else ""
        cascade_sql = " CASCADE" if cascade else ""
        self.execute(
            f"DROP SCHEMA{exists_sql} "
            f"{exp.to_table(schema_name).sql(dialect=self.dialect, identify=True)}"
            f"{cascade_sql}"
        )

    def drop_table(self, table_name: TableName, exists: bool = True, **kwargs: t.Any) -> None:
        exists_sql = " IF EXISTS" if exists else ""
        table = exp.to_table(table_name)
        self.execute(f"DROP TABLE{exists_sql} {self._to_sql(table)}")
        self._clear_data_object_cache(table)

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        exists_sql = " IF EXISTS" if ignore_if_not_exists else ""
        view = exp.to_table(view_name)
        kind = "MATERIALIZED VIEW" if materialized else "VIEW"
        self.execute(f"DROP {kind}{exists_sql} {self._to_sql(view)}")
        self._clear_data_object_cache(view)

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        where: t.Optional[exp.Condition] = None,
    ) -> None:
        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=table_name,
            source_columns=source_columns,
        )
        self._insert_overwrite_by_condition(
            table_name,
            source_queries,
            target_columns_to_types=target_columns_to_types,
            where=where,
            partitioned_by=partitioned_by,
        )

    def _insert_source_queries_by_partition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: t.Optional[exp.Condition] = None,
        overwrite: bool = True,
        track_rows_processed: bool = True,
    ) -> None:
        projection_order = self._projection_order_for_partition_overwrite(
            target_columns_to_types, partitioned_by
        )
        partition_sql = ", ".join(
            self._quote_identifier(name) for name in self._partition_column_names(partitioned_by)
        )
        table = exp.to_table(table_name)
        for i, source_query in enumerate(source_queries):
            command = "INSERT OVERWRITE TABLE" if overwrite and i == 0 else "INSERT INTO TABLE"
            with source_query as query:
                ordered_query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                    where=where,
                )
                self.execute(
                    f"{command} "
                    f"{self._to_sql(table)} "
                    f"PARTITION ({partition_sql}) "
                    f"{self._to_sql(ordered_query)}",
                    track_rows_processed=track_rows_processed,
                )

    def _insert_append_query(
        self,
        table_name: TableName,
        query: Query,
        target_columns_to_types: t.Dict[str, exp.DataType],
        order_projections: bool = True,
        track_rows_processed: bool = True,
    ) -> None:
        partitioned_by = self._manual_partition_columns(table_name)
        if partitioned_by:
            self._insert_source_queries_by_partition(
                table_name,
                [SourceQuery(query_factory=lambda: query)],
                partitioned_by=[exp.column(name) for name in partitioned_by],
                target_columns_to_types=target_columns_to_types,
                overwrite=False,
                track_rows_processed=track_rows_processed,
            )
            return
        if order_projections:
            query = self._order_projections_and_filter(query, target_columns_to_types)
        table = exp.to_table(table_name)
        columns = ", ".join(self._quote_identifier(name) for name in target_columns_to_types)
        self.execute(
            f"INSERT INTO {self._to_sql(table)} ({columns}) {self._to_sql(query)}",
            track_rows_processed=track_rows_processed,
        )

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        partitioned_by = kwargs.get("partitioned_by")
        if partitioned_by:
            target_columns_to_types = target_columns_to_types or self.columns(table_name)
            has_partition_expression = any(
                not isinstance(partition, exp.Column) for partition in partitioned_by
            )
            if has_partition_expression:
                auto_partition = self._auto_partition_expression(
                    partitioned_by, target_columns_to_types
                )
                if auto_partition is None:
                    raise SQLMeshError(
                        "MaxCompute partition expressions must use TRUNC_TIME for automatic partitioning"
                    )
                if not self._target_automatic_partition_matches(table_name, auto_partition):
                    raise SQLMeshError(
                        "MaxCompute automatic partition expression does not match the target table"
                    )
                if where is None:
                    raise SQLMeshError(
                        "MaxCompute automatic partition overwrite requires a bounded condition"
                    )
                self._replace_automatic_partition_interval(
                    table_name,
                    source_queries,
                    target_columns_to_types,
                    where,
                )
                return
            self._insert_source_queries_by_partition(
                table_name,
                source_queries,
                partitioned_by=partitioned_by,
                target_columns_to_types=target_columns_to_types,
                where=where,
                overwrite=True,
            )
            return

        insert_overwrite_strategy = (
            insert_overwrite_strategy_override or self.INSERT_OVERWRITE_STRATEGY
        )
        if not insert_overwrite_strategy.is_insert_overwrite:
            return super()._insert_overwrite_by_condition(
                table_name,
                source_queries,
                target_columns_to_types=target_columns_to_types,
                where=where,
                insert_overwrite_strategy_override=insert_overwrite_strategy_override,
                **kwargs,
            )

        target_columns_to_types = target_columns_to_types or self.columns(table_name)
        projection_order = list(target_columns_to_types)
        table = exp.to_table(table_name)
        for i, source_query in enumerate(source_queries):
            with source_query as query:
                query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                    where=where,
                )
                if i > 0:
                    self._insert_append_query(
                        table_name,
                        query,
                        target_columns_to_types=target_columns_to_types,
                        order_projections=False,
                    )
                else:
                    self.execute(
                        f"INSERT OVERWRITE TABLE {self._to_sql(table)} {self._to_sql(query)}",
                        track_rows_processed=True,
                    )

    def _replace_automatic_partition_interval(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: exp.Condition,
    ) -> None:
        table = exp.to_table(table_name)
        projection_order = list(target_columns_to_types)
        preserved_query: Query = (
            exp.select(*(exp.column(name) for name in projection_order))
            .from_(table)
            .where(exp.not_(where.copy()))
        )

        with contextlib.ExitStack() as stack:
            replacement_query = preserved_query
            for source_query in source_queries:
                query = stack.enter_context(source_query)
                ordered_query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                    where=where,
                )
                replacement_query = exp.union(
                    replacement_query,
                    ordered_query,
                    distinct=False,
                )

            with self.temp_table(
                replacement_query,
                name=table,
                target_columns_to_types=target_columns_to_types,
                table_properties={"lifecycle": exp.Literal.number(1)},
            ) as temp_table:
                temp_query = exp.select(*(exp.column(name) for name in projection_order)).from_(
                    temp_table
                )
                self.execute(
                    f"INSERT OVERWRITE TABLE {self._to_sql(table)} {self._to_sql(temp_query)}",
                    track_rows_processed=True,
                )

    def _insert_overwrite_by_time_partition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: exp.Condition,
        **kwargs: t.Any,
    ) -> None:
        return self._insert_overwrite_by_condition(
            table_name,
            source_queries,
            target_columns_to_types=target_columns_to_types,
            where=where,
            **kwargs,
        )

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        table, project, schema = self._table_parts(table_name)
        odps_table = self.odps.get_table(table, project=project, schema=schema)
        return {
            column.name: self._odps_type_to_sqlglot(column.type)
            for column in odps_table.table_schema.columns
        }

    def table_exists(self, table_name: TableName) -> bool:
        table, project, schema = self._table_parts(table_name)
        return bool(self.odps.exist_table(table, project=project, schema=schema))

    def _fetch_native_df(
        self, query: t.Union[exp.Expr, str], quote_identifiers: bool = False
    ) -> t.Any:
        import pandas as pd

        with self.transaction():
            self.execute(query, quote_identifiers=quote_identifiers)
            description = self.cursor.description or []
            columns = [column[0] for column in description]
            return pd.DataFrame(self.cursor.fetchall(), columns=columns)

    def get_table_last_modified_ts(self, table_names: t.List[TableName]) -> t.List[int]:
        timestamps: t.List[int] = []
        for table_name in table_names:
            table, project, schema = self._table_parts(table_name)
            modified_at = self.odps.get_table(
                table, project=project, schema=schema
            ).last_data_modified_time
            if not isinstance(modified_at, datetime):
                raise SQLMeshError(f"MaxCompute table '{table_name}' has no last modified time")
            if modified_at.tzinfo is None:
                modified_at = modified_at.replace(tzinfo=timezone.utc)
            timestamps.append(int(modified_at.timestamp() * 1000))
        return timestamps

    def _truncate_table(self, table_name: TableName) -> None:
        if self._table_partition_columns(table_name):
            raise SQLMeshError(
                "MaxCompute cannot truncate a partitioned table without a partition spec"
            )
        self.execute(f"TRUNCATE TABLE {self._to_sql(exp.to_table(table_name))}")

    def _rename_table(self, old_table_name: TableName, new_table_name: TableName) -> None:
        old_table = exp.to_table(old_table_name)
        new_table = exp.to_table(new_table_name)
        old_project = old_table.catalog or self._default_catalog
        new_project = new_table.catalog or self._default_catalog
        if old_project != new_project or old_table.db != new_table.db:
            raise SQLMeshError(
                "MaxCompute can only rename tables within the same project and schema"
            )
        self.execute(
            f"ALTER TABLE {self._to_sql(old_table)} RENAME TO {self._quote_identifier(self._normalize_table(new_table).name)}"
        )

    def alter_table(
        self,
        alter_expressions: t.Union[t.List[exp.Alter], t.List[TableAlterOperation]],
    ) -> None:
        expressions = [
            operation.expression if isinstance(operation, TableAlterOperation) else operation
            for operation in alter_expressions
        ]
        rendered_statements: t.List[str] = []
        current_columns_by_table: t.Dict[str, t.Dict[str, exp.DataType]] = {}

        for alter_expression in expressions:
            table = exp.to_table(alter_expression.this)
            table_sql = self._to_sql(table)
            for action in alter_expression.args.get("actions") or []:
                if isinstance(action, exp.ColumnDef):
                    data_type = action.args.get("kind")
                    if not isinstance(data_type, exp.DataType):
                        raise SQLMeshError("MaxCompute ADD COLUMNS requires a concrete data type")
                    rendered_statements.append(
                        f"ALTER TABLE {table_sql} ADD COLUMNS "
                        f"({self._quote_identifier(action.name)} {data_type.sql(dialect=self.dialect)})"
                    )
                elif isinstance(action, exp.Drop) and action.args.get("kind") == "COLUMN":
                    rendered_statements.append(
                        f"ALTER TABLE {table_sql} DROP COLUMN {self._quote_identifier(action.name)}"
                    )
                elif isinstance(action, exp.AlterColumn):
                    data_type = action.args.get("dtype")
                    if not isinstance(data_type, exp.DataType):
                        raise SQLMeshError("MaxCompute CHANGE COLUMN requires a concrete data type")
                    table_key = table.sql(dialect=self.dialect)
                    current_columns = current_columns_by_table.setdefault(
                        table_key, self.columns(table)
                    )
                    current_type = current_columns.get(action.name)
                    if current_type is None:
                        raise SQLMeshError(
                            f"MaxCompute column '{action.name}' does not exist in '{table}'"
                        )
                    if not self._is_safe_type_change(current_type, data_type):
                        raise SQLMeshError(
                            "Refusing unsafe MaxCompute type change for "
                            f"'{action.name}': {current_type.sql(dialect=self.dialect)} -> "
                            f"{data_type.sql(dialect=self.dialect)}"
                        )
                    column = self._quote_identifier(action.name)
                    rendered_statements.append(
                        f"ALTER TABLE {table_sql} CHANGE COLUMN {column} {column} "
                        f"{data_type.sql(dialect=self.dialect)}"
                    )
                else:
                    raise SQLMeshError(
                        f"Unsupported MaxCompute schema evolution action: {action.sql()}"
                    )

        for statement in rendered_statements:
            self.execute(statement)

    def _is_safe_type_change(self, current_type: exp.DataType, new_type: exp.DataType) -> bool:
        if current_type == new_type:
            return True
        if current_type.this == new_type.this:
            current_parameters = self._data_type_parameters(current_type)
            new_parameters = self._data_type_parameters(new_type)
            if (
                not current_parameters
                or not new_parameters
                or len(current_parameters) != len(new_parameters)
            ):
                return False
            if current_type.is_type("decimal"):
                if len(current_parameters) != 2:
                    return False
                current_precision, current_scale = current_parameters
                new_precision, new_scale = new_parameters
                return (
                    new_scale >= current_scale
                    and new_precision - new_scale >= current_precision - current_scale
                )
            return all(new >= current for current, new in zip(current_parameters, new_parameters))

        widening_types = ["tinyint", "smallint", "int", "bigint", "float", "double"]
        current_rank = next(
            (i for i, name in enumerate(widening_types) if current_type.is_type(name)), None
        )
        new_rank = next(
            (i for i, name in enumerate(widening_types) if new_type.is_type(name)), None
        )
        return current_rank is not None and new_rank is not None and new_rank > current_rank

    def _data_type_parameters(self, data_type: exp.DataType) -> t.List[int]:
        parameters: t.List[int] = []
        for parameter in data_type.expressions:
            value = parameter.this if isinstance(parameter, exp.DataTypeParam) else parameter
            try:
                parameters.append(int(value.name))
            except (TypeError, ValueError):
                return []
        return parameters

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expr],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expr] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        odps_table = self._odps_table(target_table)
        if not bool(getattr(odps_table, "is_transactional", False)):
            raise SQLMeshError("MaxCompute MERGE requires a transactional target table")

        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            source_table,
            target_columns_to_types,
            target_table=target_table,
            source_columns=source_columns,
        )
        target_columns_to_types = target_columns_to_types or self.columns(target_table)
        on = exp.and_(
            *(
                add_table(part, MERGE_TARGET_ALIAS).eq(add_table(part, MERGE_SOURCE_ALIAS))
                for part in unique_key
            )
        )
        if merge_filter is not None:
            on = exp.and_(merge_filter, on)

        immutable_columns = set(self._manual_partition_columns(target_table, odps_table))
        immutable_columns.update(getattr(odps_table, "primary_key", None) or [])
        if when_matched:
            match_expressions = when_matched.copy().expressions
        else:
            update_expressions = [
                exp.column(column, MERGE_TARGET_ALIAS).eq(exp.column(column, MERGE_SOURCE_ALIAS))
                for column in target_columns_to_types
                if column not in immutable_columns
            ]
            match_expressions = (
                [
                    exp.When(
                        matched=True,
                        source=False,
                        then=exp.Update(expressions=update_expressions),
                    )
                ]
                if update_expressions
                else []
            )
        match_expressions.append(
            exp.When(
                matched=False,
                source=False,
                then=exp.Insert(
                    this=exp.Tuple(
                        expressions=[exp.column(column) for column in target_columns_to_types]
                    ),
                    expression=exp.Tuple(
                        expressions=[
                            exp.column(column, MERGE_SOURCE_ALIAS)
                            for column in target_columns_to_types
                        ]
                    ),
                ),
            )
        )
        for source_query in source_queries:
            with source_query as query:
                self._merge(
                    target_table,
                    query,
                    on,
                    exp.Whens(expressions=match_expressions),
                )

    def _get_data_objects(
        self,
        schema_name: SchemaName,
        object_names: t.Optional[t.Set[str]] = None,
    ) -> t.List[DataObject]:
        schema = to_schema(schema_name)
        project = schema.catalog or self._default_catalog
        namespace = schema.db or ""
        list_tables_kwargs = {"project": project}
        if namespace and self._is_schema_namespace_enabled():
            list_tables_kwargs["schema"] = namespace

        object_name_by_actual_name: t.Dict[str, str] = {}
        actual_object_names = object_names
        folded_prefix = ""
        if object_names and not self._is_schema_namespace_enabled():
            object_name_by_actual_name = {name: name for name in object_names}
            if namespace and namespace != project:
                folded_prefix = f"{namespace}__"
                object_name_by_actual_name = {
                    f"{folded_prefix}{name}": name for name in object_names
                }
            actual_object_names = set(object_name_by_actual_name)
        elif not self._is_schema_namespace_enabled() and namespace and namespace != project:
            folded_prefix = f"{namespace}__"

        objects: t.List[DataObject] = []
        for table in self.odps.list_tables(**list_tables_kwargs):
            if actual_object_names and table.name not in actual_object_names:
                continue
            if folded_prefix and not table.name.startswith(folded_prefix):
                continue
            logical_name = object_name_by_actual_name.get(table.name)
            if logical_name is None:
                logical_name = table.name[len(folded_prefix) :] if folded_prefix else table.name
            objects.append(
                DataObject(
                    catalog=project,
                    schema=namespace,
                    name=logical_name,
                    type=(
                        DataObjectType.MATERIALIZED_VIEW
                        if getattr(table, "is_materialized_view", False)
                        else (
                            DataObjectType.VIEW
                            if getattr(table, "is_virtual_view", False)
                            else DataObjectType.TABLE
                        )
                    ),
                )
            )
        return objects

    def adjust_physical_properties_for_incremental(
        self,
        physical_properties: t.Dict[str, t.Any],
        *,
        model_kind: ModelKind,
        partitioned_by: t.List[exp.Expr],
        requires_delete_capable_table: bool,
        unique_key: t.Optional[t.List[exp.Expr]],
        model_name: str,
    ) -> t.Dict[str, t.Any]:
        if not (model_kind.is_incremental_by_unique_key or model_kind.is_scd_type_2):
            return physical_properties

        if model_kind.is_scd_type_2 and partitioned_by:
            raise SQLMeshError("MaxCompute SCD Type 2 models do not support partitioned_by")

        properties = self._parse_maxcompute_properties(physical_properties)
        if properties.transactional is not True:
            raise SQLMeshError(
                f"MaxCompute model '{model_name}' requires physical_properties "
                "(transactional = true)"
            )
        if unique_key and properties.primary_key:
            unique_key_columns = self._column_names(unique_key, "unique_key")
            if unique_key_columns != properties.primary_key:
                raise SQLMeshError(
                    f"MaxCompute model '{model_name}' primary_key must match its unique_key"
                )
        return physical_properties

    def _partition_column_names(self, partitioned_by: t.Optional[t.List[exp.Expr]]) -> t.List[str]:
        names: t.List[str] = []
        for partition in partitioned_by or []:
            if not isinstance(partition, exp.Column) or len(partition.parts) != 1:
                raise SQLMeshError(
                    "MaxCompute partitioned_by only supports simple column references"
                )
            names.append(partition.name)
        return names

    def _split_columns(
        self,
        target_columns_to_types: t.Dict[str, exp.DataType],
        partitioned_by: t.Optional[t.List[exp.Expr]],
    ) -> t.Tuple[t.Dict[str, exp.DataType], t.Dict[str, exp.DataType]]:
        partition_names = self._partition_column_names(partitioned_by)
        partition_name_set = set(partition_names)
        missing = [name for name in partition_names if name not in target_columns_to_types]
        if missing:
            raise SQLMeshError(
                f"MaxCompute partition columns must exist in target columns: {missing}"
            )
        data_columns = {
            name: dtype
            for name, dtype in target_columns_to_types.items()
            if name not in partition_name_set
        }
        partition_columns = {name: target_columns_to_types[name] for name in partition_names}
        return data_columns, partition_columns

    def _parse_maxcompute_properties(
        self, table_properties: t.Optional[t.Dict[str, t.Any]]
    ) -> _MaxComputeProperties:
        properties = dict(table_properties or {})
        lifecycle_value = properties.pop("lifecycle", None)
        transactional_value = properties.pop("transactional", None)
        primary_key_value = properties.pop("primary_key", None)
        write_bucket_value = properties.pop("write_bucket_num", None)
        cluster_bucket_value = properties.pop("cluster_bucket_num", None)

        lifecycle = (
            self._property_positive_int(lifecycle_value, "lifecycle")
            if lifecycle_value is not None
            else None
        )
        transactional = (
            self._property_bool(transactional_value, "transactional")
            if transactional_value is not None
            else None
        )
        primary_key = self._column_names(primary_key_value, "primary_key")
        write_bucket_num = (
            int(self._property_positive_int(write_bucket_value, "write_bucket_num"))
            if write_bucket_value is not None
            else None
        )
        cluster_bucket_num = (
            int(self._property_positive_int(cluster_bucket_value, "cluster_bucket_num"))
            if cluster_bucket_value is not None
            else None
        )
        return _MaxComputeProperties(
            lifecycle,
            transactional,
            primary_key,
            write_bucket_num,
            cluster_bucket_num,
            properties,
        )

    def _render_maxcompute_properties(self, properties: _MaxComputeProperties) -> str:
        parts: t.List[str] = []
        table_properties = dict(properties.remaining)
        if properties.transactional is not None:
            table_properties["transactional"] = exp.Boolean(this=properties.transactional)
        if properties.write_bucket_num is not None:
            table_properties["write.bucket.num"] = exp.Literal.number(properties.write_bucket_num)
        if table_properties:
            rendered = ", ".join(
                f"'{key}'='{_property_value_name(value)}'"
                for key, value in table_properties.items()
            )
            parts.append(f"TBLPROPERTIES ({rendered})")
        if properties.lifecycle is not None:
            parts.append(f"LIFECYCLE {properties.lifecycle}")
        return " ".join(parts)

    def _property_bool(self, value: t.Any, name: str) -> bool:
        normalized = _property_value_name(value).lower()
        if normalized not in {"true", "false"}:
            raise SQLMeshError(f"MaxCompute {name} must be true or false")
        return normalized == "true"

    def _property_positive_int(self, value: t.Any, name: str) -> str:
        normalized = _property_value_name(value)
        try:
            parsed = int(normalized)
        except (TypeError, ValueError) as ex:
            raise SQLMeshError(f"MaxCompute {name} must be a positive integer") from ex
        if parsed <= 0:
            raise SQLMeshError(f"MaxCompute {name} must be a positive integer")
        return str(parsed)

    def _column_names(self, expressions: t.Any, property_name: str) -> t.List[str]:
        if expressions is None:
            return []
        if isinstance(expressions, exp.Tuple):
            expressions = expressions.expressions
        elif isinstance(expressions, exp.Column):
            expressions = [expressions]
        if not isinstance(expressions, (list, tuple)):
            raise SQLMeshError(f"MaxCompute {property_name} must contain simple columns")
        names: t.List[str] = []
        for expression in expressions:
            if not isinstance(expression, exp.Column) or len(expression.parts) != 1:
                raise SQLMeshError(f"MaxCompute {property_name} must contain simple columns")
            names.append(expression.name)
        return names

    def _auto_partition_expression(
        self,
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Dict[str, exp.DataType],
        partition_interval_unit: t.Any = None,
    ) -> t.Optional[exp.Expr]:
        if len(partitioned_by) != 1:
            return None
        partition = partitioned_by[0]
        expression = partition.this if isinstance(partition, exp.Alias) else partition
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TRUNC_TIME":
            arguments = expression.expressions
            if (
                len(arguments) != 2
                or not isinstance(arguments[0], exp.Column)
                or len(arguments[0].parts) != 1
                or not isinstance(arguments[1], exp.Literal)
                or not arguments[1].is_string
                or arguments[1].this.lower() not in {"day", "hour", "month", "year"}
            ):
                raise SQLMeshError(
                    "MaxCompute automatic partitioning requires "
                    "TRUNC_TIME(column, 'day|hour|month|year')"
                )
            source_column = t.cast(exp.Column, arguments[0]).name
            if source_column not in target_columns_to_types:
                raise SQLMeshError(
                    f"MaxCompute automatic partition source column '{source_column}' "
                    "must exist in the model projection"
                )
            source_type = target_columns_to_types[source_column]
            if not source_type.is_type("date", "datetime", "timestamp", "timestampntz"):
                raise SQLMeshError(
                    f"MaxCompute automatic partition source column '{source_column}' "
                    "must be DATE, DATETIME, TIMESTAMP, or TIMESTAMP_NTZ"
                )
            return partition

        if isinstance(partition, exp.Column) and partition_interval_unit is not None:
            data_type = target_columns_to_types.get(partition.name)
            if data_type and data_type.is_type("date", "datetime", "timestamp", "timestampntz"):
                raise SQLMeshError(
                    "MaxCompute automatic partitioning requires an explicit TRUNC_TIME "
                    "expression in partitioned_by"
                )
        return None

    def _automatic_partition_signature(self, partition: exp.Expr) -> t.Optional[t.Tuple[str, str]]:
        expression = partition.this if isinstance(partition, exp.Alias) else partition
        if not isinstance(expression, exp.Anonymous) or expression.name.upper() != "TRUNC_TIME":
            return None
        arguments = expression.expressions
        if (
            len(arguments) != 2
            or not isinstance(arguments[0], exp.Column)
            or len(arguments[0].parts) != 1
            or not isinstance(arguments[1], exp.Literal)
            or not arguments[1].is_string
        ):
            return None
        return arguments[0].name.casefold(), str(arguments[1].this).casefold()

    def _target_automatic_partition_matches(
        self, table_name: TableName, declared_partition: exp.Expr
    ) -> bool:
        target_partitions = self._table_partition_columns(table_name)
        if len(target_partitions) != 1:
            return False

        target_partition = target_partitions[0]
        generated_expression = getattr(target_partition, "generate_expression", None)
        if not generated_expression:
            return False
        if isinstance(generated_expression, exp.Expr):
            parsed_expression = generated_expression
        else:
            try:
                parsed_expression = parse_one(str(generated_expression), dialect=self.dialect)
            except ParseError:
                return False

        declared_alias = (
            declared_partition.alias if isinstance(declared_partition, exp.Alias) else ""
        )
        if declared_alias and declared_alias.casefold() != str(target_partition.name).casefold():
            return False
        return self._automatic_partition_signature(
            declared_partition
        ) == self._automatic_partition_signature(parsed_expression)

    def _column_comment_sql(
        self, name: str, column_descriptions: t.Optional[t.Dict[str, str]]
    ) -> str:
        if not self.comments_enabled or not column_descriptions or name not in column_descriptions:
            return ""
        return f" COMMENT {self._string_literal(column_descriptions[name])}"

    def _string_literal(self, value: str) -> str:
        return exp.Literal.string(value).sql(dialect=self.dialect)

    def _odps_table(self, table_name: TableName) -> t.Any:
        table, project, schema = self._table_parts(table_name)
        return self.odps.get_table(table, project=project, schema=schema)

    def _table_partition_columns(
        self, table_name: TableName, odps_table: t.Optional[t.Any] = None
    ) -> t.List[t.Any]:
        odps_table = odps_table or self._odps_table(table_name)
        partitions = getattr(getattr(odps_table, "table_schema", None), "partitions", None)
        if not partitions:
            return []
        try:
            return list(partitions)
        except TypeError:
            return []

    def _manual_partition_columns(
        self, table_name: TableName, odps_table: t.Optional[t.Any] = None
    ) -> t.List[str]:
        return [
            partition.name
            for partition in self._table_partition_columns(table_name, odps_table)
            if getattr(partition, "generate_expression", None) is None
        ]

    def _requires_two_step_ctas(self, **kwargs: t.Any) -> bool:
        table_properties = kwargs.get("table_properties") or {}
        return bool(
            kwargs.get("partitioned_by")
            or table_properties
            or kwargs.get("table_description")
            or kwargs.get("column_descriptions")
        )

    def _projection_order_for_partition_overwrite(
        self,
        target_columns_to_types: t.Dict[str, exp.DataType],
        partitioned_by: t.List[exp.Expr],
    ) -> t.List[str]:
        partition_names = self._partition_column_names(partitioned_by)
        partition_name_set = set(partition_names)
        normal_names = [name for name in target_columns_to_types if name not in partition_name_set]
        return normal_names + partition_names

    def _select_columns_in_order(
        self,
        query: Query,
        names: t.List[str],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: t.Optional[exp.Condition] = None,
    ) -> Query:
        if isinstance(query, exp.Select) and where is None:
            projections_by_name = {
                projection.alias_or_name: projection
                for projection in query.expressions
                if projection.alias_or_name
            }
            if all(name in projections_by_name for name in names):
                return query.select(
                    *(projections_by_name[name].copy() for name in names),
                    append=False,
                    copy=True,
                )

        ordered_columns_to_types = {name: target_columns_to_types[name] for name in names}
        return self._order_projections_and_filter(query, ordered_columns_to_types, where=where)

    def _table_parts(self, table_name: TableName) -> t.Tuple[str, t.Optional[str], t.Optional[str]]:
        table = exp.to_table(table_name)
        if not self._is_schema_namespace_enabled():
            physical_project = table.catalog or self._default_catalog or table.db
            return self._normalize_table(table).name, physical_project, None

        project = table.catalog or self._default_catalog
        return table.name, project or table.db, table.db if project else None

    def _odps_type_to_sqlglot(self, type_: t.Any) -> exp.DataType:
        type_name = str(getattr(type_, "name", type_))
        return exp.DataType.build(type_name, dialect=self.dialect)

    def _quote_identifier(self, name: str) -> str:
        return exp.to_identifier(name).sql(dialect=self.dialect, identify=True)
