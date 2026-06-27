from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
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
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import Query, QueryOrDF


def _property_value_name(value: exp.Expr | str) -> str:
    return value.name if isinstance(value, exp.Expr) else str(value)


@set_catalog()
class MaxComputeEngineAdapter(EngineAdapter):
    DIALECT = "maxcompute"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_GRANTS = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.SINGLE_CATALOG_ONLY

    @property
    def odps(self) -> t.Any:
        return self.connection.odps

    def _is_schema_namespace_enabled(self) -> bool:
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

    def _normalize_query(self, query: Query) -> Query:
        if self._is_schema_namespace_enabled():
            return query

        return t.cast(
            "Query",
            query.transform(
                self._normalize_table_expression,
                copy=True,
            ),
        )

    def _normalize_table_expression(self, node: t.Any) -> t.Any:
        return self._normalize_table(node) if isinstance(node, exp.Table) else node

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        supports_replace_table_override: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> None:
        if kwargs.get("partitioned_by"):
            table = exp.to_table(table_name)
            target_data_object = self.get_data_object(table)
            table_exists = target_data_object is not None
            if self.drop_data_object_on_type_mismatch(target_data_object, DataObjectType.TABLE):
                table_exists = False

            if table_exists:
                self.drop_table(table, exists=True)
                return self.ctas(
                    table,
                    query_or_df,
                    target_columns_to_types=target_columns_to_types,
                    exists=False,
                    source_columns=source_columns,
                    table_description=table_description,
                    column_descriptions=column_descriptions,
                    **kwargs,
                )

        return super().replace_query(
            table_name,
            query_or_df,
            target_columns_to_types=target_columns_to_types,
            table_description=table_description,
            column_descriptions=column_descriptions,
            source_columns=source_columns,
            supports_replace_table_override=supports_replace_table_override,
            **kwargs,
        )

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
        if not self._is_schema_namespace_enabled():
            table_name = self._normalize_table(table_name)
            for source_query in source_queries:
                source_query.add_transform(self._normalize_table_expression)

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
        if primary_key:
            raise SQLMeshError("MaxCompute adapter does not support primary keys")
        if not columns_to_types_all_known(target_columns_to_types):
            if exists and self.table_exists(table_name):
                return
            raise SQLMeshError(
                "Cannot create a MaxCompute table without known column types. "
                f"Columns to types: {target_columns_to_types}"
            )

        data_columns, partition_columns = self._split_columns(
            target_columns_to_types, kwargs.get("partitioned_by")
        )
        table = self._normalize_table(table_name)
        if_not_exists = " IF NOT EXISTS" if exists else ""
        data_schema = ", ".join(
            f"{self._quote_identifier(name)} {dtype.sql(dialect=self.dialect)}"
            for name, dtype in data_columns.items()
        )
        sql = (
            f"CREATE TABLE{if_not_exists} "
            f"{table.sql(dialect=self.dialect, identify=True)} ({data_schema})"
        )
        if partition_columns:
            partition_schema = ", ".join(
                f"{self._quote_identifier(name)} {dtype.sql(dialect=self.dialect)}"
                for name, dtype in partition_columns.items()
            )
            sql += f" PARTITIONED BY ({partition_schema})"
        properties_sql = self._build_maxcompute_properties_sql(kwargs.get("table_properties"))
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

        if self._requires_two_step_ctas(**kwargs):
            self.create_table(
                table_name,
                target_columns_to_types=target_columns_to_types,
                exists=exists,
                **kwargs,
            )
            if kwargs.get("partitioned_by"):
                self._insert_source_queries_by_partition(
                    table_name,
                    source_queries,
                    partitioned_by=kwargs["partitioned_by"],
                    target_columns_to_types=target_columns_to_types,
                    overwrite=True,
                )
            else:
                for source_query in source_queries:
                    with source_query as query:
                        self._insert_append_query(table_name, query, target_columns_to_types)
            return

        table = self._normalize_table(table_name)
        exists_sql = " IF NOT EXISTS" if exists else ""
        for source_query in source_queries:
            with source_query as query:
                query = self._normalize_query(query)
                self.execute(
                    f"CREATE TABLE{exists_sql} "
                    f"{table.sql(dialect=self.dialect, identify=True)} AS "
                    f"{query.sql(dialect=self.dialect, identify=True)}"
                )
                self._clear_data_object_cache(table)

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        **kwargs: t.Any,
    ) -> None:
        source_queries, _ = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=view_name,
        )
        view = self._normalize_table(view_name)
        prefix = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW"
        for source_query in source_queries:
            with source_query as query:
                query = self._normalize_query(query)
                self.execute(
                    f"{prefix} {view.sql(dialect=self.dialect, identify=True)} AS "
                    f"{query.sql(dialect=self.dialect, identify=True)}"
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
        table = self._normalize_table(table_name)
        self.execute(f"DROP TABLE{exists_sql} {table.sql(dialect=self.dialect, identify=True)}")
        self._clear_data_object_cache(table)

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        exists_sql = " IF EXISTS" if ignore_if_not_exists else ""
        view = self._normalize_table(view_name)
        self.execute(f"DROP VIEW{exists_sql} {view.sql(dialect=self.dialect, identify=True)}")
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
        self._insert_by_partition(
            table_name,
            query_or_df,
            partitioned_by,
            target_columns_to_types=target_columns_to_types,
            source_columns=source_columns,
            where=where,
            overwrite=True,
        )

    def _insert_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        where: t.Optional[exp.Condition] = None,
        overwrite: bool = True,
    ) -> None:
        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=table_name,
            source_columns=source_columns,
        )
        target_columns_to_types = target_columns_to_types or self.columns(table_name)
        self._insert_source_queries_by_partition(
            table_name,
            source_queries,
            partitioned_by=partitioned_by,
            target_columns_to_types=target_columns_to_types,
            where=where,
            overwrite=overwrite,
        )

    def _insert_source_queries_by_partition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: t.Optional[exp.Condition] = None,
        overwrite: bool = True,
    ) -> None:
        projection_order = self._projection_order_for_partition_overwrite(
            target_columns_to_types, partitioned_by
        )
        partition_sql = ", ".join(
            self._quote_identifier(name) for name in self._partition_column_names(partitioned_by)
        )
        table = self._normalize_table(table_name)
        for i, source_query in enumerate(source_queries):
            command = "INSERT OVERWRITE TABLE" if overwrite and i == 0 else "INSERT INTO TABLE"
            with source_query as query:
                ordered_query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                    where=where,
                )
                ordered_query = self._normalize_query(ordered_query)
                self.execute(
                    f"{command} "
                    f"{table.sql(dialect=self.dialect, identify=True)} "
                    f"PARTITION ({partition_sql}) "
                    f"{ordered_query.sql(dialect=self.dialect, identify=True)}",
                    track_rows_processed=True,
                )

    def _insert_append_query(
        self,
        table_name: TableName,
        query: Query,
        target_columns_to_types: t.Dict[str, exp.DataType],
        order_projections: bool = True,
        track_rows_processed: bool = True,
    ) -> None:
        if order_projections:
            query = self._order_projections_and_filter(query, target_columns_to_types)
        query = self._normalize_query(query)
        table = self._normalize_table(table_name)
        columns = ", ".join(self._quote_identifier(name) for name in target_columns_to_types)
        self.execute(
            f"INSERT INTO {table.sql(dialect=self.dialect, identify=True)} "
            f"({columns}) {query.sql(dialect=self.dialect, identify=True)}",
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
        table = self._normalize_table(table_name)
        for i, source_query in enumerate(source_queries):
            with source_query as query:
                query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                    where=where,
                )
                query = self._normalize_query(query)
                if i > 0:
                    self._insert_append_query(
                        table_name,
                        query,
                        target_columns_to_types=target_columns_to_types,
                        order_projections=False,
                    )
                else:
                    self.execute(
                        f"INSERT OVERWRITE TABLE "
                        f"{table.sql(dialect=self.dialect, identify=True)} "
                        f"{query.sql(dialect=self.dialect, identify=True)}",
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
        if object_names and not self._is_schema_namespace_enabled():
            object_name_by_actual_name = {name: name for name in object_names}
            if namespace and namespace != project:
                object_name_by_actual_name = {f"{namespace}__{name}": name for name in object_names}
            actual_object_names = set(object_name_by_actual_name)

        objects: t.List[DataObject] = []
        for table in self.odps.list_tables(**list_tables_kwargs):
            if actual_object_names and table.name not in actual_object_names:
                continue
            objects.append(
                DataObject(
                    catalog=project,
                    schema=namespace if self._is_schema_namespace_enabled() else "",
                    name=object_name_by_actual_name.get(table.name, table.name),
                    type=(
                        DataObjectType.VIEW
                        if getattr(table, "is_virtual_view", False)
                        else DataObjectType.TABLE
                    ),
                )
            )
        return objects

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

    def _build_maxcompute_properties_sql(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expr]],
    ) -> str:
        table_properties = dict(table_properties or {})
        lifecycle = table_properties.pop("lifecycle", None)

        parts: t.List[str] = []
        if lifecycle is not None:
            parts.append(f"LIFECYCLE {_property_value_name(lifecycle)}")
        if table_properties:
            rendered = ", ".join(
                f"'{key}'='{_property_value_name(value)}'"
                for key, value in table_properties.items()
            )
            parts.append(f"TBLPROPERTIES ({rendered})")
        return " ".join(parts)

    def _requires_two_step_ctas(self, **kwargs: t.Any) -> bool:
        table_properties = kwargs.get("table_properties") or {}
        return bool(kwargs.get("partitioned_by") or table_properties)

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
            project = table.catalog or self._default_catalog or table.db
            return self._normalize_table(table).name, project, None

        project = table.catalog or self._default_catalog
        return table.name, project or table.db, table.db if project else None

    def _odps_type_to_sqlglot(self, type_: t.Any) -> exp.DataType:
        type_name = str(getattr(type_, "name", type_))
        return exp.DataType.build(type_name, dialect=self.dialect)

    def _quote_identifier(self, name: str) -> str:
        return exp.to_identifier(name).sql(dialect=self.dialect, identify=True)
