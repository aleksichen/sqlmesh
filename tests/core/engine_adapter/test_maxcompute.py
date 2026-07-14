import typing as t
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlglot import exp, parse_one

import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import MaxComputeEngineAdapter, create_engine_adapter
from sqlmesh.core.engine_adapter.mixins import RowDiffMixin
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.config.connection import _connection_config_validator
from sqlmesh.utils.errors import SQLMeshError

from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.maxcompute, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> MaxComputeEngineAdapter:
    return make_mocked_engine_adapter(MaxComputeEngineAdapter, register_comments=False)


def make_config(**kwargs) -> t.Any:
    return _connection_config_validator(None, kwargs)  # type: ignore


def make_no_schema_adapter(
    make_mocked_engine_adapter: t.Callable,
) -> MaxComputeEngineAdapter:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    adapter.connection.odps.is_schema_namespace_enabled.return_value = False
    return adapter


def test_maxcompute_adapter_registration() -> None:
    adapter = create_engine_adapter(lambda: None, dialect="maxcompute")
    assert isinstance(adapter, MaxComputeEngineAdapter)


def test_maxcompute_adapter_capabilities(adapter: MaxComputeEngineAdapter) -> None:
    assert adapter.dialect == "maxcompute"
    assert adapter.SUPPORTS_TRANSACTIONS is False
    assert adapter.SUPPORTS_REPLACE_TABLE is False
    assert adapter.SUPPORTS_UNPARTITIONED_INSERT_OVERWRITE is True
    assert adapter.SUPPORTS_MATERIALIZED_VIEWS is True
    assert adapter.SUPPORTS_METADATA_TABLE_LAST_MODIFIED_TS is True
    assert adapter.SUPPORTS_GRANTS is False
    assert adapter.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.INSERT_OVERWRITE
    assert adapter.COMMENT_CREATION_TABLE == CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    assert adapter.COMMENT_CREATION_VIEW == CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    assert isinstance(adapter, RowDiffMixin)


def test_maxcompute_connection_config_passes_hints_to_adapter() -> None:
    config = make_config(
        type="maxcompute",
        project="warehouse",
        endpoint="https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        access_key_id="ak",
        access_key_secret="sk",
        quota_name="q1",
        sql_hints={"odps.sql.allow.fullscan": "true"},
        check_import=False,
    )

    adapter = config.create_engine_adapter()

    assert isinstance(adapter, MaxComputeEngineAdapter)
    assert adapter.default_catalog == "warehouse"


def test_maxcompute_create_partitioned_table_lifecycle_properties(
    adapter: MaxComputeEngineAdapter,
) -> None:
    expressions = d.parse(
        """
        MODEL (
            name analytics.daily_orders,
            kind FULL,
            partitioned_by (ds),
            physical_properties (
                lifecycle = 30,
                compression = 'zstd'
            ),
            dialect maxcompute
        );

        SELECT 1::bigint AS order_id, 2::decimal(18, 2) AS amount, '2026-06-27' AS ds;
        """
    )
    model = load_sql_based_model(expressions)

    adapter.create_table(
        model.name,
        target_columns_to_types=model.columns_to_types_or_raise,
        table_properties=model.physical_properties,
        partitioned_by=model.partitioned_by,
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`order_id` BIGINT, `amount` DECIMAL(18, 2)) PARTITIONED BY (`ds` STRING) TBLPROPERTIES ('compression'='zstd') LIFECYCLE 30"
    ]


def test_maxcompute_rejects_non_column_partition_expression(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(
        SQLMeshError, match="MaxCompute partitioned_by only supports simple column references"
    ):
        adapter.create_table(
            "analytics.bad_partition",
            target_columns_to_types={"ds": exp.DataType.build("string")},
            partitioned_by=[parse_one("DATE(ds)")],
        )


def test_maxcompute_plain_ctas_uses_no_column_schema(adapter: MaxComputeEngineAdapter) -> None:
    adapter.ctas(
        "analytics.full_orders",
        query_or_df=parse_one("SELECT 1 AS order_id"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`full_orders` AS SELECT 1 AS `order_id`"
    ]


def test_maxcompute_partitioned_ctas_uses_create_then_partition_overwrite(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.ctas(
        "analytics.daily_orders",
        query_or_df=parse_one("SELECT ds, order_id FROM staging.orders"),
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
        },
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`order_id` BIGINT) PARTITIONED BY (`ds` STRING)",
        "INSERT OVERWRITE TABLE `analytics`.`daily_orders` PARTITION (`ds`) SELECT `order_id`, `ds` FROM `staging`.`orders`",
    ]
    assert all(
        "CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`ds` STRING, `order_id` BIGINT) AS SELECT"
        not in sql
        for sql in to_sql_calls(adapter)
    )


def test_maxcompute_lifecycle_ctas_uses_create_then_insert(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.ctas(
        "analytics.orders",
        query_or_df=parse_one("SELECT 1 AS order_id"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
        table_properties={"lifecycle": exp.Literal.number(7)},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (`order_id` BIGINT) LIFECYCLE 7",
        "INSERT INTO `analytics`.`orders` (`order_id`) SELECT 1 AS `order_id`",
    ]


def test_maxcompute_lifecycle_replace_query_uses_create_then_insert(
    adapter: MaxComputeEngineAdapter, mocker
) -> None:
    mocker.patch.object(adapter, "get_data_object", return_value=None)
    execute = mocker.spy(adapter, "execute")

    adapter.replace_query(
        "analytics.orders",
        query_or_df=parse_one("SELECT 1 AS order_id"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
        table_properties={"lifecycle": exp.Literal.number(7)},
        track_rows_processed=False,
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (`order_id` BIGINT) LIFECYCLE 7",
        "INSERT INTO `analytics`.`orders` (`order_id`) SELECT 1 AS `order_id`",
    ]
    assert execute.call_args_list[1].kwargs["track_rows_processed"] is False


def test_maxcompute_insert_append(adapter: MaxComputeEngineAdapter) -> None:
    adapter._insert_append_query(
        "analytics.orders",
        parse_one("SELECT 1 AS order_id, '2026-06-27' AS ds"),
        {"order_id": exp.DataType.build("bigint"), "ds": exp.DataType.build("string")},
    )

    assert to_sql_calls(adapter) == [
        "INSERT INTO `analytics`.`orders` (`order_id`, `ds`) SELECT 1 AS `order_id`, '2026-06-27' AS `ds`"
    ]


def test_maxcompute_insert_overwrite_partition_moves_partition_columns_to_end(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.insert_overwrite_by_partition(
        "analytics.fact_order_daily",
        parse_one("SELECT ds, price * quantity AS amount, order_id FROM staging.orders"),
        partitioned_by=[exp.column("ds")],
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `price` * `quantity` AS `amount`, `ds` FROM `staging`.`orders`"
    ]


def test_maxcompute_insert_overwrite_multiple_partitions_are_last_in_declared_order(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.insert_overwrite_by_partition(
        "analytics.fact_order_daily",
        parse_one("SELECT region, ds, amount, order_id FROM staging.orders"),
        partitioned_by=[exp.column("ds"), exp.column("region")],
        target_columns_to_types={
            "order_id": exp.DataType.build("bigint"),
            "ds": exp.DataType.build("string"),
            "region": exp.DataType.build("string"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`, `region`) SELECT `order_id`, `amount`, `ds`, `region` FROM `staging`.`orders`"
    ]


def test_maxcompute_time_partition_overwrite_routes_to_partition_clause(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter._insert_overwrite_by_time_partition(
        table_name="analytics.fact_order_daily",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one("SELECT ds, amount, order_id FROM staging.orders"),
                cleanup_func=lambda: None,
            )
        ],
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
        where=parse_one("ds BETWEEN '2026-06-01' AND '2026-06-27'"),
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM (SELECT `ds`, `amount`, `order_id` FROM `staging`.`orders`) AS `_subquery` WHERE `ds` BETWEEN '2026-06-01' AND '2026-06-27'"
    ]


def test_maxcompute_time_partition_overwrite_preserves_existing_where(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter._insert_overwrite_by_time_partition(
        table_name="analytics.fact_order_daily",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT ds, amount, order_id FROM staging.orders WHERE region = 'CN'"
                ),
                cleanup_func=lambda: None,
            )
        ],
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
        where=parse_one("ds BETWEEN '2026-06-01' AND '2026-06-27'"),
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM (SELECT `ds`, `amount`, `order_id` FROM `staging`.`orders` WHERE `region` = 'CN') AS `_subquery` WHERE `ds` BETWEEN '2026-06-01' AND '2026-06-27'"
    ]


def test_maxcompute_time_partition_overwrite_filters_projected_alias(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter._insert_overwrite_by_time_partition(
        table_name="analytics.fact_order_daily",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT DATE(order_ts) AS ds, amount, order_id FROM staging.orders"
                ),
                cleanup_func=lambda: None,
            )
        ],
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
        where=parse_one("ds BETWEEN '2026-06-01' AND '2026-06-27'"),
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM (SELECT DATE(`order_ts`) AS `ds`, `amount`, `order_id` FROM `staging`.`orders`) AS `_subquery` WHERE `ds` BETWEEN '2026-06-01' AND '2026-06-27'"
    ]


def test_maxcompute_partition_overwrite_appends_after_first_source_query(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter._insert_overwrite_by_condition(
        table_name="analytics.fact_order_daily",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT ds, amount, order_id FROM staging.orders_batch_1"
                ),
                cleanup_func=lambda: None,
            ),
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT ds, amount, order_id FROM staging.orders_batch_2"
                ),
                cleanup_func=lambda: None,
            ),
        ],
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders_batch_1`",
        "INSERT INTO TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders_batch_2`",
    ]


def test_maxcompute_public_partition_overwrite_appends_after_first_source_query(
    adapter: MaxComputeEngineAdapter,
    mocker,
) -> None:
    target_columns_to_types = {
        "ds": exp.DataType.build("string"),
        "order_id": exp.DataType.build("bigint"),
        "amount": exp.DataType.build("decimal(18,2)"),
    }
    mocker.patch.object(
        adapter,
        "_get_source_queries_and_columns_to_types",
        return_value=(
            [
                SourceQuery(
                    query_factory=lambda: parse_one(
                        "SELECT ds, amount, order_id FROM staging.orders_batch_1"
                    ),
                    cleanup_func=lambda: None,
                ),
                SourceQuery(
                    query_factory=lambda: parse_one(
                        "SELECT ds, amount, order_id FROM staging.orders_batch_2"
                    ),
                    cleanup_func=lambda: None,
                ),
            ],
            target_columns_to_types,
        ),
    )

    adapter.insert_overwrite_by_partition(
        table_name="analytics.fact_order_daily",
        query_or_df=parse_one("SELECT ds, amount, order_id FROM staging.orders"),
        partitioned_by=[exp.column("ds")],
        target_columns_to_types=target_columns_to_types,
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders_batch_1`",
        "INSERT INTO TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders_batch_2`",
    ]


def test_maxcompute_partitioned_ctas_appends_after_first_source_query(
    adapter: MaxComputeEngineAdapter,
    mocker,
) -> None:
    target_columns_to_types = {
        "ds": exp.DataType.build("string"),
        "order_id": exp.DataType.build("bigint"),
    }
    mocker.patch.object(
        adapter,
        "_get_source_queries_and_columns_to_types",
        return_value=(
            [
                SourceQuery(
                    query_factory=lambda: parse_one(
                        "SELECT ds, order_id FROM staging.orders_batch_1"
                    ),
                    cleanup_func=lambda: None,
                ),
                SourceQuery(
                    query_factory=lambda: parse_one(
                        "SELECT ds, order_id FROM staging.orders_batch_2"
                    ),
                    cleanup_func=lambda: None,
                ),
            ],
            target_columns_to_types,
        ),
    )

    adapter.ctas(
        "analytics.daily_orders",
        query_or_df=parse_one("SELECT ds, order_id FROM staging.orders"),
        target_columns_to_types=target_columns_to_types,
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`order_id` BIGINT) PARTITIONED BY (`ds` STRING)",
        "INSERT OVERWRITE TABLE `analytics`.`daily_orders` PARTITION (`ds`) SELECT `order_id`, `ds` FROM `staging`.`orders_batch_1`",
        "INSERT INTO TABLE `analytics`.`daily_orders` PARTITION (`ds`) SELECT `order_id`, `ds` FROM `staging`.`orders_batch_2`",
    ]


def test_maxcompute_replace_query_existing_partitioned_table_overwrites_without_recreate(
    make_mocked_engine_adapter: t.Callable,
    mocker,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
    )
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="warehouse",
            schema="analytics",
            name="fact_order_daily",
            type=DataObjectType.TABLE,
        ),
    )

    adapter.replace_query(
        "warehouse.analytics.fact_order_daily",
        parse_one("SELECT ds, amount, order_id FROM staging.orders"),
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
            "amount": exp.DataType.build("decimal(18,2)"),
        },
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders`",
    ]


def test_maxcompute_replace_query_existing_unpartitioned_table_uses_position_overwrite(
    make_mocked_engine_adapter: t.Callable,
    mocker,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
    )
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="warehouse",
            schema="analytics",
            name="dim_customer",
            type=DataObjectType.TABLE,
        ),
    )

    adapter.replace_query(
        "warehouse.analytics.dim_customer",
        parse_one("SELECT name, customer_id FROM staging.customers"),
        target_columns_to_types={
            "customer_id": exp.DataType.build("bigint"),
            "name": exp.DataType.build("string"),
        },
    )

    calls = to_sql_calls(adapter)
    assert calls == [
        "INSERT OVERWRITE TABLE `analytics`.`dim_customer` SELECT `customer_id`, `name` FROM `staging`.`customers`"
    ]
    assert "(`customer_id`, `name`)" not in calls[0]


def _odps_column(name: str, type_name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, type=SimpleNamespace(name=type_name))


def test_maxcompute_columns_uses_pyodps_table_schema(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    table = SimpleNamespace(
        table_schema=SimpleNamespace(
            columns=[
                _odps_column("order_id", "bigint"),
                _odps_column("amount", "decimal(18,2)"),
                _odps_column("tags", "array<string>"),
                _odps_column("attrs", "map<string,bigint>"),
                _odps_column("payload", "struct<a:bigint,b:string>"),
                _odps_column("ds", "string"),
            ]
        )
    )
    adapter.connection.odps.get_table.return_value = table

    assert adapter.columns("warehouse.analytics.orders") == {
        "order_id": exp.DataType.build("BIGINT"),
        "amount": exp.DataType.build("DECIMAL(18, 2)"),
        "tags": exp.DataType.build("ARRAY<STRING>", dialect="maxcompute"),
        "attrs": exp.DataType.build("MAP<STRING, BIGINT>", dialect="maxcompute"),
        "payload": exp.DataType.build("STRUCT<a: BIGINT, b: STRING>", dialect="maxcompute"),
        "ds": exp.DataType.build("STRING"),
    }
    adapter.connection.odps.get_table.assert_called_once_with(
        "orders", project="warehouse", schema="analytics"
    )


def test_maxcompute_no_schema_create_schema_is_noop(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    adapter.create_schema("analytics")
    adapter.create_schema("warehouse")

    assert to_sql_calls(adapter) == []


def test_maxcompute_configured_schema_marker_enables_schema_namespace(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="york_fic",
        patch_get_data_objects=False,
    )
    adapter.connection._sqlmesh_schema_namespace_configured = True

    class ODPS:
        is_schema_namespace_enabled = MagicMock(side_effect=AssertionError("unexpected call"))

        @property
        def schema(self) -> t.NoReturn:
            raise AssertionError("odps.schema must not be read")

    adapter.connection.odps = ODPS()

    adapter.create_schema("sqlmesh_smoke_1234")

    assert to_sql_calls(adapter) == ["CREATE SCHEMA IF NOT EXISTS `sqlmesh_smoke_1234`"]


def test_maxcompute_schema_namespace_detection_does_not_read_odps_schema(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )

    class ODPS:
        is_schema_namespace_enabled = MagicMock(return_value=False)

        @property
        def schema(self) -> t.NoReturn:
            raise AssertionError("odps.schema must not be read")

    odps = ODPS()
    adapter.connection.odps = odps

    assert adapter._is_schema_namespace_enabled() is False
    odps.is_schema_namespace_enabled.assert_called_once_with()


def test_maxcompute_no_schema_create_table_folds_schema_into_table_name(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    adapter.create_table(
        "warehouse.analytics.daily_orders",
        target_columns_to_types={
            "order_id": exp.DataType.build("bigint"),
            "ds": exp.DataType.build("string"),
        },
        partitioned_by=[exp.column("ds")],
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics__daily_orders` (`order_id` BIGINT) PARTITIONED BY (`ds` STRING)"
    ]


def test_maxcompute_no_schema_view_creation_folds_schema_into_table_name(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    adapter.connection.odps.list_tables.return_value = []

    adapter.create_view(
        "warehouse.analytics.orders_v",
        parse_one("SELECT order_id FROM analytics.orders"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE OR REPLACE VIEW `analytics__orders_v` AS SELECT `order_id` FROM `analytics__orders`"
    ]


def test_maxcompute_no_schema_insert_overwrite_folds_physical_table_name(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    adapter._insert_overwrite_by_condition(
        table_name="warehouse.sqlmesh__analytics.analytics__fact_order_daily__1234",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT order_id, ds FROM sqlmesh__staging.staging__orders__5678"
                ),
                cleanup_func=lambda: None,
            )
        ],
        target_columns_to_types={
            "order_id": exp.DataType.build("bigint"),
            "ds": exp.DataType.build("string"),
        },
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `sqlmesh__analytics__analytics__fact_order_daily__1234` SELECT `order_id`, `ds` FROM `sqlmesh__staging__staging__orders__5678`"
    ]


def test_maxcompute_no_schema_normalizes_all_rendered_asts(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    adapter.execute(parse_one("SELECT * FROM analytics.orders"))

    assert to_sql_calls(adapter) == ["SELECT * FROM `analytics__orders`"]


def test_maxcompute_no_schema_replace_query_new_table_folds_base_ctas_path(
    make_mocked_engine_adapter: t.Callable,
    mocker,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    mocker.patch.object(adapter, "get_data_object", return_value=None)

    adapter.replace_query(
        "warehouse.sqlmesh__analytics.analytics__dim_customer__1234",
        parse_one("SELECT customer_id FROM staging.customers"),
        target_columns_to_types={"customer_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `sqlmesh__analytics__analytics__dim_customer__1234` AS SELECT CAST(`customer_id` AS BIGINT) AS `customer_id` FROM (SELECT `customer_id` FROM `staging__customers`) AS `_subquery`"
    ]


def test_maxcompute_no_schema_project_db_is_not_folded_into_table_name(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    adapter.create_table(
        "warehouse.analytics__daily_orders__1234",
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics__daily_orders__1234` (`order_id` BIGINT)"
    ]


def test_maxcompute_table_exists(make_mocked_engine_adapter: t.Callable) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    adapter.connection.odps.exist_table.return_value = True
    assert adapter.table_exists("warehouse.analytics.orders") is True
    adapter.connection.odps.exist_table.assert_called_once_with(
        "orders", project="warehouse", schema="analytics"
    )


def test_maxcompute_no_schema_table_parts_uses_project_without_schema(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    adapter.connection.odps.exist_table.return_value = True

    assert adapter.table_exists("warehouse.analytics.orders") is True
    adapter.connection.odps.exist_table.assert_called_once_with(
        "analytics__orders", project="warehouse", schema=None
    )


def test_maxcompute_get_data_objects_maps_tables_and_views(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="orders", is_virtual_view=False),
        SimpleNamespace(name="orders_v", is_virtual_view=True),
        SimpleNamespace(name="orders_mv", is_virtual_view=False, is_materialized_view=True),
    ]

    objects = adapter._get_data_objects("warehouse.analytics")

    assert [(obj.name, obj.type) for obj in objects] == [
        ("orders", DataObjectType.TABLE),
        ("orders_v", DataObjectType.VIEW),
        ("orders_mv", DataObjectType.MATERIALIZED_VIEW),
    ]
    adapter.connection.odps.list_tables.assert_called_once_with(
        project="warehouse", schema="analytics"
    )


def test_maxcompute_no_schema_get_data_objects_lists_project_without_schema(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="analytics__orders", is_virtual_view=False),
        SimpleNamespace(name="analytics__orders_v", is_virtual_view=True),
        SimpleNamespace(name="other__orders", is_virtual_view=False),
    ]

    objects = adapter._get_data_objects("warehouse.analytics", {"orders", "orders_v"})

    assert [(obj.catalog, obj.schema_name, obj.name, obj.type) for obj in objects] == [
        ("warehouse", "analytics", "orders", DataObjectType.TABLE),
        ("warehouse", "analytics", "orders_v", DataObjectType.VIEW),
    ]
    adapter.connection.odps.list_tables.assert_called_once_with(project="warehouse")


def test_maxcompute_no_schema_get_data_objects_does_not_match_unfolded_project_table(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="orders", is_virtual_view=False),
    ]

    assert adapter._get_data_objects("warehouse.analytics", {"orders"}) == []
    adapter.connection.odps.list_tables.assert_called_once_with(project="warehouse")


def test_maxcompute_no_schema_get_data_objects_filters_unbounded_listing(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="analytics__orders", is_virtual_view=False),
        SimpleNamespace(name="finance__payments", is_virtual_view=False),
    ]

    objects = adapter._get_data_objects("warehouse.analytics")

    assert [(obj.schema_name, obj.name) for obj in objects] == [("analytics", "orders")]


def test_maxcompute_get_data_objects_omits_empty_schema(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="orders", is_virtual_view=False),
    ]

    objects = adapter._get_data_objects(exp.Table())

    assert [(obj.catalog, obj.schema_name, obj.name, obj.type) for obj in objects] == [
        ("warehouse", "", "orders", DataObjectType.TABLE),
    ]
    adapter.connection.odps.list_tables.assert_called_once_with(project="warehouse")


def test_maxcompute_view_creation(adapter: MaxComputeEngineAdapter) -> None:
    adapter.create_view(
        "analytics.orders_v",
        parse_one("SELECT order_id FROM analytics.orders"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE OR REPLACE VIEW `analytics`.`orders_v` AS SELECT `order_id` FROM `analytics`.`orders`"
    ]


def test_maxcompute_empty_partition_overwrite_uses_unpartitioned_statement(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.insert_overwrite_by_partition(
        "analytics.orders",
        parse_one("SELECT order_id FROM staging.orders"),
        partitioned_by=[],
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "INSERT OVERWRITE TABLE `analytics`.`orders` SELECT `order_id` FROM `staging`.`orders`"
    ]


def test_maxcompute_partitioned_insert_append_uses_dynamic_partition_clause(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(partitions=[_odps_column("ds", "string")])
    )

    adapter.insert_append(
        "analytics.orders",
        parse_one("SELECT ds, order_id FROM staging.orders"),
        target_columns_to_types={
            "ds": exp.DataType.build("string"),
            "order_id": exp.DataType.build("bigint"),
        },
    )

    assert to_sql_calls(adapter) == [
        "INSERT INTO TABLE `analytics`.`orders` PARTITION (`ds`) SELECT `order_id`, `ds` FROM `staging`.`orders`"
    ]


def test_maxcompute_auto_partitioned_insert_append_omits_partition_clause(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(
            partitions=[
                SimpleNamespace(name="ds", generate_expression="TRUNC_TIME(event_ts, 'day')")
            ]
        )
    )

    adapter.insert_append(
        "analytics.events",
        parse_one("SELECT event_id, event_ts FROM staging.events"),
        target_columns_to_types={
            "event_id": exp.DataType.build("bigint"),
            "event_ts": exp.DataType.build("timestamp"),
        },
    )

    assert to_sql_calls(adapter) == [
        "INSERT INTO `analytics`.`events` (`event_id`, `event_ts`) SELECT `event_id`, `event_ts` FROM `staging`.`events`"
    ]


def test_maxcompute_auto_partitioned_overwrite_requires_bounded_condition(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(
            partitions=[
                SimpleNamespace(name="ds", generate_expression="TRUNC_TIME(event_ts, 'day')")
            ]
        )
    )

    with pytest.raises(SQLMeshError, match="requires a bounded condition"):
        adapter.insert_overwrite_by_partition(
            "analytics.events",
            parse_one("SELECT event_id, event_ts FROM staging.events"),
            partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
            target_columns_to_types={
                "event_id": exp.DataType.build("bigint"),
                "event_ts": exp.DataType.build("timestamp"),
            },
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_auto_partitioned_time_overwrite_preserves_other_intervals(
    adapter: MaxComputeEngineAdapter,
    mocker,
) -> None:
    adapter._default_catalog = "warehouse"
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(
            partitions=[
                SimpleNamespace(name="ds", generate_expression="TRUNC_TIME(event_ts, 'day')")
            ]
        )
    )
    mocker.patch.object(
        adapter,
        "_get_temp_table",
        return_value=exp.to_table("analytics.__temp_events"),
    )

    adapter._insert_overwrite_by_time_partition(
        "analytics.events",
        [
            SourceQuery(
                query_factory=lambda: parse_one("SELECT event_id, event_ts FROM staging.events")
            )
        ],
        target_columns_to_types={
            "event_id": exp.DataType.build("bigint"),
            "event_ts": exp.DataType.build("timestamp"),
        },
        where=parse_one(
            "event_ts >= CAST('2026-07-14 00:00:00' AS TIMESTAMP) "
            "AND event_ts < CAST('2026-07-15 00:00:00' AS TIMESTAMP)"
        ),
        partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
    )

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 5
    assert sql_calls[0] == "CREATE SCHEMA IF NOT EXISTS `analytics`"
    assert sql_calls[1] == (
        "CREATE TABLE IF NOT EXISTS `analytics`.`__temp_events` "
        "(`event_id` BIGINT, `event_ts` TIMESTAMP) LIFECYCLE 1"
    )
    assert sql_calls[2].startswith(
        "INSERT INTO `analytics`.`__temp_events` (`event_id`, `event_ts`) "
    )
    assert "FROM `analytics`.`events` WHERE NOT" in sql_calls[2]
    assert "UNION ALL" in sql_calls[2]
    assert "FROM `staging`.`events`" in sql_calls[2]
    assert sql_calls[3] == (
        "INSERT OVERWRITE TABLE `analytics`.`events` "
        "SELECT `event_id`, `event_ts` FROM `analytics`.`__temp_events`"
    )
    assert sql_calls[4] == "DROP TABLE IF EXISTS `analytics`.`__temp_events`"


@pytest.mark.parametrize(
    "target_partitions",
    [
        [],
        [SimpleNamespace(name="ds", generate_expression="TRUNC_TIME(event_ts, 'month')")],
    ],
)
def test_maxcompute_auto_partitioned_overwrite_validates_target_expression(
    adapter: MaxComputeEngineAdapter,
    target_partitions: list[SimpleNamespace],
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(partitions=target_partitions)
    )

    with pytest.raises(SQLMeshError, match="does not match the target table"):
        adapter._insert_overwrite_by_time_partition(
            "analytics.events",
            [
                SourceQuery(
                    query_factory=lambda: parse_one("SELECT event_id, event_ts FROM staging.events")
                )
            ],
            target_columns_to_types={
                "event_id": exp.DataType.build("bigint"),
                "event_ts": exp.DataType.build("timestamp"),
            },
            where=parse_one("event_ts >= CAST('2026-07-14' AS TIMESTAMP)"),
            partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_rejects_dataframe_writes(adapter: MaxComputeEngineAdapter) -> None:
    import pandas as pd

    with pytest.raises(SQLMeshError, match="does not support DataFrame writes"):
        adapter.insert_append(
            "analytics.orders",
            pd.DataFrame({"order_id": [1]}),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
        )


def test_maxcompute_fetchdf_uses_cursor_description_and_fetchall(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.cursor.description = [("order_id",), ("name",)]
    adapter.cursor.fetchall.return_value = [(1, "one"), (2, "two")]

    result = adapter.fetchdf("SELECT order_id, name FROM analytics.orders")

    assert result.to_dict("records") == [
        {"order_id": 1, "name": "one"},
        {"order_id": 2, "name": "two"},
    ]
    adapter.cursor.fetchall.assert_called_once_with()


def test_maxcompute_get_table_last_modified_ts_uses_pyodps_metadata(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(
        MaxComputeEngineAdapter,
        register_comments=False,
        default_catalog="warehouse",
        patch_get_data_objects=False,
    )
    adapter.connection.odps.get_table.side_effect = [
        SimpleNamespace(last_data_modified_time=datetime(2026, 1, 1, tzinfo=timezone.utc)),
        SimpleNamespace(last_data_modified_time=datetime(2026, 1, 2, tzinfo=timezone.utc)),
    ]

    assert adapter.get_table_last_modified_ts(
        ["warehouse.analytics.orders", "warehouse.analytics.customers"]
    ) == [1767225600000, 1767312000000]
    assert adapter.connection.odps.get_table.call_args_list == [
        (("orders",), {"project": "warehouse", "schema": "analytics"}),
        (("customers",), {"project": "warehouse", "schema": "analytics"}),
    ]


def test_maxcompute_truncate_rejects_partitioned_tables(adapter: MaxComputeEngineAdapter) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(partitions=[_odps_column("ds", "string")])
    )

    with pytest.raises(SQLMeshError, match="cannot truncate a partitioned table"):
        adapter._truncate_table("analytics.orders")

    assert to_sql_calls(adapter) == []


def test_maxcompute_truncate_non_partitioned_table(adapter: MaxComputeEngineAdapter) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(partitions=[])
    )

    adapter._truncate_table("analytics.orders")

    assert to_sql_calls(adapter) == ["TRUNCATE TABLE `analytics`.`orders`"]


def test_maxcompute_rename_requires_same_namespace(adapter: MaxComputeEngineAdapter) -> None:
    with pytest.raises(SQLMeshError, match="within the same project and schema"):
        adapter.rename_table("analytics.orders", "reporting.orders")

    assert to_sql_calls(adapter) == []


def test_maxcompute_no_schema_rename_requires_same_logical_namespace(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_no_schema_adapter(make_mocked_engine_adapter)

    with pytest.raises(SQLMeshError, match="within the same project and schema"):
        adapter.rename_table("warehouse.analytics.orders", "warehouse.reporting.orders")

    assert to_sql_calls(adapter) == []


def test_maxcompute_rename_uses_unqualified_new_name(adapter: MaxComputeEngineAdapter) -> None:
    adapter.rename_table("analytics.orders", "analytics.orders_v2")

    assert to_sql_calls(adapter) == ["ALTER TABLE `analytics`.`orders` RENAME TO `orders_v2`"]


def test_maxcompute_create_table_renders_comments_at_create_time(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(MaxComputeEngineAdapter, register_comments=True)

    adapter.create_table(
        "analytics.orders",
        {"order_id": exp.DataType.build("bigint"), "name": exp.DataType.build("string")},
        table_description="Orders 'ready'",
        column_descriptions={"order_id": "Order id", "name": "Display name"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (`order_id` BIGINT COMMENT 'Order id', `name` STRING COMMENT 'Display name') COMMENT 'Orders \\'ready\\''"
    ]


def test_maxcompute_create_table_renders_partition_column_comments(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(MaxComputeEngineAdapter, register_comments=True)

    adapter.create_table(
        "analytics.orders",
        {
            "order_id": exp.DataType.build("bigint"),
            "ds": exp.DataType.build("string"),
        },
        partitioned_by=[exp.column("ds")],
        column_descriptions={"ds": "Business date"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (`order_id` BIGINT) "
        "PARTITIONED BY (`ds` STRING COMMENT 'Business date')"
    ]


def test_maxcompute_create_view_renders_comments_at_create_time(
    make_mocked_engine_adapter: t.Callable,
) -> None:
    adapter = make_mocked_engine_adapter(MaxComputeEngineAdapter, register_comments=True)

    adapter.create_view(
        "analytics.orders_v",
        parse_one("SELECT order_id, name FROM analytics.orders"),
        target_columns_to_types={
            "order_id": exp.DataType.build("bigint"),
            "name": exp.DataType.build("string"),
        },
        table_description="Orders view",
        column_descriptions={"order_id": "Order id", "name": "Display name"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE OR REPLACE VIEW `analytics`.`orders_v` (`order_id` COMMENT 'Order id', `name` COMMENT 'Display name') COMMENT 'Orders view' AS SELECT `order_id`, `name` FROM `analytics`.`orders`"
    ]


def test_maxcompute_delta_table_physical_properties(adapter: MaxComputeEngineAdapter) -> None:
    adapter.create_table(
        "analytics.orders",
        {
            "order_id": exp.DataType.build("bigint"),
            "name": exp.DataType.build("string"),
            "ds": exp.DataType.build("string"),
        },
        partitioned_by=[exp.column("ds")],
        table_properties={
            "transactional": exp.true(),
            "primary_key": exp.Tuple(expressions=[exp.column("order_id")]),
            "write_bucket_num": exp.Literal.number(64),
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (`order_id` BIGINT NOT NULL, `name` STRING, PRIMARY KEY (`order_id`)) PARTITIONED BY (`ds` STRING) TBLPROPERTIES ('transactional'='true', 'write.bucket.num'='64')"
    ]


def test_maxcompute_delta_table_rejects_clustering(adapter: MaxComputeEngineAdapter) -> None:
    with pytest.raises(SQLMeshError, match="cannot be clustered"):
        adapter.create_table(
            "analytics.orders",
            {"order_id": exp.DataType.build("bigint")},
            clustered_by=[exp.column("order_id")],
            table_properties={
                "transactional": exp.true(),
                "primary_key": exp.column("order_id"),
                "cluster_bucket_num": exp.Literal.number(32),
            },
        )


def test_maxcompute_primary_key_requires_transactional_property(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="primary_key requires transactional = true"):
        adapter.create_table(
            "analytics.orders",
            {"order_id": exp.DataType.build("bigint")},
            table_properties={"primary_key": exp.Tuple(expressions=[exp.column("order_id")])},
        )


def test_maxcompute_unique_key_model_requires_transactional_property(
    adapter: MaxComputeEngineAdapter,
) -> None:
    model = load_sql_based_model(
        d.parse(
            """
            MODEL (
              name analytics.orders,
              kind INCREMENTAL_BY_UNIQUE_KEY (unique_key order_id),
              dialect maxcompute
            );
            SELECT 1 AS order_id;
            """
        )
    )

    with pytest.raises(SQLMeshError, match="transactional = true"):
        adapter.adjust_physical_properties_for_incremental(
            {},
            model_kind=model.kind,
            partitioned_by=model.partitioned_by,
            requires_delete_capable_table=True,
            unique_key=model.unique_key,
            model_name=model.name,
        )


def test_maxcompute_unique_key_primary_key_must_match_model_key(
    adapter: MaxComputeEngineAdapter,
) -> None:
    model = load_sql_based_model(
        d.parse(
            """
            MODEL (
              name analytics.orders,
              kind INCREMENTAL_BY_UNIQUE_KEY (unique_key order_id),
              dialect maxcompute
            );
            SELECT 1 AS order_id, 2 AS customer_id;
            """
        )
    )

    with pytest.raises(SQLMeshError, match="primary_key must match"):
        adapter.adjust_physical_properties_for_incremental(
            {
                "transactional": exp.true(),
                "primary_key": exp.column("customer_id"),
            },
            model_kind=model.kind,
            partitioned_by=model.partitioned_by,
            requires_delete_capable_table=True,
            unique_key=model.unique_key,
            model_name=model.name,
        )


def test_maxcompute_scd_type_2_rejects_partitioning(
    adapter: MaxComputeEngineAdapter,
) -> None:
    model = load_sql_based_model(
        d.parse(
            """
            MODEL (
              name analytics.customers,
              kind SCD_TYPE_2_BY_TIME (unique_key id, updated_at_name updated_at),
              columns (id BIGINT, updated_at TIMESTAMP, ds STRING),
              partitioned_by [ds],
              dialect maxcompute
            );
            SELECT 1 AS id, CAST('2026-07-14' AS TIMESTAMP) AS updated_at, '2026-07-14' AS ds;
            """
        )
    )

    with pytest.raises(SQLMeshError, match="do not support partitioned_by"):
        adapter.adjust_physical_properties_for_incremental(
            {"transactional": exp.true()},
            model_kind=model.kind,
            partitioned_by=model.partitioned_by,
            requires_delete_capable_table=True,
            unique_key=model.unique_key,
            model_name=model.name,
        )


@pytest.mark.parametrize(
    ("source_type", "rendered_type"),
    [
        ("date", "DATE"),
        ("datetime", "DATETIME"),
        ("timestamp", "TIMESTAMP"),
        ("timestamp_ntz", "TIMESTAMP_NTZ"),
    ],
)
def test_maxcompute_auto_partitions_temporal_columns_with_trunc_time(
    adapter: MaxComputeEngineAdapter, source_type: str, rendered_type: str
) -> None:
    adapter.create_table(
        "analytics.events",
        {
            "event_id": exp.DataType.build("bigint"),
            "event_ts": exp.DataType.build(source_type),
        },
        partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`events` "
        f"(`event_id` BIGINT, `event_ts` {rendered_type}) "
        "AUTO PARTITIONED BY (TRUNC_TIME(`event_ts`, 'day') AS `ds`)"
    ]


def test_maxcompute_auto_partition_rejects_non_temporal_source(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="must be DATE, DATETIME, TIMESTAMP, or TIMESTAMP_NTZ"):
        adapter.create_table(
            "analytics.events",
            {"event_ts": exp.DataType.build("string")},
            partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
        )

    assert to_sql_calls(adapter) == []


@pytest.mark.parametrize("source_type", ["date", "datetime", "timestamp", "timestamp_ntz"])
def test_maxcompute_auto_partition_requires_explicit_trunc_time(
    adapter: MaxComputeEngineAdapter, source_type: str
) -> None:
    with pytest.raises(SQLMeshError, match="explicit TRUNC_TIME"):
        adapter.create_table(
            "analytics.events",
            {"event_ts": exp.DataType.build(source_type)},
            partitioned_by=[exp.column("event_ts")],
            partition_interval_unit="day",
        )


def test_maxcompute_schema_evolution_renders_native_alter_syntax(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(
            columns=[
                _odps_column("order_id", "int"),
                _odps_column("obsolete", "string"),
            ],
            partitions=[],
        )
    )

    adapter.alter_table(
        [
            exp.Alter(
                this=exp.to_table("analytics.orders"),
                kind="TABLE",
                actions=[
                    exp.ColumnDef(
                        this=exp.to_identifier("amount"),
                        kind=exp.DataType.build("decimal(18, 2)"),
                    )
                ],
            ),
            exp.Alter(
                this=exp.to_table("analytics.orders"),
                kind="TABLE",
                actions=[exp.Drop(this=exp.to_identifier("obsolete"), kind="COLUMN")],
            ),
            exp.Alter(
                this=exp.to_table("analytics.orders"),
                kind="TABLE",
                actions=[
                    exp.AlterColumn(
                        this=exp.to_identifier("order_id"),
                        dtype=exp.DataType.build("bigint"),
                    )
                ],
            ),
        ]
    )

    assert to_sql_calls(adapter) == [
        "ALTER TABLE `analytics`.`orders` ADD COLUMNS (`amount` DECIMAL(18, 2))",
        "ALTER TABLE `analytics`.`orders` DROP COLUMN `obsolete`",
        "ALTER TABLE `analytics`.`orders` CHANGE COLUMN `order_id` `order_id` BIGINT",
    ]


def test_maxcompute_schema_evolution_rejects_unsafe_type_change_before_ddl(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        table_schema=SimpleNamespace(columns=[_odps_column("order_id", "bigint")], partitions=[])
    )

    with pytest.raises(SQLMeshError, match="unsafe MaxCompute type change"):
        adapter.alter_table(
            [
                exp.Alter(
                    this=exp.to_table("analytics.orders"),
                    kind="TABLE",
                    actions=[
                        exp.AlterColumn(
                            this=exp.to_identifier("order_id"),
                            dtype=exp.DataType.build("int"),
                        )
                    ],
                )
            ]
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_hash_clustering_requires_bucket_property(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="cluster_bucket_num"):
        adapter.create_table(
            "analytics.orders",
            {"order_id": exp.DataType.build("bigint")},
            clustered_by=[exp.column("order_id")],
        )


def test_maxcompute_native_merge_requires_transactional_table(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        is_transactional=False,
        table_schema=SimpleNamespace(partitions=[]),
    )

    with pytest.raises(SQLMeshError, match="requires a transactional target table"):
        adapter.merge(
            "analytics.orders",
            parse_one("SELECT order_id, name FROM staging.orders"),
            {"order_id": exp.DataType.build("bigint"), "name": exp.DataType.build("string")},
            unique_key=[exp.column("order_id")],
        )


def test_maxcompute_native_merge_excludes_partition_columns_from_update(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        is_transactional=True,
        primary_key=["order_id"],
        table_schema=SimpleNamespace(partitions=[_odps_column("ds", "string")]),
    )

    adapter.merge(
        "analytics.orders",
        parse_one("SELECT order_id, name, ds FROM staging.orders"),
        {
            "order_id": exp.DataType.build("bigint"),
            "name": exp.DataType.build("string"),
            "ds": exp.DataType.build("string"),
        },
        unique_key=[exp.column("order_id")],
    )

    assert to_sql_calls(adapter) == [
        "MERGE INTO `analytics`.`orders` AS `__MERGE_TARGET__` USING (SELECT `order_id`, `name`, `ds` FROM `staging`.`orders`) AS `__MERGE_SOURCE__` ON `__MERGE_TARGET__`.`order_id` = `__MERGE_SOURCE__`.`order_id` WHEN MATCHED THEN UPDATE SET `__MERGE_TARGET__`.`name` = `__MERGE_SOURCE__`.`name` WHEN NOT MATCHED THEN INSERT (`order_id`, `name`, `ds`) VALUES (`__MERGE_SOURCE__`.`order_id`, `__MERGE_SOURCE__`.`name`, `__MERGE_SOURCE__`.`ds`)"
    ]


def test_maxcompute_native_merge_omits_empty_update_clause(
    adapter: MaxComputeEngineAdapter,
) -> None:
    adapter.connection.odps.get_table.return_value = SimpleNamespace(
        is_transactional=True,
        primary_key=["order_id"],
        table_schema=SimpleNamespace(partitions=[]),
    )

    adapter.merge(
        "analytics.orders",
        parse_one("SELECT order_id FROM staging.orders"),
        {"order_id": exp.DataType.build("bigint")},
        unique_key=[exp.column("order_id")],
    )

    assert to_sql_calls(adapter) == [
        "MERGE INTO `analytics`.`orders` AS `__MERGE_TARGET__` USING (SELECT `order_id` FROM `staging`.`orders`) AS `__MERGE_SOURCE__` ON `__MERGE_TARGET__`.`order_id` = `__MERGE_SOURCE__`.`order_id` WHEN NOT MATCHED THEN INSERT (`order_id`) VALUES (`__MERGE_SOURCE__`.`order_id`)"
    ]


def test_maxcompute_create_and_drop_materialized_view(adapter: MaxComputeEngineAdapter) -> None:
    adapter.create_view(
        "analytics.orders_mv",
        parse_one("SELECT order_id, ds FROM analytics.orders"),
        target_columns_to_types={
            "order_id": exp.DataType.build("bigint"),
            "ds": exp.DataType.build("string"),
        },
        replace=False,
        materialized=True,
        materialized_properties={
            "partitioned_by": [exp.column("ds")],
            "clustered_by": [exp.column("order_id")],
        },
        view_properties={
            "lifecycle": exp.Literal.number(7),
            "cluster_bucket_num": exp.Literal.number(16),
            "enable_auto_refresh": exp.true(),
        },
    )
    adapter.drop_view("analytics.orders_mv", materialized=True)

    assert to_sql_calls(adapter) == [
        "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`orders_mv` LIFECYCLE 7 PARTITIONED ON (`ds`) CLUSTERED BY (`order_id`) INTO 16 BUCKETS TBLPROPERTIES ('enable_auto_refresh'='true') AS SELECT `order_id`, `ds` FROM `analytics`.`orders`",
        "DROP MATERIALIZED VIEW IF EXISTS `analytics`.`orders_mv`",
    ]


def test_maxcompute_validates_materialized_view_before_replacing(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="cluster_bucket_num"):
        adapter.create_view(
            "analytics.orders_mv",
            parse_one("SELECT order_id FROM analytics.orders"),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
            materialized=True,
            replace=True,
            materialized_properties={"clustered_by": [exp.column("order_id")]},
        )

    assert to_sql_calls(adapter) == []


@pytest.mark.parametrize("property_name", ["transactional", "primary_key", "write_bucket_num"])
def test_maxcompute_materialized_view_rejects_transactional_table_properties(
    adapter: MaxComputeEngineAdapter,
    property_name: str,
) -> None:
    property_value = (
        exp.column("order_id")
        if property_name == "primary_key"
        else exp.true()
        if property_name == "transactional"
        else exp.Literal.number(16)
    )

    with pytest.raises(SQLMeshError, match="not supported for materialized views"):
        adapter.create_view(
            "analytics.orders_mv",
            parse_one("SELECT order_id FROM analytics.orders"),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
            materialized=True,
            replace=True,
            view_properties={property_name: property_value},
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_materialized_view_cluster_bucket_requires_clustered_by(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="cluster_bucket_num requires clustered_by"):
        adapter.create_view(
            "analytics.orders_mv",
            parse_one("SELECT order_id FROM analytics.orders"),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
            materialized=True,
            replace=True,
            view_properties={"cluster_bucket_num": exp.Literal.number(16)},
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_regular_view_rejects_materialized_properties(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="only supported for materialized views"):
        adapter.create_view(
            "analytics.orders_v",
            parse_one("SELECT order_id FROM analytics.orders"),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
            materialized_properties={"partitioned_by": [exp.column("order_id")]},
        )

    assert to_sql_calls(adapter) == []


def test_maxcompute_regular_view_rejects_storage_properties(
    adapter: MaxComputeEngineAdapter,
) -> None:
    with pytest.raises(SQLMeshError, match="only supported for materialized views"):
        adapter.create_view(
            "analytics.orders_v",
            parse_one("SELECT order_id FROM analytics.orders"),
            target_columns_to_types={"order_id": exp.DataType.build("bigint")},
            view_properties={"lifecycle": exp.Literal.number(1)},
        )

    assert to_sql_calls(adapter) == []


@pytest.mark.parametrize(
    ("existing_type", "materialized", "expected_prefix"),
    [
        (DataObjectType.TABLE, False, "DROP TABLE IF EXISTS"),
        (DataObjectType.VIEW, True, "DROP VIEW IF EXISTS"),
        (DataObjectType.MATERIALIZED_VIEW, False, "DROP MATERIALIZED VIEW IF EXISTS"),
    ],
)
def test_maxcompute_view_reconciles_existing_object_type(
    adapter: MaxComputeEngineAdapter,
    mocker,
    existing_type: DataObjectType,
    materialized: bool,
    expected_prefix: str,
) -> None:
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="",
            schema="analytics",
            name="orders_v",
            type=existing_type,
        ),
    )

    adapter.create_view(
        "analytics.orders_v",
        parse_one("SELECT order_id FROM analytics.orders"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
        materialized=materialized,
    )

    assert to_sql_calls(adapter)[0].startswith(expected_prefix)


def test_maxcompute_type_widening_rejects_parameter_arity_mismatch(
    adapter: MaxComputeEngineAdapter,
) -> None:
    assert not adapter._is_safe_type_change(
        exp.DataType.build("DECIMAL(10, 2)"), exp.DataType.build("DECIMAL(20)")
    )


def test_maxcompute_execution_and_postgres_state_connection_can_coexist() -> None:
    execution = make_config(
        type="maxcompute",
        project="warehouse",
        endpoint="https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        access_key_id="ak",
        access_key_secret="sk",
        check_import=False,
    )
    state = make_config(
        type="postgres",
        host="localhost",
        port=5432,
        user="sqlmesh",
        password="sqlmesh",
        database="sqlmesh_state",
        check_import=False,
    )

    assert execution.is_forbidden_for_state_sync is True
    assert state.is_recommended_for_state_sync is True
