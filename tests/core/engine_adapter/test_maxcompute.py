import typing as t
from types import SimpleNamespace

import pytest
from sqlglot import exp, parse_one

import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import MaxComputeEngineAdapter, create_engine_adapter
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
    assert adapter.SUPPORTS_MATERIALIZED_VIEWS is False
    assert adapter.SUPPORTS_GRANTS is False
    assert adapter.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.INSERT_OVERWRITE
    assert adapter.COMMENT_CREATION_TABLE == CommentCreationTable.UNSUPPORTED
    assert adapter.COMMENT_CREATION_VIEW == CommentCreationView.UNSUPPORTED


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
        "CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`order_id` BIGINT, `amount` DECIMAL(18, 2)) PARTITIONED BY (`ds` STRING) LIFECYCLE 30 TBLPROPERTIES ('compression'='zstd')"
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


def test_maxcompute_replace_query_existing_partitioned_table_drops_and_recreates(
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
        "DROP TABLE IF EXISTS `analytics`.`fact_order_daily`",
        "CREATE TABLE `analytics`.`fact_order_daily` (`order_id` BIGINT, `amount` DECIMAL(18, 2)) PARTITIONED BY (`ds` STRING)",
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
    ]

    objects = adapter._get_data_objects("warehouse.analytics")

    assert [(obj.name, obj.type) for obj in objects] == [
        ("orders", DataObjectType.TABLE),
        ("orders_v", DataObjectType.VIEW),
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
        ("warehouse", "", "orders", DataObjectType.TABLE),
        ("warehouse", "", "orders_v", DataObjectType.VIEW),
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
