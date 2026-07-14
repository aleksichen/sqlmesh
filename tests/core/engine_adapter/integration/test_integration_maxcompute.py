import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.categorizer import CategorizerConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig, MaxComputeConnectionConfig
from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter.shared import SourceQuery
from sqlmesh.utils.date import to_timestamp

pytestmark = pytest.mark.maxcompute

_TWO_TIER_PROJECT_ERROR = "not 3-tier model project"


def _has_maxcompute_env() -> bool:
    return all(
        os.getenv(name)
        for name in (
            "MAXCOMPUTE_PROJECT",
            "MAXCOMPUTE_ENDPOINT",
            "MAXCOMPUTE_ACCESS_KEY_ID",
            "MAXCOMPUTE_ACCESS_KEY_SECRET",
        )
    )


def _schema_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_SCHEMA_SMOKE", "").lower() in {"1", "true", "yes"}


def _no_schema_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_NO_SCHEMA_SMOKE", "").lower() in {"1", "true", "yes"}


def _lifecycle_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_LIFECYCLE_SMOKE", "").lower() in {"1", "true", "yes"}


def _audit_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_AUDIT_SMOKE", "").lower() in {"1", "true", "yes"}


def _capability_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_CAPABILITY_SMOKE", "").lower() in {"1", "true", "yes"}


def _schema_evolution_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_SCHEMA_EVOLUTION_SMOKE", "").lower() in {
        "1",
        "true",
        "yes",
    }


def _transactional_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_TRANSACTIONAL_SMOKE", "").lower() in {
        "1",
        "true",
        "yes",
    }


def _maxqa_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_MAXQA_SMOKE", "").lower() in {"1", "true", "yes"}


def _model_kind_smoke_requested() -> bool:
    return os.getenv("MAXCOMPUTE_MODEL_KIND_SMOKE", "").lower() in {"1", "true", "yes"}


def _is_two_tier_project_error(error: Exception) -> bool:
    return _TWO_TIER_PROJECT_ERROR in str(error).casefold()


def _cleanup_prefixed_objects(odps, project: str, schema: str, prefixes: tuple[str, ...]) -> None:
    from odps.errors import NoSuchObject

    cleanup_prefixes = prefixes + tuple(f"__temp_{prefix}" for prefix in prefixes)
    deadline = time.monotonic() + 30
    consecutive_empty_lists = 0
    while time.monotonic() < deadline:
        matching_objects = [
            table
            for table in odps.list_tables(project=project, schema=schema)
            if table.name.startswith(cleanup_prefixes)
        ]
        if not matching_objects:
            consecutive_empty_lists += 1
            if consecutive_empty_lists == 3:
                return
            time.sleep(1)
            continue

        consecutive_empty_lists = 0
        for table in matching_objects:
            try:
                if getattr(table, "is_materialized_view", False):
                    odps.delete_materialized_view(
                        table.name, project=project, schema=schema, if_exists=True
                    )
                elif getattr(table, "is_virtual_view", False):
                    odps.delete_view(table.name, project=project, schema=schema, if_exists=True)
                else:
                    odps.delete_table(table.name, project=project, schema=schema, if_exists=True)
            except NoSuchObject:
                continue
        time.sleep(1)

    remaining = [
        table.name
        for table in odps.list_tables(project=project, schema=schema)
        if table.name.startswith(prefixes)
    ]
    raise AssertionError(f"Timed out cleaning MaxCompute test objects: {remaining}")


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _maxqa_smoke_requested(),
    reason="MaxCompute MaxQA smoke test is not explicitly enabled",
)
def test_maxcompute_maxqa_query() -> None:
    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    quota_name = os.getenv("MAXCOMPUTE_QUOTA_NAME")
    if project != "york_fic" or schema != "sqlmesh":
        pytest.fail("The MaxQA smoke test is restricted to york_fic.sqlmesh")
    if not quota_name:
        pytest.fail("MAXCOMPUTE_QUOTA_NAME is required for the MaxQA smoke test")

    connection = MaxComputeConnectionConfig(
        project=project,
        schema=schema,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        quota_name=quota_name,
        execution_mode="maxqa",
        maxqa_fallback_policy="none",
        sql_hints={
            "odps.namespace.schema": "true",
            "odps.sql.allow.namespace.schema": "true",
        },
    )
    adapter = connection.create_engine_adapter()
    try:
        assert adapter.fetchone("SELECT 1") == [1]
    finally:
        adapter.close()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RuntimeError("Project is not 3-tier model project"), True),
        (RuntimeError("PROJECT IS NOT 3-TIER MODEL PROJECT"), True),
        (RuntimeError("AccessDenied: schema listing is forbidden"), False),
        (TimeoutError("schema listing timed out"), False),
    ],
)
def test_is_two_tier_project_error(error: Exception, expected: bool) -> None:
    assert _is_two_tier_project_error(error) is expected


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _audit_smoke_requested(),
    reason="MaxCompute audit smoke test is not explicitly enabled",
)
def test_maxcompute_real_audit_execution(tmp_path) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    if project != "york_fic":
        pytest.fail("The audit smoke test is restricted to MAXCOMPUTE_PROJECT=york_fic")
    if schema != "sqlmesh":
        pytest.fail("The audit smoke test is restricted to MAXCOMPUTE_SCHEMA=sqlmesh")

    test_id = uuid.uuid4().hex
    model_prefix = f"sqlmesh_audit_{test_id}"
    source_table = f"{model_prefix}_source"
    audited_model = f"{model_prefix}_view"
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        schema=schema,
    )

    if not bootstrap_odps.exist_schema(schema, project=project):
        pytest.fail(f"The pre-created MaxCompute schema {project}.{schema} does not exist")
    initial_objects = {
        table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
    }

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "audited_view.sql").write_text(
        f"""
        MODEL (
          name {schema}.{audited_model},
          kind VIEW,
          audits (not_null(columns := [payload])),
          dialect maxcompute
        );

        SELECT id, payload
        FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
        physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
        gateways={
            "maxcompute": GatewayConfig(
                connection=MaxComputeConnectionConfig(
                    project=project,
                    schema=schema,
                    endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                    access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                    access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                    quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                    sql_hints=namespace_hints,
                ),
                state_connection=DuckDBConnectionConfig(
                    database=str(tmp_path / "state.duckdb"), concurrent_tasks=1
                ),
            )
        },
        default_gateway="maxcompute",
    )

    context = None
    try:
        context = Context(paths=tmp_path, config=config)
        adapter = context.engine_adapter
        assert adapter._is_schema_namespace_enabled()

        source_name = exp.table_(source_table, db=schema)
        source_columns = {
            "id": exp.DataType.build("BIGINT"),
            "payload": exp.DataType.build("STRING"),
        }
        adapter.create_table(
            source_name,
            source_columns,
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.replace_query(
            source_name,
            parse_one("SELECT CAST(1 AS BIGINT) AS id, 'valid' AS payload", dialect="maxcompute"),
            target_columns_to_types=source_columns,
        )

        plan = context.plan(no_prompts=True, auto_apply=False)
        assert plan.context_diff.has_changes
        context.apply(plan)

        model_name = f"{schema}.{audited_model}"
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{audited_model}` ORDER BY id"
        ) == [[1, "valid"]]
        assert context.audit(
            start="2026-07-14",
            end="2026-07-14",
            models=iter([model_name]),
            execution_time="2026-07-14 01:00:00 UTC",
        )

        adapter.replace_query(
            source_name,
            parse_one(
                "SELECT CAST(1 AS BIGINT) AS id, CAST(NULL AS STRING) AS payload",
                dialect="maxcompute",
            ),
            target_columns_to_types=source_columns,
        )
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{audited_model}` ORDER BY id"
        ) == [[1, None]]
        assert not context.audit(
            start="2026-07-14",
            end="2026-07-14",
            models=iter([model_name]),
            execution_time="2026-07-14 01:00:00 UTC",
        )
    finally:
        if re.fullmatch(r"sqlmesh_audit_[0-9a-f]{32}", model_prefix) is None:
            raise AssertionError(f"Unsafe audit test prefix: {model_prefix}")

        _cleanup_prefixed_objects(
            bootstrap_odps,
            project,
            schema,
            (model_prefix, f"{schema}__{model_prefix}"),
        )

        assert bootstrap_odps.exist_schema(schema, project=project)
        assert {
            table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
        } == initial_objects


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _capability_smoke_requested(),
    reason="MaxCompute capability smoke test is not explicitly enabled",
)
def test_maxcompute_schema_adapter_capabilities(tmp_path) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    if project != "york_fic" or schema != "sqlmesh":
        pytest.fail("The capability smoke test is restricted to york_fic.sqlmesh")

    test_id = uuid.uuid4().hex
    prefix = f"sqlmesh_capability_{test_id}"
    base_table = f"{prefix}_base"
    copy_table = f"{prefix}_copy"
    renamed_table = f"{prefix}_renamed"
    evolution_table = f"{prefix}_evolution"
    transactional_table = f"{prefix}_transactional"
    automatic_partition_table = f"{prefix}_auto_partition"
    materialized_view = f"{prefix}_mv"
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        schema=schema,
    )
    if not bootstrap_odps.exist_schema(schema, project=project):
        pytest.fail(f"The pre-created MaxCompute schema {project}.{schema} does not exist")
    initial_objects = {
        table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
    }
    initial_schemas = {candidate.name for candidate in bootstrap_odps.list_schemas(project=project)}

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
        physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
        gateways={
            "maxcompute": GatewayConfig(
                connection=MaxComputeConnectionConfig(
                    project=project,
                    schema=schema,
                    endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                    access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                    access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                    quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                    sql_hints=namespace_hints,
                    register_comments=True,
                ),
                state_connection=DuckDBConnectionConfig(
                    database=str(tmp_path / "state.duckdb"), concurrent_tasks=1
                ),
            )
        },
        default_gateway="maxcompute",
    )

    context = Context(paths=tmp_path, config=config)
    adapter = context.engine_adapter
    base_columns = {
        "id": exp.DataType.build("BIGINT"),
        "payload": exp.DataType.build("STRING"),
        "ds": exp.DataType.build("STRING"),
    }

    try:
        adapter.create_table(
            exp.table_(base_table, db=schema),
            base_columns,
            table_description="SQLMesh capability base table",
            column_descriptions={"id": "Primary identifier", "payload": "Payload"},
            partitioned_by=[exp.column("ds")],
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.replace_query(
            exp.table_(base_table, db=schema),
            parse_one(
                """
                SELECT CAST(1 AS BIGINT) AS id, 'one' AS payload, '2026-07-14' AS ds
                UNION ALL
                SELECT CAST(2 AS BIGINT) AS id, 'two' AS payload, '2026-07-14' AS ds
                """,
                dialect="maxcompute",
            ),
            target_columns_to_types=base_columns,
            partitioned_by=[exp.column("ds")],
        )
        base_metadata = bootstrap_odps.get_table(base_table, project=project, schema=schema)
        assert base_metadata.comment == "SQLMesh capability base table"
        assert base_metadata.table_schema["id"].comment == "Primary identifier"
        assert adapter.fetchdf(
            f"SELECT id, payload, ds FROM `{schema}`.`{base_table}` ORDER BY id"
        ).to_dict("records") == [
            {"id": 1, "payload": "one", "ds": "2026-07-14"},
            {"id": 2, "payload": "two", "ds": "2026-07-14"},
        ]

        adapter.ctas(
            exp.table_(copy_table, db=schema),
            parse_one(
                f"SELECT id, payload, ds FROM `{schema}`.`{base_table}`",
                dialect="maxcompute",
            ),
            target_columns_to_types=base_columns,
        )
        table_diff = context.table_diff(
            source=f"{schema}.{base_table}",
            target=f"{schema}.{copy_table}",
            on=["id"],
            show=False,
            temp_schema=schema,
        )[0]
        assert not table_diff.schema_diff().added
        assert table_diff.row_diff(temp_schema=schema).full_match_count == 2

        adapter.rename_table(
            exp.table_(copy_table, db=schema), exp.table_(renamed_table, db=schema)
        )
        assert bootstrap_odps.exist_table(renamed_table, project=project, schema=schema)
        adapter._truncate_table(exp.table_(renamed_table, db=schema))
        assert adapter.fetchone(f"SELECT COUNT(*) FROM `{schema}`.`{renamed_table}`") == [0]

        if _schema_evolution_smoke_requested():
            adapter.create_table(
                exp.table_(evolution_table, db=schema),
                {"id": exp.DataType.build("INT")},
                table_properties={"lifecycle": exp.Literal.number(1)},
            )
            adapter.alter_table(
                [
                    exp.Alter(
                        this=exp.table_(evolution_table, db=schema),
                        kind="TABLE",
                        actions=[
                            exp.ColumnDef(
                                this=exp.to_identifier("amount"),
                                kind=exp.DataType.build("DECIMAL(18, 2)"),
                            )
                        ],
                    ),
                    exp.Alter(
                        this=exp.table_(evolution_table, db=schema),
                        kind="TABLE",
                        actions=[
                            exp.AlterColumn(
                                this=exp.to_identifier("id"),
                                dtype=exp.DataType.build("BIGINT"),
                            )
                        ],
                    ),
                ]
            )
            assert adapter.columns(exp.table_(evolution_table, db=schema)) == {
                "id": exp.DataType.build("BIGINT"),
                "amount": exp.DataType.build("DECIMAL(18, 2)"),
            }

        transactional_columns = {
            "id": exp.DataType.build("BIGINT"),
            "payload": exp.DataType.build("STRING"),
        }
        adapter.create_table(
            exp.table_(transactional_table, db=schema),
            transactional_columns,
            table_properties={
                "transactional": exp.true(),
                "primary_key": exp.column("id"),
                "write_bucket_num": exp.Literal.number(16),
                "lifecycle": exp.Literal.number(1),
            },
        )
        adapter.insert_append(
            exp.table_(transactional_table, db=schema),
            parse_one("SELECT CAST(1 AS BIGINT) AS id, 'before' AS payload"),
            target_columns_to_types=transactional_columns,
        )
        adapter.merge(
            exp.table_(transactional_table, db=schema),
            parse_one(
                """
                SELECT CAST(1 AS BIGINT) AS id, 'after' AS payload
                UNION ALL SELECT CAST(2 AS BIGINT) AS id, 'new' AS payload
                """
            ),
            transactional_columns,
            unique_key=[exp.column("id")],
        )
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{transactional_table}` ORDER BY id"
        ) == [[1, "after"], [2, "new"]]
        assert (
            adapter.get_table_last_modified_ts([exp.table_(transactional_table, db=schema)])[0] > 0
        )

        automatic_columns = {
            "event_id": exp.DataType.build("BIGINT"),
            "event_ts": exp.DataType.build("TIMESTAMP"),
        }
        adapter.create_table(
            exp.table_(automatic_partition_table, db=schema),
            automatic_columns,
            partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.insert_append(
            exp.table_(automatic_partition_table, db=schema),
            parse_one(
                "SELECT CAST(1 AS BIGINT) AS event_id, "
                "CAST('2026-07-13 01:00:00' AS TIMESTAMP) AS event_ts "
                "UNION ALL SELECT CAST(10 AS BIGINT) AS event_id, "
                "CAST('2026-07-14 01:00:00' AS TIMESTAMP) AS event_ts"
            ),
            target_columns_to_types=automatic_columns,
        )
        auto_table = bootstrap_odps.get_table(
            automatic_partition_table, project=project, schema=schema
        )
        assert auto_table.table_schema.partitions[0].generate_expression
        assert adapter.fetchone(
            f"SELECT COUNT(*) FROM `{schema}`.`{automatic_partition_table}`"
        ) == [2]
        adapter._insert_overwrite_by_time_partition(
            exp.table_(automatic_partition_table, db=schema),
            [
                SourceQuery(
                    query_factory=lambda: parse_one(
                        "SELECT CAST(2 AS BIGINT) AS event_id, "
                        "CAST('2026-07-14 02:00:00' AS TIMESTAMP) AS event_ts"
                    )
                )
            ],
            target_columns_to_types=automatic_columns,
            where=parse_one(
                "event_ts >= CAST('2026-07-14 00:00:00' AS TIMESTAMP) "
                "AND event_ts < CAST('2026-07-15 00:00:00' AS TIMESTAMP)"
            ),
            partitioned_by=[parse_one("TRUNC_TIME(event_ts, 'day') AS ds")],
        )
        assert adapter.fetchall(
            f"SELECT event_id FROM `{schema}`.`{automatic_partition_table}` ORDER BY event_id"
        ) == [[1], [2]]

        adapter.create_view(
            exp.table_(materialized_view, db=schema),
            parse_one(
                f"SELECT id, payload, ds FROM `{schema}`.`{base_table}`",
                dialect="maxcompute",
            ),
            target_columns_to_types=base_columns,
            replace=False,
            materialized=True,
            table_description="SQLMesh capability materialized view",
            materialized_properties={
                "partitioned_by": [exp.column("ds")],
                "clustered_by": [exp.column("id")],
            },
            view_properties={
                "lifecycle": exp.Literal.number(1),
                "cluster_bucket_num": exp.Literal.number(8),
            },
        )
        mv_metadata = bootstrap_odps.get_table(materialized_view, project=project, schema=schema)
        assert mv_metadata.is_materialized_view
        assert adapter.fetchone(f"SELECT COUNT(*) FROM `{schema}`.`{materialized_view}`") == [2]
    finally:
        if re.fullmatch(r"sqlmesh_capability_[0-9a-f]{32}", prefix) is None:
            raise AssertionError(f"Unsafe capability test prefix: {prefix}")
        _cleanup_prefixed_objects(bootstrap_odps, project, schema, (prefix,))

        assert bootstrap_odps.exist_schema(schema, project=project)
        assert {
            table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
        } == initial_objects
        assert {
            candidate.name for candidate in bootstrap_odps.list_schemas(project=project)
        } == initial_schemas


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _transactional_smoke_requested(),
    reason="MaxCompute transactional model smoke test is not explicitly enabled",
)
def test_maxcompute_transactional_models_plan_apply(tmp_path) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    if project != "york_fic" or schema != "sqlmesh":
        pytest.fail("The transactional smoke test is restricted to york_fic.sqlmesh")

    test_id = uuid.uuid4().hex
    prefix = f"sqlmesh_transactional_{test_id}"
    source_table = f"{prefix}_source"
    unique_model = f"{prefix}_unique"
    scd_time_model = f"{prefix}_scd_time"
    scd_column_model = f"{prefix}_scd_column"
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        schema=schema,
    )
    initial_objects = {
        table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
    }

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "unique.sql").write_text(
        f"""
        MODEL (
          name {schema}.{unique_model},
          kind INCREMENTAL_BY_UNIQUE_KEY (unique_key id),
          columns (id BIGINT, payload STRING, updated_at TIMESTAMP),
          start '2026-07-13',
          cron '@daily',
          dialect maxcompute,
          physical_properties (
            transactional = true,
            primary_key = id,
            write_bucket_num = 16,
            lifecycle = 1
          )
        );

        SELECT id, payload, updated_at FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )
    (models_dir / "scd_time.sql").write_text(
        f"""
        MODEL (
          name {schema}.{scd_time_model},
          kind SCD_TYPE_2_BY_TIME (
            unique_key id,
            updated_at_name updated_at,
            invalidate_hard_deletes true
          ),
          columns (id BIGINT, payload STRING, updated_at TIMESTAMP),
          start '2026-07-13',
          cron '@daily',
          dialect maxcompute,
          physical_properties (transactional = true, lifecycle = 1)
        );

        SELECT id, payload, updated_at FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )
    (models_dir / "scd_column.sql").write_text(
        f"""
        MODEL (
          name {schema}.{scd_column_model},
          kind SCD_TYPE_2_BY_COLUMN (
            unique_key id,
            columns [payload],
            invalidate_hard_deletes true
          ),
          columns (id BIGINT, payload STRING, updated_at TIMESTAMP),
          start '2026-07-13',
          cron '@daily',
          dialect maxcompute,
          physical_properties (transactional = true, lifecycle = 1)
        );

        SELECT id, payload, updated_at FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
        physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
        gateways={
            "maxcompute": GatewayConfig(
                connection=MaxComputeConnectionConfig(
                    project=project,
                    schema=schema,
                    endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                    access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                    access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                    quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                    sql_hints=namespace_hints,
                ),
                state_connection=DuckDBConnectionConfig(
                    database=str(tmp_path / "state.duckdb"), concurrent_tasks=1
                ),
            )
        },
        default_gateway="maxcompute",
    )
    context = Context(paths=tmp_path, config=config)
    adapter = context.engine_adapter
    source_columns = {
        "id": exp.DataType.build("BIGINT"),
        "payload": exp.DataType.build("STRING"),
        "updated_at": exp.DataType.build("TIMESTAMP"),
    }

    def replace_source(rows_sql: str) -> None:
        adapter.replace_query(
            exp.table_(source_table, db=schema),
            parse_one(rows_sql, dialect="maxcompute"),
            target_columns_to_types=source_columns,
        )

    try:
        adapter.create_table(
            exp.table_(source_table, db=schema),
            source_columns,
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        replace_source(
            "SELECT CAST(1 AS BIGINT) AS id, 'one' AS payload, "
            "CAST('2026-07-13 01:00:00' AS TIMESTAMP) AS updated_at"
        )

        plan = context.plan(
            execution_time="2026-07-14 01:00:00 UTC",
            no_prompts=True,
            auto_apply=False,
        )
        context.apply(plan)
        context.apply(
            context.plan(
                execution_time="2026-07-14 01:00:00 UTC",
                no_prompts=True,
                auto_apply=False,
            )
        )

        replace_source(
            """
            SELECT CAST(1 AS BIGINT) AS id, 'one-updated' AS payload,
                   CAST('2026-07-14 01:00:00' AS TIMESTAMP) AS updated_at
            UNION ALL
            SELECT CAST(2 AS BIGINT) AS id, 'two' AS payload,
                   CAST('2026-07-14 01:00:00' AS TIMESTAMP) AS updated_at
            """
        )
        assert context.run(end="2026-07-14", execution_time="2026-07-15 01:00:00 UTC").is_success
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{unique_model}` ORDER BY id"
        ) == [[1, "one-updated"], [2, "two"]]
        for model_name in (scd_time_model, scd_column_model):
            assert adapter.fetchall(
                f"SELECT id, payload FROM `{schema}`.`{model_name}` "
                "WHERE valid_to IS NULL ORDER BY id"
            ) == [[1, "one-updated"], [2, "two"]]

        replace_source(
            "SELECT CAST(2 AS BIGINT) AS id, 'two' AS payload, "
            "CAST('2026-07-15 01:00:00' AS TIMESTAMP) AS updated_at"
        )
        assert context.run(end="2026-07-15", execution_time="2026-07-16 01:00:00 UTC").is_success
        for model_name in (scd_time_model, scd_column_model):
            assert adapter.fetchall(
                f"SELECT id FROM `{schema}`.`{model_name}` WHERE valid_to IS NULL ORDER BY id"
            ) == [[2]]

        for model_name in (unique_model, scd_time_model, scd_column_model):
            snapshot = context.get_snapshot(f"{schema}.{model_name}", raise_if_missing=True)
            physical_table = exp.to_table(snapshot.table_name()).name
            metadata = bootstrap_odps.get_table(physical_table, project=project, schema=schema)
            assert metadata.is_transactional
            assert metadata.lifecycle == 1
    finally:
        if re.fullmatch(r"sqlmesh_transactional_[0-9a-f]{32}", prefix) is None:
            raise AssertionError(f"Unsafe transactional test prefix: {prefix}")
        _cleanup_prefixed_objects(bootstrap_odps, project, schema, (prefix, f"{schema}__{prefix}"))
        assert {
            table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
        } == initial_objects


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _model_kind_smoke_requested(),
    reason="MaxCompute model kind smoke test is not explicitly enabled",
)
def test_maxcompute_additional_model_kinds_plan_apply(tmp_path) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    if project != "york_fic" or schema != "sqlmesh":
        pytest.fail("The model kind smoke test is restricted to york_fic.sqlmesh")

    test_id = uuid.uuid4().hex
    prefix = f"sqlmesh_model_kind_{test_id}"
    source_table = f"{prefix}_source"
    external_table = f"{prefix}_external"
    partition_model = f"{prefix}_partition"
    unmanaged_model = f"{prefix}_unmanaged"
    embedded_model = f"{prefix}_embedded"
    embedded_consumer = f"{prefix}_embedded_consumer"
    materialized_view_model = f"{prefix}_mv"
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        schema=schema,
    )
    initial_objects = {
        table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
    }

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "partition.sql").write_text(
        f"""
        MODEL (
          name {schema}.{partition_model},
          kind INCREMENTAL_BY_PARTITION,
          columns (id BIGINT, payload STRING, ds STRING),
          partitioned_by [ds],
          start '2026-07-13',
          cron '@daily',
          dialect maxcompute,
          physical_properties (lifecycle = 1)
        );

        SELECT id, payload, ds FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )
    (models_dir / "unmanaged.sql").write_text(
        f"""
        MODEL (
          name {schema}.{unmanaged_model},
          kind INCREMENTAL_UNMANAGED (insert_overwrite true),
          columns (id BIGINT, payload STRING),
          start '2026-07-13',
          cron '@daily',
          dialect maxcompute,
          physical_properties (lifecycle = 1)
        );

        SELECT id, payload FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )
    (models_dir / "external.sql").write_text(
        f"""
        MODEL (
          name {schema}.{external_table},
          kind EXTERNAL,
          columns (id BIGINT, payload STRING),
          dialect maxcompute
        );

        SELECT 1;
        """,
        encoding="utf-8",
    )
    (models_dir / "embedded.sql").write_text(
        f"""
        MODEL (
          name {schema}.{embedded_model},
          kind EMBEDDED,
          columns (id BIGINT, payload STRING),
          dialect maxcompute
        );

        SELECT id, payload FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )
    (models_dir / "embedded_consumer.sql").write_text(
        f"""
        MODEL (
          name {schema}.{embedded_consumer},
          kind FULL,
          columns (id BIGINT, payload STRING),
          dialect maxcompute,
          physical_properties (lifecycle = 1)
        );

        SELECT id, payload FROM {schema}.{embedded_model};
        """,
        encoding="utf-8",
    )
    (models_dir / "materialized_view.sql").write_text(
        f"""
        MODEL (
          name {schema}.{materialized_view_model},
          kind VIEW (materialized true),
          columns (id BIGINT, payload STRING, ds STRING),
          partitioned_by [ds],
          clustered_by [id],
          dialect maxcompute,
          physical_properties (lifecycle = 1, cluster_bucket_num = 8)
        );

        SELECT id, payload, ds FROM {schema}.{source_table};
        """,
        encoding="utf-8",
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
        physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
        gateways={
            "maxcompute": GatewayConfig(
                connection=MaxComputeConnectionConfig(
                    project=project,
                    schema=schema,
                    endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                    access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                    access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                    sql_hints=namespace_hints,
                ),
                state_connection=DuckDBConnectionConfig(
                    database=str(tmp_path / "state.duckdb"), concurrent_tasks=1
                ),
            )
        },
        default_gateway="maxcompute",
    )
    context = Context(paths=tmp_path, config=config)
    adapter = context.engine_adapter
    source_columns = {
        "id": exp.DataType.build("BIGINT"),
        "payload": exp.DataType.build("STRING"),
        "ds": exp.DataType.build("STRING"),
    }

    try:
        adapter.create_table(
            exp.table_(source_table, db=schema),
            source_columns,
            partitioned_by=[exp.column("ds")],
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.insert_append(
            exp.table_(source_table, db=schema),
            parse_one("SELECT CAST(1 AS BIGINT) AS id, 'one' AS payload, '2026-07-13' AS ds"),
            target_columns_to_types=source_columns,
        )
        adapter.create_table(
            exp.table_(external_table, db=schema),
            {"id": exp.DataType.build("BIGINT"), "payload": exp.DataType.build("STRING")},
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.insert_append(
            exp.table_(external_table, db=schema),
            parse_one("SELECT CAST(9 AS BIGINT) AS id, 'external' AS payload"),
            target_columns_to_types={
                "id": exp.DataType.build("BIGINT"),
                "payload": exp.DataType.build("STRING"),
            },
        )

        context.apply(
            context.plan(
                execution_time="2026-07-14 01:00:00 UTC",
                no_prompts=True,
                auto_apply=False,
            )
        )
        context.apply(
            context.plan(
                execution_time="2026-07-14 01:00:00 UTC",
                no_prompts=True,
                auto_apply=False,
            )
        )

        assert adapter.fetchall(
            f"SELECT id, payload, ds FROM `{schema}`.`{partition_model}` ORDER BY id"
        ) == [[1, "one", "2026-07-13"]]
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{unmanaged_model}` ORDER BY id"
        ) == [[1, "one"]]
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{embedded_consumer}` ORDER BY id"
        ) == [[1, "one"]]
        assert adapter.fetchall(
            f"SELECT id, payload FROM `{schema}`.`{external_table}` ORDER BY id"
        ) == [[9, "external"]]
        assert not bootstrap_odps.exist_table(embedded_model, project=project, schema=schema)
        logical_mv_metadata = bootstrap_odps.get_table(
            materialized_view_model, project=project, schema=schema
        )
        assert logical_mv_metadata.is_virtual_view
        mv_snapshot = context.get_snapshot(
            f"{schema}.{materialized_view_model}", raise_if_missing=True
        )
        physical_mv_name = exp.to_table(mv_snapshot.table_name()).name
        assert bootstrap_odps.get_table(
            physical_mv_name, project=project, schema=schema
        ).is_materialized_view
        assert adapter.fetchall(
            f"SELECT id, payload, ds FROM `{schema}`.`{materialized_view_model}` ORDER BY id"
        ) == [[1, "one", "2026-07-13"]]
    finally:
        if re.fullmatch(r"sqlmesh_model_kind_[0-9a-f]{32}", prefix) is None:
            raise AssertionError(f"Unsafe model kind test prefix: {prefix}")
        _cleanup_prefixed_objects(bootstrap_odps, project, schema, (prefix, f"{schema}__{prefix}"))
        assert {
            table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
        } == initial_objects


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _no_schema_smoke_requested(),
    reason="MaxCompute no-schema smoke test is not explicitly enabled",
)
def test_maxcompute_no_schema_smoke_plan_apply(tmp_path) -> None:
    project = os.environ["MAXCOMPUTE_PROJECT"]
    if project != "york_data" or os.getenv("MAXCOMPUTE_SCHEMA"):
        pytest.fail(
            "The no-schema smoke test is restricted to york_data with MAXCOMPUTE_SCHEMA unset"
        )
    model_prefix = f"sqlmesh_smoke_{uuid.uuid4().hex[:8]}"
    dim_model = f"{model_prefix}_dim_customer"
    fact_model = f"{model_prefix}_fact_order_daily"

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "dim_customer.sql").write_text(
        f"""
        MODEL (
          name analytics.{dim_model},
          kind FULL,
          dialect maxcompute,
          physical_properties (lifecycle = 1)
        );

        SELECT 1 AS customer_id, 'alice' AS customer_name;
        """,
        encoding="utf-8",
    )
    (models_dir / "fact_order_daily.sql").write_text(
        f"""
        MODEL (
          name analytics.{fact_model},
          kind INCREMENTAL_BY_TIME_RANGE (
            time_column ds
          ),
          partitioned_by [ds],
          dialect maxcompute,
          start '2026-06-26',
          cron '@daily',
          physical_properties (lifecycle = 1)
        );

        SELECT 1 AS order_id, CAST('2026-06-26' AS STRING) AS ds;
        """,
        encoding="utf-8",
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
        physical_schema_mapping={re.compile("^analytics$"): project},
        gateways={
            "maxcompute": GatewayConfig(
                connection=MaxComputeConnectionConfig(
                    project=project,
                    schema=None,
                    endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                    access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                    access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                    quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                    sql_hints={"odps.sql.allow.fullscan": "true"},
                ),
                state_connection=DuckDBConnectionConfig(database=str(tmp_path / "state.duckdb")),
            )
        },
        default_gateway="maxcompute",
    )

    context = Context(paths=tmp_path, config=config)
    adapter = context.engine_adapter
    if adapter.odps.is_schema_namespace_enabled():
        pytest.skip("This smoke test validates MaxCompute projects without schema namespace")
    try:
        # Two-tier projects reject schema listing; a successful read proves three-tier capability.
        list(adapter.odps.list_schemas(project=project))
    except Exception as ex:
        if not _is_two_tier_project_error(ex):
            pytest.skip(
                "Cannot safely verify that the MaxCompute project uses the two-tier model "
                f"because schema listing raised {type(ex).__name__}"
            )
    else:
        pytest.skip("This smoke test cannot run against a schema-capable MaxCompute project")

    try:
        plan = context.plan(no_prompts=True, auto_apply=False)
        assert plan.context_diff.has_changes
        context.apply(plan)
        context.apply(context.plan(no_prompts=True, auto_apply=False))

        assert adapter.fetchall(
            f"SELECT customer_id, customer_name FROM analytics__{dim_model} ORDER BY customer_id"
        ) == [[1, "alice"]]
        assert adapter.fetchall(
            f"SELECT order_id, ds FROM analytics__{fact_model} ORDER BY order_id"
        ) == [[1, "2026-06-26"]]
    finally:
        prefixes = (
            f"analytics__{model_prefix}",
            f"sqlmesh__analytics__analytics__{model_prefix}",
        )
        for prefix in prefixes:
            for table in adapter.odps.list_tables(project=project, prefix=prefix):
                name = table.name
                if getattr(table, "is_virtual_view", False):
                    adapter.drop_view(name, ignore_if_not_exists=True)
                else:
                    adapter.drop_table(name, exists=True)


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _schema_smoke_requested(),
    reason="MaxCompute schema smoke test is not explicitly enabled",
)
def test_maxcompute_schema_smoke_plan_apply(tmp_path) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = f"sqlmesh_smoke_{uuid.uuid4().hex[:12]}"
    start_ds = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
    )

    context = None
    schema_created = False
    try:
        # Listing schemas is a read-only capability check and must succeed before any DDL.
        list(bootstrap_odps.list_schemas(project=project))
        bootstrap_odps.create_schema(schema, project=project)
        schema_created = True

        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "dim_customer.sql").write_text(
            f"""
            MODEL (
              name {schema}.dim_customer,
              kind FULL,
              dialect maxcompute,
              physical_properties (lifecycle = 1)
            );

            SELECT 1 AS customer_id, 'alice' AS customer_name;
            """,
            encoding="utf-8",
        )
        (models_dir / "fact_order_daily.sql").write_text(
            f"""
            MODEL (
              name {schema}.fact_order_daily,
              kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds
              ),
              partitioned_by [ds],
              dialect maxcompute,
              start '{start_ds}',
              cron '@daily',
              physical_properties (lifecycle = 1)
            );

            SELECT 1 AS order_id, CAST('{start_ds}' AS STRING) AS ds;
            """,
            encoding="utf-8",
        )

        config = Config(
            model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
            physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
            gateways={
                "maxcompute": GatewayConfig(
                    connection=MaxComputeConnectionConfig(
                        project=project,
                        schema=schema,
                        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                        access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                        access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                        quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                        sql_hints=namespace_hints,
                    ),
                    state_connection=DuckDBConnectionConfig(
                        database=str(tmp_path / "state.duckdb")
                    ),
                )
            },
            default_gateway="maxcompute",
        )

        context = Context(paths=tmp_path, config=config)
        adapter = context.engine_adapter
        assert adapter._is_schema_namespace_enabled()
        assert adapter.odps.schema == schema

        plan = context.plan(no_prompts=True, auto_apply=False)
        assert plan.context_diff.has_changes
        context.apply(plan)
        context.apply(context.plan(no_prompts=True, auto_apply=False))

        assert adapter.fetchall(
            f"SELECT customer_id, customer_name FROM `{schema}`.`dim_customer` ORDER BY customer_id"
        ) == [[1, "alice"]]
        assert adapter.fetchall(
            f"SELECT order_id, ds FROM `{schema}`.`fact_order_daily` ORDER BY order_id"
        ) == [[1, start_ds]]
    finally:
        if re.fullmatch(r"sqlmesh_smoke_[0-9a-f]{12}", schema) is None:
            raise AssertionError(f"Unsafe generated schema identifier: {schema}")
        try:
            if context is not None:
                context.engine_adapter.drop_schema(schema, ignore_if_not_exists=True, cascade=True)
        finally:
            if schema_created and bootstrap_odps.exist_schema(schema, project=project):
                bootstrap_odps.execute_sql(
                    f"DROP SCHEMA IF EXISTS `{schema}` CASCADE",
                    project=project,
                    hints=namespace_hints,
                )
            if schema_created:
                assert not bootstrap_odps.exist_schema(schema, project=project)


@pytest.mark.skipif(
    not _has_maxcompute_env() or not _lifecycle_smoke_requested(),
    reason="MaxCompute lifecycle smoke test is not explicitly enabled",
)
def test_maxcompute_schema_lifecycle_restate_janitor(tmp_path, monkeypatch) -> None:
    from odps import ODPS

    project = os.environ["MAXCOMPUTE_PROJECT"]
    schema = os.getenv("MAXCOMPUTE_SCHEMA", "")
    if project != "york_fic":
        pytest.fail("The lifecycle smoke test is restricted to MAXCOMPUTE_PROJECT=york_fic")
    if schema != "sqlmesh":
        pytest.fail("The lifecycle smoke test is restricted to MAXCOMPUTE_SCHEMA=sqlmesh")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        pytest.fail(f"Unsafe MAXCOMPUTE_SCHEMA identifier: {schema!r}")

    test_id = uuid.uuid4().hex
    model_prefix = f"sqlmesh_lifecycle_{test_id}"
    source_table = f"{model_prefix}_source"
    full_model = f"{model_prefix}_full"
    incremental_model = f"{model_prefix}_daily"
    dev_only_model = f"{model_prefix}_dev_only"
    dev_environment = f"lc_{test_id[:12]}"
    dev_schema = f"{schema}__{dev_environment}"
    namespace_hints = {
        "odps.namespace.schema": "true",
        "odps.sql.allow.namespace.schema": "true",
        "odps.sql.allow.fullscan": "true",
    }
    bootstrap_odps = ODPS(
        os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
        os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
        project=project,
        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
        schema=schema,
    )

    if not bootstrap_odps.exist_schema(schema, project=project):
        pytest.fail(f"The pre-created MaxCompute schema {project}.{schema} does not exist")
    initial_base_objects = {
        table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
    }

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    full_model_path = models_dir / "full.sql"
    incremental_model_path = models_dir / "daily.sql"
    dev_only_model_path = models_dir / "dev_only.sql"

    def write_full_model(revision: int) -> None:
        full_model_path.write_text(
            f"""
            MODEL (
              name {schema}.{full_model},
              kind FULL,
              dialect maxcompute,
              physical_properties (lifecycle = 1)
            );

            SELECT {revision} AS revision;
            """,
            encoding="utf-8",
        )

    def source_query(ds_12_payload: str, ds_13_payload: str) -> exp.Query:
        return parse_one(
            f"""
            SELECT CAST(11 AS BIGINT) AS id, 'initial-11' AS payload, '2026-07-11' AS ds
            UNION ALL
            SELECT CAST(12 AS BIGINT) AS id, '{ds_12_payload}' AS payload, '2026-07-12' AS ds
            UNION ALL
            SELECT CAST(13 AS BIGINT) AS id, '{ds_13_payload}' AS payload, '2026-07-13' AS ds
            """,
            dialect="maxcompute",
        )

    def lifecycle_object_name(name: str) -> bool:
        return name.startswith(model_prefix) or name.startswith(f"{schema}__{model_prefix}")

    def dev_schema_exists() -> bool:
        return any(
            candidate.name == dev_schema
            for candidate in bootstrap_odps.list_schemas(project=project, prefix=dev_schema)
        )

    context = None
    try:
        write_full_model(1)
        incremental_model_path.write_text(
            f"""
            MODEL (
              name {schema}.{incremental_model},
              kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                auto_restatement_cron '0 6 * * *',
                auto_restatement_intervals 1
              ),
              partitioned_by [ds],
              columns (
                id BIGINT,
                payload STRING,
                ds STRING
              ),
              dialect maxcompute,
              start '2026-07-11',
              cron '@daily',
              physical_properties (lifecycle = 1)
            );

            SELECT id, payload, ds
            FROM {schema}.{source_table}
            WHERE ds BETWEEN @start_ds AND @end_ds;
            """,
            encoding="utf-8",
        )

        config = Config(
            model_defaults=ModelDefaultsConfig(dialect="maxcompute"),
            physical_schema_mapping={re.compile(f"^{re.escape(schema)}$"): schema},
            gateways={
                "maxcompute": GatewayConfig(
                    connection=MaxComputeConnectionConfig(
                        project=project,
                        schema=schema,
                        endpoint=os.environ["MAXCOMPUTE_ENDPOINT"],
                        access_key_id=os.environ["MAXCOMPUTE_ACCESS_KEY_ID"],
                        access_key_secret=os.environ["MAXCOMPUTE_ACCESS_KEY_SECRET"],
                        quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME"),
                        sql_hints=namespace_hints,
                    ),
                    state_connection=DuckDBConnectionConfig(
                        database=str(tmp_path / "state.duckdb"), concurrent_tasks=1
                    ),
                )
            },
            default_gateway="maxcompute",
        )

        context = Context(paths=tmp_path, config=config)
        adapter = context.engine_adapter
        assert adapter._is_schema_namespace_enabled()

        original_drop_schema = adapter.drop_schema

        def guarded_drop_schema(schema_name, *args, **kwargs):
            target = exp.to_table(schema_name)
            target_schema = target.db or target.name
            if target_schema != dev_schema:
                raise AssertionError(f"Refusing to drop protected schema: {target_schema}")
            return original_drop_schema(schema_name, *args, **kwargs)

        monkeypatch.setattr(adapter, "drop_schema", guarded_drop_schema)

        source_name = exp.table_(source_table, db=schema)
        source_columns = {
            "id": exp.DataType.build("BIGINT"),
            "payload": exp.DataType.build("STRING"),
            "ds": exp.DataType.build("STRING"),
        }
        adapter.create_table(
            source_name,
            source_columns,
            table_properties={"lifecycle": exp.Literal.number(1)},
        )
        adapter.replace_query(
            source_name,
            source_query("initial-12", "initial-13"),
            target_columns_to_types=source_columns,
        )
        assert bootstrap_odps.get_table(source_table, project=project, schema=schema).lifecycle == 1

        with patch(
            "sqlmesh.core.snapshot.definition.now_timestamp",
            return_value=to_timestamp("2026-07-14 01:00:00 UTC"),
        ):
            initial_plan = context.plan(
                execution_time="2026-07-14 01:00:00 UTC",
                no_prompts=True,
                auto_apply=False,
            )
        assert initial_plan.context_diff.has_changes
        context.apply(initial_plan)

        repeat_plan = context.plan(
            execution_time="2026-07-14 01:00:00 UTC",
            no_prompts=True,
            auto_apply=False,
        )
        assert not repeat_plan.context_diff.has_changes
        assert not repeat_plan.requires_backfill
        context.apply(repeat_plan)

        expected_initial_rows = [
            [11, "initial-11", "2026-07-11"],
            [12, "initial-12", "2026-07-12"],
            [13, "initial-13", "2026-07-13"],
        ]
        assert (
            adapter.fetchall(
                f"SELECT id, payload, ds FROM `{schema}`.`{incremental_model}` ORDER BY ds"
            )
            == expected_initial_rows
        )

        full_snapshot = context.get_snapshot(f"{schema}.{full_model}", raise_if_missing=True)
        incremental_snapshot = context.get_snapshot(
            f"{schema}.{incremental_model}", raise_if_missing=True
        )
        for snapshot in (full_snapshot, incremental_snapshot):
            physical_table = exp.to_table(snapshot.table_name())
            assert physical_table.db == schema
            assert (
                bootstrap_odps.get_table(
                    physical_table.name, project=project, schema=schema
                ).lifecycle
                == 1
            )

        stored_incremental = context.state_sync.get_snapshots([incremental_snapshot.snapshot_id])[
            incremental_snapshot.snapshot_id
        ]
        initial_intervals = list(stored_incremental.intervals)
        assert initial_intervals
        assert context.state_sync.get_environment("prod") is not None

        adapter.replace_query(
            source_name,
            source_query("manual-12", "initial-13"),
            target_columns_to_types=source_columns,
        )
        restatement_plan = context.plan(
            restate_models=[f"{schema}.{incremental_model}"],
            start="2026-07-12",
            end="2026-07-12",
            execution_time="2026-07-14 01:00:00 UTC",
            no_prompts=True,
            auto_apply=False,
        )
        assert incremental_snapshot.snapshot_id in restatement_plan.restatements
        context.apply(restatement_plan)

        assert adapter.fetchall(
            f"SELECT id, payload, ds FROM `{schema}`.`{incremental_model}` ORDER BY ds"
        ) == [
            [11, "initial-11", "2026-07-11"],
            [12, "manual-12", "2026-07-12"],
            [13, "initial-13", "2026-07-13"],
        ]
        stored_incremental = context.state_sync.get_snapshots([incremental_snapshot.snapshot_id])[
            incremental_snapshot.snapshot_id
        ]
        assert stored_incremental.intervals == initial_intervals
        assert not stored_incremental.pending_restatement_intervals

        adapter.replace_query(
            source_name,
            source_query("manual-12", "automatic-13"),
            target_columns_to_types=source_columns,
        )

        assert context is not None
        with patch.object(context, "_run_janitor", wraps=context._run_janitor) as janitor_spy:
            run_status = context.run(end="2026-07-13", execution_time="2026-07-14 06:01:00 UTC")
            assert run_status.is_success
        janitor_spy.assert_called_once_with()

        adapter = context.engine_adapter
        assert adapter.fetchall(
            f"SELECT id, payload, ds FROM `{schema}`.`{incremental_model}` ORDER BY ds"
        ) == [
            [11, "initial-11", "2026-07-11"],
            [12, "manual-12", "2026-07-12"],
            [13, "automatic-13", "2026-07-13"],
        ]
        stored_incremental = context.state_sync.get_snapshots([incremental_snapshot.snapshot_id])[
            incremental_snapshot.snapshot_id
        ]
        assert stored_incremental.next_auto_restatement_ts == to_timestamp(
            "2026-07-15 06:00:00 UTC"
        )
        assert not stored_incremental.pending_restatement_intervals

        dev_only_model_path.write_text(
            f"""
            MODEL (
              name {schema}.{dev_only_model},
              kind FULL,
              dialect maxcompute,
              physical_properties (lifecycle = 1)
            );

            SELECT 1 AS dev_value;
            """,
            encoding="utf-8",
        )
        context.load()
        dev_plan = context.plan(
            environment=dev_environment,
            execution_time="2026-07-14 06:01:00 UTC",
            no_prompts=True,
            auto_apply=False,
        )
        assert dev_plan.context_diff.has_changes
        context.apply(dev_plan)

        assert dev_schema_exists()
        assert context.state_sync.get_environment(dev_environment) is not None
        dev_snapshot = context.get_snapshot(f"{schema}.{dev_only_model}", raise_if_missing=True)
        dev_physical_candidates = {
            exp.to_table(dev_snapshot.table_name(is_deployable=is_deployable))
            for is_deployable in (True, False)
        }
        existing_dev_physical_tables = [
            table
            for table in dev_physical_candidates
            if bootstrap_odps.exist_table(table.name, project=project, schema=schema)
        ]
        assert len(existing_dev_physical_tables) == 1
        dev_physical_table = existing_dev_physical_tables[0]
        assert dev_physical_table.db == schema
        assert (
            bootstrap_odps.get_table(
                dev_physical_table.name, project=project, schema=schema
            ).lifecycle
            == 1
        )

        context.invalidate_environment(dev_environment)

        assert context is not None
        assert context.run_janitor(ignore_ttl=True, environment=dev_environment)
        assert context.state_sync.get_environment(dev_environment) is None
        for _ in range(15):
            if not dev_schema_exists():
                break
            time.sleep(1)
        assert not dev_schema_exists()
        assert context.engine_adapter.fetchone(
            f"SELECT revision FROM `{schema}`.`{full_model}`"
        ) == [1]

        dev_only_model_path.unlink()
        write_full_model(2)
        context.load()
        old_full_snapshot = full_snapshot
        old_full_table = exp.to_table(old_full_snapshot.table_name()).name
        replacement_plan = context.plan(
            execution_time="2026-07-14 06:01:01 UTC",
            no_prompts=True,
            auto_apply=False,
            categorizer_config=CategorizerConfig.all_full(),
        )
        assert replacement_plan.context_diff.has_changes
        context.apply(replacement_plan)

        current_full_snapshot = context.get_snapshot(
            f"{schema}.{full_model}", raise_if_missing=True
        )
        current_incremental_snapshot = context.get_snapshot(
            f"{schema}.{incremental_model}", raise_if_missing=True
        )
        current_full_table = exp.to_table(current_full_snapshot.table_name()).name
        assert current_full_snapshot.snapshot_id != old_full_snapshot.snapshot_id
        assert current_full_table != old_full_table
        assert bootstrap_odps.exist_table(old_full_table, project=project, schema=schema)
        assert bootstrap_odps.exist_table(current_full_table, project=project, schema=schema)
        assert context.engine_adapter.fetchone(
            f"SELECT revision FROM `{schema}`.`{full_model}`"
        ) == [2]

        raw_state_sync = context.state_sync.state_sync
        expired_snapshot_ids = {old_full_snapshot.snapshot_id, dev_snapshot.snapshot_id}
        assert raw_state_sync.snapshots_exist(expired_snapshot_ids) == expired_snapshot_ids
        assert raw_state_sync.interval_state.get_snapshot_intervals(
            [old_full_snapshot, dev_snapshot]
        )

        assert context.run_janitor(ignore_ttl=True)
        assert not bootstrap_odps.exist_table(old_full_table, project=project, schema=schema)
        assert not bootstrap_odps.exist_table(
            dev_physical_table.name, project=project, schema=schema
        )
        assert not raw_state_sync.snapshots_exist(expired_snapshot_ids)
        assert not raw_state_sync.interval_state.get_snapshot_intervals(
            [old_full_snapshot, dev_snapshot]
        )
        assert raw_state_sync.snapshots_exist(
            {current_full_snapshot.snapshot_id, current_incremental_snapshot.snapshot_id}
        ) == {current_full_snapshot.snapshot_id, current_incremental_snapshot.snapshot_id}

        objects_after_first_janitor = {
            table.name
            for table in bootstrap_odps.list_tables(project=project, schema=schema)
            if lifecycle_object_name(table.name)
        }
        assert context.run_janitor(ignore_ttl=True)
        assert {
            table.name
            for table in bootstrap_odps.list_tables(project=project, schema=schema)
            if lifecycle_object_name(table.name)
        } == objects_after_first_janitor
        assert context.engine_adapter.fetchall(
            f"SELECT id, payload, ds FROM `{schema}`.`{incremental_model}` ORDER BY ds"
        ) == [
            [11, "initial-11", "2026-07-11"],
            [12, "manual-12", "2026-07-12"],
            [13, "automatic-13", "2026-07-13"],
        ]
    finally:
        if re.fullmatch(r"sqlmesh_lifecycle_[0-9a-f]{32}", model_prefix) is None:
            raise AssertionError(f"Unsafe lifecycle test prefix: {model_prefix}")
        if re.fullmatch(rf"{re.escape(schema)}__lc_[0-9a-f]{{12}}", dev_schema) is None:
            raise AssertionError(f"Unsafe lifecycle dev schema: {dev_schema}")

        for candidate_schema in bootstrap_odps.list_schemas(project=project, prefix=dev_schema):
            if candidate_schema.name != dev_schema:
                continue
            for table in list(bootstrap_odps.list_tables(project=project, schema=dev_schema)):
                if getattr(table, "is_virtual_view", False):
                    bootstrap_odps.delete_view(
                        table.name, project=project, schema=dev_schema, if_exists=True
                    )
                else:
                    bootstrap_odps.delete_table(
                        table.name, project=project, schema=dev_schema, if_exists=True
                    )
            bootstrap_odps.delete_schema(dev_schema, project=project)

        _cleanup_prefixed_objects(
            bootstrap_odps,
            project,
            schema,
            (model_prefix, f"{schema}__{model_prefix}"),
        )

        assert bootstrap_odps.exist_schema(schema, project=project)
        assert not dev_schema_exists()
        assert {
            table.name for table in bootstrap_odps.list_tables(project=project, schema=schema)
        } == initial_base_objects
