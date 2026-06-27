import os
import re
import uuid

import pytest

from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig, MaxComputeConnectionConfig
from sqlmesh.core.context import Context

pytestmark = pytest.mark.maxcompute


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


@pytest.mark.skipif(
    not _has_maxcompute_env(), reason="MaxCompute smoke credentials are not configured"
)
def test_maxcompute_no_schema_smoke_plan_apply(tmp_path) -> None:
    project = os.environ["MAXCOMPUTE_PROJECT"]
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
                    schema=os.getenv("MAXCOMPUTE_SCHEMA"),
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
        for table in adapter.odps.list_tables(project=project, prefix=f"analytics__{model_prefix}"):
            name = table.name
            if getattr(table, "is_virtual_view", False):
                adapter.drop_view(name, ignore_if_not_exists=True)
            else:
                adapter.drop_table(name, exists=True)
