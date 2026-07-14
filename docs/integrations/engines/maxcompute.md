# MaxCompute

SQLMesh can use Alibaba Cloud MaxCompute/ODPS as an execution engine through the `maxcompute` connection type. The adapter is intended for offline MaxCompute SQL workloads and uses PyODPS for SQL execution and metadata access.

MaxCompute is not supported as a SQLMesh state backend. Configure a separate `state_connection`; Postgres is recommended for production, while DuckDB is useful for local smoke tests.

## Supported Models

- `VIEW`, rendered as `CREATE OR REPLACE VIEW`.
- `FULL` table models.
- Partition-aligned `INCREMENTAL_BY_TIME_RANGE` models.

Partitioned incremental models must use simple column partition expressions such as `partitioned_by [ds]`. Transform expressions such as `DATE(ds)` are rejected because MaxCompute dynamic partition writes require direct partition column names.

## Unsupported In The First Version

- SQLMesh state sync on MaxCompute
- Python models
- pandas DataFrame writes
- Materialized views
- SCD Type 2
- grants
- full table atomic replace semantics
- automatic conversion from non-MaxCompute SQL dialects
- MaxQA/MCQA execution mode
- `partitioned_by` transform expressions such as `DATE(ds)`

## Configuration

Install SQLMesh with the MaxCompute extra so PyODPS is available:

```bash
pip install "sqlmesh[maxcompute]"
```

Configure MaxCompute for execution and a separate backend for SQLMesh state:

```yaml
gateways:
  prod:
    connection:
      type: maxcompute
      project: warehouse
      schema: analytics
      endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api
      access_key_id: ${MAXCOMPUTE_ACCESS_KEY_ID}
      access_key_secret: ${MAXCOMPUTE_ACCESS_KEY_SECRET}
      security_token: ${MAXCOMPUTE_SECURITY_TOKEN}  # Optional STS token.
      tunnel_endpoint: https://dt.cn-hangzhou.maxcompute.aliyun.com
      quota_name: default
      execution_mode: offline
      sql_hints:
        odps.sql.allow.fullscan: "true"
    state_connection:
      type: postgres
      host: ${SQLMESH_STATE_HOST}
      port: 5432
      user: ${SQLMESH_STATE_USER}
      password: ${SQLMESH_STATE_PASSWORD}
      database: sqlmesh_state
```

`security_token` enables STS authentication and is passed to PyODPS as an `StsAccount`. `tunnel_endpoint` is optional and is forwarded to `ODPS(...)` for projects that require an explicit tunnel service endpoint.

Setting `schema` explicitly opts the connection into schema namespace behavior. This applies even when PyODPS tenant detection reports that schema namespace is disabled.

The first version uses the PyODPS DBAPI offline execution path. Keep `execution_mode` set to `offline`; MaxQA/MCQA execution requires a different cursor execution path and is outside this version.

## Schema Namespace Behavior

MaxCompute projects may or may not have schema namespace enabled. The adapter treats schema namespace as enabled when either PyODPS tenant detection (`odps.is_schema_namespace_enabled()`) returns true or the connection has an explicit `schema`. An explicit connection schema therefore takes precedence when tenant detection returns false. If tenant detection fails, the adapter also defaults to schema-enabled behavior to avoid silently folding names that should remain schema-qualified.

When schema namespace is enabled, SQLMesh logical schemas render as MaxCompute schemas:

```sql
CREATE TABLE `analytics`.`dim_customer` ...
```

When schema namespace is disabled and the connection does not specify `schema`, the adapter does not send `CREATE SCHEMA` or `DROP SCHEMA`. It folds SQLMesh logical schemas into object names with a double underscore:

```sql
analytics.dim_customer -> analytics__dim_customer
```

The same rule applies to physical snapshot objects. For a logical model `analytics.fact_order_daily`, a physical table in a no-schema project is created under the configured MaxCompute project with a folded name such as:

```text
sqlmesh__analytics__analytics__fact_order_daily__<version>
```

For no-schema projects, omit `schema` from the connection and map the logical schema to the MaxCompute project so SQLMesh does not try to create a physical schema:

```yaml
physical_schema_mapping:
  "^analytics$": warehouse
```

## Partitioned Incremental Model

```sql
MODEL (
  name analytics.fact_order_daily,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds
  ),
  partitioned_by [ds],
  dialect maxcompute,
  physical_properties (
    lifecycle = 365
  )
);

SELECT
  order_id,
  customer_id,
  amount,
  ds
FROM raw.orders
WHERE ds BETWEEN @start_ds AND @end_ds;
```

The adapter writes partitioned incremental models with `INSERT OVERWRITE TABLE ... PARTITION (...)` and keeps partition columns at the end of the `SELECT` projection.

For example, even if the model query projects `ds` first, the adapter writes normal columns first and partition columns last:

```sql
INSERT OVERWRITE TABLE `analytics__fact_order_daily` PARTITION (`ds`)
SELECT
  `order_id`,
  `customer_id`,
  `amount`,
  `ds`
FROM ...
```

## Table Lifecycle

Use `physical_properties` to set a MaxCompute table lifecycle:

```sql
MODEL (
  name analytics.dim_customer,
  kind FULL,
  dialect maxcompute,
  physical_properties (
    lifecycle = 30
  )
);

SELECT
  customer_id,
  customer_name
FROM raw.customer;
```

SQLMesh renders this as MaxCompute `LIFECYCLE 30` syntax rather than a regular `TBLPROPERTIES` entry.

## Operational Semantics

MaxCompute is treated as a non-transactional execution engine. Full table replacement is not atomic. For partitioned `FULL` replacement, SQLMesh may drop and recreate the table; if recreation fails, rerun the plan after fixing the cause. Partitioned incremental reruns overwrite the same partition range.

The adapter avoids MaxCompute's invalid `CREATE TABLE (cols) AS SELECT ...` form. Partitioned tables and tables with `physical_properties` are created with a two-step flow: `CREATE TABLE ...` followed by `INSERT INTO` or `INSERT OVERWRITE TABLE ... PARTITION (...)`.

## Real Smoke Test

The repository includes a gated integration smoke test for real MaxCompute projects:

```bash
export MAXCOMPUTE_PROJECT=warehouse
export MAXCOMPUTE_ENDPOINT=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_ACCESS_KEY_ID=...
export MAXCOMPUTE_ACCESS_KEY_SECRET=...

pytest tests/core/engine_adapter/integration/test_integration_maxcompute.py -v
```

With credentials alone, the no-schema smoke uses a single DuckDB file for SQLMesh state and validates `Context.plan()` plus repeated `Context.apply()`. It skips when the target project has schema namespace enabled.

The schema-enabled smoke performs schema DDL and must be explicitly enabled:

```bash
export MAXCOMPUTE_SCHEMA_SMOKE=1

pytest tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_schema_smoke_plan_apply -v
```

This smoke first verifies that schemas can be listed, then creates an isolated random schema named `sqlmesh_smoke_<random>`. Both the connection schema and physical schema mapping point to that namespace. The test runs `Context.plan()` and repeated `Context.apply()`, reads the resulting FULL and partitioned incremental models, and removes the schema afterward. It never creates test objects in the default namespace. This schema-enabled workflow has passed against the real `york_fic` project, including the case where PyODPS tenant detection returned false while the connection specified the random schema.

The lifecycle smoke is also gated and is restricted to the pre-created, schema-enabled `york_fic.sqlmesh` namespace:

```bash
export MAXCOMPUTE_PROJECT=york_fic
export MAXCOMPUTE_SCHEMA=sqlmesh
export MAXCOMPUTE_LIFECYCLE_SMOKE=1

pytest tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_schema_lifecycle_restate_janitor -v
```

This workflow has passed against the real `york_fic.sqlmesh` namespace. It uses one local DuckDB state file, not SQLite, and verifies idempotent `Context.plan()` and `Context.apply()`, a manual restatement, `auto_restatement_cron` processing through `Context.run()`, the production run invoking the janitor, development environment invalidation followed by a scoped janitor run, expired snapshot table and state cleanup, a repeated idempotent janitor run, and the MaxCompute `LIFECYCLE` metadata on source and physical tables. It does not execute `Context.destroy()`.
