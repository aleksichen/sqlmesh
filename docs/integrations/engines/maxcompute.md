# MaxCompute

SQLMesh can use Alibaba Cloud MaxCompute/ODPS as an execution engine through the `maxcompute` connection type.

MaxCompute is not supported as a SQLMesh state backend. Production projects should configure a separate `state_connection`, for example Postgres.

## Supported Models

- `VIEW`
- `FULL`
- Partition-aligned `INCREMENTAL_BY_TIME_RANGE`

## Unsupported In The First Version

- SQLMesh state sync on MaxCompute
- Python models
- pandas DataFrame writes
- Materialized views
- SCD Type 2
- grants
- full table atomic replace semantics
- automatic conversion from non-MaxCompute SQL dialects

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

The first version uses the PyODPS DBAPI offline execution path. Keep `execution_mode` set to `offline`; MaxQA/MCQA execution requires a different cursor execution path and is outside this version.

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

MaxCompute is treated as a non-transactional execution engine. Full table replacement is not atomic. Failed runs may leave created objects behind, but partitioned incremental reruns overwrite the same partition range.
