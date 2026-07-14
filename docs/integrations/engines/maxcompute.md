# MaxCompute

SQLMesh can use Alibaba Cloud MaxCompute/ODPS as an execution engine through the `maxcompute` connection type. PyODPS provides SQL execution and metadata access.

MaxCompute is not supported as a SQLMesh state backend. Configure a separate `state_connection`. Production deployments should use a durable supported backend; the repository's real integration tests use one local DuckDB file. PostgreSQL state was intentionally not included in the real verification matrix.

## Configuration

Install the optional dependency:

```bash
pip install "sqlmesh[maxcompute]"
```

Configure offline execution and a separate state backend:

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
      execution_mode: offline
      register_comments: true
      sql_hints:
        odps.sql.allow.fullscan: "true"
    state_connection:
      type: duckdb
      database: ./sqlmesh_state.duckdb
```

`security_token` creates a PyODPS `StsAccount`. `tunnel_endpoint` and `quota_name` are passed to PyODPS when configured.

### MaxQA

MaxQA is selected explicitly and requires a quota:

```yaml
connection:
  type: maxcompute
  project: warehouse
  schema: analytics
  endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api
  access_key_id: ${MAXCOMPUTE_ACCESS_KEY_ID}
  access_key_secret: ${MAXCOMPUTE_ACCESS_KEY_SECRET}
  execution_mode: maxqa
  quota_name: ${MAXCOMPUTE_QUOTA_NAME}
  maxqa_fallback_policy: none  # none, default, or all
```

The adapter maps `maxqa` to PyODPS MCQA V2 with `use_sqa="v2"`. Configuration and mapping are unit tested. A gated read-only `SELECT 1` test exists, but MaxQA has not been verified against a real quota because no test quota was available.

## Namespace Behavior

MaxCompute projects can operate with or without schema namespace support.

When schema namespace is enabled, set `schema`. SQLMesh renders `project.schema.object` names and sends schema DDL. The schema-enabled real tests are restricted to the pre-created `york_fic.sqlmesh` namespace; they do not access the project's default business schema.

When schema namespace is disabled, omit `schema`. The adapter does not send `CREATE SCHEMA` or `DROP SCHEMA`. It folds each logical schema into the physical object name:

```text
analytics.dim_customer -> analytics__dim_customer
```

The same normalization is applied to physical snapshot objects and inherited DDL/DML operations. Map a logical schema to the project:

```yaml
physical_schema_mapping:
  "^analytics$": york_data
```

The no-schema smoke is explicitly gated and restricted to `york_data`. It verifies folded names, `Context.plan()`, repeated `Context.apply()`, and readable results.

## Capability Matrix

`Real` means the capability has passed a gated test against MaxCompute. `Unit` means SQL generation or adapter behavior is covered locally but the current test project did not permit or exercise the operation.

| Capability | Status | Notes |
| --- | --- | --- |
| `VIEW`, `FULL` | Real | End-to-end plan/apply and repeated apply. |
| `INCREMENTAL_BY_TIME_RANGE` | Real / Unit | Manual and explicit automatic partitions are Real. A repeated two-day `york_fic` smoke used actual PyODPS metadata and preserved the unaffected day. Automatic-partition source type validation is Unit. |
| `INCREMENTAL_BY_PARTITION` | Real | Requires simple manual physical partition columns; automatic `TRUNC_TIME` partitions are not supported for this kind. |
| `INCREMENTAL_UNMANAGED (insert_overwrite true)` | Real | Verified for an unpartitioned target; no empty `PARTITION ()`. |
| `INCREMENTAL_BY_UNIQUE_KEY` | Real | Requires `transactional = true`; uses native `MERGE`. |
| `SCD_TYPE_2_BY_TIME`, `SCD_TYPE_2_BY_COLUMN` | Real | Non-partitioned transactional tables only. |
| `EXTERNAL`, `EMBEDDED` | Real | Plan/apply behavior is covered by the model-kind smoke. |
| Materialized views | Real | Evaluator plan/apply plus lifecycle, partition, HASH cluster, query, and drop/create replacement. Janitor type routing is unit tested. |
| Regular view storage properties | Unit | Regular `VIEW` rejects materialized/storage properties before execution; only materialized views accept them. |
| Audit execution | Real | Both passing and failing audit results are verified. |
| Restatement, cron, run, janitor | Real | DuckDB file state; manual and automatic restatement plus cleanup. |
| Comments | Real / Unit | Table and non-partition column metadata are verified on MaxCompute. Manual partition column comments render inside `PARTITIONED BY` at creation (Unit). View/MV comment rendering is unit tested; MV creation accepted comment-bearing DDL, but the comment was not read back. |
| Pandas query reads, RowDiff/TableDiff | Real | `_fetch_native_df()` supports query-result DataFrames. |
| Metadata | Real | Object type, columns including complex types, and last modified time. |
| Native temporal type rendering and implicit partition guard | Unit | The MaxCompute dialect preserves `DATETIME` and `TIMESTAMP_NTZ`; all four supported temporal types require explicit `TRUNC_TIME` when `partition_interval` implies automatic partitioning. |
| Rename and truncate | Real / Unit | Schema-enabled same-namespace rename and non-partition truncate are Real. No-schema rename is Unit and requires the source and target to retain the same logical namespace before folding. |
| Add/drop column and safe type widening | Unit | Requires MaxCompute schema-evolution project capability; disabled on `york_fic`. |
| `disable_rewrite` for materialized views | Unit | Rendering is covered; the `york_fic` parser rejected it. |
| MaxQA | Unit | Gated read-only test exists; no quota was available for real verification. |
| DataFrame, Seed, Python model writes | Unsupported | Pandas query-result reads remain supported. |
| Grants, `MANAGED`, WAP, clone | Unsupported | Grants are intentionally disabled for business accounts. |
| Multi-catalog and MaxCompute state backend | Unsupported | The configured project is the execution catalog only. |
| Atomic table replacement | Unsupported | `SUPPORTS_REPLACE_TABLE=False`. |

`SUPPORTS_TRANSACTIONS=False` describes DBAPI multi-statement transaction support. It does not prohibit MaxCompute transactional tables or native transactional-table `MERGE` operations. The adapter does not claim rollback across statements.

## Partitions And Clustering

Manual partitioning uses simple columns:

```sql
partitioned_by [ds]
```

For manual partitions, writes use `INSERT ... PARTITION (...)`, and the adapter moves the existing partition projection to the end without replacing computed expressions.

Automatic partitioning must be explicit:

```sql
partitioned_by [TRUNC_TIME(event_ts, 'day') AS ds]
```

Supported units are `day`, `hour`, `month`, and `year`. The adapter does not reinterpret `DATE(ds)` as a MaxCompute partition declaration. Automatic-partition append writes omit the `PARTITION (...)` clause and let MaxCompute generate the partition.

The `TRUNC_TIME` source column must be `DATE`, `DATETIME`, `TIMESTAMP`, or `TIMESTAMP_NTZ`. The adapter validates this before issuing DDL. Unit tests cover every accepted type and verify that non-temporal source columns fail explicitly.

The MaxCompute dialect renders `DATETIME` and `TIMESTAMP_NTZ` as their native MaxCompute types instead of inheriting Hive's coercion to `TIMESTAMP`. This rendering is unit tested. The implicit `partition_interval` guard applies consistently to `DATE`, `DATETIME`, `TIMESTAMP`, and `TIMESTAMP_NTZ`: declaring an interval on any of these source types does not infer an automatic partition expression and fails with a requirement to declare `TRUNC_TIME` explicitly. The native rendering and guard are Unit evidence.

An automatic-partition `INCREMENTAL_BY_TIME_RANGE` overwrite must receive a bounded interval condition. Before writing, the adapter reads the existing target's actual PyODPS partition metadata and requires its generated expression, source column, unit, and alias to match the declared `TRUNC_TIME`. It fails before DML if the target is unpartitioned or if the generated expression has a different source, alias, or granularity.

MaxCompute cannot directly overwrite a generated partition by its expression, so the adapter combines target rows outside the condition with replacement rows inside the condition, stages that complete result in a temporary table with `LIFECYCLE 1`, and performs a full target overwrite. This preserves unaffected partitions. The two-day real smoke on `york_fic` passed again using actual PyODPS metadata: it replaced one day and verified that the other day remained unchanged. The operation is still subject to the adapter's non-atomic multi-step replacement semantics.

`INCREMENTAL_BY_PARTITION` continues to require simple manual physical partition columns and does not accept automatic `TRUNC_TIME` partition expressions.

HASH clustering requires a bucket count:

```sql
clustered_by [customer_id],
physical_properties (
  cluster_bucket_num = 32
)
```

Range/sorted clustering is not supported. Transactional tables and clustering cannot be combined by this adapter.

## Transactional Models

`INCREMENTAL_BY_UNIQUE_KEY` and both SCD2 kinds require an explicit transactional property:

```sql
MODEL (
  name analytics.customer_current,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key customer_id),
  physical_properties (
    transactional = true,
    primary_key = (customer_id),
    write_bucket_num = 16,
    lifecycle = 30
  ),
  dialect maxcompute
);
```

`primary_key` is optional. When present, it must contain simple columns and equal the model's unique key. SCD2 models currently reject `partitioned_by`; they use SQLMesh's full-table replacement algorithm on a non-partitioned transactional table. Native `MERGE` excludes primary-key and manual partition columns from its default update list and preserves a model's custom `when_matched` clause.

## Materialized Views

Materialized views support lifecycle, a source-backed partition, HASH clustering, and comments. Replacement is `DROP MATERIALIZED VIEW` followed by `CREATE MATERIALIZED VIEW`; the adapter does not claim `CREATE OR REPLACE MATERIALIZED VIEW` support.

Regular `VIEW` definitions reject materialized/storage properties such as partition, lifecycle, clustering, and rewrite controls. These properties are accepted only when creating a materialized view. This rejection is covered by unit tests.

`disable_rewrite` is rendered and unit tested, but it is not marked real because the `york_fic` parser rejected that clause. Availability depends on the target MaxCompute environment.

## Operational Semantics

MaxCompute execution is not wrapped in a DBAPI multi-statement transaction. Full replacement and materialized-view replacement are not atomic. A failed multi-step create/write or drop/create operation can leave an intermediate state; rerun after correcting the cause.

The adapter avoids invalid `CREATE TABLE (cols) AS SELECT` SQL. Tables with explicit columns, partitions, properties, comments, or lifecycle use `CREATE TABLE` followed by a write. Metadata is read through PyODPS object APIs rather than parsing `DESCRIBE` output.

When comments are registered, a manual partition column comment is emitted with the partition column definition inside `PARTITIONED BY (...)` at table creation. This rendering is unit tested; the existing real comment readback covers table and non-partition columns.

Rename remains a same-namespace operation. In no-schema mode, folding names must not erase this boundary: source and target must have the same logical project and schema before they are folded to `schema__table`. The no-schema rejection path is unit tested.

Schema evolution validates supported add/drop and conservative widening operations before DDL execution. The MaxCompute project must separately enable schema evolution. The real `york_fic` project reported that schema-evolution DDL was disabled, so this capability remains unit verified there.

The generic shared cloud adapter harness is not used because it performs broad schema and data writes that are unsafe for the protected test project. MaxCompute uses dedicated, gated integration tests with strict project/schema checks and UUID object prefixes.

## Real Verification

All write tests require credentials plus an explicit gate. Tests targeting schema namespace mode require `MAXCOMPUTE_PROJECT=york_fic` and `MAXCOMPUTE_SCHEMA=sqlmesh`. They inventory objects before running, clean only their UUID-scoped prefixes and the corresponding generated `__temp_` prefixes, restore the original object set, and never call `Context.destroy()`.

The no-schema test is restricted to `york_data` with `MAXCOMPUTE_SCHEMA` unset:

```bash
unset MAXCOMPUTE_SCHEMA
MAXCOMPUTE_PROJECT=york_data \
MAXCOMPUTE_NO_SCHEMA_SMOKE=1 \
.venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_no_schema_smoke_plan_apply -v
```

Schema-enabled tests use the pre-created namespace:

```bash
export MAXCOMPUTE_PROJECT=york_fic
export MAXCOMPUTE_SCHEMA=sqlmesh
export MAXCOMPUTE_ENDPOINT=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_ACCESS_KEY_ID=...
export MAXCOMPUTE_ACCESS_KEY_SECRET=...

MAXCOMPUTE_AUDIT_SMOKE=1 .venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_real_audit_execution -v

MAXCOMPUTE_CAPABILITY_SMOKE=1 .venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_schema_adapter_capabilities -v

MAXCOMPUTE_TRANSACTIONAL_SMOKE=1 .venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_transactional_models_plan_apply -v

MAXCOMPUTE_MODEL_KIND_SMOKE=1 .venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_additional_model_kinds_plan_apply -v

MAXCOMPUTE_LIFECYCLE_SMOKE=1 .venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_schema_lifecycle_restate_janitor -v
```

`MAXCOMPUTE_SCHEMA_EVOLUTION_SMOKE=1` is a nested opt-in inside the capability test. It is expected to fail or skip unless the project has schema-evolution DDL enabled. `MAXCOMPUTE_MAXQA_SMOKE=1` additionally requires `MAXCOMPUTE_QUOTA_NAME` and runs only the read-only MaxQA query test. `MAXCOMPUTE_SCHEMA_SMOKE=1` retains the earlier isolated-schema plan/apply smoke where schema creation is permitted.

The lifecycle test uses a DuckDB file for SQLMesh state, freezes snapshot creation time for deterministic expiry, and verifies initial and repeated apply, manual restatement, `auto_restatement_cron` through `Context.run()`, dev-environment invalidation, janitor cleanup, snapshot/interval state removal, and janitor idempotency. PostgreSQL state is not part of this verification by request.

Run all gated tests only in an approved isolated account or namespace. Credentials must be supplied through environment variables and must not be stored in configuration files committed to source control.
