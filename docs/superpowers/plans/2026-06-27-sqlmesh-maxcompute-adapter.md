# SQLMesh MaxCompute Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to implement this plan task-by-task. The implementing AI must complete all tasks in one pass, summarize the result, and then wait for Claude to perform code review. Do not create a worktree; make changes on the current branch.

## Current Implementation Status（2026-07-14）

This plan has been implemented and subsequently extended on the current branch. The original TDD tasks remain below as historical traceability; they describe the minimum adapter, not the final capability boundary.

Current implementation truth:

- Namespace behavior is centralized: schema-enabled projects preserve `project.schema.object`; no-schema projects skip schema DDL and fold names as `schema__table`. Real no-schema coverage is explicitly gated and restricted to `york_data`; protected schema tests use the pre-created `york_fic.sqlmesh` namespace.
- Real plan/apply evidence covers `VIEW`, `FULL`, `INCREMENTAL_BY_TIME_RANGE`, `INCREMENTAL_BY_PARTITION`, unpartitioned `INCREMENTAL_UNMANAGED (insert_overwrite true)`, `INCREMENTAL_BY_UNIQUE_KEY`, both SCD2 kinds, `EXTERNAL`, and `EMBEDDED`.
- UNIQUE_KEY and SCD2 require `physical_properties (transactional = true)`. Optional simple `primary_key` must equal the unique key; `write_bucket_num` is supported. SCD2 currently rejects `partitioned_by`.
- Manual partitions render a `PARTITION (...)` clause and move existing partition projections to the end. Automatic partitions require explicit `TRUNC_TIME(ts, 'day|hour|month|year') AS alias`; the source must be `DATE`, `DATETIME`, `TIMESTAMP`, or `TIMESTAMP_NTZ`, validated before DDL with Unit coverage for accepted and rejected types. The dialect preserves native `DATETIME` and `TIMESTAMP_NTZ` instead of Hive's `TIMESTAMP` coercion, and the implicit `partition_interval` guard requires explicit `TRUNC_TIME` for all four supported temporal types (Unit). Append omits the DML partition clause. Automatic time-range overwrite requires a bounded condition, validates the target's actual PyODPS generated expression and alias, and uses a `LIFECYCLE 1` temporary full replacement to preserve rows outside the interval. It fails for an unpartitioned target or mismatched source/alias/granularity. A two-day `york_fic` smoke passed again with actual metadata and preserved the unaffected day. `INCREMENTAL_BY_PARTITION` remains limited to simple manual columns, and `DATE(ds)` is not implicitly reinterpreted.
- HASH `clustered_by` requires `cluster_bucket_num`. Transactional and clustered table properties are rejected as an unsupported combination.
- Materialized views support lifecycle, source partition, HASH cluster, comments, metadata, query, and drop/create replacement. Evaluator plan/apply is real verified; janitor object-type routing is unit tested. Regular `VIEW` rejects materialized/storage properties (Unit), and the adapter does not claim `CREATE OR REPLACE MATERIALIZED VIEW`.
- Table/non-partition column comments, Pandas query reads, RowDiff/TableDiff, metadata type/last-modified fields, schema-enabled same-namespace rename, and non-partition truncate have Real evidence. Manual partition column comments render during creation (Unit). No-schema rename requires the same logical namespace before name folding (Unit). Schema evolution has Unit coverage but is conditional because the `york_fic` project has schema-evolution DDL disabled.
- Audit and the restatement/cron/run/janitor lifecycle have passed against `york_fic.sqlmesh` with DuckDB file state. The lifecycle test freezes snapshot creation time for deterministic expiry. PostgreSQL state was intentionally not tested.
- MaxQA supports `execution_mode: offline | maxqa`, requires `quota_name`, maps to `use_sqa="v2"`, and supports fallback `none | default | all`. It has Unit coverage and a gated read-only test, but no real quota was available.

Current boundaries:

- `SUPPORTS_TRANSACTIONS=False` means no DBAPI multi-statement transaction contract; transactional MaxCompute tables remain supported. `SUPPORTS_REPLACE_TABLE=False` remains accurate.
- DataFrame/Seed/Python model writes, grants, MANAGED, WAP, clone, multi-catalog, MaxCompute state backend, and atomic replacement are unsupported. Pandas query-result reads are supported.
- MV `disable_rewrite` is rendered and unit tested, but the `york_fic` parser rejected it.
- Dedicated gated integration tests are used instead of the shared cloud harness because the generic harness performs schema and data writes that are unsafe for the protected project.
- Every protected real test inventories and restores `york_fic.sqlmesh`, removes only UUID-scoped objects plus replacement tables with the corresponding generated `__temp_` prefixes, and never calls `Context.destroy()`.

| Capability group | Status | Evidence boundary |
| --- | --- | --- |
| Core, partition, transactional, SCD2, EXTERNAL/EMBEDDED models | Real / Unit | Model workflows and repeated two-day automatic overwrite are Real; automatic source-type validation is Unit |
| Materialized views and audit | Real | Dedicated gated evaluator tests on `york_fic.sqlmesh`; MV janitor type routing is Unit |
| Lifecycle and janitor | Real | Environment/snapshot cleanup through the dedicated gated lifecycle test |
| Comments, reads/diffs, metadata, rename and truncate | Real / Unit | Core operations are Real; partition-column comment and no-schema logical rename boundary are Unit |
| Regular View storage-property rejection | Unit | Only materialized views accept materialized/storage properties |
| Schema evolution and MV `disable_rewrite` | Unit | Target project/parser capability unavailable |
| Native temporal type rendering and implicit partition guard | Unit | `DATETIME`/`TIMESTAMP_NTZ` remain native; all four temporal types require explicit `TRUNC_TIME` with `partition_interval` |
| MaxQA | Unit | Mapping and gated test exist; no quota available |
| Data writes from Python/DataFrame/Seed and administrative features | Unsupported | Explicit adapter boundary |

**Goal:** Add a built-in SQLMesh `maxcompute` execution adapter that can plan and apply supported offline SQL models against Alibaba Cloud MaxCompute/ODPS while keeping SQLMesh state in an external state backend.

**Architecture:** Add a first-party `MaxComputeEngineAdapter` with explicit MaxCompute DDL/DML rendering for table, view, CTAS fallback, insert append, and partition overwrite paths. Add a `MaxComputeConnectionConfig` that lazily imports PyODPS DBAPI, maps SQLMesh config fields to PyODPS connection parameters, and forbids MaxCompute as a state sync engine. Use PyODPS object APIs for metadata instead of parsing `DESCRIBE` output.

**Tech Stack:** Python 3.9+, SQLMesh engine adapter framework, SQLGlot AST/rendering, PyODPS DBAPI/Object API, pytest, pytest-mock.

---

## In Scope

- `type: maxcompute` execution connection with offline and MaxQA modes.
- Schema namespace and no-schema folded object naming.
- Supported SQL model kinds listed in the current status, including transactional UNIQUE_KEY/SCD2 and materialized views.
- Manual partitions and explicit `TRUNC_TIME` automatic time partitions, including bounded temporary replacement for time-range overwrite; `INCREMENTAL_BY_PARTITION` remains simple-column only.
- Lifecycle, comments including manual partition-column creation syntax, transactional properties, primary/write buckets, and HASH clustering.
- DDL/DML, PyODPS metadata, Pandas query reads, RowDiff/TableDiff, rename, truncate, and validated schema evolution.
- Gated real MaxCompute audit, capability, transactional, model-kind, no-schema, schema, lifecycle, and MaxQA tests.
- DuckDB file state for real verification and documentation of external production state separation.

## Out Of Scope

- SQLMesh state sync stored in MaxCompute.
- Python model, Seed, and pandas DataFrame writes.
- Grants.
- Full table atomic replace guarantees.
- Automatic conversion from arbitrary SQL dialects to MaxCompute SQL.
- Cross-project catalog behavior beyond mapping `project` to default catalog.
- MaxCompute project/user/role administration.
- MANAGED, WAP, clone, and multi-catalog workflows.
- Implicit partition transformations such as `DATE(ds)`; automatic partitioning requires explicit `TRUNC_TIME`.
- PostgreSQL state plus MaxCompute execution real integration coverage for this iteration.

## Reference Notes

- `/Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/dbapi.py` confirms DBAPI `connect` accepts `hints` and `quota_name`; cursor execution merges connection hints with per-call hints.
- `/Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/core.py` confirms `ODPS.__init__` supports `schema` and `tunnel_endpoint`; re-check this exact signature during Task 2 before coding because the unit tests only inspect kwargs and do not open a PyODPS connection.
- `/Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/types.py` confirms `table_schema.columns` includes normal and partition columns, while `simple_columns` excludes partition columns.
- `/Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/accounts.py` confirms STS auth should use `StsAccount(access_id, secret_access_key, sts_token)`.
- `/Users/aleksichen/git/github/dbt-maxcompute/tests/functional/maxcompute/test_insert_overwrite_multi_partition.py` pins multi-column dynamic partition overwrite behavior.

---

### Task 1: Register `maxcompute` Dialect And Adapter Skeleton

**Files:**
- Create: `sqlmesh/core/engine_adapter/maxcompute.py`
- Modify: `sqlmesh/core/engine_adapter/__init__.py`
- Modify: `sqlmesh/core/dialect.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Test: `tests/core/test_dialect.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add these tests:

```python
# tests/core/engine_adapter/test_maxcompute.py
import typing as t

import pytest
from sqlglot import exp

from sqlmesh.core.engine_adapter import MaxComputeEngineAdapter, create_engine_adapter
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    InsertOverwriteStrategy,
)

pytestmark = [pytest.mark.maxcompute, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> MaxComputeEngineAdapter:
    return make_mocked_engine_adapter(MaxComputeEngineAdapter)


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
```

```python
# tests/core/test_dialect.py
def test_maxcompute_dialect_alias_extends_sqlmesh_model_syntax() -> None:
    ast = d.parse_one("MODEL (name analytics.orders)", dialect="maxcompute")
    assert isinstance(ast, d.Model)
    assert ast.sql(dialect="maxcompute") == "MODEL (\nname analytics.orders\n)"
    assert exp.DataType.build("string", dialect="maxcompute").sql(dialect="maxcompute") == "STRING"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_adapter_registration tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_adapter_capabilities tests/core/test_dialect.py::test_maxcompute_dialect_alias_extends_sqlmesh_model_syntax -v
```

Expected: FAIL with `ImportError: cannot import name 'MaxComputeEngineAdapter'` or SQLGlot unknown dialect error for `maxcompute`.

- [ ] **Step 3: Write minimal implementation**

Add the adapter skeleton:

```python
# sqlmesh/core/engine_adapter/maxcompute.py
from __future__ import annotations

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    InsertOverwriteStrategy,
)


class MaxComputeEngineAdapter(EngineAdapter):
    DIALECT = "maxcompute"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_GRANTS = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
```

Register the adapter:

```python
# sqlmesh/core/engine_adapter/__init__.py
from sqlmesh.core.engine_adapter.maxcompute import MaxComputeEngineAdapter

DIALECT_TO_ENGINE_ADAPTER = {
    # existing entries...
    "maxcompute": MaxComputeEngineAdapter,
}
```

Register a local SQLGlot dialect backed by Hive syntax:

```python
# sqlmesh/core/dialect.py
from sqlglot.dialects import DuckDB, Hive, Snowflake, TSQL


class MaxCompute(Hive):
    pass
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_adapter_registration tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_adapter_capabilities tests/core/test_dialect.py::test_maxcompute_dialect_alias_extends_sqlmesh_model_syntax -v
```

Expected: PASS.

---

### Task 2: Add MaxCompute Connection Config And State Sync Guard

**Files:**
- Modify: `sqlmesh/core/config/connection.py`
- Test: `tests/core/test_connection_config.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add imports and tests:

```python
# tests/core/test_connection_config.py
from sqlmesh.core.config.connection import MaxComputeConnectionConfig


def test_maxcompute_connection_config(make_config):
    config = make_config(
        type="maxcompute",
        project="warehouse",
        schema="analytics",
        endpoint="https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        access_key_id="ak",
        access_key_secret="sk",
        security_token="token",
        tunnel_endpoint="https://dt.cn-hangzhou.maxcompute.aliyun.com",
        quota_name="default",
        sql_hints={"odps.sql.allow.fullscan": "true"},
        check_import=False,
    )

    assert isinstance(config, MaxComputeConnectionConfig)
    assert config.type_ == "maxcompute"
    assert config.DIALECT == "maxcompute"
    assert config.concurrent_tasks == 1
    assert config.register_comments is False
    assert config.pre_ping is False
    assert config.get_catalog() == "warehouse"
    assert config.is_recommended_for_state_sync is False
    assert config.is_forbidden_for_state_sync is True

    kwargs = config._static_connection_kwargs
    assert kwargs["project"] == "warehouse"
    assert kwargs["schema"] == "analytics"
    assert kwargs["endpoint"] == "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
    assert kwargs["tunnel_endpoint"] == "https://dt.cn-hangzhou.maxcompute.aliyun.com"
    assert kwargs["hints"] == {"odps.sql.allow.fullscan": "true"}
    assert kwargs["quota_name"] == "default"
    assert "security_token" not in kwargs
```

```python
# tests/core/engine_adapter/test_maxcompute.py
def test_maxcompute_connection_config_passes_hints_to_adapter(make_config) -> None:
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/test_connection_config.py::test_maxcompute_connection_config tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_connection_config_passes_hints_to_adapter -v
```

Expected: FAIL with `ImportError: cannot import name 'MaxComputeConnectionConfig'` or `Unknown connection type 'maxcompute'`.

- [ ] **Step 3: Write minimal implementation**

Before coding, verify the PyODPS constructor path:

```bash
sed -n '74,112p' /Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/dbapi.py
sed -n '100,145p' /Users/aleksichen/git/github/aliyun-odps-python-sdk/odps/core.py
```

Expected: `odps.dbapi.Connection.__init__` forwards unknown kwargs to `ODPS(...)`, and `ODPS.__init__` accepts `schema` and `tunnel_endpoint`.

Add `maxcompute` to forbidden state sync engines:

```python
# sqlmesh/core/config/connection.py
FORBIDDEN_STATE_SYNC_ENGINES = {
    "spark",
    "trino",
    "clickhouse",
    "starrocks",
    "maxcompute",
}
```

Add the config class near similar warehouse connection configs:

```python
class MaxComputeConnectionConfig(ConnectionConfig):
    project: str
    schema_: t.Optional[str] = Field(alias="schema", default=None)
    endpoint: str
    access_key_id: t.Optional[str] = None
    access_key_secret: t.Optional[str] = None
    security_token: t.Optional[str] = None
    tunnel_endpoint: t.Optional[str] = None
    quota_name: t.Optional[str] = None
    execution_mode: t.Literal["offline"] = "offline"
    sql_hints: t.Dict[str, str] = Field(default_factory=dict)

    concurrent_tasks: int = 1
    register_comments: t.Literal[False] = False
    pre_ping: t.Literal[False] = False

    type_: t.Literal["maxcompute"] = Field(alias="type", default="maxcompute")
    DIALECT: t.ClassVar[t.Literal["maxcompute"]] = "maxcompute"
    DISPLAY_NAME: t.ClassVar[t.Literal["MaxCompute"]] = "MaxCompute"
    DISPLAY_ORDER: t.ClassVar[t.Literal[19]] = 19

    _engine_import_validator = _get_engine_import_validator("odps", "maxcompute")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return set()

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MaxComputeEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from odps.dbapi import connect

        return connect

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        kwargs: t.Dict[str, t.Any] = {
            "project": self.project,
            "endpoint": self.endpoint,
            "schema": self.schema_,
            "tunnel_endpoint": self.tunnel_endpoint,
            "hints": dict(self.sql_hints),
            "quota_name": self.quota_name,
        }
        if self.security_token:
            from odps.accounts import StsAccount

            kwargs["account"] = StsAccount(
                self.access_key_id,
                self.access_key_secret,
                self.security_token,
            )
        else:
            kwargs["access_id"] = self.access_key_id
            kwargs["secret_access_key"] = self.access_key_secret
        return {key: value for key, value in kwargs.items() if value is not None}

    def get_catalog(self) -> t.Optional[str]:
        return self.project
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/test_connection_config.py::test_maxcompute_connection_config tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_connection_config_passes_hints_to_adapter -v
```

Expected: PASS.

---

### Task 3: Render MaxCompute Table DDL, Partitions, Lifecycle, And Table Properties

**Files:**
- Modify: `sqlmesh/core/engine_adapter/maxcompute.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add tests:

```python
from sqlglot import parse_one
from sqlmesh.core.model import load_sql_based_model
import sqlmesh.core.dialect as d
from tests.core.engine_adapter import to_sql_calls


def test_maxcompute_create_partitioned_table_lifecycle_properties(adapter: MaxComputeEngineAdapter) -> None:
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


def test_maxcompute_rejects_non_column_partition_expression(adapter: MaxComputeEngineAdapter) -> None:
    with pytest.raises(SQLMeshError, match="MaxCompute partitioned_by only supports simple column references"):
        adapter.create_table(
            "analytics.bad_partition",
            target_columns_to_types={"ds": exp.DataType.build("string")},
            partitioned_by=[parse_one("DATE(ds)")],
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_create_partitioned_table_lifecycle_properties tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_rejects_non_column_partition_expression -v
```

Expected: FAIL because base adapter renders partition fields in the main column list, does not render `LIFECYCLE`, or does not reject transform partitions.

- [ ] **Step 3: Write minimal implementation**

Implement helper methods and override create table expression:

```python
# sqlmesh/core/engine_adapter/maxcompute.py
import typing as t
from sqlglot import exp

from sqlmesh.core._typing import TableName
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils.errors import SQLMeshError


def _property_value_name(value: exp.Expr) -> str:
    return value.name if isinstance(value, exp.Expr) else str(value)


class MaxComputeEngineAdapter(EngineAdapter):
    # existing class constants...

    def _partition_column_names(self, partitioned_by: t.Optional[t.List[exp.Expr]]) -> t.List[str]:
        names: t.List[str] = []
        for partition in partitioned_by or []:
            if not isinstance(partition, exp.Column) or partition.parts[:-1]:
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
        table = exp.to_table(table_name)
        if_not_exists = " IF NOT EXISTS" if exists else ""
        data_schema = ", ".join(
            f"{exp.to_identifier(name).sql(dialect=self.dialect, identify=True)} {dtype.sql(dialect=self.dialect)}"
            for name, dtype in data_columns.items()
        )
        sql = f"CREATE TABLE{if_not_exists} {table.sql(dialect=self.dialect, identify=True)} ({data_schema})"
        if partition_columns:
            partition_schema = ", ".join(
                f"{exp.to_identifier(name).sql(dialect=self.dialect, identify=True)} {dtype.sql(dialect=self.dialect)}"
                for name, dtype in partition_columns.items()
            )
            sql += f" PARTITIONED BY ({partition_schema})"
        properties_sql = self._build_maxcompute_properties_sql(kwargs.get("table_properties"))
        if properties_sql:
            sql += f" {properties_sql}"
        self.execute(sql)
        self._clear_data_object_cache(table)
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_create_partitioned_table_lifecycle_properties tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_rejects_non_column_partition_expression -v
```

Expected: PASS.

---

### Task 4: Implement CTAS Rules And Two-Step Create/Write Fallback

**Files:**
- Modify: `sqlmesh/core/engine_adapter/maxcompute.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add tests:

```python
def test_maxcompute_plain_ctas_uses_no_column_schema(adapter: MaxComputeEngineAdapter) -> None:
    adapter.ctas(
        "analytics.full_orders",
        query_or_df=parse_one("SELECT 1 AS order_id"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `analytics`.`full_orders` AS SELECT 1 AS `order_id`"
    ]


def test_maxcompute_partitioned_ctas_uses_create_then_partition_overwrite(adapter: MaxComputeEngineAdapter) -> None:
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
    assert all("CREATE TABLE IF NOT EXISTS `analytics`.`daily_orders` (`ds` STRING, `order_id` BIGINT) AS SELECT" not in sql for sql in to_sql_calls(adapter))


def test_maxcompute_lifecycle_ctas_uses_create_then_insert(adapter: MaxComputeEngineAdapter) -> None:
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_plain_ctas_uses_no_column_schema tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_partitioned_ctas_uses_create_then_partition_overwrite tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_lifecycle_ctas_uses_create_then_insert -v
```

Expected: FAIL because the base CTAS path can render `CREATE TABLE (cols) AS SELECT` or does not perform the two-step path.

- [ ] **Step 3: Write minimal implementation**

Override `ctas`:

```python
from sqlmesh.core.engine_adapter._typing import QueryOrDF


class MaxComputeEngineAdapter(EngineAdapter):
    # existing methods...

    def _requires_two_step_ctas(self, **kwargs: t.Any) -> bool:
        table_properties = kwargs.get("table_properties") or {}
        return bool(kwargs.get("partitioned_by") or table_properties)

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
        if target_columns_to_types is None:
            target_columns_to_types = {}

        if self._requires_two_step_ctas(**kwargs):
            self.create_table(
                table_name,
                target_columns_to_types=target_columns_to_types,
                exists=exists,
                **kwargs,
            )
            for source_query in source_queries:
                with source_query as query:
                    if kwargs.get("partitioned_by"):
                        self.insert_overwrite_by_partition(
                            table_name,
                            query,
                            partitioned_by=kwargs["partitioned_by"],
                            target_columns_to_types=target_columns_to_types,
                        )
                    else:
                        self._insert_append_query(table_name, query, target_columns_to_types)
            return

        for source_query in source_queries:
            with source_query as query:
                table = exp.to_table(table_name)
                exists_sql = " IF NOT EXISTS" if exists else ""
                self.execute(
                    f"CREATE TABLE{exists_sql} {table.sql(dialect=self.dialect, identify=True)} AS {query.sql(dialect=self.dialect, identify=True)}"
                )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_plain_ctas_uses_no_column_schema tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_partitioned_ctas_uses_create_then_partition_overwrite tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_lifecycle_ctas_uses_create_then_insert -v
```

Expected: PASS and no generated SQL contains `CREATE TABLE ... (cols) ... AS SELECT`.

---

### Task 5: Render Insert Append And Dynamic Partition Overwrite With Correct Projection Order

**Files:**
- Modify: `sqlmesh/core/engine_adapter/maxcompute.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add tests:

```python
def test_maxcompute_insert_append(adapter: MaxComputeEngineAdapter) -> None:
    adapter._insert_append_query(
        "analytics.orders",
        parse_one("SELECT 1 AS order_id, '2026-06-27' AS ds"),
        {"order_id": exp.DataType.build("bigint"), "ds": exp.DataType.build("string")},
    )

    assert to_sql_calls(adapter) == [
        "INSERT INTO `analytics`.`orders` (`order_id`, `ds`) SELECT 1 AS `order_id`, '2026-06-27' AS `ds`"
    ]


def test_maxcompute_insert_overwrite_partition_moves_partition_columns_to_end(adapter: MaxComputeEngineAdapter) -> None:
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


def test_maxcompute_insert_overwrite_multiple_partitions_are_last_in_declared_order(adapter: MaxComputeEngineAdapter) -> None:
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_append tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_overwrite_partition_moves_partition_columns_to_end tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_overwrite_multiple_partitions_are_last_in_declared_order -v
```

Expected: FAIL because the default insert overwrite path does not render `PARTITION (...)` and does not guarantee partition projections at the end. The computed `price * quantity AS amount` assertion must fail if the implementation rebuilds projections as bare `exp.column("amount")` references.

- [ ] **Step 3: Write minimal implementation**

Add projection ordering and DML rendering:

```python
class MaxComputeEngineAdapter(EngineAdapter):
    # existing methods...

    def _projection_order_for_partition_overwrite(
        self,
        target_columns_to_types: t.Dict[str, exp.DataType],
        partitioned_by: t.List[exp.Expr],
    ) -> t.List[str]:
        partition_names = self._partition_column_names(partitioned_by)
        partition_name_set = set(partition_names)
        normal_names = [
            name for name in target_columns_to_types if name not in partition_name_set
        ]
        return normal_names + partition_names

    def _select_columns_in_order(
        self,
        query: exp.Query,
        names: t.List[str],
        target_columns_to_types: t.Dict[str, exp.DataType],
    ) -> exp.Query:
        if isinstance(query, exp.Select):
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

        ordered_columns_to_types = {
            name: target_columns_to_types[name]
            for name in names
        }
        return self._order_projections_and_filter(query, ordered_columns_to_types)

    def _insert_append_query(
        self,
        table_name: TableName,
        query: exp.Query,
        target_columns_to_types: t.Dict[str, exp.DataType],
        order_projections: bool = True,
        track_rows_processed: bool = True,
    ) -> None:
        if order_projections:
            query = self._order_projections_and_filter(query, target_columns_to_types)
        table = exp.to_table(table_name)
        columns = ", ".join(
            exp.to_identifier(name).sql(dialect=self.dialect, identify=True)
            for name in target_columns_to_types
        )
        self.execute(
            f"INSERT INTO {table.sql(dialect=self.dialect, identify=True)} ({columns}) {query.sql(dialect=self.dialect, identify=True)}",
            track_rows_processed=track_rows_processed,
        )

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=table_name,
            source_columns=source_columns,
        )
        if target_columns_to_types is None:
            target_columns_to_types = self.columns(table_name)
        projection_order = self._projection_order_for_partition_overwrite(
            target_columns_to_types, partitioned_by
        )
        partition_sql = ", ".join(
            exp.to_identifier(name).sql(dialect=self.dialect, identify=True)
            for name in self._partition_column_names(partitioned_by)
        )
        table = exp.to_table(table_name)
        for source_query in source_queries:
            with source_query as query:
                ordered_query = self._select_columns_in_order(
                    query,
                    projection_order,
                    target_columns_to_types,
                )
                self.execute(
                    f"INSERT OVERWRITE TABLE {table.sql(dialect=self.dialect, identify=True)} PARTITION ({partition_sql}) {ordered_query.sql(dialect=self.dialect, identify=True)}"
                )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_append tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_overwrite_partition_moves_partition_columns_to_end tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_insert_overwrite_multiple_partitions_are_last_in_declared_order -v
```

Expected: PASS.

---

### Task 6: Route Time-Range Incremental Partition Overwrite To MaxCompute Partition DML

**Files:**
- Modify: `sqlmesh/core/engine_adapter/maxcompute.py`
- Modify: `sqlmesh/core/snapshot/evaluator.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Test: `tests/core/test_snapshot_evaluator.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add adapter-level test:

```python
from sqlmesh.core.model.kind import TimeColumn


def test_maxcompute_time_partition_overwrite_routes_to_partition_clause(adapter: MaxComputeEngineAdapter) -> None:
    adapter._insert_overwrite_by_time_partition(
        table_name="analytics.fact_order_daily",
        source_queries=[
            SourceQuery(
                query_factory=lambda: parse_one(
                    "SELECT ds, amount, order_id FROM staging.orders WHERE ds BETWEEN '2026-06-01' AND '2026-06-27'"
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
        "INSERT OVERWRITE TABLE `analytics`.`fact_order_daily` PARTITION (`ds`) SELECT `order_id`, `amount`, `ds` FROM `staging`.`orders` WHERE `ds` BETWEEN '2026-06-01' AND '2026-06-27'"
    ]
```

Add evaluator routing test:

```python
# tests/core/test_snapshot_evaluator.py
from unittest.mock import Mock

from sqlglot import exp, parse_one

from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.snapshot.evaluator import IncrementalByTimeRangeStrategy
import sqlmesh.core.dialect as d


def test_incremental_by_time_range_passes_partitioned_by_to_adapter() -> None:
    model = load_sql_based_model(
        d.parse(
            """
            MODEL (
              name analytics.fact_order_daily,
              kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds
              ),
              partitioned_by [ds],
              dialect maxcompute
            );

            SELECT 1 AS order_id, '2026-06-27' AS ds;
            """
        )
    )
    adapter = Mock()
    strategy = IncrementalByTimeRangeStrategy(adapter)

    strategy.insert(
        table_name="analytics.fact_order_daily",
        query_or_df=parse_one("SELECT 1 AS order_id, '2026-06-27' AS ds"),
        model=model,
        is_first_insert=False,
        render_kwargs={},
        start="2026-06-27",
        end="2026-06-27",
    )

    assert adapter.insert_overwrite_by_time_partition.call_args.kwargs["partitioned_by"] == [
        exp.column("ds")
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_time_partition_overwrite_routes_to_partition_clause tests/core/test_snapshot_evaluator.py::test_incremental_by_time_range_passes_partitioned_by_to_adapter -v
```

Expected: FAIL because `_insert_overwrite_by_time_partition` currently delegates to condition overwrite and `IncrementalByTimeRangeStrategy.insert` does not pass `model.partitioned_by` into `insert_overwrite_by_time_partition`.

- [ ] **Step 3: Write minimal implementation**

Pass `partitioned_by` from the evaluator:

```python
# sqlmesh/core/snapshot/evaluator.py
class IncrementalByTimeRangeStrategy(IncrementalStrategy):
    def insert(...):
        # existing setup...
        self.adapter.insert_overwrite_by_time_partition(
            table_name,
            query_or_df,
            start=kwargs["start"],
            end=kwargs["end"],
            time_formatter=model.convert_to_time_column,
            time_column=model.time_column,
            target_columns_to_types=columns_to_types,
            source_columns=source_columns,
            partitioned_by=model.partitioned_by,
            **kwargs,
        )
```

Override `_insert_overwrite_by_time_partition`:

```python
from sqlmesh.core.engine_adapter.shared import SourceQuery


class MaxComputeEngineAdapter(EngineAdapter):
    # existing methods...

    def _insert_overwrite_by_time_partition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Dict[str, exp.DataType],
        where: exp.Condition,
        **kwargs: t.Any,
    ) -> None:
        partitioned_by = kwargs.get("partitioned_by")
        if partitioned_by:
            for source_query in source_queries:
                with source_query as query:
                    query = query.where(where, copy=True) if isinstance(query, exp.Select) else query
                    self.insert_overwrite_by_partition(
                        table_name,
                        query,
                        partitioned_by=partitioned_by,
                        target_columns_to_types=target_columns_to_types,
                    )
            return
        return super()._insert_overwrite_by_time_partition(
            table_name,
            source_queries,
            target_columns_to_types,
            where,
            **kwargs,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_time_partition_overwrite_routes_to_partition_clause tests/core/test_snapshot_evaluator.py::test_incremental_by_time_range_passes_partitioned_by_to_adapter -v
```

Expected: PASS.

---

### Task 7: Implement PyODPS Metadata APIs

**Files:**
- Modify: `sqlmesh/core/engine_adapter/maxcompute.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add tests with lightweight mocks:

```python
from types import SimpleNamespace
from sqlmesh.core.engine_adapter.shared import DataObjectType


def _odps_column(name: str, type_name: str):
    return SimpleNamespace(name=name, type=SimpleNamespace(name=type_name))


def test_maxcompute_columns_uses_pyodps_table_schema(adapter: MaxComputeEngineAdapter) -> None:
    table = SimpleNamespace(
        table_schema=SimpleNamespace(
            columns=[
                _odps_column("order_id", "bigint"),
                _odps_column("amount", "decimal(18,2)"),
                _odps_column("ds", "string"),
            ]
        )
    )
    adapter.connection.odps.get_table.return_value = table

    assert adapter.columns("analytics.orders") == {
        "order_id": exp.DataType.build("BIGINT"),
        "amount": exp.DataType.build("DECIMAL(18, 2)"),
        "ds": exp.DataType.build("STRING"),
    }
    adapter.connection.odps.get_table.assert_called_once_with(
        "orders", project="analytics", schema=None
    )


def test_maxcompute_table_exists(adapter: MaxComputeEngineAdapter) -> None:
    adapter.connection.odps.exist_table.return_value = True
    assert adapter.table_exists("analytics.orders") is True
    adapter.connection.odps.exist_table.assert_called_once_with(
        "orders", project="analytics", schema=None
    )


def test_maxcompute_get_data_objects_maps_tables_and_views(adapter: MaxComputeEngineAdapter) -> None:
    adapter.connection.odps.list_tables.return_value = [
        SimpleNamespace(name="orders", is_virtual_view=False),
        SimpleNamespace(name="orders_v", is_virtual_view=True),
    ]

    objects = adapter._get_data_objects("analytics")

    assert [(obj.name, obj.type) for obj in objects] == [
        ("orders", DataObjectType.TABLE),
        ("orders_v", DataObjectType.VIEW),
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_columns_uses_pyodps_table_schema tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_table_exists tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_get_data_objects_maps_tables_and_views -v
```

Expected: FAIL because `columns`, `table_exists`, and `_get_data_objects` still use base SQL metadata paths.

- [ ] **Step 3: Write minimal implementation**

Implement metadata helpers:

```python
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType


class MaxComputeEngineAdapter(EngineAdapter):
    # existing methods...

    @property
    def odps(self) -> t.Any:
        return self.connection.odps

    def _table_parts(self, table_name: TableName) -> t.Tuple[str, t.Optional[str], t.Optional[str]]:
        table = exp.to_table(table_name)
        return table.name, table.db or self.default_catalog, table.catalog

    def _odps_type_to_sqlglot(self, type_: t.Any) -> exp.DataType:
        type_name = getattr(type_, "name", str(type_)).upper()
        return exp.DataType.build(type_name, dialect=self.dialect)

    def columns(self, table_name: TableName, include_pseudo_columns: bool = False) -> t.Dict[str, exp.DataType]:
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
        schema_name: str,
        object_names: t.Optional[t.Set[str]] = None,
    ) -> t.List[DataObject]:
        objects: t.List[DataObject] = []
        for table in self.odps.list_tables(project=self.default_catalog, schema=schema_name):
            if object_names and table.name not in object_names:
                continue
            objects.append(
                DataObject(
                    catalog=self.default_catalog,
                    schema=schema_name,
                    name=table.name,
                    type=DataObjectType.VIEW
                    if getattr(table, "is_virtual_view", False)
                    else DataObjectType.TABLE,
                )
            )
        return objects
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_columns_uses_pyodps_table_schema tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_table_exists tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_get_data_objects_maps_tables_and_views -v
```

Expected: PASS.

---

### Task 8: Add End-To-End SQLMesh Behavior Tests With Mocked MaxCompute Adapter

**Files:**
- Modify: `tests/core/engine_adapter/test_maxcompute.py`
- Modify: `tests/core/test_connection_config.py`
- Test: `tests/core/engine_adapter/test_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add tests that exercise SQLMesh model-level calls without real MaxCompute:

```python
def test_maxcompute_view_creation(adapter: MaxComputeEngineAdapter) -> None:
    adapter.create_view(
        "analytics.orders_v",
        parse_one("SELECT order_id FROM analytics.orders"),
        target_columns_to_types={"order_id": exp.DataType.build("bigint")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE OR REPLACE VIEW `analytics`.`orders_v` AS SELECT `order_id` FROM `analytics`.`orders`"
    ]


def test_maxcompute_execution_and_postgres_state_connection_can_coexist(make_config) -> None:
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
        user="sqlmesh",
        password="sqlmesh",
        database="sqlmesh_state",
        check_import=False,
    )

    assert execution.is_forbidden_for_state_sync is True
    assert state.is_recommended_for_state_sync is True
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_view_creation tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_execution_and_postgres_state_connection_can_coexist -v
```

Expected: FAIL because view SQL or config import paths are not yet complete.

- [ ] **Step 3: Write minimal implementation**

Override view creation only if base rendering does not match MaxCompute:

```python
class MaxComputeEngineAdapter(EngineAdapter):
    # existing methods...

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
        view = exp.to_table(view_name)
        for source_query in source_queries:
            with source_query as query:
                prefix = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW"
                self.execute(
                    f"{prefix} {view.sql(dialect=self.dialect, identify=True)} AS {query.sql(dialect=self.dialect, identify=True)}"
                )
        self._clear_data_object_cache(view_name)
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_view_creation tests/core/engine_adapter/test_maxcompute.py::test_maxcompute_execution_and_postgres_state_connection_can_coexist -v
```

Expected: PASS.

---

### Task 9: Add Gated Real MaxCompute Smoke Test Fixture

**Files:**
- Create: `tests/core/engine_adapter/integration/test_integration_maxcompute.py`
- Modify: none
- Test: `tests/core/engine_adapter/integration/test_integration_maxcompute.py`
- Delete: none

- [ ] **Step 1: Write the failing test**

Add a gated integration test:

```python
# tests/core/engine_adapter/integration/test_integration_maxcompute.py
import os
import re
import uuid

import pytest

from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig, MaxComputeConnectionConfig
from sqlmesh.core.context import Context

pytestmark = [pytest.mark.maxcompute, pytest.mark.integration]


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


@pytest.mark.skipif(not _has_maxcompute_env(), reason="MaxCompute smoke credentials are not configured")
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
```

The `maxcompute` optional dependency and pytest marker already exist in `pyproject.toml`; do not edit that file for this task unless the implementation has removed them.

- [ ] **Step 2: Run test to verify it fails or skips**

Run:

```bash
pytest tests/core/engine_adapter/integration/test_integration_maxcompute.py -v
```

Expected without credentials: SKIPPED with `MaxCompute smoke credentials are not configured`. Expected with credentials before implementation completion: FAIL in adapter execution.

- [ ] **Step 3: Write minimal implementation**

No additional production code should be added for this task. Fix only integration-test wiring issues uncovered by imports or current SQLMesh `Config` constructor signatures. Keep the test gated by environment variables.

- [ ] **Step 4: Run test to verify it passes or skips correctly**

Run:

```bash
pytest tests/core/engine_adapter/integration/test_integration_maxcompute.py -v
```

Expected without credentials: SKIPPED. Expected with credentials: PASS, creating supported MaxCompute table/view objects and allowing repeated apply.

---

### Task 10: Add User Documentation

> Historical note: this task records the minimum documentation proposed for the first adapter version. The effective user documentation and the status sections above supersede its original `Supported Models` / `Unsupported In The First Version` lists.

**Files:**
- Create: `docs/integrations/engines/maxcompute.md`
- Modify: `docs/integrations/engines.md` if this index exists
- Test: `docs/integrations/engines/maxcompute.md`
- Delete: none

- [ ] **Step 1: Write the failing documentation check**

Run this before creating the doc:

```bash
test -f docs/integrations/engines/maxcompute.md
```

Expected: FAIL with exit code `1`.

- [ ] **Step 2: Run command to verify it fails**

Run:

```bash
test -f docs/integrations/engines/maxcompute.md
```

Expected: FAIL with exit code `1`.

- [ ] **Step 3: Write minimal documentation**

Create:

```markdown
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
      quota_name: default
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

## Operational Semantics

MaxCompute is treated as a non-transactional execution engine. Full table replacement is not atomic. Failed runs may leave created objects behind, but partitioned incremental reruns overwrite the same partition range.
```

- [ ] **Step 4: Run command to verify it passes**

Run:

```bash
test -f docs/integrations/engines/maxcompute.md && rg -n "state_connection|INSERT OVERWRITE TABLE|LIFECYCLE|non-transactional" docs/integrations/engines/maxcompute.md
```

Expected: PASS and `rg` prints all four required topics.

---

## Final Exit Criteria

- [x] `type: maxcompute`, adapter registration, dialect extension and external state separation are implemented.
- [x] Invalid explicit-column CTAS is avoided; lifecycle, comments, partition and other physical properties use create+write when required.
- [x] Manual dynamic partition writes preserve computed expressions and move partition projections to the end.
- [x] Explicit `TRUNC_TIME` automatic partitioning validates `DATE`/`DATETIME`/`TIMESTAMP`/`TIMESTAMP_NTZ` sources before DDL and rejects non-temporal types; native `DATETIME`/`TIMESTAMP_NTZ` rendering and the four-type implicit `partition_interval` guard have Unit coverage; append omits the DML `PARTITION` clause, while time-range overwrite requires a bounded condition, validates actual target generated-expression metadata, and uses a `LIFECYCLE 1` temporary full replacement that preserves unaffected partitions.
- [x] FULL, VIEW, MV, time/partition/unmanaged/unique-key incremental, both SCD2 kinds, EXTERNAL and EMBEDDED have gated Real evidence.
- [x] Transactional properties, optional primary key/write bucket, native MERGE and SCD2 restrictions are enforced.
- [x] Table/non-partition comments, fetchdf, RowDiff/TableDiff, metadata, schema-enabled rename and truncate are covered by the dedicated capability smoke.
- [x] A two-day `york_fic` smoke passed again with actual PyODPS metadata, verifying automatic time-range overwrite replaces the bounded day and preserves the unaffected day.
- [x] Regular View storage-property rejection, no-schema same-logical-namespace rename validation, and manual partition-column comment rendering have Unit coverage.
- [x] Schema-enabled and no-schema namespace paths have independent Real evidence.
- [x] Audit, manual/auto restatement, cron, `Context.run()`, janitor and lifecycle pass with DuckDB file state.
- [x] Protected real tests restore `york_fic.sqlmesh` to its pre-test object set and never call `Context.destroy()`.
- [x] Unsupported write/model/administration features fail explicitly or remain capability-disabled.
- [ ] Schema evolution awaits a project with schema-evolution DDL enabled.
- [ ] MV `disable_rewrite` awaits an instance whose parser supports the clause.
- [ ] MaxQA awaits an available test quota; config, mapping and gated read-only coverage are complete.

## Validation Command Package

Run local tests first:

```bash
.venv312/bin/python -m pytest tests/core/engine_adapter/test_maxcompute.py -q
.venv312/bin/python -m pytest tests/core/test_connection_config.py -q -k maxcompute
.venv312/bin/python -m pytest tests/core/test_snapshot_evaluator.py -q
.venv312/bin/python -m pytest tests/core/engine_adapter/test_base.py -q
.venv312/bin/python -m pytest tests/core/test_janitor.py -q
make style
```

Real tests require credentials through environment variables. Schema-enabled write tests use the protected pre-created namespace:

```bash
export MAXCOMPUTE_PROJECT=york_fic
export MAXCOMPUTE_SCHEMA=sqlmesh
export MAXCOMPUTE_ENDPOINT=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_ACCESS_KEY_ID=...
export MAXCOMPUTE_ACCESS_KEY_SECRET=...
```

Run each capability through its independent gate:

```bash
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

Schema evolution is nested inside the capability test and has an additional opt-in:

```bash
MAXCOMPUTE_CAPABILITY_SMOKE=1 \
MAXCOMPUTE_SCHEMA_EVOLUTION_SMOKE=1 \
.venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_schema_adapter_capabilities -v
```

The target project must enable schema-evolution DDL. `york_fic` currently does not, so the regular capability smoke leaves this segment disabled.

The no-schema test uses a separate project and requires `MAXCOMPUTE_SCHEMA` to be unset:

```bash
unset MAXCOMPUTE_SCHEMA
MAXCOMPUTE_PROJECT=york_data \
MAXCOMPUTE_NO_SCHEMA_SMOKE=1 \
.venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_no_schema_smoke_plan_apply -v
```

The earlier schema smoke remains separately gated with `MAXCOMPUTE_SCHEMA_SMOKE=1`. MaxQA requires a real quota and is read-only:

```bash
MAXCOMPUTE_MAXQA_SMOKE=1 \
MAXCOMPUTE_QUOTA_NAME=... \
.venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_maxqa_query -v
```

The complete gate list is `MAXCOMPUTE_SCHEMA_SMOKE`, `MAXCOMPUTE_NO_SCHEMA_SMOKE`, `MAXCOMPUTE_AUDIT_SMOKE`, `MAXCOMPUTE_CAPABILITY_SMOKE`, `MAXCOMPUTE_SCHEMA_EVOLUTION_SMOKE`, `MAXCOMPUTE_TRANSACTIONAL_SMOKE`, `MAXCOMPUTE_MODEL_KIND_SMOKE`, `MAXCOMPUTE_LIFECYCLE_SMOKE`, and `MAXCOMPUTE_MAXQA_SMOKE`.

All real lifecycle/audit/capability tests use DuckDB file state. No PostgreSQL state combination is part of this plan.

## Execution Recommendation Order

1. Task 1: Register dialect and adapter skeleton.
2. Task 2: Add connection config and state sync guard.
3. Task 3: Implement table DDL, partition, lifecycle, and properties.
4. Task 5: Implement append and dynamic partition overwrite DML.
5. Task 4: Implement CTAS and two-step create/write fallback.
6. Task 6: Route time-range incremental partition overwrite.
7. Task 7: Implement PyODPS metadata APIs.
8. Task 8: Add mocked SQLMesh behavior tests.
9. Task 9: Add gated real MaxCompute smoke test.
10. Task 10: Add documentation.

Capability-extension execution order after the original ten tasks:

1. Centralize namespace normalization and inherited DDL/DML behavior.
2. Add metadata, read, comments, rename, truncate and schema-evolution contracts.
3. Add transactional UNIQUE_KEY/SCD2 support.
4. Add automatic partitions, HASH clustering and materialized views.
5. Add MaxQA configuration and dedicated gated real harnesses.
6. Run developer -> code-reviewer iteration and update user/design/plan documentation.

## Highest Risk 3 Points

1. **Project capability variation:** schema evolution and MV `DISABLE REWRITE` are accepted by local rendering but unavailable on `york_fic`; documentation and tests must not promote them to Real without a capable project.
2. **Non-atomic replacement:** `SUPPORTS_REPLACE_TABLE=False` is intentional. Full-table and MV drop/create flows can leave intermediate state and must remain rerunnable.
3. **Protected namespace safety:** shared generic cloud tests are too broad. Gated tests must keep strict project/schema assertions, UUID cleanup, complete object-inventory restoration, and the prohibition on `Context.destroy()`.

## Remaining Follow-Ups

1. Run the existing MaxQA read-only gate when an approved `MAXCOMPUTE_QUOTA_NAME` becomes available.
2. Re-run the nested schema-evolution segment on a project with schema-evolution DDL enabled.
3. Re-evaluate MV `disable_rewrite` only on an instance whose parser supports the clause.
4. Expand real nested `struct`, `array`, and `map` metadata fixtures without touching the business default schema.
