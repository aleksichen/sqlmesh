# SQLMesh MaxCompute Adapter 能力设计

## 当前实现状态（2026-07-14）

本设计最初定义了离线 `VIEW`、`FULL` 和 `INCREMENTAL_BY_TIME_RANGE` 的最小适配器。当前分支已经在不虚报事务和原子替换能力的前提下扩展到 transactional table、SCD2、物化视图、自动分区、元数据、生命周期和 MaxQA 配置。

状态定义：

- **Real**：已通过 gated 真机测试。
- **Unit**：SQL 渲染、参数映射或 adapter 契约已有单测，但目标实例未开放或未提供所需资源。
- **Unsupported**：adapter 明确拒绝或不声明能力。

| 能力 | 状态 | 当前结论 |
| --- | --- | --- |
| Schema namespace / no-schema folding | Real | schema 模式使用预创建的 `york_fic.sqlmesh`；no-schema 模式在 `york_data` 折叠为 `schema__table`。 |
| `VIEW`, `FULL`, `INCREMENTAL_BY_TIME_RANGE` | Real / Unit | plan/apply 和自动分区两日 smoke 为 Real；`TRUNC_TIME` source type 白名单及非时间类型拒绝为 Unit。 |
| `INCREMENTAL_BY_PARTITION` | Real | 仅支持简单手工物理分区列，不接受自动 `TRUNC_TIME` 分区。 |
| `INCREMENTAL_UNMANAGED (insert_overwrite true)` | Real | 已验证无分区 overwrite，不生成 `PARTITION ()`。 |
| `INCREMENTAL_BY_UNIQUE_KEY` | Real | 要求事务表，使用原生 `MERGE`。 |
| `SCD_TYPE_2_BY_TIME`, `SCD_TYPE_2_BY_COLUMN` | Real | 当前限定非分区事务表。 |
| `EXTERNAL`, `EMBEDDED` | Real | model-kind plan/apply smoke 已覆盖。 |
| Materialized view | Real / Unit | evaluator plan/apply、lifecycle、分区、HASH cluster、查询和 drop/create 为 Real；janitor 类型路由为 Unit。 |
| 普通 View storage properties | Unit | 普通 `VIEW` 在执行前拒绝 materialized/storage properties；只有 materialized view 接受。 |
| Audit | Real | 真机覆盖成功和失败结果。 |
| Restate / cron / run / janitor | Real | DuckDB 文件 state 下完整生命周期已验证。 |
| Comments / fetchdf / RowDiff / TableDiff | Real / Unit | table 和普通 column comment 为 Real；手工 partition column comment 在建表时渲染为 Unit。包含查询结果 Pandas DataFrame 读取。 |
| Metadata / rename / truncate | Real / Unit | metadata、schema-enabled 同 namespace rename 和非分区 truncate 为 Real；no-schema 逻辑 namespace 校验为 Unit。 |
| 原生时间类型渲染 / 隐式分区 guard | Unit | dialect 保留 `DATETIME`、`TIMESTAMP_NTZ`；四种支持时间类型的 `partition_interval` 都要求显式 `TRUNC_TIME`。 |
| Schema evolution | Unit | add/drop column 和保守 widening 有单测；`york_fic` 未开启 schema-evolution DDL。 |
| MV `disable_rewrite` | Unit | 可渲染；`york_fic` parser 拒绝该子句。 |
| MaxQA | Unit | 配置和 MCQA V2 映射已测；缺少 quota，未真机验证。 |
| DataFrame/Seed/Python writes, grants, MANAGED, WAP, clone | Unsupported | 查询结果 DataFrame 读取不受影响。 |
| Multi-catalog, MaxCompute state backend, atomic replace | Unsupported | `project` 仅作为执行 catalog。 |

## 1. Goal / Architecture / Tech Stack

**Goal**：让 SQLMesh 能以 MaxCompute 为执行引擎完成受支持模型的 plan/apply、audit、restatement 和生命周期管理，同时将 SQLMesh state 保存在独立 backend。

**Architecture**：

- `MaxComputeConnectionConfig` 负责 PyODPS DBAPI 连接、STS、offline/MaxQA 参数和 state backend 禁止规则。
- `MaxComputeEngineAdapter` 负责统一名称规范化、MaxCompute DDL/DML、物理属性解析、PyODPS metadata 和能力校验。
- evaluator 只传递模型 kind 与 partition 信息；UNIQUE_KEY 和 SCD2 复用 SQLMesh evaluator 算法及 MaxCompute 原生事务表能力。
- 真机测试使用专用 gated harness。共享 cloud harness 会执行通用 schema/data 写入，不适用于受保护业务 project，因此未接入。

**Tech Stack**：SQLMesh、SQLGlot AST、PyODPS DBAPI/Object API、MaxCompute SQL、pytest、DuckDB file state。

## 2. Scope

### In Scope

- `type: maxcompute` execution connection；offline 和 MaxQA 配置。
- schema namespace 和 no-schema namespace 两种对象命名。
- 表、视图、物化视图的创建、替换、删除和 metadata。
- FULL、时间增量、分区增量、unmanaged overwrite、UNIQUE_KEY、两种 SCD2、EXTERNAL、EMBEDDED。
- 手工分区、显式自动分区、HASH clustering、lifecycle、transactional、primary key、bucket 和 comments。
- 查询结果 DataFrame、RowDiff/TableDiff、rename、truncate、受限 schema evolution。
- audit、manual/auto restatement、cron、`Context.run()`、environment/snapshot janitor。

### Out Of Scope

- MaxCompute 作为 SQLMesh state backend。
- DataFrame、Seed 和 Python model 输出写入。
- Grants、MANAGED、WAP、clone。
- 跨 project/multi-catalog 管理。
- DBAPI 多语句事务、rollback 和原子 table/MV replace。
- Postgres state 与 MaxCompute execution 的真机组合测试；本轮按要求只用 DuckDB file state。

## 3. 核心能力契约

```python
DIALECT = "maxcompute"
SUPPORTS_TRANSACTIONS = False
SUPPORTS_REPLACE_TABLE = False
SUPPORTS_MATERIALIZED_VIEWS = True
SUPPORTS_GRANTS = False
```

`SUPPORTS_TRANSACTIONS=False` 表示 PyODPS DBAPI 不提供 SQLMesh 所需的多语句 transaction/rollback 契约，不代表 MaxCompute transactional table 不可用。`SUPPORTS_REPLACE_TABLE=False` 表示 adapter 不承诺原子表替换。

MaxCompute 特定 SQL 由 adapter 显式渲染；本地 `maxcompute` SQLGlot dialect 基于 Hive，只负责通用 AST 与 SQLMesh MODEL/AUDIT 解析，不宣称任意方言到 MaxCompute 的完整转写。

## 4. Namespace 设计

adapter 使用 PyODPS `odps.is_schema_namespace_enabled()` 和显式 connection `schema` 判断 namespace。显式 `schema` 是启用信号；检测异常时保守按 schema-enabled 处理，避免误折叠合法三层名称。

### Schema namespace enabled

- `project` 映射 catalog，schema 和 object 保持独立。
- 允许 schema-qualified DDL/DML 和 metadata。
- 真机写测试固定 `MAXCOMPUTE_PROJECT=york_fic`、`MAXCOMPUTE_SCHEMA=sqlmesh`。
- `sqlmesh` 必须预先存在；测试不访问 default 业务 schema。

### Schema namespace disabled

- 不发送 `CREATE SCHEMA` / `DROP SCHEMA`。
- `_to_sql()` 的统一 AST 转换入口将 `db.name` 折叠为 `db__name`。
- PyODPS metadata 使用 `project=...`、`schema=None`。
- 逻辑和物理 snapshot 对象使用同一折叠规则。
- 真机 smoke 必须显式设置 `MAXCOMPUTE_NO_SCHEMA_SMOKE=1`，且只允许 `york_data`、`MAXCOMPUTE_SCHEMA` 未设置。

## 5. DDL 与 DML

### CTAS 和 replacement

adapter 不生成 MaxCompute 非法的 `CREATE TABLE t (cols) AS SELECT ...`。显式列、partition、properties、comments、lifecycle 等场景统一使用 `CREATE TABLE` 加后续写入。

非分区 overwrite 使用位置匹配：

```sql
INSERT OVERWRITE TABLE target SELECT ...
```

不渲染 overwrite column list。Full table 和 materialized view replacement 均非原子；失败后通过 rerun 恢复。

### 手工和自动分区

手工分区仅接受简单列。动态写入渲染 `PARTITION (...)`，并移动已有投影表达式，使分区列位于末尾，不重建计算 alias：

```sql
INSERT OVERWRITE TABLE target PARTITION (ds)
SELECT order_id, price * quantity AS amount, ds FROM source
```

自动分区只接受：

```sql
TRUNC_TIME(ts, 'day|hour|month|year') AS alias
```

自动分区 append 写入不渲染 `PARTITION (...)`，由 MaxCompute 服务端生成分区。adapter 不把 `DATE(ds)` 或普通时间列隐式改写成自动分区。

`TRUNC_TIME` source column 只允许 `DATE`、`DATETIME`、`TIMESTAMP` 或 `TIMESTAMP_NTZ`。adapter 必须在发送 DDL 前完成类型校验；四种允许类型均有 Unit 测试，非时间类型必须明确失败。

`maxcompute` dialect 必须保留原生 `DATETIME` 和 `TIMESTAMP_NTZ`，不能继承 Hive generator 将其强制渲染为 `TIMESTAMP`。该 dialect 渲染具备 Unit 证据。隐式 `partition_interval` guard 同时覆盖 `DATE`、`DATETIME`、`TIMESTAMP`、`TIMESTAMP_NTZ`：任一类型只声明 interval 都不能推导自动分区，必须明确提示使用显式 `TRUNC_TIME`。该 guard 为 Unit 证据。

自动分区的 `INCREMENTAL_BY_TIME_RANGE` overwrite 必须带有 bounded interval condition。执行 DML 前，adapter 通过 PyODPS 读取现有目标表的真实 partition metadata，并校验 generated expression 的 source column、`TRUNC_TIME` unit 和 alias 与模型声明完全一致。目标表没有自动分区，或 source、alias、day/hour/month/year 粒度不匹配时，必须提前失败。

MaxCompute 不能按生成表达式直接覆盖指定自动分区，因此 adapter 构造“condition 外的现有目标行 + condition 内的新查询行”，先写入带 `LIFECYCLE 1` 的临时全量 replacement 表，再以该完整结果执行目标表全量 overwrite。该流程保留未受影响分区，但仍属于非原子的多步 replacement。`york_fic` 两日真机 smoke 已使用真实 PyODPS metadata 再次通过：只替换其中一天并保留另一天。

`INCREMENTAL_BY_PARTITION` 仍只接受简单手工物理分区列，不接受自动 `TRUNC_TIME` partition expression。

`INCREMENTAL_UNMANAGED (insert_overwrite true)` 的无分区路径必须生成普通全表 overwrite，不能生成空 `PARTITION ()`。

### Clustering

首期只支持 HASH：

- `clustered_by` 必须同时声明 `physical_properties (cluster_bucket_num=N)`。
- 拒绝缺失 bucket 数、range/sorted clustering。
- transactional table 与 cluster 组合明确拒绝。

### Comments 与 schema evolution

当 `register_comments=true` 时支持 table、column 和 view comments。带 comments 的 CTAS 使用 create+insert，避免丢失显式 metadata。手工 partition column comment 在建表时随分区列渲染到 `PARTITIONED BY (...)`；该路径为 Unit，现有真机 readback 覆盖 table 和非分区 column comment。

支持 add/drop column 和保守的类型 widening，非法变化在 DDL 执行前失败。目标 project 必须开启 MaxCompute schema evolution；`york_fic` 未开启该能力，因此当前只有 Unit 证据。

### Rename 和 truncate

- rename 只允许同 project/schema；跨 namespace 在执行前拒绝。no-schema 模式必须先比较折叠前的逻辑 project/schema，不能因为最终都成为单段对象名而允许跨逻辑 namespace rename；该边界为 Unit。
- truncate 支持普通表；分区表在没有 partition spec 时明确拒绝。

## 6. Transactional、UNIQUE_KEY 与 SCD2

`INCREMENTAL_BY_UNIQUE_KEY`、`SCD_TYPE_2_BY_TIME` 和 `SCD_TYPE_2_BY_COLUMN` 必须声明：

```sql
physical_properties (transactional = true)
```

可选属性：

- `primary_key=(...)`：仅简单列；对于 UNIQUE_KEY 必须与 model unique key 相同。
- `write_bucket_num=N`：只用于 transactional table。
- 主键列按 MaxCompute 要求渲染 NOT NULL。

UNIQUE_KEY 复用 SQLMesh 原生 MERGE 路径。执行 MERGE 前通过 PyODPS 确认现有目标是 transactional table；默认 UPDATE 排除 primary key 和手工 partition 列，自定义 `when_matched` 保持不变。

两种 SCD2 复用 SQLMesh 全表 replace 算法，当前限定非分区 transactional table。硬删除失效、插入、更新、重复 apply 和临时表清理已有 Real 证据。

## 7. Materialized View

Materialized view 支持：

- lifecycle；
- 基于源分区的 partition；
- HASH cluster 和 bucket；
- comments；
- metadata object type 和 janitor 删除。

替换采用 drop+create，不使用也不声明 `CREATE OR REPLACE MATERIALIZED VIEW`。`disable_rewrite` 的语法渲染有单测，但 `york_fic` parser 拒绝该子句，因此该选项是 Unit/instance-dependent，不属于当前 Real 基线。

普通 `VIEW` 不接受 partition、lifecycle、cluster、rewrite 等 materialized/storage properties。adapter 在执行 DDL 前明确拒绝这些属性；只有 `materialized=True` 的创建路径可以消费它们。该拒绝契约为 Unit。

## 8. Connection 与 MaxQA

连接字段包括 `project`、`schema`、`endpoint`、AK/SK、STS token、tunnel endpoint、quota、SQL hints：

```yaml
execution_mode: offline  # offline | maxqa
maxqa_fallback_policy: none  # none | default | all
```

`execution_mode=maxqa` 必须配置 `quota_name`，并映射到 `use_sqa="v2"`。fallback 只由显式 policy 控制。配置和参数映射已有 Unit 测试；仓库提供受限于 `york_fic.sqlmesh` 的 gated read-only 测试，但因没有测试 quota 尚无 Real 证据。

## 9. Metadata 与读取

metadata 使用 PyODPS object API，不解析 `DESCRIBE` 文本：

- `columns()` 返回普通列、partition 列和复杂类型。
- `table_exists()` 使用 `exist_table()`，不吞掉非 not-found 异常。
- `_get_data_objects()` 区分 TABLE、VIEW、MATERIALIZED_VIEW。
- metadata 包含 `last_data_modified_time`。
- `_fetch_native_df()` 使用 cursor `description` 和 `fetchall()` 构造 Pandas DataFrame。
- RowDiff/TableDiff 复用公共 mixin；真机测试显式指定安全临时 schema。

DataFrame/Seed/Python model **写入**通过 `_df_to_source_queries` 明确失败；这不影响 SQL 查询结果读取为 Pandas DataFrame。

## 10. Lifecycle、Audit 与 State

真实 audit smoke 在 `york_fic.sqlmesh` 上通过 `Context.plan()` / `apply()` 部署 UUID view，读取真实结果，并验证 `Context.audit()` 对有效数据和 NULL 违规分别返回 `True` / `False`。

生命周期 smoke 使用一份本地 DuckDB state 文件，不是 SQLite，也未测试 PostgreSQL state。它在冻结 snapshot creation time 后覆盖：

- 初始和重复 plan/apply；
- 指定区间 manual restatement；
- `auto_restatement_cron` 和 prod `Context.run()`；
- dev environment invalidate 和 scoped janitor；
- 过期 snapshot 物理对象、snapshot state、interval state 清理；
- 第二次 janitor 幂等；
- MaxCompute `LIFECYCLE` metadata。

所有 `york_fic.sqlmesh` 真机测试：

- 记录运行前完整对象集合；
- 只创建和删除本次 UUID-scoped 前缀对象，以及 full replacement 产生的对应 `__temp_` 前缀临时表；
- finally cleanup 后要求对象集合完全恢复；
- 保留预创建的 `sqlmesh` schema；
- 永不调用 `Context.destroy()`。

## 11. Gated Real Verification

公共凭证变量：

```bash
export MAXCOMPUTE_ENDPOINT=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_ACCESS_KEY_ID=...
export MAXCOMPUTE_ACCESS_KEY_SECRET=...
```

| Gate | Test | Target |
| --- | --- | --- |
| `MAXCOMPUTE_NO_SCHEMA_SMOKE` | `test_maxcompute_no_schema_smoke_plan_apply` | `york_data`, schema unset |
| `MAXCOMPUTE_SCHEMA_SMOKE` | `test_maxcompute_schema_smoke_plan_apply` | schema-enabled isolated smoke |
| `MAXCOMPUTE_AUDIT_SMOKE` | `test_maxcompute_real_audit_execution` | `york_fic.sqlmesh` |
| `MAXCOMPUTE_CAPABILITY_SMOKE` | `test_maxcompute_schema_adapter_capabilities` | `york_fic.sqlmesh` |
| `MAXCOMPUTE_SCHEMA_EVOLUTION_SMOKE` | nested capability segment | project must enable evolution DDL |
| `MAXCOMPUTE_TRANSACTIONAL_SMOKE` | `test_maxcompute_transactional_models_plan_apply` | `york_fic.sqlmesh` |
| `MAXCOMPUTE_MODEL_KIND_SMOKE` | `test_maxcompute_additional_model_kinds_plan_apply` | `york_fic.sqlmesh` |
| `MAXCOMPUTE_LIFECYCLE_SMOKE` | `test_maxcompute_schema_lifecycle_restate_janitor` | `york_fic.sqlmesh` |
| `MAXCOMPUTE_MAXQA_SMOKE` | `test_maxcompute_maxqa_query` | read-only; also needs quota |

示例：

```bash
MAXCOMPUTE_PROJECT=york_fic \
MAXCOMPUTE_SCHEMA=sqlmesh \
MAXCOMPUTE_TRANSACTIONAL_SMOKE=1 \
.venv312/bin/python -m pytest \
  tests/core/engine_adapter/integration/test_integration_maxcompute.py::test_maxcompute_transactional_models_plan_apply -v
```

专用 harness 是安全边界的一部分。未接入 shared cloud harness 是有意设计决定：通用 harness 会创建 schema、写入 generic fixtures，并可能删除非 UUID 隔离对象，不满足业务账号测试约束。

## 12. 原始设计偏差与决策记录

下表保留 2026-06-27 最小设计与当前实现之间的演进，不回写为“最初就支持”。

| 原始设计 | 当前决策 | 原因/证据 |
| --- | --- | --- |
| 仅 `VIEW`、`FULL`、时间增量 | 新增分区、unmanaged、UNIQUE_KEY、SCD2、EXTERNAL、EMBEDDED | evaluator 与 MaxCompute 能力可可靠复用，均有 gated plan/apply。 |
| SCD2 不支持 | 非分区 transactional SCD2 支持 | 事务表 overwrite/MERGE 与 SQLMesh replace 算法真机通过。 |
| Materialized view 不支持 | 开启支持，replace 为 drop+create | evaluator plan/apply、lifecycle/partition/cluster/query 真机通过；janitor 类型路由为单测。 |
| 只允许简单 `partitioned_by` | time-range 新增显式 `TRUNC_TIME` 自动分区 | 避免方言猜测；`DATE(ds)` 继续拒绝；`INCREMENTAL_BY_PARTITION` 仍只支持简单手工列。 |
| comments 关闭 | 可由 `register_comments=true` 启用 | table/column metadata 真机通过；view/MV comment 渲染为单测。 |
| 仅 offline | 增加 MaxQA 配置和 gated query | MCQA V2 参数已单测；无 quota，暂不标 Real。 |
| schema-enabled smoke 创建随机 schema | 安全写测试固定预创建 `york_fic.sqlmesh` | default namespace 不可触碰；finally 恢复对象集合。旧 `MAXCOMPUTE_SCHEMA_SMOKE` 仍保留。 |
| 建议 Postgres state 真机组合 | 本轮只验证 DuckDB file state | 用户明确要求不做 PostgreSQL state 组合。 |
| 共享 integration harness | 使用专用 gated harness | 通用 schema/data 写入不满足真实业务 project 的安全边界。 |
| 分区 FULL drop→CTAS 特例 | 删除，恢复基类自引用临时表保护 | 避免自引用 replacement 破坏源表。 |

## 13. 已知限制与风险

- Full table 和 MV replacement 非原子，失败可能留下中间状态。
- Schema evolution 是否可用取决于 project 开关；`york_fic` 当前禁用。
- `DISABLE REWRITE` 在当前实例 parser 不可用。
- MaxQA 尚缺真实 quota 验证。
- SQLGlot 的 `maxcompute` alias 不是完整的一等方言；MaxCompute-specific SQL 依赖 adapter 显式实现。
- 自动分区、MV 和 transactional table 的组合受 MaxCompute 服务端约束，adapter 对未验证组合优先提前拒绝。
- 真实测试不包含 PostgreSQL state、多 catalog、grants 或业务 default schema。

## 14. Exit Criteria 状态

- [x] FULL、VIEW、MV、时间/分区/unmanaged/unique-key 增量、两种 SCD2 完成真机 plan/apply 验证。
- [x] EXTERNAL、EMBEDDED model-kind workflow 完成真机验证。
- [x] table/non-partition column comments、fetchdf、RowDiff/TableDiff、metadata、schema-enabled rename、非分区 truncate 完成真实能力验证。
- [x] 自动分区 time-range overwrite 校验真实 PyODPS generated expression/alias，使用 bounded condition 和 `LIFECYCLE 1` 临时全量 replacement 保留未受影响分区；两日 `york_fic` smoke 已再次通过。
- [x] 普通 View storage property 拒绝、no-schema 逻辑 namespace rename 边界和手工分区列 comment 渲染具备 Unit 证据。
- [x] no-schema 与 schema namespace 路径完成真实验证。
- [x] audit、restate、cron、run、janitor 和 lifecycle 完成 DuckDB state 真机验证。
- [x] DataFrame/Seed/Python writes、grants、MANAGED、WAP、clone、multi-catalog、MaxCompute state 和 atomic replace 明确保持不支持。
- [ ] Schema evolution 等待启用对应 project capability 后补充 Real 证据。
- [ ] MaxQA 等待可用 quota 后补充 Real 证据。
- [ ] `disable_rewrite` 等待目标实例支持该语法后补充 Real 证据。
