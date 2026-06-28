# SQLMesh MaxCompute Adapter 能力设计

## 当前实现状态（2026-06-28）

本设计已经落地为当前分支的 `MaxComputeEngineAdapter` 和 `MaxComputeConnectionConfig`。实现状态如下：

- 已注册 `type: maxcompute` execution connection、`maxcompute` dialect alias、adapter registry、`sqlmesh[maxcompute]` 可选依赖和 pytest marker。
- 已实现 `VIEW`、`FULL`、面向简单分区列的 `INCREMENTAL_BY_TIME_RANGE` 主路径。
- 已禁止 MaxCompute 作为 SQLMesh state sync backend；真实端到端 smoke 使用 MaxCompute 执行连接和 DuckDB state connection，生产仍建议使用独立 Postgres state backend。
- 已实现并单测固定三条硬约束：非法 CTAS 规避、动态分区列移动到投影末尾、`lifecycle` 渲染为 `LIFECYCLE n`。
- 已补齐非分区 FULL 既有表 replacement 的 `INSERT OVERWRITE TABLE target SELECT ...` 位置匹配路径，不渲染 overwrite 列清单。
- 已支持 no-schema namespace MaxCompute project：跳过 `CREATE/DROP SCHEMA`，将 SQLMesh 逻辑 schema 折叠进对象名，例如 `analytics.dim_customer` 渲染为 `analytics__dim_customer`。
- 已通过真实 MaxCompute no-schema project 的 `Context.plan()` + 两次 `Context.apply()` smoke，覆盖 FULL、分区增量、view/table 可读性和幂等 apply。
- 当前仅支持 `execution_mode: offline`；`maxqa` / MCQA 执行模式未实现且配置会被拒绝。
- 当前 `partitioned_by` 仅支持简单列引用，不支持 `DATE(ds)` 等 transform partition。

## 1. 概述

为 SQLMesh 增加内置 `maxcompute` engine adapter，使 SQLMesh 可以将离线 SQL 模型部署并执行到阿里云 MaxCompute/ODPS。

SQLMesh 元数据不存储在 MaxCompute 中。生产环境的元数据应通过独立的 `state_connection` 管理，推荐使用 Postgres；本地和集成 smoke 可使用 DuckDB。MaxCompute 只作为模型 SQL、audit 查询和物理表/视图操作的执行引擎。

第一版优先保证稳定的离线 SQL 主路径：

- 部署 `VIEW`、`FULL` 和面向分区的 `INCREMENTAL_BY_TIME_RANGE` 模型。
- 执行由模型作者编写的 MaxCompute/ODPS SQL。
- 使用 PyODPS 进行连接和元数据访问。
- 避免依赖 `DESCRIBE` 文本解析。
- 不承诺 MaxCompute 无法保证的事务或原子替换语义。

基于 PyODPS 与 dbt-maxcompute 参考实现的实现前校验，第一版还必须满足三条硬约束：

- 不生成 MaxCompute 非法的 `CREATE TABLE (cols) ... AS SELECT` 形态。
- 动态分区写入时，分区列必须位于 `SELECT` 投影末尾。
- `lifecycle` 必须渲染为 MaxCompute `LIFECYCLE n` 语法，不能落入普通 table properties。

## 2. 目标

adapter 应支持以下 SQLMesh 工作流：

- `sqlmesh plan` 可以为执行连接为 MaxCompute 的项目生成计划。
- `sqlmesh apply` 可以在 MaxCompute 中创建、替换和评估受支持的物理模型。
- SQLMesh snapshot 可以被评估为 MaxCompute 表或视图。
- audit 可以在 MaxCompute 结果表上执行。
- 表存在性检查、列发现、对象列表读取通过 PyODPS 元数据 API 完成。
- Postgres `state_connection` 可以与 MaxCompute execution connection 共存。

## 3. 非目标

第一版不支持：

- 在 MaxCompute 上进行 SQLMesh state sync。
- Python model。
- pandas DataFrame 写入。
- Materialized View。
- SCD Type 2 模型类型。
- grants 权限管理。
- 从任意 SQL 方言自动转换为 MaxCompute SQL。
- full table atomic replace 语义。
- MaxCompute project/user/role 等细粒度管理能力。

目标是可恢复、可重跑的离线执行，而不是事务型数仓语义。

## 4. 支持的 SQLMesh 模型类型

### VIEW

通过渲染 MaxCompute 兼容的 view DDL 支持。

当前实现：

- 使用 `CREATE OR REPLACE VIEW`。
- 该路径已在真实 no-schema MaxCompute smoke 中验证。
- 尚未实现按 project 能力自动回退到 `DROP VIEW IF EXISTS` 后接 `CREATE VIEW`。

第一版应关闭 comments 注册，因为 MaxCompute comment 支持和 SQLMesh comment 注册语义需要单独验证。

### FULL

通过模型查询创建物理表。

预期执行路径：

- 非分区表首次创建可使用不带显式列清单的 `CREATE TABLE ... AS SELECT ...`。
- 当需要显式列定义、分区定义、lifecycle 或 table properties 时，不使用 `CREATE TABLE (cols) ... AS SELECT`，而是采用两步法：
  - 先 `CREATE TABLE ... (cols) PARTITIONED BY (...) ...` 创建空表。
  - 再通过 `INSERT INTO` 或 `INSERT OVERWRITE TABLE ... PARTITION (...)` 写入数据。
- 替换时优先使用可恢复的流程：
  - 写入 SQLMesh 为 snapshot 管理的物理表名；或
  - 在 SQLMesh 生命周期已经提供安全物理表边界时使用 drop/create。

adapter 不能声明具备原子 `CREATE OR REPLACE TABLE` 语义。

### INCREMENTAL_BY_TIME_RANGE

当模型将时间范围映射到 MaxCompute 简单列分区写入时支持。

推荐模式：

```sql
INSERT OVERWRITE TABLE target PARTITION (partition_col) SELECT ...
```

第一版聚焦分区 overwrite 行为。delete/merge 型增量策略不在范围内，因为它们依赖的引擎能力和表格式不属于稳定的 MaxCompute 离线主路径。

动态分区写入必须保证分区列位于 `SELECT` 投影的末尾。adapter 在渲染分区 overwrite 前，应将 `partitioned_by` 中的列强制移动到投影末尾，避免 MaxCompute 将字段值写错到普通列或分区列。

## 5. SQL 操作

adapter 应支持以下操作：

- `CREATE SCHEMA`
- `DROP SCHEMA`
- `CREATE TABLE`
- `CREATE TABLE AS SELECT`
- `CREATE TABLE` 后接 `INSERT` 的两步建表写入
- `CREATE VIEW`
- `DROP TABLE`
- `DROP VIEW`
- `INSERT INTO`
- `INSERT OVERWRITE TABLE`
- `INSERT OVERWRITE TABLE ... PARTITION (...)`

当 SQLGlot 或 SQLMesh base adapter 行为不足以表达 MaxCompute 语法时，实现应显式渲染 MaxCompute 特定 SQL。

## 6. MaxCompute 表能力

第一版应支持：

- 普通表。
- 分区表。
- SQLMesh `partitioned_by`。
- MaxCompute `LIFECYCLE`。
- MaxCompute table properties。
- 通过 PyODPS hints 传递 SQL hints 和类似 `SET` 的执行选项。

分区列应渲染在 `PARTITIONED BY (...)` 中，而不是主字段列表中：

```sql
CREATE TABLE IF NOT EXISTS analytics.daily_orders (
  order_id BIGINT,
  amount DECIMAL(18, 2)
)
PARTITIONED BY (ds STRING)
LIFECYCLE 30
TBLPROPERTIES ('key'='value')
```

`LIFECYCLE` 必须作为 MaxCompute 建表语法渲染，而不是落入 `TBLPROPERTIES`。如果 SQLMesh 将模型的 `physical_properties` 作为 `table_properties` 传给 adapter，adapter 需要从中抽取 `lifecycle`，渲染为 `LIFECYCLE n`，并从剩余 table properties 中移除。

## 7. SQL 方言边界

第一版假设模型 SQL 使用 MaxCompute/ODPS 兼容 SQL 编写。

adapter 不承诺从 Spark、Hive、Postgres、Trino、DuckDB 或其他方言自动转写为 MaxCompute。SQLMesh 仍可使用 SQLGlot 做 AST 渲染和标识符引用，但 MaxCompute 特定 DDL/DML 必须由 adapter 显式负责。

方言实现有两个可选方向：

- 短期：注册一个本地 SQLGlot dialect alias，名称为 `maxcompute`，基于 Hive 渲染，并在 adapter 中覆盖 MaxCompute 特定 SQL 生成。
- 长期：在真实工作负载差异明确后，向 SQLGlot 贡献或维护一套一等公民的 `maxcompute` dialect。

第一版可以采用短期方案，因为目标是 MaxCompute SQL 执行稳定性，而不是跨方言兼容。

## 8. 架构设计

### 8.1 Engine Adapter

文件：

`sqlmesh/core/engine_adapter/maxcompute.py`

类：

`MaxComputeEngineAdapter`

核心类配置：

```python
DIALECT = "maxcompute"
SUPPORTS_TRANSACTIONS = False
SUPPORTS_REPLACE_TABLE = False
SUPPORTS_MATERIALIZED_VIEWS = False
SUPPORTS_GRANTS = False
INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
```

catalog 支持应保守处理：

- `project` 映射为 SQLMesh default catalog。
- 如果目标环境启用了 schema namespace，SQLMesh schema 映射为 MaxCompute schema。
- 如果目标环境未启用 schema namespace，adapter 跳过 `CREATE/DROP SCHEMA`，并将 SQLMesh schema 折叠进表/视图名，例如 `analytics.orders` -> `analytics__orders`。
- 第一版不承诺跨 project 引用能力。

### 8.2 Connection Config

文件：

`sqlmesh/core/config/connection.py`

类：

`MaxComputeConnectionConfig`

字段：

- `type: "maxcompute"`
- `project: str`
- `schema: Optional[str]`
- `endpoint: str`
- `access_key_id: Optional[str]`
- `access_key_secret: Optional[str]`
- `security_token: Optional[str]`
- `tunnel_endpoint: Optional[str]`
- `quota_name: Optional[str]`
- `execution_mode: Literal["offline"]`
- `sql_hints: Dict[str, str]`

默认值：

- 第一版 `concurrent_tasks = 1`。
- `register_comments = False`。
- `pre_ping = False`。

连接行为：

- 使用 PyODPS DBAPI 执行 SQL。
- 使用 PyODPS 对象 API 读取元数据。
- 懒加载 PyODPS，使配置解析和文档工具不要求安装 MaxCompute 依赖。
- 在 PyODPS cursor 支持时，将 `sql_hints` 传递给查询执行。
- STS 凭证通过 `security_token` 构造 PyODPS `StsAccount`。

state sync 行为：

- 将 `maxcompute` 加入禁止作为 state sync engine 的集合。
- 文档中应展示 MaxCompute execution connection 与独立 state connection 的组合配置。

### 8.4 Schema Namespace 规则

adapter 运行时调用 `odps.is_schema_namespace_enabled()` 判断目标 project 是否启用 schema namespace。如果该调用失败，默认按启用 schema namespace 处理，避免误跳过合法 schema 行为。

schema namespace 启用时：

- 表名、视图名和 metadata API 保持 `project.schema.object` 语义。
- `CREATE SCHEMA` / `DROP SCHEMA` 会发送给 MaxCompute。

schema namespace 未启用时：

- `CREATE SCHEMA` / `DROP SCHEMA` 直接 no-op。
- DDL/DML 中清空 table 的 `catalog` 和 `db`，将 `db + name` 折叠为单段对象名。
- 逻辑对象 `analytics.dim_customer` 渲染为 `` `analytics__dim_customer` ``。
- 物理对象 `warehouse.sqlmesh__analytics.analytics__fact_order_daily__1234` 渲染为 `` `sqlmesh__analytics__analytics__fact_order_daily__1234` ``。
- PyODPS metadata API 调用 `list_tables(project=project)` / `get_table(name, project=project, schema=None)`。
- `_get_data_objects(..., object_names=...)` 会用折叠后的真实对象名过滤，再返回 SQLMesh 期望的未折叠对象名。

### 8.3 Adapter 注册

文件：

`sqlmesh/core/engine_adapter/__init__.py`

映射：

```python
"maxcompute": MaxComputeEngineAdapter
```

可选依赖：

```toml
maxcompute = ["pyodps"]
```

## 9. Adapter 方法要求

### 9.1 DDL

adapter 应在必要时覆盖或定制表/视图 DDL 生成：

- `create_table`
- `ctas`
- `create_view`
- `drop_table`
- `drop_view`
- `create_schema`
- `drop_schema`

建表必须支持：

- 主字段。
- 分区字段。
- lifecycle。
- table properties。

MaxCompute CTAS 约束：

- 不生成 `CREATE TABLE t (cols) AS SELECT ...`。
- 不生成 `CREATE TABLE t (cols) PARTITIONED BY (...) AS SELECT ...`。
- 非分区 CTAS 只能在不需要显式列清单和物理属性时使用。
- 分区表、带 lifecycle 的表、带 table properties 的表统一走两步法：先建空表，再写入数据。

分区字段校验：

- 第一版 `partitioned_by` 只接受简单列引用。
- 每个分区列必须存在于 `target_columns_to_types` 中。
- 分区列必须从主字段定义列表中移除。
- 动态分区写入时，分区列必须被移动到 `SELECT` 投影末尾。
- 单元测试必须固定这一投影顺序，不能只断言 SQL 字符串大致形状。

physical properties 处理：

- `lifecycle` 从 SQLMesh 传入的 physical/table properties 中抽取。
- 抽取后渲染为 `LIFECYCLE n`。
- `lifecycle` 不再作为普通 `TBLPROPERTIES` 项输出。
- 其他 table properties 保留为 `TBLPROPERTIES ('k'='v')`。

### 9.2 DML

append 路径：

```sql
INSERT INTO target SELECT ...
```

overwrite 路径：

```sql
INSERT OVERWRITE TABLE target SELECT ...
```

分区 overwrite 路径：

```sql
INSERT OVERWRITE TABLE target PARTITION (ds) SELECT ...
```

分区 overwrite 是 time-range incremental 模型的关键路径。对于动态分区，`SELECT` 的最终投影顺序必须是普通列在前、分区列在后，例如：

```sql
INSERT OVERWRITE TABLE analytics.fact_order_daily PARTITION (ds)
SELECT
  order_id,
  customer_id,
  amount,
  ds
FROM staging.orders;
```

adapter 不能直接沿用 `target_columns_to_types` 的字典顺序，因为该顺序不一定满足 MaxCompute 动态分区要求。

### 9.3 INCREMENTAL_BY_TIME_RANGE 路由要求

设计目标是让分区化 `INCREMENTAL_BY_TIME_RANGE` 模型进入 MaxCompute 分区 overwrite 路径，而不是退化为基于 `WHERE` 条件的 delete/insert 或普通 insert overwrite。

实现和验证必须覆盖：

- 当模型同时声明 `time_column ds` 和 `partitioned_by [ds]` 时，SQLMesh evaluation path 能路由到 `insert_overwrite_by_partition` 或等价的 adapter 分区 overwrite 方法。
- 如果 SQLMesh 默认路由进入 `_insert_overwrite_by_condition`，adapter 需要显式接管该路径，基于模型分区信息生成 `INSERT OVERWRITE TABLE ... PARTITION (...)`。
- Phase 2 smoke test 必须验证真实运行时写入的是目标分区，而不是全表 overwrite 或非分区条件覆盖。

### 9.4 元数据

adapter 应避免解析 `DESCRIBE` 返回的文本。

必需元数据方法：

- `columns(table_name)`
  - 使用 `odps.get_table(...).table_schema`。
  - 包含普通列和分区列。
  - 将 MaxCompute 类型转换为 SQLMesh/SQLGlot 数据类型。

- `table_exists(table_name)`
  - 当前实现使用 `odps.exist_table(table, project=..., schema=...)`。
  - 不通过宽泛 `except Exception` 吞掉真实 PyODPS 错误。

- `_get_data_objects(schema_name, object_names=None)`
  - 使用 PyODPS table listing API。
  - 将表映射为 `DataObjectType.TABLE`。
  - 将虚拟视图映射为 `DataObjectType.VIEW`。
  - 不声明支持 materialized view。

## 10. 配置示例

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

## 11. 模型示例

### 11.1 Full Model

```sql
MODEL (
  name analytics.dim_customer,
  kind FULL,
  dialect maxcompute
);

SELECT
  customer_id,
  customer_name,
  updated_at
FROM raw.customer;
```

### 11.2 分区增量模型

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

当 SQLMesh 评估一个时间窗口时，adapter 应将写入渲染为 MaxCompute 分区 overwrite。

## 12. 失败与重跑语义

对于该 adapter，MaxCompute 视为非事务引擎。

adapter 应遵循以下原则：

- 优先使用 `CREATE TABLE IF NOT EXISTS` 和 `DROP ... IF EXISTS` 等幂等 DDL。
- 增量模型优先使用分区 overwrite。
- 当单条 MaxCompute SQL 能完成目标时，避免使用多步骤操作引入不明确的最终状态。
- 不调用 rollback。
- DDL 成功后清理 SQLMesh 对象缓存。
- 依赖 SQLMesh retry/rerun 使用相同物理对象名，并覆盖相同分区范围。

已知非原子区域：

- full table replacement 不保证原子性。
- 如果不使用 `CREATE OR REPLACE VIEW`，drop/create view replacement 中间会有短暂空窗。
- CTAS 失败时是否留下部分对象取决于 MaxCompute 行为。

这些限制必须通过文档明确说明，而不是用兼容分支隐藏。

## 13. 实施阶段

### Phase 1：最小内置 Adapter

状态：已完成。

交付内容：

- `MaxComputeConnectionConfig`
- `MaxComputeEngineAdapter`
- adapter registry entry
- optional dependency metadata
- config、注册、DDL/DML 渲染、metadata mock 的单元测试
- 分区表两步建表写入测试
- 动态分区列投影末尾测试
- `lifecycle` 从 physical/table properties 抽取并渲染为 `LIFECYCLE` 的测试
- 本设计文档

目标结果：

- SQLMesh 可以基于 mock MaxCompute 行为创建和评估受支持的表/视图模型。
- Phase 1 不能以非法 CTAS 字符串的 mock 断言作为完成标准。

### Phase 2：真实 MaxCompute Smoke Test

状态：已完成 no-schema project 覆盖。

交付内容：

- 内部集成 fixture：`tests/core/engine_adapter/integration/test_integration_maxcompute.py`。
- 使用真实 MaxCompute 凭证的 gated smoke test。
- smoke 使用 DuckDB state connection，避免测试依赖外部 Postgres。
- smoke 覆盖 no-schema namespace project；schema namespace enabled project 会 skip。

验收标准：

- `sqlmesh plan` 成功。
- `sqlmesh apply` 成功。
- 配置的 MaxCompute project 下生成预期表/视图；no-schema project 中对象名按 `schema__table` 折叠。
- 对受支持模型重复执行同一 apply 具备幂等性。
- 分区增量模型确认走 `INSERT OVERWRITE TABLE ... PARTITION (...)`。
- 真实数据验证分区列未错位，分区值落入预期分区。

### Phase 3：增量增强

交付内容：

- 更精确的 PyODPS 异常处理。
- 基于真实 schema fixture 继续补充类型映射边界。
- 更完整的 schema/project 行为校验。
- 面向分区增量模型的文档指引。

可选后续工作：

- 一等公民的 SQLGlot MaxCompute dialect。
- 经能力验证后支持更多模型类型。
- 更丰富的 MaxCompute 认证模式。
- MaxQA/MCQA execution mode。
- schema namespace enabled project 的真实端到端 smoke。

## 14. 测试计划

### 单元测试

connection/config：

- 解析 `type: maxcompute`。
- 校验默认值。
- 校验 `project` 映射到 `get_catalog()`。
- 校验 `schema` alias。
- 校验 `register_comments` 为 false。
- 校验 `maxcompute` 禁止作为 state sync engine。
- 校验 adapter 创建时传递 `sql_hints`。

adapter 注册：

- `create_engine_adapter(..., dialect="maxcompute")` 返回 `MaxComputeEngineAdapter`。

DDL 渲染：

- 普通表 `CREATE TABLE`。
- 分区表 `CREATE TABLE`。
- 带 lifecycle 的 `CREATE TABLE`。
- 带 table properties 的 `CREATE TABLE`。
- 非分区 CTAS 不带显式列清单。
- 分区表不使用 CTAS，而是先建空表再执行分区写入。
- 带 lifecycle/table properties 的表不使用 `CREATE TABLE (cols) AS SELECT`。
- view creation 路径。

DML 渲染：

- `INSERT INTO`。
- `INSERT OVERWRITE TABLE`。
- `INSERT OVERWRITE TABLE ... PARTITION (...)`。
- 动态分区写入时，`partitioned_by` 列位于 `SELECT` 投影末尾。
- `lifecycle` 不进入 `TBLPROPERTIES`。

元数据：

- `columns()` 读取普通列。
- `columns()` 读取分区列。
- `table_exists()` true/false 路径。
- `_get_data_objects()` 表/视图映射。

非事务行为：

- adapter 不调用 rollback。
- 多 source 操作不假设事务支持。

### SQLMesh 行为测试

- `VIEW` 模型部署。
- `FULL` 模型部署。
- `INCREMENTAL_BY_TIME_RANGE` 模型写入一个分区范围。
- `INCREMENTAL_BY_TIME_RANGE` 分区模型确认路由到 MaxCompute 分区 overwrite。
- audit SQL 执行并读取结果行。
- Postgres state connection 与 MaxCompute execution connection 可共同配置。

### 集成验收

针对真实 MaxCompute project：

- `sqlmesh plan` 完成。
- `sqlmesh apply` 完成。
- 预期物理表/视图被创建。
- 重复 apply 不产生非预期变更。
- 失败运行可清理并重跑。
- no-schema namespace 项目不发送非法 `CREATE SCHEMA`，对象名按 `schema__table` 规则折叠。

## 15. 风险与缓解

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| SQLGlot 没有原生 MaxCompute dialect | SQL 渲染存在缺口 | 使用本地 `maxcompute` alias，并在 adapter 中显式渲染 DDL/DML |
| 生成非法 CTAS 语法 | 真机执行直接失败 | 禁止 `CREATE TABLE (cols) AS SELECT`；分区/带物理属性的表走两步法 |
| 动态分区列不在投影末尾 | 数据列或分区值错位 | adapter 写入前重排投影，强制普通列在前、分区列在后 |
| lifecycle 被当作普通 table property | 建表生命周期失效 | 从 physical/table properties 中抽取并渲染为 `LIFECYCLE n` |
| 非事务执行 | 失败后可能留下部分对象 | 优先使用幂等 DDL 和分区 overwrite，并文档化非原子区域 |
| PyODPS DBAPI cursor 行为与 SQLMesh 假设不一致 | 运行时失败 | metadata 使用 PyODPS object API；Phase 2 用真实环境验证执行 |
| 类型映射存在边界缺口 | 少数复杂类型解析错误 | PyODPS `odps_type.name` 可覆盖 decimal/array/map 等常见复杂类型；继续用真实 schema fixture 补边界 |
| schema/project 语义因 MaxCompute 环境差异而变化 | 对象解析错误 | 将 `project` 视为 catalog；schema namespace 关闭时折叠对象名；no-schema 路径已用真实账号验证 |
| 增量模型未与分区对齐或未路由到分区 overwrite | overwrite 语义不安全 | 明确文档化仅支持面向分区的增量路径，并在 Phase 2 验证实际路由 |

## 16. 验收标准

第一版 adapter 达到可用标准时应满足：

- 代码暴露 `type: maxcompute` 作为合法 execution connection。
- SQLMesh state 仍通过 Postgres 或其他受支持的 state backend 配置。
- 受支持模型类型可以 plan 和 apply。
- table、view、append、partition overwrite 的 MaxCompute DDL/DML 渲染正确。
- 不生成 MaxCompute 非法 CTAS 形态。
- 动态分区写入保证分区列在投影末尾。
- `lifecycle` 正确渲染为 MaxCompute `LIFECYCLE` 语法。
- 元数据读取使用 PyODPS API，而不是 `DESCRIBE` 文本解析。
- 单元测试覆盖第一版能力契约。
- 真实 MaxCompute smoke test 验证端到端执行。
- no-schema namespace project 下 `Context.plan()` + repeated `Context.apply()` 成功。

## 17. 开放问题

- `CREATE OR REPLACE VIEW` 已在当前真实 no-schema smoke project 验证，但是否覆盖所有 MaxCompute project 仍需更多环境验证；当前未实现 drop/create fallback。
- `execution_mode = "maxqa"` / MCQA 需要不同的 PyODPS execution path；当前配置层拒绝该模式。
- struct 等复杂 MaxCompute 类型已有基础单测覆盖，但仍需要真实 schema fixture 扩展边界。
- `lifecycle` 除了从 `physical_properties/table_properties` 抽取外，是否还需要专用模型属性。
- schema namespace enabled project 需要补充真实端到端 smoke。
