# Issue 46：全类型兼容的离线、实时和版本增量资格测试

## 研究范围

本记录服务于 [架构地图：MySQL 与 PostgreSQL 全类型兼容](https://github.com/songyuhao95/database_sync_tool/issues/39) 的票据“研究：全类型兼容的离线、实时和版本增量资格测试”。研究对象是 SourceTypeMapping、ChangeEvent、Sink Capability Manifest、ColumnConversionPlan、运行时事务和能力失效的测试证据。

## 当前测试体系事实

仓库当前已经把不同证据层分开：

- [`tests/qualification_matrix.rs`](../../tests/qualification_matrix.rs) 生成 6×6 离线方向矩阵；Source fixture 和 Sink renderer 分开验证，PG16/17 作为明确 `UNSUPPORTED`。
- [`scripts/qualify.ps1`](../../scripts/qualify.ps1) 将离线方向、4 个 SourceAdapter live suite、4 个 SinkAdapter live suite、公共事务恢复和 representative route smoke 分开记录。
- [`scripts/qualification-matrix.json`](../../scripts/qualification-matrix.json) 是数据库/connector roster 和 suite 配置来源。
- [`tests/support/matrix_fixture.rs`](../../tests/support/matrix_fixture.rs) 在 SourceAdapter/SinkAdapter 公共 seam 上生成可回放 fixture。
- [`crates/web_ui/src/qualification_recovery_tests.rs`](../../crates/web_ui/src/qualification_recovery_tests.rs) 覆盖完整事务、回滚、重复投递、CommitUnknown、重启和停止恢复。

这套语义是正确的：Source live PASS 证明原生日志能进入统一模型，Sink live PASS 证明目标适配器能执行统一模型，不能把二者伪装成每条 Source→Sink 都有真实数据库链路证据。全类型扩展后必须保留这种证据分层。

## 全类型测试资格层级

每个 native type declaration 和每个目标表示都要有机器可读的 qualification record，至少包含：

```text
source connector identity / server build
sink connector identity / server build
native source declaration + definition fingerprint
logical type fingerprint
target representation + target definition fingerprint
conversion rule / plan digest
qualification level
loss statement
locator safety
required extensions and session profile
fixture values and expected values
```

资格级别不是简单 PASS/FAIL：

| 级别 | 必须证明的事实 | 可否进入普通同步 |
|---|---|---|
| `EXACT` | 目标表示和源语义等价，边界和特殊值一致 | 可以自动放行 |
| `RANGE_CHECKED` | 目标表示更窄或参数不同，但所有值域/精度/长度检查已固定 | 可以，按计划运行；失败整事务回滚 |
| `EXPLICIT_CONVERSION` | 值或结构可按明确规则转换，但数据库行为有声明损失 | 必须逐字段确认；主键/Row Locator 默认禁止 |
| `VALUE_PRESERVED` | 原始值/结构可恢复，但目标原生操作语义不保留 | 必须明确确认；不能宣称原生等价 |
| `UNSUPPORTED/BLOCKED` | 无法证明值或必要语义可保存 | 不允许选择、创建或启动 |

`VALUE_PRESERVED` 是全类型目标新增的展示/资格标签；如果沿用现有四级枚举，也必须在 `EXPLICIT_CONVERSION` 的规则和风险解释中明确区分“可恢复值承载”和“语义转换”。

## 离线 fixture 矩阵

### Source fixture 类别

每个已实现 Source 版本至少需要一套固定定义 fixture 和一套真实协议捕获回放 fixture：

1. 标量：所有整数宽度及 signed/unsigned、Decimal precision/scale、float、BIT、文本/二进制、日期时间；
2. 特殊枚举：ENUM 标签、SET 空集合/单成员/多成员/声明顺序变化/未知成员/重复成员；
3. JSON：NULL、布尔、负数、大整数、精确小数、数组、对象、深层嵌套、重复键策略；
4. 容器：一维/多维数组、非一基下界、元素 NULL、空数组；composite 字段顺序、嵌套和 NULL；Map key/value/NULL；
5. Range：空范围、无限边界、左右包含性、相邻范围、canonicalization；MultiRange 的空值、顺序和合并；
6. Spatial：WKB/EWKB、每种 geometry subtype、XY/XYZ/XYM/XYZM、SRID/CRS、空 geometry 和无效值；
7. 扩展/自定义：domain 约束、自定义 base type、PostGIS geometry/geography、hstore、其他已注册能力；
8. 特殊值：MySQL zero date、负 TIME、NaN/Infinity、二进制 NUL、字符编码边界、超长值和非法字节。

### Sink fixture 类别

每个 Sink 对每种 fixture 都要分别检查：

- native equivalent：目标类型、目标约束、比较和索引行为；
- value-preserving：BYTEA/TEXT/JSONB 或结构化承载是否可逆恢复；
- explicit conversion：计划参数、示例结果、损失说明和确认门控；
- unavailable/unchanged/null presence；
- 参数化绑定，不使用拼接 SQL；
- 一个 Transaction Batch 内多表、多行、多字段的原子应用；
- 失败后目标业务数据和 checkpoint 均不变。

### 每条 fixture 的最小断言

```text
source declaration -> expected LogicalType
native wire value -> expected LogicalValue
roundtrip JSON -> same semantic ChangeEvent
plan -> expected qualification / risk / loss statement
rendered parameter -> target driver type/value
target read-back -> expected value or declared conversion result
invalid value -> Target Capability Failure, rollback, checkpoint unchanged
```

## 6×6 方向矩阵

保留 MySQL 5.7、8.0、8.4 和 PostgreSQL 15、16、17 的 36 个 Source×Sink 方向，但每个方向内部按以下层次生成证据：

1. Source fixture compatibility：源 fixture 是否能由 SourceTypeMapping 生成；
2. Sink manifest qualification：目标能力清单是否为该 LogicalType 提供目标表示；
3. Plan qualification：ColumnConversionPlan 是否固定且摘要稳定；
4. Offline render/apply：离线 fixture 是否得到目标参数或明确 `UNSUPPORTED/BLOCKED`；
5. Recovery: 计划转换失败、约束失败、CommitUnknown、重复投递和重启是否保持事务/Checkpoint 语义；
6. Live component evidence：实际数据库 SourceAdapter 和 SinkAdapter 是否在各自 live suite 中通过。

PG16/17 当前未实现时，36 个方向仍必须出现在报告中并明确 `UNSUPPORTED`，不能丢失方向或误报 PASS。新增 PostgreSQL 16/17 connector 后，既要补 Source fixture，也要补 Sink fixture 和 live component suite。

## Live qualification 语义

真实数据库测试应继续采用 4 Source + 4 Sink + 公共 recovery + representative route smoke 的结构：

- 4 Source live：各版本读取原生日志并生成 ChangeEvent；
- 4 Sink live：每个目标适配器消费四个 source fixture roster；
- common recovery live/offline：事务原子性和 checkpoint 恢复；
- route smoke：少量真实 Source→Sink 端到端路径，作为运行链路证据，不替代完整 6×6 类型矩阵。

每个扩展类型另加 capability suite：

- extension unavailable；
- extension available but not installed；
- extension installed in another database；
- installed version matches qualification；
- version/schema/target definition changed after plan creation；
- reader lacks catalog privilege；
- writer lacks target type/function privilege。

报告必须保留 `PASS`、`FAIL`、`REQUIRES_LIVE`、`UNSUPPORTED` 和 `MISSING_TEST` 的差异。`REQUIRES_LIVE` 不能被转写为 PASS；`UNSUPPORTED` 只能在 connector 明确声明未实现且离线结果同样为 unsupported 时使用。

## 全类型恢复和错误注入

所有 `EXACT`、`RANGE_CHECKED`、`EXPLICIT_CONVERSION` 和 `VALUE_PRESERVED` 计划都必须进入相同的 runtime fault matrix：

1. source transaction begin/commit 边界；
2. 多表、多行和跨字段转换；
3. value conversion failure；
4. target constraint/type/function failure；
5. checkpoint write failure；
6. CommitUnknown/Applied、NotApplied、Unprovable；
7. process stop before/after target commit；
8. duplicate delivery and restart；
9. stale plan digest/revision/extension fingerprint；
10. target capability disappears during activation。

每个失败都必须证明：完整 Sink Apply Transaction 回滚、Replication Metadata/CDC checkpoint 不推进、日志保留 source cursor 和可读的损失/失败原因。只有目标权威 metadata 能证明提交已发生时，才允许把 CommitUnknown 解析为 Applied。

## 版本增量规则

新增一个数据库版本或扩展能力时，固定增加：

- 1 个 Source fixture roster；
- 1 个 Sink capability roster；
- 与现有 N 个实现 connector 的 `2N + 1` 个离线方向证据（新 Source→旧 Sinks、旧 Sources→新 Sink、新→新）；
- 1 个 Source live suite；
- 1 个 Sink live suite；
- 1 个扩展/环境探测 suite（如果是扩展能力）；
- 1 组 runtime recovery suite；
- qualification-matrix.json roster 和报告 schema 更新。

新增类型而不是新增版本时，必须为所有受影响 Source×Sink 方向补齐该类型的 fixture 和资格记录；不能只添加一个源适配器单测。

## 当前缺口

- `tests/qualification_matrix.rs` 的 fixture 目前主要覆盖标量、JSON、ENUM/SET、BIT、时间和部分类型边界；数组、composite、domain、range/multirange、扩展空间和 hstore 需要新增 canonical fixtures。
- `scripts/qualify.ps1` 当前正确区分 6×6 离线与 4+4 live component evidence，但还没有扩展能力探测报告字段。
- PostgreSQL 15 live worker 仍是当前已实现 PostgreSQL connector，PG16/17 仍为明确 unsupported。
- 当前 `SinkAdapter` 对 SET/Spatial 的阻断测试是正确的 fail-closed 基线；完成新地图时必须将它们升级为目标表示和显式资格测试，而不是删除阻断逻辑。

## 研究依据

- [`scripts/qualify.ps1`](../../scripts/qualify.ps1)
- [`scripts/qualification-matrix.json`](../../scripts/qualification-matrix.json)
- [`tests/qualification_matrix.rs`](../../tests/qualification_matrix.rs)
- [`tests/support/matrix_fixture.rs`](../../tests/support/matrix_fixture.rs)
- [`crates/web_ui/src/qualification_recovery_tests.rs`](../../crates/web_ui/src/qualification_recovery_tests.rs)
- [PostgreSQL 17 CREATE EXTENSION](https://www.postgresql.org/docs/17/sql-createextension.html)
- [MySQL 8.4 Data Types](https://dev.mysql.com/doc/refman/8.4/en/data-types.html)
