# 研究：现有适配器和 Web 字段映射的兼容性缺口

研究范围是 MySQL 5.7、8.0、8.4 与 PostgreSQL 15 的 DML 增量同步、已存在目标表和当前任务创建页面。本记录只新增研究文档，不修改业务代码。

## 结论摘要

1. 当前系统有两层互不相同的兼容检查：Web 目录层以数据库原生类型字符串返回一个 `bool`，Sink 运行时再对每个 ChangeEvent 值做参数化渲染和能力检查。两层没有共享结构化结果，也没有共享 `ColumnConversionPlan`。
2. `change_event` 已经提供了足够好的公共事件边界：`NativeType`、`LogicalValue`、值存在状态、主键 ordinal 和 `ValidatedTransaction`。核心校验故意不检查目标能力；目标能力属于 Sink。这部分可以直接作为兼容方案的输入。
3. 四个 Sink 都实现了 `CapabilityManifest`、`qualify` 和 `plan`，并包含可复用的值绑定、范围/精度/字符集/时间检查。可是 `qualify` 只是完整 SQL 计划的丢弃结果，错误只有一条字符串；没有按列解释、风险等级、转换规则或策略参数。
4. MySQL 5.7/8.0/8.4 的 Sink 代码和能力面几乎相同，三者均不声明 `boolean`/`uuid`，运行时也明确拒绝这两种 LogicalValue。相反，Web 的 PostgreSQL → MySQL 映射接受 `boolean → tinyint`、`uuid → char(36)/varchar(36)`，因此当前页面可以把运行时必然失败的字段表现为可选。
5. 任务只持久化同名表、同名列的选择列表、数据库名、实例 revision 和起点模式。没有兼容策略、列绑定指纹、目标能力/规则版本、转换计划摘要或目标 Schema Fingerprint；实例 revision 变化能使任务失效，但目录或能力变化不会单独使它失效。
6. 当前“不可选字段”并非全都是真正禁止：源契约无效、缺主键、缺 row locator、目标结构不满足和无法保真的语义必须阻止；整数宽度/范围、十进制参数、精度和某些字符集/时间策略可成为显式资格化的兼容选项候选，但必须通过目标能力、风险分类和不可变计划落盘后才能放行。

## 证据和可复用代码

### `change_event` 公共边界

`CapabilityManifest` 目前只有 connector、target、contract、支持的 LogicalType 名称、presence 和是否要求主键；`SinkAdapter` 暴露 `capability_manifest`、`qualify` 和 `plan`，`SourceAdapter` 只负责产生事务（[lib.rs](../../crates/change_event/src/lib.rs#L18-L49)）。这正好提供了 Source/Sink 独立组合的扩展点，但能力描述仍是无参数字符串集合。

`ColumnDatum` 保留 ordinal、名称、`native_type`、主键 ordinal、generated 和 collation；`Datum` 区分 `Unavailable`、`Unchanged`、`Null`、`Value`，`LogicalValue` 已覆盖整数 signed/bits、Decimal unscaled/scale、IEEE 浮点位、带 charset 的文本、二进制、日期时间、Duration、Instant、Year 和 JSON（[model.rs](../../crates/change_event/src/model.rs#L80-L170)）。`ValidatedTransaction` 的公共只读入口与核心校验可复用，但校验明确保持 vendor-neutral，不替代目标能力检查（[validate.rs](../../crates/change_event/src/validate.rs#L52-L80)）。

核心校验还允许 keyless row 进入目标检查，以便把“事件形状合法”和“目标不能定位行”区分开（[change_event/src/tests.rs](../../crates/change_event/src/tests.rs#L152-L156)）。因此兼容方案不应把目标主键要求塞回公共 validator。

### Source 侧定义和解码

MySQL 8.4 decoder 的 `ColumnInfo` 已读取精确 `COLUMN_TYPE`、`DATA_TYPE`、字符集、排序规则、生成表达式和主键 ordinal，并在 row image 中把 native type 与 LogicalValue 一起写入 `ColumnDatum`（[decoder.rs](../../crates/mysql_8_4/src/decoder.rs#L20-L34)、[decoder.rs](../../crates/mysql_8_4/src/decoder.rs#L145-L176)、[decoder.rs](../../crates/mysql_8_4/src/decoder.rs#L358-L395)）。值解码保留 MySQL 整数 signedness/宽度，并拒绝不能成为完整 ChangeEvent 的 partial JSON image（[decoder.rs](../../crates/mysql_8_4/src/decoder.rs#L398-L432)）。5.7/8.0 decoder 沿用同一契约形状；未来的 SourceTypeMapping 可从这里抽出，但本票据不改实现。

PostgreSQL 15 decoder 以 OID allowlist 选择受支持类型，读取 boolean、整数、numeric、IEEE 浮点、UTF-8 文本、bytea、日期时间、uuid 和 jsonb（[types.rs](../../crates/postgresql_15/src/types.rs#L6-L26)、[types.rs](../../crates/postgresql_15/src/types.rs#L28-L65)）。PG source contract 还检查主键、ordinal、generated、presence、native type 以及 native/logical 对应关系（[source_contract.rs](../../crates/postgresql_15/src/source_contract.rs#L86-L124)）。这些是源端的硬边界，不应由“目的端兼容选项”放宽。

### MySQL 5.7/8.0/8.4 Sink

三套 Sink 都把 manifest 映射到同一组 LogicalType：integer、decimal、float、text、binary、date、local_datetime、duration、instant、year、json；均要求主键，但没有 boolean/uuid（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L32-L53)、[mysql_8_0/sql.rs](../../crates/mysql_8_0/src/sql.rs#L32-L53)、[mysql_8_4/sql.rs](../../crates/mysql_8_4/src/sql.rs#L32-L53)）。`qualify` 与 `plan` 都调用 `sql`，所以现在没有便宜的列级预检接口（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L68-L83)）。三者主要只在 connector/target version、连接版本校验和文件细节上区分；研究结论不能把三版当作未经验证的同一能力身份。

已有的执行前能力检查很有复用价值：

- `sql` 遍历完整事务，检查每个 change，再生成参数化 INSERT/UPDATE/DELETE 计划；UPDATE/DELETE 会生成主键 probe（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L165-L205)）。
- 生成列和 `Unavailable`/`Unchanged` 会被排除出可写列；主键缺失、不可用值作 locator、空 UPDATE 都是能力失败（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L395-L426)、[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L429-L475)）。
- `bind_logical_value` 已包含整数解析、DECIMAL 65/30 限制、非有限浮点拒绝、UTF-8/UTF-8MB4 限制、Duration 范围和 Instant 微秒精度限制；boolean/uuid 直接以 `Target Capability Failure` 拒绝（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L478-L598)）。
- `ensure_supported_change` 会检查 before/after 的值和主键，但只依据事件值，不读取目标列的结构化类型定义（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L767-L801)）。

结论是：这些函数可成为未来 Sink Conversion Planner 的值域验证器和 SQL 参数绑定器；不能直接当作 activation-time 的列计划，因为它们现在按事件逐次检查且没有目标列参数。

### PostgreSQL 15 Sink

PG15 manifest 声明 boolean、uuid、integer、decimal、float、text、binary、date、local_datetime、instant、duration、year、json，仍要求主键；`qualify` 同样只是丢弃 `sql` 结果（[sql.rs](../../crates/postgresql_15/src/sql.rs#L13-L36)、[sql.rs](../../crates/postgresql_15/src/sql.rs#L51-L65)）。

它提供了更完整的参数化类型分支：boolean/uuid、unsigned integer 大于 `i64` 时转 numeric、Decimal、浮点、文本 UTF-8 检查、bytea、时间、interval、Instant 微秒精度和 JSON（[sql.rs](../../crates/postgresql_15/src/sql.rs#L494-L595)）。它还会拒绝没有 generated observation 的 generated column，而 MySQL Sink 的 `writable_columns` 只跳过 generated 列；这说明“generated 是否可兼容”必须进入结构化 plan，而不能仅由页面的字符串映射决定（[sql.rs](../../crates/postgresql_15/src/sql.rs#L469-L492)）。

### Registry、catalog 和 Web 任务校验

`SourceRegistry`/`SinkRegistry` 是独立目录，精确按 kind/version 查找，不含 source-to-sink pair entry；Sink descriptor 从 adapter manifest 投影出宽泛 LogicalType/presence 能力（[registry.rs](../../crates/web_ui/src/registry.rs#L22-L40)、[registry.rs](../../crates/web_ui/src/registry.rs#L99-L119)、[registry.rs](../../crates/web_ui/src/registry.rs#L178-L216)）。这是新增数据库版本时避免 pair-specific dispatch 的可复用结构，但 manifest 投影丢失了精度、范围、字符集、时区、目标原生候选和能力证据。

后端 `compatible_column` 只接收一个 Sink connector、源表/列和目标列，返回 `bool`。同库只做归一化类型字符串比较；跨库使用内置 MySQL↔PostgreSQL 字符串映射，再比较 nullable、collation、generated/extra（[registry.rs](../../crates/web_ui/src/registry.rs#L218-L273)、[registry.rs](../../crates/web_ui/src/registry.rs#L275-L365)）。这是当前唯一可复用的 Web 级字段判断入口，但应升级为 target-owned、结构化的 explain/plan 结果，而不是继续增加 pair 映射分支。

Catalog 只返回 name、`column_type`、nullable、extra、collation、default_value；表级不可用原因只有引擎、主键和列定义是否可读（[catalog.rs](../../crates/web_ui/src/catalog.rs#L51-L84)）。MySQL catalog 依赖 `INFORMATION_SCHEMA.COLUMNS` 的 `COLUMN_TYPE` 等字符串，PG catalog 依赖 `format_type`、`is_generated`、collation 和 default（[catalog.rs](../../crates/web_ui/src/catalog.rs#L312-L367)、[catalog.rs](../../crates/web_ui/src/catalog.rs#L398-L433)）。因此目录可以作为原生定义的起点，但还不足以产生稳定的 LogicalType、Schema Fingerprint 或风险证据。

任务后端先检查实例/数据库/表/主键，再按选中列调用 `compatible_column`，并拒绝遗漏的非空、无默认值、非 auto-increment、非 generated 目标列（[tasks.rs](../../crates/web_ui/src/tasks.rs#L219-L277)、[tasks.rs](../../crates/web_ui/src/tasks.rs#L316-L408)）。创建时只把 `mappings` JSON、两个数据库名、两个实例 revision 和 start mode 写入 SQLite（[tasks.rs](../../crates/web_ui/src/tasks.rs#L410-L448)）。`TableMapping.columns` 只有列名，空数组表示全列（[tasks.rs](../../crates/web_ui/src/tasks.rs#L18-L42)）。这些校验和原子 revision recheck 可保留，但持久化结构需承载兼容决策。

运行时通过实例 revision 查找精确 Source/Sink connector 并拒绝配置变化；启动时只检查表写入重叠，checkpoint 只存位点/统计，未存兼容 plan（[runtime_store.rs](../../crates/web_ui/src/runtime_store.rs#L90-L146)、[runtime_store.rs](../../crates/web_ui/src/runtime_store.rs#L212-L264)）。Worker 对每个事件先按列名做 `project`，再调用具体 Sink 的 `plan`；没有 activation-time plan，也没有把 plan 传给 Sink（[task_worker.rs](../../crates/web_ui/src/task_worker.rs#L236-L274)、[task_worker.rs](../../crates/web_ui/src/task_worker.rs#L446-L484)）。

## 当前字段“禁用”逻辑

前端同时存在“真实 disabled”和“有效不可选”两套语义：

1. 字段 checkbox 的 HTML `disabled` 只在同名源/目标不存在、没有可选字段，或 schema 仍在加载时设置；`columnPairReason` 返回不兼容时并没有设置 `disabled`，只加 title 和 `is-incompatible`（[tasks.js](../../crates/web_ui/assets/tasks.js#L460-L500)、[tasks.js](../../crates/web_ui/assets/tasks.js#L628-L646)）。由于 `rowKeys` 对不兼容字段返回空集合，它实际上无法通过行/列选择逻辑加入选择，但无障碍语义和视觉语义不一致。
2. 已选主键使用 `aria-disabled` 和 click preventDefault 锁定，不能单独取消；要取消必须取消整张表（[tasks.js](../../crates/web_ui/assets/tasks.js#L634-L644)）。这是增量定位的硬要求，不是兼容选项。
3. 页面把 `crossType`/`postgresqlToMysql` 复制了一份到 JavaScript，后端又有一份 Rust 逻辑；页面只显示一个“字段定义不兼容”，没有 code、risk、可选方案或 Sink 能力证据（[tasks.js](../../crates/web_ui/assets/tasks.js#L405-L478)）。真正提交时后端还会重新连接并完整校验，所以 UI 不是权威决策源。
4. 选择结果只生成 `{source_schema, source_table, sink_schema, sink_table, columns}`；POST payload 没有 compatibility options 或 conversion plan（[tasks.js](../../crates/web_ui/assets/tasks.js#L754-L768)、[tasks.js](../../crates/web_ui/assets/tasks.js#L829-L838)）。实例和起点模式的禁用/警告是另一层 connector capability 检查，不应与列值兼容混为一谈。

## 硬禁止与可选兼容的边界

### 必须继续阻止

- `change_event` 形状、image/presence、logical value 不合法，或者 Source contract 的 native type 不支持、native/logical 不匹配、PG generated source column 不支持：这是源事件无法可靠解释，目的端选项不能补出丢失的源语义（[validate.rs](../../crates/change_event/src/validate.rs#L76-L188)、[source_contract.rs](../../crates/postgresql_15/src/source_contract.rs#L111-L171)）。
- 没有主键/稳定 row locator、主键值为 `Unavailable`/`Unchanged`、目标表不是支持的表类型、目标主键不一致：这是 DML 定位或结构约束，不是值转换；MySQL/PG Sink 都会在 plan 前后检查其中一部分（[mysql_5_7/sql.rs](../../crates/mysql_5_7/src/sql.rs#L429-L461)、[postgresql_15/sql.rs](../../crates/postgresql_15/src/sql.rs#L396-L435)）。
- 同名绑定缺失、列名/列数/主键顺序结构不一致，以及选中列遗漏目标端必填且无默认值的列：除非另开“目标结构/默认值语义”设计，否则不能被字段兼容选项掩盖（[tasks.rs](../../crates/web_ui/src/tasks.rs#L197-L210)、[tasks.rs](../../crates/web_ui/src/tasks.rs#L263-L275)）。
- MySQL 当前对 Boolean/Uuid 的拒绝、超出 DECIMAL/时间精度、非有限浮点和不受支持字符集的拒绝，不能靠 UI 勾选直接放行。只有新增了明确的 LogicalValue→目标类型编码、目标列约束/读回等价证明和计划语义，才可能变成一个新资格，而不是“关闭禁用”。

### 可以成为显式选项候选，但目前还没有实现

- 整数宽度/unsigned 到更宽目标类型：可以产生 `RANGE_CHECKED` 计划，在 activation 时按声明值域证明目标可表示，运行时只做计划规定的范围检查；不能按样本最大值推断。
- Decimal precision/scale、文本长度/字符集、时间精度和 Instant 微秒截断：只有在目标能力明确声明 exact 或可审计的降级策略时才能放行；默认应是 `UNSUPPORTED`/阻止。尤其精度丢失、collation 变化和时区语义变化不是普通字符串映射。
- PG Boolean/Uuid 到 MySQL 的编码：需要目标 schema 是可证明的 `TINYINT`/固定文本方案、读回和比较语义、版本化 rule/capability 证据；不能复用当前 `postgresql_to_mysql` 的 `bool`。
- JSON 到 JSON/JSONB、Duration/时间族、generated observation：可以由独立的 operation/type signature 和 session profile 资格化；缺少 observation、目标规范化或 session 语义不等价时仍必须阻止。

这些候选的共同条件是：结果必须带 stable code、risk/action、规则版本和 evidence；选项只能选择已嵌入 manifest 的资格，不能创造或扩大能力。这与现有设计中 `EXACT`/`RANGE_CHECKED`/`UNSUPPORTED`、`Known Omission` 和 `Target Capability Failure` 的边界一致（[capability-qualification.md](../design/capability-qualification.md#L3-L18)、[mysql-value-semantics.md](../design/mysql-value-semantics.md#L9-L17)、[target-schema-plan.md](../design/target-schema-plan.md#L23-L29)）。

## 实现“目的端字段兼容选项”还缺少的接口和数据

### 必要接口

1. SourceTypeMapping：输入精确 Source connector/build、NativeType 和列语义，输出 LogicalType、解码规则版本和证据摘要；不要让 Web 从 `column_type` 字符串自行推断。
2. Sink 结构化 explain/qualify：输入 source column definition、target catalog column、Source/Sink identity、目标 build、事件操作/存在状态和 route policy，输出 `Compatible`、`NeedsConfirmation`、`Unsupported` 或 `Blocked`，带 stable code、reason data、risk 和候选 target native type。
3. ColumnConversionPlan builder：固定 source/target lineage、LogicalType、目标类型参数、转换动作、overflow/truncation/timezone/charset/JSON/generation policy、mapping rule version、capability-entry identity 和 plan digest。每列只产生一个不可变计划。
4. Sink Transaction Plan / worker apply API：worker 应在任务激活或重资格化时构建/验证列计划，并将计划或其内容寻址 identity 交给 Sink；`apply` 不能只接收裸 `ChangeTransaction`。事务计划应覆盖完整批次、目标 schema fingerprint、session profile 和 route/config revision。
5. 结构化 `TargetCapabilityFailure`：至少要有 stable code、route/event identity、source/target column lineage、exact target build、缺失 capability key、conversion-plan digest 和 target schema fingerprint；本地化 message 只能是派生显示文本。当前类型是私有字符串 tuple（[validate.rs](../../crates/change_event/src/validate.rs#L23-L50)）。
6. Web compatibility preview API：返回每列判定、风险、原因、可用选项、需要确认的字段和被硬禁止的字段；前端只渲染该结果，删除 `tasks.js` 与 `registry.rs` 的重复映射。

### 必要数据与持久化

- CatalogColumn 增加解析后的 LogicalType 和完整 native parameters：signedness/width、precision/scale、长度单位和上限、charset/encoding、collation identity、temporal precision/time zone、generated/default/identity 语义、JSON/enum/spatial/extension 证据；同时生成按列顺序和表约束计算的 Schema Fingerprint。
- CapabilityManifest 增加精确 target build、manifest digest、能力 code、operation/image role、参数化 type signature、目标原生候选、presence/PK/locator、session profile、范围/精度/字符集/规范化/读回证据和 rule/evidence-suite version，而不只是 LogicalType 名称数组。
- TaskInput/TableMapping 增加 target column binding（现在虽同名，仍应持久化 lineage/ordinal 或 binding identity）、每列 compatibility option、确认 actor/time、policy revision、mapping rule version、source/target schema fingerprint、connector/build/manifest identity 和 ColumnConversionPlan digest；选项的默认值不能靠反序列化缺省悄悄改变语义。
- SQLite 至少需要保存 route 的 compatibility policy/config revision、plan digest、source/target catalog fingerprint、manifest/capability digest 和重资格化状态；checkpoint 不应代替这些配置身份。实例 revision 变化之外，catalog fingerprint、connector/build、manifest 或规则版本变化都必须使任务暂停重检。
- Runtime/Sink metadata 需要记录成功 apply 所用的 plan/profile/manifest digests 和 route/epoch fence；完整 DML SQL/参数不需要持久化，但必须可从事件、定义、配置和 manifest 重建相同 plan（[sink-transaction-plan.md](../design/sink-transaction-plan.md#L5-L24)）。

## 验证

只执行了只读测试，没有修改业务代码：

- `cargo test -p change_event`：17 passed，1 doc-test passed。
- `cargo test -p web_ui --lib`：37 passed，现场数据库测试按预期 ignored。

现有测试覆盖 registry 的独立 Source/Sink、精确版本查找、当前跨库类型映射、主键/默认值校验、任务 revision 失效和运行时恢复；没有覆盖结构化兼容结果、显式选项、ColumnConversionPlan、计划摘要持久化或能力/目录指纹失效。
