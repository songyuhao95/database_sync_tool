# Issue 58：任意表列类型的日志值能否通用保留

## 结论

两种协议都能传输一段按列界定的值字节，但它们不提供统一、可自描述且对任意类型都可逆的 LogicalValue。

- **MySQL row binlog** 使用 `TABLE_MAP` 中的列类型码和类型元数据解释行事件。它携带的是 MySQL replication 编码，不是存储引擎页字节，也不是通用逻辑值。对目标版本内已知类型，可逐类型解析；未知类型码没有通用长度/解码规则，不能只靠把剩余字节塞进 Raw 安全解析后续列。[MySQL 8.0 binlog protocol](https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classbinary__log_1_1Table__map__event.html)；[MySQL 5.7](https://dev.mysql.com/doc/refman/5.7/en/data-types.html)、[8.0](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)、[8.4](https://dev.mysql.com/doc/refman/8.4/en/data-types.html) 类型目录
- **PostgreSQL `pgoutput`** 可逐列发送 text 或 binary 格式。text 是该类型的 output function 结果；binary 是类型自己的 send function 结果，且发送函数可能不存在。两种字节都可以原样捕获，但对任意用户类型，协议不保证该表示能无损恢复内部值；binary 也不保证跨数据库版本可用。[PG 17 pgoutput 格式](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html)、[PG 格式规则](https://www.postgresql.org/docs/17/protocol-overview.html)、[`CREATE TYPE` 的 I/O 函数要求](https://www.postgresql.org/docs/17/sql-createtype.html)

因此，**generic Raw 足以保留传输表示，不足以单独证明“任意类型值已可逆同步”**。按现有决议 #42/#43，“无稳定 codec 或恢复规则就阻断”；如果要把不保证可逆的协议表示也算作支持，必须由负责人修改“值完整保留”的定义。Issue #58 应保持 OPEN 等待这个边界决策。

## 协议实际提供什么

| 源协议 | 值表示 | 能通用捕获的部分 | 可逆性边界 |
|---|---|---|---|
| MySQL row binlog | `TABLE_MAP` 给列类型码、逐类型 metadata、null bitmap 和可选元数据；row event 的值按列类型编码 | 解析器掌握完整类型码和 metadata 后，可以截取该列的原始 replication value bytes | bytes 不是自描述的；必须保留 type code、metadata、版本及表定义。未知 type code 没有通用跳过方式。wire bytes 可原样放入 BLOB，但不能据此声称另一数据库能解释该值。[协议格式](https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classbinary__log_1_1Table__map__event.html) |
| PostgreSQL `pgoutput` | Relation 提供列名、type OID、typmod；TupleData 逐列标记 NULL、unchanged、text 或 binary，并给字节长度。[协议消息](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html) | `pg_walstream` 保留 format tag 和原始 `ColumnData` bytes；不必先解析值就能复制 text/binary payload | pgoutput 的 binary 选项默认为 false。启用后，仅当类型有有效 `typsend` 才发送 binary；否则仍调用 `typoutput` 发送 text。base type 必须有 input/output，但 receive/send 是可选的；PostgreSQL 文档要求自定义类型作者自行保证 input/output 互逆，并说明复杂 binary 表示可能随版本改变。[pgoutput 选项](https://www.postgresql.org/docs/17/protocol-logical-replication.html)、[PG 17 源码 `proto.c`](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/replication/logical/proto.c)、[CREATE TYPE](https://www.postgresql.org/docs/17/sql-createtype.html)、[版本与格式说明](https://www.postgresql.org/docs/17/protocol-overview.html) |

MySQL 的 `mysql_common 0.37.3` 将 binlog 值转成类型化 `BinlogValue`：decimal 解码为十进制文本，ENUM 解成 ordinal，SET 保留 bitmask，JSON 解析为 JSON DOM，blob/geometry 保留字节；未识别类型走错误分支。这个 API 没有为每个字段同时暴露原始 binlog 切片。[上游 `binlog/value.rs`](https://github.com/blackbeam/rust_mysql_common/blob/v0.37.3/src/binlog/value.rs)、[上游 `binlog/row.rs`](https://github.com/blackbeam/rust_mysql_common/blob/v0.37.3/src/binlog/row.rs)

PostgreSQL `pgoutput` 的 Type 消息只给非内建类型的 OID、schema 和名字，不携带完整 type DDL、扩展实现或 codec 定义。OID 需结合当前 source catalog 解释，不能当跨实例类型身份。[PG 逻辑复制协议](https://www.postgresql.org/docs/17/protocol-logical-replication.html)；[PostgreSQL 15/16/17 协议](https://www.postgresql.org/docs/15/protocol-logical-replication.html)、[16](https://www.postgresql.org/docs/16/protocol-logical-replication.html)、[17](https://www.postgresql.org/docs/17/protocol-logical-replication.html)

## RawValueCarrier 要保证的级别

建议把“原始承载”定义成保留**源协议表示**，而不是虚称数据库无关逻辑值。现有 `RawValueCarrier` 已有 codec identity、native type、definition digest、encoding、payload bytes、可选 canonical text；要支撑六种 Sink 和恢复，还需补足或确保可从 ChangeEvent 的不可变 source definition 取回：

1. **表示种类**：`mysql.binlog-column`、`pgoutput.text-output`、`pgoutput.binary-send` 或经 codec 规范化的表示；不可只写模糊的 `raw`。
2. **协议解码上下文**：connector/server major+build、MySQL type code 与完整列 metadata/相关 TABLE_MAP 元数据，或 PostgreSQL type OID、typmod 与 text/binary tag。MySQL 原始字段值不能离开此上下文独立解释。
3. **完整类型定义指纹**：schema-qualified type identity、递归依赖/元素/属性/域约束/range subtype、collation，以及 extension 名称和版本。OID/本地函数 OID 只能作为源端证据，不能单独充当稳定身份。
4. **codec 与会话证据**：codec 名称/版本及其输入输出或 send/receive 函数实现指纹；字符集/服务端编码和会改变输出文本的会话设置。源端不存在 binary send/receive 时必须明确标注 text-only。
5. **完整性证据**：无损 byte payload、长度、payload digest、format 和捕获协议 cursor；可选 canonical text 不能替代 raw bytes。
6. **恢复声明**：区分“字节可原样存取”“同一已验证 codec 可恢复源类型值”“已映射为数据库无关逻辑值”。只有经过验证的恢复 codec 才能宣称 value-preserved；否则 UI 应说“仅保存源表示，目标无法按原类型查询/运算”。

将 carrier 写入预建 BLOB/BYTEA 时，应存 self-describing envelope 或将 envelope metadata 持久化到可事务关联的控制记录；只写裸 bytes 会丢失解释上下文。TEXT/JSON carrier 还要显式编码 binary payload（例如 base64url）。这种方案保留的是证据/表示，不自动保留比较、索引、算术、空间运算或约束语义。

## 本仓库的准确断点

- **MySQL Source**：[`mysql_5_7/src/decoder.rs`](../../crates/mysql_5_7/src/decoder.rs) 经 `RowsEventData::rows()` 取得已解析 `BinlogRow`，再转 `Value`/`LogicalValue`；未知类型在上游类型 parser 层即报错。加 Raw 必须在列解析处截取 bytes 和 TableMap metadata，且要给目标版本所有可存储类型提供 parser 分支；版本 mapping 还需补齐所有声明形状。
- **PostgreSQL Source**：[`postgresql_15/src/decoder.rs`](../../crates/postgresql_15/src/decoder.rs) 仅接受 `b't'`，binary value 报错，`M::Type` 也被拒绝；[`types.rs`](../../crates/postgresql_15/src/types.rs) 是标量 OID allow-list；[`catalog.rs`](../../crates/postgresql_15/src/catalog.rs) 虽调用递归 mapping，随后仍由 allow-list 排除数组、复合、domain、range 等值。当前 [`pg_walstream 0.8.1 TupleData`](https://github.com/isdaniel/pg-walstream/blob/v0.8.1/src/protocol.rs) 已保留 format tag 和 bytes，是 generic capture 的现成低层 seam。
- **ChangeEvent**：[`model.rs`](../../crates/change_event/src/model.rs) 已有 `RawValueCarrier`，但它是值模型，不会自动证明 codec 可逆；Source definition reference/digest 必须覆盖递归依赖与相关 codec/session 证据。
- **六个 Sink**：MySQL binder [`mysql_5_7/src/sql.rs`](../../crates/mysql_5_7/src/sql.rs) 明确拒绝 Raw、数组、复合、map、range、多范围、domain、网络、XML 和空间值。PostgreSQL binder [`postgresql_15/src/sql.rs`](../../crates/postgresql_15/src/sql.rs) 仅部分处理 Raw/结构类型，map、部分时态/特殊值路径仍拒绝；其 `CustomBinary` 不能被当作任意 source type 在 target 上可解码的证明。三版本 MySQL 与三版本 PostgreSQL Sink 都需要逐项能力和 live 写入证据。
- **Qualification**：[`tests/qualification_matrix.rs`](../../tests/qualification_matrix.rs) 在 SourceTypeMapping 失败时生成 `UNSUPPORTED/BLOCKED`，但 `offline: PASS`。这是“拦截正确”的测试，不是该类型可同步证据；报告必须分别统计捕获成功、逻辑值解码、carrier 原样落地、恢复验证和语义等价。

## 实现路径

1. 按下方决策先锁定 `VALUE_PRESERVED` 对任意自定义类型的定义；据此稳定 Raw envelope 和计划摘要。
2. PG Source 将 tuple 原始 format+bytes 连同递归 catalog/extension/codec 指纹送入 ChangeEvent；已知类型继续解成 LogicalValue。把 binary receive/send 能力和 output/input identity 纳入 Source/Sink probe。
3. MySQL Source 扩展 row decoder，在每列解析时保留 replication value slice、type code、metadata；同时为 MySQL 5.7/8.0/8.4 的有限原生类型 roster 补齐具语义的 codec，不把 wire bytes 当成跨库 LogicalValue。
4. 六种 Sink 为 Raw envelope 提供可预创建的 BLOB/BYTEA/TEXT/JSON 表示计划；兼容选项展示“保存了哪种表示、能否恢复原类型、哪些语义不可用”，并按既定键安全规则限制 PK/unique/Row Locator。
5.  qualification 对每个 native type 分别测试 Source capture、ChangeEvent replay、六个 Sink carrier 写入与回读 digest；仅拦截不得计入全类型支持。需要恢复声明的用对应类型 input/receive codec 做 round-trip qualification。

## 需要负责人回答的精确问题

对于 PostgreSQL 任意合法 base/extension type：只有 `pgoutput` text output bytes（或者有 `typsend` 时的 binary-send bytes）可以被 self-describing envelope 原样存入目标 BLOB/BYTEA/TEXT/JSON，但没有已验证的 input/receive/目标 codec 时，**这是否算“支持该类型同步 / VALUE_PRESERVED”**？

- **A：不算，值必须可恢复为类型值**（符合已关闭的 #42/#43 决议）。每种任意用户/扩展类型都必须由适配器或类型所有者提供并资格验证稳定 codec；无 codec 时仍阻断。这样保证的是有 codec 类型的完整语义路径，但“所有任意用户类型”取决于 codec 是否存在。
- **B：算表示级保留**。允许用户显式选择 envelope，同步协议输出/二进制表示；目标可存、可取回字节，但不能承诺源类型可恢复、可查询或跨版本解释。这需要修订 #42/#43 中“无可恢复 codec 即阻断”的决议，并将 UI/qualification 的 VALUE_PRESERVED 明确改名或限定为 `SOURCE_REPRESENTATION_PRESERVED`。
- **C：混合**（建议）。优先 codec 驱动的逻辑值及原生/显式转换；对无 codec 类型提供显式、风险确认的表示级 envelope，但单独标记为 representation-only，绝不冒充语义值完整保留。它也需要修改 #43 的“即使用户确认也必须阻断”边界，并将表示级同步纳入地图完成条件。

在负责人回答前，不应把任意自定义类型的 raw bytes 记为已支持，也不应关闭 Issue 58 或将离线拦截结果计入通过。
