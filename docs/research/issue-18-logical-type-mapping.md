# 研究：跨数据库公共 LogicalType 与版本化类型映射

研究对象是 MySQL 5.7、8.0、8.4 与 PostgreSQL 15、16、17；结论服务于现有 ChangeEvent v1 的 DML 增量同步和已存在目标表范围。本记录只修改研究文档，不修改实现代码。

## 结论摘要

1. `LogicalType` 描述可跨数据库复用的值域和必须保真的语义；`NativeType` 保留精确的源数据库声明、版本和源专属证据；目标数据库类型、转换动作、风险和能力证明只进入 `ColumnConversionPlan`/Sink `Capability Manifest`。不允许根据运行时值或目标数据库类型名推断语义。
2. 整数必须保留 signedness 和声明宽度；定点数必须保留 precision 与 signed scale；浮点数必须保留 IEEE-754 宽度和位模式。MySQL 的 `TINYINT(1)`/`BOOLEAN` 不是独立布尔存储语义，不能自动当作 `Boolean`。
3. 文本的编码属于 `LogicalType` 的解码语义；长度单位、固定/变长和填充语义也属于类型约束。排序规则不属于文本载荷，而是 Column Definition 的源语义属性；目标排序规则和等价性证明属于 `ColumnConversionPlan`。
4. 时间必须区分 `Date`、`LocalTime`、`LocalDatetime`、`Instant`、带偏移的时间、固定时长和日历间隔。会话时区/数据库 `TimeZone` 是解析、显示和执行环境，放入源/目标环境或 `Sink Session Profile`，不能隐含进 `LocalDatetime`。
5. `Json` 采用版本化的类型化树。第一版公共语义选择“规范化 JSON 文档”：数组顺序、数值类别和精确数值保留，对象按键字节规范化且不承诺空白/键顺序/重复键。PostgreSQL `json` 的原始文本语义必须单独声明为文本保真或被显式标记为有损转换，不能与 MySQL `JSON`、PostgreSQL `jsonb` 默认为同一语义。
6. ENUM、SET、位字符串、空间、数组、复合、范围/多范围均不得降级为普通字符串或 `Binary`。没有等价目标原生能力时，Sink 应报告 Target Capability Failure；只有明确写入转换方案并经风险确认，才允许显式有损编码。

## 版本差异

| 版本 | 类型面 | 对映射的实质影响 |
| --- | --- | --- |
| MySQL 5.7 | 数值、日期时间、字符串/字节、空间、JSON、ENUM、SET | `YEAR(2)` 已在 5.7.5 移除；JSON 自 5.7.8 为原生类型；空间类型没有 8.0 的列级 `SRID`/SRS 约束。`TIMESTAMP` 受连接 `time_zone` 转 UTC/回转，宽松 SQL mode 还可能保留零日期。见 [5.7 Data Types](https://docs.oracle.com/cd/E17952_01/mysql-5.7-en/data-types.html)、[YEAR](https://docs.oracle.com/cd/E17952_01/mysql-5.7-en/year.html)、[空间类型](https://docs.oracle.com/cd/E17952_01/mysql-5.7-en/spatial-type-overview.html)、[JSON](https://docs.oracle.com/cd/E17952_01/mysql-5.7-en/json.html)。 |
| MySQL 8.0 | 与 5.7 的核心类型族基本相同 | 引入/完善列级 SRID 和 Cartesian/geographic SRS 能力；`YEAR(2)` 不支持，`YEAR(4)` 自 8.0.19 弃用；8.0.19 起时间输入允许时区偏移；8.0.17 起 InnoDB 支持 JSON 多值索引。版本差异要进入 Capability Manifest，而不是改变事件中的 Native Type 名称。见 [8.0 Data Types](https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/data-types.html)、[8.0 日期时间](https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/datetime.html)、[8.0 空间类型](https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/spatial-type-overview.html)。 |
| MySQL 8.4 | 对本研究的类型族与 8.0 基本相同 | 继续使用 `UNSIGNED`、`DECIMAL`、fsp 0--6、JSON、ENUM/SET 和空间/SRID 语义；`YEAR(4)` 仍是弃用声明。8.4 不应与 8.0/5.7 共享未经验证的规则版本。见 [8.4 Data Types](https://dev.mysql.com/doc/refman/8.4/en/data-types.html)。 |
| PostgreSQL 15 | 2/4/8 字节有符号整数、任意精度 numeric、real/double、字符、bytea、完整日期时间、Boolean、ENUM、bit、UUID、JSON/jsonb、数组、复合、范围/多范围及大量专有类型 | `numeric` 从 15 起支持负 scale，也允许 scale 大于 precision；`serial`/`bigserial` 是序列创建语法，不是独立值类型。公共映射必须保留 typmod、序列/identity 等源元数据。见 [15 numeric](https://www.postgresql.org/docs/15/datatype-numeric.html)、[15 Data Types](https://www.postgresql.org/docs/15/datatype.html)。 |
| PostgreSQL 16 | 与 15 的相关核心类型族和主要值语义稳定 | 仍需按 16 的服务器、扩展、排序规则和驱动能力选择规则；不能仅按 `postgresql` 大版本复用目标能力。见 [16 numeric](https://www.postgresql.org/docs/16/datatype-numeric.html)、[16 Data Types](https://www.postgresql.org/docs/16/datatype.html)。 |
| PostgreSQL 17 | 与 15/16 的相关核心类型族和主要值语义稳定 | 继续保留 SQL/JSON、数组元素、复合字段、范围边界以及 ICU/libc 排序规则等能力差异；版本化主要用于能力、环境和扩展证明，而不是再造一套 LogicalType。见 [17 Data Types](https://www.postgresql.org/docs/17/datatype.html)。 |

MySQL 的参考手册把整数、定点数、浮点数、时间、字符串/字节、空间和 JSON 分开定义；PostgreSQL 17 的总表还包括 `money`、网络、XML、全文检索、数组、复合和范围等类型。因此“核心族稳定”不等于“任意两版或任意两端可互换”。

## 类型族和边界

| LogicalType | MySQL | PostgreSQL | 规则 |
| --- | --- | --- | --- |
| `Boolean` | `BOOLEAN`/`BOOL` 是 `TINYINT(1)` 的别名；`BIT(1)` 仍是位值 | `boolean` 是独立 true/false 类型 | 只有源声明和适配器规则明确为 Boolean 才映射；普通 `TINYINT(1)` 保持 `Integer(8,signed)`。 |
| `Integer` | `TINYINT` 8、`SMALLINT` 16、`MEDIUMINT` 24、`INT` 32、`BIGINT` 64 bit；每种可 `UNSIGNED` | `smallint` 16、`integer` 32、`bigint` 64，均有符号 | `width_bits` 允许 8/16/24/32/64，另有 `signedness`。目标无同宽类型时用已证明的宽类型或 exact `Decimal`；`BIGINT UNSIGNED` 不能放进 PostgreSQL `bigint`。数值宽度不能从实际数据范围推导。见 [MySQL integer](https://dev.mysql.com/doc/refman/8.4/en/integer-types.html)、[PostgreSQL numeric](https://www.postgresql.org/docs/17/datatype-numeric.html)。 |
| `Decimal` | `DECIMAL/NUMERIC` 等价，精度最多 65，scale 0--30 | `numeric/decimal` 等价；声明 precision 最多 1000，未约束 numeric 的实现上限更大，scale 可为 -1000--1000 | LogicalType 用任意精度 unscaled value + **有符号** scale，列约束另存 precision/scale；目标缩窄、舍入、负 scale 要进入计划并预先校验。 |
| `Float` | 4/8 byte `FLOAT`/`DOUBLE`；`FLOAT(M,D)` 等非标准精度/小数位语法在 8.4 已弃用 | `real`/`double precision` 为 IEEE 单/双精度，支持 `Infinity`/`-Infinity`/`NaN` | 保留宽度和精确 IEEE 位；特殊值能力必须按版本核验，不能转成 Decimal 或字符串。 |
| `BitString` | `BIT(M)`，M 为 1--64，短值左侧补零 | `bit(n)` 固定长度，`bit varying(n)` 变长，位长度是语义 | 独立于 Integer 和 Binary，值携带 bit length；padding/truncate 只由计划显式决定。见 [MySQL BIT](https://dev.mysql.com/doc/refman/8.4/en/bit-type.html)、[PostgreSQL bit](https://www.postgresql.org/docs/17/datatype-bit.html)。 |
| `Text` | `CHAR/VARCHAR` 的长度按字符；TEXT 分为四个按字节上限的层级；列可指定 charset/collation | `char(n)`/`varchar(n)` 按字符；`text`/无长度 varchar 无声明上限但受实现限制；编码由 database 决定，列可有 collation | 记录 encoding、长度单位/上限、fixedness、padding/trailing-space 语义。MySQL `CHAR`、PostgreSQL `char` 的空格语义不能假定与变长文本相同。见 [MySQL CHAR/VARCHAR](https://dev.mysql.com/doc/refman/8.4/en/char.html)、[PostgreSQL character](https://www.postgresql.org/docs/17/datatype-character.html)。 |
| `Binary` | `BINARY/VARBINARY` 与 `BLOB` 四层，长度按字节；`BINARY` 有固定填充 | `bytea` 是可变长字节串 | 值永远是 bytes；类型约束保留 max bytes、fixedness、padding。不能把 text 的 charset/collation 复制给 Binary。见 [MySQL binary](https://dev.mysql.com/doc/refman/8.4/en/binary-varbinary.html)、[MySQL BLOB/TEXT](https://dev.mysql.com/doc/refman/8.4/en/blob.html)、[PostgreSQL bytea](https://www.postgresql.org/docs/17/datatype-binary.html)。 |
| `Date` / `LocalTime` / `LocalDatetime` | `DATE`、`TIME`、`DATETIME`；TIME 既可表达一天内时间也可表达负的、最长约 838 小时的时长；fsp 0--6 | `date`、`time`、`timestamp without time zone`；时间精度 p 0--6 | 现有 `Duration` 不能承载 PostgreSQL `time` 的一天内语义和 `interval` 的日历语义，下一版应拆出 LocalTime 与 CalendarInterval。MySQL 零日期/无效日期须为 Invalid Temporal 或阻断。 |
| `Instant` / `TimeWithOffset` / `CalendarInterval` | `TIMESTAMP` 存储按连接时区转换为 UTC；没有原生带时区 `TIME` 或含月日字段的 interval | `timestamptz` 存 UTC、输出按当前 `TimeZone` 显示且不保留原始 zone；`timetz` 保留 offset；`interval` 区分 fields 并可含年月日时分秒 | `Instant` 值用 UTC；源 session TimeZone、目标 session TimeZone 和 tzdata/解析环境进入环境或 Session Profile。不要把 `DATETIME`/timestamp without time zone 当作 Instant；CalendarInterval 保留 months/days/microseconds。见 [MySQL datetime](https://dev.mysql.com/doc/refman/8.4/en/datetime.html)、[MySQL TIME](https://dev.mysql.com/doc/refman/8.4/en/time.html)、[PostgreSQL date/time](https://www.postgresql.org/docs/17/datatype-datetime.html)。 |
| `Year` | 1 byte，1901--2155 和 0000；输入 0/两位数字有特殊解释 | 无同名内置类型 | 保留为 Year；转 integer/date 只能由显式方案决定，不能把 0000 静默改成 NULL 或 2000。见 [MySQL YEAR](https://dev.mysql.com/doc/refman/8.4/en/year.html)。 |
| `Json` | 5.7.8 起原生、自动校验并以内部二进制形式存储；JSON 字符串使用 utf8mb4/utf8mb4_bin 规则 | `json` 保存输入文本；`jsonb` 保存分解后的二进制，不保留空白、键顺序和重复键 | 公共 `Json` 默认使用 normalized-document profile；类型化数字区分 signed/unsigned integer、Decimal、exact double bits，数组有序，对象按 canonical key bytes 表示。PG `json` 若必须保留原文则走 `JsonText`/Opaque 或显式有损计划。见 [MySQL JSON](https://dev.mysql.com/doc/refman/8.4/en/json.html)、[PG JSON](https://www.postgresql.org/docs/17/datatype-json.html)。 |
| `Enum` | 列内有序 label 列表，最多 65,535；按声明顺序排序，非 strict mode 的非法输入可落为 ordinal 0 的 error value；受 charset/collation 影响 | 独立命名类型，有序 label 列表，label 区分大小写和空格，标准构建 label 最长 63 bytes | LogicalType 保存 label 的精确字节/编码、声明顺序和比较语义；值按 label，必要时附 native ordinal/error-state。目标按 label 映射，绝不能按 ordinal 直接写。见 [MySQL ENUM](https://dev.mysql.com/doc/refman/8.4/en/enum.html)、[PG enum](https://www.postgresql.org/docs/17/datatype-enum.html)。 |
| `EnumSet` | `SET` 是 0 个或多个成员，最多 64 个，底层按成员顺序为 bitmap | 无等价原生多选枚举 | 不压平成逗号字符串；保存 member list、选择集合和 native bitmap。到 PostgreSQL array/text 只能是显式 conversion plan，默认不兼容。见 [MySQL SET](https://dev.mysql.com/doc/refman/8.4/en/set.html)。 |
| `Spatial` | OpenGIS `GEOMETRY`/`POINT`/`LINESTRING`/`POLYGON` 和多几何集合；8.0 有 SRID/SRS 约束，5.7 没有列级 SRID | 核心仅有二维 planar `point`/`line`/`lseg`/`box`/`path`/`polygon`/`circle`，坐标为 double；`geometry/geography` 需要单独的 PostGIS 能力 | LogicalType 至少保留 model、geometry kind、dimensions、SRID/CRS、axis order 和有效性约束；值用 WKB/EWKB + SRID。PostgreSQL core 不等于 PostGIS；没有等价 SRS/几何能力不得改写为 bytea。见 [MySQL spatial](https://dev.mysql.com/doc/refman/8.4/en/spatial-type-overview.html)、[MySQL formats](https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html)、[PG geometric](https://www.postgresql.org/docs/17/datatype-geometric.html)。 |
| `Array` / `Struct` / `Map` / `Range` | MySQL 没有原生 typed array、map、composite 或 range；JSON 不能自动获得这些静态类型 | PG array 可嵌套任意 built-in/user-defined/enum/composite/range/domain；composite 是字段名+字段类型；range/multirange 有 subtype、空/无界及开闭边界 | 递归 LogicalType 可表达 element/field/subtype；数组值另保留维度和 lower bounds，range 值保留 empty/unbounded/inclusive/exclusive。MySQL JSON 仍是 Json，不推断为 Map/Struct；到 MySQL 需显式 JSON 编码并声明语义损失。见 [PG arrays](https://www.postgresql.org/docs/17/arrays.html)、[PG composites](https://www.postgresql.org/docs/17/rowtypes.html)、[PG ranges](https://www.postgresql.org/docs/17/rangetypes.html)。 |

PostgreSQL `money` 还受 `lc_monetary` 的精度和 locale-sensitive 输出影响；`inet/cidr/macaddr`、XML、`tsvector/tsquery` 和核心 geometric 也没有 MySQL 对等类型。它们应保留精确 NativeType 并报告不支持，除非未来单独增加 LogicalType 和能力资格；不能把 `money` 无条件当作 Decimal。

## 信息放置

### LogicalType（公共值域）

建议的下一版 LogicalType 结构至少包含：

- `Integer { signedness, width_bits }`；`Decimal { precision?, scale: signed }`；`Float { width_bits, special_values }`；`BitString { fixedness, length_bits }`；
- `Text { encoding, length {unit, max}, fixedness, padding }`；`Binary { length {unit: bytes, max}, fixedness, padding }`；
- `Date`、`LocalTime { precision }`、`LocalDatetime { precision }`、`Instant { precision }`、`TimeWithOffset { precision }`、`Duration`、`CalendarInterval { fields, precision }`、`Year`；
- `Json { profile, number_policy }`；`Enum { labels, order, comparison_profile }`；`EnumSet { members, order }`；`Uuid`；
- `Spatial { model, kind, dimensions, crs/srid, axis_order }`；递归的 `Array`、`Struct`、`Map`、`Range`/`MultiRange`；以及需要显式 Sink 映射的 `Opaque`。

`LogicalValue` 必须按上述类型携带无损值：Decimal 以 unscaled integer + signed scale，Float 以 32/64-bit，Text 以源 bytes/encoding/validity，Binary 以 raw bytes，JSON 以 typed tree，空间以 WKB/EWKB + SRID。对象集合的 canonical ordering 是事件编码规则，不应改变 JSON 数组顺序或源声明顺序。

### NativeType 与源定义

`NativeType` 保留 source kind、精确 server version/build、原生类型名和完整声明/typmod。Column Definition 继续保存源端的 charset、完整 collation identity、padding、enum/set 成员、空间 SRS 限制、PG domain/type OID/extension、MySQL SQL mode 影响等不能被公共值域概括的证据。它们参与 Schema Fingerprint；不要把这些信息散落到每一行的运行时值中。

字符集和排序规则必须拆开：字符集/encoding 决定如何解释文本 bytes，进入 LogicalType 与 NativeType；collation 是比较、排序、大小写/重音和唯一性语义，进入 Column Definition 的 source semantic attributes。PostgreSQL collation 还包含 provider、deterministic、locale/ICU rules 与 provider version；MySQL 也允许 charset/collation 在 server/database/table/column 多层指定，因此必须记录解析后的列级完整身份，而不是只保留默认名。见 [MySQL charset/collation](https://dev.mysql.com/doc/refman/8.4/en/charset-general.html)、[PG collation](https://www.postgresql.org/docs/17/collation.html)、[PG pg_collation](https://www.postgresql.org/docs/17/catalog-pg-collation.html)。

### SourceTypeMapping、Sink Capability 与 ColumnConversionPlan

类型映射拆成两个独立方向：

1. `SourceTypeMapping` 由精确 `Connector Identity`、服务器版本/构建、NativeType 模式和源语义条件匹配，产出一个 LogicalType、值解码器和 mapping rule version。规则版本改变就不能重新解释已发布事件。
2. `Sink Capability Manifest` 由目标 Connector Identity/服务器版本匹配 LogicalType 和其完整参数，声明目标 native candidate、parameter schema、目标 charset/collation/timezone/SRS、溢出/舍入/填充/JSON/enum/spatial 执行能力和 qualification digest。
3. `ColumnConversionPlan` 绑定 source column lineage/schema fingerprint、LogicalType digest、target connector identity、target column/native candidate、mapping/capability rule versions、明确的转换参数和风险/确认状态。它还记录是否 `EXACT`、`RANGE_CHECKED`、需要显式接受或 `UNSUPPORTED`；计划摘要变化即失效并需重新激活。

因此：源端 `MySQL MEDIUMINT` 如何进入 `Integer(24)` 是 SourceTypeMapping；`Integer(24)` 在某个 PostgreSQL 版本选择 `integer` 并做范围校验是 Sink Capability + ColumnConversionPlan；选择 `text`、改变 collation、把 PG `json` 规范化到 `jsonb`、把 `SET` 编为 JSON、或做空间 CRS 转换，全部是显式计划，不属于公共 LogicalType 的自动规则。

## 版本化和验证约束

- mapping/capability key 必须至少包含 database kind、精确 major/minor family、Connector Identity、NativeType 参数和相关环境 profile；只有经过同一组 golden vectors 和真实版本执行测试证明不变，才可合并版本范围。
- 计划选择以声明类型和源定义为输入，不以当前行的最大值、样本内容或目标 driver 的隐式 cast 为输入。
- 能力检查必须在 Sink Apply Transaction 前完成。超范围、不可表示的精度、字符集不可编码、collation/SRS 不等价、JSON 规范化损失、enum 未知 label、bit padding 变化、temporal zone/invalid date 变化都必须在计划阶段阻断或要求显式风险接受；运行时失败不能静默变成 NULL、截断、默认值或字符串。
- 现有 ChangeEvent v1 的初始公共类型集合可继续作为 DML 核心，但要覆盖本票据范围，下一版必须补齐 `LocalTime`、`CalendarInterval`、`BitString`、`Enum`、`EnumSet`、`Spatial` 和递归类型/`Opaque` 的正式契约。该票据不修改代码；#20 决定领域关系，#21 决定风险分类，#22 决定目标能力和 `ColumnConversionPlan` 最小字段，#26 决定完整测试矩阵。

