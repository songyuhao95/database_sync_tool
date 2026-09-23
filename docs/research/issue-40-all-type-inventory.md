# Issue 40：MySQL 与 PostgreSQL 全类型语义清单和版本差异

## 研究范围

本记录服务于 [架构地图：MySQL 与 PostgreSQL 全类型兼容](https://github.com/songyuhao95/database_sync_tool/issues/39) 的票据“研究：MySQL 与 PostgreSQL 全类型语义清单和版本差异”。目标是为 SourceTypeMapping、LogicalType、Sink Capability Manifest 和 Column Conversion Plan 提供类型清单和边界事实。

本研究覆盖 MySQL 5.7、8.0、8.4 和 PostgreSQL 15、16、17 的内建类型，以及需要单独能力探测的用户定义类型和扩展类型。当前实现范围仍是 DML、预先创建的目标表和已有 ChangeEvent 事务模型；本记录不引入 DDL 或自动改表。

## 结论

1. “所有类型兼容”不能实现为源类型名称到目标类型名称的一张表。必须先保留源值语义，再由目标能力清单选择原生等价、值完整保留或显式语义转换。
2. “值完整保留”与“原生行为等价”是两个不同验收维度。例如空间值写入 `bytea` 可以保留 WKB/EWKB 字节，但不会保留目标端空间类型、空间函数、SRID 约束和空间索引行为；MySQL `SET` 写入 JSON 数组可以保留成员集合，但不会保留 SET 的位图存储和原生成员约束。
3. PostgreSQL 的类型系统允许用户定义 base type、enum、composite、range 和 domain，并且自动为类型提供数组类型；因此 PostgreSQL 的“所有类型”无法在不读取类型定义和扩展元数据的情况下闭合。未知或无法解释的类型必须进入能力探测或明确的 Opaque/原始值承载路径，不能从行值推断。
4. 主键、唯一键和 Row Locator 使用的字段需要更严格的资格条件。只保留字节或文本值而改变比较、排序、规范化、时区或精度语义的方案不能自动用于定位更新和删除。

## 类型语义清单

### 1. 精确数值

| 家族 | MySQL 语义 | PostgreSQL 语义 | ChangeEvent 处理 | 主要风险 |
|---|---|---|---|---|
| 整数 | `TINYINT`、`SMALLINT`、`MEDIUMINT`、`INT`、`BIGINT`，并有 `UNSIGNED` | `smallint`、`integer`、`bigint`；另有 serial 家族 | `Integer { signed, bits, value }`，必须保留宽度和 signedness | unsigned 64 位不能放入 PostgreSQL bigint；需要 `numeric(20,0)` 或其他经证明的承载 |
| 定点数 | `DECIMAL/NUMERIC(M,D)`，MySQL 8.4 文档规定精度和 scale 受版本限制 | `numeric/decimal` 可声明精度和 scale，未声明时为任意精度范围 | `Decimal { unscaled, scale }`；计划固定目标精度、scale 和舍入/拒绝策略 | 目标精度不足、scale 变化和隐式舍入会造成数据损失 |
| 浮点 | `FLOAT`、`DOUBLE`，近似值语义 | `real`、`double precision`，近似值语义 | IEEE 位模式 | 不能把浮点当 Decimal；NaN、Infinity、负零和比较语义必须单独资格化 |
| 位值 | MySQL `BIT(M)` | PostgreSQL `bit(n)`、`bit varying(n)` | `BitString` 保留 bit 长度、padding 和 bit order | 不能仅根据字节长度推断有效 bit 数；长度和字节序必须来自定义和协议 |

MySQL 官方类型目录将整数、定点、浮点和 BIT 分开，并将 `UNSIGNED` 作为改变值域的重要属性；PostgreSQL 官方文档也将整数、任意精度数值和浮点分开。参考：[MySQL 8.4 Data Types](https://dev.mysql.com/doc/refman/8.4/en/data-types.html)、[MySQL 8.0 Data Types](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)、[PostgreSQL 17 Numeric Types](https://www.postgresql.org/docs/17/datatype-numeric.html)。

### 2. 文本、编码、二进制和字符比较

| 家族 | MySQL 语义 | PostgreSQL 语义 | ChangeEvent 处理 | 主要风险 |
|---|---|---|---|---|
| 定长/变长文本 | `CHAR`、`VARCHAR`，长度按字符但受字符集存储边界影响 | `char(n)`、`varchar(n)`、`text`，长度按字符；`char` 有空格填充/比较规则 | `Text` 同时保留 charset、长度、长度单位和 collation | 编码转换失败、字符长度和字节长度混淆、尾随空格、排序规则变化 |
| TEXT/BLOB | `TINYTEXT`、`TEXT`、`MEDIUMTEXT`、`LONGTEXT`；二进制对应多个 BLOB 宽度 | `text` 无固定上限；二进制是 `bytea` | 文本为 `Text`，二进制为 `Binary`，保留长度边界 | 用 TEXT 承载二进制会改变字节语义；用 unbounded text 承载有上限文本会改变约束 |
| 定长/变长二进制 | `BINARY(n)`、`VARBINARY(n)`、BLOB 家族 | `bytea` | `Binary { bytes_base64url }`，计划保留或明确放宽长度 | padding、长度限制和二进制比较语义可能变化 |
| 字符排序 | MySQL charset/collation 直接参与比较和唯一性 | PostgreSQL collation 依赖数据库、操作系统/ICU 和列定义 | 作为 LogicalType/计划输入，不可仅当展示信息 | 同一字符串在两端比较结果不同会破坏唯一键、Row Locator 和更新/删除 |

MySQL 8.4 文档明确区分字符类型、字节类型、ENUM/SET 和字符集相关行为；PostgreSQL 17 文档说明 `char(n)`、`varchar(n)` 的长度按字符，并存在空格填充和超长处理差异。参考：[MySQL String Types](https://dev.mysql.com/doc/refman/8.4/en/string-types.html)、[PostgreSQL Character Types](https://www.postgresql.org/docs/17/datatype-character.html)。

### 3. 时间、日期和特殊值

| 家族 | MySQL 语义 | PostgreSQL 语义 | ChangeEvent 处理 | 主要风险 |
|---|---|---|---|---|
| 日期 | `DATE`，MySQL 还可能出现受 sql_mode 影响的零日期/不完整日期 | `date`，要求有效日期语义 | `Date`；Invalid Temporal 必须显式处理 | 零日期不能静默改成 NULL 或普通日期 |
| 本地日期时间 | `DATETIME(fsp)`，fsp 0–6 | `timestamp without time zone` | `LocalDatetime` | 精度截断、DateStyle/输入格式和范围差异 |
| 带时区瞬时值 | MySQL `TIMESTAMP(fsp)` 会受会话时区转换影响 | `timestamp with time zone` 内部按瞬时值处理 | `Instant` | 源会话时区、目标会话时区和 DST 必须固定并纳入环境指纹 |
| 时间间隔 | MySQL `TIME(fsp)` 可表示带符号且可超过 24 小时的时间 | PostgreSQL `time` 与 `interval` 分开 | `Duration` 或 `LocalTime` | 不能把 MySQL TIME 自动当作 PostgreSQL time；负值和超 24 小时需要计划 |
| 年 | MySQL `YEAR` | 无完全对应的专用 year 类型 | `Year`，常用 `smallint`/`integer` 承载 | 年范围、显示格式和无效年份处理必须明确 |

MySQL 官方数据类型目录规定 `TIME`、`DATETIME`、`TIMESTAMP` 的 fsp 范围为 0–6，且默认精度与标准不同；PostgreSQL 的日期/时间类型和时区语义独立。参考：[MySQL Date and Time Types](https://dev.mysql.com/doc/refman/8.4/en/date-and-time-types.html)、[PostgreSQL Date/Time Types](https://www.postgresql.org/docs/17/datatype-datetime.html)。

### 4. JSON、ENUM 和 SET

| 家族 | 源语义 | 可保留内容 | 不能自动假设的内容 |
|---|---|---|---|
| JSON | MySQL `JSON` 会校验 JSON 文档并采用自己的存储/规范化行为；PostgreSQL `json` 与 `jsonb` 的文本、键顺序和重复键语义不同 | 结构化 JSON 值、数组顺序、数值精度（在 ChangeEvent 范围内） | 原始空白、对象键顺序、重复键、数据库专属规范化行为 |
| ENUM | 有序的声明标签集合；MySQL binlog 可能使用 ordinal wire encoding | 标签和声明顺序 | ordinal 不能直接跨库；PostgreSQL 每个 enum 类型是独立类型，标签长度还受 NAMEDATALEN 影响 |
| SET | 声明成员的集合，MySQL wire 层可能是位图；成员顺序不是值集合语义 | 成员名称集合 | PostgreSQL 没有原生 SET；JSON 数组、文本或自定义类型都会有不同约束/查询语义 |

ChangeEvent 已有 `JsonValue`、`Enum` 和 `Set`，MySQL SourceAdapter 已将 ENUM ordinal 和 SET bitmask 解码为标签/成员。Sink 仍必须为 SET 选择并说明目标表示。PostgreSQL enum 的类型安全和每个 enum 类型独立性见 [PostgreSQL Enumerated Types](https://www.postgresql.org/docs/17/datatype-enum.html)；PostgreSQL JSON/JSONB 差异见 [PostgreSQL JSON Types](https://www.postgresql.org/docs/17/datatype-json.html)。

### 5. 空间类型

MySQL 的空间类型包括 `GEOMETRY`、`POINT`、`LINESTRING`、`POLYGON`、多几何类型和 `GEOMETRYCOLLECTION`，并带有 WKB/EWKB、SRID、维度、几何类型和有效性语义。PostgreSQL 核心没有 PostGIS 的完整空间类型；PostGIS geometry/geography 必须作为扩展能力单独探测。

当前 ChangeEvent 已有 `Spatial { format, bytes, geometry_type, dimensions, srid, crs }`，方向正确，但完整资格还需要：

- 明确 WKB/EWKB 和 SRID 的编码规则；
- 校验 geometry subtype、维度、SRID/CRS 是否匹配；
- 探测目标是否安装 PostGIS 以及具体版本；
- 区分原生空间表示和 `bytea` 原始值承载；
- 记录空间索引、空间函数和约束是否保留。

因此 `Spatial → bytea` 是“值完整保留”候选，不是“原生等价”候选。

### 6. PostgreSQL 容器、自定义类型和扩展类型

PostgreSQL 官方类型系统将类型分为 base、container、domain 和 pseudo-types；`CREATE TYPE` 支持 composite、enum、range、base 和 shell 类型。数组会围绕类型形成容器；range 还可以有对应 multirange。参考：[PostgreSQL Type System](https://www.postgresql.org/docs/17/extend-type-system.html)、[CREATE TYPE](https://www.postgresql.org/docs/17/sql-createtype.html)、[User-Defined Types](https://www.postgresql.org/docs/17/xtypes.html)。

兼容性分类如下：

| PostgreSQL 类型 | 推荐 LogicalType | 无扩展目标的候选承载 | 关键限制 |
|---|---|---|---|
| 数组 | `Array(element)` | JSONB | 元素类型、NULL、维度、下界和多维结构必须保留 |
| composite | `Struct(fields)` | JSONB | 字段定义、字段顺序、NULL 和嵌套类型必须来自目录定义 |
| hstore/Map | `Map(key,value)` 或扩展专用 LogicalType | JSONB | key NULL 规则、排序/重复 key 和类型约束必须明确 |
| range | `Range(element)` | JSONB 结构 | 空范围、边界包含性、无限边界、canonicalization 不能丢失 |
| multirange | `MultiRange(element)` | JSONB 数组结构 | 子范围顺序、合并规则和空值语义必须保留 |
| domain | 被包裹的 LogicalType + domain constraints | 同底层类型的值承载 | CHECK、NOT NULL、collation 等约束不能被默认为目标存在 |
| 自定义 base type | 只有有明确 input/output/send/receive 语义才可映射 | 原始 bytes/text Opaque | Opaque 不提供跨数据库行为等价 |
| 网络/XML/全文检索等 | 专用 LogicalType 或结构化承载 | JSONB/TEXT/BYTEA | 运算符、规范化和索引语义通常不是跨库等价 |
| PostGIS geometry/geography | `Spatial` + CRS/SRID/维度 | BYTEA WKB/EWKB | 必须探测 PostGIS、版本和目标列类型 |

### 7. OID、系统标识和协议相关类型

PostgreSQL 的 OID、`xid`、`cid`、LSN 等类型可能表达数据库内部标识或复制协议坐标；它们不能仅按“整数”处理。作为普通业务列时可以选择明确的整数/文本/二进制承载，但如果字段参与复制位点、系统目录或内部语义，则必须与 ChangeEvent 的 Source Cursor 分离，并由 SourceAdapter 保持不透明。MySQL 的 `YEAR`、显示宽度、AUTO_INCREMENT 和 binlog wire encoding 也属于源定义证据，不能直接进入目标类型选择。

## 版本差异结论

- MySQL 5.7、8.0、8.4 的共同核心类型可复用语义族，但版本专属类型、校验行为、JSON/空间能力、字符集/排序规则和协议编码必须由版本 SourceTypeMapping 声明。MySQL 8.0 与 8.4 官方手册都将数值、时间、字符串、空间和 JSON 分组，但不应把 8.4 手册直接当作 5.7 的证据。
- PostgreSQL 15、16、17 的核心类型家族相近，但扩展、排序规则 provider、类型 OID、内置函数和具体服务器能力不能只按主版本名称推断。每个版本和目标实例都需要 Connector Identity、Server Build Identity 和能力证据。
- PostgreSQL 用户定义类型使“全部类型”成为开放集合。系统必须把类型目录定义、扩展版本和目标能力作为资格输入；无法解释的类型不能静默回退到 TEXT。

## 对当前代码的直接结论

当前 [`crates/change_event/src/model.rs`](../../crates/change_event/src/model.rs) 已包含 Array、Struct、Map、Range、MultiRange、Spatial、Json、Enum 和 Set 的初步表达，`LogicalType` 也有相应递归分支。当前 PostgreSQL 15 SourceTypeMapping [`crates/postgresql_15/src/type_mapping.rs`](../../crates/postgresql_15/src/type_mapping.rs) 仍只识别常见标量、JSONB、UUID、位串、时间和受限 spatial declaration；数组、复合、domain、range/multirange 和多数扩展类型会被拒绝。当前 PostgreSQL 15 Sink Manifest [`crates/postgresql_15/src/compatibility.rs`](../../crates/postgresql_15/src/compatibility.rs) 明确没有为数组等递归值提供目标表示，SQL renderer 对 SET 和 Spatial 仍返回 Target Capability Failure。

因此下一张决策票据必须先确定公共模型和“值完整保留”的承诺，之后才能为每种类型增加目标表示；不能先把 PostgreSQL `SET` 或空间列改成 `text/bytea`，再倒推它们已经兼容。

## 资格测试分类

每个类型方向至少需要分别记录：

1. Source Capture：原生协议能否读取并生成完整 LogicalValue；
2. Event Validation：类型定义、值边界、presence state 和事务结构是否通过；
3. Sink Native Equivalent：目标端是否保留原生类型行为；
4. Sink Value Preservation：目标端是否保留值和必要的结构元数据；
5. Explicit Conversion：用户确认后是否允许已声明的语义损失；
6. Locator Safety：字段是否可以安全用于主键、唯一键和 Row Locator；
7. Recovery：失败回滚、重试、CommitUnknown 和 checkpoint 是否保持原子性。

## 研究依据

- [MySQL 8.4 Reference Manual: Data Types](https://dev.mysql.com/doc/refman/8.4/en/data-types.html)
- [MySQL 8.0 Reference Manual: Data Types](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)
- [MySQL 5.7 Reference Manual: Data Types](https://dev.mysql.com/doc/refman/5.7/en/data-types.html)
- [PostgreSQL 15: Chapter 8 Data Types](https://www.postgresql.org/docs/15/datatype.html)
- [PostgreSQL 16: Chapter 8 Data Types](https://www.postgresql.org/docs/16/datatype.html)
- [PostgreSQL 17: Chapter 8 Data Types](https://www.postgresql.org/docs/17/datatype.html)
- [PostgreSQL 17: The PostgreSQL Type System](https://www.postgresql.org/docs/17/extend-type-system.html)
- [PostgreSQL 17: CREATE TYPE](https://www.postgresql.org/docs/17/sql-createtype.html)
- [PostgreSQL 17: User-Defined Types](https://www.postgresql.org/docs/17/xtypes.html)
