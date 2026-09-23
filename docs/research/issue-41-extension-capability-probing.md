# Issue 41：扩展类型和目标实例能力探测策略

## 研究范围

本记录服务于 [架构地图：MySQL 与 PostgreSQL 全类型兼容](https://github.com/songyuhao95/database_sync_tool/issues/39) 的票据“研究：扩展类型和目标实例能力探测策略”。目标是确定 SourceTypeMapping、Sink Capability Manifest 和实例预检需要哪些实际数据库证据。

## 结论

1. 数据库版本、连接成功和扩展可安装不是同一个事实。能力资格必须绑定到具体数据库、具体 Server Build Identity、扩展版本、目标 schema/column 定义以及必要的会话设置。
2. PostgreSQL 扩展必须区分“服务器文件可用”和“当前数据库已安装”。`pg_available_extensions` 只能说明可加载版本，`pg_extension` 才能说明当前数据库已经安装；安装扩展还可能创建类型、函数、运算符和索引支持方法，因此能力清单必须绑定当前 database 和 schema。
3. PostGIS 还需要检查 `postgis_full_version()` 的完整版本和编译组件。PostGIS geometry/geography 的可用性不能由 PostgreSQL 版本推断。
4. 类型能力探测必须递归读取 PostgreSQL 类型目录：base/enum/domain/composite/range、多维数组和 multirange 的元素或子类型、collation、typmod、约束和扩展所属。只识别一个 `udt_name` 会丢失递归定义。
5. 没有原生扩展能力时，可以提供明确的 BYTEA/TEXT/JSONB 值承载方案，但它必须是独立的 Capability Qualification，并说明空间函数、索引、约束、排序和查询语义不会被保留。

## PostgreSQL 探测证据

### 实例和扩展版本

连接后应在同一数据库、同一角色权限下读取：

```sql
SELECT version();
SELECT current_database(), current_user, current_schema();
SELECT name, default_version, installed_version, comment
FROM pg_available_extensions
WHERE installed_version IS NOT NULL OR name IN ('postgis', 'hstore');
SELECT extname, extversion, extnamespace::regnamespace::text
FROM pg_extension;
```

`pg_available_extensions`/`pg_available_extension_versions` 是 PostgreSQL 官方推荐的“可加载扩展”视图；`pg_extension` 是当前数据库实际安装记录。`CREATE EXTENSION` 会执行脚本并注册它创建的对象，因此扩展版本是目标能力的组成部分。依据：[PostgreSQL CREATE EXTENSION](https://www.postgresql.org/docs/17/sql-createextension.html)、[Packaging Related Objects into an Extension](https://www.postgresql.org/docs/17/extend-extensions.html)。

### 类型递归定义

目录访问需要至少覆盖以下字段：

```sql
SELECT
  n.nspname AS type_schema,
  t.oid,
  t.typname,
  t.typtype,
  t.typcategory,
  t.typbasetype,
  t.typelem,
  t.typrelid,
  t.typcollation,
  t.typtypmod,
  t.typnamespace::regnamespace::text AS type_namespace
FROM pg_type AS t
JOIN pg_namespace AS n ON n.oid = t.typnamespace;
```

然后按类型类别读取：

- `typtype = 'd'`：domain，读取 `pg_constraint` 的 CHECK/NOT NULL 约束以及底层 `typbasetype`；
- `typtype = 'e'`：enum，读取 `pg_enum` 的 label 和排序顺序；
- `typtype = 'c'`：composite，读取 `typrelid` 对应 `pg_attribute` 的字段顺序、字段类型和字段约束；
- `typtype = 'r'`：range，读取 `pg_range.rngsubtype`、collation、canonical/subtype diff 证据以及对应 multirange；
- `typelem != 0` 或 `typarray`：array，递归读取元素类型，保留维度、下界和 NULL；
- 扩展拥有的类型：通过 `pg_depend`/`pg_extension` 关联扩展身份，不能只按类型名判断。

PostgreSQL 官方类型系统明确区分 base、container、domain 和 pseudo-types；`CREATE TYPE` 明确支持 composite、enum、range、base 和 shell 形式。依据：[The PostgreSQL Type System](https://www.postgresql.org/docs/17/extend-type-system.html)、[CREATE TYPE](https://www.postgresql.org/docs/17/sql-createtype.html)。

### 目标列和会话前置条件

对每一个预先创建的目标列，能力预检还要读取：

```sql
SELECT
  table_schema,
  table_name,
  column_name,
  udt_schema,
  udt_name,
  data_type,
  domain_schema,
  domain_name,
  is_nullable,
  character_maximum_length,
  character_octet_length,
  numeric_precision,
  numeric_scale,
  datetime_precision,
  collation_schema,
  collation_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2;
```

此外，能力指纹至少要包含或引用：

- `server_version_num`、`version()` 的 build evidence；
- `server_encoding`、`lc_collate`、`lc_ctype`、`default_table_access_method` 等会影响解释或执行的环境项；
- 连接会话的 `TimeZone`、`DateStyle`、`IntervalStyle`、`standard_conforming_strings`、`bytea_output`；
- 目标 schema、目标列的 type OID/definition fingerprint、collation OID 和约束/索引摘要；
- 扩展名、安装版本、扩展 schema 及其能力探测结果。

仅改变 `search_path` 也可能改变自定义类型、函数和运算符解析，因此 SQL 执行必须使用固定、可审计的 schema qualification 或固定 Sink Session Profile。

## PostGIS 能力

目标连接需要在当前数据库执行：

```sql
SELECT PostGIS_Full_Version();
SELECT * FROM pg_available_extensions WHERE name = 'postgis';
SELECT postgis_version();
```

PostGIS 官方文档说明 `PostGIS_Full_Version()` 返回 PostGIS 版本和构建组件，并建议通过 `pg_available_extensions` 检查可用版本。依据：[PostGIS_Full_Version](https://postgis.net/docs/PostGIS_Full_Version.html)、[PostGIS Getting Started](https://postgis.net/documentation/getting_started/)。

空间能力资格至少要匹配：

- geometry 还是 geography；
- geometry subtype：Point、LineString、Polygon、Multi*、GeometryCollection；
- dimensions：XY、XYZ、XYM、XYZM；
- SRID/CRS 定义和轴序；
- WKB/EWKB 输入输出规则；
- 目标列约束、空间索引和 PostGIS/PROJ/GEOS 组件版本；
- 目标 Sink 是否需要执行 `ST_GeomFromWKB`、`ST_GeomFromEWKB` 或 geography 专用构造。

MySQL 空间能力也必须从源目录读取：空间列的 geometry subtype、SRID restriction、NULL 属性和空间索引。MySQL 官方文档说明 SRID 属性会限制列可接受的值并影响空间索引能力；空间索引还有 NOT NULL 和特定 SRID 条件。依据：[MySQL Spatial Data Types](https://dev.mysql.com/doc/refman/8.4/en/spatial-type-overview.html)、[MySQL Spatial Reference Systems](https://dev.mysql.com/doc/refman/8.0/en/spatial-reference-systems.html)、[MySQL Spatial Indexes](https://dev.mysql.com/doc/refman/8.4/en/creating-spatial-indexes.html)。

## hstore 能力

PostgreSQL `hstore` 是扩展提供的 key/value 类型，key 和 value 都是文本；其外部表示、NULL value、key 集合、索引和操作符都是类型语义的一部分。官方文档还说明它是 trusted extension，但这只表示安装权限条件，不表示其他数据库有等价类型。依据：[PostgreSQL hstore](https://www.postgresql.org/docs/17/hstore.html)。

建议能力分层：

| 目标能力 | 资格条件 | 承诺 |
|---|---|---|
| PostgreSQL 原生 hstore | 当前 DB 已安装 hstore，目标列确实是该扩展类型，版本和 schema 指纹匹配 | 保留 hstore 值和目标端 hstore 操作语义 |
| hstore → JSONB | 可验证 key/value/null 转换，目标列 JSONB，计划明确策略 | 保留结构化 key/value；不承诺 hstore 操作符、索引和外部表示 |
| hstore → TEXT | 只在文本外部表示规则固定并允许损失时提供 | 仅保留可逆或有明确规范的文本；不能声明结构化等价 |
| hstore → BYTEA | 只有在定义了稳定原始编码且 Sink 能恢复时才提供 | 仅值承载；不能用于普通查询或 Row Locator |

## MySQL 扩展/特殊能力

MySQL 空间类型和 SRID 是第一类类型能力；JSON、ENUM、SET 也不是普通字符串。MySQL 8.4 官方类型目录还包含 VECTOR 等版本相关类型，因此版本化 SourceTypeMapping 不能假设 5.7、8.0 和 8.4 的类型清单完全相同。对于存储引擎、插件或发行版专属类型，应通过 `INFORMATION_SCHEMA`、`SHOW PLUGINS`、`SHOW VARIABLES` 和列/表目录证据确认；无法证明完整语义时进入 Opaque 或阻断，而不是默认转 TEXT。

## 能力指纹与失效规则

`TargetCapabilityManifest` 和 `ColumnConversionPlan` 的指纹必须包含：

```text
ConnectorIdentity
ServerBuildIdentity
SemanticEnvironmentFingerprint
database + schema + column identity
target type OID / native definition fingerprint
extension name + installed version + extension schema
spatial SRID / CRS / geometry subtype / dimensions
collation and encoding
Sink Session Profile
Conversion Rule version and qualification evidence
```

以下变化必须让计划失效并阻断 Route，要求重新预检：

- PostgreSQL build、extension version、extension schema 或 PostGIS 组件变化；
- 目标列 native type、typmod、domain、collation、constraint、index 或 SRID 变化；
- 数据库/连接会话的编码、时区、日期格式、interval 或 bytea 输出策略变化；
- MySQL 源的字符集、排序规则、空间 SRID 限制、空间 subtype 或 storage/index 约束变化；
- Connector Identity、ChangeEvent contract、SourceTypeMapping 或 Capability Manifest 版本变化。

## 对实现的结论

1. 增加一个公共 `TargetCapabilityProbe` 结果模型，区分 detected、available、installed、qualified、missing、permission_denied 和 incompatible。
2. PostgreSQL Sink 的探测必须按 database 执行；不能只连接到实例默认数据库后把结果复用于其他数据库。
3. PostgreSQL reader/writer 账号需要能够读取 `pg_catalog`、`information_schema` 和扩展对象的目录元数据；若不能读取某项，返回“权限不足导致无法资格化”，不能返回兼容。
4. PostGIS、hstore、其他扩展都应作为独立 Capability Manifest 条目，不把扩展类型硬编码进 PostgreSQL 核心标量映射。
5. 无原生能力时，BYTEA/TEXT/JSONB 方案必须显示为“值承载/显式转换”，并禁止作为主键、唯一键或 Row Locator，除非另有比较等价证据。

## 研究依据

- [PostgreSQL 17 CREATE EXTENSION](https://www.postgresql.org/docs/17/sql-createextension.html)
- [PostgreSQL 17 Packaging Related Objects into an Extension](https://www.postgresql.org/docs/17/extend-extensions.html)
- [PostgreSQL 17 The PostgreSQL Type System](https://www.postgresql.org/docs/17/extend-type-system.html)
- [PostgreSQL 17 hstore](https://www.postgresql.org/docs/17/hstore.html)
- [PostGIS Full Version](https://postgis.net/docs/PostGIS_Full_Version.html)
- [PostGIS Getting Started](https://postgis.net/documentation/getting_started/)
- [MySQL 8.4 Spatial Data Types](https://dev.mysql.com/doc/refman/8.4/en/spatial-type-overview.html)
- [MySQL 8.0 Spatial Reference Systems](https://dev.mysql.com/doc/refman/8.0/en/spatial-reference-systems.html)
- [MySQL 8.4 Spatial Indexes](https://dev.mysql.com/doc/refman/8.4/en/creating-spatial-indexes.html)
