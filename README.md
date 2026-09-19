# CDC binlog tail

## Web 控制台

`web_ui` crate 负责浏览器页面、鉴权 API 和 SQLite 控制数据。当前保存登录账号、
会话、显示主题、数据库实例和同步任务配置；采集与目的端写入仍由数据库版本 crate 执行。
登录密码使用 Argon2id 保存，实例的读取/写入密码使用 AES-256-GCM 加密，API 只返回
“是否已配置密码”，不会把密码发回浏览器。

首次启动不需要提前设置平台密码：

```powershell
cargo run --release -p web_ui --bin cdc-web
```

默认仅监听 `127.0.0.1:8080`，控制数据库和自动生成的密钥位于 `data/`。然后访问
`http://127.0.0.1:8080`，使用默认账号 `admin / admin` 登录，并立即在“设置 → 账号管理”
中点击该账号的“修改”，设置至少 12 字节的新密码。默认账号只在 SQLite 中没有任何账号时创建，重启不会重置密码。
之后从“添加实例”录入 MySQL 或 PostgreSQL 15 地址及分工明确的读取、写入账号。
首页的“探测”只使用读取账号查询版本和对应连接器的元信息；Web 通过独立的 Source/Sink
注册表发布可用版本与能力，不按源端–目的端组合分派运行时适配器。

“添加任务”使用源实例的读取账号和目的实例的写入账号加载真实库表。当前支持选择
1–100 张同库同名的 InnoDB 或 PostgreSQL 主键表；创建前会重新检查源端复制能力、起点要求、
主键、列名、列类型、可空性、排序规则和生成属性。任务创建后保存为“待启动”，在
“同步任务”列表或详情页点击“开始”后持续同步；“停止”会等待当前目的端事务结束。
详情页的“自动开始任务”默认不勾选；管理员勾选后立即保存，下次 CDC Web 服务启动时自动运行。
保存开关不会立即开始或停止任务；手动停止仍保留开关，下次服务启动会再次运行。
自动开始沿用现有检查和位点恢复，失败后显示错误，等待手动处理或下次服务启动。
页面自动刷新运行状态、已提交行数和复制位点。详情页可切换“源端日志”和“写入日志”，
各自保留滚动位置；在底部时跟随新日志，查看历史时不会自动跳转，可点击“查看最新”恢复跟随。
管理员可在任务列表删除已停止的任务；删除只移除平台任务及页面日志记录，
已同步的业务数据、目的端 `CDC.log_info` 记录和文件日志保留。

首次开始任务时，目的实例自动创建 InnoDB 表 `CDC.log_info`，每个任务一行，
以 `task_id` 为主键。业务数据和复制位点在同一个目的端事务中提交。
保存内容包括源实例 UUID、任务配置绑定、读取模式、binlog 文件/position 或 PostgreSQL LSN、GTID 集合、
最后事务 ID 和提交计数。GTID 是源端读取方式，目的端无需启用 GTID。
MySQL Source 新任务首次启动先全量、再从快照位点跟踪增量；PostgreSQL 15 Source 当前为增量起步，
使用任务专属 logical replication slot。已有增量位点的任务继续从目的端恢复。
全量要求目的表预先建好、为空、使用 InnoDB 且有主键，第一版不支持目的端外键或触发器。
源端使用读取账号执行短暂 `FLUSH TABLES WITH READ LOCK`（需要 RELOAD 或对应 FLUSH 权限），
取得文件/position 和完整 GTID 集合，在同一边界建立 REPEATABLE READ 只读快照后立即解锁。
全部表共享快照并按主键每批最多 256 行扫描；每批原始值上限 16 MiB，超出会失败而非截断。
全量写入使用一个目的端事务，与 `CDC.log_info.phase=incremental` 一起提交；
中断则整体回滚，下次重新全量，不会自动清空目的表。大数据量会形成大事务，第一版尚不支持全量断点续扫。
锁连接单独使用 `lock_wait_timeout=5`、`wait_timeout=10`；初始化预算 8 秒，失败丢弃快照。
`wait_timeout` 是锁连接的服务端空闲超时，不是全局锁绝对 10 秒租约；原连接解锁未确认时禁止使用快照。
快照期间持有选中表的元数据锁，允许业务 DML，DDL 需等待快照结束。
源端 binlog 保留时间必须覆盖全量过程；丢失所需历史时任务报错，不跳过缺失数据。
再次开始从目的端已保存位点恢复，
SQLite 仅保留页面使用的副本。确认丢失时任务停止，重新开始会先核实目的端记录；
控制记录缺失或身份不匹配时拒绝建立新起点。每个任务只允许一个写入连接。

源端继续使用 `mysql_reader`；目的端 `mysql_writer` 还需有创建 `CDC` 库、
`log_info` 表及读取/更新其记录的权限。控制库不出现在可选业务库中。
当前仅支持兼容的 InnoDB/PostgreSQL 主键表，不同步 DDL；目的表有触发器时会停止。
恢复仍要求源端保留需要的 binlog，日志重置或恢复到其他历史不能按普通重启处理。

在目的实例查看位点：

```sql
SELECT task_id, mode, binlog_file, binlog_position, gtid_set,
       phase, snapshot_rows, snapshot_gtids,
       last_transaction_id, applied_transactions, applied_rows, updated_at
FROM CDC.log_info;
```

每个任务的文件日志位于 SQLite 同级目录的 `task-logs/<任务ID>/`：
`mysql-<版本>-binlog.log`、`change_event.log`、`write.log`。
默认可用 `Get-Content .\data\task-logs\<任务ID>\write.log -Tail 20 -Wait` 持续查看。
页面交互回归：运行 `node tests/web/serve.cjs`，打开 `http://127.0.0.1:18082`，
点击“运行全部交互回归”。该页面使用模拟数据，不连接数据库。

自动测试（仅在测试环境创建唯一命名的表，随后清理）：

```powershell
$env:CDC_MYSQL_WRITER_PASSWORD = '<测试写入密码>'
$env:CDC_MYSQL_READER_PASSWORD = '<测试读取密码>'
cargo test --test snapshot_integration -- --ignored --nocapture
cargo test --test checkpoint_integration -- --ignored --nocapture
cargo test -p web_ui live_tasks_resume_from_sink_in_both_modes -- --ignored --nocapture
```

需要自管密钥时，把 URL-safe Base64 编码的 32 字节随机密钥放入
`CDC_WEB_SECRET_KEY`，并做好独立备份；SQLite 与密钥必须成对
恢复。非本机访问必须由 HTTPS 反向代理提供 TLS，并把 `CDC_WEB_ORIGIN` 设置为浏览器
实际访问的 origin，同时保留原始 `Host` 请求头。

当前迭代通过 MySQL Replication Protocol 连接 MySQL 5.7、8.0 和 8.4，把已提交的行事务
转换成一行一个 JSON 的 `cdc.change-event-json.v0.1` 格式。读取起点默认优先使用
GTID；源端没有启用 GTID 时自动回退到 binlog 文件和 position。

`mysql_reader` 是源端账号，只用于查询源端元数据和持续读取 binlog；
主程序不会执行任何修改语句。`mysql_writer` 不参与源端读取，它留给目的端
写入。本仓库的测试数据生成器只在测试实例上临时使用该账号制造 binlog 事件。

## Workspace crate

数据库版本按 crate 拆分，版本 crate 只负责该数据库的协议、解码和 SQL 方言：

- `change_event`：统一 `ChangeTransaction`、校验和 JSON Lines 编码，不依赖数据库驱动。
- `mysql_5_7`：MySQL 5.7 Replication Protocol 读取、binlog 解码和 MySQL 5.7 SQL。
- `mysql_8_0`：MySQL 8.0 Replication Protocol 读取、binlog 解码和 MySQL 8.0 SQL。
- `mysql_8_4`：MySQL 8.4 Replication Protocol 读取、binlog 解码和 MySQL 8.4 SQL。
- `postgresql_15`：PostgreSQL 15 logical replication 读取、pgoutput 解码和 PostgreSQL 15 SQL。

调用关系保持为版本 crate 直接暴露方法，例如
`mysql_5_7::binlog(config)`、`change_event::validate(transaction)` 和
`mysql_8_4::sql(&validated)` 和 `mysql_8_4::execute(&config, &sql)`。新增数据库版本时，增加对应版本
crate，不把数据库驱动类型带入 `change_event`。

## SQL 输出

在保留 JSON 输出的同时，可以把已提交行事务输出为参数化 SQL 诊断脚本：

```powershell
$env:CDC_MYSQL_PASSWORD = '<password>'

cargo run --release -- --output sql --target-mysql 8.0
cargo run --release -- --output sql --target-mysql 8.4
```

SQL stdout 按事务输出 `START TRANSACTION`、带 `?` 占位符的行变更语句和 `COMMIT`；
它只用于诊断，实际执行计划会把值交给目标驱动绑定。INSERT 保留严格插入语义，
重复键会报错并回滚，不会覆盖目的端已有数据。UPDATE 和 DELETE 只使用 ChangeEvent
中的源主键定位，执行前锁定并确认恰好存在一行。生成列不会出现在 INSERT 或 UPDATE
赋值中。

当前首版要求源表和目的表已经创建、同名、类型兼容且使用 InnoDB，并且源表必须有
主键。支持整数、DECIMAL、FLOAT/DOUBLE、CHAR/VARCHAR/TEXT、BINARY/BLOB、DATE、
DATETIME、TIME、TIMESTAMP、YEAR 和标准 JSON。ENUM、SET、BIT、GEOMETRY、无主键表，
以及无法无损表达的 MySQL JSON 扩展标量会明确报错。

ChangeEvent 现在携带主键顺序、生成列标记和排序规则。`change_event::JsonReader` 按
begin/row/commit 重组并重新校验事务；截断、乱序、混入其他事务或旧格式日志不会提交
到目的端。

## 目的端写入

先在目的实例上创建与源端同名、类型兼容的 InnoDB 表。然后分别启动源读取器和目的端
写入器。以下示例把 MySQL 5.7 的 `CDC_test` 行变更写入 MySQL 8.0：

```powershell
# 第一个 PowerShell：源端读取
$env:CDC_MYSQL_PASSWORD = '<mysql_reader password>'
cargo run --release --bin cdc-binlog-tail -- `
  --source-mysql 5.7 `
  --change-event-log .\change_event.log

# 第二个 PowerShell：目的端持续写入
$env:CDC_MYSQL_WRITER_PASSWORD = '<mysql_writer password>'
cargo run --release --bin cdc-mysql-sink -- `
  --change-event-log .\change_event.log `
  --target-mysql 8.0 `
  --host 192.168.0.10 `
  --port 33062 `
  --follow
```

把 `--target-mysql` 和端口换成 `5.7/33061` 或 `8.4/33063`，即可使用对应版本
crate 的 SQL 转换和执行模块。不传 `--follow` 时，写入器处理完当前日志后退出，并要求
文件结尾恰好是完整事务。

本轮升级了 ChangeEvent JSON 格式。以前的 `cdc.change-event-json.v0` 日志缺少主键和
生成列信息，不能用于目的端写入；测试前请先备份并换用一个新的空日志。当前 Sink 尚未
持久化消费点，重启后会从日志开头重新读取，严格 INSERT 会在已应用事务处报重复键。
持久化消费点和事务去重是下一轮恢复能力，不应通过 upsert 掩盖。

## 运行

源库需要启用 binlog。用于订阅的账号至少需要 `REPLICATION SLAVE`，
自动读取启动点位还需要 `REPLICATION CLIENT`。为了显示真实列名，读取账号
还要有测试库的 `SELECT` 权限。

测试实例需要由管理员执行一次：

```sql
CREATE DATABASE IF NOT EXISTS CDC_test
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT SELECT, SHOW VIEW ON CDC_test.* TO 'mysql_reader'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER
  ON CDC_test.* TO 'mysql_writer'@'%';
```

PowerShell：

```powershell
$env:CDC_MYSQL_PASSWORD = '<password>'

cargo run --release -- --output json
```

程序默认使用 `mysql_reader` 连接 `192.168.0.10:33061`（MySQL 5.7）。启动时先检查
`GTID_MODE` 和 `GTID_EXECUTED`，在 GTID 可用时发送 `COM_BINLOG_DUMP_GTID`，随后持续
等待新事件；如果 GTID 不可用，则使用当前 binlog 文件和 position。MySQL 5.7/8.0
使用 `SHOW MASTER STATUS`，MySQL 8.4 使用 `SHOW BINARY LOG STATUS`。通过
`--source-mysql 8.0` 或
`--source-mysql 8.4` 切换源版本，端口默认自动切换到 33062 或 33063；也可以显式传
`--port`。默认将原始 binlog 事件追加到对应的
`mysql-<source-version>-binlog.log`，将已提交事务的 JSON ChangeEvent 追加到
`change_event.log`；运行状态写到 stderr。去掉 `--non-blocking` 就是常驻跟随模式，按
`Ctrl+C` 停止。

在 Windows 上可以分别打开两个 PowerShell 窗口持续查看日志：

```powershell
Get-Content .\mysql-5.7-binlog.log -Tail 20 -Wait
Get-Content .\change_event.log -Tail 20 -Wait
```

也可以用 `--binlog-log PATH` 和 `--change-event-log PATH` 修改日志文件路径。

可以手动选择读取起点：

```powershell
# 强制使用源端当前 GTID_EXECUTED
cargo run --release -- --start-mode gtid

# 手动指定 GTID 集合
cargo run --release -- --start-mode gtid `
  --gtid-set '430c326c-ab91-11f1-a23b-0242ac160004:1-100'

# 强制使用 binlog 文件和 position
cargo run --release -- --start-mode position `
  --binlog-file mysql-bin.000005 --binlog-pos 2188
```

`--binlog-file/--binlog-pos` 与 `--gtid-set` 不能同时使用。`--start-mode auto`
是默认值；显式提供文件和 position 时会使用 position，显式提供 GTID 集合时会
优先使用该集合。

MySQL 8.0 和 8.4 的启动示例：

```powershell
cargo run --release -- --source-mysql 8.0
cargo run --release -- --source-mysql 8.4
```

对应日志为 `mysql-8.0-binlog.log`、`mysql-8.4-binlog.log` 和同一个
`change_event.log`。如果同时运行多个读取器，给每个进程传入不同的
`--change-event-log` 和 `--binlog-log` 路径。

当前 v0.1 输出 INSERT、UPDATE、DELETE 行变更；DDL、heartbeat 和其他控制事件暂不
伪造成 RowChange。完整 v1 所需的 lineage、Schema Fingerprint、事件摘要和
SchemaChange 会在后续迭代加入。当前三个 MySQL 版本都要求 `ROW`、`FULL` 行镜像；
8.0/8.4 遇到事务压缩、部分 JSON 行更新或 XA 事务会停止并报错，避免静默丢失数据。

仓库带有一个最小测试数据生成器，所有对象统一放在 `CDC_test`：

```powershell
$env:CDC_MYSQL_WRITER_PASSWORD = '<password>'
cargo run --example generate_test_events
```

它会创建 `CDC_test.binlog_reader_demo`，依次执行一次插入、更新和删除。
数据库与测试表保留，测试行最终会被删除。启动读取器后再运行生成器，即可实时看到
对应事务的 JSON ChangeEvent。

手工测试 `CDC_test.binlog_reader_demo`：

```sql
CREATE DATABASE IF NOT EXISTS CDC_test
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS CDC_test.binlog_reader_demo (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  message VARCHAR(255) NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  metadata JSON NULL,
  changed_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

INSERT INTO CDC_test.binlog_reader_demo
  (message, amount, metadata, changed_at)
VALUES ('manual insert', 12.34, JSON_OBJECT('stage', 'insert'), NOW(6));

UPDATE CDC_test.binlog_reader_demo
SET message = 'manual update',
    amount = 56.78,
    metadata = JSON_OBJECT('stage', 'update'),
    changed_at = NOW(6)
WHERE message = 'manual insert';

DELETE FROM CDC_test.binlog_reader_demo
WHERE message = 'manual update';

DROP TABLE CDC_test.binlog_reader_demo;
```

查看全部参数：

```powershell
cargo run -- --help
```

密码只从 `CDC_MYSQL_PASSWORD` 环境变量读取，不接受命令行密码参数。

## PostgreSQL 15 增量读取

Web 任务支持 MySQL 5.7/8.0/8.4 与 PostgreSQL 15 作为独立的 Source 或 Sink。PostgreSQL 15
Source 当前只提供增量读取，不执行全量快照；实例必须已配置 logical replication、UTF8、发布
`cdc_pg15_demo`，并允许 Web 读取账号使用该发布。PostgreSQL LSN 会写入目的端 `cdc.log_info`
并用于恢复。

已增加 `postgresql_15` crate：SQLx 访问元数据，pg_walstream 读取 pgoutput，转换为已校验的 ChangeEvent JSON。启动及测试见 [PostgreSQL 15 使用说明](crates/postgresql_15/README.md)。
