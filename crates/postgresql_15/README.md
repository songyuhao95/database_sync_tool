# PostgreSQL 15 增量读取

普通 SQL 使用 SQLx；复制连接与 pgoutput 解析使用 pg_walstream 0.8.1。调用 `postgresql_15::replication(config)`，再调用 `next_transaction(&cancel)` 获取通过 change_event 校验的完整已提交事务。

## 启动与验证

先以 postgres 在 192.168.0.10:54321 的 CDC_test 中执行 `examples/postgresql15_setup.sql`。首次在仓库根目录启动：

```powershell
$env:PGPASSWORD = '你的 postgresql_reader 密码'
cargo run -p postgresql_15 --bin cdc-pg-tail -- --create-slot --output .\postgresql-15-change_event.log
```

另开 PowerShell：

```powershell
Get-Content .\postgresql-15-change_event.log -Tail 20 -Wait
```

使用 postgresql_writer 逐条执行 `examples/postgresql15_dml.sql`。Ctrl+C 停止，后续启动去掉 `--create-slot`；省略 `--output` 可直接在控制台输出 JSON。`--help` 列出参数；`--start-lsn X/Y` 指定现有槽的读取位置，`--source-id ID` 校验前一次打印的来源身份。

默认地址为 192.168.0.10:54321，数据库 CDC_test，用户 postgresql_reader，publication/slot 均为 cdc_pg15_demo。密码由 PGPASSWORD 提供。

## 位点与范围

这是 Source 诊断读取器：输出 JSON 不代表 Sink 提交，CLI 不确认消费 LSN，重启会重读未确认的事务。复制槽会保留 WAL；不再测试时，管理员可执行 `SELECT pg_drop_replication_slot('cdc_pg15_demo');` 释放它。

库的 `acknowledge(&transaction)` 只供后续持久化消费者使用，必须在数据与进度持久化后调用。来源身份包含 system identifier、timeline、database OID，不提供自动故障切换。行事件 source_cursor 是事务结束 LSN；begin_cursor 是 BEGIN 消息携带的最终提交记录 LSN。缺槽、过期槽、正在使用的槽，以及早于 confirmed_flush_lsn 的起点均报错，恢复时不自动建槽或跳到最新位置。

支持 PostgreSQL 15 / UTF8 / pgoutput v1，普通永久主键表、发布全部字段、REPLICA IDENTITY DEFAULT 或 FULL。支持整数、numeric（最多 1000 位/小数位）、浮点、布尔、UUID、text/char/varchar、bytea、date、timestamp、timestamptz、JSONB。旧值未提供标为 unavailable，未变化 TOAST 标为 unchanged，均不等于 NULL。

TRUNCATE、生成列、分区/非普通表、RLS、行/列过滤、数组、普通 JSON、interval 和自定义类型明确拒绝。暂未接入 DDL 同步与全量。事务输入默认上限 32 MiB、最多 100000 行；超限停读且不确认位点。

## Sink 写入

postgresql_15::sql 将完整 ChangeEvent 事务转换为 PostgreSQL 15 SQL；postgresql_15::execute 在一个 Sink 事务内执行。UPDATE 和 DELETE 必须携带主键，未变化的 TOAST 字段不会出现在 SET 中。Sink 要求提前创建同名且类型兼容的普通永久表；用户触发器和 RLS 暂不支持。

ChangeEvent 输出为 v0.3，来源字段为 id，新增 database、unavailable、unchanged、boolean、uuid；JsonReader 仍可读 MySQL v0.1 的 server_uuid。`Replication` 同时实现数据库无关的 `change_event::SourceAdapter`，供同步 worker 调用；异步调用方继续使用带取消令牌的 `next_transaction`。

## 回归测试

```powershell
cargo test --workspace
$env:PG_CDC_TEST_PASSWORD = '测试账号当前共用的密码'
cargo test -p postgresql_15 --test live_capture -- --ignored --nocapture
```

真机测试只操作 CDC_test 中唯一命名的专用对象，并在结束后清理；postgres 准备对象、postgresql_writer 写数据、postgresql_reader 捕获。
