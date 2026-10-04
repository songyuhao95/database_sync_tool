# 固定回归测试

Issue #57 的完整 6×6 类型与恢复资格矩阵使用 `./scripts/qualify.ps1`。
它分别记录 Native Equivalent、Value Preserved、Explicit Conversion、
UNSUPPORTED、BLOCKED、REQUIRES_LIVE 和 FAIL，并保留稳定摘要、密码命中检查、
`types.json`、`recovery.json` 和日志。PostgreSQL 16/17 有独立离线 Source/Sink
fixture；PG15/16/17 也各有独立真实 Source 和 Sink 套件。离线通过不代表
live qualification；缺少对应实例或凭据时仍标记为 `REQUIRES_LIVE`。
使用 `-Live -ConfigFile ./scripts/test.txt` 可执行已登记的 live 组件测试，
未配置实例或尚缺完整方向证据时保持 REQUIRES_LIVE。
扩展规则、证据边界和报告格式见 [类型资格测试](../docs/testing/type-qualification.md)。

在仓库根目录的 PowerShell 运行。真机测试默认从同目录的 `test.txt` 读取连接信息；该文件已被 Git 忽略，修改密码时只需更新这个文件。

```powershell
# 本地：固定 ChangeEvent 校验、JSON 往返、各版本 SQL 预期
.\scripts\test.ps1

# 真机：从 scripts/test.txt 读取账号密码
.\scripts\test.ps1 -Live

# 单项 / 单版本；-List 只列出将运行的测试
.\scripts\test.ps1 -Live -Database mysql_8_4 -Stage Read
.\scripts\test.ps1 -Database mysql_5_7,mysql_8_0 -Stage Sql
.\scripts\test.ps1 -Live -Database postgresql_15 -Stage ChangeEvent
.\scripts\test.ps1 -Live -Database mysql_5_7,mysql_8_0,mysql_8_4 -Stage Web
# 按登记的 suite 精确运行，避免重跑矩阵中的其他路线
.\scripts\test.ps1 -Live -Database mysql_5_7 -Stage Web -SuiteId mysql_5_7.web_same_version_all_types

# 临时使用另一个配置文件
.\scripts\test.ps1 -Live -ConfigFile .\my-test.txt
```

`test.txt` 使用 `KEY=VALUE` 格式，支持空行和以 `#` 开头的注释。值不要加引号；等号后的内容会原样作为配置值。未知字段、重复字段、空值或格式错误会立即停止测试。已经设置的同名进程环境变量优先于文件，方便 CI 临时覆盖。

`-SuiteId` 可按 `test-matrix.json` 中的 suite ID 过滤。多个 ID 可用逗号分隔；无效 ID 会在测试前报错，`-List` 可预览筛选结果。

控制台只显示最终覆盖矩阵和 `Report` 路径。Cargo 编译过程、每项测试输出和失败详情保存在该次报告目录的日志文件中。

每次生成 `target/test-results/<时间-随机标识>/summary.json`、各用例日志、真机读取的 ChangeEvent JSONL，以及 MySQL 原始协议事件摘要。失败继续检查其他版本，最终退出码为 1；连接失败、筛选到零个测试都算失败。缺少凭据或未运行的真机测试显示 REQUIRES_LIVE，不会算作通过。

`qualify.ps1 -Live` 的 `all_types_live_qualified` 只有在离线 6×6 矩阵、完整逐类型清单、六个 Source/Sink live suite、事务恢复、能力失效和必需路由证据均通过时才为 `true`。Source 和 Sink 的逐类型证据按 ChangeEvent 解耦组合；实际 Web 路由和同版本 MySQL 组件组合分别报告，不能把组件组合说成 Web 端到端通过。

如果只修复资格报告生成逻辑，可以用 `reassess-type-qualification.ps1` 基于已保存的 live `summary.json`、重新生成的 `types.json` 及两份路由审计重算门禁。复评报告记录四个输入文件的 SHA-256，并明确不重新执行数据库测试；原始报告保持不变。组件组合证据只证明适配器可以配合工作；“所有类型可同步”的最终门禁还要求每条方向、每种源类型都有实际 Web 兼容计划回执，缺失时 `qualified=false`。

| 阶段 | 本地检查 | 真机检查 |
|---|---|---|
| Read | 真机必需 | 复制协议、版本、MySQL 自动选择/GTID/文件位点、PG15/16/17 LSN、重新读取与恢复 |
| ChangeEvent | 校验、完整事务、JSON 往返、无效位点与缺失值拒绝 | 真实 INSERT/UPDATE/DELETE、事务多行、回滚排除、精确值、复合主键变化；PG15/16/17 DEFAULT/FULL、TOAST、JSONB |
| Sql | 固定事件 × 三种 MySQL 来源 × 六种 Sink 目标，匹配预期 SQL | MySQL 与 PG15/16/17 Sink 均写入并查询六种 Source fixture；校验重复键整事务回滚及 PG checkpoint 重启恢复 |

Read 和 ChangeEvent 共用一次真机捕获，脚本不会重复执行同一用例。MySQL 当前还需要查询源表元信息，本地检查不代表已验证二进制解码。三种 MySQL 来源的 SQL 矩阵使用固定事件；这不是九条真实实例之间的端到端迁移测试。Web 阶段运行登记的代表性端到端页面预检、创建、启动和写入测试。同版本 MySQL 的真实 Web 测试要求在 `test.txt` 配置对应的 `CDC_MYSQL57_SINK_HOST/PORT`、`CDC_MYSQL80_SINK_HOST/PORT`、`CDC_MYSQL84_SINK_HOST/PORT` 独立目标端；没有独立目标时，逐类型 Source + ChangeEvent + Sink 证据及离线 6×6 方向共同验证该适配器组合，报告明确标记 `COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE` 并保留缺失的 Web 实测计划数。全量和故障注入测试不属于这些阶段；其他本地回归仍可执行 `cargo test --workspace`。

`scripts/qualify.ps1 -Live` 还运行并单独报告目标能力失效：保存 MySQL 5.7 → PostgreSQL 15 计划后，测试改变本次创建的目标表定义，验证重新预检将计划标记为 stale，且任务无法启动。该项需要 MySQL 5.7 与 PostgreSQL 15 的 reader、writer、admin 配置；无配置时报告 `REQUIRES_LIVE`。

真机范围仅 `CDC_test` 中本次唯一命名的表/PG schema、publication、slot，正常结束及断言失败时清理；进程被强制杀死时可能残留，日志记录对象名称。读取使用 reader，测试造数使用 writer，PG 对象准备使用 postgres。没有全局锁，不操作现有业务表或任务位点。MySQL 未开启 GTID 时检验显式 GTID 拒绝及自动回退，不修改全局配置；当前环境 GTID 开启时，不声称已真机验证关闭 GTID 的环境。

默认的 `test.txt` 已填写当前测试环境：192.168.0.10，MySQL 33061/33062/33063 和 PG15 54321，以及对应 reader/writer/admin 账号。PG16/17 使用单独的 `PG_CDC16_*` / `PG_CDC17_*` 配置，字段为 HOST、PORT、ADMIN_USER、READER_USER、WRITER_USER、TEST_PASSWORD。PG 三个账号共用各自版本的测试密码变量；进程环境变量仍可覆盖任意字段。

新增数据库：实现对应的公开接口测试，在 `test-matrix.json` 登记数据库及 Read/ChangeEvent/Sql 用例。未实现的阶段放入 unsupported；脚本对已声明支持却缺少用例的阶段报 MISSING_TEST。预期 SQL 需人工核对后修改，测试不会自动覆盖预期文件。
