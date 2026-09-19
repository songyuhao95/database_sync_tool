# 固定回归测试

Issue #37 的完整 6×6 类型与恢复资格矩阵使用 `./scripts/qualify.ps1`。
它按字段分别输出 EXACT、RANGE_CHECKED、EXPLICIT_CONVERSION、UNSUPPORTED/BLOCKED，
并保留 `summary.json`、`types.json`、`recovery.json` 和日志。
PostgreSQL 16/17 明确为 UNSUPPORTED；离线通过不代表 live qualification。
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

# 临时使用另一个配置文件
.\scripts\test.ps1 -Live -ConfigFile .\my-test.txt
```

`test.txt` 使用 `KEY=VALUE` 格式，支持空行和以 `#` 开头的注释。值不要加引号；等号后的内容会原样作为配置值。未知字段、重复字段、空值或格式错误会立即停止测试。已经设置的同名进程环境变量优先于文件，方便 CI 临时覆盖。

控制台只显示最终覆盖矩阵和 `Report` 路径。Cargo 编译过程、每项测试输出和失败详情保存在该次报告目录的日志文件中。

每次生成 `target/test-results/<时间-随机标识>/summary.json`、各用例日志、真机读取的 ChangeEvent JSONL，以及 MySQL 原始协议事件摘要。失败继续检查其他版本，最终退出码为 1；连接失败、筛选到零个测试都算失败。缺少凭据或未运行的真机测试显示 REQUIRES_LIVE，不会算作通过。

| 阶段 | 本地检查 | 真机检查 |
|---|---|---|
| Read | 真机必需 | 复制协议、版本、MySQL 自动选择/GTID/文件位点、PG LSN、重新读取与恢复 |
| ChangeEvent | 校验、完整事务、JSON 往返、无效位点与缺失值拒绝 | 真实 INSERT/UPDATE/DELETE、事务多行、回滚排除、精确值、复合主键变化；PG DEFAULT/FULL、TOAST、JSONB |
| Sql | 同一固定事件 × 三种 MySQL 来源 × 三种目标版本，分别匹配固定 SQL 文件 | 每次 INSERT/UPDATE/DELETE 后查询目标数据、重复键错误整事务回滚 |

Read 和 ChangeEvent 共用一次真机捕获，脚本不会重复执行同一用例。MySQL 当前还需要查询源表元信息，本地检查不代表已验证二进制解码。三种 MySQL 来源的 SQL 矩阵使用固定事件；这不是九条真实实例之间的端到端迁移测试。全量、故障注入和 Web 测试暂不属于这三个阶段；其他本地回归仍可执行 `cargo test --workspace`。

真机范围仅 `CDC_test` 中本次唯一命名的表/PG schema、publication、slot，正常结束及断言失败时清理；进程被强制杀死时可能残留，日志记录对象名称。读取使用 reader，测试造数使用 writer，PG 对象准备使用 postgres。没有全局锁，不操作现有业务表或任务位点。MySQL 未开启 GTID 时检验显式 GTID 拒绝及自动回退，不修改全局配置；当前环境 GTID 开启时，不声称已真机验证关闭 GTID 的环境。

默认的 `test.txt` 已填写当前测试环境：192.168.0.10，MySQL 33061/33062/33063，PG15 54321，以及对应 reader/writer/admin 账号。支持的完整字段可直接查看该文件。PG 三个账号当前共用 `PG_CDC_TEST_PASSWORD`；进程环境变量仍可覆盖任意字段。

新增数据库：实现对应的公开接口测试，在 `test-matrix.json` 登记数据库及 Read/ChangeEvent/Sql 用例。未实现的阶段放入 unsupported；脚本对已声明支持却缺少用例的阶段报 MISSING_TEST。预期 SQL 需人工核对后修改，测试不会自动覆盖预期文件。
