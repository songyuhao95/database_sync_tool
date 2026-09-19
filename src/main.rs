mod format;
use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process;

use format::{MysqlVersion, OutputFormat, StartMode};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    server_id: u32,
    start_mode: StartMode,
    binlog_file: Option<String>,
    binlog_pos: Option<u64>,
    gtid_set: Option<String>,
    max_events: Option<usize>,
    non_blocking: bool,
    output: OutputFormat,
    source_mysql: MysqlVersion,
    binlog_log: PathBuf,
    change_event_log: PathBuf,
}

fn main() {
    match Config::from_env_and_args() {
        Ok(Some(config)) => {
            if let Err(error) = run(config) {
                eprintln!("[error] {error}");
                process::exit(1);
            }
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!("[error] {error}");
            eprintln!("Run with --help to see usage.");
            process::exit(2);
        }
    }
}

impl Config {
    fn from_env_and_args() -> AppResult<Option<Self>> {
        let mut host = env::var("CDC_MYSQL_HOST").unwrap_or_else(|_| "192.168.0.10".to_owned());
        let mut port = parse_env("CDC_MYSQL_PORT")?;
        let mut source_mysql = env::var("CDC_SOURCE_MYSQL")
            .unwrap_or_else(|_| "5.7".into())
            .parse::<MysqlVersion>()
            .map_err(message)?;
        let mut user = env::var("CDC_MYSQL_USER").unwrap_or_else(|_| "mysql_reader".to_owned());
        let mut server_id = parse_env("CDC_MYSQL_SERVER_ID")?
            .unwrap_or_else(|| 2_000_000_u32.saturating_add(process::id()));
        let mut start_mode = env::var("CDC_START_MODE")
            .unwrap_or_else(|_| "auto".into())
            .parse::<StartMode>()
            .map_err(message)?;
        let mut binlog_file = env::var("CDC_BINLOG_FILE").ok();
        let mut binlog_pos = parse_env("CDC_BINLOG_POS")?;
        let mut gtid_set = env::var("CDC_GTID_SET").ok();
        let mut max_events = parse_env("CDC_MAX_EVENTS")?;
        let mut non_blocking = parse_bool_env("CDC_NON_BLOCKING")?.unwrap_or(false);
        let mut output_name = env::var("CDC_OUTPUT").unwrap_or_else(|_| "json".to_owned());
        let mut target_mysql = env::var("CDC_TARGET_MYSQL")
            .ok()
            .map(|value| value.parse::<MysqlVersion>().map_err(message))
            .transpose()?
            .unwrap_or(MysqlVersion::V8_4);
        let mut binlog_log = env::var_os("CDC_BINLOG_LOG").map(PathBuf::from);
        let mut change_event_log = PathBuf::from(
            env::var("CDC_CHANGE_EVENT_LOG").unwrap_or_else(|_| "change_event.log".to_owned()),
        );

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    print_help();
                    return Ok(None);
                }
                "--host" => host = next_value(&mut args, "--host")?,
                "--port" => port = Some(parse_flag(&next_value(&mut args, "--port")?, "--port")?),
                "--source-mysql" => {
                    source_mysql = next_value(&mut args, "--source-mysql")?
                        .parse()
                        .map_err(message)?
                }
                "--user" => user = next_value(&mut args, "--user")?,
                "--server-id" => {
                    server_id = parse_flag(&next_value(&mut args, "--server-id")?, "--server-id")?
                }
                "--start-mode" => {
                    start_mode = next_value(&mut args, "--start-mode")?
                        .parse()
                        .map_err(message)?
                }
                "--binlog-file" => binlog_file = Some(next_value(&mut args, "--binlog-file")?),
                "--binlog-pos" => {
                    binlog_pos = Some(parse_flag(
                        &next_value(&mut args, "--binlog-pos")?,
                        "--binlog-pos",
                    )?)
                }
                "--gtid-set" => gtid_set = Some(next_value(&mut args, "--gtid-set")?),
                "--max-events" => {
                    max_events = Some(parse_flag(
                        &next_value(&mut args, "--max-events")?,
                        "--max-events",
                    )?)
                }
                "--non-blocking" => non_blocking = true,
                "--binlog-log" => {
                    binlog_log = Some(PathBuf::from(next_value(&mut args, "--binlog-log")?))
                }
                "--change-event-log" => {
                    change_event_log = PathBuf::from(next_value(&mut args, "--change-event-log")?)
                }
                "--output" => output_name = next_value(&mut args, "--output")?,
                "--target-mysql" => {
                    target_mysql = next_value(&mut args, "--target-mysql")?
                        .parse::<MysqlVersion>()
                        .map_err(message)?
                }
                "--password" => {
                    return Err(message(
                        "--password is intentionally unsupported; set CDC_MYSQL_PASSWORD instead",
                    ));
                }
                _ => return Err(message(format!("unknown argument: {arg}"))),
            }
        }

        if binlog_file.is_some() != binlog_pos.is_some() {
            return Err(message(
                "CDC_BINLOG_FILE/--binlog-file and CDC_BINLOG_POS/--binlog-pos must be supplied together",
            ));
        }
        if binlog_file.is_some() && gtid_set.is_some() {
            return Err(message(
                "binlog file/position and GTID set cannot be used together",
            ));
        }
        if start_mode == StartMode::Gtid && binlog_file.is_some() {
            return Err(message(
                "--start-mode gtid cannot be combined with --binlog-file/--binlog-pos",
            ));
        }
        if start_mode == StartMode::Position && gtid_set.is_some() {
            return Err(message(
                "--start-mode position cannot be combined with --gtid-set",
            ));
        }
        if gtid_set
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(message("GTID set must not be empty"));
        }
        if server_id == 0 {
            return Err(message("replication server id must be greater than zero"));
        }
        if binlog_pos.is_some_and(|pos| pos < 4 || pos > u32::MAX as u64) {
            return Err(message("binlog position must be between 4 and 4294967295"));
        }
        if binlog_file.as_ref().is_some_and(|file| file.is_empty()) {
            return Err(message("binlog filename must not be empty"));
        }
        if max_events == Some(0) {
            return Err(message("max-events must be greater than zero"));
        }

        let output = match output_name.to_ascii_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "sql" => OutputFormat::Sql(target_mysql),
            _ => {
                return Err(message(format!(
                    "unsupported output {output_name:?}; expected json or sql"
                )));
            }
        };

        let password = env::var("CDC_MYSQL_PASSWORD").map_err(|_| {
            message("set CDC_MYSQL_PASSWORD; passwords are never accepted as arguments")
        })?;

        Ok(Some(Self {
            host,
            port: port.unwrap_or_else(|| source_mysql.default_port()),
            user,
            password,
            server_id,
            start_mode,
            binlog_file,
            binlog_pos,
            gtid_set,
            max_events,
            non_blocking,
            output,
            source_mysql,
            binlog_log: binlog_log.unwrap_or_else(|| source_mysql.binlog_log().into()),
            change_event_log,
        }))
    }
}

fn run(config: Config) -> AppResult<()> {
    let change_event_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.change_event_log)?;
    let mut change_event_writer = BufWriter::new(change_event_file);
    eprintln!(
        "[startup] connecting source={}:{} user={} replication_server_id={}",
        config.host, config.port, config.user, config.server_id
    );
    // Each version owns its config and native stream. The CLI only combines their public API.
    macro_rules! capture {
        ($adapter:ident) => {{
            let mut capture = $adapter::BinlogConfig::new(
                config.host.clone(), config.port, config.user.clone(), config.password.clone());
            capture.server_id = config.server_id;
            capture.start_mode = match config.start_mode {
                StartMode::Auto => $adapter::BinlogStartMode::Auto,
                StartMode::Gtid => $adapter::BinlogStartMode::Gtid,
                StartMode::Position => $adapter::BinlogStartMode::Position,
            };
            capture.non_blocking = config.non_blocking;
            capture.max_events = config.max_events;
            capture.gtid_set = config.gtid_set.clone();
            capture.binlog_log_path = Some(config.binlog_log.clone());
            capture.start = config.binlog_file.clone().zip(config.binlog_pos)
                .map(|(file, position)| $adapter::BinlogPosition { file, position });
            let stream = $adapter::binlog(capture)?;
            eprintln!("[source] version={} server_uuid={} log_bin=ON binlog_format=ROW binlog_row_image=FULL",
                stream.source().version, stream.source().id);
            eprintln!("[stream] starting file={} position={} mode={}",
                stream.start_position().file, stream.start_position().position,
                format!("{}; {}", stream.start_mode().label(),
                    if config.non_blocking { "catch-up" } else { "follow" }));
            Box::new(stream)
                as Box<dyn change_event::SourceAdapter<Error = io::Error>>
        }};
    }
    let mut stream = match config.source_mysql {
        MysqlVersion::V5_7 => capture!(mysql_5_7),
        MysqlVersion::V8_0 => capture!(mysql_8_0),
        MysqlVersion::V8_4 => capture!(mysql_8_4),
    };
    eprintln!(
        "[stream] connected; binlog_log={} change_event_log={} output={} (Ctrl+C to stop)",
        config.binlog_log.display(),
        config.change_event_log.display(),
        config.output.label()
    );
    let mut count = 0;
    let stdout = io::stdout();
    let mut sql_writer = stdout.lock();
    while let Some(validated) = stream.next_transaction()? {
        let json = change_event::json(&validated)?;
        change_event_writer.write_all(json.as_bytes())?;
        change_event_writer.flush()?;
        if let OutputFormat::Sql(target) = config.output {
            let sql = match target {
                MysqlVersion::V5_7 => mysql_5_7::sql(&validated)?.script(),
                MysqlVersion::V8_0 => mysql_8_0::sql(&validated)?.script(),
                MysqlVersion::V8_4 => mysql_8_4::sql(&validated)?.script(),
            };
            sql_writer.write_all(sql.as_bytes())?;
            sql_writer.flush()?;
        }
        count += 1;
    }
    eprintln!("[stream] ended committed_transactions={count}");
    Ok(())
}

fn parse_env<T>(name: &str) -> AppResult<Option<T>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    env::var(name)
        .ok()
        .map(|value| value.parse::<T>().map_err(Into::into))
        .transpose()
}

fn parse_bool_env(name: &str) -> AppResult<Option<bool>> {
    env::var(name)
        .ok()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(message(format!(
                "{name} must be one of true/false, 1/0, yes/no, or on/off"
            ))),
        })
        .transpose()
}

fn parse_flag<T>(value: &str, flag: &str) -> AppResult<T>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    value
        .parse()
        .map_err(|error| message(format!("invalid value for {flag}: {error}")))
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> AppResult<String> {
    args.next()
        .ok_or_else(|| message(format!("{flag} requires a value")))
}

fn message(text: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(text.into()))
}

fn print_help() {
    println!(
        "\
Tail a MySQL 5.7, 8.0 or 8.4 binary log through the Replication Protocol.

USAGE:
    cdc-binlog-tail [OPTIONS]

CONNECTION:
    --host HOST              default: CDC_MYSQL_HOST or 192.168.0.10
    --source-mysql VERSION   5.7 (default), 8.0 or 8.4; may use CDC_SOURCE_MYSQL
    --port PORT              CDC_MYSQL_PORT or 33061/33062/33063 by source version
    --user USER              default: CDC_MYSQL_USER or mysql_reader
    CDC_MYSQL_PASSWORD       required; never accepted on the command line

REPLICATION:
    --server-id ID           default: CDC_MYSQL_SERVER_ID or a process-based ID
    --start-mode MODE        auto (default), gtid or position; may use CDC_START_MODE
    --gtid-set SET           manual GTID set; may use CDC_GTID_SET
    --binlog-file FILE       start file; must be paired with --binlog-pos
    --binlog-pos POSITION    start position; must be paired with --binlog-file
    --non-blocking           read available events and exit instead of following
    --max-events COUNT       stop after COUNT protocol events (partial transaction errors)
    --binlog-log PATH        binlog event log (default: mysql-<source-version>-binlog.log)
    --change-event-log PATH  JSON ChangeEvent log (default: change_event.log)

OUTPUT:
    --output FORMAT          json (default) or sql to stdout; may use CDC_OUTPUT
    --target-mysql VERSION   5.7, 8.0 or 8.4; may use CDC_TARGET_MYSQL
                             (used when --output sql)

In auto mode, the program prefers GTID_MODE=ON/ON_PERMISSIVE and
GTID_EXECUTED, then falls back to the current binlog file and position when GTID
is unavailable. Use --start-mode gtid or --start-mode position to force a mode.
When forcing GTID without --gtid-set, the current GTID_EXECUTED is used. The
position mode reads SHOW MASTER STATUS (or SHOW BINARY LOG STATUS for 8.4).
Logs append and flush immediately. ChangeEvent log is always JSON, even with SQL output."
    );
}
