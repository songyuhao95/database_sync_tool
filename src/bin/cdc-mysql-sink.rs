use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::Duration;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy)]
enum TargetVersion {
    V5_7,
    V8_0,
    V8_4,
}

impl TargetVersion {
    fn parse(value: &str) -> AppResult<Self> {
        match value {
            "5.7" | "5_7" => Ok(Self::V5_7),
            "8.0" | "8_0" => Ok(Self::V8_0),
            "8.4" | "8_4" => Ok(Self::V8_4),
            _ => Err(message("target version must be 5.7, 8.0 or 8.4")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::V5_7 => "5.7",
            Self::V8_0 => "8.0",
            Self::V8_4 => "8.4",
        }
    }

    fn test_port(self) -> u16 {
        match self {
            Self::V5_7 => 33061,
            Self::V8_0 => 33062,
            Self::V8_4 => 33063,
        }
    }
}

struct Config {
    log: PathBuf,
    target: TargetVersion,
    host: String,
    port: u16,
    user: String,
    password: String,
    follow: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("[sink:error] {error}");
        process::exit(1);
    }
}

fn run() -> AppResult<()> {
    let Some(config) = Config::read()? else {
        return Ok(());
    };
    let file = File::open(&config.log)?;
    let mut reader = change_event::JsonReader::new(BufReader::new(file));
    eprintln!(
        "[sink:start] log={} target={}:{} mysql={} mode={}",
        config.log.display(),
        config.host,
        config.port,
        config.target.label(),
        if config.follow { "follow" } else { "once" }
    );

    let mut applied = 0usize;
    loop {
        if let Some(transaction) = reader.next_transaction()? {
            let (transaction_id, statements) = match config.target {
                TargetVersion::V5_7 => {
                    let plan = mysql_5_7::sql(&transaction)?;
                    let result = mysql_5_7::execute(
                        &mysql_5_7::TargetConfig {
                            host: config.host.clone(),
                            port: config.port,
                            user: config.user.clone(),
                            password: config.password.clone(),
                        },
                        &plan,
                    )?;
                    (result.source_transaction_id, result.statements_executed)
                }
                TargetVersion::V8_0 => {
                    let plan = mysql_8_0::sql(&transaction)?;
                    let result = mysql_8_0::execute(
                        &mysql_8_0::TargetConfig {
                            host: config.host.clone(),
                            port: config.port,
                            user: config.user.clone(),
                            password: config.password.clone(),
                        },
                        &plan,
                    )?;
                    (result.source_transaction_id, result.statements_executed)
                }
                TargetVersion::V8_4 => {
                    let plan = mysql_8_4::sql(&transaction)?;
                    let result = mysql_8_4::execute(
                        &mysql_8_4::TargetConfig {
                            host: config.host.clone(),
                            port: config.port,
                            user: config.user.clone(),
                            password: config.password.clone(),
                        },
                        &plan,
                    )?;
                    (result.source_transaction_id, result.statements_executed)
                }
            };
            applied += 1;
            eprintln!(
                "[sink:commit] transaction={} statements={} applied_transactions={}",
                transaction_id, statements, applied
            );
            continue;
        }

        if !config.follow {
            reader.finish()?;
            eprintln!("[sink:end] applied_transactions={applied}");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(250));
    }
}

impl Config {
    fn read() -> AppResult<Option<Self>> {
        let mut log = PathBuf::from(
            env::var("CDC_CHANGE_EVENT_LOG").unwrap_or_else(|_| "change_event.log".into()),
        );
        let mut target =
            TargetVersion::parse(&env::var("CDC_TARGET_MYSQL").unwrap_or_else(|_| "8.4".into()))?;
        let mut host = env::var("CDC_TARGET_HOST").unwrap_or_else(|_| "192.168.0.10".into());
        let mut port = env::var("CDC_TARGET_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?;
        let mut user = env::var("CDC_TARGET_USER").unwrap_or_else(|_| "mysql_writer".into());
        let mut follow = false;

        let mut args = env::args().skip(1);
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "-h" | "--help" => {
                    help();
                    return Ok(None);
                }
                "--change-event-log" => log = PathBuf::from(next(&mut args, &argument)?),
                "--target-mysql" => target = TargetVersion::parse(&next(&mut args, &argument)?)?,
                "--host" => host = next(&mut args, &argument)?,
                "--port" => port = Some(next(&mut args, &argument)?.parse()?),
                "--user" => user = next(&mut args, &argument)?,
                "--follow" => follow = true,
                _ => return Err(message(format!("unknown argument: {argument}"))),
            }
        }

        let password = env::var("CDC_MYSQL_WRITER_PASSWORD")
            .or_else(|_| env::var("CDC_TARGET_PASSWORD"))
            .map_err(|_| message("set CDC_MYSQL_WRITER_PASSWORD or CDC_TARGET_PASSWORD"))?;

        Ok(Some(Self {
            log,
            target,
            host,
            port: port.unwrap_or_else(|| target.test_port()),
            user,
            password,
            follow,
        }))
    }
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> AppResult<String> {
    args.next()
        .ok_or_else(|| message(format!("{flag} requires a value")))
}

fn message(text: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(text.into()))
}

fn help() {
    println!(
        "\
Apply complete ChangeEvent JSONL transactions to one MySQL target.

USAGE:
    cdc-mysql-sink [OPTIONS]

OPTIONS:
    --change-event-log PATH  default: CDC_CHANGE_EVENT_LOG or change_event.log
    --target-mysql VERSION   5.7, 8.0 or 8.4; default: CDC_TARGET_MYSQL or 8.4
    --host HOST              default: CDC_TARGET_HOST or 192.168.0.10
    --port PORT              default: CDC_TARGET_PORT or 33061/33062/33063 by version
    --user USER              default: CDC_TARGET_USER or mysql_writer
    --follow                 wait for newly appended committed transactions

PASSWORD:
    CDC_MYSQL_WRITER_PASSWORD or CDC_TARGET_PASSWORD (never accepted as an argument)

Without --follow, the input must end after a complete transaction commit."
    );
}
