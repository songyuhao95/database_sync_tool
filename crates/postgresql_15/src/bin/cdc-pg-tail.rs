use postgresql_15::{CancellationToken, Config, Result};
use std::{
    env,
    fs::OpenOptions,
    io::{self, Write},
};
#[tokio::main]
async fn main() -> Result<()> {
    let mut c = Config::new(
        "192.168.0.10",
        54321,
        "CDC_test",
        "postgresql_reader",
        env::var("PGPASSWORD").unwrap_or_default(),
        "cdc_pg15_demo",
        "cdc_pg15_demo",
    );
    let mut output = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            println!(
                "cdc-pg-tail [--host HOST] [--port PORT] [--database DB] [--user USER] [--publication NAME] [--slot NAME] [--create-slot] [--start-lsn LSN] [--source-id ID] [--output FILE]\nPassword: PGPASSWORD. First run: --create-slot. Later runs: existing slot. JSON output is diagnostic and does NOT acknowledge WAL; replays are expected."
            );
            return Ok(());
        }
        if arg == "--create-slot" {
            c.create_slot = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| io::Error::other(format!("missing value for {arg}")))?;
        match arg.as_str() {
            "--host" => c.host = value,
            "--port" => c.port = value.parse()?,
            "--database" => c.database = value,
            "--user" => c.username = value,
            "--publication" => c.publication = value,
            "--slot" => c.slot = value,
            "--start-lsn" => c.start_lsn = Some(value),
            "--source-id" => c.expected_source_id = Some(value),
            "--output" => output = Some(value),
            _ => return Err(io::Error::other(format!("unknown option {arg}")).into()),
        }
    }
    if c.password.is_empty() {
        return Err(io::Error::other("set PGPASSWORD first").into());
    }
    let password = c.password.clone();
    let mut capture = match postgresql_15::replication(c).await {
        Ok(c) => c,
        Err(e) => {
            return Err(io::Error::other(e.to_string().replace(&password, "[redacted]")).into());
        }
    };
    eprintln!(
        "[stream] PostgreSQL {} source={} start={} (Ctrl+C to stop; no WAL acknowledgement)",
        capture.source().version,
        capture.source().id,
        capture.start_lsn()
    );
    let mut out: Box<dyn Write> = match output {
        Some(path) => Box::new(OpenOptions::new().create(true).append(true).open(path)?),
        None => Box::new(io::stdout()),
    };
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    loop {
        match capture.next_transaction(&cancel).await {
            Ok(tx) => {
                out.write_all(change_event::json(&tx)?.as_bytes())?;
                out.flush()?;
            }
            Err(_) if cancel.is_cancelled() => break,
            Err(e) => {
                handle.abort();
                return Err(
                    io::Error::other(e.to_string().replace(&password, "[redacted]")).into(),
                );
            }
        }
    }
    handle.abort();
    eprintln!("[stream] stopped; the persistent slot is retained for replay");
    Ok(())
}
