use crate::decoder::Decoder;
use crate::type_mapping::validate_native_type;
use change_event::{ChangeTransaction, Source, ValidatedTransaction};
use mysql_common::packets::Sid;
use mysql_driver::binlog::events::{Event, EventData};
use mysql_driver::{BinlogDumpFlags, BinlogRequest, Conn, OptsBuilder, prelude::Queryable};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct BinlogPosition {
    pub file: String,
    pub position: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinlogStartMode {
    Auto,
    Gtid,
    Position,
}

impl BinlogStartMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Gtid => "gtid",
            Self::Position => "position",
        }
    }
}

/// Explicit configuration; this library never reads process environment variables.
pub struct BinlogConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub server_id: u32,
    pub start_mode: BinlogStartMode,
    pub start: Option<BinlogPosition>,
    pub gtid_set: Option<String>,
    pub non_blocking: bool,
    /// Protocol event limit; a partial final transaction is an error.
    pub max_events: Option<usize>,
    /// Optional local append-only log of raw Replication Protocol events.
    pub binlog_log_path: Option<PathBuf>,
    /// Cancellation discards a buffered, uncommitted transaction. Resume from the caller's checkpoint.
    pub stop: Option<Arc<AtomicBool>>,
    /// Fail closed on schema/statement events when used by a row-only replication worker.
    pub reject_statements: bool,
    /// Only decode these tables. Empty means unrestricted capture for the CLI.
    pub tables: Vec<(String, String)>,
}
impl BinlogConfig {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            server_id: 2_000_000_u32.saturating_add(std::process::id()),
            start_mode: BinlogStartMode::Auto,
            start: None,
            gtid_set: None,
            non_blocking: false,
            max_events: None,
            binlog_log_path: None,
            stop: None,
            reject_statements: true,
            tables: Vec::new(),
        }
    }
}

/// Blocking iterator of complete committed transactions. Errors end the stream.
/// Protocol controls and DDL are not emitted by this row-only capture iteration.
pub struct BinlogStream {
    native: mysql_driver::BinlogStream,
    decoder: Decoder,
    source: Source,
    start: BinlogPosition,
    start_mode: BinlogStartMode,
    count: usize,
    max_events: Option<usize>,
    raw_log: Option<BufWriter<File>>,
    raw_file: String,
    raw_position: u64,
    ended: bool,
    stop: Option<Arc<AtomicBool>>,
    start_gtid_set: Option<String>,
}
impl BinlogStream {
    pub fn start_gtid_set(&self) -> Option<&str> {
        self.start_gtid_set.as_deref()
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn start_position(&self) -> &BinlogPosition {
        &self.start
    }
    pub fn start_mode(&self) -> BinlogStartMode {
        self.start_mode
    }
    pub fn protocol_events(&self) -> usize {
        self.count
    }
}
impl Iterator for BinlogStream {
    type Item = io::Result<ChangeTransaction>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        loop {
            if self
                .stop
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Acquire))
            {
                self.ended = true;
                return None;
            }
            if self.max_events.is_some_and(|limit| self.count >= limit) {
                self.ended = true;
                return self.decoder.ensure_complete().err().map(Err);
            }
            match self.native.next() {
                None => {
                    self.ended = true;
                    return self.decoder.ensure_complete().err().map(Err);
                }
                Some(Err(error)) => {
                    self.ended = true;
                    if self
                        .stop
                        .as_ref()
                        .is_some_and(|stop| stop.load(Ordering::Acquire))
                    {
                        return None;
                    }
                    return Some(Err(io::Error::other(error)));
                }
                Some(Ok(event)) => {
                    self.count += 1;
                    if let Err(error) = self.log_raw_event(&event) {
                        self.ended = true;
                        return Some(Err(error));
                    }
                    match self.decoder.decode(&event) {
                        Ok(Some(tx)) if tx.changes.is_empty() => return Some(Ok(tx)),
                        Ok(Some(tx)) => match validate_change_event(tx) {
                            Ok(validated) => return Some(Ok(validated.transaction().clone())),
                            Err(error) => {
                                self.ended = true;
                                return Some(Err(error));
                            }
                        },
                        Ok(None) => {}
                        Err(error) => {
                            self.ended = true;
                            return Some(Err(error));
                        }
                    }
                }
            }
        }
    }
}

/// Validate one decoded MySQL transaction at the SourceAdapter boundary.
pub fn validate_change_event(tx: ChangeTransaction) -> io::Result<ValidatedTransaction> {
    let validated = change_event::validate(tx).map_err(io::Error::other)?;
    mysql_source_contract::validate_source(&validated, mysql_source_contract::MysqlVersion::V57)
        .map_err(io::Error::other)?;
    for column in validated
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
    {
        validate_native_type(&column.native_type).map_err(io::Error::other)?;
    }
    Ok(validated)
}

impl BinlogStream {
    /// Return the next non-empty, fully validated v0.3 transaction.
    pub fn next_change_event(&mut self) -> io::Result<Option<ValidatedTransaction>> {
        loop {
            match self.next() {
                None => return Ok(None),
                Some(Err(error)) => return Err(error),
                Some(Ok(transaction)) if transaction.changes.is_empty() => continue,
                Some(Ok(transaction)) => return validate_change_event(transaction).map(Some),
            }
        }
    }
}

impl change_event::SourceAdapter for BinlogStream {
    type Error = io::Error;

    fn next_transaction(&mut self) -> io::Result<Option<ValidatedTransaction>> {
        self.next_change_event()
    }
}
impl std::iter::FusedIterator for BinlogStream {}

impl BinlogStream {
    fn log_raw_event(&mut self, event: &Event) -> io::Result<()> {
        let header = event.header();
        let event_type = header
            .event_type()
            .map(|value| format!("{value:?}"))
            .unwrap_or_else(|error| format!("UNKNOWN_EVENT({})", error.0));
        let rotation = match event.read_data() {
            Ok(Some(EventData::RotateEvent(rotate))) => {
                Some((rotate.name().into_owned(), rotate.position()))
            }
            _ => None,
        };
        let event_position = rotation.as_ref().map_or(self.raw_position, |_| 0);
        let next_position = rotation
            .as_ref()
            .map_or(u64::from(header.log_pos()), |(_, position)| *position);
        if let Some(log) = self.raw_log.as_mut() {
            writeln!(
                log,
                "[{sequence:06}] file={file} event_position={event_position} next_position={next_position} timestamp={timestamp} type={event_type} server_id={server_id} event_size={event_size} flags=0x{flags:04X} payload_bytes={payload_bytes}",
                sequence = self.count,
                file = self.raw_file,
                event_position = event_position,
                next_position = next_position,
                timestamp = header.timestamp(),
                server_id = header.server_id(),
                event_size = header.event_size(),
                flags = header.flags_raw(),
                payload_bytes = event.data().len(),
            )?;
            log.flush()?;
        }
        if let Some((file, position)) = rotation {
            self.raw_file = file;
            self.raw_position = position;
        } else {
            self.raw_position = u64::from(header.log_pos());
        }
        Ok(())
    }
}

/// Only metadata queries and replication-session setup are executed on the source.
pub fn binlog(config: BinlogConfig) -> io::Result<BinlogStream> {
    if config.server_id == 0 || config.max_events == Some(0) {
        return Err(io::Error::other(
            "server_id and max_events must be greater than zero",
        ));
    }
    if config
        .start
        .as_ref()
        .is_some_and(|p| p.file.is_empty() || p.position < 4 || p.position > u32::MAX as u64)
    {
        return Err(io::Error::other("invalid binlog file/position"));
    }
    if config.start.is_some() && config.gtid_set.is_some() {
        return Err(io::Error::other(
            "binlog position and GTID set are mutually exclusive",
        ));
    }
    if config.start_mode == BinlogStartMode::Gtid && config.start.is_some() {
        return Err(io::Error::other(
            "GTID start mode cannot be combined with binlog file/position",
        ));
    }
    if config.start_mode == BinlogStartMode::Position && config.gtid_set.is_some() {
        return Err(io::Error::other(
            "position start mode cannot be combined with a GTID set",
        ));
    }
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(config.host.clone()))
        .tcp_port(config.port)
        .user(Some(config.user))
        .pass(Some(config.password))
        .tcp_connect_timeout(Some(Duration::from_secs(10)))
        .read_timeout(Some(Duration::from_secs(30)))
        .write_timeout(Some(Duration::from_secs(10)));
    let mut conn = Conn::new(opts.clone()).map_err(|e| {
        io::Error::other(format!(
            "could not connect to MySQL at {}:{}: {e}",
            config.host, config.port
        ))
    })?;
    let (version, log_bin, format, image, server_uuid): (String, u8, String, String, String) = conn
        .query_first(
            "SELECT VERSION(), @@GLOBAL.log_bin, @@GLOBAL.binlog_format,
            @@GLOBAL.binlog_row_image, @@GLOBAL.server_uuid",
        )
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("source preflight returned no row"))?;
    if !version.starts_with("5.7.") {
        return Err(io::Error::other(format!(
            "mysql_5_7 requires MySQL 5.7; source is {version}"
        )));
    }
    if log_bin != 1 || format != "ROW" || image != "FULL" {
        return Err(io::Error::other(
            "mysql_5_7 requires log_bin=ON, binlog_format=ROW, binlog_row_image=FULL",
        ));
    }
    let (file, position, _, _, executed_gtids): (String, u64, String, String, String) = conn
        .query_first("SHOW MASTER STATUS")
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("SHOW MASTER STATUS returned no binlog"))?;
    let current_position = BinlogPosition { file, position };
    let gtid_mode: String = conn
        .query_first("SELECT @@GLOBAL.GTID_MODE")
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("GTID preflight returned no row"))?;
    let (start_mode, start, sids) = resolve_start(
        config.start_mode,
        config.start,
        config.gtid_set.as_deref(),
        current_position,
        &gtid_mode,
        &executed_gtids,
    )?;
    let start_gtid_set = (start_mode == BinlogStartMode::Gtid)
        .then(|| config.gtid_set.clone().unwrap_or(executed_gtids));
    let metadata = Conn::new(opts).map_err(io::Error::other)?;
    let mut raw_log = config
        .binlog_log_path
        .map(|path| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map(BufWriter::new)
        })
        .transpose()?;
    if let Some(log) = raw_log.as_mut() {
        writeln!(
            log,
            "# stream_start mode={} file={} position={} source_version={} server_uuid={}",
            start_mode.label(),
            start.file,
            start.position,
            version,
            server_uuid
        )?;
        log.flush()?;
    }
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .map_err(io::Error::other)?;
    conn.query_drop("SET @master_heartbeat_period = 1000000000")
        .map_err(io::Error::other)?;
    let mut request = BinlogRequest::new(config.server_id)
        .with_filename(start.file.as_bytes().to_vec())
        .with_pos(start.position);
    if start_mode == BinlogStartMode::Gtid {
        request = request
            .with_filename(Vec::new())
            .with_pos(4u64)
            .with_use_gtid(true)
            .with_sids(sids);
    }
    if config.non_blocking {
        request = request.with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
    }
    let native = conn.get_binlog_stream(request).map_err(io::Error::other)?;
    let source = Source {
        kind: "mysql".into(),
        version: version.clone(),
        id: server_uuid.clone(),
    };
    let mut decoder = Decoder::new(start.file.clone(), metadata, version, server_uuid);
    decoder.reject_statements = config.reject_statements;
    decoder.capture_tables = config.tables;
    Ok(BinlogStream {
        native,
        decoder,
        source,
        start: start.clone(),
        start_mode,
        count: 0,
        max_events: config.max_events,
        raw_log,
        raw_file: start.file.clone(),
        raw_position: start.position,
        ended: false,
        stop: config.stop,
        start_gtid_set,
    })
}

fn resolve_start(
    requested_mode: BinlogStartMode,
    requested_position: Option<BinlogPosition>,
    requested_gtid: Option<&str>,
    current_position: BinlogPosition,
    gtid_mode: &str,
    executed_gtids: &str,
) -> io::Result<(BinlogStartMode, BinlogPosition, Vec<Sid<'static>>)> {
    match requested_mode {
        BinlogStartMode::Position => Ok((
            BinlogStartMode::Position,
            requested_position.unwrap_or(current_position),
            Vec::new(),
        )),
        BinlogStartMode::Gtid => {
            if !gtid_mode_enabled(gtid_mode) {
                return Err(io::Error::other(format!(
                    "GTID start requested but source GTID_MODE={gtid_mode}"
                )));
            }
            let value = requested_gtid.unwrap_or(executed_gtids);
            let sids = parse_gtid_set(value)?;
            Ok((BinlogStartMode::Gtid, current_position, sids))
        }
        BinlogStartMode::Auto => {
            if let Some(position) = requested_position {
                return Ok((BinlogStartMode::Position, position, Vec::new()));
            }
            if gtid_mode_enabled(gtid_mode) {
                let value = requested_gtid.unwrap_or(executed_gtids);
                let sids = parse_gtid_set(value)?;
                return Ok((BinlogStartMode::Gtid, current_position, sids));
            }
            Ok((BinlogStartMode::Position, current_position, Vec::new()))
        }
    }
}

fn gtid_mode_enabled(value: &str) -> bool {
    value.eq_ignore_ascii_case("ON") || value.eq_ignore_ascii_case("ON_PERMISSIVE")
}

fn parse_gtid_set(value: &str) -> io::Result<Vec<Sid<'static>>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|sid| !sid.is_empty())
        .map(|sid| {
            Sid::from_str(sid).map_err(|error| {
                io::Error::other(format!("invalid GTID set entry {sid:?}: {error}"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current_position() -> BinlogPosition {
        BinlogPosition {
            file: "mysql-bin.000001".into(),
            position: 123,
        }
    }

    #[test]
    fn auto_prefers_gtid_when_enabled() {
        let (mode, position, sids) = resolve_start(
            BinlogStartMode::Auto,
            None,
            None,
            current_position(),
            "ON",
            "430c326c-ab91-11f1-a23b-0242ac160004:1-10",
        )
        .unwrap();
        assert_eq!(mode, BinlogStartMode::Gtid);
        assert_eq!(position.position, 123);
        assert_eq!(sids.len(), 1);
    }

    #[test]
    fn auto_falls_back_to_position_when_gtid_is_disabled() {
        let (mode, _, sids) = resolve_start(
            BinlogStartMode::Auto,
            None,
            None,
            current_position(),
            "OFF",
            "430c326c-ab91-11f1-a23b-0242ac160004:1-10",
        )
        .unwrap();
        assert_eq!(mode, BinlogStartMode::Position);
        assert!(sids.is_empty());
    }

    #[test]
    fn explicit_gtid_rejects_disabled_source() {
        let error = resolve_start(
            BinlogStartMode::Gtid,
            None,
            Some("430c326c-ab91-11f1-a23b-0242ac160004:1"),
            current_position(),
            "OFF",
            "",
        )
        .unwrap_err();
        assert!(error.to_string().contains("GTID_MODE=OFF"));
    }
}
