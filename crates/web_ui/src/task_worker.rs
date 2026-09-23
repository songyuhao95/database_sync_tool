//! Connects a task to the version-owned capture and atomic Sink writer APIs.
use crate::{
    Error, Result, Store,
    catalog::EndpointRole,
    registry::AdapterKind,
    runtime_store::{Checkpoint, Endpoint},
    tasks::TableMapping,
};
use change_event::{
    ChangeTransaction, ColumnConversionPlan, CommitResolution, SinkAdapter as _, SnapshotBatch,
    SnapshotBoundary, SnapshotTable, TargetApplyErrorKind, TargetCapabilityFailure,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    io,
    sync::mpsc::{Receiver, sync_channel},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

type Stream = Box<dyn Iterator<Item = io::Result<ChangeTransaction>>>;
type SnapshotStream = Box<dyn Iterator<Item = io::Result<SnapshotBatch>>>;
type SnapshotProgress<'a> = dyn FnMut(&SnapshotBatch, u64) -> io::Result<()> + 'a;

struct PostgresqlStream {
    receiver: Option<Receiver<io::Result<ChangeTransaction>>>,
    stop: Arc<AtomicBool>,
    reader_stop: Arc<AtomicBool>,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl Iterator for PostgresqlStream {
    type Item = io::Result<ChangeTransaction>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stop.load(Ordering::Acquire) {
            return None;
        }
        self.receiver.as_ref()?.recv().ok()
    }
}

impl Drop for PostgresqlStream {
    fn drop(&mut self) {
        self.reader_stop.store(true, Ordering::Release);
        self.receiver.take();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

async fn wait_for_stop(stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

fn parse_postgresql_lsn(value: &str) -> io::Result<u64> {
    let (upper, lower) = value
        .split_once('/')
        .ok_or_else(|| io::Error::other("invalid PostgreSQL LSN"))?;
    let upper = u64::from_str_radix(upper, 16).map_err(io::Error::other)?;
    let lower = u64::from_str_radix(lower, 16).map_err(io::Error::other)?;
    if upper > u32::MAX as u64 || lower > u32::MAX as u64 {
        return Err(io::Error::other("invalid PostgreSQL LSN"));
    }
    Ok((upper << 32) | lower)
}

fn postgresql_major(adapter: AdapterKind) -> Option<u16> {
    match adapter {
        AdapterKind::Postgresql15 => Some(15),
        AdapterKind::Postgresql16 => Some(16),
        AdapterKind::Postgresql17 => Some(17),
        _ => None,
    }
}

fn open_postgresql_source(
    endpoint: &Endpoint,
    task_id: &str,
    saved: Option<&Checkpoint>,
    stop: &Arc<AtomicBool>,
    major: u16,
) -> io::Result<(Stream, Checkpoint)> {
    if let Some(saved) = saved
        && saved.mode != "postgresql_lsn"
    {
        return Err(io::Error::other(
            "目的端保存的位点不是 PostgreSQL Source 位点",
        ));
    }
    let digest = format!("{:x}", Sha256::digest(task_id.as_bytes()));
    let slot = format!("cdc_web_{}", &digest[..16]);
    let mut config = postgresql_15::Config::new(
        &endpoint.host,
        endpoint.port,
        &endpoint.database,
        &endpoint.user,
        &endpoint.password,
        format!("cdc_pg{major}_demo"),
        slot,
    );
    config.create_slot = saved.is_none();
    config.start_lsn = saved.map(|checkpoint| checkpoint.file.clone());
    config.expected_source_id = saved.map(|checkpoint| checkpoint.source_uuid.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    let mut replication = runtime
        .block_on(postgresql_15::replication_for_version(config, major))
        .map_err(io::Error::other)?;
    drop(runtime);
    let source_uuid = replication.source().id.clone();
    let start_lsn = replication.start_lsn();
    let checkpoint = Checkpoint {
        source_uuid,
        sink_uuid: String::new(),
        mode: "postgresql_lsn".into(),
        file: start_lsn.clone(),
        position: parse_postgresql_lsn(&start_lsn)?,
        gtid_set: None,
        applied_transactions: 0,
        applied_rows: 0,
        phase: "incremental".into(),
        snapshot_rows: 0,
    };
    let (sender, receiver) = sync_channel(1);
    let reader_stop = Arc::new(AtomicBool::new(false));
    let reader_stop_thread = reader_stop.clone();
    let reader = std::thread::Builder::new()
        .name(format!(
            "cdc-pg-source-{}",
            &task_id[..task_id.len().min(8)]
        ))
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = sender.send(Err(io::Error::other(error)));
                    return;
                }
            };
            runtime.block_on(async move {
                let cancel = postgresql_15::CancellationToken::new();
                loop {
                    tokio::select! {
                        result = replication.next_transaction(&cancel) => match result {
                            Ok(transaction) => {
                                if sender.send(Ok(transaction.transaction().clone())).is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                if !reader_stop_thread.load(Ordering::Acquire) {
                                    let _ = sender.send(Err(io::Error::other(error)));
                                }
                                break;
                            }
                        },
                        _ = wait_for_stop(reader_stop_thread.clone()) => {
                            cancel.cancel();
                            break;
                        }
                    }
                }
            });
        })
        .map_err(io::Error::other)?;
    Ok((
        Box::new(PostgresqlStream {
            receiver: Some(receiver),
            stop: stop.clone(),
            reader_stop,
            reader: Some(reader),
        }) as Stream,
        checkpoint,
    ))
}
trait Sink {
    fn check_snapshot_targets(&mut self, tables: &[SnapshotTable]) -> io::Result<()>;
    fn prepare_snapshot(&mut self, boundary: &SnapshotBoundary) -> io::Result<Checkpoint>;
    fn copy_snapshot(
        &mut self,
        tables: &[SnapshotTable],
        batches: &mut dyn Iterator<Item = io::Result<SnapshotBatch>>,
        progress: &mut SnapshotProgress<'_>,
    ) -> io::Result<Checkpoint>;
    fn observe(&mut self, tx: &ChangeTransaction) -> io::Result<()>;
    fn checkpoint(&self) -> Option<Checkpoint>;
    fn initialize(&mut self, cp: &Checkpoint) -> io::Result<Checkpoint>;
    fn apply(
        &mut self,
        tx: &ChangeTransaction,
        plans: &[ColumnConversionPlan],
    ) -> io::Result<(Checkpoint, String, usize, bool)>;
    fn classify_apply_error(&self, error: &io::Error) -> TargetApplyErrorKind;
    fn resolve_commit_unknown(&mut self, tx: &ChangeTransaction) -> io::Result<CommitResolution>;
}
macro_rules! sink_adapter {
    ($adapter:ident, $planner:expr) => {
        impl From<&$adapter::ReplicationCheckpoint> for Checkpoint {
            fn from(cp: &$adapter::ReplicationCheckpoint) -> Self {
                Self {
                    source_uuid: cp.source_uuid.clone(),
                    sink_uuid: cp.sink_uuid.clone(),
                    mode: cp.mode.clone(),
                    file: cp.file.clone(),
                    position: cp.position,
                    gtid_set: cp.gtid_set.clone(),
                    applied_transactions: cp.applied_transactions,
                    applied_rows: cp.applied_rows,
                    phase: cp.phase.clone(),
                    snapshot_rows: cp.snapshot_rows,
                }
            }
        }
        impl Sink for $adapter::CheckpointWriter {
            fn check_snapshot_targets(&mut self, tables: &[SnapshotTable]) -> io::Result<()> {
                self.check_snapshot_targets(tables)
            }
            fn prepare_snapshot(&mut self, boundary: &SnapshotBoundary) -> io::Result<Checkpoint> {
                self.prepare_snapshot(boundary)
                    .map(|cp| Checkpoint::from(&cp))
            }
            fn copy_snapshot(
                &mut self,
                tables: &[SnapshotTable],
                batches: &mut dyn Iterator<Item = io::Result<SnapshotBatch>>,
                progress: &mut SnapshotProgress<'_>,
            ) -> io::Result<Checkpoint> {
                self.copy_snapshot(tables, batches, progress)
                    .map(|cp| Checkpoint::from(&cp))
            }

            fn observe(&mut self, tx: &ChangeTransaction) -> io::Result<()> {
                self.observe(tx)
            }
            fn checkpoint(&self) -> Option<Checkpoint> {
                self.checkpoint().map(Checkpoint::from)
            }
            fn initialize(&mut self, cp: &Checkpoint) -> io::Result<Checkpoint> {
                self.initialize(&cp.mode, &cp.file, cp.position, cp.gtid_set.as_deref())
                    .map(|cp| Checkpoint::from(&cp))
            }
            fn apply(
                &mut self,
                tx: &ChangeTransaction,
                plans: &[ColumnConversionPlan],
            ) -> io::Result<(Checkpoint, String, usize, bool)> {
                if tx.changes.is_empty() {
                    let result = self.advance(tx)?;
                    return Ok((
                        Checkpoint::from(&result.checkpoint),
                        String::new(),
                        0,
                        result.already_applied,
                    ));
                }
                if plans.is_empty() || plans.iter().any(|plan| !plan.verify_digest()) {
                    return Err(io::Error::other(TargetCapabilityFailure::new(
                        "stored ColumnConversionPlan is missing or has an invalid digest",
                    )));
                }
                // Conversion is applied from the same immutable plans that
                // Web persisted.  The converted transaction is ephemeral;
                // retries always start from the original captured values and
                // rebuild the same result before opening the target txn.
                let converted = change_event::convert_transaction_with_plans(tx.clone(), plans)
                    .map_err(io::Error::other)?;
                let validated = change_event::validate(converted).map_err(io::Error::other)?;
                let plan = ($planner)(&validated)?;
                let result = self.apply(&plan)?;
                Ok((
                    Checkpoint::from(&result.checkpoint),
                    plan.script(),
                    result.statements_executed,
                    result.already_applied,
                ))
            }

            fn classify_apply_error(&self, error: &io::Error) -> TargetApplyErrorKind {
                $adapter::classify_apply_error(error)
            }

            fn resolve_commit_unknown(
                &mut self,
                tx: &ChangeTransaction,
            ) -> io::Result<CommitResolution> {
                self.resolve_commit_unknown(tx)
            }
        }
    };
}
sink_adapter!(mysql_5_7, |validated| {
    let sink = mysql_5_7::SinkAdapter::new();
    sink.plan(validated)
});
sink_adapter!(mysql_8_0, |validated| {
    let sink = mysql_8_0::SinkAdapter::new();
    sink.plan(validated)
});
sink_adapter!(mysql_8_4, |validated| {
    let sink = mysql_8_4::SinkAdapter::new();
    sink.plan(validated)
});
sink_adapter!(postgresql_15, postgresql_15::sql);

fn open_sink(
    endpoint: &Endpoint,
    id: &str,
    source_uuid: &str,
    binding: &str,
) -> io::Result<Box<dyn Sink>> {
    macro_rules! open {
        ($a:ident) => {{
            let config = $a::TargetConfig {
                host: endpoint.host.clone(),
                port: endpoint.port,
                user: endpoint.user.clone(),
                password: endpoint.password.clone(),
            };
            Ok(Box::new($a::CheckpointWriter::open(
                &config,
                id,
                source_uuid,
                binding,
            )?) as Box<dyn Sink>)
        }};
    }
    match endpoint.connector.adapter {
        AdapterKind::Mysql57 => open!(mysql_5_7),
        AdapterKind::Mysql80 => open!(mysql_8_0),
        AdapterKind::Mysql84 => open!(mysql_8_4),
        AdapterKind::Postgresql15 => {
            let config = postgresql_15::TargetConfig::new(
                &endpoint.host,
                &endpoint.database,
                &endpoint.user,
                &endpoint.password,
            )
            .with_port(endpoint.port);
            Ok(Box::new(postgresql_15::CheckpointWriter::open(
                &config,
                id,
                source_uuid,
                binding,
            )?))
        }
        AdapterKind::Postgresql16 | AdapterKind::Postgresql17 => Err(io::Error::other(
            "PostgreSQL 16/17 are source-only Web adapters",
        )),
    }
}
fn capture(
    store: &Store,
    endpoint: &Endpoint,
    id: &str,
    mode: &str,
    saved: Option<&Checkpoint>,
    mappings: &[TableMapping],
    stop: &Arc<AtomicBool>,
) -> io::Result<(Stream, Checkpoint)> {
    let dir = store.log_dir.join(id);
    std::fs::create_dir_all(&dir)?;
    let digest = Sha256::digest(id.as_bytes());
    let server_id = u32::from_be_bytes(digest[..4].try_into().unwrap()).max(1);
    macro_rules! open {
        ($a:ident) => {{
            let mut config = $a::BinlogConfig::new(
                &endpoint.host,
                endpoint.port,
                &endpoint.user,
                &endpoint.password,
            );
            config.server_id = server_id;
            config.stop = Some(stop.clone());
            config.reject_statements = true;
            config.tables = mappings
                .iter()
                .map(|m| (m.source_schema.clone(), m.source_table.clone()))
                .collect();
            config.binlog_log_path =
                Some(dir.join(format!("mysql-{}-binlog.log", endpoint.version)));
            config.start_mode = match saved.map(|s| s.mode.as_str()).unwrap_or(mode) {
                "gtid" => $a::BinlogStartMode::Gtid,
                "binlog" => $a::BinlogStartMode::Position,
                _ => $a::BinlogStartMode::Auto,
            };
            if let Some(cp) = saved {
                if cp.mode == "gtid" {
                    config.gtid_set = cp.gtid_set.clone();
                } else {
                    config.start = Some($a::BinlogPosition {
                        file: cp.file.clone(),
                        position: cp.position,
                    });
                }
            }
            let stream = $a::binlog(config)?;
            let source_uuid = stream.source().id.clone();
            let start_mode = stream.start_mode();
            let start_file = stream.start_position().file.clone();
            let start_position = stream.start_position().position;
            let start_gtid_set = stream.start_gtid_set().map(str::to_owned);
            // Keep empty control transactions available for checkpoint observation,
            // while applying the version-owned Source Contract to every row batch.
            let stream = stream.map(|item| {
                item.and_then(|tx| {
                    if tx.changes.is_empty() {
                        Ok(tx)
                    } else {
                        $a::validate_change_event(tx)
                            .map(|validated| validated.transaction().clone())
                    }
                })
            });
            if saved.is_some_and(|s| s.source_uuid != source_uuid) {
                return Err(io::Error::other("source UUID changed; refusing to resume"));
            }
            let cp = Checkpoint {
                source_uuid,
                sink_uuid: String::new(),
                mode: if start_mode == $a::BinlogStartMode::Gtid {
                    "gtid"
                } else {
                    "binlog"
                }
                .into(),
                file: start_file,
                position: start_position,
                gtid_set: start_gtid_set,
                applied_transactions: 0,
                applied_rows: 0,
                phase: "incremental".into(),
                snapshot_rows: 0,
            };
            Ok((Box::new(stream) as Stream, cp))
        }};
    }
    match endpoint.connector.adapter {
        AdapterKind::Mysql57 => open!(mysql_5_7),
        AdapterKind::Mysql80 => open!(mysql_8_0),
        AdapterKind::Mysql84 => open!(mysql_8_4),
        AdapterKind::Postgresql15 => open_postgresql_source(endpoint, id, saved, stop, 15),
        AdapterKind::Postgresql16 => open_postgresql_source(endpoint, id, saved, stop, 16),
        AdapterKind::Postgresql17 => open_postgresql_source(endpoint, id, saved, stop, 17),
    }
}
fn open_snapshot(
    endpoint: &Endpoint,
    mode: &str,
    tables: &[SnapshotTable],
    stop: &Arc<AtomicBool>,
) -> io::Result<(SnapshotStream, SnapshotBoundary)> {
    macro_rules! open {
        ($a:ident) => {{
            let mut config = $a::BinlogConfig::new(
                &endpoint.host,
                endpoint.port,
                &endpoint.user,
                &endpoint.password,
            );
            config.stop = Some(stop.clone());
            config.start_mode = match mode {
                "gtid" => $a::BinlogStartMode::Gtid,
                "binlog" => $a::BinlogStartMode::Position,
                _ => $a::BinlogStartMode::Auto,
            };
            let reader = $a::snapshot(config, tables.to_vec())?;
            let boundary = reader.boundary().clone();
            Ok((Box::new(reader) as SnapshotStream, boundary))
        }};
    }
    match endpoint.connector.adapter {
        AdapterKind::Mysql57 => open!(mysql_5_7),
        AdapterKind::Mysql80 => open!(mysql_8_0),
        AdapterKind::Mysql84 => open!(mysql_8_4),
        AdapterKind::Postgresql15 | AdapterKind::Postgresql16 | AdapterKind::Postgresql17 => Err(
            io::Error::other("PostgreSQL SourceAdapter 不提供 Web 全量快照"),
        ),
    }
}
fn project(mut tx: ChangeTransaction, mappings: &[TableMapping]) -> ChangeTransaction {
    tx.changes.retain_mut(|change| {
        let Some(mapping) = mappings
            .iter()
            .find(|m| m.source_schema == change.schema && m.source_table == change.table)
        else {
            return false;
        };
        if !mapping.columns.is_empty() {
            for row in [&mut change.before, &mut change.after]
                .into_iter()
                .flatten()
            {
                row.retain(|c| mapping.columns.contains(&c.name));
            }
        }
        true
    });
    tx
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Validation(error.to_string())
}

const MAX_APPLY_RETRIES: u8 = 3;

#[derive(Serialize)]
struct ApplyDiagnostic<'a> {
    task_id: &'a str,
    source_transaction_id: &'a str,
    plan_set_digest: Option<&'a str>,
    kind: TargetApplyErrorKind,
    class: change_event::FailureClass,
    phase: change_event::FailurePhase,
    retry: change_event::RetryClassification,
    code: &'static str,
}

fn apply_diagnostic(
    task_id: &str,
    transaction_id: &str,
    plan_set_digest: Option<&str>,
    kind: TargetApplyErrorKind,
) -> String {
    serde_json::to_string(&ApplyDiagnostic {
        task_id,
        source_transaction_id: transaction_id,
        plan_set_digest,
        kind,
        class: kind.failure_class(),
        phase: kind.phase(),
        retry: kind.retry_classification(),
        code: kind.stable_code(),
    })
    .unwrap_or_else(|_| kind.stable_code().to_owned())
}

fn safe_change_event_log(validated: &change_event::ValidatedTransaction) -> String {
    let transaction = validated.transaction();
    format!(
        "transaction={} source_kind={} source_version={} source_id={} changes={} begin_cursor={} commit_cursor={}\n",
        transaction.id,
        transaction.source.kind,
        transaction.source.version,
        transaction.source.id,
        transaction.changes.len(),
        transaction.begin_cursor.display,
        transaction.commit_cursor.display,
    )
}

fn validate_runtime_plans(
    task: &crate::tasks::ReplicationTask,
    tx: &ChangeTransaction,
    source: &Endpoint,
    sink: &Endpoint,
) -> io::Result<()> {
    let expected_revision = format!("{}:r{}", task.id, task.configuration_revision);
    if task.plans.is_empty() {
        return Err(io::Error::other(TargetCapabilityFailure::new(
            "task has no saved ColumnConversionPlan",
        )));
    }
    for plan in &task.plans {
        if !plan.verify_digest()
            || plan.route_id != task.id
            || plan.configuration_revision != expected_revision
            || plan.source_connector.kind != source.connector.identity.kind
            || plan.source_connector.version != source.connector.identity.version
            || plan.sink_connector.kind != sink.connector.identity.kind
            || plan.sink_connector.version != sink.connector.identity.version
        {
            return Err(io::Error::other(TargetCapabilityFailure::new(
                "saved ColumnConversionPlan does not match the active route",
            )));
        }
    }
    for change in &tx.changes {
        let Some(mapping) = task.mappings.iter().find(|mapping| {
            mapping.source_schema == change.schema && mapping.source_table == change.table
        }) else {
            return Err(io::Error::other(TargetCapabilityFailure::new(
                "source transaction contains a table outside the active Route Projection",
            )));
        };
        let source_prefix = format!("catalog:{}.{}.", change.schema, change.table);
        let target_prefix = format!("catalog:{}.{}.", mapping.sink_schema, mapping.sink_table);
        for column in change.before.iter().chain(change.after.iter()).flatten() {
            let Some(plan) = task.plans.iter().find(|plan| {
                plan.source_field.lineage_id == format!("{source_prefix}{}", column.name)
                    && plan
                        .target_field
                        .lineage_id
                        .ends_with(&format!(".{}", column.name))
                    && plan.target_field.lineage_id.starts_with(&target_prefix)
            }) else {
                return Err(io::Error::other(TargetCapabilityFailure::new(
                    "source transaction field has no saved ColumnConversionPlan",
                )));
            };
            if !column.generated {
                change_event::validate_datum_against_plan(plan, &column.datum)
                    .map_err(io::Error::other)?;
            }
        }
    }
    Ok(())
}

fn classify_endpoint_error(endpoint: &Endpoint, error: &io::Error) -> TargetApplyErrorKind {
    match endpoint.connector.adapter {
        AdapterKind::Mysql57 => mysql_5_7::classify_apply_error(error),
        AdapterKind::Mysql80 => mysql_8_0::classify_apply_error(error),
        AdapterKind::Mysql84 => mysql_8_4::classify_apply_error(error),
        AdapterKind::Postgresql15 | AdapterKind::Postgresql16 | AdapterKind::Postgresql17 => {
            postgresql_15::classify_apply_error(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_with_recovery(
    writer: &mut Option<Box<dyn Sink>>,
    endpoint: &Endpoint,
    task_id: &str,
    source_uuid: &str,
    binding: &str,
    tx: &ChangeTransaction,
    plans: &[ColumnConversionPlan],
    stop: &Arc<AtomicBool>,
) -> io::Result<(Checkpoint, String, usize, bool)> {
    let mut reopen = || open_sink(endpoint, task_id, source_uuid, binding);
    apply_with_recovery_using(writer, &mut reopen, tx, plans, stop)
}

fn apply_with_recovery_using(
    writer: &mut Option<Box<dyn Sink>>,
    reopen: &mut dyn FnMut() -> io::Result<Box<dyn Sink>>,
    tx: &ChangeTransaction,
    plans: &[ColumnConversionPlan],
    stop: &Arc<AtomicBool>,
) -> io::Result<(Checkpoint, String, usize, bool)> {
    let mut retries = 0u8;
    loop {
        if stop.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "task stopped before the Sink Apply Transaction began",
            ));
        }
        let mut active = writer
            .take()
            .ok_or_else(|| io::Error::other("Sink writer is unavailable"))?;
        match active.apply(tx, plans) {
            Ok(result) => {
                *writer = Some(active);
                return Ok(result);
            }
            Err(error) => {
                let kind = active.classify_apply_error(&error);
                if kind == TargetApplyErrorKind::CommitUnknown {
                    // The old connection may not be usable and must release
                    // its task lock before a fresh authoritative read.
                    drop(active);
                    let mut recovered = match reopen() {
                        Ok(writer) => writer,
                        Err(_) => return Err(error),
                    };
                    let resolution = match recovered.resolve_commit_unknown(tx) {
                        Ok(resolution) => resolution,
                        Err(_) => return Err(error),
                    };
                    match resolution {
                        CommitResolution::Applied => {
                            let checkpoint = recovered.checkpoint().ok_or(error)?;
                            *writer = Some(recovered);
                            return Ok((checkpoint, String::new(), 0, true));
                        }
                        CommitResolution::Unprovable => return Err(error),
                        CommitResolution::NotApplied => {
                            // This is a proven negative result, so one full
                            // replay is safe. A second unknown result remains
                            // unknown and is surfaced to the operator.
                            match recovered.apply(tx, plans) {
                                Ok(result) => {
                                    *writer = Some(recovered);
                                    return Ok(result);
                                }
                                Err(replay_error) => {
                                    *writer = Some(recovered);
                                    return Err(replay_error);
                                }
                            }
                        }
                    }
                }
                if kind.is_retryable() && retries < MAX_APPLY_RETRIES {
                    retries += 1;
                    drop(active);
                    let backoff = std::time::Duration::from_millis(50 * (1u64 << retries));
                    std::thread::sleep(backoff);
                    if stop.load(Ordering::Acquire) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "task stopped before retrying the Sink Apply Transaction",
                        ));
                    }
                    *writer = Some(reopen()?);
                    continue;
                }
                *writer = Some(active);
                return Err(error);
            }
        }
    }
}

pub(crate) fn run(store: &Store, actor: i64, id: &str, stop: &Arc<AtomicBool>) -> Result<()> {
    let task = store.task(id)?;
    store.log_task(id, "info", "正在检查库表与目的端 CDC.log_info")?;
    store.ensure_saved_plan_current(actor, &task)?;
    let source = store.endpoint_for_database(
        &task.source_id,
        task.source_revision,
        "reader",
        (!task.source_database.is_empty()).then_some(task.source_database.as_str()),
    )?;
    let sink = store.endpoint_for_database(
        &task.sink_id,
        task.sink_revision,
        "writer",
        (!task.sink_database.is_empty()).then_some(task.sink_database.as_str()),
    )?;
    let mut preopened_source = if matches!(
        source.connector.adapter,
        AdapterKind::Postgresql15 | AdapterKind::Postgresql16 | AdapterKind::Postgresql17
    ) {
        Some(
            open_postgresql_source(
                &source,
                id,
                task.runtime.checkpoint.as_ref(),
                stop,
                postgresql_major(source.connector.adapter).expect("PostgreSQL adapter"),
            )
            .map_err(failure)?,
        )
    } else {
        None
    };
    let source_uuid = preopened_source
        .as_ref()
        .map(|(_, checkpoint)| checkpoint.source_uuid.clone())
        .unwrap_or(
            store
                .catalog_connection_for_database(
                    actor,
                    &task.source_id,
                    EndpointRole::Source,
                    (!task.source_database.is_empty()).then_some(task.source_database.as_str()),
                )?
                .server_uuid,
        );
    let binding_data = serde_json::to_vec(&(
        &task.source_id,
        &task.sink_id,
        task.source_database.clone(),
        task.sink_database.clone(),
        task.start_mode.clone(),
        &task.mappings,
        task.configuration_revision,
        task.plan_set_digest.clone(),
    ))
    .map_err(failure)?;
    let binding = format!("{:x}", Sha256::digest(binding_data));
    let mut writer = Some(open_sink(&sink, id, &source_uuid, &binding).map_err(failure)?);
    let mut saved = writer.as_ref().and_then(|writer| writer.checkpoint());
    if saved.is_none() && task.runtime.checkpoint.is_some() {
        return Err(Error::Conflict(
            "目的端 CDC.log_info 中的任务进度丢失，禁止从最新位置重新开始",
        ));
    }
    if let (Some(saved), Some((_, opened))) = (saved.as_ref(), preopened_source.as_ref())
        && (saved.source_uuid != opened.source_uuid
            || saved.mode != opened.mode
            || saved.file != opened.file
            || saved.position != opened.position)
    {
        // SQLite is only a UI mirror. If the sink has a newer authoritative
        // cursor, reopen the source from that cursor before consuming rows.
        drop(preopened_source.take());
        preopened_source = Some(
            open_postgresql_source(
                &source,
                id,
                Some(saved),
                stop,
                postgresql_major(source.connector.adapter).expect("PostgreSQL adapter"),
            )
            .map_err(failure)?,
        );
    }
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }
    if source.connector.capabilities.supports_snapshot
        && saved.as_ref().is_none_or(|cp| cp.phase == "snapshot")
    {
        let tables: Vec<_> = task
            .mappings
            .iter()
            .map(|m| SnapshotTable {
                schema: m.source_schema.clone(),
                table: m.source_table.clone(),
                columns: m.columns.clone(),
            })
            .collect();
        writer
            .as_mut()
            .expect("Sink writer exists")
            .check_snapshot_targets(&tables)
            .map_err(failure)?;
        store.log_task(id,"info","首次全量：短暂全局读锁 → 取得 GTID/文件位点 → 建立共同快照 → 立即解锁；源端读取账号执行")?;
        let (mut snapshot, boundary) = match open_snapshot(&source, &task.start_mode, &tables, stop)
        {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => return Ok(()),
            result => result.map_err(failure)?,
        };
        if boundary.source.id != source_uuid {
            return Err(Error::Conflict("快照源实例身份发生变化"));
        }
        let cp = writer
            .as_mut()
            .expect("Sink writer exists")
            .prepare_snapshot(&boundary)
            .map_err(failure)?;
        store.save_checkpoint(id, &cp, 0)?;
        store.mark_running(id, &cp)?;
        store.log_task(
            id,
            "info",
            &format!("源端全局读锁已释放。快照边界 {}", boundary.cursor.display),
        )?;
        let result = writer.as_mut().expect("Sink writer exists").copy_snapshot(
            &tables,
            &mut snapshot,
            &mut |batch, rows| {
                if stop.load(Ordering::Acquire) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "snapshot cancelled",
                    ));
                }
                let message = format!(
                    "全量读取 {}.{}，累计暂存 {rows} 行（尚未提交）",
                    batch.schema, batch.table
                );
                store
                    .snapshot_progress(id, &message)
                    .map_err(io::Error::other)?;
                store
                    .log_task(id, "info", &message)
                    .map_err(io::Error::other)?;
                Ok(())
            },
        );
        drop(snapshot); // Release the common read view before starting binlog capture.
        let cp = match result {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                store.snapshot_progress(id, "全量已取消；未提交数据已回滚，下次启动重新全量")?;
                store.log_task(id, "info", "全量已取消；目的端全量事务已回滚")?;
                return Ok(());
            }
            result => result.map_err(failure)?,
        };
        store.save_checkpoint(id, &cp, 0)?;
        let message = format!(
            "全量已提交 {} 行；全量数据与进入增量的位点已原子提交",
            cp.snapshot_rows
        );
        store.log_task(id, "info", &message)?;
        store.append_task_file(id, "write.log", &format!("-- {message}"))?;
        saved = Some(cp);
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
    }
    if !source.connector.capabilities.supports_snapshot && saved.is_none() {
        store.log_task(
            id,
            "info",
            "PostgreSQL SourceAdapter 当前只提供增量复制；任务将从复制槽位起点开始读取",
        )?;
    }
    let (mut stream, initial) = if let Some(source) = preopened_source.take() {
        source
    } else {
        capture(
            store,
            &source,
            id,
            &task.start_mode,
            saved.as_ref(),
            &task.mappings,
            stop,
        )
        .map_err(failure)?
    };
    if initial.source_uuid != source_uuid {
        return Err(Error::Conflict("检查期间源实例身份发生变化"));
    }
    let cp = writer
        .as_mut()
        .expect("Sink writer exists")
        .initialize(&initial)
        .map_err(failure)?;
    store.save_checkpoint(id, &cp, 0)?;
    store.mark_running(id, &cp)?;
    store.log_task(
        id,
        "info",
        &format!(
            "{}：{} {}:{}；复制进度以目的端 CDC.log_info 为准",
            if saved.is_some() {
                "恢复同步"
            } else {
                "开始增量同步"
            },
            cp.mode,
            cp.file,
            cp.position
        ),
    )?;
    let mut last_filtered = None;
    for item in &mut stream {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let tx = project(item.map_err(failure)?, &task.mappings);
        // No business effect: leaving the durable cursor behind is safe to replay.
        // Do not write a new Sink control transaction in response to control-only traffic.
        if tx.changes.is_empty() {
            writer
                .as_mut()
                .expect("Sink writer exists")
                .observe(&tx)
                .map_err(failure)?;
            last_filtered = Some(tx);
            continue;
        }
        let validated = change_event::validate(tx.clone()).map_err(failure)?;
        if let Err(_error) = validate_runtime_plans(&task, &tx, &source, &sink) {
            let diagnostic = apply_diagnostic(
                id,
                &tx.id,
                task.plan_set_digest.as_deref(),
                TargetApplyErrorKind::Capability,
            );
            store.log_task(id, "error", &diagnostic)?;
            return Err(Error::Validation(diagnostic));
        }
        store.append_task_file(id, "change_event.log", &safe_change_event_log(&validated))?;
        let (cp, _sql, rows, replayed) = match apply_with_recovery(
            &mut writer,
            &sink,
            id,
            &source_uuid,
            &binding,
            &tx,
            &task.plans,
            stop,
        ) {
            Ok(result) => result,
            Err(error) => {
                if error.kind() == io::ErrorKind::Interrupted {
                    return Ok(());
                }
                let kind = classify_endpoint_error(&sink, &error);
                let diagnostic =
                    apply_diagnostic(id, &tx.id, task.plan_set_digest.as_deref(), kind);
                store.log_task(id, "error", &diagnostic)?;
                return Err(Error::Validation(diagnostic));
            }
        };
        // A failure in local logging cannot roll back an already committed Sink transaction.
        // On restart its counters and cursor are recovered from the Sink.
        store.save_checkpoint(id, &cp, rows)?;
        last_filtered = None;
        if !replayed {
            store.append_task_file(
                id,
                "write.log",
                &format!(
                    "transaction={} rows={} plan_set_digest={}\n",
                    tx.id,
                    rows,
                    task.plan_set_digest.as_deref().unwrap_or("unknown"),
                ),
            )?;
        }
        store.log_task(
            id,
            "info",
            &format!(
                "{}事务 {}，{} 行；位点 {}:{}",
                if replayed {
                    "跳过已提交"
                } else {
                    "已提交"
                },
                tx.id,
                rows,
                cp.file,
                cp.position
            ),
        )?;
    }
    if !stop.load(Ordering::Acquire) {
        return Err(Error::Validation(
            "源端 binlog 流已结束；进度保留，可重新启动".into(),
        ));
    }
    if let Some(tx) = last_filtered {
        let result = apply_with_recovery(
            &mut writer,
            &sink,
            id,
            &source_uuid,
            &binding,
            &tx,
            &task.plans,
            stop,
        );
        let (cp, _, _, _) = match result {
            Ok(result) => result,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(error) => {
                let kind = classify_endpoint_error(&sink, &error);
                let diagnostic =
                    apply_diagnostic(id, &tx.id, task.plan_set_digest.as_deref(), kind);
                store.log_task(id, "error", &diagnostic)?;
                return Err(Error::Validation(diagnostic));
            }
        };
        store.save_checkpoint(id, &cp, 0)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "qualification_recovery_tests.rs"]
mod qualification_recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use change_event::{Operation, RowChange, Source, SourceCursor};
    use std::sync::Mutex;

    fn test_checkpoint() -> Checkpoint {
        Checkpoint {
            source_uuid: "source".into(),
            sink_uuid: "sink".into(),
            mode: "binlog".into(),
            file: "binlog.000001".into(),
            position: 200,
            gtid_set: None,
            applied_transactions: 1,
            applied_rows: 1,
            phase: "incremental".into(),
            snapshot_rows: 0,
        }
    }

    fn test_transaction() -> ChangeTransaction {
        let cursor = |display: &str| SourceCursor {
            format: "test.cursor".into(),
            value: display.into(),
            display: display.into(),
        };
        ChangeTransaction {
            source: Source {
                kind: "test".into(),
                version: "1".into(),
                id: "source".into(),
            },
            id: "source:transaction-1".into(),
            begin_cursor: cursor("100"),
            commit_cursor: cursor("200"),
            changes: vec![RowChange {
                database: None,
                operation: Operation::Insert,
                schema: "source".into(),
                table: "items".into(),
                source_cursor: cursor("150"),
                source_timestamp: 1,
                schema_basis: "test".into(),
                before: None,
                after: Some(Vec::new()),
            }],
        }
    }

    struct InjectedSink {
        checkpoint: Checkpoint,
        fail_once: bool,
        attempts: Arc<Mutex<Vec<(String, usize)>>>,
    }

    impl Sink for InjectedSink {
        fn check_snapshot_targets(&mut self, _tables: &[SnapshotTable]) -> io::Result<()> {
            Ok(())
        }

        fn prepare_snapshot(&mut self, _boundary: &SnapshotBoundary) -> io::Result<Checkpoint> {
            Ok(self.checkpoint.clone())
        }

        fn copy_snapshot(
            &mut self,
            _tables: &[SnapshotTable],
            _batches: &mut dyn Iterator<Item = io::Result<SnapshotBatch>>,
            _progress: &mut SnapshotProgress<'_>,
        ) -> io::Result<Checkpoint> {
            Ok(self.checkpoint.clone())
        }

        fn observe(&mut self, _tx: &ChangeTransaction) -> io::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> Option<Checkpoint> {
            Some(self.checkpoint.clone())
        }

        fn initialize(&mut self, checkpoint: &Checkpoint) -> io::Result<Checkpoint> {
            self.checkpoint = checkpoint.clone();
            Ok(self.checkpoint.clone())
        }

        fn apply(
            &mut self,
            tx: &ChangeTransaction,
            _plans: &[ColumnConversionPlan],
        ) -> io::Result<(Checkpoint, String, usize, bool)> {
            self.attempts
                .lock()
                .expect("fault-injection attempt log should not be poisoned")
                .push((tx.id.clone(), tx.changes.len()));
            if self.fail_once {
                self.fail_once = false;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "injected connection failure",
                ));
            }
            Ok((
                self.checkpoint.clone(),
                "complete transaction".into(),
                tx.changes.len(),
                false,
            ))
        }

        fn classify_apply_error(&self, error: &io::Error) -> TargetApplyErrorKind {
            if error.kind() == io::ErrorKind::TimedOut {
                TargetApplyErrorKind::Connection
            } else {
                TargetApplyErrorKind::Sql
            }
        }

        fn resolve_commit_unknown(
            &mut self,
            _tx: &ChangeTransaction,
        ) -> io::Result<CommitResolution> {
            Ok(CommitResolution::Unprovable)
        }
    }

    #[test]
    fn fault_injection_replays_complete_source_transaction_after_connection_failure() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let initial = InjectedSink {
            checkpoint: test_checkpoint(),
            fail_once: true,
            attempts: attempts.clone(),
        };
        let reopened = attempts.clone();
        let mut writer: Option<Box<dyn Sink>> = Some(Box::new(initial));
        let mut reopen = || {
            Ok(Box::new(InjectedSink {
                checkpoint: test_checkpoint(),
                fail_once: false,
                attempts: reopened.clone(),
            }) as Box<dyn Sink>)
        };
        let stop = Arc::new(AtomicBool::new(false));
        let transaction = test_transaction();
        let (_, _, rows, replayed) =
            apply_with_recovery_using(&mut writer, &mut reopen, &transaction, &[], &stop)
                .expect("the complete transaction should recover after the injected failure");

        let attempts = attempts
            .lock()
            .expect("fault-injection attempt log should not be poisoned")
            .clone();
        assert_eq!(
            attempts,
            vec![(transaction.id.clone(), 1), (transaction.id, 1)]
        );
        assert_eq!(rows, 1);
        assert!(!replayed);
    }
}
