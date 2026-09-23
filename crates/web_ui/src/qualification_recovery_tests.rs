//! Fault injection into the production recovery loop. The durable store and
//! transaction executor are fakes, so this evidence is explicitly offline.
use super::*;
use crate::registry::{SinkRegistry, SourceRegistry};
use change_event::{Datum, LogicalValue};
use serde_json::json;
use std::sync::Mutex;

#[allow(dead_code)]
#[path = "../../../tests/support/matrix_fixture.rs"]
mod fixture;

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    Fail(TargetApplyErrorKind),
    Unknown(CommitResolution),
}

#[derive(Default)]
struct Durable {
    rows: usize,
    commits: usize,
    transaction: Option<String>,
    attempts: Vec<(String, usize, Vec<String>)>,
}

struct Executor {
    durable: Arc<Mutex<Durable>>,
    fault: Fault,
    resolution: CommitResolution,
    checkpoint: Checkpoint,
}

impl Sink for Executor {
    fn check_snapshot_targets(&mut self, _: &[SnapshotTable]) -> io::Result<()> {
        unreachable!()
    }
    fn prepare_snapshot(&mut self, _: &SnapshotBoundary) -> io::Result<Checkpoint> {
        unreachable!()
    }
    fn copy_snapshot(
        &mut self,
        _: &[SnapshotTable],
        _: &mut dyn Iterator<Item = io::Result<SnapshotBatch>>,
        _: &mut SnapshotProgress<'_>,
    ) -> io::Result<Checkpoint> {
        unreachable!()
    }
    fn observe(&mut self, _: &ChangeTransaction) -> io::Result<()> {
        Ok(())
    }
    fn checkpoint(&self) -> Option<Checkpoint> {
        self.durable
            .lock()
            .unwrap()
            .transaction
            .as_ref()
            .map(|_| self.checkpoint.clone())
    }
    fn initialize(&mut self, _: &Checkpoint) -> io::Result<Checkpoint> {
        unreachable!()
    }
    fn apply(
        &mut self,
        tx: &ChangeTransaction,
        plans: &[ColumnConversionPlan],
    ) -> io::Result<(Checkpoint, String, usize, bool)> {
        let mut db = self.durable.lock().unwrap();
        db.attempts.push((
            tx.id.clone(),
            tx.changes.len(),
            plans.iter().map(|p| p.plan_digest.clone()).collect(),
        ));
        if db.transaction.as_deref() == Some(&tx.id) {
            return Ok((self.checkpoint.clone(), String::new(), 0, true));
        }
        let mut input = tx.clone();
        if matches!(self.fault, Fault::Fail(TargetApplyErrorKind::Conversion)) {
            let column = input.changes[0]
                .after
                .as_mut()
                .unwrap()
                .iter_mut()
                .find(|c| c.name == "ratio")
                .unwrap();
            column.datum = Datum::Value(LogicalValue::Float {
                bits: 64,
                ieee754_hex: "7fefffffffffffff".into(),
            });
        }
        let converted =
            change_event::convert_transaction_with_plans(input, plans).map_err(io::Error::other)?;
        // Stage every row privately. A fault after a staged row must not publish
        // either the row mutations or Replication Metadata.
        let mut staged = 0;
        for _ in &converted.changes {
            staged += 1;
            if let Fault::Fail(kind) = self.fault {
                return Err(io::Error::other(Injected(kind)));
            }
        }
        if matches!(
            self.fault,
            Fault::Unknown(CommitResolution::NotApplied | CommitResolution::Unprovable)
        ) {
            return Err(io::Error::other(Injected(
                TargetApplyErrorKind::CommitUnknown,
            )));
        }
        db.rows += staged;
        db.commits += 1;
        db.transaction = Some(tx.id.clone());
        if matches!(self.fault, Fault::Unknown(_)) {
            return Err(io::Error::other(Injected(
                TargetApplyErrorKind::CommitUnknown,
            )));
        }
        Ok((self.checkpoint.clone(), String::new(), staged, false))
    }
    fn classify_apply_error(&self, error: &io::Error) -> TargetApplyErrorKind {
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<Injected>())
            .map_or(TargetApplyErrorKind::Conversion, |e| e.0)
    }
    fn resolve_commit_unknown(&mut self, tx: &ChangeTransaction) -> io::Result<CommitResolution> {
        let db = self.durable.lock().unwrap();
        if db.transaction.as_deref() == Some(&tx.id) {
            Ok(CommitResolution::Applied)
        } else {
            Ok(self.resolution)
        }
    }
}

#[derive(Debug)]
struct Injected(TargetApplyErrorKind);
impl std::fmt::Display for Injected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.stable_code())
    }
}
impl std::error::Error for Injected {}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        source_uuid: "fixture-source".into(),
        sink_uuid: "fixture-sink".into(),
        mode: "binlog".into(),
        file: "fixture".into(),
        position: 200,
        gtid_set: None,
        applied_transactions: 1,
        applied_rows: 3,
        phase: "incremental".into(),
        snapshot_rows: 0,
    }
}

fn exercise(tx: &ChangeTransaction, plans: &[ColumnConversionPlan], fault: Fault) {
    let durable = Arc::new(Mutex::new(Durable::default()));
    let resolution = match fault {
        Fault::Unknown(r) => r,
        _ => CommitResolution::NotApplied,
    };
    let make = |fault| {
        Box::new(Executor {
            durable: durable.clone(),
            fault,
            resolution,
            checkpoint: checkpoint(),
        }) as Box<dyn Sink>
    };
    let mut writer = Some(make(fault));
    let mut reopen = || Ok(make(Fault::None));
    let stop = Arc::new(AtomicBool::new(false));
    let result = apply_with_recovery_using(&mut writer, &mut reopen, tx, plans, &stop);
    let blocked = matches!(fault,Fault::Fail(kind) if !kind.is_retryable())
        || matches!(fault, Fault::Unknown(CommitResolution::Unprovable));
    {
        let db = durable.lock().unwrap();
        assert_eq!(result.is_err(), blocked, "{fault:?}");
        assert_eq!(db.commits, usize::from(!blocked));
        assert_eq!(db.rows, if blocked { 0 } else { tx.changes.len() });
        assert_eq!(db.transaction.is_some(), !blocked);
        let attempts = if matches!(fault,Fault::Fail(k) if k.is_retryable())
            || matches!(fault, Fault::Unknown(CommitResolution::NotApplied))
        {
            2
        } else {
            1
        };
        assert_eq!(db.attempts.len(), attempts);
        assert!(db.attempts.iter().all(|a| a == &db.attempts[0]));
    }
    if !blocked {
        // A new writer has no process-local state; replay resolves from the
        // shared durable metadata and cannot commit business rows again.
        writer = Some(make(Fault::None));
        let (_, _, rows, replayed) =
            apply_with_recovery_using(&mut writer, &mut reopen, tx, plans, &stop).unwrap();
        assert_eq!(rows, 0);
        assert!(replayed);
        assert_eq!(durable.lock().unwrap().commits, 1);
    }
    stop.store(true, Ordering::Release);
    writer = Some(make(Fault::None));
    let attempts = durable.lock().unwrap().attempts.len();
    assert_eq!(
        apply_with_recovery_using(&mut writer, &mut reopen, tx, plans, &stop)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(durable.lock().unwrap().attempts.len(), attempts);
}

#[test]
fn qualification_recovery_matrix() {
    let config: serde_json::Value =
        serde_json::from_str(include_str!("../../../scripts/qualification-matrix.json")).unwrap();
    let ids = config["databases"].as_array().unwrap();
    let mut report = Vec::new();
    for source in ids {
        for sink in ids {
            let source_id = source.as_str().unwrap();
            let sink_id = sink.as_str().unwrap();
            let identity = |id: &str| {
                let (kind, version) = id.split_once('_').unwrap();
                (kind.to_owned(), version.replace('_', "."))
            };
            let (sk, sv) = identity(source_id);
            let (tk, tv) = identity(sink_id);
            let supported =
                SourceRegistry.find(&sk, &sv).is_some() && SinkRegistry.find(&tk, &tv).is_some();
            let sink_is_source_only = config["source_only"]
                .as_array()
                .is_some_and(|values| values.contains(sink));
            let implemented_source = config["implemented"].as_array().unwrap().contains(source);
            let implemented_sink = config["implemented"].as_array().unwrap().contains(sink);
            let source_only_source = config["source_only"].as_array().unwrap().contains(source);
            let declared = (implemented_source && implemented_sink && !sink_is_source_only)
                || (source_only_source && implemented_sink);
            assert_eq!(
                supported, declared,
                "registry and qualification roster disagree: {source_id} -> {sink_id}"
            );
            if !supported {
                report.push(json!({"source":source,"sink":sink,"offline":"UNSUPPORTED","code":"connector.not_implemented"}));
                continue;
            }
            let source_version = match source_id {
                "mysql_5_7" => fixture::SourceVersion::Mysql57,
                "mysql_8_0" => fixture::SourceVersion::Mysql80,
                "mysql_8_4" => fixture::SourceVersion::Mysql84,
                "postgresql_15" => fixture::SourceVersion::Postgresql15,
                "postgresql_16" => fixture::SourceVersion::Postgresql16,
                "postgresql_17" => fixture::SourceVersion::Postgresql17,
                _ => panic!("registered source needs a fixed native fixture"),
            };
            let tx = fixture::validate_source(
                source_version,
                fixture::transaction(source_version, "recovery"),
            )
            .unwrap();
            let mut complete = tx.transaction().clone();
            // FULL before evidence for the converted non-key column; presence
            // rejection is separately exercised by the planning matrix.
            for row in &mut complete.changes {
                if let Some(before) = &mut row.before {
                    before.iter_mut().find(|c| c.name == "ratio").unwrap().datum =
                        Datum::Value(LogicalValue::Float {
                            bits: 64,
                            ieee754_hex: "3ff8000000000000".into(),
                        });
                }
            }
            let plan = recovery_plan(&complete, &sk, &sv, &tk, &tv);
            let saved: ColumnConversionPlan =
                serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
            assert_eq!(saved.plan_digest, plan.plan_digest);
            let plans = [saved];
            let mut cross_table = complete.clone();
            cross_table.changes[2].table = "second_table".into();
            for transaction in [&complete, &cross_table] {
                for fault in [
                    Fault::None,
                    Fault::Fail(TargetApplyErrorKind::Conversion),
                    Fault::Fail(TargetApplyErrorKind::Capability),
                    Fault::Fail(TargetApplyErrorKind::Constraint),
                    Fault::Fail(TargetApplyErrorKind::Sql),
                    Fault::Fail(TargetApplyErrorKind::CheckpointConflict),
                    Fault::Fail(TargetApplyErrorKind::Connection),
                    Fault::Fail(TargetApplyErrorKind::LockTimeout),
                    Fault::Unknown(CommitResolution::Applied),
                    Fault::Unknown(CommitResolution::NotApplied),
                    Fault::Unknown(CommitResolution::Unprovable),
                ] {
                    exercise(transaction, &plans, fault);
                }
            }
            report.push(json!({"source":source,"sink":sink,"offline":"PASS","live":"REQUIRES_LIVE","evidence_scope":"production_recovery_loop_with_fake_executor","checks":["transaction_boundary","cross_table","atomic_dml_checkpoint","rollback","conversion_failure","constraint_failure","metadata_failure","whole_transaction_retry","CommitUnknown.Applied","CommitUnknown.NotApplied","CommitUnknown.Unprovable","restart","duplicate_delivery","stop"]}));
        }
    }
    if let Some(path) = std::env::var_os("CDC_QUALIFICATION_RECOVERY_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

fn recovery_plan(
    tx: &ChangeTransaction,
    sk: &str,
    sv: &str,
    tk: &str,
    tv: &str,
) -> ColumnConversionPlan {
    use change_event::{
        DefinitionReference, FieldCompatibilityInput, FieldDefinition, LogicalType, Operation,
        PresenceState, RiskConfirmation, RouteOptions, ServerBuildIdentity,
    };
    let source = SourceRegistry.find(sk, sv).unwrap();
    let sink = SinkRegistry.find(tk, tv).unwrap();
    let native = if sk == "mysql" {
        "double"
    } else {
        "double precision"
    };
    let column = crate::catalog::CatalogColumn {
        name: "ratio".into(),
        column_type: native.into(),
        nullable: true,
        collation: None,
        default_value: None,
        extra: String::new(),
    };
    let mapping = source.source_type_mapping(&column).unwrap();
    let manifest = sink.structured_manifest(ServerBuildIdentity::new(
        tk,
        "offline-fixture",
        tv,
        "fixture-not-live",
    ));
    let field = |target| FieldDefinition {
        reference: DefinitionReference::new(
            format!(
                "catalog:{}.{}.ratio",
                if target {
                    "target"
                } else {
                    &tx.changes[0].schema
                },
                tx.changes[0].table
            ),
            "fixture-definition",
        ),
        ordinal: 4,
        name: "ratio".into(),
        native_type: if target {
            if tk == "mysql" { "float" } else { "real" }
        } else {
            native
        }
        .into(),
        logical_type: LogicalType::float(if target { 32 } else { 64 }),
        nullable: true,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    };
    let input = |options| FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: None,
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: mapping.clone(),
        source_field: field(false),
        target_field: field(true),
        manifest: &manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: true,
        options,
    };
    let mut options = RouteOptions {
        route_id: "recovery".into(),
        configuration_revision: "fixture-v1".into(),
        ..RouteOptions::default()
    };
    let pending = change_event::plan_field_compatibility(input(options.clone()))
        .unwrap()
        .plan
        .unwrap();
    options.confirmations.push(RiskConfirmation {
        source_field_lineage: pending.source_field.lineage_id.clone(),
        target_field_lineage: pending.target_field.lineage_id.clone(),
        rule: pending.rule.clone(),
        plan_digest: pending.plan_digest.clone(),
        actor: "fixture".into(),
        confirmed_at: "2026-09-18T00:00:00Z".into(),
        reason: Some("offline test".into()),
    });
    let result = change_event::plan_field_compatibility(input(options)).unwrap();
    assert!(result.is_selectable());
    result.plan.unwrap()
}
