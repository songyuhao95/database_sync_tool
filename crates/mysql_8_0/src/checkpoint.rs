//! Replication progress committed atomically with business data on the Sink.
use crate::sql::{self, SqlTransaction, TargetConfig};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{ChangeTransaction, SourceCursor};
use mysql_driver::{
    Conn, Row, TxOpts,
    prelude::{FromValue, Queryable},
};
use std::{cmp::Ordering, io};

const FIELDS: &str = "task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set,last_transaction_id,revision,applied_transactions,applied_rows,phase,snapshot_rows,snapshot_gtids";
const DDL: &str = "CREATE TABLE IF NOT EXISTS CDC.log_info (
 task_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  source_uuid VARCHAR(255) CHARACTER SET ascii NOT NULL,
 sink_uuid VARCHAR(36) CHARACTER SET ascii NOT NULL,
 binding VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  mode VARCHAR(32) CHARACTER SET ascii NOT NULL,
 binlog_file VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 binlog_position BIGINT UNSIGNED NOT NULL,
 gtid_set LONGTEXT CHARACTER SET ascii,
 last_transaction_id VARCHAR(512) CHARACTER SET ascii,
 revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
 applied_transactions BIGINT UNSIGNED NOT NULL DEFAULT 0,
 applied_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
 phase VARCHAR(16) NOT NULL DEFAULT 'incremental',
 snapshot_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
 snapshot_gtids LONGTEXT CHARACTER SET ascii,
 updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB";

fn decode_snapshot_boundary(cursor: &SourceCursor) -> io::Result<(String, String, u64, String)> {
    if cursor.format != "mysql.snapshot-boundary.v1" {
        return Err(io::Error::other("unsupported snapshot boundary cursor"));
    }
    let value: serde_json::Value = serde_json::from_str(&cursor.value).map_err(io::Error::other)?;
    let field = |name| {
        value
            .get(name)
            .ok_or_else(|| io::Error::other(format!("snapshot boundary is missing {name}")))
    };
    let mode = field("mode")?
        .as_str()
        .ok_or_else(|| io::Error::other("snapshot boundary mode is not text"))?
        .to_owned();
    let file = field("file")?
        .as_str()
        .ok_or_else(|| io::Error::other("snapshot boundary file is not text"))?
        .to_owned();
    let position = field("position")?
        .as_u64()
        .ok_or_else(|| io::Error::other("snapshot boundary position is not an integer"))?;
    let executed_gtids = field("executed_gtids")?
        .as_str()
        .ok_or_else(|| io::Error::other("snapshot boundary GTID set is not text"))?
        .to_owned();
    Ok((mode, file, position, executed_gtids))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationCheckpoint {
    pub task_id: String,
    pub source_uuid: String,
    pub sink_uuid: String,
    pub binding: String,
    pub mode: String,
    pub file: String,
    pub position: u64,
    pub gtid_set: Option<String>,
    pub last_transaction_id: Option<String>,
    pub revision: u64,
    pub applied_transactions: u64,
    pub applied_rows: u64,
    pub phase: String,
    pub snapshot_rows: u64,
    pub snapshot_gtids: Option<String>,
}
#[derive(Debug)]
pub struct CheckpointApplyResult {
    pub checkpoint: ReplicationCheckpoint,
    pub statements_executed: usize,
    pub already_applied: bool,
}

/// The connection holds an advisory lock for the task. Reconnect only through open(),
/// which reloads the authoritative checkpoint, including after a lost COMMIT response.
pub struct CheckpointWriter {
    conn: Conn,
    task_id: String,
    source_uuid: String,
    sink_uuid: String,
    binding: String,
    checkpoint: Option<ReplicationCheckpoint>,
    observed_gtids: Option<String>,
    observed_position: Option<(String, u64)>,
}
impl CheckpointWriter {
    /// Creates CDC control objects, never business tables.
    pub fn open(
        config: &TargetConfig,
        task_id: &str,
        source_uuid: &str,
        binding: &str,
    ) -> io::Result<Self> {
        if task_id.is_empty()
            || task_id.len() > 128
            || !task_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
            || source_uuid.is_empty()
            || source_uuid.len() > 255
            || !source_uuid.is_ascii()
            || binding.len() != 64
            || !binding.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Err(io::Error::other(
                "invalid task identity or configuration binding",
            ));
        }
        let mut conn = sql::connect(config, None)?;
        let sink_uuid: String = conn
            .query_first("SELECT @@server_uuid")
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("missing sink UUID"))?;
        if sink_uuid == source_uuid {
            return Err(io::Error::other(
                "source and sink must be different MySQL instances",
            ));
        }
        let acquired: Option<u8> = conn
            .exec_first(
                "SELECT GET_LOCK(SHA2(CONCAT('CDC.log_info:', ?), 256), 0)",
                (task_id,),
            )
            .map_err(io::Error::other)?;
        if acquired != Some(1) {
            return Err(io::Error::other("task is already owned by another writer"));
        }
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC")
            .map_err(io::Error::other)?;
        let migration_lock: Option<u8> = conn
            .query_first("SELECT GET_LOCK('CDC.log_info.schema',5)")
            .map_err(io::Error::other)?;
        if migration_lock != Some(1) {
            return Err(io::Error::other("CDC schema migration is busy"));
        }
        let exists: u64=conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info'").map_err(io::Error::other)?.unwrap_or(0);
        if exists == 0 {
            conn.query_drop(DDL).map_err(io::Error::other)?;
        }
        for (column, ddl) in [
            (
                "phase",
                "ALTER TABLE CDC.log_info ADD COLUMN phase VARCHAR(16) NOT NULL DEFAULT 'incremental'",
            ),
            (
                "snapshot_rows",
                "ALTER TABLE CDC.log_info ADD COLUMN snapshot_rows BIGINT UNSIGNED NOT NULL DEFAULT 0",
            ),
            (
                "snapshot_gtids",
                "ALTER TABLE CDC.log_info ADD COLUMN snapshot_gtids LONGTEXT CHARACTER SET ascii",
            ),
        ] {
            let present: u64=conn.exec_first("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info' AND COLUMN_NAME=?",(column,)).map_err(io::Error::other)?.unwrap_or(0);
            if present == 0 {
                conn.query_drop(ddl).map_err(io::Error::other)?;
            }
        }
        // These columns existed before PostgreSQL source cursors were supported;
        // MODIFY is intentionally unconditional so existing installations are
        // upgraded even though the columns are already present.
        conn.query_drop("ALTER TABLE CDC.log_info MODIFY COLUMN source_uuid VARCHAR(255) CHARACTER SET ascii NOT NULL")
            .map_err(io::Error::other)?;
        conn.query_drop(
            "ALTER TABLE CDC.log_info MODIFY COLUMN mode VARCHAR(32) CHARACTER SET ascii NOT NULL",
        )
        .map_err(io::Error::other)?;
        conn.query_drop("DO RELEASE_LOCK('CDC.log_info.schema')")
            .map_err(io::Error::other)?;
        let engine: Option<String> = conn.query_first(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info'"
        ).map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(io::Error::other("CDC.log_info must use InnoDB"));
        }
        let keys: Vec<String> = conn.query(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info' AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX"
        ).map_err(io::Error::other)?;
        if keys != ["task_id"] {
            return Err(io::Error::other(
                "CDC.log_info requires PRIMARY KEY(task_id)",
            ));
        }
        let checkpoint = load(&mut conn, task_id, false)?;
        if let Some(cp) = &checkpoint {
            check_binding(cp, source_uuid, &sink_uuid, binding)?;
        }
        Ok(Self {
            conn,
            task_id: task_id.into(),
            source_uuid: source_uuid.into(),
            sink_uuid,
            binding: binding.into(),
            checkpoint,
            observed_gtids: None,
            observed_position: None,
        })
    }
    pub fn checkpoint(&self) -> Option<&ReplicationCheckpoint> {
        self.checkpoint.as_ref()
    }

    /// Resolve a lost COMMIT acknowledgement using only the authoritative
    /// checkpoint row. This method never executes business DML. An equal
    /// cursor without the exact source transaction identity is deliberately
    /// returned as Unprovable rather than guessed.
    pub fn resolve_commit_unknown(
        &mut self,
        transaction: &ChangeTransaction,
    ) -> io::Result<change_event::CommitResolution> {
        if transaction.source.id != self.source_uuid || transaction.id.is_empty() {
            return Err(io::Error::other(
                "commit resolution source identity mismatch",
            ));
        }
        let (file, position) = file_position(&transaction.commit_cursor)?;
        let mut tx = self
            .conn
            .start_transaction(TxOpts::default())
            .map_err(io::Error::other)?;
        let cp = load(&mut tx, &self.task_id, true)?
            .ok_or_else(|| io::Error::other("CDC.log_info checkpoint is missing"))?;
        check_binding(&cp, &self.source_uuid, &self.sink_uuid, &self.binding)?;
        if cp.last_transaction_id.as_deref() == Some(transaction.id.as_str()) {
            tx.rollback().map_err(io::Error::other)?;
            return Ok(change_event::CommitResolution::Applied);
        }
        let order = compare_checkpoint_position(
            Some(cp.mode.as_str()),
            &file,
            position,
            &cp.file,
            cp.position,
        )?;
        let applied_by_gtid = if cp.mode == "gtid" {
            let gtid = canonical_gtids(&mut tx, &transaction.id)?;
            let included: u8 = tx
                .exec_first(
                    "SELECT GTID_SUBSET(?, ?)",
                    (&gtid, cp.gtid_set.as_deref().unwrap_or("")),
                )
                .map_err(io::Error::other)?
                .ok_or_else(|| io::Error::other("GTID check returned no value"))?;
            included != 0
        } else {
            false
        };
        let result = if applied_by_gtid || order == Ordering::Less {
            change_event::CommitResolution::Applied
        } else if order == Ordering::Greater {
            change_event::CommitResolution::NotApplied
        } else {
            change_event::CommitResolution::Unprovable
        };
        tx.rollback().map_err(io::Error::other)?;
        Ok(result)
    }

    /// Remember filtered transactions without creating control-transaction feedback loops.
    /// They become durable together with the next apply/advance, never ahead of it.
    pub fn observe(&mut self, transaction: &ChangeTransaction) -> io::Result<()> {
        if !transaction.changes.is_empty() || transaction.source.id != self.source_uuid {
            return Err(io::Error::other(
                "observe requires an empty projection from this source",
            ));
        }
        let (file, position) = file_position(&transaction.commit_cursor)?;
        if let Some((old_file, old_position)) = &self.observed_position
            && compare_checkpoint_position(
                self.checkpoint.as_ref().map(|cp| cp.mode.as_str()),
                &file,
                position,
                old_file,
                *old_position,
            )? != Ordering::Greater
        {
            return Err(io::Error::other(
                "filtered transactions must be observed in source order",
            ));
        }
        if self.checkpoint.as_ref().is_some_and(|cp| cp.mode == "gtid") {
            let combined = format!(
                "{},{}",
                self.observed_gtids.as_deref().unwrap_or(""),
                transaction.id
            );
            self.observed_gtids =
                Some(canonical_gtids(&mut self.conn, combined.trim_matches(','))?);
        }
        self.observed_position = Some((file, position));
        Ok(())
    }

    /// Persist a new route's starting boundary before consuming source transactions.
    pub fn initialize(
        &mut self,
        mode: &str,
        file: &str,
        position: u64,
        gtid_set: Option<&str>,
    ) -> io::Result<ReplicationCheckpoint> {
        if let Some(cp) = &self.checkpoint {
            return Ok(cp.clone());
        }
        validate_checkpoint_position(mode, file, position)?;
        if !["gtid", "binlog", "postgresql_lsn"].contains(&mode)
            || (mode == "gtid") != gtid_set.is_some()
            || (mode == "postgresql_lsn" && gtid_set.is_some())
        {
            return Err(io::Error::other("checkpoint mode and GTID set disagree"));
        }
        let gtid_set = gtid_set
            .map(|set| canonical_gtids(&mut self.conn, set))
            .transpose()?;
        let mut tx = self
            .conn
            .start_transaction(TxOpts::default())
            .map_err(io::Error::other)?;
        tx.exec_drop("INSERT INTO CDC.log_info(task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set) VALUES(?,?,?,?,?,?,?,?)",
            (&self.task_id, &self.source_uuid, &self.sink_uuid, &self.binding, mode, file, position, &gtid_set)).map_err(io::Error::other)?;
        let cp = load(&mut tx, &self.task_id, true)?
            .ok_or_else(|| io::Error::other("checkpoint insert failed"))?;
        tx.commit().map_err(sql::unknown_commit)?;
        self.checkpoint = Some(cp.clone());
        Ok(cp)
    }

    /// Fail before locking the source if initial target contents are incompatible.
    pub fn check_snapshot_targets(
        &mut self,
        tables: &[change_event::SnapshotTable],
    ) -> io::Result<()> {
        sql::snapshot_targets(&mut self.conn, tables, false)
    }
    /// A snapshot marker is durable, but none of its business rows are committed yet.
    /// A crashed snapshot restarts with a new read view and new boundary.
    pub fn prepare_snapshot(
        &mut self,
        boundary: &change_event::SnapshotBoundary,
    ) -> io::Result<ReplicationCheckpoint> {
        if boundary.source.id != self.source_uuid {
            return Err(io::Error::other("snapshot source identity mismatch"));
        }
        let (mode, file, position, executed_gtids) = decode_snapshot_boundary(&boundary.cursor)?;
        validate_position(&file, position)?;
        if !["gtid", "binlog"].contains(&mode.as_str()) {
            return Err(io::Error::other("invalid snapshot mode"));
        }
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.phase != "snapshot")
        {
            return Err(io::Error::other(
                "incremental checkpoint cannot be replaced by a snapshot",
            ));
        }
        let executed = canonical_gtids(&mut self.conn, &executed_gtids)?;
        let gtids = (mode == "gtid").then_some(executed.clone());
        let mut tx = self
            .conn
            .start_transaction(TxOpts::default())
            .map_err(io::Error::other)?;
        let actual = load(&mut tx, &self.task_id, true)?;
        if actual != self.checkpoint {
            return Err(io::Error::other(
                "snapshot checkpoint changed outside its owner",
            ));
        }
        if actual.is_some() {
            tx.exec_drop("UPDATE CDC.log_info SET mode=?,binlog_file=?,binlog_position=?,gtid_set=?,snapshot_gtids=?,revision=revision+1 WHERE task_id=?", (&mode,&file,position,&gtids,&executed,&self.task_id)).map_err(io::Error::other)?;
        } else {
            tx.exec_drop("INSERT INTO CDC.log_info(task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set,phase,snapshot_gtids) VALUES(?,?,?,?,?,?,?,?, 'snapshot',?)", (&self.task_id,&self.source_uuid,&self.sink_uuid,&self.binding,&mode,&file,position,&gtids,&executed)).map_err(io::Error::other)?;
        }
        let cp = load(&mut tx, &self.task_id, true)?
            .ok_or_else(|| io::Error::other("snapshot checkpoint missing"))?;
        tx.commit().map_err(sql::unknown_commit)?;
        self.checkpoint = Some(cp.clone());
        Ok(cp)
    }
    /// First edition: all full rows and the transition to incremental commit together.
    /// Failure/stop drops the transaction; recovery consults this connection's durable marker.
    pub fn copy_snapshot(
        &mut self,
        tables: &[change_event::SnapshotTable],
        batches: &mut dyn Iterator<Item = io::Result<change_event::SnapshotBatch>>,
        progress: &mut dyn FnMut(&change_event::SnapshotBatch, u64) -> io::Result<()>,
    ) -> io::Result<ReplicationCheckpoint> {
        let expected = self
            .checkpoint
            .clone()
            .ok_or_else(|| io::Error::other("snapshot was not prepared"))?;
        if expected.phase != "snapshot" {
            return Err(io::Error::other("snapshot phase required"));
        }
        let mut tx = self
            .conn
            .start_transaction(
                TxOpts::default()
                    .set_isolation_level(Some(mysql_driver::IsolationLevel::RepeatableRead)),
            )
            .map_err(io::Error::other)?;
        if load(&mut tx, &self.task_id, true)?.as_ref() != Some(&expected) {
            return Err(io::Error::other("snapshot checkpoint changed"));
        }
        let engine: Option<String> = tx.query_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info'").map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(io::Error::other("CDC.log_info must use InnoDB"));
        }
        sql::snapshot_targets(&mut tx, tables, true)?;
        let mut table_index = 0usize;
        let mut row_count = 0u64;
        for batch in batches {
            let batch = batch?;
            let scope = tables
                .get(table_index)
                .ok_or_else(|| io::Error::other("unexpected extra snapshot table"))?;
            if batch.source.id != self.source_uuid
                || batch.schema != scope.schema
                || batch.table != scope.table
            {
                return Err(io::Error::other("snapshot batch identity/order mismatch"));
            }
            let validated = change_event::validate_snapshot(batch).map_err(io::Error::other)?;
            let plan = sql::snapshot_sql(&validated)?;
            sql::execute_snapshot(&mut tx, &plan)?;
            let batch = validated.batch();
            row_count = row_count
                .checked_add(batch.rows.len() as u64)
                .ok_or_else(|| io::Error::other("snapshot row count overflow"))?;
            progress(batch, row_count)?;
            if batch.last_in_table {
                table_index += 1;
            }
        }
        if table_index != tables.len() {
            return Err(io::Error::other(
                "snapshot stream ended before all tables completed",
            ));
        }
        tx.exec_drop("UPDATE CDC.log_info SET phase='incremental',snapshot_rows=?,revision=revision+1 WHERE task_id=?", (row_count,&self.task_id)).map_err(io::Error::other)?;
        let cp = load(&mut tx, &self.task_id, true)?
            .ok_or_else(|| io::Error::other("snapshot checkpoint disappeared"))?;
        tx.commit().map_err(sql::unknown_commit)?;
        self.checkpoint = Some(cp.clone());
        Ok(cp)
    }

    pub fn apply(&mut self, plan: &SqlTransaction) -> io::Result<CheckpointApplyResult> {
        self.apply_inner(
            &plan.source_uuid,
            plan.source_transaction_id(),
            &plan.commit_cursor,
            Some(plan),
        )
    }

    /// Advance past a committed source transaction with no rows in this route's scope.
    pub fn advance(
        &mut self,
        transaction: &ChangeTransaction,
    ) -> io::Result<CheckpointApplyResult> {
        if !transaction.changes.is_empty() {
            return Err(io::Error::other(
                "advance requires an empty route projection",
            ));
        }
        self.apply_inner(
            &transaction.source.id,
            &transaction.id,
            &transaction.commit_cursor,
            None,
        )
    }

    fn apply_inner(
        &mut self,
        source_uuid: &str,
        transaction_id: &str,
        cursor: &SourceCursor,
        plan: Option<&SqlTransaction>,
    ) -> io::Result<CheckpointApplyResult> {
        if source_uuid != self.source_uuid
            || transaction_id.len() > 512
            || transaction_id.is_empty()
            || !transaction_id.is_ascii()
        {
            return Err(io::Error::other("transaction source identity mismatch"));
        }
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.phase != "incremental")
        {
            return Err(io::Error::other("snapshot is not committed yet"));
        }
        let (file, position) = file_position(cursor)?;
        let expected = self
            .checkpoint
            .as_ref()
            .ok_or_else(|| io::Error::other("initialize checkpoint before apply"))?;
        let mut tx = self
            .conn
            .start_transaction(TxOpts::default())
            .map_err(io::Error::other)?;
        let mut cp = load(&mut tx, &self.task_id, true)?.ok_or_else(|| {
            io::Error::other("CDC.log_info checkpoint is missing; refusing a new baseline")
        })?;
        // load() holds the control table's metadata lock through this commit.
        let engine: Option<String> = tx.query_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info'").map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(io::Error::other("CDC.log_info is no longer InnoDB"));
        }
        check_binding(&cp, &self.source_uuid, &self.sink_uuid, &self.binding)?;
        if &cp != expected {
            return Err(io::Error::other("checkpoint changed outside this writer"));
        }
        let order = compare_checkpoint_position(
            Some(cp.mode.as_str()),
            &file,
            position,
            &cp.file,
            cp.position,
        )?;
        let already_applied = if cp.mode == "gtid" {
            let gtid = canonical_gtids(&mut tx, transaction_id)?;
            let (_, sequence) = transaction_id
                .rsplit_once(':')
                .ok_or_else(|| io::Error::other("missing transaction GTID"))?;
            if transaction_id.contains(',')
                || sequence.parse::<u64>().ok().filter(|n| *n > 0).is_none()
            {
                return Err(io::Error::other("invalid single transaction GTID"));
            }
            let included: u8 = tx
                .exec_first(
                    "SELECT GTID_SUBSET(?, ?)",
                    (&gtid, cp.gtid_set.as_deref().unwrap_or("")),
                )
                .map_err(io::Error::other)?
                .ok_or_else(|| io::Error::other("GTID check returned no value"))?;
            if included == 0 && order != Ordering::Greater {
                return Err(io::Error::other("GTID and binlog history disagree"));
            }
            included != 0
        } else {
            order != Ordering::Greater
        };
        if already_applied {
            tx.rollback().map_err(io::Error::other)?;
            return Ok(CheckpointApplyResult {
                checkpoint: cp,
                statements_executed: 0,
                already_applied: true,
            });
        }
        if let Some((observed_file, observed_position)) = &self.observed_position
            && compare_checkpoint_position(
                Some(cp.mode.as_str()),
                &file,
                position,
                observed_file,
                *observed_position,
            )? == Ordering::Less
        {
            return Err(io::Error::other(
                "cannot commit ahead-of-cursor GTID observations",
            ));
        }
        let rows = if let Some(plan) = plan {
            sql::execute_statements(&mut tx, plan)?;
            plan.statements().count()
        } else {
            0
        };
        if cp.mode == "gtid" {
            let combined = [
                cp.gtid_set.as_deref().unwrap_or(""),
                self.observed_gtids.as_deref().unwrap_or(""),
                transaction_id,
            ]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(",");
            cp.gtid_set = Some(canonical_gtids(&mut tx, combined.trim_matches(','))?);
        }
        cp.file = file;
        cp.position = position;
        cp.last_transaction_id = Some(transaction_id.into());
        cp.revision = cp
            .revision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("checkpoint revision overflow"))?;
        cp.applied_transactions += u64::from(rows > 0);
        cp.applied_rows += rows as u64;
        tx.exec_drop("UPDATE CDC.log_info SET binlog_file=?,binlog_position=?,gtid_set=?,last_transaction_id=?,revision=?,applied_transactions=?,applied_rows=? WHERE task_id=?",
            (&cp.file, cp.position, &cp.gtid_set, &cp.last_transaction_id, cp.revision, cp.applied_transactions, cp.applied_rows, &self.task_id)).map_err(io::Error::other)?;
        tx.commit().map_err(sql::unknown_commit)?;
        self.checkpoint = Some(cp.clone());
        self.observed_gtids = None;
        self.observed_position = None;
        Ok(CheckpointApplyResult {
            checkpoint: cp,
            statements_executed: rows,
            already_applied: false,
        })
    }
}
fn check_binding(
    cp: &ReplicationCheckpoint,
    source: &str,
    sink: &str,
    binding: &str,
) -> io::Result<()> {
    if cp.source_uuid != source || cp.sink_uuid != sink || cp.binding != binding {
        return Err(io::Error::other(
            "saved checkpoint belongs to a different source, sink or task configuration",
        ));
    }
    if !["snapshot", "incremental"].contains(&cp.phase.as_str()) {
        return Err(io::Error::other("invalid checkpoint phase"));
    }
    validate_checkpoint_position(&cp.mode, &cp.file, cp.position)?;
    if !["gtid", "binlog", "postgresql_lsn"].contains(&cp.mode.as_str())
        || (cp.mode == "gtid") != cp.gtid_set.is_some()
        || (cp.mode == "postgresql_lsn" && cp.gtid_set.is_some())
    {
        return Err(io::Error::other("invalid saved checkpoint mode"));
    }
    Ok(())
}
fn field<T: FromValue>(row: &Row, name: &str) -> io::Result<T> {
    row.get_opt(name)
        .ok_or_else(|| io::Error::other(format!("CDC.log_info missing {name}")))?
        .map_err(io::Error::other)
}
fn load(
    conn: &mut impl Queryable,
    task_id: &str,
    lock: bool,
) -> io::Result<Option<ReplicationCheckpoint>> {
    let row: Option<Row> = conn
        .exec_first(
            format!(
                "SELECT {FIELDS} FROM CDC.log_info WHERE task_id=?{}",
                if lock { " FOR UPDATE" } else { "" }
            ),
            (task_id,),
        )
        .map_err(io::Error::other)?;
    row.map(|r| {
        Ok(ReplicationCheckpoint {
            task_id: field(&r, "task_id")?,
            source_uuid: field(&r, "source_uuid")?,
            sink_uuid: field(&r, "sink_uuid")?,
            binding: field(&r, "binding")?,
            mode: field(&r, "mode")?,
            file: field(&r, "binlog_file")?,
            position: field(&r, "binlog_position")?,
            gtid_set: field(&r, "gtid_set")?,
            last_transaction_id: field(&r, "last_transaction_id")?,
            revision: field(&r, "revision")?,
            applied_transactions: field(&r, "applied_transactions")?,
            applied_rows: field(&r, "applied_rows")?,
            phase: field(&r, "phase")?,
            snapshot_rows: field(&r, "snapshot_rows")?,
            snapshot_gtids: field(&r, "snapshot_gtids")?,
        })
    })
    .transpose()
}
fn canonical_gtids(conn: &mut impl Queryable, value: &str) -> io::Result<String> {
    conn.exec_first("SELECT GTID_SUBTRACT(?, '')", (value,))
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("invalid GTID set"))
}
pub(crate) fn file_position(cursor: &SourceCursor) -> io::Result<(String, u64)> {
    if cursor.format == "postgresql.lsn.v1" {
        let (upper, lower) = cursor
            .value
            .split_once('/')
            .ok_or_else(|| io::Error::other("invalid PostgreSQL LSN"))?;
        let upper = u64::from_str_radix(upper, 16).map_err(io::Error::other)?;
        let lower = u64::from_str_radix(lower, 16).map_err(io::Error::other)?;
        if lower > u32::MAX as u64 {
            return Err(io::Error::other("invalid PostgreSQL LSN"));
        }
        return Ok((cursor.value.clone(), (upper << 32) | lower));
    }
    if cursor.format != "mysql.binlog.file-position.v1" {
        return Err(io::Error::other("unsupported source cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(&cursor.value)
        .map_err(io::Error::other)?;
    let split = bytes
        .iter()
        .position(|c| *c == 0)
        .ok_or_else(|| io::Error::other("invalid source cursor"))?;
    let file = String::from_utf8(bytes[..split].to_vec()).map_err(io::Error::other)?;
    let position = u32::from_be_bytes(
        bytes[split + 1..]
            .try_into()
            .map_err(|_| io::Error::other("invalid cursor position"))?,
    ) as u64;
    validate_position(&file, position)?;
    Ok((file, position))
}
fn validate_checkpoint_position(mode: &str, file: &str, position: u64) -> io::Result<()> {
    if mode == "postgresql_lsn" {
        if parse_postgresql_lsn(file)? != position {
            return Err(io::Error::other(
                "PostgreSQL LSN and checkpoint position disagree",
            ));
        }
        Ok(())
    } else {
        validate_position(file, position)
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
fn compare_checkpoint_position(
    mode: Option<&str>,
    a: &str,
    ap: u64,
    b: &str,
    bp: u64,
) -> io::Result<Ordering> {
    if mode == Some("postgresql_lsn") {
        if parse_postgresql_lsn(a)? != ap || parse_postgresql_lsn(b)? != bp {
            return Err(io::Error::other(
                "PostgreSQL LSN and checkpoint position disagree",
            ));
        }
        return Ok(ap.cmp(&bp));
    }
    compare_position(a, ap, b, bp)
}
fn validate_position(file: &str, position: u64) -> io::Result<()> {
    if file.is_empty()
        || file.len() > 255
        || !file.is_ascii()
        || file.contains(['\0', '/', '\\'])
        || !(4..=u32::MAX as u64).contains(&position)
    {
        return Err(io::Error::other("invalid binlog file or position"));
    }
    Ok(())
}
fn compare_position(a: &str, ap: u64, b: &str, bp: u64) -> io::Result<Ordering> {
    if a == b {
        return Ok(ap.cmp(&bp));
    }
    let split = |file: &str| -> io::Result<(String, u64)> {
        let (prefix, number) = file
            .rsplit_once('.')
            .ok_or_else(|| io::Error::other("cannot compare binlog files"))?;
        Ok((prefix.into(), number.parse().map_err(io::Error::other)?))
    };
    let (prefix_a, number_a) = split(a)?;
    let (prefix_b, number_b) = split(b)?;
    if prefix_a != prefix_b {
        return Err(io::Error::other("binlog history prefix changed"));
    }
    Ok(number_a.cmp(&number_b).then(ap.cmp(&bp)))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotation_is_numeric_and_history_changes_are_rejected() {
        assert_eq!(
            compare_position("bin.10", 4, "bin.9", 999).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_position("bin.10", 4, "bin.10", 8).unwrap(),
            Ordering::Less
        );
        assert!(compare_position("other.11", 4, "bin.10", 8).is_err());
    }
}
