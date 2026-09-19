//! PostgreSQL Sink progress committed atomically with business data.
use crate::sql::{self, SqlTransaction, TargetConfig};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, SnapshotBatch, SnapshotBoundary, SnapshotTable, SourceCursor,
};
use sqlx::{Connection, PgConnection, Postgres, Row, Transaction};
use std::{cmp::Ordering, collections::BTreeMap, io};

const DDL: &str = "CREATE TABLE IF NOT EXISTS cdc.log_info(
 task_id text PRIMARY KEY,source_uuid varchar(255) NOT NULL,sink_uuid text NOT NULL,binding text NOT NULL,
 mode varchar(32) NOT NULL,binlog_file text NOT NULL,binlog_position bigint NOT NULL,gtid_set text,
 last_transaction_id text,revision bigint NOT NULL DEFAULT 0,applied_transactions bigint NOT NULL DEFAULT 0,
 applied_rows bigint NOT NULL DEFAULT 0,phase text NOT NULL DEFAULT 'incremental',snapshot_rows bigint NOT NULL DEFAULT 0,
 snapshot_gtids text,updated_at timestamptz NOT NULL DEFAULT clock_timestamp())";

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

pub struct CheckpointWriter {
    runtime: tokio::runtime::Runtime,
    conn: PgConnection,
    task_id: String,
    source_uuid: String,
    sink_uuid: String,
    binding: String,
    checkpoint: Option<ReplicationCheckpoint>,
    observed_gtids: Option<String>,
    observed_position: Option<(String, u64)>,
}

impl CheckpointWriter {
    pub fn open(
        config: &TargetConfig,
        task_id: &str,
        source_uuid: &str,
        binding: &str,
    ) -> io::Result<Self> {
        validate_identity(task_id, source_uuid, binding)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(io::Error::other)?;
        let mut conn = runtime.block_on(sql::connect(config))?;
        let (sink_uuid, checkpoint) = runtime.block_on(async {
            let mut tx = conn.begin().await.map_err(io::Error::other)?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('cdc.log_info.schema',0))")
                .execute(&mut *tx)
                .await
                .map_err(io::Error::other)?;
            sqlx::query("CREATE SCHEMA IF NOT EXISTS cdc")
                .execute(&mut *tx)
                .await
                .map_err(io::Error::other)?;
            sqlx::query("CREATE TABLE IF NOT EXISTS cdc.sink_identity(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),sink_uuid uuid NOT NULL)")
                .execute(&mut *tx).await.map_err(io::Error::other)?;
            sqlx::query("INSERT INTO cdc.sink_identity(singleton,sink_uuid) VALUES(true,gen_random_uuid()) ON CONFLICT(singleton) DO NOTHING")
                .execute(&mut *tx).await.map_err(io::Error::other)?;
            sqlx::query(DDL).execute(&mut *tx).await.map_err(io::Error::other)?;
            // Existing installations may still have the pre-Web source UUID
            // width; widen the columns before loading or writing a checkpoint.
            sqlx::query("ALTER TABLE cdc.log_info ALTER COLUMN source_uuid TYPE varchar(255), ALTER COLUMN mode TYPE varchar(32)")
                .execute(&mut *tx).await.map_err(io::Error::other)?;
            let sink_uuid: String =
                sqlx::query_scalar("SELECT sink_uuid::text FROM cdc.sink_identity WHERE singleton=true")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(io::Error::other)?;
            tx.commit().await.map_err(sql::commit_error)?;
            let owned: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_lock(hashtextextended($1,0))",
            )
            .bind(format!("cdc.log_info:{task_id}"))
            .fetch_one(&mut conn)
            .await
            .map_err(io::Error::other)?;
            if !owned {
                return Err(io::Error::other("task is already owned by another writer"));
            }
            let checkpoint = load_conn(&mut conn, task_id).await?;
            Ok::<_, io::Error>((sink_uuid, checkpoint))
        })?;
        if sink_uuid == source_uuid {
            return Err(io::Error::other("source and sink identities must differ"));
        }
        if let Some(cp) = &checkpoint {
            check_binding(cp, source_uuid, &sink_uuid, binding)?;
        }
        Ok(Self {
            runtime,
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

    /// Resolve a lost COMMIT acknowledgement from the authoritative row only.
    /// No business statement is executed while resolving the outcome.
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
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        let cp = self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .ok_or_else(|| io::Error::other("cdc.log_info checkpoint is missing"))?;
        check_binding(&cp, &self.source_uuid, &self.sink_uuid, &self.binding)?;
        if cp.last_transaction_id.as_deref() == Some(transaction.id.as_str()) {
            self.runtime
                .block_on(tx.rollback())
                .map_err(io::Error::other)?;
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
            let one = canonical_single_gtid(&transaction.id)?;
            gtid_contains(cp.gtid_set.as_deref().unwrap_or(""), &one)?
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
        self.runtime
            .block_on(tx.rollback())
            .map_err(io::Error::other)?;
        Ok(result)
    }

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
            self.observed_gtids = Some(merge_gtids(
                self.observed_gtids.as_deref(),
                &transaction.id,
            )?);
        }
        self.observed_position = Some((file, position));
        Ok(())
    }

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
        let gtid_set = gtid_set.map(canonical_gtids).transpose()?;
        let mut tx = self
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        self.runtime
            .block_on(
                sqlx::query("INSERT INTO cdc.log_info(task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(&self.task_id).bind(&self.source_uuid).bind(&self.sink_uuid)
                    .bind(&self.binding).bind(mode).bind(file).bind(to_i64(position)?)
                    .bind(&gtid_set).execute(&mut *tx),
            )
            .map_err(io::Error::other)?;
        let cp = self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .ok_or_else(|| io::Error::other("checkpoint insert failed"))?;
        self.runtime
            .block_on(tx.commit())
            .map_err(sql::commit_error)?;
        self.checkpoint = Some(cp.clone());
        Ok(cp)
    }

    pub fn check_snapshot_targets(&mut self, tables: &[SnapshotTable]) -> io::Result<()> {
        let mut tx = self
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        self.runtime
            .block_on(sql::snapshot_targets(&mut tx, tables, false))?;
        self.runtime
            .block_on(tx.rollback())
            .map_err(io::Error::other)
    }

    pub fn prepare_snapshot(
        &mut self,
        boundary: &SnapshotBoundary,
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
        let executed = canonical_gtids(&executed_gtids)?;
        let gtids = (mode == "gtid").then_some(executed.clone());
        let mut tx = self
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        let actual = self.runtime.block_on(load(&mut tx, &self.task_id, true))?;
        if actual != self.checkpoint {
            return Err(io::Error::other(
                "snapshot checkpoint changed outside its owner",
            ));
        }
        if actual.is_some() {
            self.runtime.block_on(
                sqlx::query("UPDATE cdc.log_info SET mode=$1,binlog_file=$2,binlog_position=$3,gtid_set=$4,snapshot_gtids=$5,revision=revision+1,updated_at=clock_timestamp() WHERE task_id=$6")
                    .bind(&mode).bind(&file).bind(to_i64(position)?)
                    .bind(&gtids).bind(&executed).bind(&self.task_id).execute(&mut *tx),
            ).map_err(io::Error::other)?;
        } else {
            self.runtime.block_on(
                sqlx::query("INSERT INTO cdc.log_info(task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set,phase,snapshot_gtids) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'snapshot',$9)")
                    .bind(&self.task_id).bind(&self.source_uuid).bind(&self.sink_uuid)
                    .bind(&self.binding).bind(&mode).bind(&file)
                    .bind(to_i64(position)?).bind(&gtids).bind(&executed)
                    .execute(&mut *tx),
            ).map_err(io::Error::other)?;
        }
        let cp = self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .ok_or_else(|| io::Error::other("snapshot checkpoint missing"))?;
        self.runtime
            .block_on(tx.commit())
            .map_err(sql::commit_error)?;
        self.checkpoint = Some(cp.clone());
        Ok(cp)
    }

    pub fn copy_snapshot(
        &mut self,
        tables: &[SnapshotTable],
        batches: &mut dyn Iterator<Item = io::Result<SnapshotBatch>>,
        progress: &mut dyn FnMut(&SnapshotBatch, u64) -> io::Result<()>,
    ) -> io::Result<ReplicationCheckpoint> {
        let expected = self
            .checkpoint
            .clone()
            .ok_or_else(|| io::Error::other("snapshot was not prepared"))?;
        if expected.phase != "snapshot" {
            return Err(io::Error::other("snapshot phase required"));
        }
        let mut tx = self
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        if self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .as_ref()
            != Some(&expected)
        {
            return Err(io::Error::other("snapshot checkpoint changed"));
        }
        self.runtime
            .block_on(sql::snapshot_targets(&mut tx, tables, true))?;
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
            self.runtime
                .block_on(sql::execute_snapshot(&mut tx, &plan))?;
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
        self.runtime.block_on(
            sqlx::query("UPDATE cdc.log_info SET phase='incremental',snapshot_rows=$1,revision=revision+1,updated_at=clock_timestamp() WHERE task_id=$2")
                .bind(to_i64(row_count)?).bind(&self.task_id).execute(&mut *tx),
        ).map_err(io::Error::other)?;
        let cp = self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .ok_or_else(|| io::Error::other("snapshot checkpoint disappeared"))?;
        self.runtime
            .block_on(tx.commit())
            .map_err(sql::commit_error)?;
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
            || transaction_id.is_empty()
            || transaction_id.len() > 512
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
            .clone()
            .ok_or_else(|| io::Error::other("initialize checkpoint before apply"))?;
        let mut tx = self
            .runtime
            .block_on(self.conn.begin())
            .map_err(io::Error::other)?;
        let mut cp = self
            .runtime
            .block_on(load(&mut tx, &self.task_id, true))?
            .ok_or_else(|| io::Error::other("cdc.log_info checkpoint is missing"))?;
        check_binding(&cp, &self.source_uuid, &self.sink_uuid, &self.binding)?;
        if cp != expected {
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
            let one = canonical_single_gtid(transaction_id)?;
            let included = gtid_contains(cp.gtid_set.as_deref().unwrap_or(""), &one)?;
            if !included && order != Ordering::Greater {
                return Err(io::Error::other("GTID and binlog history disagree"));
            }
            included
        } else {
            order != Ordering::Greater
        };
        if already_applied {
            self.runtime
                .block_on(tx.rollback())
                .map_err(io::Error::other)?;
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
            self.runtime
                .block_on(sql::execute_statements(&mut tx, plan))?;
            plan.statements().count()
        } else {
            0
        };
        if cp.mode == "gtid" {
            let merged = merge_gtids(
                cp.gtid_set.as_deref(),
                self.observed_gtids.as_deref().unwrap_or(""),
            )?;
            cp.gtid_set = Some(merge_gtids(Some(&merged), transaction_id)?);
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
        self.runtime.block_on(
            sqlx::query("UPDATE cdc.log_info SET binlog_file=$1,binlog_position=$2,gtid_set=$3,last_transaction_id=$4,revision=$5,applied_transactions=$6,applied_rows=$7,updated_at=clock_timestamp() WHERE task_id=$8")
                .bind(&cp.file).bind(to_i64(cp.position)?).bind(&cp.gtid_set)
                .bind(&cp.last_transaction_id).bind(to_i64(cp.revision)?)
                .bind(to_i64(cp.applied_transactions)?).bind(to_i64(cp.applied_rows)?)
                .bind(&self.task_id).execute(&mut *tx),
        ).map_err(io::Error::other)?;
        self.runtime
            .block_on(tx.commit())
            .map_err(sql::commit_error)?;
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

async fn load_conn(
    conn: &mut PgConnection,
    task_id: &str,
) -> io::Result<Option<ReplicationCheckpoint>> {
    let row = sqlx::query("SELECT task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set,last_transaction_id,revision,applied_transactions,applied_rows,phase,snapshot_rows,snapshot_gtids FROM cdc.log_info WHERE task_id=$1")
        .bind(task_id).fetch_optional(conn).await.map_err(io::Error::other)?;
    row.map(checkpoint_from_row).transpose()
}

async fn load(
    tx: &mut Transaction<'_, Postgres>,
    task_id: &str,
    lock: bool,
) -> io::Result<Option<ReplicationCheckpoint>> {
    let query = format!(
        "SELECT task_id,source_uuid,sink_uuid,binding,mode,binlog_file,binlog_position,gtid_set,last_transaction_id,revision,applied_transactions,applied_rows,phase,snapshot_rows,snapshot_gtids FROM cdc.log_info WHERE task_id=$1{}",
        if lock { " FOR UPDATE" } else { "" }
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(query))
        .bind(task_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(io::Error::other)?;
    row.map(checkpoint_from_row).transpose()
}

fn checkpoint_from_row(row: sqlx::postgres::PgRow) -> io::Result<ReplicationCheckpoint> {
    Ok(ReplicationCheckpoint {
        task_id: row.try_get("task_id").map_err(io::Error::other)?,
        source_uuid: row.try_get("source_uuid").map_err(io::Error::other)?,
        sink_uuid: row.try_get("sink_uuid").map_err(io::Error::other)?,
        binding: row.try_get("binding").map_err(io::Error::other)?,
        mode: row.try_get("mode").map_err(io::Error::other)?,
        file: row.try_get("binlog_file").map_err(io::Error::other)?,
        position: to_u64(row.try_get("binlog_position").map_err(io::Error::other)?)?,
        gtid_set: row.try_get("gtid_set").map_err(io::Error::other)?,
        last_transaction_id: row
            .try_get("last_transaction_id")
            .map_err(io::Error::other)?,
        revision: to_u64(row.try_get("revision").map_err(io::Error::other)?)?,
        applied_transactions: to_u64(
            row.try_get("applied_transactions")
                .map_err(io::Error::other)?,
        )?,
        applied_rows: to_u64(row.try_get("applied_rows").map_err(io::Error::other)?)?,
        phase: row.try_get("phase").map_err(io::Error::other)?,
        snapshot_rows: to_u64(row.try_get("snapshot_rows").map_err(io::Error::other)?)?,
        snapshot_gtids: row.try_get("snapshot_gtids").map_err(io::Error::other)?,
    })
}

fn validate_identity(task: &str, source: &str, binding: &str) -> io::Result<()> {
    if task.is_empty()
        || task.len() > 128
        || !task
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        || source.is_empty()
        || source.len() > 255
        || !source.is_ascii()
        || binding.len() != 64
        || !binding.bytes().all(|c| c.is_ascii_hexdigit())
    {
        Err(io::Error::other(
            "invalid task identity or configuration binding",
        ))
    } else {
        Ok(())
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

fn to_i64(value: u64) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| io::Error::other("checkpoint exceeds PostgreSQL bigint"))
}
fn to_u64(value: i64) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| io::Error::other("negative checkpoint number"))
}
fn file_position(cursor: &SourceCursor) -> io::Result<(String, u64)> {
    if cursor.format == "postgresql.lsn.v1" {
        let position = parse_postgresql_lsn(&cursor.value)?;
        return Ok((cursor.value.clone(), position));
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
        Err(io::Error::other("invalid binlog file or position"))
    } else {
        Ok(())
    }
}
fn compare_position(a: &str, ap: u64, b: &str, bp: u64) -> io::Result<Ordering> {
    if a == b {
        return Ok(ap.cmp(&bp));
    }
    let split = |file: &str| -> io::Result<(String, u64)> {
        let (prefix, number) = file
            .rsplit_once('.')
            .ok_or_else(|| io::Error::other("cannot compare binlog files"))?;
        Ok((prefix.to_owned(), number.parse().map_err(io::Error::other)?))
    };
    let (prefix_a, number_a) = split(a)?;
    let (prefix_b, number_b) = split(b)?;
    if prefix_a != prefix_b {
        return Err(io::Error::other("binlog history prefix changed"));
    }
    Ok(number_a.cmp(&number_b).then(ap.cmp(&bp)))
}

type GtidMap = BTreeMap<(String, Option<String>), Vec<(u64, u64)>>;

fn parse_gtids(value: &str) -> io::Result<GtidMap> {
    let mut map = GtidMap::new();
    for group in value.split(',').filter(|part| !part.is_empty()) {
        let parts = group.split(':').collect::<Vec<_>>();
        if parts.len() < 2 {
            return Err(io::Error::other("invalid GTID set"));
        }
        let uuid = parts[0].to_ascii_lowercase();
        if uuid.len() != 36 {
            return Err(io::Error::other("invalid GTID UUID"));
        }
        let (tag, start) = if is_gtid_interval(parts[1]) {
            (None, 1)
        } else {
            if parts.len() < 3
                || parts[1].is_empty()
                || !parts[1]
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_')
            {
                return Err(io::Error::other("invalid tagged GTID"));
            }
            (Some(parts[1].to_ascii_lowercase()), 2)
        };
        let ranges = map.entry((uuid, tag)).or_default();
        for part in &parts[start..] {
            let (a, b) = match part.split_once('-') {
                Some((a, b)) => (
                    a.parse().map_err(io::Error::other)?,
                    b.parse().map_err(io::Error::other)?,
                ),
                None => {
                    let n = part.parse().map_err(io::Error::other)?;
                    (n, n)
                }
            };
            if a == 0 || b < a {
                return Err(io::Error::other("invalid GTID interval"));
            }
            ranges.push((a, b));
        }
    }
    for ranges in map.values_mut() {
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for &(a, b) in ranges.iter() {
            if let Some(last) = merged.last_mut()
                && a <= last.1.saturating_add(1)
            {
                last.1 = last.1.max(b);
            } else {
                merged.push((a, b));
            }
        }
        *ranges = merged;
    }
    Ok(map)
}

fn is_gtid_interval(value: &str) -> bool {
    match value.split_once('-') {
        Some((start, end)) => {
            !start.is_empty()
                && !end.is_empty()
                && start.bytes().all(|c| c.is_ascii_digit())
                && end.bytes().all(|c| c.is_ascii_digit())
        }
        None => !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()),
    }
}

fn render_gtids(map: &GtidMap) -> String {
    map.iter()
        .map(|((uuid, tag), ranges)| {
            let mut value = uuid.clone();
            if let Some(tag) = tag {
                value.push(':');
                value.push_str(tag);
            }
            for (start, end) in ranges {
                value.push(':');
                value.push_str(&if start == end {
                    start.to_string()
                } else {
                    format!("{start}-{end}")
                });
            }
            value
        })
        .collect::<Vec<_>>()
        .join(",")
}
fn canonical_gtids(value: &str) -> io::Result<String> {
    Ok(render_gtids(&parse_gtids(value)?))
}
fn canonical_single_gtid(value: &str) -> io::Result<String> {
    let map = parse_gtids(value)?;
    if map.len() != 1
        || map
            .values()
            .next()
            .is_none_or(|ranges| ranges.len() != 1 || ranges[0].0 != ranges[0].1)
    {
        return Err(io::Error::other("invalid single transaction GTID"));
    }
    Ok(render_gtids(&map))
}
fn merge_gtids(left: Option<&str>, right: &str) -> io::Result<String> {
    let combined = [left.unwrap_or(""), right]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    canonical_gtids(&combined)
}
fn gtid_contains(set: &str, one: &str) -> io::Result<bool> {
    let set = parse_gtids(set)?;
    let one = parse_gtids(one)?;
    let (key, ranges) = one
        .iter()
        .next()
        .ok_or_else(|| io::Error::other("empty GTID"))?;
    let (start, end) = ranges[0];
    Ok(set
        .get(key)
        .is_some_and(|items| items.iter().any(|(a, b)| *a <= start && *b >= end)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_and_checks_tagged_gtids() {
        let set = merge_gtids(
            Some("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA:1-2"),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:3",
        )
        .unwrap();
        assert_eq!(set, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3");
        assert!(gtid_contains(&set, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:2").unwrap());
        let tagged = canonical_gtids("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:tag:4-5").unwrap();
        assert!(gtid_contains(&tagged, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:tag:5").unwrap());
    }
}
