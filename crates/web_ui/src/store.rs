use crate::{
    error::{Error, Result},
    secrets,
};
use aes_gcm::Aes256Gcm;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub struct Store {
    pub(crate) workers: Mutex<std::collections::HashMap<String, crate::runtime::Worker>>,
    pub(crate) log_dir: std::path::PathBuf,
    pub(crate) conn: Mutex<Connection>,
    pub(crate) cipher: Aes256Gcm,
    pub(crate) dummy_hash: String,
}
pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn migrate_instances(conn: &mut Connection) -> Result<()> {
    // Rebuild the CHECK constraint without rewriting task foreign-key references.
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result = (|| {
        let tx = conn.transaction()?;
        tx.execute_batch(include_str!("migration_7.sql"))?;
        if tx
            .prepare("PRAGMA foreign_key_check")?
            .query([])?
            .next()?
            .is_some()
        {
            return Err(Error::Invalid("SQLite 实例升级外键检查失败"));
        }
        tx.commit()?;
        Ok(())
    })();
    let restored = conn.pragma_update(None, "foreign_keys", "ON");
    result?;
    restored?;
    Ok(())
}
fn migrate_task_databases(conn: &mut Connection) -> Result<()> {
    for column in ["source_database", "sink_database"] {
        let present: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('replication_tasks') WHERE name=?1)",
            [column],
            |row| row.get(0),
        )?;
        if !present {
            conn.execute(
                &format!(
                    "ALTER TABLE replication_tasks ADD COLUMN {column} TEXT NOT NULL DEFAULT ''"
                ),
                [],
            )?;
        }
    }
    conn.execute(
        "UPDATE replication_tasks
         SET source_database=(SELECT database_name FROM instances WHERE instances.id=replication_tasks.source_id)
         WHERE source_database='' AND EXISTS(
             SELECT 1 FROM instances
             WHERE instances.id=replication_tasks.source_id
               AND instances.kind='postgresql'
               AND instances.database_name<>''
         )",
        [],
    )?;
    conn.execute(
        "UPDATE replication_tasks
         SET sink_database=(SELECT database_name FROM instances WHERE instances.id=replication_tasks.sink_id)
         WHERE sink_database='' AND EXISTS(
             SELECT 1 FROM instances
             WHERE instances.id=replication_tasks.sink_id
               AND instances.kind='postgresql'
               AND instances.database_name<>''
         )",
        [],
    )?;
    conn.pragma_update(None, "user_version", 9)?;
    Ok(())
}
fn migrate_task_plans(conn: &mut Connection) -> Result<()> {
    let present: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('replication_tasks') WHERE name='plan_version')",
        [],
        |row| row.get(0),
    )?;
    if !present {
        conn.execute_batch(include_str!("migration_10.sql"))?;
        return Ok(());
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS task_conversion_plans (
            task_id TEXT NOT NULL REFERENCES replication_tasks(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            source_field_lineage TEXT NOT NULL,
            target_field_lineage TEXT NOT NULL,
            plan_digest TEXT NOT NULL,
            plan_json TEXT NOT NULL,
            PRIMARY KEY(task_id, ordinal)
        );
        CREATE INDEX IF NOT EXISTS task_conversion_plans_digest ON task_conversion_plans(task_id, plan_digest);
        PRAGMA user_version=10;",
    )?;
    Ok(())
}

fn migrate_task_configuration_revisions(conn: &mut Connection) -> Result<()> {
    let present: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_configuration_revisions')",
        [],
        |row| row.get(0),
    )?;
    if !present {
        conn.execute_batch(include_str!("migration_11.sql"))?;
        return Ok(());
    }

    let has_desired_revision: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('replication_tasks') WHERE name='desired_configuration_revision')",
        [],
        |row| row.get(0),
    )?;
    if !has_desired_revision {
        conn.execute_batch(
            "ALTER TABLE replication_tasks
             ADD COLUMN desired_configuration_revision INTEGER NOT NULL DEFAULT 1
             CHECK(desired_configuration_revision > 0);",
        )?;
    }
    let has_effective_revision: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('replication_tasks') WHERE name='effective_configuration_revision')",
        [],
        |row| row.get(0),
    )?;
    if !has_effective_revision {
        conn.execute_batch(
            "ALTER TABLE replication_tasks
             ADD COLUMN effective_configuration_revision INTEGER
             CHECK(effective_configuration_revision IS NULL OR effective_configuration_revision > 0);",
        )?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO task_configuration_revisions (
             task_id,revision,name,source_id,sink_id,source_database,sink_database,
             source_revision,sink_revision,start_mode,mappings_json,plan_version,plan_status,
             plan_invalid_reason,source_metadata_fingerprint,sink_metadata_fingerprint,
             connector_summary_json,capability_summary_json,capability_manifest_digest,
             rule_summary_digest,plan_set_digest,plans_json,risk_confirmations_json,
             created_at,created_by
         )
         SELECT id,configuration_revision,name,source_id,sink_id,source_database,sink_database,
                source_revision,sink_revision,start_mode,mappings_json,plan_version,plan_status,
                plan_invalid_reason,source_metadata_fingerprint,sink_metadata_fingerprint,
                connector_summary_json,capability_summary_json,capability_manifest_digest,
                rule_summary_digest,plan_set_digest,plans_json,risk_confirmations_json,
                created_at,created_by
           FROM replication_tasks",
        [],
    )?;
    conn.execute(
        "UPDATE replication_tasks SET desired_configuration_revision=configuration_revision",
        [],
    )?;
    conn.pragma_update(None, "user_version", 11)?;
    Ok(())
}

impl Store {
    pub fn open(path: impl AsRef<Path>, key: [u8; 32]) -> Result<Self> {
        let mut conn = Connection::open(path.as_ref())?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > 12 {
            return Err(Error::Invalid("SQLite 数据版本比当前程序新"));
        }
        match version {
            0 => conn.execute_batch(include_str!("schema.sql"))?,
            1 => {
                conn.execute_batch(include_str!("migration_2.sql"))?;
                conn.execute_batch(include_str!("migration_3.sql"))?;
                conn.execute_batch(include_str!("migration_4.sql"))?;
            }
            2 => {
                conn.execute_batch(include_str!("migration_3.sql"))?;
                conn.execute_batch(include_str!("migration_4.sql"))?;
            }
            3 => conn.execute_batch(include_str!("migration_4.sql"))?,
            _ => {}
        }
        let cipher = secrets::cipher(&key);
        if version < 5 {
            conn.execute_batch(include_str!("migration_5.sql"))?;
        }
        if version < 6 {
            conn.execute_batch(include_str!("migration_6.sql"))?;
        }
        if version < 7 {
            migrate_instances(&mut conn)?;
        }
        if version < 8 {
            conn.execute_batch(include_str!("migration_8.sql"))?;
        }
        if version < 9 {
            migrate_task_databases(&mut conn)?;
        }
        if version < 10 {
            migrate_task_plans(&mut conn)?;
        }
        if version < 11 {
            migrate_task_configuration_revisions(&mut conn)?;
        }
        if version < 12 {
            conn.execute_batch(include_str!("migration_12.sql"))?;
        }
        conn.execute("UPDATE task_runtime SET state='stopped',stopped_at=?1 WHERE state IN ('starting','running','stopping')", [now()])?;
        let verifier: Option<Vec<u8>> = conn
            .query_row(
                "SELECT value FROM secrets WHERE name='key_check'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match verifier {
            Some(value) => {
                if secrets::unseal(&cipher, &value, "key_check")? != "cdc.web-ui.v1" {
                    return Err(Error::Internal);
                }
            }
            None => {
                conn.execute(
                    "INSERT INTO secrets(name,value) VALUES('key_check',?1)",
                    params![secrets::seal(&cipher, "cdc.web-ui.v1", "key_check")?],
                )?;
            }
        }
        Ok(Self {
            workers: Mutex::new(std::collections::HashMap::new()),
            log_dir: path
                .as_ref()
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("task-logs"),
            conn: Mutex::new(conn),
            cipher,
            dummy_hash: secrets::hash_password(&secrets::random_token())?,
        })
    }
    pub(crate) fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| Error::Internal)
    }
    pub fn needs_admin(&self) -> Result<bool> {
        Ok(self
            .db()?
            .query_row("SELECT count(*) FROM users", [], |r| r.get::<_, i64>(0))?
            == 0)
    }
}
