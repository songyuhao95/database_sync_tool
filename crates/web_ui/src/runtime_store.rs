//! SQLite is a UI mirror; only the Sink's CDC.log_info decides recovery.
use crate::{
    Error, Result, Store,
    auth::admin,
    registry::{ConnectorDescriptor, SinkRegistry, SourceRegistry},
    secrets,
    store::now,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::Write};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub source_uuid: String,
    pub sink_uuid: String,
    pub mode: String,
    pub file: String,
    pub position: u64,
    pub gtid_set: Option<String>,
    #[serde(default)]
    pub applied_transactions: u64,
    #[serde(default)]
    pub applied_rows: u64,
    #[serde(default = "incremental_phase")]
    pub phase: String,
    #[serde(default)]
    pub snapshot_rows: u64,
}
fn incremental_phase() -> String {
    "incremental".into()
}
#[derive(Clone, Serialize, Default)]
pub(crate) struct RuntimeInfo {
    pub checkpoint: Option<Checkpoint>,
    pub pending_transaction: Option<String>,
    pub last_error: Option<String>,
    pub started_at: Option<i64>,
    pub stopped_at: Option<i64>,
    pub applied_transactions: u64,
    pub applied_rows: u64,
    pub last_applied_at: Option<i64>,
}
#[derive(Serialize)]
pub(crate) struct TaskLog {
    pub id: i64,
    pub timestamp: i64,
    pub level: String,
    pub message: String,
}
pub(crate) struct Endpoint {
    pub host: String,
    pub port: u16,
    pub version: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub connector: &'static ConnectorDescriptor,
}
impl Store {
    pub(crate) fn snapshot_progress(&self, id: &str, message: &str) -> Result<()> {
        self.db()?.execute(
            "UPDATE task_runtime SET pending_transaction=?2 WHERE task_id=?1",
            params![id, message],
        )?;
        Ok(())
    }

    pub(crate) fn load_runtime(&self, task: &mut crate::tasks::ReplicationTask) -> Result<()> {
        let row = self.db()?.query_row(
            "SELECT state,checkpoint_json,pending_transaction,last_error,started_at,stopped_at,applied_transactions,applied_rows,last_applied_at FROM task_runtime WHERE task_id=?1", [&task.id],
            |r| Ok((r.get::<_,String>(0)?, r.get::<_,Option<String>>(1)?,
                RuntimeInfo { checkpoint: None, pending_transaction:r.get(2)?,last_error:r.get(3)?,started_at:r.get(4)?,stopped_at:r.get(5)?,
                    applied_transactions:r.get::<_,i64>(6)? as u64,applied_rows:r.get::<_,i64>(7)? as u64,last_applied_at:r.get(8)? }))
        ).optional()?;
        if let Some((state, json, mut info)) = row {
            info.checkpoint = json
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|_| Error::Internal)?;
            task.status = state;
            task.runtime = info;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn endpoint(&self, id: &str, revision: i64, role: &str) -> Result<Endpoint> {
        self.endpoint_for_database(id, revision, role, None)
    }
    pub(crate) fn endpoint_for_database(
        &self,
        id: &str,
        revision: i64,
        role: &str,
        requested_database: Option<&str>,
    ) -> Result<Endpoint> {
        let conn = self.db()?;
        let (kind, version): (String, String) = conn.query_row(
            "SELECT kind,version FROM instances WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let connector = if role == "reader" {
            SourceRegistry.find(&kind, &version)
        } else if role == "writer" {
            SinkRegistry.find(&kind, &version)
        } else {
            None
        }
        .ok_or(Error::Invalid("Web 未注册该数据库连接器"))?;
        let (host,port,default_database,databases_json,user,sealed,actual): (String,u16,String,String,String,Option<Vec<u8>>,i64) =
            conn.query_row(&format!("SELECT host,port,database_name,database_names_json,{role}_username,{role}_secret,revision FROM instances WHERE id=?1"),
                [id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
        if actual != revision {
            return Err(Error::Conflict("实例配置已变化，请重新创建任务"));
        }
        let configured_databases: Vec<String> = serde_json::from_str(&databases_json)
            .unwrap_or_else(|_| {
                if default_database.is_empty() {
                    Vec::new()
                } else {
                    vec![default_database.clone()]
                }
            });
        let database = if kind == "postgresql" {
            let selected = requested_database
                .map(str::to_owned)
                .or_else(|| configured_databases.first().cloned())
                .ok_or(Error::Invalid("PostgreSQL 实例尚未配置连接数据库"))?;
            if !configured_databases.iter().any(|value| value == &selected) {
                return Err(Error::Invalid("所选 PostgreSQL 连接数据库未在实例配置中"));
            }
            selected
        } else {
            String::new()
        };
        let sealed = sealed.ok_or(Error::Invalid("实例未配置相应账号"))?;
        Ok(Endpoint {
            host,
            port,
            version,
            database,
            user,
            password: secrets::unseal(&self.cipher, &sealed, &format!("{id}:{role}"))?,
            connector,
        })
    }
    pub(crate) fn require_admin(&self, actor: i64) -> Result<()> {
        admin(&*self.db()?, actor)
    }
    pub(crate) fn task_logs(&self, id: &str, after: i64) -> Result<Vec<TaskLog>> {
        self.task(id)?;
        let conn = self.db()?;
        Ok(conn.prepare("SELECT id,timestamp,level,message FROM (SELECT id,timestamp,level,message FROM task_logs WHERE task_id=?1 AND id>?2 ORDER BY id DESC LIMIT 200) ORDER BY id")?
            .query_map(params![id,after],|r|Ok(TaskLog {id:r.get(0)?,timestamp:r.get(1)?,level:r.get(2)?,message:r.get(3)?}))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub(crate) fn log_task(&self, id: &str, level: &str, message: &str) -> Result<()> {
        let message: String = message.chars().take(4096).collect();
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO task_logs(task_id,timestamp,level,message) VALUES(?1,?2,?3,?4)",
            params![id, now(), level, message],
        )?;
        tx.execute("DELETE FROM task_logs WHERE task_id=?1 AND id NOT IN (SELECT id FROM task_logs WHERE task_id=?1 ORDER BY id DESC LIMIT 200)",[id])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn append_task_file(&self, id: &str, file: &str, value: &str) -> Result<()> {
        let dir = self.log_dir.join(id);
        std::fs::create_dir_all(&dir).map_err(|_| Error::Internal)?;
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(file))
            .map_err(|_| Error::Internal)?;
        log.write_all(value.as_bytes())
            .map_err(|_| Error::Internal)?;
        log.flush().map_err(|_| Error::Internal)
    }
    pub(crate) fn save_checkpoint(
        &self,
        id: &str,
        checkpoint: &Checkpoint,
        rows: usize,
    ) -> Result<()> {
        let json = serde_json::to_string(checkpoint).map_err(|_| Error::Internal)?;
        let transactions =
            i64::try_from(checkpoint.applied_transactions).map_err(|_| Error::Internal)?;
        let applied_rows = i64::try_from(checkpoint.applied_rows).map_err(|_| Error::Internal)?;
        self.db()?.execute("UPDATE task_runtime SET checkpoint_json=?2,pending_transaction=NULL,applied_transactions=?3,applied_rows=?4,last_applied_at=CASE WHEN ?5>0 THEN ?6 ELSE last_applied_at END WHERE task_id=?1",
            params![id,json,transactions,applied_rows,rows>0,now()])?;
        Ok(())
    }
    pub(crate) fn finish_task(&self, id: &str, error: Option<&str>) -> Result<()> {
        self.finish_task_with_state(id, error, false)
    }

    pub(crate) fn finish_task_with_state(
        &self,
        id: &str,
        error: Option<&str>,
        blocked: bool,
    ) -> Result<()> {
        self.db()?.execute(
            "UPDATE task_runtime SET state=?2,last_error=?3,stopped_at=?4 WHERE task_id=?1",
            params![
                id,
                if blocked {
                    "blocked"
                } else if error.is_some() {
                    "failed"
                } else {
                    "stopped"
                },
                error,
                now()
            ],
        )?;
        self.log_task(
            id,
            if blocked || error.is_some() {
                "error"
            } else {
                "info"
            },
            error.unwrap_or("任务已停止，进度保存在目的端 CDC.log_info"),
        )
    }
    #[cfg(test)]
    pub(crate) fn begin_task(&self, actor: i64, id: &str) -> Result<()> {
        let task = self.task(id)?;
        self.begin_task_at_revision(actor, id, task.configuration_revision, false)
    }

    pub(crate) fn activate_task(&self, actor: i64, id: &str, desired_revision: i64) -> Result<()> {
        self.begin_task_at_revision(actor, id, desired_revision, true)
    }

    fn begin_task_at_revision(
        &self,
        actor: i64,
        id: &str,
        expected_revision: i64,
        require_valid_plan: bool,
    ) -> Result<()> {
        let task = self.task(id)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        let current: (i64, i64, i64, i64, i64, i64, String, Option<String>, i64) = tx.query_row(
            "SELECT t.configuration_revision,t.desired_configuration_revision,
                    t.source_revision,t.sink_revision,s.revision,d.revision,
                    t.plan_status,t.plan_version,json_array_length(t.plans_json)
             FROM replication_tasks t
             JOIN instances s ON s.id=t.source_id
             JOIN instances d ON d.id=t.sink_id
             WHERE t.id=?1",
            [id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )?;
        if current.0 != expected_revision || current.1 != expected_revision {
            return Err(Error::Conflict(
                "Desired Configuration 已变化，请重新预检后再启动",
            ));
        }
        if current.2 != current.4 || current.3 != current.5 {
            return Err(Error::Conflict("关联实例配置已变化，请重新创建任务"));
        }
        if require_valid_plan
            && (current.6 != "valid"
                || current.7.as_deref() != Some(crate::tasks::TASK_PLAN_VERSION)
                || current.8 <= 0)
        {
            return Err(Error::Conflict(
                "任务没有有效的 ColumnConversionPlan，请先重新预检",
            ));
        }
        let active: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM task_runtime WHERE task_id=?1 AND state IN ('starting','running','stopping'))",[id],|r|r.get(0))?;
        if active {
            return Err(Error::Conflict("任务已经启动或正在停止"));
        }
        let others:Vec<(String,String)>=tx.prepare("SELECT t.sink_database,t.mappings_json FROM replication_tasks t JOIN task_runtime r ON r.task_id=t.id WHERE t.sink_id=?1 AND r.state IN ('starting','running','stopping')")?
            .query_map([&task.sink_id],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        for (other_database, other) in others {
            if other_database != task.sink_database {
                continue;
            }
            let mappings: Vec<crate::tasks::TableMapping> =
                serde_json::from_str(&other).map_err(|_| Error::Internal)?;
            if overlaps(&mappings, &task.mappings) {
                return Err(Error::Conflict("另一个运行任务正在写入相同的目的表"));
            }
        }
        tx.execute(
            "UPDATE replication_tasks SET effective_configuration_revision=?2 WHERE id=?1",
            params![id, expected_revision],
        )?;
        tx.execute("INSERT INTO task_runtime(task_id,state,started_at) VALUES(?1,'starting',?2) ON CONFLICT(task_id) DO UPDATE SET state='starting',last_error=NULL,started_at=?2,stopped_at=NULL",params![id,now()])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn mark_running(&self, id: &str, checkpoint: &Checkpoint) -> Result<()> {
        let task = self.task(id)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        let others:Vec<(String,String,String)>=tx.prepare("SELECT t.sink_database,t.mappings_json,r.checkpoint_json FROM task_runtime r JOIN replication_tasks t ON t.id=r.task_id WHERE r.task_id<>?1 AND r.state IN ('starting','running','stopping') AND r.checkpoint_json IS NOT NULL")?
            .query_map([id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
        for (sink_database, mapping, cp) in others {
            let other: Checkpoint = serde_json::from_str(&cp).map_err(|_| Error::Internal)?;
            if other.sink_uuid != checkpoint.sink_uuid {
                continue;
            }
            if sink_database != task.sink_database {
                continue;
            }
            let mapping: Vec<crate::tasks::TableMapping> =
                serde_json::from_str(&mapping).map_err(|_| Error::Internal)?;
            if overlaps(&mapping, &task.mappings) {
                return Err(Error::Conflict("相同 MySQL 目的表已被另一个任务占用"));
            }
        }
        let json = serde_json::to_string(checkpoint).map_err(|_| Error::Internal)?;
        tx.execute("UPDATE task_runtime SET state=CASE WHEN state='stopping' THEN state ELSE 'running' END,checkpoint_json=?2 WHERE task_id=?1",params![id,json])?;
        tx.commit()?;
        Ok(())
    }
}
fn overlaps(a: &[crate::tasks::TableMapping], b: &[crate::tasks::TableMapping]) -> bool {
    a.iter().any(|a| {
        b.iter()
            .any(|b| a.sink_schema == b.sink_schema && a.sink_table == b.sink_table)
    })
}
