//! Persisted startup preference; capture and recovery reuse the normal worker.
use crate::{Error, Result, Store, auth::admin, store::now, tasks::ReplicationTask};
use rusqlite::{OptionalExtension, params};
use std::sync::Arc;

impl Store {
    pub(crate) fn set_task_auto_start(
        &self,
        actor: i64,
        id: &str,
        enabled: bool,
    ) -> Result<ReplicationTask> {
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        if tx.execute(
            "UPDATE replication_tasks SET auto_start=?2 WHERE id=?1",
            params![id, enabled],
        )? != 1
        {
            return Err(Error::NotFound);
        }
        tx.commit()?;
        drop(conn);
        self.task(id)
    }

    /// Called once at Web startup, after binding the listener and before serving.
    /// Saving a preference never starts a worker; failures wait for manual action
    /// or the next service startup, without an automatic retry loop.
    pub fn start_auto_tasks(self: &Arc<Self>) -> Result<usize> {
        let (actor, ids) = {
            let conn = self.db()?;
            // Service-owned tasks outlive the administrator who created them.
            let actor: Option<i64> = conn
                .query_row(
                    "SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let ids = conn
                .prepare(
                    "SELECT id FROM replication_tasks WHERE auto_start=1 ORDER BY created_at,id",
                )?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            (actor, ids)
        };
        if ids.is_empty() {
            return Ok(0);
        }
        let actor = actor.ok_or(Error::Forbidden)?;
        let mut started = 0;
        for id in ids {
            match self.start_task(actor, id.clone()) {
                Ok(_) => {
                    started += 1;
                }
                Err(error) => {
                    let message = format!("自动开始任务失败：{error}");
                    // Preserve checkpoints and any already active worker's state.
                    self.db()?.execute("INSERT INTO task_runtime(task_id,state,last_error,stopped_at) VALUES(?1,'failed',?2,?3) ON CONFLICT(task_id) DO UPDATE SET state='failed',last_error=?2,stopped_at=?3 WHERE task_runtime.state NOT IN ('starting','running','stopping')", params![id,message,now()])?;
                    self.log_task(&id, "error", &message)?;
                }
            }
        }
        Ok(started)
    }
}
