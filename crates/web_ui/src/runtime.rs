//! One owned worker per task. Web request lifetimes never own capture lifetimes.
use crate::{Error, Result, Store, store::now};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

pub(crate) struct Worker {
    pub cancel: Arc<AtomicBool>,
    pub handle: JoinHandle<()>,
}
impl Store {
    /// Remove a stopped platform task. Business data, remote checkpoints and file
    /// logs are retained; generated task IDs are never reused.
    pub(crate) fn delete_task(&self, actor: i64, id: &str) -> Result<()> {
        self.require_admin(actor)?;
        // Same lock order as start_task: no new worker can start during deletion.
        let mut workers = self.workers.lock().map_err(|_| Error::Internal)?;
        if workers.get(id).is_some_and(|w| !w.handle.is_finished()) {
            return Err(Error::Conflict("请先停止任务，待停止完成后再删除"));
        }
        self.task(id)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        crate::auth::admin(&tx, actor)?;
        let active: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM task_runtime WHERE task_id=?1 AND state IN ('starting','running','stopping'))",[id],|r|r.get(0))?;
        if active {
            return Err(Error::Conflict("请先停止任务，待停止完成后再删除"));
        }
        if tx.execute("DELETE FROM replication_tasks WHERE id=?1", [id])? != 1 {
            return Err(Error::NotFound);
        }
        tx.commit()?;
        if let Some(worker) = workers.remove(id) {
            let _ = worker.handle.join();
        }
        Ok(())
    }

    pub(crate) fn start_task(
        self: &Arc<Self>,
        actor: i64,
        id: String,
    ) -> Result<crate::tasks::ReplicationTask> {
        self.require_admin(actor)?;
        let task = self.task(&id)?;
        self.ensure_saved_plan_current(actor, &task)?;
        let mut workers = self.workers.lock().map_err(|_| Error::Internal)?;
        if workers.get(&id).is_some_and(|w| !w.handle.is_finished()) {
            return Err(Error::Conflict("任务已经启动或正在停止"));
        }
        if workers.values().filter(|w| !w.handle.is_finished()).count() >= 16 {
            return Err(Error::Conflict("当前最多同时运行 16 个任务"));
        }
        if let Some(old) = workers.remove(&id) {
            let _ = old.handle.join();
        }
        self.activate_task(actor, &id, task.configuration_revision)?;
        let cancel = Arc::new(AtomicBool::new(false));
        let thread_cancel = cancel.clone();
        let store = self.clone();
        let task_id = id.clone();
        let handle = thread::Builder::new()
            .name(format!("cdc-{}", &id[..id.len().min(8)]))
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::task_worker::run(&store, actor, &task_id, &thread_cancel)
                }));
                let (error, blocked) = match outcome {
                    Ok(Ok(())) => (None, false),
                    Ok(Err(crate::Error::Blocked(message))) => (Some(message), true),
                    Ok(Err(e)) => (Some(e.to_string()), false),
                    Err(_) => (
                        Some("任务线程异常退出；再次启动将从目的端 CDC.log_info 恢复".into()),
                        false,
                    ),
                };
                if store
                    .finish_task_with_state(&task_id, error.as_deref(), blocked)
                    .is_err()
                {
                    eprintln!("[web] could not persist final task state");
                }
            })
            .map_err(|_| {
                let _ = self.finish_task(&id, Some("无法创建任务线程"));
                Error::Internal
            })?;
        workers.insert(id.clone(), Worker { cancel, handle });
        drop(workers);
        self.task(&id)
    }
    pub(crate) fn stop_task(&self, actor: i64, id: &str) -> Result<crate::tasks::ReplicationTask> {
        self.require_admin(actor)?;
        self.task(id)?;
        let workers = self.workers.lock().map_err(|_| Error::Internal)?;
        if let Some(worker) = workers.get(id) {
            // Persist the request before setting cancellation; completion cannot be overwritten.
            self.db()?.execute("UPDATE task_runtime SET state='stopping' WHERE task_id=?1 AND state IN ('starting','running')",[id])?;
            worker.cancel.store(true, Ordering::Release);
        }
        drop(workers);
        self.task(id)
    }
    /// Called by the Web process on Ctrl+C. Wait for in-flight target transactions.
    pub fn shutdown_tasks(&self) {
        let workers = match self.workers.lock() {
            Ok(mut registry) => std::mem::take(&mut *registry),
            Err(_) => return,
        };
        for worker in workers.values() {
            worker.cancel.store(true, Ordering::Release);
        }
        for (_, worker) in workers {
            let _ = worker.handle.join();
        }
        let _=self.db().and_then(|conn|{
            conn.execute("UPDATE task_runtime SET state='stopped',stopped_at=?1 WHERE state IN ('starting','running','stopping')",[now()])?;
            Ok(())
        });
    }
}
