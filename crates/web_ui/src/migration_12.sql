BEGIN IMMEDIATE;
ALTER TABLE task_runtime RENAME TO task_runtime_v12_old;
CREATE TABLE task_runtime (
 task_id TEXT PRIMARY KEY REFERENCES replication_tasks(id) ON DELETE CASCADE,
 state TEXT NOT NULL CHECK(state IN ('starting','running','stopping','stopped','failed','blocked')),
 checkpoint_json TEXT,
 pending_transaction TEXT,
 last_error TEXT,
 started_at INTEGER,
 stopped_at INTEGER,
 applied_transactions INTEGER NOT NULL DEFAULT 0,
 applied_rows INTEGER NOT NULL DEFAULT 0,
 last_applied_at INTEGER
);
INSERT INTO task_runtime(task_id,state,checkpoint_json,pending_transaction,last_error,started_at,stopped_at,applied_transactions,applied_rows,last_applied_at)
SELECT task_id,state,checkpoint_json,pending_transaction,last_error,started_at,stopped_at,applied_transactions,applied_rows,last_applied_at
FROM task_runtime_v12_old;
DROP TABLE task_runtime_v12_old;
PRAGMA user_version=12;
COMMIT;
