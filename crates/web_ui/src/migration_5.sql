
BEGIN IMMEDIATE;
CREATE TABLE task_runtime (
 task_id TEXT PRIMARY KEY REFERENCES replication_tasks(id) ON DELETE CASCADE,
 state TEXT NOT NULL CHECK(state IN ('starting','running','stopping','stopped','failed')),
 checkpoint_json TEXT,
 pending_transaction TEXT,
 last_error TEXT,
 started_at INTEGER,
 stopped_at INTEGER,
 applied_transactions INTEGER NOT NULL DEFAULT 0,
 applied_rows INTEGER NOT NULL DEFAULT 0,
 last_applied_at INTEGER
);
CREATE TABLE task_logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 task_id TEXT NOT NULL REFERENCES replication_tasks(id) ON DELETE CASCADE,
 timestamp INTEGER NOT NULL,
 level TEXT NOT NULL,
 message TEXT NOT NULL
);
CREATE INDEX task_logs_route ON task_logs(task_id,id);
PRAGMA user_version=5;
COMMIT;
