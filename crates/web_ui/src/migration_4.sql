BEGIN IMMEDIATE;
CREATE TABLE replication_tasks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_id TEXT NOT NULL REFERENCES instances(id) ON DELETE RESTRICT,
    sink_id TEXT NOT NULL REFERENCES instances(id) ON DELETE RESTRICT,
    source_revision INTEGER NOT NULL,
    sink_revision INTEGER NOT NULL,
    start_mode TEXT NOT NULL CHECK(start_mode IN ('auto','gtid','binlog')),
    mappings_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'configured' CHECK(status='configured'),
    created_at INTEGER NOT NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    CHECK(source_id <> sink_id)
);
PRAGMA user_version=4;
COMMIT;
