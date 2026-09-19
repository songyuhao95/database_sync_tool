BEGIN IMMEDIATE;
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    owner TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('admin','viewer')),
    theme TEXT NOT NULL DEFAULT 'dark' CHECK(theme IN ('dark','light')),
    note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE sessions (
    token_hash BLOB PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    csrf_token TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX session_expiry ON sessions(expires_at);
CREATE TABLE login_attempts (
    name TEXT PRIMARY KEY, window_start INTEGER NOT NULL, attempts INTEGER NOT NULL
);
CREATE TABLE secrets (name TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE instances (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    host TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
    version TEXT NOT NULL CHECK(version IN ('5.7','8.0','8.4')),
    reader_username TEXT NOT NULL, reader_secret BLOB,
    writer_username TEXT NOT NULL, writer_secret BLOB,
    metadata_json TEXT, checked_at INTEGER, probe_error TEXT,
    revision INTEGER NOT NULL DEFAULT 1
);
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
