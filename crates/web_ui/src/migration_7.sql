-- Executed in a transaction with foreign keys disabled by Store::open.
CREATE TABLE instances_v7 (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    host TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
    kind TEXT NOT NULL DEFAULT 'mysql' CHECK(kind IN ('mysql','postgresql')),
    version TEXT NOT NULL,
    database_name TEXT NOT NULL DEFAULT '',
    reader_username TEXT NOT NULL, reader_secret BLOB,
    writer_username TEXT NOT NULL, writer_secret BLOB,
    metadata_json TEXT, checked_at INTEGER, probe_error TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    CHECK((kind='mysql' AND version IN ('5.7','8.0','8.4') AND database_name='')
       OR (kind='postgresql' AND version IN ('15','16','17') AND length(database_name) BETWEEN 1 AND 63))
);
INSERT INTO instances_v7
    (id,name,host,port,version,reader_username,reader_secret,writer_username,writer_secret,
     metadata_json,checked_at,probe_error,revision)
SELECT id,name,host,port,version,reader_username,reader_secret,writer_username,writer_secret,
       metadata_json,checked_at,probe_error,revision FROM instances;
DROP TABLE instances;
ALTER TABLE instances_v7 RENAME TO instances;
PRAGMA user_version=7;
