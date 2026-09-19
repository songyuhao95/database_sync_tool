BEGIN IMMEDIATE;
ALTER TABLE replication_tasks ADD COLUMN auto_start INTEGER NOT NULL DEFAULT 0 CHECK(auto_start IN (0,1));
PRAGMA user_version=6;
COMMIT;
