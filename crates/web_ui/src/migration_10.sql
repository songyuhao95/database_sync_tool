BEGIN IMMEDIATE;
ALTER TABLE replication_tasks ADD COLUMN plan_version TEXT;
ALTER TABLE replication_tasks ADD COLUMN plan_status TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE replication_tasks ADD COLUMN plan_invalid_reason TEXT;
ALTER TABLE replication_tasks ADD COLUMN configuration_revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE replication_tasks ADD COLUMN source_metadata_fingerprint TEXT;
ALTER TABLE replication_tasks ADD COLUMN sink_metadata_fingerprint TEXT;
ALTER TABLE replication_tasks ADD COLUMN connector_summary_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE replication_tasks ADD COLUMN capability_summary_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE replication_tasks ADD COLUMN capability_manifest_digest TEXT;
ALTER TABLE replication_tasks ADD COLUMN rule_summary_digest TEXT;
ALTER TABLE replication_tasks ADD COLUMN plan_set_digest TEXT;
ALTER TABLE replication_tasks ADD COLUMN plans_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE replication_tasks ADD COLUMN risk_confirmations_json TEXT NOT NULL DEFAULT '[]';
CREATE TABLE task_conversion_plans (
    task_id TEXT NOT NULL REFERENCES replication_tasks(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    source_field_lineage TEXT NOT NULL,
    target_field_lineage TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    PRIMARY KEY(task_id, ordinal)
);
CREATE INDEX task_conversion_plans_digest ON task_conversion_plans(task_id, plan_digest);
PRAGMA user_version=10;
COMMIT;
