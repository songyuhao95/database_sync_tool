BEGIN IMMEDIATE;
CREATE TABLE task_configuration_revisions (
    task_id TEXT NOT NULL REFERENCES replication_tasks(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK(revision > 0),
    name TEXT NOT NULL,
    source_id TEXT NOT NULL,
    sink_id TEXT NOT NULL,
    source_database TEXT NOT NULL,
    sink_database TEXT NOT NULL,
    source_revision INTEGER NOT NULL,
    sink_revision INTEGER NOT NULL,
    start_mode TEXT NOT NULL CHECK(start_mode IN ('auto','gtid','binlog')),
    mappings_json TEXT NOT NULL,
    plan_version TEXT,
    plan_status TEXT NOT NULL,
    plan_invalid_reason TEXT,
    source_metadata_fingerprint TEXT,
    sink_metadata_fingerprint TEXT,
    connector_summary_json TEXT NOT NULL,
    capability_summary_json TEXT NOT NULL,
    capability_manifest_digest TEXT,
    rule_summary_digest TEXT,
    plan_set_digest TEXT,
    plans_json TEXT NOT NULL,
    risk_confirmations_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    PRIMARY KEY(task_id, revision)
);
ALTER TABLE replication_tasks
    ADD COLUMN desired_configuration_revision INTEGER NOT NULL DEFAULT 1
    CHECK(desired_configuration_revision > 0);
ALTER TABLE replication_tasks
    ADD COLUMN effective_configuration_revision INTEGER
    CHECK(effective_configuration_revision IS NULL OR effective_configuration_revision > 0);
INSERT INTO task_configuration_revisions (
    task_id,revision,name,source_id,sink_id,source_database,sink_database,
    source_revision,sink_revision,start_mode,mappings_json,plan_version,plan_status,
    plan_invalid_reason,source_metadata_fingerprint,sink_metadata_fingerprint,
    connector_summary_json,capability_summary_json,capability_manifest_digest,
    rule_summary_digest,plan_set_digest,plans_json,risk_confirmations_json,
    created_at,created_by
)
SELECT id,configuration_revision,name,source_id,sink_id,source_database,sink_database,
       source_revision,sink_revision,start_mode,mappings_json,plan_version,plan_status,
       plan_invalid_reason,source_metadata_fingerprint,sink_metadata_fingerprint,
       connector_summary_json,capability_summary_json,capability_manifest_digest,
       rule_summary_digest,plan_set_digest,plans_json,risk_confirmations_json,
       created_at,created_by
FROM replication_tasks;
UPDATE replication_tasks
SET desired_configuration_revision=configuration_revision;
PRAGMA user_version=11;
COMMIT;
