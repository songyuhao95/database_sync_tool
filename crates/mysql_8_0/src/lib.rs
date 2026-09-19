//! MySQL 8.0 Replication Protocol capture and target SQL.
mod binlog;
mod checkpoint;
mod compatibility;
mod decoder;
mod snapshot;
mod sql;
pub use snapshot::{SnapshotReader, snapshot};
pub use sql::{SnapshotSql, snapshot_sql};

pub use binlog::{
    BinlogConfig, BinlogPosition, BinlogStartMode, BinlogStream, binlog, validate_change_event,
};
pub use checkpoint::{CheckpointApplyResult, CheckpointWriter, ReplicationCheckpoint};
pub use compatibility::{
    capability_manifest_for, compatibility_manifest, plan_compatibility, source_type_mapping,
    structured_capability_manifest, target_capability_manifest,
};
pub use sql::{
    ApplyResult, CAPABILITY_MANIFEST, SinkAdapter, SqlTransaction, TargetConfig,
    capability_manifest, classify_apply_error, commit_outcome_unknown, execute, sql,
};
