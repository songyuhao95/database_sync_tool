//! MySQL 5.7 capture and target SQL. Native driver types stay inside this crate.
mod binlog;
mod checkpoint;
mod compatibility;
mod decoder;
mod snapshot;
mod sql;
mod type_mapping;
pub use snapshot::{SnapshotReader, snapshot};
pub use sql::{SnapshotSql, snapshot_sql};

pub use checkpoint::{CheckpointApplyResult, CheckpointWriter, ReplicationCheckpoint};
pub use compatibility::{
    capability_manifest_for, compatibility_manifest, plan_compatibility,
    structured_capability_manifest, target_capability_manifest,
};

pub use binlog::{
    BinlogConfig, BinlogPosition, BinlogStartMode, BinlogStream, binlog, validate_change_event,
};
pub use decoder::decode_temporal_components;
pub use sql::{
    ApplyResult, CAPABILITY_MANIFEST, SinkAdapter, SqlTransaction, TargetConfig,
    capability_manifest, classify_apply_error, commit_outcome_unknown, execute, execute_with_plans,
    probe_target, sql, sql_with_plans,
};
pub use type_mapping::{
    MAPPING_VERSION, SourceTypeMappingError, decode_enum_set_ordinal, decode_set_bitmask,
    decode_spatial_value, map_source_type, source_type_mapping, validate_native_type,
};
