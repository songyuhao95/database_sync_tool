//! PostgreSQL 15 committed pgoutput capture. SQL access uses SQLx.
mod catalog;
mod compatibility;
mod metadata;
pub use metadata::{Metadata, databases, metadata};
mod checkpoint;
mod decoder;
mod replication;
mod source_contract;
mod sql;
mod type_mapping;
mod types;
pub use checkpoint::{CheckpointApplyResult, CheckpointWriter, ReplicationCheckpoint};
pub use compatibility::{
    capability_manifest_for, compatibility_manifest, plan_compatibility,
    structured_capability_manifest, target_capability_manifest,
};
pub use pg_walstream::CancellationToken;
pub use replication::{Config, Replication, replication};
pub use sql::{
    ApplyResult, CAPABILITY_MANIFEST, Parameter, SinkAdapter, SnapshotSql, SqlTransaction,
    TargetConfig, capability_manifest, classify_apply_error, commit_outcome_unknown, execute,
    snapshot_sql, sql,
};
pub use type_mapping::{
    MAPPING_VERSION, SourceTypeMappingError, map_source_type, source_type_mapping,
    validate_native_type,
};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Validate one PostgreSQL 15 transaction at the SourceAdapter boundary.
pub fn validate_change_event(
    transaction: change_event::ChangeTransaction,
) -> Result<change_event::ValidatedTransaction> {
    let validated = change_event::validate(transaction)?;
    source_contract::validate(&validated)?;
    Ok(validated)
}
pub(crate) fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}
pub(crate) fn cursor(lsn: u64) -> change_event::SourceCursor {
    let value = pg_walstream::format_lsn(lsn);
    change_event::SourceCursor {
        format: "postgresql.lsn.v1".into(),
        display: value.clone(),
        value,
    }
}
