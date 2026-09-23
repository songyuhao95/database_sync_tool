//! PostgreSQL 15 committed pgoutput capture. SQL access uses SQLx.
mod catalog;
mod compatibility;
mod metadata;
pub use metadata::{Metadata, databases, metadata, metadata_for_version};
mod checkpoint;
mod decoder;
mod replication;
mod source_contract;
mod sql;
mod type_mapping;
mod types;
pub use catalog::source_type_catalog;
pub use checkpoint::{CheckpointApplyResult, CheckpointWriter, ReplicationCheckpoint};
pub use compatibility::{
    capability_manifest_for, compatibility_manifest, compatibility_manifest_for_version,
    plan_compatibility, structured_capability_manifest, target_capability_manifest,
};
pub use pg_walstream::CancellationToken;
pub use replication::{Config, Replication, replication, replication_for_version};
pub use sql::{
    ApplyResult, CAPABILITY_MANIFEST, CAPABILITY_MANIFEST_16, CAPABILITY_MANIFEST_17, Parameter,
    SinkAdapter, SnapshotSql, SqlTransaction, TargetConfig, capability_manifest,
    capability_manifest_for_version, classify_apply_error, commit_outcome_unknown, execute,
    execute_for_version, probe_target, probe_target_for_version, snapshot_sql, sql,
    sql_for_version, sql_with_plans, sql_with_plans_for_version,
};
pub use type_mapping::{
    MAPPING_VERSION, MAPPING_VERSION_16, MAPPING_VERSION_17, SourceExtension, SourceTypeCatalog,
    SourceTypeDefinition, SourceTypeDefinitionKind, SourceTypeField, SourceTypeMappingError,
    map_source_type, source_type_mapping, source_type_mapping_for_version,
    source_type_mapping_with_catalog, source_type_mapping_with_catalog_for_version,
    source_type_mapping_with_source_evidence, source_type_mapping_with_source_evidence_for_version,
    validate_native_type, validate_native_type_for_version,
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
