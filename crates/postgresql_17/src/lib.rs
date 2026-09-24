//! PostgreSQL 17 SourceTypeMapping.
//!
//! PostgreSQL 17 shares the public value semantics of the PostgreSQL 15
//! mapping, but its connector identity and evidence are version-specific.
//! Replication uses the shared protocol runtime with an explicit PostgreSQL 17
//! server-version check; the live qualification suite asserts the connected
//! server major before it records capture evidence.

use change_event::{ServerBuildIdentity, SourceTypeMapping};
pub use postgresql_15::CAPABILITY_MANIFEST_17 as CAPABILITY_MANIFEST;
pub use postgresql_15::MAPPING_VERSION_17 as MAPPING_VERSION;
pub use postgresql_15::{
    ApplyResult, Parameter, SnapshotSql, SqlTransaction, TargetConfig, classify_apply_error,
    commit_outcome_unknown,
};
pub use postgresql_15::{
    CancellationToken, Config, Replication, SourceExtension, SourceTypeCatalog,
    SourceTypeDefinition, SourceTypeDefinitionKind, SourceTypeField, SourceTypeMappingError,
};
pub type Result<T> = postgresql_15::Result<T>;

#[derive(Debug, Clone, Copy, Default)]
pub struct SinkAdapter;

impl SinkAdapter {
    pub const fn new() -> Self {
        Self
    }
}

impl change_event::SinkAdapter for SinkAdapter {
    type Plan = SqlTransaction;
    type Error = std::io::Error;

    fn capability_manifest(&self) -> change_event::CapabilityManifest {
        postgresql_15::capability_manifest_for_version("17")
    }

    fn qualify(&self, transaction: &change_event::ValidatedTransaction) -> std::io::Result<()> {
        postgresql_15::SinkAdapter::new_for_version("17").qualify(transaction)
    }

    fn plan(
        &self,
        transaction: &change_event::ValidatedTransaction,
    ) -> std::io::Result<Self::Plan> {
        postgresql_15::SinkAdapter::new_for_version("17").plan(transaction)
    }
}

pub fn capability_manifest() -> change_event::CapabilityManifest {
    postgresql_15::capability_manifest_for_version("17")
}

pub fn compatibility_manifest(
    target_build: change_event::ServerBuildIdentity,
) -> change_event::TargetCapabilityManifest {
    postgresql_15::compatibility_manifest_for_version(target_build, "17")
}

pub fn target_capability_manifest(
    target_build: change_event::ServerBuildIdentity,
) -> change_event::TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn structured_capability_manifest(
    target_build: change_event::ServerBuildIdentity,
) -> change_event::TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn plan_compatibility(
    input: change_event::CompatibilityInput<'_>,
) -> std::result::Result<change_event::CompatibilityResult, change_event::CompatibilityError> {
    postgresql_15::plan_compatibility(input)
}

impl SinkAdapter {
    pub fn structured_capability_manifest(
        &self,
        target_build: change_event::ServerBuildIdentity,
    ) -> change_event::TargetCapabilityManifest {
        compatibility_manifest(target_build)
    }

    pub fn plan_compatibility(
        &self,
        input: change_event::CompatibilityInput<'_>,
    ) -> std::result::Result<change_event::CompatibilityResult, change_event::CompatibilityError>
    {
        plan_compatibility(input)
    }

    pub fn sql_with_plans(
        &self,
        transaction: &change_event::ValidatedTransaction,
        plans: &[change_event::ColumnConversionPlan],
    ) -> std::io::Result<SqlTransaction> {
        sql_with_plans(transaction, plans)
    }
}

pub fn sql(transaction: &change_event::ValidatedTransaction) -> std::io::Result<SqlTransaction> {
    postgresql_15::sql_for_version("17", transaction)
}

pub fn sql_with_plans(
    transaction: &change_event::ValidatedTransaction,
    plans: &[change_event::ColumnConversionPlan],
) -> std::io::Result<SqlTransaction> {
    postgresql_15::sql_with_plans_for_version("17", transaction, plans)
}

pub fn snapshot_sql(batch: &change_event::ValidatedSnapshotBatch) -> std::io::Result<SnapshotSql> {
    postgresql_15::snapshot_sql(batch)
}

pub async fn execute(config: &TargetConfig, plan: &SqlTransaction) -> std::io::Result<ApplyResult> {
    postgresql_15::execute_for_version(config, plan).await
}

pub async fn probe_target(
    config: &TargetConfig,
    schema: &str,
    table: &str,
    column: &str,
) -> std::io::Result<change_event::TargetCapabilityProbe> {
    postgresql_15::probe_target_for_version(config, schema, table, column, "17").await
}

pub fn source_type_mapping(
    native_type: &str,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_for_version("17", native_type)
}

pub fn source_type_mapping_with_catalog(
    native_type: &str,
    catalog: &SourceTypeCatalog,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_with_catalog_for_version("17", native_type, catalog)
}

pub fn source_type_mapping_with_source_evidence(
    native_type: &str,
    catalog: &SourceTypeCatalog,
    source_build: ServerBuildIdentity,
    environment_fingerprint: impl Into<String>,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_with_source_evidence_for_version(
        "17",
        native_type,
        catalog,
        source_build,
        environment_fingerprint,
    )
}

pub fn map_source_type(
    native_type: &str,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping(native_type)
}

pub fn validate_native_type(native_type: &str) -> std::result::Result<(), SourceTypeMappingError> {
    postgresql_15::validate_native_type_for_version("17", native_type)
}

pub fn validate_change_event(
    transaction: change_event::ChangeTransaction,
) -> Result<change_event::ValidatedTransaction> {
    postgresql_15::validate_change_event(transaction)
}

pub async fn replication(config: Config) -> Result<Replication> {
    postgresql_15::replication_for_version(config, 17).await
}

pub async fn metadata(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<postgresql_15::Metadata> {
    postgresql_15::metadata_for_version(host, port, database, username, password, 17).await
}

pub async fn databases(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Vec<String>> {
    postgresql_15::databases(host, port, username, password).await
}
