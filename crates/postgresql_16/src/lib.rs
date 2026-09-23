//! PostgreSQL 16 SourceTypeMapping.
//!
//! PostgreSQL 16 shares the public value semantics of the PostgreSQL 15
//! mapping, but its connector identity and evidence are version-specific.
//! Capture/runtime support remains owned by a versioned worker and is not
//! implied by this mapping-only crate.

use change_event::{ServerBuildIdentity, SourceTypeMapping};
pub use postgresql_15::MAPPING_VERSION_16 as MAPPING_VERSION;
pub use postgresql_15::{
    CancellationToken, Config, Replication, SourceExtension, SourceTypeCatalog,
    SourceTypeDefinition, SourceTypeDefinitionKind, SourceTypeField, SourceTypeMappingError,
};
pub type Result<T> = postgresql_15::Result<T>;

pub fn source_type_mapping(
    native_type: &str,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_for_version("16", native_type)
}

pub fn source_type_mapping_with_catalog(
    native_type: &str,
    catalog: &SourceTypeCatalog,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_with_catalog_for_version("16", native_type, catalog)
}

pub fn source_type_mapping_with_source_evidence(
    native_type: &str,
    catalog: &SourceTypeCatalog,
    source_build: ServerBuildIdentity,
    environment_fingerprint: impl Into<String>,
) -> std::result::Result<SourceTypeMapping, SourceTypeMappingError> {
    postgresql_15::source_type_mapping_with_source_evidence_for_version(
        "16",
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
    postgresql_15::validate_native_type_for_version("16", native_type)
}

pub fn validate_change_event(
    transaction: change_event::ChangeTransaction,
) -> Result<change_event::ValidatedTransaction> {
    postgresql_15::validate_change_event(transaction)
}

pub async fn replication(config: Config) -> Result<Replication> {
    postgresql_15::replication_for_version(config, 16).await
}

pub async fn metadata(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<postgresql_15::Metadata> {
    postgresql_15::metadata_for_version(host, port, database, username, password, 16).await
}

pub async fn databases(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Vec<String>> {
    postgresql_15::databases(host, port, username, password).await
}
