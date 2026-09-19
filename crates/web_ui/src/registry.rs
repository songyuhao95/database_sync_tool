//! Independent connector catalogs for the Web control plane.
//!
//! A connector is selected by its own identity and role.  The registries do not
//! contain source-to-sink pair entries; a new Source or Sink therefore only
//! changes the catalog for that role.
use crate::catalog::{CatalogColumn, CatalogTable};
use change_event::{
    CapabilityManifest, CompatibilityError, CompatibilityResult,
    ConnectorIdentity as ModelConnectorIdentity, DefinitionReference, FieldCompatibilityInput,
    FieldDefinition, LogicalType, Operation, PresenceState, RouteOptions, ServerBuildIdentity,
    SourceTypeMapping, TargetCapabilityManifest,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ConnectorIdentity {
    pub kind: &'static str,
    pub version: &'static str,
}

impl ConnectorIdentity {
    pub(crate) const fn new(kind: &'static str, version: &'static str) -> Self {
        Self { kind, version }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ConnectorCapabilities {
    pub supports_transactions: bool,
    pub supports_primary_key_rows: bool,
    pub supports_snapshot: bool,
    pub supports_gtid: bool,
    pub supports_file_position: bool,
    pub supported_logical_types: &'static [&'static str],
    pub supported_presence: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ConnectorDescriptor {
    pub identity: ConnectorIdentity,
    pub role: &'static str,
    pub capabilities: ConnectorCapabilities,
    #[serde(skip)]
    pub(crate) adapter: AdapterKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdapterKind {
    Mysql57,
    Mysql80,
    Mysql84,
    Postgresql15,
}

const MYSQL_LOGICAL_TYPES: &[&str] = &[
    "boolean",
    "uuid",
    "integer",
    "decimal",
    "float",
    "text",
    "binary",
    "bit_string",
    "date",
    "local_datetime",
    "instant",
    "duration",
    "year",
    "json",
    "enum",
    "set",
];
const POSTGRES_LOGICAL_TYPES: &[&str] = &[
    "boolean",
    "uuid",
    "integer",
    "decimal",
    "float",
    "text",
    "binary",
    "bit_string",
    "date",
    "local_datetime",
    "instant",
    "duration",
    "year",
    "json",
    "enum",
];
const PRESENCE: &[&str] = &["value", "null", "unchanged", "unavailable"];
const TRANSACTION_CAPABILITIES: ConnectorCapabilities = ConnectorCapabilities {
    supports_transactions: true,
    supports_primary_key_rows: true,
    supports_snapshot: true,
    supports_gtid: true,
    supports_file_position: true,
    supported_logical_types: MYSQL_LOGICAL_TYPES,
    supported_presence: PRESENCE,
};
const POSTGRES_SOURCE_CAPABILITIES: ConnectorCapabilities = ConnectorCapabilities {
    supports_transactions: true,
    supports_primary_key_rows: true,
    supports_snapshot: false,
    supports_gtid: false,
    supports_file_position: false,
    supported_logical_types: POSTGRES_LOGICAL_TYPES,
    supported_presence: PRESENCE,
};

const fn source(
    kind: &'static str,
    version: &'static str,
    adapter: AdapterKind,
    capabilities: ConnectorCapabilities,
) -> ConnectorDescriptor {
    ConnectorDescriptor {
        identity: ConnectorIdentity::new(kind, version),
        role: "source",
        capabilities,
        adapter,
    }
}

const fn sink(
    kind: &'static str,
    version: &'static str,
    adapter: AdapterKind,
    manifest: CapabilityManifest,
) -> ConnectorDescriptor {
    ConnectorDescriptor {
        identity: ConnectorIdentity::new(kind, version),
        role: "sink",
        capabilities: ConnectorCapabilities {
            supports_transactions: true,
            supports_primary_key_rows: manifest.requires_primary_key,
            supports_snapshot: true,
            supports_gtid: false,
            supports_file_position: true,
            supported_logical_types: manifest.supported_logical_types,
            supported_presence: manifest.supported_presence,
        },
        adapter,
    }
}

const SOURCES: &[ConnectorDescriptor] = &[
    source(
        "mysql",
        "5.7",
        AdapterKind::Mysql57,
        TRANSACTION_CAPABILITIES,
    ),
    source(
        "mysql",
        "8.0",
        AdapterKind::Mysql80,
        TRANSACTION_CAPABILITIES,
    ),
    source(
        "mysql",
        "8.4",
        AdapterKind::Mysql84,
        TRANSACTION_CAPABILITIES,
    ),
    source(
        "postgresql",
        "15",
        AdapterKind::Postgresql15,
        POSTGRES_SOURCE_CAPABILITIES,
    ),
];

const SINKS: &[ConnectorDescriptor] = &[
    sink(
        "mysql",
        "5.7",
        AdapterKind::Mysql57,
        mysql_5_7::CAPABILITY_MANIFEST,
    ),
    sink(
        "mysql",
        "8.0",
        AdapterKind::Mysql80,
        mysql_8_0::CAPABILITY_MANIFEST,
    ),
    sink(
        "mysql",
        "8.4",
        AdapterKind::Mysql84,
        mysql_8_4::CAPABILITY_MANIFEST,
    ),
    sink(
        "postgresql",
        "15",
        AdapterKind::Postgresql15,
        postgresql_15::CAPABILITY_MANIFEST,
    ),
];

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SourceRegistry;

impl SourceRegistry {
    pub(crate) fn all(&self) -> impl Iterator<Item = &'static ConnectorDescriptor> {
        SOURCES.iter()
    }

    pub(crate) fn find(&self, kind: &str, version: &str) -> Option<&'static ConnectorDescriptor> {
        SOURCES.iter().find(|connector| {
            connector.identity.kind == kind && connector.identity.version == version
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SinkRegistry;

impl SinkRegistry {
    pub(crate) fn all(&self) -> impl Iterator<Item = &'static ConnectorDescriptor> {
        SINKS.iter()
    }

    pub(crate) fn find(&self, kind: &str, version: &str) -> Option<&'static ConnectorDescriptor> {
        SINKS.iter().find(|connector| {
            connector.identity.kind == kind && connector.identity.version == version
        })
    }
}

#[derive(Serialize)]
pub(crate) struct ConnectorCatalog {
    pub sources: Vec<ConnectorDescriptor>,
    pub sinks: Vec<ConnectorDescriptor>,
}

pub(crate) fn catalog() -> ConnectorCatalog {
    ConnectorCatalog {
        sources: SourceRegistry.all().copied().collect(),
        sinks: SinkRegistry.all().copied().collect(),
    }
}

impl ConnectorDescriptor {
    pub(crate) fn structured_manifest(
        &self,
        target_build: ServerBuildIdentity,
    ) -> TargetCapabilityManifest {
        match self.adapter {
            AdapterKind::Mysql57 => mysql_5_7::compatibility_manifest(target_build),
            AdapterKind::Mysql80 => mysql_8_0::compatibility_manifest(target_build),
            AdapterKind::Mysql84 => mysql_8_4::compatibility_manifest(target_build),
            AdapterKind::Postgresql15 => postgresql_15::compatibility_manifest(target_build),
        }
    }

    pub(crate) fn source_type_mapping(
        &self,
        column: &CatalogColumn,
    ) -> Result<SourceTypeMapping, String> {
        match self.adapter {
            AdapterKind::Mysql57 => mysql_5_7::source_type_mapping(
                &column.column_type,
                charset_from_collation(column.collation.as_deref()),
                column.collation.as_deref(),
            )
            .map_err(|error| error.to_string()),
            AdapterKind::Postgresql15 => postgresql_15::source_type_mapping(&column.column_type)
                .map_err(|error| error.to_string()),
            AdapterKind::Mysql80 => mysql_8_0::source_type_mapping(
                &column.column_type,
                charset_from_collation(column.collation.as_deref()),
                column.collation.as_deref(),
            ),
            AdapterKind::Mysql84 => mysql_8_4::source_type_mapping(
                &column.column_type,
                charset_from_collation(column.collation.as_deref()),
                column.collation.as_deref(),
            ),
        }
    }
}

/// Build one public field-planning request from catalog metadata.  Both Web
/// preflight and any future compatibility preview use this function, so the
/// Web layer does not maintain native type-pair rules of its own.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) fn field_compatibility(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    source_column: &CatalogColumn,
    sink_column: &CatalogColumn,
    route_id: &str,
    configuration_revision: &str,
    source_build: Option<ServerBuildIdentity>,
    target_build: Option<ServerBuildIdentity>,
) -> Result<CompatibilityResult, CompatibilityError> {
    field_compatibility_with_options(
        source_connector,
        sink_connector,
        source,
        sink,
        source_column,
        sink_column,
        route_id,
        configuration_revision,
        source_build,
        target_build,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn field_compatibility_with_options(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    source_column: &CatalogColumn,
    sink_column: &CatalogColumn,
    route_id: &str,
    configuration_revision: &str,
    source_build: Option<ServerBuildIdentity>,
    target_build: Option<ServerBuildIdentity>,
    confirmations: &[change_event::RiskConfirmation],
) -> Result<CompatibilityResult, CompatibilityError> {
    field_compatibility_with_parameters(
        source_connector,
        sink_connector,
        source,
        sink,
        source_column,
        sink_column,
        route_id,
        configuration_revision,
        source_build,
        target_build,
        &BTreeMap::new(),
        confirmations,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn field_compatibility_with_parameters(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    source_column: &CatalogColumn,
    sink_column: &CatalogColumn,
    route_id: &str,
    configuration_revision: &str,
    source_build: Option<ServerBuildIdentity>,
    target_build: Option<ServerBuildIdentity>,
    parameters: &BTreeMap<String, String>,
    confirmations: &[change_event::RiskConfirmation],
) -> Result<CompatibilityResult, CompatibilityError> {
    let source_mapping = source_connector
        .source_type_mapping(source_column)
        .map_err(|message| {
            CompatibilityError::SourceContract(change_event::CompatibilityFailure {
                class: change_event::FailureClass::SourceContract,
                code: "source_contract.type_mapping_failed".into(),
                phase: change_event::FailurePhase::SourceContract,
                retry: change_event::RetryClassification::NotRetryable,
                message,
            })
        })?;
    let target_mapping = sink_connector.source_type_mapping(sink_column).ok();
    let source_key = source
        .primary_key
        .iter()
        .position(|name| name == &source_column.name);
    let target_key = sink
        .primary_key
        .iter()
        .position(|name| name == &sink_column.name);
    let source_field = field_definition(
        source,
        source_column,
        source_mapping.logical_type.clone(),
        source_key,
    );
    let target_field = field_definition(
        sink,
        sink_column,
        target_mapping
            .map(|mapping| mapping.logical_type)
            .unwrap_or_else(|| LogicalType::Opaque {
                source_type: sink_column.column_type.clone(),
                format: "catalog-native".into(),
            }),
        target_key,
    );
    let manifest_build = target_build.clone().unwrap_or_else(|| {
        ServerBuildIdentity::new(
            sink_connector.identity.kind,
            "catalog",
            sink_connector.identity.version,
            "catalog",
        )
    });
    let manifest = sink_connector.structured_manifest(manifest_build);
    change_event::plan_field_compatibility(FieldCompatibilityInput {
        source_field,
        target_field,
        source_type_mapping: source_mapping,
        source_connector: ModelConnectorIdentity::new(
            source_connector.identity.kind,
            source_connector.identity.version,
        ),
        sink_connector: ModelConnectorIdentity::new(
            sink_connector.identity.kind,
            sink_connector.identity.version,
        ),
        source_build,
        target_build: Some(manifest.target_build.clone()),
        manifest: &manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
            PresenceState::Unavailable,
        ],
        source_has_primary_key: !source.primary_key.is_empty(),
        options: RouteOptions {
            route_id: route_id.into(),
            configuration_revision: configuration_revision.into(),
            parameters: parameters.clone(),
            confirmations: confirmations.to_vec(),
            ..RouteOptions::default()
        },
    })
}

fn field_definition(
    table: &CatalogTable,
    column: &CatalogColumn,
    logical_type: LogicalType,
    primary_key_ordinal: Option<usize>,
) -> FieldDefinition {
    let fingerprint = fingerprint(table);
    FieldDefinition {
        reference: DefinitionReference::new(
            format!("catalog:{}.{}.{}", table.schema, table.name, column.name),
            fingerprint,
        ),
        ordinal: table
            .columns
            .iter()
            .position(|item| item.name == column.name)
            .unwrap_or_default(),
        name: column.name.clone(),
        native_type: column.column_type.clone(),
        logical_type,
        nullable: column.nullable,
        collation: column.collation.clone(),
        generated: column.extra.to_ascii_lowercase().contains("generated"),
        primary_key_ordinal,
        unique: primary_key_ordinal.is_some(),
        row_locator: primary_key_ordinal.is_some(),
    }
}

fn fingerprint(table: &CatalogTable) -> String {
    let bytes = serde_json::to_vec(table).expect("catalog metadata is serializable");
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn catalog_fingerprint(table: &CatalogTable) -> String {
    fingerprint(table)
}

fn charset_from_collation(collation: Option<&str>) -> Option<&str> {
    collation.and_then(|value| value.split('_').next())
}
