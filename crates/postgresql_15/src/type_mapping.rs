//! PostgreSQL 15's strict Source Type Mapping.
//!
//! PostgreSQL's formatted catalog type is used only as source-definition
//! evidence. Values are never inspected to choose a mapping. Qualified
//! semantic codecs produce LogicalValues; other safely framed pgoutput text
//! values are captured as source-representation envelopes and remain distinct
//! from semantic values until a user selects a qualified representation.

use change_event::{
    ConnectorIdentity, DuplicateKeyPolicy, JsonProfile, LengthUnit, LogicalField, LogicalType,
    ServerBuildIdentity, SourceRepresentationFormat, SourceRepresentationTypeEvidence,
    SourceTypeMapping,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

pub const MAPPING_VERSION: &str = "postgresql-15.source-type-mapping.v3";
pub const MAPPING_VERSION_16: &str = "postgresql-16.source-type-mapping.v3";
pub const MAPPING_VERSION_17: &str = "postgresql-17.source-type-mapping.v3";

/// The source-owned catalog evidence needed to map a user-defined PostgreSQL
/// type.  The mapping layer intentionally receives this as an immutable
/// snapshot: it never looks at row values to discover a type.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceTypeCatalog {
    pub types: Vec<SourceTypeDefinition>,
    #[serde(default)]
    pub extensions: Vec<SourceExtension>,
}

impl SourceTypeCatalog {
    pub fn new(types: impl IntoIterator<Item = SourceTypeDefinition>) -> Self {
        Self {
            types: types.into_iter().collect(),
            extensions: Vec::new(),
        }
    }

    pub fn with_extensions(
        types: impl IntoIterator<Item = SourceTypeDefinition>,
        extensions: impl IntoIterator<Item = SourceExtension>,
    ) -> Self {
        Self {
            types: types.into_iter().collect(),
            extensions: extensions.into_iter().collect(),
        }
    }

    pub fn evidence_digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("PostgreSQL type catalog is serializable");
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub(crate) fn definition_closure(
        &self,
        root_oid: u32,
    ) -> Result<SourceTypeDefinitionClosure, String> {
        use std::collections::HashSet;

        fn visit(
            catalog: &SourceTypeCatalog,
            oid: u32,
            seen: &mut HashSet<u32>,
            types: &mut Vec<SourceTypeDefinition>,
        ) -> Result<(), String> {
            if !seen.insert(oid) {
                return Ok(());
            }
            let definition = catalog
                .types
                .iter()
                .find(|definition| definition.oid == oid)
                .ok_or_else(|| format!("PostgreSQL type dependency OID {oid} is missing"))?;
            types.push(definition.clone());
            let dependencies = match &definition.kind {
                SourceTypeDefinitionKind::Domain { base_oid, .. } => vec![*base_oid],
                SourceTypeDefinitionKind::Composite { fields } => {
                    fields.iter().map(|field| field.type_oid).collect()
                }
                SourceTypeDefinitionKind::Array { element_oid, .. } => vec![*element_oid],
                SourceTypeDefinitionKind::Range { subtype_oid } => vec![*subtype_oid],
                SourceTypeDefinitionKind::MultiRange { range_oid } => vec![*range_oid],
                _ => Vec::new(),
            };
            for dependency in dependencies {
                visit(catalog, dependency, seen, types)?;
            }
            Ok(())
        }

        let mut types = Vec::new();
        visit(self, root_oid, &mut HashSet::new(), &mut types)?;
        types.sort_by_key(|definition| definition.oid);
        let mut extension_names = types
            .iter()
            .filter_map(|definition| match &definition.kind {
                SourceTypeDefinitionKind::Extension { extension, .. } => Some(extension),
                _ => None,
            })
            .collect::<Vec<_>>();
        extension_names.sort();
        extension_names.dedup();
        let extensions = extension_names
            .into_iter()
            .map(|name| {
                self.extensions
                    .iter()
                    .find(|extension| extension.name.eq_ignore_ascii_case(name))
                    .cloned()
                    .ok_or_else(|| format!("PostgreSQL extension evidence for {name} is missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SourceTypeDefinitionClosure { types, extensions })
    }

    fn digest(&self) -> String {
        self.evidence_digest()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SourceTypeDefinitionClosure {
    pub types: Vec<SourceTypeDefinition>,
    pub extensions: Vec<SourceExtension>,
}

impl SourceTypeDefinitionClosure {
    pub(crate) fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("type closure is serializable");
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

pub(crate) fn validate_type_definition_closure(
    closure: &SourceTypeDefinitionClosure,
    root_oid: u32,
) -> bool {
    let definitions = closure
        .types
        .iter()
        .map(|definition| (definition.oid, definition))
        .collect::<std::collections::HashMap<_, _>>();
    if definitions.len() != closure.types.len()
        || !definitions.contains_key(&root_oid)
        || closure
            .types
            .windows(2)
            .any(|pair| pair[0].oid >= pair[1].oid)
    {
        return false;
    }
    if closure
        .types
        .iter()
        .any(|definition| definition.definition_digest != expected_definition_digest(definition))
    {
        return false;
    }
    let mut required_extensions = closure
        .types
        .iter()
        .filter_map(|definition| match &definition.kind {
            SourceTypeDefinitionKind::Extension { extension, .. } => Some(extension.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    required_extensions.sort_by_key(|name| name.to_ascii_lowercase());
    required_extensions.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    if required_extensions.len() != closure.extensions.len()
        || closure.extensions.iter().any(|extension| {
            !extension.installed
                || extension.version.trim().is_empty()
                || extension.schema.trim().is_empty()
        })
        || closure
            .extensions
            .windows(2)
            .any(|pair| pair[0].name.to_ascii_lowercase() >= pair[1].name.to_ascii_lowercase())
        || required_extensions
            .iter()
            .zip(&closure.extensions)
            .any(|(required, actual)| !required.eq_ignore_ascii_case(&actual.name))
    {
        return false;
    }

    let mut reachable = std::collections::HashSet::new();
    let mut pending = vec![root_oid];
    while let Some(oid) = pending.pop() {
        if !reachable.insert(oid) {
            continue;
        }
        let Some(definition) = definitions.get(&oid) else {
            return false;
        };
        pending.extend(type_definition_dependencies(definition));
    }
    reachable.len() == definitions.len()
}

fn type_definition_dependencies(definition: &SourceTypeDefinition) -> Vec<u32> {
    match &definition.kind {
        SourceTypeDefinitionKind::Domain { base_oid, .. } => vec![*base_oid],
        SourceTypeDefinitionKind::Composite { fields } => {
            fields.iter().map(|field| field.type_oid).collect()
        }
        SourceTypeDefinitionKind::Array { element_oid, .. } => vec![*element_oid],
        SourceTypeDefinitionKind::Range { subtype_oid } => vec![*subtype_oid],
        SourceTypeDefinitionKind::MultiRange { range_oid } => vec![*range_oid],
        _ => Vec::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceExtension {
    pub name: String,
    pub version: String,
    pub schema: String,
    pub installed: bool,
    /// Whether the server advertises the extension in pg_available_extensions.
    #[serde(default = "default_available")]
    pub available: bool,
    /// Optional target-side qualification result. `Some(false)` is an
    /// explicit, explainable target block; `None` means target probing has not
    /// been performed yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_compatible: Option<bool>,
}

fn default_available() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceTypeDefinition {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub kind: SourceTypeDefinitionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(default)]
    pub definition_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SourceTypeDefinitionKind {
    /// PostgreSQL protocol/catalog helper type that cannot be stored in a
    /// table column. Kept only so composite catalog definitions can preserve
    /// complete dependency evidence.
    Pseudo,
    Builtin {
        native_type: String,
    },
    Enum {
        labels: Vec<String>,
    },
    Domain {
        base_oid: u32,
        constraints: Vec<String>,
        not_null: bool,
    },
    Composite {
        fields: Vec<SourceTypeField>,
    },
    Array {
        element_oid: u32,
        #[serde(default = "default_array_delimiter")]
        delimiter: char,
    },
    Range {
        subtype_oid: u32,
    },
    MultiRange {
        range_oid: u32,
    },
    Extension {
        extension: String,
        codec_identity: Option<String>,
        encoding: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        logical_type: Option<LogicalType>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceTypeField {
    pub name: String,
    pub type_oid: u32,
    pub nullable: bool,
}

impl SourceTypeDefinition {
    pub fn pseudo(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        collation: Option<String>,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Pseudo,
            collation,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn builtin(oid: u32, schema: impl Into<String>, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            oid,
            schema: schema.into(),
            kind: SourceTypeDefinitionKind::Builtin {
                native_type: name.clone(),
            },
            name,
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn enum_type(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        labels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Enum {
                labels: labels.into_iter().map(Into::into).collect(),
            },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn domain(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        base_oid: u32,
        constraints: impl IntoIterator<Item = impl Into<String>>,
        not_null: bool,
        collation: Option<String>,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Domain {
                base_oid,
                constraints: constraints.into_iter().map(Into::into).collect(),
                not_null,
            },
            collation,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn composite(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        fields: impl IntoIterator<Item = SourceTypeField>,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Composite {
                fields: fields.into_iter().collect(),
            },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn array(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        element_oid: u32,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Array {
                element_oid,
                delimiter: ',',
            },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn array_with_delimiter(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        element_oid: u32,
        delimiter: char,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Array {
                element_oid,
                delimiter,
            },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn range(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        subtype_oid: u32,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Range { subtype_oid },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn multi_range(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        range_oid: u32,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::MultiRange { range_oid },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    pub fn extension(
        oid: u32,
        schema: impl Into<String>,
        name: impl Into<String>,
        extension: impl Into<String>,
        codec_identity: impl Into<String>,
        encoding: impl Into<String>,
        logical_type: Option<LogicalType>,
    ) -> Self {
        Self {
            oid,
            schema: schema.into(),
            name: name.into(),
            kind: SourceTypeDefinitionKind::Extension {
                extension: extension.into(),
                codec_identity: Some(codec_identity.into()),
                encoding: encoding.into(),
                logical_type,
            },
            collation: None,
            definition_digest: String::new(),
        }
        .finalized()
    }

    fn finalized(mut self) -> Self {
        self.definition_digest = expected_definition_digest(&self);
        self
    }
}

fn default_array_delimiter() -> char {
    ','
}

fn expected_definition_digest(definition: &SourceTypeDefinition) -> String {
    let bytes = serde_json::to_vec(&(
        definition.oid,
        &definition.schema,
        &definition.name,
        &definition.kind,
        &definition.collation,
    ))
    .expect("PostgreSQL type definition is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl SourceTypeField {
    pub fn new(name: impl Into<String>, type_oid: u32, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_oid,
            nullable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTypeMappingError {
    code: String,
    message: String,
}

impl SourceTypeMappingError {
    fn invalid(version: &str, message: impl Into<String>) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.invalid_declaration"),
            message: message.into(),
        }
    }

    fn unsupported(version: &str, native_type: &str) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.unsupported"),
            message: format!(
                "PostgreSQL {version} native type {native_type:?} has no lossless LogicalType mapping"
            ),
        }
    }

    fn missing_catalog(version: &str, native_type: &str) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.catalog_required"),
            message: format!(
                "PostgreSQL {version} type {native_type:?} requires immutable catalog evidence"
            ),
        }
    }

    fn missing_codec(version: &str, native_type: &str) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.codec_unqualified"),
            message: format!(
                "PostgreSQL {version} type {native_type:?} has no verified source codec"
            ),
        }
    }

    fn extension_target_blocked(version: &str, native_type: &str, extension: &str) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.extension_target_blocked"),
            message: format!(
                "PostgreSQL {version} type {native_type:?} requires extension {extension:?}, which is not qualified on the target"
            ),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Display for SourceTypeMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl Error for SourceTypeMappingError {}

/// Map a PostgreSQL 15 declaration without consulting row values.  This
/// remains the compatibility entry point used by the existing connector.
pub fn source_type_mapping(native_type: &str) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping_for_version("15", native_type)
}

/// Map a PostgreSQL 15/16/17 declaration using the version-owned mapping
/// rules.  The three releases share value semantics, but their evidence is
/// deliberately versioned so a new server release cannot reinterpret an old
/// event silently.
pub fn source_type_mapping_for_version(
    version: &str,
    native_type: &str,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping_with_catalog_for_version(
        version,
        native_type,
        &SourceTypeCatalog::default(),
    )
}

pub fn source_type_mapping_with_catalog(
    native_type: &str,
    catalog: &SourceTypeCatalog,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping_with_catalog_for_version("15", native_type, catalog)
}

pub fn source_type_mapping_with_source_evidence(
    native_type: &str,
    catalog: &SourceTypeCatalog,
    source_build: ServerBuildIdentity,
    environment_fingerprint: impl Into<String>,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping_with_source_evidence_for_version(
        "15",
        native_type,
        catalog,
        source_build,
        environment_fingerprint,
    )
}

pub fn source_type_mapping_with_catalog_for_version(
    version: &str,
    native_type: &str,
    catalog: &SourceTypeCatalog,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    ensure_supported_version(version)?;
    let source_native_type = native_type.trim().to_owned();
    let native_type = normalize(native_type, version)?;
    let logical_type = logical_type_with_catalog(&native_type, catalog, version)?;
    logical_type
        .validate()
        .map_err(|error| SourceTypeMappingError::invalid(version, error.to_string()))?;
    let source_definition_fingerprint = match &logical_type {
        LogicalType::Raw {
            source_definition_digest,
            ..
        } => Some(source_definition_digest.clone()),
        _ if array_declaration(&native_type).is_some()
            || base_name(&native_type).eq_ignore_ascii_case("enum")
            || logical_type_from_builtin(&native_type, version)?.is_some() =>
        {
            None
        }
        _ => Some(definition_digest(
            find_definition(&native_type, catalog, version)?,
            version,
        )?),
    };
    let base = base_name(&native_type).to_ascii_lowercase();
    let mapping_version = mapping_version(version);
    let mapping_id = format!("postgresql{version}.source-type.{base}");
    let mut value_representation = BTreeMap::new();
    if let LogicalType::BitString { length } = &logical_type {
        value_representation.insert("bit_order".into(), "msb_first".into());
        value_representation.insert(
            "bit_padding".into(),
            if length.is_multiple_of(8) {
                "none"
            } else {
                "zero"
            }
            .into(),
        );
    } else if matches!(logical_type, LogicalType::VariableBitString { .. }) {
        value_representation.insert("bit_order".into(), "msb_first".into());
        value_representation.insert("bit_padding".into(), "variable".into());
    }
    let evidence_digest = evidence_digest(
        &mapping_version,
        &native_type,
        &logical_type,
        &catalog.digest(),
        &value_representation,
    );
    let source_representation_evidence =
        source_representation_evidence(&source_native_type, &logical_type, catalog, version)?;
    Ok(SourceTypeMapping {
        connector: ConnectorIdentity::new("postgresql", version),
        native_type,
        logical_type,
        mapping_id,
        mapping_version,
        evidence_digest: Some(evidence_digest),
        source_definition_fingerprint,
        source_build: None,
        environment_fingerprint: None,
        source_representation_evidence,
        value_representation,
    })
}

fn source_representation_evidence(
    source_native_type: &str,
    logical_type: &LogicalType,
    catalog: &SourceTypeCatalog,
    version: &str,
) -> Result<Option<SourceRepresentationTypeEvidence>, SourceTypeMappingError> {
    let LogicalType::Raw {
        native_type,
        source_definition_digest,
        ..
    } = logical_type
    else {
        return Ok(None);
    };
    let definition = find_definition(native_type, catalog, version)?;
    let expected_digest = definition_digest(definition, version)?;
    if source_definition_digest
        .strip_prefix("sha256:")
        .unwrap_or(source_definition_digest)
        != expected_digest
    {
        return Err(SourceTypeMappingError::invalid(
            version,
            "opaque mapping and catalog type definition have different digests",
        ));
    }
    let closure = catalog
        .definition_closure(definition.oid)
        .map_err(|message| SourceTypeMappingError::invalid(version, message))?;
    let closure_json = serde_json::to_string(&closure)
        .expect("PostgreSQL source type definition closure must serialize");
    let root_json = serde_json::to_string(definition)
        .expect("PostgreSQL source type definition must serialize");
    let catalog_digest = catalog.evidence_digest();
    let type_metadata = BTreeMap::from([
        ("column_oid".into(), definition.oid.to_string()),
        ("native_type".into(), source_native_type.to_owned()),
        ("type_schema".into(), definition.schema.clone()),
        ("type_name".into(), definition.name.clone()),
        ("type_definition".into(), root_json),
        (
            "type_definition_digest".into(),
            format!("sha256:{}", definition.definition_digest),
        ),
        ("type_definition_closure".into(), closure_json),
        (
            "type_definition_closure_digest".into(),
            format!("sha256:{}", closure.digest()),
        ),
        ("source_catalog_digest".into(), catalog_digest),
        ("text_output_profile".into(), "pgoutput-v1/text/UTF8".into()),
        ("session.datestyle".into(), "ISO, YMD".into()),
        ("session.intervalstyle".into(), "iso_8601".into()),
        ("session.timezone".into(), "UTC".into()),
        ("session.bytea_output".into(), "hex".into()),
        ("session.extra_float_digits".into(), "3".into()),
        ("session.search_path".into(), "pg_catalog".into()),
    ]);
    Ok(Some(SourceRepresentationTypeEvidence {
        source_type_identity: format!(
            "postgresql.pg_type.v1:{}:{}.{}",
            definition.oid, definition.schema, definition.name
        ),
        protocol: "pgoutput.v1".into(),
        format: SourceRepresentationFormat::Text,
        type_metadata,
        allowed_context_metadata_keys: BTreeSet::from([
            "relation_oid".into(),
            "relation_schema".into(),
            "relation_name".into(),
            "column_name".into(),
            "type_modifier".into(),
            "environment.lc_monetary".into(),
        ]),
    }))
}

/// Bind a mapping to the exact server and semantic session that qualified it.
/// The plain mapping functions remain useful for static planning; live source
/// activation should use this form after collecting [`crate::Metadata`].
pub fn source_type_mapping_with_source_evidence_for_version(
    version: &str,
    native_type: &str,
    catalog: &SourceTypeCatalog,
    source_build: ServerBuildIdentity,
    environment_fingerprint: impl Into<String>,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    let mapping = source_type_mapping_with_catalog_for_version(version, native_type, catalog)?;
    let definition_fingerprint = mapping
        .source_definition_fingerprint
        .clone()
        .unwrap_or_else(|| catalog.digest());
    Ok(mapping.with_source_evidence(
        definition_fingerprint,
        source_build,
        environment_fingerprint,
    ))
}

pub fn map_source_type(native_type: &str) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping(native_type)
}

pub fn validate_native_type(native_type: &str) -> Result<(), SourceTypeMappingError> {
    validate_native_type_for_version("15", native_type)
}

pub fn validate_native_type_for_version(
    version: &str,
    native_type: &str,
) -> Result<(), SourceTypeMappingError> {
    ensure_supported_version(version)?;
    let native_type = normalize(native_type, version)?;
    logical_type_with_catalog(&native_type, &SourceTypeCatalog::default(), version).map(|_| ())
}

fn ensure_supported_version(version: &str) -> Result<(), SourceTypeMappingError> {
    if matches!(version, "15" | "16" | "17") {
        Ok(())
    } else {
        Err(SourceTypeMappingError::invalid(
            version,
            "PostgreSQL SourceTypeMapping supports only versions 15, 16, and 17",
        ))
    }
}

fn mapping_version(version: &str) -> String {
    match version {
        "15" => MAPPING_VERSION,
        "16" => MAPPING_VERSION_16,
        "17" => MAPPING_VERSION_17,
        _ => unreachable!("version is checked before mapping_version"),
    }
    .to_owned()
}

fn normalize(native_type: &str, version: &str) -> Result<String, SourceTypeMappingError> {
    let raw = native_type.trim();
    let lower = raw.to_ascii_lowercase();
    if raw.is_empty() || raw.contains(';') || raw.contains('\0') {
        return Err(SourceTypeMappingError::invalid(
            version,
            "native type declaration is empty or contains forbidden syntax",
        ));
    }
    if lower.starts_with("enum(") {
        return Ok(raw.to_owned());
    }
    Ok(lower
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("[ ]", "[]")
        .replace(" []", "[]"))
}

fn base_name(native_type: &str) -> &str {
    native_type
        .split_once(['(', ' '])
        .map_or(native_type, |(base, _)| base)
}

fn logical_type_with_catalog(
    native_type: &str,
    catalog: &SourceTypeCatalog,
    version: &str,
) -> Result<LogicalType, SourceTypeMappingError> {
    if let Some((element, dimensions)) = array_declaration(native_type) {
        if catalog.types.is_empty() {
            return Err(SourceTypeMappingError::missing_catalog(
                version,
                native_type,
            ));
        }
        let mut logical = logical_type_with_catalog(element, catalog, version)?;
        if matches!(logical, LogicalType::Raw { .. }) {
            let definition = array_definition_for_element(element, catalog, version)?
                .ok_or_else(|| SourceTypeMappingError::missing_catalog(version, native_type))?;
            return Ok(LogicalType::raw(
                "postgresql.pgoutput.text-envelope.v1",
                format!("{}.{}", definition.schema, definition.name),
                definition_digest(definition, version)?,
                "UTF-8",
            ));
        }
        for _ in 0..dimensions {
            logical = LogicalType::Array {
                element: Box::new(logical),
            };
        }
        return Ok(logical);
    }
    if base_name(native_type).eq_ignore_ascii_case("enum") {
        return Ok(LogicalType::Enum {
            members: enum_members(native_type, version)?,
        });
    }
    // Parse the declared SQL shape before resolving its catalog alias. The
    // catalog name (for example bpchar/varbit/float4) can discard a column's
    // typmod or SQL alias semantics if it is resolved first.
    if let Some(logical) = logical_type_from_builtin(native_type, version)? {
        return Ok(logical);
    }
    if let Some(canonical_builtin) = builtin_catalog_name(native_type) {
        if let Some(logical) = logical_type_from_builtin(canonical_builtin, version)? {
            return Ok(logical);
        }
        if let Some(definition) = catalog.types.iter().find(|definition| {
            definition.schema == "pg_catalog" && definition.name == canonical_builtin
        }) {
            return logical_type_from_definition(definition, catalog, version, &mut Vec::new());
        }
    }
    let definition = find_definition(native_type, catalog, version)?;
    match &definition.kind {
        SourceTypeDefinitionKind::Pseudo => {
            return Err(SourceTypeMappingError::invalid(
                version,
                format!(
                    "PostgreSQL pseudo-type {}.{} is not storable",
                    definition.schema, definition.name
                ),
            ));
        }
        SourceTypeDefinitionKind::Array { element_oid, .. }
            if catalog.types.iter().any(|element| {
                element.oid == *element_oid
                    && matches!(element.kind, SourceTypeDefinitionKind::Pseudo)
            }) =>
        {
            return Err(SourceTypeMappingError::invalid(
                version,
                format!(
                    "PostgreSQL array {}.{} has a pseudo-type element and is not storable",
                    definition.schema, definition.name
                ),
            ));
        }
        _ => {}
    }
    if !has_semantic_codec(catalog, definition.oid)
        && !matches!(definition.kind, SourceTypeDefinitionKind::Extension { .. })
    {
        return Ok(LogicalType::raw(
            "postgresql.pgoutput.text-envelope.v1",
            format!("{}.{}", definition.schema, definition.name),
            definition_digest(definition, version)?,
            "UTF-8",
        ));
    }
    let mut stack = Vec::new();
    logical_type_from_definition(definition, catalog, version, &mut stack)
}

fn array_definition_for_element<'a>(
    element_native_type: &str,
    catalog: &'a SourceTypeCatalog,
    version: &str,
) -> Result<Option<&'a SourceTypeDefinition>, SourceTypeMappingError> {
    let element_definition = find_definition(element_native_type, catalog, version)
        .ok()
        .or_else(|| {
            let name = builtin_catalog_name(element_native_type)?;
            catalog
                .types
                .iter()
                .find(|definition| definition.schema == "pg_catalog" && definition.name == name)
        });
    let Some(element_definition) = element_definition else {
        return Ok(None);
    };
    Ok(catalog.types.iter().find(|definition| {
        matches!(
            definition.kind,
            SourceTypeDefinitionKind::Array { element_oid: known, .. } if known == element_definition.oid
        )
    }))
}

fn builtin_catalog_name(native_type: &str) -> Option<&'static str> {
    Some(match native_type.trim().to_ascii_lowercase().as_str() {
        "boolean" | "bool" => "bool",
        "smallint" | "int2" => "int2",
        "integer" | "int" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "real" | "float4" => "float4",
        "double precision" | "float8" => "float8",
        "numeric" | "decimal" => "numeric",
        "text" => "text",
        "character varying" | "varchar" => "varchar",
        "character" | "char" | "bpchar" => "bpchar",
        "\"char\"" => "char",
        "name" => "name",
        "bytea" => "bytea",
        "bit" | "\"bit\"" => "bit",
        "bit varying" | "varbit" => "varbit",
        "date" => "date",
        "time" | "time without time zone" => "time",
        "time with time zone" | "timetz" => "timetz",
        "timestamp" | "timestamp without time zone" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "interval" => "interval",
        "uuid" => "uuid",
        "json" => "json",
        "jsonb" => "jsonb",
        "xml" => "xml",
        "money" => "money",
        "point" => "point",
        "line" => "line",
        "lseg" => "lseg",
        "box" => "box",
        "path" => "path",
        "polygon" => "polygon",
        "circle" => "circle",
        "cidr" => "cidr",
        "inet" => "inet",
        "macaddr" => "macaddr",
        "macaddr8" => "macaddr8",
        "tsvector" => "tsvector",
        "tsquery" => "tsquery",
        "oid" => "oid",
        "oidvector" => "oidvector",
        "int2vector" => "int2vector",
        "tid" => "tid",
        "xid" => "xid",
        "xid8" => "xid8",
        "cid" => "cid",
        "pg_lsn" => "pg_lsn",
        "pg_snapshot" => "pg_snapshot",
        "txid_snapshot" => "txid_snapshot",
        "regproc" => "regproc",
        "regprocedure" => "regprocedure",
        "regoper" => "regoper",
        "regoperator" => "regoperator",
        "regclass" => "regclass",
        "regtype" => "regtype",
        "regconfig" => "regconfig",
        "regdictionary" => "regdictionary",
        "regnamespace" => "regnamespace",
        "regrole" => "regrole",
        "regcollation" => "regcollation",
        "int4range" => "int4range",
        "int8range" => "int8range",
        "numrange" => "numrange",
        "tsrange" => "tsrange",
        "tstzrange" => "tstzrange",
        "daterange" => "daterange",
        "int4multirange" => "int4multirange",
        "int8multirange" => "int8multirange",
        "nummultirange" => "nummultirange",
        "tsmultirange" => "tsmultirange",
        "tstzmultirange" => "tstzmultirange",
        "datemultirange" => "datemultirange",
        _ => return None,
    })
}

fn logical_type_from_builtin(
    native_type: &str,
    version: &str,
) -> Result<Option<LogicalType>, SourceTypeMappingError> {
    let logical = match native_type {
        "boolean" | "bool" => Ok(LogicalType::boolean()),
        "smallint" => Ok(LogicalType::integer(true, 16)),
        "integer" | "int" => Ok(LogicalType::integer(true, 32)),
        "bigint" => Ok(LogicalType::integer(true, 64)),
        "oid" | "xid" | "cid" => Ok(LogicalType::integer(false, 32)),
        "xid8" => Ok(LogicalType::integer(false, 64)),
        "real" | "float4" => Ok(LogicalType::float(32)),
        "double precision" | "float8" => Ok(LogicalType::float(64)),
        "text" | "character" | "char" | "\"char\"" | "name" | "character varying" | "varchar" => {
            Ok(LogicalType::Text {
                charset: "UTF8".into(),
                max_length: None,
                length_unit: LengthUnit::Characters,
                collation: None,
            })
        }
        "bytea" => Ok(LogicalType::binary(None)),
        "bit" => Ok(LogicalType::bit_string(1)),
        "date" => Ok(LogicalType::date()),
        "time without time zone" | "time" | "time with time zone" | "timetz" => {
            time(native_type, version)
        }
        "uuid" => Ok(LogicalType::uuid()),
        "jsonb" => Ok(LogicalType::Json {
            profile: JsonProfile::default(),
        }),
        "json" => Ok(LogicalType::Json {
            profile: JsonProfile {
                normalized: false,
                duplicate_keys: DuplicateKeyPolicy::PreserveLast,
            },
        }),
        "inet" => Ok(LogicalType::network("ip", false)),
        "cidr" => Ok(LogicalType::network("ip", true)),
        "macaddr" | "macaddr8" => Ok(LogicalType::network(native_type, false)),
        "xml" => Ok(LogicalType::xml()),
        "interval" => interval(native_type, version),
        "numeric" | "decimal" => numeric(native_type, version),
        "timestamp" | "timestamp without time zone" => Ok(LogicalType::LocalDatetime {
            fractional_precision: 6,
        }),
        "timestamptz" | "timestamp with time zone" => Ok(LogicalType::Instant {
            fractional_precision: 6,
        }),
        // `money` formatting depends on lc_monetary. Until a semantic money
        // codec is qualified, the catalog-backed mapping uses the explicit
        // pgoutput text representation path below.
        "money" => return Ok(None),
        _ if native_type.starts_with("time(") => time(native_type, version),
        _ if native_type.starts_with("interval") => interval(native_type, version),
        _ if native_type.starts_with("numeric(") => numeric(native_type, version),
        _ if native_type.starts_with("decimal(") => numeric(native_type, version),
        _ if native_type.starts_with("bit(") => {
            let length = parenthesized_u64(native_type, "bit", version)?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "bit length must be positive and bounded",
                ));
            }
            Ok(LogicalType::bit_string(length))
        }
        _ if native_type.starts_with("bit varying(") => {
            let length = parenthesized_u64(native_type, "bit varying", version)?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "bit varying length must be positive and bounded",
                ));
            }
            Ok(LogicalType::variable_bit_string(Some(length)))
        }
        _ if native_type.starts_with("varbit(") => {
            let length = parenthesized_u64(native_type, "varbit", version)?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "varbit length must be positive and bounded",
                ));
            }
            Ok(LogicalType::variable_bit_string(Some(length)))
        }
        _ if native_type.starts_with("geometry(") => spatial(native_type, version),
        _ if native_type.starts_with("character varying(")
            || native_type.starts_with("varchar(") =>
        {
            let prefix = if native_type.starts_with("varchar(") {
                "varchar"
            } else {
                "character varying"
            };
            let length = parenthesized_u64(native_type, prefix, version)?;
            if length == 0 {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "character varying length must be positive",
                ));
            }
            Ok(LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(length),
                length_unit: LengthUnit::Characters,
                collation: None,
            })
        }
        _ if native_type.starts_with("character(") || native_type.starts_with("char(") => {
            let prefix = if native_type.starts_with("char(") {
                "char"
            } else {
                "character"
            };
            let length = parenthesized_u64(native_type, prefix, version)?;
            if length == 0 {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "character length must be positive",
                ));
            }
            Ok(LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(length),
                length_unit: LengthUnit::Characters,
                collation: None,
            })
        }
        _ if native_type.starts_with("time(") => time(native_type, version),
        _ if native_type.starts_with("timestamp") => timestamp(native_type, version),
        "int2" => Ok(LogicalType::integer(true, 16)),
        "int4" => Ok(LogicalType::integer(true, 32)),
        "int8" => Ok(LogicalType::integer(true, 64)),
        _ => return Ok(None),
    }?;
    Ok(Some(logical))
}

fn array_declaration(native_type: &str) -> Option<(&str, u8)> {
    let mut base = native_type;
    let mut dimensions = 0_u8;
    while let Some(stripped) = base.strip_suffix("[]") {
        base = stripped;
        dimensions = dimensions.checked_add(1)?;
    }
    (dimensions > 0).then_some((base, dimensions))
}

fn find_definition<'a>(
    native_type: &str,
    catalog: &'a SourceTypeCatalog,
    version: &str,
) -> Result<&'a SourceTypeDefinition, SourceTypeMappingError> {
    let (schema, name) = native_type
        .split_once('.')
        .map_or((None, native_type), |(schema, name)| (Some(schema), name));
    let matches = catalog.types.iter().filter(|definition| {
        definition.name.eq_ignore_ascii_case(name)
            && schema.is_none_or(|schema| definition.schema.eq_ignore_ascii_case(schema))
    });
    let mut matches = matches.peekable();
    let Some(definition) = matches.next() else {
        return Err(SourceTypeMappingError::missing_catalog(
            version,
            native_type,
        ));
    };
    if matches.peek().is_some() && schema.is_none() {
        return Err(SourceTypeMappingError::invalid(
            version,
            format!("PostgreSQL type {native_type:?} is ambiguous; use a qualified name"),
        ));
    }
    Ok(definition)
}

fn definition_digest(
    definition: &SourceTypeDefinition,
    version: &str,
) -> Result<String, SourceTypeMappingError> {
    if definition.definition_digest.trim().is_empty() {
        return Err(SourceTypeMappingError::invalid(
            version,
            format!(
                "PostgreSQL type {}.{} is missing a stable definition digest",
                definition.schema, definition.name
            ),
        ));
    }
    let expected = expected_definition_digest(definition);
    if definition.definition_digest != expected {
        return Err(SourceTypeMappingError::invalid(
            version,
            format!(
                "PostgreSQL type {}.{} has a mismatched definition digest",
                definition.schema, definition.name
            ),
        ));
    }
    Ok(expected)
}

fn definition_by_oid<'a>(
    oid: u32,
    catalog: &'a SourceTypeCatalog,
    version: &str,
) -> Result<&'a SourceTypeDefinition, SourceTypeMappingError> {
    catalog
        .types
        .iter()
        .find(|definition| definition.oid == oid)
        .ok_or_else(|| {
            SourceTypeMappingError::invalid(
                version,
                format!("PostgreSQL type catalog is missing OID {oid}"),
            )
        })
}

pub(crate) fn has_semantic_codec(catalog: &SourceTypeCatalog, oid: u32) -> bool {
    fn visit(catalog: &SourceTypeCatalog, oid: u32, stack: &mut Vec<u32>) -> bool {
        if stack.contains(&oid) {
            return false;
        }
        let Some(definition) = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
        else {
            return false;
        };
        stack.push(oid);
        let result = match &definition.kind {
            SourceTypeDefinitionKind::Pseudo => false,
            SourceTypeDefinitionKind::Builtin { .. } => crate::types::supported(oid),
            SourceTypeDefinitionKind::Enum { .. } => true,
            SourceTypeDefinitionKind::Domain { base_oid, .. } => visit(catalog, *base_oid, stack),
            SourceTypeDefinitionKind::Composite { fields } => fields
                .iter()
                .all(|field| visit(catalog, field.type_oid, stack)),
            SourceTypeDefinitionKind::Array { element_oid, .. } => {
                visit(catalog, *element_oid, stack)
            }
            SourceTypeDefinitionKind::Range { subtype_oid } => visit(catalog, *subtype_oid, stack),
            SourceTypeDefinitionKind::MultiRange { range_oid } => {
                let Some(range) = catalog.types.iter().find(|value| value.oid == *range_oid) else {
                    stack.pop();
                    return false;
                };
                matches!(&range.kind, SourceTypeDefinitionKind::Range { subtype_oid }
                    if visit(catalog, *subtype_oid, stack))
            }
            SourceTypeDefinitionKind::Extension { extension, .. } => {
                let installed = catalog.extensions.iter().any(|candidate| {
                    candidate.name.eq_ignore_ascii_case(extension)
                        && candidate.installed
                        && candidate.available
                });
                installed
                    && ((extension.eq_ignore_ascii_case("hstore")
                        && definition.name.eq_ignore_ascii_case("hstore"))
                        || (extension.eq_ignore_ascii_case("postgis")
                            && matches!(definition.name.as_str(), "geometry" | "geography")))
            }
        };
        stack.pop();
        result
    }
    visit(catalog, oid, &mut Vec::new())
}

pub(crate) fn captures_source_representation(
    catalog: &SourceTypeCatalog,
    oid: u32,
    logical_type: &LogicalType,
) -> bool {
    matches!(logical_type, LogicalType::Raw { .. }) || !has_semantic_codec(catalog, oid)
}

fn logical_type_from_definition(
    definition: &SourceTypeDefinition,
    catalog: &SourceTypeCatalog,
    version: &str,
    stack: &mut Vec<u32>,
) -> Result<LogicalType, SourceTypeMappingError> {
    if stack.contains(&definition.oid) {
        return Err(SourceTypeMappingError::invalid(
            version,
            format!(
                "PostgreSQL type catalog contains a recursive cycle at OID {}",
                definition.oid
            ),
        ));
    }
    stack.push(definition.oid);
    let result = match &definition.kind {
        SourceTypeDefinitionKind::Pseudo => Err(SourceTypeMappingError::invalid(
            version,
            format!(
                "PostgreSQL pseudo-type {}.{} is not storable",
                definition.schema, definition.name
            ),
        )),
        SourceTypeDefinitionKind::Builtin { native_type } => {
            if let Some(logical_type) =
                logical_type_for_catalog_builtin(definition.oid, native_type, version)?
            {
                Ok(logical_type)
            } else if definition.schema == "pg_catalog" {
                Ok(LogicalType::raw(
                    "postgresql.pgoutput.text-envelope.v1",
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    "UTF-8",
                ))
            } else {
                // PostgreSQL base types can be supplied by applications or
                // extensions. Without a qualified semantic codec, preserve
                // the pgoutput text representation bound to this catalog
                // definition so the value can still be copied losslessly to
                // an explicit bytea/blob representation carrier.
                Ok(LogicalType::raw(
                    "postgresql.pgoutput.text-envelope.v1",
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    "UTF-8",
                ))
            }
        }
        SourceTypeDefinitionKind::Enum { labels } => {
            validate_enum_labels(labels, version)?;
            Ok(LogicalType::Enum {
                members: labels.clone(),
            })
        }
        SourceTypeDefinitionKind::Domain {
            base_oid,
            constraints,
            not_null,
        } => {
            let base = definition_by_oid(*base_oid, catalog, version)?;
            let base = logical_type_from_definition(base, catalog, version, stack)?;
            Ok(LogicalType::domain(
                format!("{}.{}", definition.schema, definition.name),
                base,
                constraints.clone(),
                *not_null,
                definition.collation.clone(),
                definition_digest(definition, version)?,
            ))
        }
        SourceTypeDefinitionKind::Composite { fields } => {
            let mut logical_fields = Vec::with_capacity(fields.len());
            let mut names = BTreeMap::new();
            for field in fields {
                if names.insert(field.name.to_ascii_lowercase(), ()).is_some() {
                    return Err(SourceTypeMappingError::invalid(
                        version,
                        format!("PostgreSQL composite type repeats field {:?}", field.name),
                    ));
                }
                let field_definition = definition_by_oid(field.type_oid, catalog, version)?;
                let logical_type =
                    logical_type_from_definition(field_definition, catalog, version, stack)?;
                logical_fields.push(LogicalField {
                    name: field.name.clone(),
                    logical_type,
                    nullable: field.nullable,
                });
            }
            Ok(LogicalType::Struct {
                fields: logical_fields,
            })
        }
        SourceTypeDefinitionKind::Array { element_oid, .. } => {
            let element = definition_by_oid(*element_oid, catalog, version)?;
            let logical_element = logical_type_from_definition(element, catalog, version, stack)?;
            if matches!(logical_element, LogicalType::Raw { .. }) {
                Ok(LogicalType::raw(
                    "postgresql.pgoutput.text-envelope.v1",
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    "UTF-8",
                ))
            } else {
                Ok(LogicalType::Array {
                    element: Box::new(logical_element),
                })
            }
        }
        SourceTypeDefinitionKind::Range { subtype_oid } => {
            let subtype = definition_by_oid(*subtype_oid, catalog, version)?;
            Ok(LogicalType::Range {
                element: Box::new(logical_type_from_definition(
                    subtype, catalog, version, stack,
                )?),
            })
        }
        SourceTypeDefinitionKind::MultiRange { range_oid } => {
            let range = definition_by_oid(*range_oid, catalog, version)?;
            let SourceTypeDefinitionKind::Range { subtype_oid } = range.kind else {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    format!(
                        "PostgreSQL multirange OID {} does not reference a range type",
                        definition.oid
                    ),
                ));
            };
            let subtype = definition_by_oid(subtype_oid, catalog, version)?;
            Ok(LogicalType::MultiRange {
                element: Box::new(logical_type_from_definition(
                    subtype, catalog, version, stack,
                )?),
            })
        }
        SourceTypeDefinitionKind::Extension {
            extension,
            codec_identity,
            encoding,
            logical_type,
        } => {
            let extension_state = catalog
                .extensions
                .iter()
                .find(|known| known.name.eq_ignore_ascii_case(extension));
            if extension_state.is_none_or(|known| !known.installed) {
                Err(SourceTypeMappingError::missing_catalog(
                    version,
                    &format!("extension {extension} type {}", definition.name),
                ))
            } else if extension_state.is_some_and(|known| known.target_compatible == Some(false)) {
                Err(SourceTypeMappingError::extension_target_blocked(
                    version,
                    &definition.name,
                    extension,
                ))
            } else if extension_state.is_some_and(|known| !known.available) {
                Ok(LogicalType::raw(
                    "postgresql.pgoutput.text-envelope.v1",
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    "UTF-8",
                ))
            } else if let Some(logical_type) = logical_type {
                Ok(logical_type.clone())
            } else if codec_identity
                .as_deref()
                .is_none_or(|identity| identity.trim().is_empty())
            {
                Ok(LogicalType::raw(
                    "postgresql.pgoutput.text-envelope.v1",
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    encoding,
                ))
            } else {
                let Some(codec_identity) = codec_identity else {
                    return Err(SourceTypeMappingError::missing_codec(
                        version,
                        &definition.name,
                    ));
                };
                Ok(LogicalType::raw(
                    codec_identity,
                    format!("{}.{}", definition.schema, definition.name),
                    definition_digest(definition, version)?,
                    encoding,
                ))
            }
        }
    };
    stack.pop();
    result
}

fn logical_type_for_catalog_builtin(
    oid: u32,
    native_type: &str,
    version: &str,
) -> Result<Option<LogicalType>, SourceTypeMappingError> {
    let logical = match oid {
        1083 => Some(LogicalType::LocalTime {
            fractional_precision: 6,
        }),
        1114 => Some(LogicalType::LocalDatetime {
            fractional_precision: 6,
        }),
        1184 => Some(LogicalType::Instant {
            fractional_precision: 6,
        }),
        1266 => Some(LogicalType::offset_time(6)),
        _ => None,
    };
    logical.map_or_else(
        || logical_type_from_builtin(native_type, version),
        |value| Ok(Some(value)),
    )
}

fn validate_enum_labels(labels: &[String], version: &str) -> Result<(), SourceTypeMappingError> {
    if labels.is_empty()
        || labels
            .iter()
            .any(|label| label.is_empty() || label.contains('\0'))
        || labels
            .iter()
            .enumerate()
            .any(|(index, label)| labels[..index].contains(label))
    {
        return Err(SourceTypeMappingError::invalid(
            version,
            "PostgreSQL ENUM labels must be non-empty and unique",
        ));
    }
    Ok(())
}

fn enum_members(native_type: &str, version: &str) -> Result<Vec<String>, SourceTypeMappingError> {
    let open = native_type.find('(').ok_or_else(|| {
        SourceTypeMappingError::invalid(version, "PostgreSQL ENUM declaration has no members")
    })?;
    let close = native_type
        .rfind(')')
        .filter(|close| *close > open && native_type[close + 1..].trim().is_empty())
        .ok_or_else(|| {
            SourceTypeMappingError::invalid(version, "PostgreSQL ENUM declaration is malformed")
        })?;
    let mut chars = native_type[open + 1..close].chars().peekable();
    let mut members = Vec::new();
    loop {
        while chars
            .next_if(|character| character.is_whitespace())
            .is_some()
        {}
        if chars.peek().is_none() {
            break;
        }
        if chars.next() != Some('\'') {
            return Err(SourceTypeMappingError::invalid(
                version,
                "PostgreSQL ENUM labels must be quoted",
            ));
        }
        let mut member = String::new();
        let mut closed = false;
        while let Some(character) = chars.next() {
            match character {
                '\\' => {
                    let escaped = chars.next().ok_or_else(|| {
                        SourceTypeMappingError::invalid(
                            version,
                            "PostgreSQL ENUM label has a dangling escape",
                        )
                    })?;
                    member.push(escaped);
                }
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    member.push('\'');
                }
                '\'' => {
                    closed = true;
                    break;
                }
                other => member.push(other),
            }
        }
        if !closed || member.is_empty() || member.contains('\0') {
            return Err(SourceTypeMappingError::invalid(
                version,
                "PostgreSQL ENUM label is empty, unterminated, or contains NUL",
            ));
        }
        if members.iter().any(|known| known == &member) {
            return Err(SourceTypeMappingError::invalid(
                version,
                "PostgreSQL ENUM labels must be unique",
            ));
        }
        members.push(member);
        while chars
            .next_if(|character| character.is_whitespace())
            .is_some()
        {}
        match chars.next() {
            None => break,
            Some(',') => {}
            Some(_) => {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "PostgreSQL ENUM labels must be comma separated",
                ));
            }
        }
    }
    if members.is_empty() {
        return Err(SourceTypeMappingError::invalid(
            version,
            "PostgreSQL ENUM requires at least one label",
        ));
    }
    Ok(members)
}

fn numeric(native_type: &str, version: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let Some(parameters) = native_type
        .split_once('(')
        .and_then(|(_, value)| value.strip_suffix(')'))
    else {
        return if matches!(native_type, "numeric" | "decimal") {
            Ok(LogicalType::decimal_unbounded())
        } else {
            Err(SourceTypeMappingError::invalid(
                version,
                "numeric declaration parameters are malformed",
            ))
        };
    };
    let mut parameters = parameters.split(',');
    let precision = parameters
        .next()
        .unwrap_or_default()
        .trim()
        .parse::<u16>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "numeric precision is invalid"))?;
    let scale = parameters
        .next()
        .unwrap_or("0")
        .trim()
        .parse::<i32>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "numeric scale is invalid"))?;
    if parameters.next().is_some()
        || !(1..=1000).contains(&precision)
        || !(-1000..=1000).contains(&scale)
    {
        return Err(SourceTypeMappingError::invalid(
            version,
            "numeric precision/scale is outside the ChangeEvent range",
        ));
    }
    Ok(LogicalType::decimal(precision, scale))
}

fn parenthesized_u64(
    native_type: &str,
    prefix: &str,
    version: &str,
) -> Result<u64, SourceTypeMappingError> {
    let value = native_type
        .strip_prefix(&format!("{prefix}("))
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| SourceTypeMappingError::invalid(version, "type parameters are malformed"))?;
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "type length is invalid"))
}

fn spatial(native_type: &str, version: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let parameters = native_type
        .strip_prefix("geometry(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| {
            SourceTypeMappingError::invalid(version, "geometry declaration is malformed")
        })?;
    let mut parts = parameters.split(',').map(str::trim);
    let declared_subtype = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SourceTypeMappingError::invalid(version, "geometry subtype is missing"))?;
    let srid = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SourceTypeMappingError::invalid(version, "geometry SRID is missing"))?
        .parse::<i32>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "geometry SRID is invalid"))?;
    if parts.next().is_some() || srid < 0 {
        return Err(SourceTypeMappingError::invalid(
            version,
            "geometry requires exactly one non-negative SRID",
        ));
    }
    let declared_subtype = declared_subtype.to_ascii_lowercase();
    let (subtype, dimensions) = if let Some(subtype) = declared_subtype.strip_suffix("zm") {
        (subtype, 4)
    } else if let Some(subtype) = declared_subtype.strip_suffix('z') {
        (subtype, 3)
    } else if let Some(subtype) = declared_subtype.strip_suffix('m') {
        (subtype, 3)
    } else {
        (declared_subtype.as_str(), 2)
    };
    if !matches!(
        subtype,
        "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    ) {
        return Err(SourceTypeMappingError::unsupported(version, native_type));
    }
    Ok(LogicalType::spatial(subtype, Some(srid), dimensions))
}

fn time(native_type: &str, version: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let with_time_zone = native_type == "timetz" || native_type.ends_with("with time zone");
    let precision = if let Some(parameters) = native_type.strip_prefix("time(") {
        let (precision, suffix) = parameters.split_once(')').ok_or_else(|| {
            SourceTypeMappingError::invalid(version, "time declaration is malformed")
        })?;
        if !suffix.is_empty() && suffix != " with time zone" && suffix != " without time zone" {
            return Err(SourceTypeMappingError::invalid(
                version,
                "time declaration has an invalid zone qualifier",
            ));
        }
        precision
            .parse::<u8>()
            .map_err(|_| SourceTypeMappingError::invalid(version, "time precision is invalid"))?
    } else {
        6
    };
    if precision > 6 {
        return Err(SourceTypeMappingError::invalid(
            version,
            "time precision is outside PostgreSQL limits",
        ));
    }
    Ok(if with_time_zone {
        LogicalType::offset_time(precision)
    } else {
        LogicalType::LocalTime {
            fractional_precision: precision,
        }
    })
}

fn interval(native_type: &str, version: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let Some(mut remainder) = native_type.strip_prefix("interval") else {
        return Err(SourceTypeMappingError::unsupported(version, native_type));
    };
    let mut precision = 6;
    let mut has_explicit_precision = false;
    if remainder.starts_with('(') {
        let Some(close) = remainder.find(')') else {
            return Err(SourceTypeMappingError::invalid(
                version,
                "interval precision is malformed",
            ));
        };
        precision = remainder[1..close].parse::<u8>().map_err(|_| {
            SourceTypeMappingError::invalid(version, "interval precision is invalid")
        })?;
        has_explicit_precision = true;
        remainder = &remainder[close + 1..];
        if !remainder.trim().is_empty() {
            return Err(SourceTypeMappingError::invalid(
                version,
                "interval leading precision cannot be combined with a field qualifier",
            ));
        }
    } else if let Some(open) = remainder.rfind('(')
        && let Some(precision_text) = remainder[open + 1..].strip_suffix(')')
    {
        precision = precision_text.parse::<u8>().map_err(|_| {
            SourceTypeMappingError::invalid(version, "interval precision is invalid")
        })?;
        has_explicit_precision = true;
        remainder = &remainder[..open];
    }
    if precision > 6 {
        return Err(SourceTypeMappingError::invalid(
            version,
            "interval precision is outside PostgreSQL limits",
        ));
    }
    let qualifier = remainder.trim();
    if !matches!(
        qualifier,
        "" | "year"
            | "month"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "year to month"
            | "day to hour"
            | "day to minute"
            | "day to second"
            | "hour to minute"
            | "hour to second"
            | "minute to second"
    ) {
        return Err(SourceTypeMappingError::invalid(
            version,
            "interval field qualifier is invalid",
        ));
    }
    if has_explicit_precision && !qualifier.is_empty() && !qualifier.ends_with("second") {
        return Err(SourceTypeMappingError::invalid(
            version,
            "interval precision requires a qualifier ending in SECOND",
        ));
    }
    Ok(LogicalType::calendar_interval(precision))
}

fn timestamp(native_type: &str, version: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let (prefix, zone) = if native_type.ends_with(" without time zone") {
        ("timestamp", " without time zone")
    } else if native_type.ends_with(" with time zone") {
        ("timestamp", " with time zone")
    } else {
        return Err(SourceTypeMappingError::invalid(
            version,
            "timestamp must declare its time-zone semantics",
        ));
    };
    let precision = native_type
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(zone))
        .map(|value| value.trim_matches(['(', ')']))
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<u8>().map_err(|_| {
                SourceTypeMappingError::invalid(version, "timestamp precision is invalid")
            })
        })
        .transpose()?
        .unwrap_or(6);
    if precision > 6 {
        return Err(SourceTypeMappingError::invalid(
            version,
            "timestamp precision is outside PostgreSQL 15 limits",
        ));
    }
    Ok(if zone == " with time zone" {
        LogicalType::Instant {
            fractional_precision: precision,
        }
    } else {
        LogicalType::LocalDatetime {
            fractional_precision: precision,
        }
    })
}

fn evidence_digest(
    mapping_version: &str,
    native_type: &str,
    logical_type: &LogicalType,
    catalog_digest: &str,
    value_representation: &BTreeMap<String, String>,
) -> String {
    let bytes = serde_json::to_vec(&(
        mapping_version,
        native_type,
        logical_type,
        catalog_digest,
        value_representation,
    ))
    .expect("PostgreSQL SourceTypeMapping evidence is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_postgresql_native_shapes_without_value_inference() {
        assert_eq!(
            source_type_mapping("numeric(30,6)").unwrap().logical_type,
            LogicalType::decimal(30, 6)
        );
        assert_eq!(
            source_type_mapping("timestamp(3) with time zone")
                .unwrap()
                .logical_type,
            LogicalType::Instant {
                fractional_precision: 3
            }
        );
        assert_eq!(
            source_type_mapping("character varying(255)")
                .unwrap()
                .logical_type,
            LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(255),
                length_unit: LengthUnit::Characters,
                collation: None,
            }
        );
    }

    #[test]
    fn aliases_and_typmods_match_the_semantic_decoder() {
        assert_eq!(
            source_type_mapping("bool").unwrap().logical_type,
            LogicalType::boolean()
        );
        assert_eq!(
            source_type_mapping("float4").unwrap().logical_type,
            LogicalType::float(32)
        );
        assert_eq!(
            source_type_mapping("float8").unwrap().logical_type,
            LogicalType::float(64)
        );
        assert_eq!(
            source_type_mapping("character(8)").unwrap().logical_type,
            LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(8),
                length_unit: LengthUnit::Characters,
                collation: None,
            }
        );
        assert_eq!(
            source_type_mapping("bit varying(64)").unwrap().logical_type,
            LogicalType::variable_bit_string(Some(64))
        );
    }

    #[test]
    fn variable_bit_string_accepts_shorter_values_but_enforces_its_declared_maximum() {
        let mapping = source_type_mapping("bit varying(64)").unwrap();
        let value = |bit_length| change_event::LogicalValue::BitString {
            bytes_base64url: "AA".into(),
            bit_length,
            padding: change_event::BitPadding::Zero,
            bit_order: change_event::BitOrder::MsbFirst,
        };

        assert!(mapping.logical_type.matches_value(&value(3)));
        assert!(!mapping.logical_type.matches_value(&value(65)));
        assert!(
            !source_type_mapping("bit(64)")
                .unwrap()
                .logical_type
                .matches_value(&value(3))
        );
    }

    #[test]
    fn inet_and_cidr_preserve_address_family_and_prefix_semantics() {
        let network =
            |family: &str, address: &str, prefix_length| change_event::LogicalValue::Network {
                family: family.into(),
                address: address.into(),
                prefix_length,
            };
        let inet = source_type_mapping("inet").unwrap().logical_type;
        let cidr = source_type_mapping("cidr").unwrap().logical_type;

        assert!(inet.matches_value(&network("ipv4", "192.0.2.1", Some(24))));
        assert!(inet.matches_value(&network("ipv6", "2001:db8::1", Some(64))));
        assert!(inet.matches_value(&network("ipv4", "192.0.2.1", None)));
        assert!(cidr.matches_value(&network("ipv4", "192.0.2.0", Some(24))));
        assert!(cidr.matches_value(&network("ipv6", "2001:db8::", Some(64))));
        assert!(!cidr.matches_value(&network("ipv4", "192.0.2.0", None)));
        assert!(!inet.matches_value(&network("macaddr", "08:00:2b:01:02:03", None)));
    }

    #[test]
    fn raw_column_mapping_uses_representation_capture_even_if_a_codec_exists() {
        let catalog =
            SourceTypeCatalog::new([SourceTypeDefinition::builtin(1562, "pg_catalog", "varbit")]);
        let unbounded = source_type_mapping_with_catalog("varbit", &catalog).unwrap();
        assert!(matches!(unbounded.logical_type, LogicalType::Raw { .. }));
        assert!(captures_source_representation(
            &catalog,
            1562,
            &unbounded.logical_type
        ));

        let bounded = source_type_mapping_with_catalog("varbit(64)", &catalog).unwrap();
        assert_eq!(
            bounded.logical_type,
            LogicalType::variable_bit_string(Some(64))
        );
        assert!(!captures_source_representation(
            &catalog,
            1562,
            &bounded.logical_type
        ));
    }

    #[test]
    fn maps_postgresql_negative_numeric_scale_as_source_evidence() {
        assert_eq!(
            source_type_mapping("numeric(2,-3)").unwrap().logical_type,
            LogicalType::decimal(2, -3)
        );
        assert!(validate_native_type_for_version("16", "numeric(2,-3)").is_ok());
    }

    #[test]
    fn maps_unbounded_numeric_and_temporal_components() {
        assert_eq!(
            source_type_mapping("numeric").unwrap().logical_type,
            LogicalType::decimal_unbounded()
        );
        assert_eq!(
            source_type_mapping("decimal").unwrap().logical_type,
            LogicalType::decimal_unbounded()
        );
        assert!(source_type_mapping("numeric(").is_err());
        assert!(source_type_mapping("decimal(12,3").is_err());
        assert_eq!(
            source_type_mapping("time with time zone")
                .unwrap()
                .logical_type,
            LogicalType::offset_time(6)
        );
        assert_eq!(
            source_type_mapping("time(3) with time zone")
                .unwrap()
                .logical_type,
            LogicalType::offset_time(3)
        );
        assert_eq!(
            source_type_mapping("interval").unwrap().logical_type,
            LogicalType::calendar_interval(6)
        );
        assert!(source_type_mapping("interval(3) day to second").is_err());
        assert_eq!(
            source_type_mapping("interval day to second(2)")
                .unwrap()
                .logical_type,
            LogicalType::calendar_interval(2)
        );
        assert!(source_type_mapping("interval day to second(7)").is_err());
        assert!(source_type_mapping("interval year(3)").is_err());
        assert!(source_type_mapping("interval(3) year to month").is_err());
    }

    #[test]
    fn blocks_unproven_json_arrays() {
        assert!(source_type_mapping("integer[]").is_err());
    }

    #[test]
    fn maps_postgresql_enum_labels_without_using_ordinals() {
        assert_eq!(
            source_type_mapping("enum('Ready','blocked')")
                .unwrap()
                .logical_type,
            LogicalType::Enum {
                members: vec!["Ready".into(), "blocked".into()]
            }
        );
        assert!(source_type_mapping("enum('a','a')").is_err());
        assert_eq!(
            source_type_mapping("bit varying(12)").unwrap().logical_type,
            LogicalType::variable_bit_string(Some(12))
        );
        assert_eq!(
            source_type_mapping("geometry(point,4326)")
                .unwrap()
                .logical_type,
            LogicalType::spatial("point", Some(4326), 2)
        );
    }

    #[test]
    fn maps_versioned_postgresql_releases_with_distinct_evidence() {
        let pg15 = source_type_mapping_for_version("15", "integer").unwrap();
        let pg16 = source_type_mapping_for_version("16", "integer").unwrap();
        let pg17 = source_type_mapping_for_version("17", "integer").unwrap();
        assert_eq!(pg15.connector.version, "15");
        assert_eq!(pg16.connector.version, "16");
        assert_eq!(pg17.connector.version, "17");
        assert_ne!(pg15.mapping_version, pg16.mapping_version);
        assert_ne!(pg16.evidence_digest, pg17.evidence_digest);
        assert!(source_type_mapping_for_version("14", "integer").is_err());
        let qualified = source_type_mapping_with_source_evidence(
            "integer",
            &SourceTypeCatalog::default(),
            ServerBuildIdentity::new("postgresql", "community", "15.19", "postgres-15.19"),
            "environment-1",
        )
        .unwrap();
        assert!(qualified.source_build.is_some());
        assert_eq!(
            qualified.environment_fingerprint.as_deref(),
            Some("environment-1")
        );
        assert!(qualified.source_definition_fingerprint.is_some());
    }

    #[test]
    fn recursively_maps_catalog_enum_domain_composite_array_and_ranges() {
        let catalog = SourceTypeCatalog::new([
            SourceTypeDefinition::builtin(23, "pg_catalog", "integer"),
            SourceTypeDefinition::enum_type(8_000, "public", "state", ["new", "done"]),
            SourceTypeDefinition::domain(
                8_001,
                "public",
                "positive_id",
                23,
                ["VALUE > 0"],
                true,
                None,
            ),
            SourceTypeDefinition::composite(
                8_002,
                "public",
                "item",
                [
                    SourceTypeField::new("id", 8_001, false),
                    SourceTypeField::new("state", 8_000, true),
                ],
            ),
            SourceTypeDefinition::array(8_003, "public", "_item", 8_002),
            SourceTypeDefinition::range(8_004, "public", "item_range", 23),
            SourceTypeDefinition::multi_range(8_005, "public", "item_ranges", 8_004),
        ]);

        assert!(matches!(
            source_type_mapping_with_catalog("public.state", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::Enum { ref members } if members == &["new", "done"]
        ));
        assert!(matches!(
            source_type_mapping_with_catalog("public.positive_id", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::Domain { ref constraints, not_null: true, .. }
                if constraints == &["VALUE > 0"]
        ));
        assert!(matches!(
            source_type_mapping_with_catalog("public.item", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::Struct { ref fields }
                if fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>()
                    == ["id", "state"]
        ));
        assert!(matches!(
            source_type_mapping_with_catalog("public._item", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::Array { element } if matches!(*element, LogicalType::Struct { .. })
        ));
        assert!(matches!(
            source_type_mapping_with_catalog("public.item_range", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::Range { element } if *element == LogicalType::integer(true, 32)
        ));
        assert!(matches!(
            source_type_mapping_with_catalog("public.item_ranges", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::MultiRange { element } if *element == LogicalType::integer(true, 32)
        ));
    }

    #[test]
    fn user_defined_base_types_without_semantic_codecs_use_bound_pgoutput_text_envelopes() {
        let catalog = SourceTypeCatalog::new([
            SourceTypeDefinition::builtin(90_001, "application", "invoice_code"),
            SourceTypeDefinition::array(90_002, "application", "_invoice_code", 90_001),
        ]);

        for version in ["15", "16", "17"] {
            let mapping = source_type_mapping_with_catalog_for_version(
                version,
                "application.invoice_code",
                &catalog,
            )
            .unwrap_or_else(|error| panic!("PostgreSQL {version}: {error}"));
            assert!(matches!(
                mapping.logical_type,
                LogicalType::Raw { ref codec_identity, ref native_type, .. }
                    if codec_identity == "postgresql.pgoutput.text-envelope.v1"
                        && native_type == "application.invoice_code"
            ));
            let representation = mapping
                .source_representation_evidence
                .expect("custom base mapping must bind a raw text envelope to its catalog type");
            assert_eq!(representation.protocol, "pgoutput.v1");
            assert_eq!(representation.format, SourceRepresentationFormat::Text);
            assert_eq!(
                representation.source_type_identity,
                "postgresql.pg_type.v1:90001:application.invoice_code"
            );
            assert_eq!(
                representation.type_metadata["type_definition_digest"],
                format!("sha256:{}", catalog.types[0].definition_digest)
            );

            let array = source_type_mapping_with_catalog_for_version(
                version,
                "application.invoice_code[]",
                &catalog,
            )
            .unwrap_or_else(|error| panic!("PostgreSQL {version} array: {error}"));
            assert!(matches!(
                array.logical_type,
                LogicalType::Raw { ref codec_identity, ref native_type, .. }
                    if codec_identity == "postgresql.pgoutput.text-envelope.v1"
                        && native_type == "application._invoice_code"
            ));
            assert!(array.source_representation_evidence.is_some());
        }
    }

    #[test]
    fn catalog_retains_pseudotype_dependencies_but_refuses_them_as_stored_values() {
        let pseudo = SourceTypeDefinition::pseudo(90_101, "pg_catalog", "anyelement", None);
        let pseudo_array =
            SourceTypeDefinition::array(90_102, "pg_catalog", "_anyelement", pseudo.oid);
        let dependent_composite = SourceTypeDefinition::composite(
            90_103,
            "pg_catalog",
            "catalog_record",
            [SourceTypeField::new("payload", pseudo.oid, true)],
        );
        let catalog = SourceTypeCatalog::new([pseudo, pseudo_array, dependent_composite]);

        assert!(source_type_mapping_with_catalog("pg_catalog.anyelement", &catalog).is_err());
        assert!(source_type_mapping_with_catalog("pg_catalog._anyelement", &catalog).is_err());
        let mapping = source_type_mapping_with_catalog("pg_catalog.catalog_record", &catalog)
            .expect(
                "catalog composite can be represented opaquely with a complete dependency closure",
            );
        assert!(matches!(mapping.logical_type, LogicalType::Raw { .. }));
        assert!(mapping.source_representation_evidence.is_some());
    }

    #[test]
    fn extension_mapping_requires_installed_extension_and_codec_evidence() {
        let postgis = SourceTypeDefinition::extension(
            9_001,
            "public",
            "geometry",
            "postgis",
            "postgis-3.4.geometry-ewkb",
            "ewkb",
            Some(LogicalType::spatial("point", Some(4326), 2)),
        );
        let raw = SourceTypeDefinition::extension(
            9_002,
            "public",
            "hstore",
            "hstore",
            "hstore-1.text-v1",
            "text",
            None,
        );
        let catalog = SourceTypeCatalog::with_extensions(
            [postgis, raw],
            [
                SourceExtension {
                    name: "postgis".into(),
                    version: "3.4.0".into(),
                    schema: "public".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
                SourceExtension {
                    name: "hstore".into(),
                    version: "1.8".into(),
                    schema: "public".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
            ],
        );
        assert_eq!(
            source_type_mapping_with_catalog("public.geometry", &catalog)
                .unwrap()
                .logical_type,
            LogicalType::spatial("point", Some(4326), 2)
        );
        assert!(
            source_type_mapping_with_catalog("public.hstore", &catalog)
                .unwrap()
                .logical_type
                .family_name()
                == "raw"
        );
        let missing = SourceTypeCatalog::new([SourceTypeDefinition::extension(
            9_003,
            "public",
            "geometry",
            "postgis",
            "postgis-3.4.geometry-ewkb",
            "ewkb",
            None,
        )]);
        assert_eq!(
            source_type_mapping_with_catalog("public.geometry", &missing)
                .unwrap_err()
                .code(),
            "postgresql15.source_type.catalog_required"
        );

        let mut unavailable = catalog.clone();
        unavailable.extensions[0].available = false;
        let unavailable_mapping =
            source_type_mapping_with_catalog("public.geometry", &unavailable).unwrap();
        assert!(matches!(
            unavailable_mapping.logical_type,
            LogicalType::Raw { ref codec_identity, .. }
                if codec_identity == "postgresql.pgoutput.text-envelope.v1"
        ));
        let representation = unavailable_mapping
            .source_representation_evidence
            .expect("Raw mapping binds source representation identity and metadata");
        assert_eq!(
            representation.source_type_identity,
            "postgresql.pg_type.v1:9001:public.geometry"
        );
        assert_eq!(representation.protocol, "pgoutput.v1");
        assert_eq!(representation.format, SourceRepresentationFormat::Text);
        assert_eq!(representation.type_metadata["column_oid"], "9001");
        assert_eq!(
            representation.type_metadata["native_type"],
            "public.geometry"
        );
        assert_eq!(representation.type_metadata["type_name"], "geometry");
        assert_eq!(
            representation.type_metadata["text_output_profile"],
            "pgoutput-v1/text/UTF8"
        );
        assert_eq!(
            representation.allowed_context_metadata_keys,
            BTreeSet::from([
                "relation_oid".into(),
                "relation_schema".into(),
                "relation_name".into(),
                "column_name".into(),
                "type_modifier".into(),
                "environment.lc_monetary".into(),
            ])
        );
        assert!(!has_semantic_codec(&unavailable, 9_001));
        let unqualified_mapping =
            source_type_mapping_with_catalog("geometry", &unavailable).unwrap();
        assert_eq!(
            unqualified_mapping
                .source_representation_evidence
                .expect("unqualified column declaration still binds its source catalog type")
                .type_metadata["native_type"],
            "geometry"
        );

        let mut target_blocked = catalog.clone();
        target_blocked.extensions[0].target_compatible = Some(false);
        assert_eq!(
            source_type_mapping_with_catalog("public.geometry", &target_blocked)
                .unwrap_err()
                .code(),
            "postgresql15.source_type.extension_target_blocked"
        );

        let mut forged = SourceTypeDefinition::enum_type(9_004, "public", "forged", ["one", "two"]);
        forged.definition_digest = "forged".into();
        assert_eq!(
            source_type_mapping_with_catalog("public.forged", &SourceTypeCatalog::new([forged]),)
                .unwrap_err()
                .code(),
            "postgresql15.source_type.invalid_declaration"
        );
    }

    #[test]
    fn recursive_type_definition_closure_is_complete_canonical_and_extension_bound() {
        let catalog = SourceTypeCatalog::with_extensions(
            [
                SourceTypeDefinition::builtin(23, "pg_catalog", "int4"),
                SourceTypeDefinition::extension(
                    9_002,
                    "extensions",
                    "hstore",
                    "hstore",
                    "postgresql.hstore.text.v1",
                    "UTF-8",
                    Some(LogicalType::Map {
                        key: Box::new(LogicalType::text("UTF8", None)),
                        value: Box::new(LogicalType::text("UTF8", None)),
                    }),
                ),
                SourceTypeDefinition::composite(
                    9_003,
                    "app",
                    "record_with_map",
                    [
                        SourceTypeField::new("id", 23, false),
                        SourceTypeField::new("attributes", 9_002, true),
                    ],
                ),
            ],
            [SourceExtension {
                name: "hstore".into(),
                version: "1.8".into(),
                schema: "extensions".into(),
                installed: true,
                available: true,
                target_compatible: None,
            }],
        );
        let closure = catalog.definition_closure(9_003).unwrap();
        assert_eq!(
            closure
                .types
                .iter()
                .map(|definition| definition.oid)
                .collect::<Vec<_>>(),
            [23, 9_002, 9_003]
        );
        assert!(validate_type_definition_closure(&closure, 9_003));

        let mut missing = closure.clone();
        missing.types.retain(|definition| definition.oid != 9_002);
        assert!(!validate_type_definition_closure(&missing, 9_003));

        let mut extra = closure.clone();
        extra
            .types
            .push(SourceTypeDefinition::builtin(25, "pg_catalog", "text"));
        extra.types.sort_by_key(|definition| definition.oid);
        assert!(!validate_type_definition_closure(&extra, 9_003));

        let mut no_extension_evidence = closure.clone();
        no_extension_evidence.extensions.clear();
        assert!(!validate_type_definition_closure(
            &no_extension_evidence,
            9_003
        ));

        let mut unavailable = closure.clone();
        unavailable.extensions[0].available = false;
        assert!(validate_type_definition_closure(&unavailable, 9_003));

        let mut not_installed = closure;
        not_installed.extensions[0].installed = false;
        assert!(!validate_type_definition_closure(&not_installed, 9_003));
    }
}
