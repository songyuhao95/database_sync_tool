//! PostgreSQL 15's strict Source Type Mapping.
//!
//! PostgreSQL's formatted catalog type is used only as source-definition
//! evidence.  Values are never inspected to choose a mapping.  Types whose
//! comparison, padding, JSON, array, or extension semantics are not carried
//! by the current ChangeEvent contract fail closed.

use change_event::{
    ConnectorIdentity, DuplicateKeyPolicy, JsonProfile, LengthUnit, LogicalField, LogicalType,
    ServerBuildIdentity, SourceTypeMapping,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, error::Error, fmt};

pub const MAPPING_VERSION: &str = "postgresql-15.source-type-mapping.v2";
pub const MAPPING_VERSION_16: &str = "postgresql-16.source-type-mapping.v2";
pub const MAPPING_VERSION_17: &str = "postgresql-17.source-type-mapping.v2";

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

    fn digest(&self) -> String {
        self.evidence_digest()
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
            kind: SourceTypeDefinitionKind::Array { element_oid },
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

    fn extension_unavailable(version: &str, native_type: &str, extension: &str) -> Self {
        Self {
            code: format!("postgresql{version}.source_type.extension_unavailable"),
            message: format!(
                "PostgreSQL {version} type {native_type:?} requires extension {extension:?}, which is not available"
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
    let native_type = normalize(native_type, version)?;
    let logical_type = logical_type_with_catalog(&native_type, catalog, version)?;
    logical_type
        .validate()
        .map_err(|error| SourceTypeMappingError::invalid(version, error.to_string()))?;
    let source_definition_fingerprint = if array_declaration(&native_type).is_some()
        || base_name(&native_type).eq_ignore_ascii_case("enum")
        || logical_type_from_builtin(&native_type, version)?.is_some()
    {
        None
    } else {
        Some(definition_digest(
            find_definition(&native_type, catalog, version)?,
            version,
        )?)
    };
    let base = base_name(&native_type).to_ascii_lowercase();
    let mapping_version = mapping_version(version);
    let mapping_id = format!("postgresql{version}.source-type.{base}");
    let evidence_digest = evidence_digest(
        &mapping_version,
        &native_type,
        &logical_type,
        &catalog.digest(),
    );
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
    })
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
    if let Some(logical) = logical_type_from_builtin(native_type, version)? {
        return Ok(logical);
    }
    let definition = find_definition(native_type, catalog, version)?;
    let mut stack = Vec::new();
    logical_type_from_definition(definition, catalog, version, &mut stack)
}

fn logical_type_from_builtin(
    native_type: &str,
    version: &str,
) -> Result<Option<LogicalType>, SourceTypeMappingError> {
    let logical = match native_type {
        "boolean" => Ok(LogicalType::boolean()),
        "smallint" => Ok(LogicalType::integer(true, 16)),
        "integer" | "int" => Ok(LogicalType::integer(true, 32)),
        "bigint" => Ok(LogicalType::integer(true, 64)),
        "oid" | "xid" | "cid" => Ok(LogicalType::integer(false, 32)),
        "real" => Ok(LogicalType::float(32)),
        "double precision" => Ok(LogicalType::float(64)),
        "text" | "character" | "char" | "character varying" | "varchar" => Ok(LogicalType::Text {
            charset: "UTF8".into(),
            max_length: None,
            length_unit: LengthUnit::Characters,
            collation: None,
        }),
        "bytea" => Ok(LogicalType::binary(None)),
        "bit" => Ok(LogicalType::bit_string(1)),
        "date" => Ok(LogicalType::date()),
        "time without time zone" | "time" => Ok(LogicalType::LocalTime {
            fractional_precision: 6,
        }),
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
        "inet" => Ok(LogicalType::network("inet", false)),
        "cidr" => Ok(LogicalType::network("inet", true)),
        "macaddr" | "macaddr8" => Ok(LogicalType::network(native_type, false)),
        "xml" => Ok(LogicalType::xml()),
        "numeric" | "decimal" | "interval" | "timetz" | "money" => {
            Err(SourceTypeMappingError::unsupported(version, native_type))
        }
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
            Ok(LogicalType::bit_string(length))
        }
        _ if native_type.starts_with("varbit(") => {
            let length = parenthesized_u64(native_type, "varbit", version)?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    version,
                    "varbit length must be positive and bounded",
                ));
            }
            Ok(LogicalType::bit_string(length))
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
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            Ok(LogicalType::spatial(native_type, None, 2))
        }
        "int4range" => Ok(LogicalType::Range {
            element: Box::new(LogicalType::integer(true, 32)),
        }),
        "int8range" => Ok(LogicalType::Range {
            element: Box::new(LogicalType::integer(true, 64)),
        }),
        "daterange" => Ok(LogicalType::Range {
            element: Box::new(LogicalType::date()),
        }),
        "tsrange" => Ok(LogicalType::Range {
            element: Box::new(LogicalType::local_datetime(6)),
        }),
        "tstzrange" => Ok(LogicalType::Range {
            element: Box::new(LogicalType::instant(6)),
        }),
        "int4multirange" => Ok(LogicalType::MultiRange {
            element: Box::new(LogicalType::integer(true, 32)),
        }),
        "int8multirange" => Ok(LogicalType::MultiRange {
            element: Box::new(LogicalType::integer(true, 64)),
        }),
        "datemultirange" => Ok(LogicalType::MultiRange {
            element: Box::new(LogicalType::date()),
        }),
        "tsmultirange" => Ok(LogicalType::MultiRange {
            element: Box::new(LogicalType::local_datetime(6)),
        }),
        "tstzmultirange" => Ok(LogicalType::MultiRange {
            element: Box::new(LogicalType::instant(6)),
        }),
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
        SourceTypeDefinitionKind::Builtin { native_type } => {
            logical_type_from_builtin(native_type, version)?
                .ok_or_else(|| SourceTypeMappingError::unsupported(version, native_type))
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
        SourceTypeDefinitionKind::Array { element_oid } => {
            let element = definition_by_oid(*element_oid, catalog, version)?;
            Ok(LogicalType::Array {
                element: Box::new(logical_type_from_definition(
                    element, catalog, version, stack,
                )?),
            })
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
            } else if extension_state.is_some_and(|known| !known.available) {
                Err(SourceTypeMappingError::extension_unavailable(
                    version,
                    &definition.name,
                    extension,
                ))
            } else if extension_state.is_some_and(|known| known.target_compatible == Some(false)) {
                Err(SourceTypeMappingError::extension_target_blocked(
                    version,
                    &definition.name,
                    extension,
                ))
            } else if let Some(logical_type) = logical_type {
                Ok(logical_type.clone())
            } else if codec_identity
                .as_deref()
                .is_none_or(|identity| identity.trim().is_empty())
            {
                Err(SourceTypeMappingError::missing_codec(
                    version,
                    &definition.name,
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
    let parameters = native_type
        .split_once('(')
        .and_then(|(_, value)| value.strip_suffix(')'))
        .ok_or_else(|| {
            SourceTypeMappingError::invalid(version, "numeric parameters are malformed")
        })?;
    let (precision, scale) = parameters.split_once(',').ok_or_else(|| {
        SourceTypeMappingError::invalid(version, "numeric requires precision and scale")
    })?;
    let precision = precision
        .trim()
        .parse::<u16>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "numeric precision is invalid"))?;
    let scale = scale
        .trim()
        .parse::<i32>()
        .map_err(|_| SourceTypeMappingError::invalid(version, "numeric scale is invalid"))?;
    if !(1..=1000).contains(&precision) || !(-1000..=i32::from(precision)).contains(&scale) {
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
    let precision = native_type
        .strip_prefix("time(")
        .and_then(|value| value.strip_suffix(") without time zone"))
        .or_else(|| {
            native_type
                .strip_prefix("time(")
                .and_then(|value| value.strip_suffix(")"))
        })
        .map(|value| {
            value
                .parse::<u8>()
                .map_err(|_| SourceTypeMappingError::invalid(version, "time precision is invalid"))
        })
        .transpose()?
        .unwrap_or(6);
    if precision > 6 {
        return Err(SourceTypeMappingError::invalid(
            version,
            "time precision is outside PostgreSQL limits",
        ));
    }
    Ok(LogicalType::LocalTime {
        fractional_precision: precision,
    })
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
) -> String {
    let bytes = serde_json::to_vec(&(mapping_version, native_type, logical_type, catalog_digest))
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
    fn maps_postgresql_negative_numeric_scale_as_source_evidence() {
        assert_eq!(
            source_type_mapping("numeric(2,-3)").unwrap().logical_type,
            LogicalType::decimal(2, -3)
        );
        assert!(validate_native_type_for_version("16", "numeric(2,-3)").is_ok());
    }

    #[test]
    fn blocks_unproven_json_arrays_and_unbounded_numeric() {
        for native_type in ["integer[]", "numeric"] {
            assert!(source_type_mapping(native_type).is_err(), "{native_type}");
        }
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
            LogicalType::bit_string(12)
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
        assert_eq!(
            source_type_mapping_with_catalog("public.geometry", &unavailable)
                .unwrap_err()
                .code(),
            "postgresql15.source_type.extension_unavailable"
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
}
