//! PostgreSQL 15-native Source Contract validation.
//!
//! This module deliberately lives in the versioned adapter.  The database-neutral
//! `change_event` crate validates shape and logical values; PostgreSQL owns LSN,
//! identity, pgoutput image, and native type rules.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ColumnDatum, Datum, LogicalValue, Operation, SourceContractError, SourceCursor,
    SourceRepresentationFormat, ValidatedTransaction,
};

pub(crate) fn validate(transaction: &ValidatedTransaction) -> Result<(), SourceContractError> {
    let tx = transaction.transaction();
    ensure(
        tx.source.kind == "postgresql",
        "PostgreSQL Source Contract requires source kind postgresql",
    )?;
    let version = tx.source.version.split('.').collect::<Vec<_>>();
    ensure(
        version.len() == 2
            && matches!(version[0], "15" | "16" | "17")
            && version[1].parse::<u32>().is_ok(),
        "unsupported PostgreSQL source version; expected 15.x, 16.x, or 17.x",
    )?;
    let identity = tx.source.id.split(':').collect::<Vec<_>>();
    ensure(
        matches!(identity.len(), 4 | 5)
            && identity[0] == "postgresql"
            && identity[1].parse::<u64>().is_ok_and(|value| value > 0)
            && identity[2].parse::<u32>().is_ok_and(|value| value > 0)
            && identity[3].parse::<u32>().is_ok_and(|value| value > 0),
        "invalid PostgreSQL source identity",
    )?;

    let begin = lsn(&tx.begin_cursor)?;
    let end = lsn(&tx.commit_cursor)?;
    ensure(
        begin < end,
        "PostgreSQL commit end must follow its commit record",
    )?;
    let (xid, suffix) = tx
        .id
        .strip_prefix("pg:")
        .and_then(|value| value.split_once(':'))
        .ok_or_else(|| SourceContractError::new("invalid PostgreSQL transaction id"))?;
    ensure(
        xid.parse::<u32>().is_ok_and(|value| value > 0) && suffix == tx.commit_cursor.value,
        "PostgreSQL transaction id/end LSN mismatch",
    )?;

    let database = tx
        .changes
        .first()
        .and_then(|change| change.database.as_deref())
        .unwrap_or("");
    ensure(
        !database.is_empty() && !database.contains('\0'),
        "PostgreSQL database is required",
    )?;
    if let Some(encoded_database) = identity.get(4) {
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded_database)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok());
        ensure(
            decoded.as_deref() == Some(database),
            "PostgreSQL source identity/database mismatch",
        )?;
    }
    for change in &tx.changes {
        ensure(
            change.database.as_deref() == Some(database),
            "PostgreSQL transaction spans databases",
        )?;
        ensure(
            lsn(&change.source_cursor)? == end,
            "PostgreSQL row cursor must equal its transaction end",
        )?;
        for (is_before, image) in [(true, &change.before), (false, &change.after)] {
            if let Some(image) = image {
                validate_image(change, is_before, image, version[0])?;
            }
        }
    }
    Ok(())
}

fn validate_image(
    change: &change_event::RowChange,
    is_before: bool,
    image: &[ColumnDatum],
    version: &str,
) -> Result<(), SourceContractError> {
    let Some(basis) = change.schema_basis.strip_prefix("pgoutput+catalog:") else {
        return Err(SourceContractError::new("invalid PostgreSQL schema basis"));
    };
    let (oid, digest) = basis
        .split_once(':')
        .map_or((basis, None), |(oid, digest)| (oid, Some(digest)));
    ensure(
        oid.parse::<u32>().is_ok_and(|value| value > 0)
            && digest.is_none_or(|value| {
                value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            }),
        "invalid PostgreSQL schema basis",
    )?;
    ensure(
        image
            .iter()
            .any(|column| column.primary_key_ordinal.is_some()),
        "PostgreSQL capture requires a primary key",
    )?;
    ensure(
        image
            .iter()
            .enumerate()
            .all(|(ordinal, column)| column.ordinal == ordinal),
        "PostgreSQL image ordinals must be contiguous",
    )?;
    for column in image {
        ensure(
            !column.generated,
            "generated PostgreSQL columns are unsupported",
        )?;
        validate_presence(change.operation, is_before, column)?;
        match &column.datum {
            Datum::SourceRepresentationEnvelope(envelope) => {
                validate_representation(
                    change,
                    column,
                    envelope,
                    &change.source_cursor,
                    oid,
                    digest,
                    version,
                )?;
            }
            Datum::Value(value) => {
                validate_native_type_for_version(version, &column.native_type)?;
                if !logical_matches_native(value, &column.native_type, version) {
                    return Err(SourceContractError::new(format!(
                        "PostgreSQL column '{}' declared as '{}' does not match its logical value type",
                        column.name, column.native_type
                    )));
                }
            }
            _ => validate_native_type_for_version(version, &column.native_type)?,
        }
    }
    Ok(())
}

fn validate_presence(
    operation: Operation,
    is_before: bool,
    column: &ColumnDatum,
) -> Result<(), SourceContractError> {
    match column.datum {
        Datum::Unavailable => ensure(
            is_before
                && !matches!(operation, Operation::Insert)
                && column.primary_key_ordinal.is_none(),
            "unavailable value is only valid in non-key before columns",
        ),
        Datum::Unchanged => ensure(
            !is_before
                && matches!(operation, Operation::Update)
                && column.primary_key_ordinal.is_none(),
            "unchanged value is only valid in non-key UPDATE after columns",
        ),
        _ => Ok(()),
    }
}

fn validate_native_type_for_version(
    version: &str,
    native_type: &str,
) -> Result<(), SourceContractError> {
    let native = native_type.trim().to_ascii_lowercase();
    let supported = matches!(
        native.as_str(),
        "boolean"
            | "uuid"
            | "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "text"
            | "bytea"
            | "date"
            | "time"
            | "time without time zone"
            | "time with time zone"
            | "timetz"
            | "interval"
            | "inet"
            | "cidr"
            | "macaddr"
            | "macaddr8"
            | "xml"
            | "name"
            | "\"char\""
            | "money"
            | "point"
            | "line"
            | "lseg"
            | "box"
            | "path"
            | "polygon"
            | "circle"
            | "tsvector"
            | "tsquery"
            | "oidvector"
            | "int2vector"
            | "tid"
            | "oid"
            | "xid"
            | "xid8"
            | "cid"
            | "pg_lsn"
            | "pg_snapshot"
            | "txid_snapshot"
            | "regproc"
            | "regprocedure"
            | "regoper"
            | "regoperator"
            | "regclass"
            | "regtype"
            | "regconfig"
            | "regdictionary"
            | "regnamespace"
            | "regrole"
            | "regcollation"
            | "int4range"
            | "int8range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
            | "int4multirange"
            | "int8multirange"
            | "nummultirange"
            | "tsmultirange"
            | "tstzmultirange"
            | "datemultirange"
            | "jsonb"
            | "json"
            | "timestamp without time zone"
            | "timestamp with time zone"
    ) || native.ends_with("[]")
        || is_qualified_type_name(&native)
        || matches!(native.as_str(), "hstore" | "geometry" | "geography")
        || valid_character(&native)
        || valid_parameterized_numeric(&native)
        || valid_parameterized_timestamp(&native)
        || valid_parameterized_time(&native)
        || valid_parameterized_bit(&native)
        || (native.starts_with("interval")
            && crate::type_mapping::validate_native_type_for_version(version, native_type).is_ok())
        || (native.starts_with("enum(")
            && crate::type_mapping::validate_native_type_for_version(version, native_type).is_ok());
    ensure(supported, "unsupported PostgreSQL native type")
}

fn validate_representation(
    change: &change_event::RowChange,
    column: &ColumnDatum,
    envelope: &change_event::SourceRepresentationEnvelope,
    cursor: &SourceCursor,
    relation_oid: &str,
    catalog_digest: Option<&str>,
    version: &str,
) -> Result<(), SourceContractError> {
    let context = &envelope.context;
    let metadata = &context.type_metadata;
    let type_oid = metadata
        .get("column_oid")
        .and_then(|value| value.parse::<u32>().ok());
    let type_schema = metadata.get("type_schema").map(String::as_str);
    let type_name = metadata.get("type_name").map(String::as_str);
    let expected_identity = type_oid
        .zip(type_schema.zip(type_name))
        .map(|(oid, (schema, name))| format!("postgresql.pg_type.v1:{oid}:{schema}.{name}"));
    let definition = metadata
        .get("type_definition")
        .and_then(|definition| serde_json::from_str::<serde_json::Value>(definition).ok());
    let definition_digest_matches = definition
        .as_ref()
        .and_then(|definition| definition.get("definition_digest"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|digest| format!("sha256:{digest}") == context.source_type_definition_digest);
    let definition_identity_matches = definition.as_ref().is_some_and(|definition| {
        definition.get("oid").and_then(serde_json::Value::as_u64) == type_oid.map(u64::from)
            && definition.get("schema").and_then(serde_json::Value::as_str) == type_schema
            && definition.get("name").and_then(serde_json::Value::as_str) == type_name
    });
    let type_definition_closure = metadata.get("type_definition_closure").and_then(|closure| {
        serde_json::from_str::<crate::type_mapping::SourceTypeDefinitionClosure>(closure).ok()
    });
    let closure_digest_matches = type_definition_closure.as_ref().is_some_and(|closure| {
        metadata
            .get("type_definition_closure_digest")
            .map(String::as_str)
            == Some(format!("sha256:{}", closure.digest()).as_str())
    });
    let closure_root_matches =
        type_definition_closure
            .as_ref()
            .zip(type_oid)
            .is_some_and(|(closure, root_oid)| {
                closure
                    .types
                    .iter()
                    .find(|definition| definition.oid == root_oid)
                    .and_then(|definition| serde_json::to_value(definition).ok())
                    == definition.clone()
            });
    let closure_valid =
        type_definition_closure
            .as_ref()
            .zip(type_oid)
            .is_some_and(|(closure, root_oid)| {
                crate::type_mapping::validate_type_definition_closure(closure, root_oid)
            });
    ensure(
        context.connector.kind == "postgresql"
            && context.connector.version == version
            && context.server_build.product == "postgresql"
            && context
                .server_build
                .version
                .starts_with(&format!("{version}."))
            && context.protocol == "pgoutput.v1"
            && context.format == SourceRepresentationFormat::Text
            && envelope.encoding == "UTF-8"
            && context.source_cursor == *cursor
            && metadata.get("relation_oid").map(String::as_str) == Some(relation_oid)
            && metadata.get("relation_schema").map(String::as_str) == Some(change.schema.as_str())
            && metadata.get("relation_name").map(String::as_str) == Some(change.table.as_str())
            && metadata.get("column_name").map(String::as_str) == Some(column.name.as_str())
            && type_schema.is_some_and(|schema| !schema.is_empty())
            && type_name.is_some_and(|name| !name.is_empty())
            && type_oid.is_some_and(|oid| oid > 0)
            && metadata
                .get("type_modifier")
                .is_some_and(|modifier| modifier.parse::<i32>().is_ok())
            && metadata.get("native_type").map(String::as_str) == Some(column.native_type.as_str())
            && metadata.get("type_definition_digest").map(String::as_str)
                == Some(context.source_type_definition_digest.as_str())
            && metadata.get("source_catalog_digest").is_some_and(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            && metadata.get("source_catalog_digest").map(String::as_str) == catalog_digest
            && metadata.get("text_output_profile").map(String::as_str)
                == Some("pgoutput-v1/text/UTF8")
            && metadata.get("session.search_path").map(String::as_str) == Some("pg_catalog")
            && metadata
                .get("environment.lc_monetary")
                .is_some_and(|locale| !locale.is_empty())
            && definition_digest_matches
            && definition_identity_matches
            && closure_digest_matches
            && closure_root_matches
            && closure_valid
            && expected_identity.as_deref() == Some(context.source_type_identity.as_str()),
        "PostgreSQL source representation context does not match its checked type definition closure",
    )
}

fn valid_character(native: &str) -> bool {
    if matches!(native, "character" | "character varying") {
        return true;
    }
    let rest = native
        .strip_prefix("character(")
        .or_else(|| native.strip_prefix("character varying("));
    rest.and_then(|value| value.strip_suffix(')'))
        .is_some_and(|length| length.parse::<usize>().is_ok_and(|value| value > 0))
}

fn valid_parameterized_numeric(native: &str) -> bool {
    if matches!(native, "numeric" | "decimal") {
        return true;
    }
    let Some(parameters) = native
        .strip_prefix("numeric(")
        .or_else(|| native.strip_prefix("decimal("))
        .and_then(|value| value.strip_suffix(')'))
    else {
        return false;
    };
    let mut parts = parameters.split(',');
    let Ok(precision) = parts.next().unwrap_or_default().trim().parse::<usize>() else {
        return false;
    };
    let Ok(scale) = parts.next().unwrap_or("0").trim().parse::<i32>() else {
        return false;
    };
    parts.next().is_none() && precision > 0 && precision <= 1000 && (-1000..=1000).contains(&scale)
}

fn valid_parameterized_timestamp(native: &str) -> bool {
    let Some(rest) = native.strip_prefix("timestamp(") else {
        return false;
    };
    let Some((precision, suffix)) = rest.split_once(") ") else {
        return false;
    };
    precision.parse::<u8>().is_ok_and(|value| value <= 6)
        && matches!(suffix, "without time zone" | "with time zone")
}

fn is_qualified_type_name(native: &str) -> bool {
    if native.is_empty() || native.contains([';', '\0', '[', ']', '(', ')']) {
        return false;
    }
    let mut quoted = false;
    let mut dots = 0_u8;
    let mut previous = None;
    for byte in native.bytes() {
        if quoted {
            if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if byte == b'.' {
            dots = dots.saturating_add(1);
            if matches!(previous, None | Some(b'.')) {
                return false;
            }
        } else if byte.is_ascii_whitespace() {
            return false;
        }
        previous = Some(byte);
    }
    !quoted && dots == 1 && !matches!(previous, None | Some(b'.'))
}

fn logical_matches_native(value: &LogicalValue, native_type: &str, version: &str) -> bool {
    let native_raw = native_type.trim();
    let native = native_raw.to_ascii_lowercase();
    match value {
        LogicalValue::Boolean { .. } => native == "boolean",
        LogicalValue::Uuid { .. } => native == "uuid",
        LogicalValue::Integer {
            signed: true,
            bits: 16,
            ..
        } => native == "smallint",
        LogicalValue::Integer {
            signed: true,
            bits: 32,
            ..
        } => native == "integer",
        LogicalValue::Integer {
            signed: true,
            bits: 64,
            ..
        } => native == "bigint",
        LogicalValue::Integer {
            signed: false,
            bits: 32,
            ..
        } => matches!(native.as_str(), "oid" | "xid" | "cid"),
        LogicalValue::Integer {
            signed: false,
            bits: 64,
            ..
        } => native == "xid8",
        LogicalValue::Decimal { unscaled, .. } => {
            if !valid_parameterized_numeric(&native) {
                false
            } else if matches!(unscaled.as_str(), "Infinity" | "-Infinity") {
                matches!(native.as_str(), "numeric" | "decimal")
            } else {
                true
            }
        }
        LogicalValue::Float { bits: 32, .. } => native == "real",
        LogicalValue::Float { bits: 64, .. } => native == "double precision",
        LogicalValue::Text { charset, .. } => {
            charset == "UTF8"
                && (native == "text"
                    || native == "name"
                    || native == "\"char\""
                    || valid_character(&native))
        }
        LogicalValue::Binary { .. } => native == "bytea",
        LogicalValue::Date { .. } => native == "date",
        LogicalValue::TemporalInfinity { kind, .. } => match kind {
            change_event::TemporalInfinityKind::Date => native == "date",
            change_event::TemporalInfinityKind::LocalDatetime => {
                native == "timestamp without time zone"
                    || valid_parameterized_timestamp(&native)
                        && native.ends_with("without time zone")
            }
            change_event::TemporalInfinityKind::Instant => {
                native == "timestamp with time zone"
                    || valid_parameterized_timestamp(&native) && native.ends_with("with time zone")
            }
            change_event::TemporalInfinityKind::CalendarInterval => {
                version.parse::<u16>().is_ok_and(|major| major >= 17)
                    && native.starts_with("interval")
            }
        },
        LogicalValue::LocalTime { .. } => {
            native == "time"
                || native == "time without time zone"
                || valid_parameterized_time(&native)
        }
        LogicalValue::OffsetTime { .. } => {
            native == "time with time zone"
                || native == "timetz"
                || valid_parameterized_time(&native) && native.ends_with("with time zone")
        }
        LogicalValue::LocalDatetime { .. } => {
            native.ends_with("without time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::Instant { .. } => {
            native.ends_with("with time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::CalendarInterval { .. } => native.starts_with("interval"),
        LogicalValue::Json { .. } => native == "jsonb" || native == "json",
        LogicalValue::BitString { .. } => native == "bit" || valid_parameterized_bit(&native),
        LogicalValue::Network {
            family,
            address,
            prefix_length,
        } => valid_network(&native, family, address, *prefix_length),
        LogicalValue::Xml { .. } => native == "xml",
        LogicalValue::Enum { label } => {
            if !native.starts_with("enum(") || label.is_empty() {
                false
            } else {
                crate::type_mapping::source_type_mapping_for_version(version, native_raw)
                    .ok()
                    .and_then(|mapping| match mapping.logical_type {
                        change_event::LogicalType::Enum { members } => Some(members),
                        _ => None,
                    })
                    .is_some_and(|members| members.iter().any(|member| member == label))
            }
        }
        LogicalValue::Domain { value } => {
            is_qualified_type_name(&native) && !matches!(value.as_ref(), LogicalValue::Null)
        }
        LogicalValue::Struct { fields } => {
            (native == "record" || is_qualified_type_name(&native))
                && fields.iter().enumerate().all(|(index, field)| {
                    !field.name.is_empty()
                        && !fields[..index].iter().any(|prior| prior.name == field.name)
                })
        }
        LogicalValue::Array { .. } | LogicalValue::ArrayWithMetadata { .. } => {
            native.ends_with("[]")
        }
        LogicalValue::Range { .. } => {
            matches!(
                native.as_str(),
                "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange"
            ) || is_qualified_type_name(&native)
        }
        LogicalValue::MultiRange { .. } => {
            matches!(
                native.as_str(),
                "int4multirange"
                    | "int8multirange"
                    | "nummultirange"
                    | "tsmultirange"
                    | "tstzmultirange"
                    | "datemultirange"
            ) || is_qualified_type_name(&native)
        }
        LogicalValue::Map { .. } => native == "hstore" || native.ends_with(".hstore"),
        LogicalValue::Spatial {
            geometry_type,
            dimensions,
            ..
        } => {
            (native == "geometry"
                || native == "geography"
                || native.starts_with("geometry(")
                || native.starts_with("geography(")
                || native.ends_with(".geometry")
                || native.ends_with(".geography"))
                && !geometry_type.is_empty()
                && (2..=4).contains(dimensions)
        }
        LogicalValue::Duration { .. } | LogicalValue::Year { .. } => false,
        _ => false,
    }
}

fn valid_network(native: &str, family: &str, address: &str, prefix_length: Option<u8>) -> bool {
    match native {
        "inet" | "cidr" => {
            let Ok(parsed) = address.parse::<std::net::IpAddr>() else {
                return false;
            };
            let is_v6 = parsed.is_ipv6();
            if (family == "ipv6") != is_v6 || (family != "ipv6" && family != "ipv4") {
                return false;
            }
            prefix_length.is_none_or(|prefix| prefix <= if is_v6 { 128 } else { 32 })
        }
        "macaddr" | "macaddr8" => {
            let expected = if native == "macaddr" { 6 } else { 8 };
            family == native
                && address.split(':').count() == expected
                && address.split(':').all(|part| {
                    part.len() == 2 && part.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                && prefix_length.is_none()
        }
        _ => false,
    }
}

fn valid_parameterized_bit(native: &str) -> bool {
    ["bit(", "bit varying(", "varbit("]
        .iter()
        .find_map(|prefix| native.strip_prefix(prefix))
        .and_then(|rest| rest.strip_suffix(')'))
        .is_some_and(|length| length.parse::<u64>().is_ok_and(|value| value > 0))
}

fn valid_parameterized_time(native: &str) -> bool {
    let Some(rest) = native.strip_prefix("time(") else {
        return false;
    };
    let Some((precision, suffix)) = rest.split_once(")") else {
        return false;
    };
    precision.trim().parse::<u8>().is_ok_and(|value| value <= 6)
        && matches!(suffix.trim(), "" | "without time zone" | "with time zone")
}

fn lsn(cursor: &SourceCursor) -> Result<u64, SourceContractError> {
    ensure(
        cursor.format == "postgresql.lsn.v1" && cursor.display == cursor.value,
        "invalid PostgreSQL cursor format or display",
    )?;
    let (high, low) = cursor
        .value
        .split_once('/')
        .ok_or_else(|| SourceContractError::new("invalid LSN"))?;
    ensure(
        [high, low].iter().all(|part| {
            !part.is_empty() && part.len() <= 8 && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        }),
        "invalid PostgreSQL LSN",
    )?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|_| SourceContractError::new("invalid PostgreSQL LSN"))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|_| SourceContractError::new("invalid PostgreSQL LSN"))?;
    let value = (high << 32) | low;
    ensure(value > 0, "zero PostgreSQL event LSN")?;
    Ok(value)
}

fn ensure(condition: bool, message: &str) -> Result<(), SourceContractError> {
    condition
        .then_some(())
        .ok_or_else(|| SourceContractError::new(message))
}

#[cfg(test)]
mod tests {
    use super::{logical_matches_native, validate_native_type_for_version};
    use change_event::{LogicalValue, ServerBuildIdentity};

    #[test]
    fn numeric_specials_follow_postgresql_typmod_rules() {
        let manifest = crate::target_capability_manifest(ServerBuildIdentity::new(
            "PostgreSQL",
            "community",
            "17.0",
            "test-build",
        ));
        let decimal_capabilities: Vec<_> = manifest
            .capabilities
            .iter()
            .filter(|entry| {
                entry
                    .target
                    .parameters
                    .get("range_kind")
                    .map(String::as_str)
                    == Some("decimal")
            })
            .collect();
        assert!(!decimal_capabilities.is_empty());
        assert!(decimal_capabilities.iter().all(|entry| {
            entry
                .target
                .parameters
                .get("decimal_special_values")
                .map(String::as_str)
                == Some("NaN")
        }));
        let exact_decimal_capabilities: Vec<_> = manifest
            .capabilities
            .iter()
            .filter(|entry| entry.code.starts_with("postgresql15.exact.decimal."))
            .collect();
        assert!(!exact_decimal_capabilities.is_empty());
        assert!(exact_decimal_capabilities.iter().all(|entry| {
            entry
                .target
                .parameters
                .get("decimal_special_values")
                .map(String::as_str)
                == Some("NaN")
        }));
        for (unscaled, expected_unconstrained, expected_bounded) in [
            ("NaN", true, true),
            ("Infinity", true, false),
            ("-Infinity", true, false),
        ] {
            let value = LogicalValue::Decimal {
                unscaled: unscaled.into(),
                scale: 0,
            };
            assert_eq!(
                logical_matches_native(&value, "numeric", "17"),
                expected_unconstrained,
                "unconstrained numeric: {unscaled}"
            );
            assert_eq!(
                logical_matches_native(&value, "numeric(10,0)", "17"),
                expected_bounded,
                "bounded numeric: {unscaled}"
            );
        }
    }

    #[test]
    fn source_contract_accepts_new_temporal_native_types() {
        for native_type in [
            "interval",
            "interval day to second(3)",
            "time with time zone",
            "timetz",
            "time(3) with time zone",
        ] {
            assert!(
                validate_native_type_for_version("15", native_type).is_ok(),
                "{native_type}"
            );
        }
        assert!(validate_native_type_for_version("15", "interval nonsense").is_err());
        assert!(logical_matches_native(
            &LogicalValue::CalendarInterval {
                months: 1,
                days: -2,
                microseconds: 3,
            },
            "interval day to second(3)",
            "15"
        ));
        assert!(logical_matches_native(
            &LogicalValue::OffsetTime {
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 0,
                offset_seconds: 4,
            },
            "timetz",
            "15"
        ));
        let interval_infinity = LogicalValue::TemporalInfinity {
            kind: change_event::TemporalInfinityKind::CalendarInterval,
            negative: false,
        };
        assert!(!logical_matches_native(
            &interval_infinity,
            "interval",
            "15"
        ));
        assert!(!logical_matches_native(
            &interval_infinity,
            "interval",
            "16"
        ));
        assert!(logical_matches_native(&interval_infinity, "interval", "17"));
    }
}
