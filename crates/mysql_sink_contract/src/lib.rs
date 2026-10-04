//! Shared MySQL target-catalog probing for the versioned SinkAdapters.
use std::{collections::BTreeMap, io};

use change_event::{
    CapabilityProbeEntry, CapabilityProbeStatus, ServerBuildIdentity, TargetCapabilityManifest,
    TargetCapabilityProbe, TargetColumnMetadata, TargetSessionProfile,
};
use mysql_driver::{Conn, prelude::Queryable};

type TargetColumnProbeRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<u64>,
    Option<i64>,
    Option<u64>,
    Option<u64>,
    String,
    String,
);

type TargetColumnCatalogRecord = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<u64>,
    Option<i64>,
    Option<u64>,
    Option<u64>,
    String,
    String,
);

/// Read a target table's shared metadata once and its columns and indexes in
/// two set queries, then build stable per-column probes. This keeps create and
/// start preflight bounded by tables rather than by the number of mapped
/// fields.
pub fn probe_targets_with_connection(
    conn: &mut Conn,
    adapter_version: &str,
    manifest_for_target: impl FnOnce(ServerBuildIdentity) -> TargetCapabilityManifest,
    schema: &str,
    table: &str,
    columns: &[String],
) -> io::Result<BTreeMap<String, TargetCapabilityProbe>> {
    if columns.is_empty() {
        return Ok(BTreeMap::new());
    }

    let (version, version_comment, time_zone, sql_mode): (String, String, String, String) = conn
        .query_first("SELECT VERSION(), @@version_comment, @@time_zone, @@sql_mode")
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("target returned no server identity"))?;
    let engine: Option<String> = conn
        .exec_first(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (schema, table),
        )
        .map_err(io::Error::other)?;
    let engine = engine.ok_or_else(|| capability_failure("target table was not found"))?;
    let triggers: u64 = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?",
            (schema, table),
        )
        .map_err(io::Error::other)?
        .unwrap_or(0);
    let foreign_keys: u64 = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE (TABLE_SCHEMA=? AND TABLE_NAME=? AND REFERENCED_TABLE_NAME IS NOT NULL) OR (REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)",
            (schema, table, schema, table),
        )
        .map_err(io::Error::other)?
        .unwrap_or(0);

    let all_columns: BTreeMap<String, TargetColumnProbeRow> = conn
        .exec_map(
            "SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, IS_NULLABLE, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (schema, table),
            |(
                column,
                native_type,
                data_type,
                charset,
                collation,
                precision,
                scale,
                length,
                temporal_precision,
                is_nullable,
                extra,
            ): TargetColumnCatalogRecord| {
                (
                    column,
                    (
                        native_type,
                        data_type,
                        charset,
                        collation,
                        precision,
                        scale,
                        length,
                        temporal_precision,
                        is_nullable,
                        extra,
                    ),
                )
            },
        )
        .map_err(io::Error::other)?
        .into_iter()
        .collect();
    let mut indexes_by_column = BTreeMap::<String, Vec<String>>::new();
    for (column, index) in conn
        .exec_map(
            "SELECT DISTINCT COLUMN_NAME, INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY COLUMN_NAME, INDEX_NAME",
            (schema, table),
            |(column, index): (String, String)| (column, index),
        )
        .map_err(io::Error::other)?
    {
        indexes_by_column.entry(column).or_default().push(index);
    }

    let target_build = ServerBuildIdentity::new(
        "mysql",
        if version_comment.trim().is_empty() {
            "oracle"
        } else {
            version_comment.trim()
        },
        version.clone(),
        format!("mysql-{version}"),
    );
    let manifest = manifest_for_target(target_build.clone());
    let target_columns = columns.iter().collect::<std::collections::BTreeSet<_>>();
    let mut probes = BTreeMap::new();
    for column in target_columns {
        let Some((
            native_type,
            data_type,
            charset,
            collation,
            precision,
            scale,
            length,
            temporal_precision,
            is_nullable,
            extra,
        )) = all_columns.get(column).cloned()
        else {
            return Err(capability_failure("target table or column was not found"));
        };
        let indexes = indexes_by_column.remove(column).unwrap_or_default();
        let is_spatial_target = matches!(
            data_type.to_ascii_lowercase().as_str(),
            "geometry"
                | "point"
                | "linestring"
                | "polygon"
                | "multipoint"
                | "multilinestring"
                | "multipolygon"
                | "geometrycollection"
                | "geomcollection"
        );
        let qualified = engine.eq_ignore_ascii_case("InnoDB") && triggers == 0 && foreign_keys == 0;
        let mut capabilities = manifest
            .capabilities
            .iter()
            .filter(|capability| {
                capability
                    .target
                    .native_type
                    .eq_ignore_ascii_case(&native_type)
                    || (capability
                        .target
                        .parameters
                        .get("target_storage")
                        .map(String::as_str)
                        == Some("mysql_geometry")
                        && is_spatial_target)
                    || (matches!(capability.target.native_type.as_str(), "enum" | "set")
                        && data_type.eq_ignore_ascii_case(&capability.target.native_type)
                        && native_type
                            .to_ascii_lowercase()
                            .starts_with(&data_type.to_ascii_lowercase()))
            })
            .map(|capability| {
                let status = if qualified {
                    CapabilityProbeStatus::Qualified
                } else {
                    CapabilityProbeStatus::Missing
                };
                CapabilityProbeEntry::new(capability.code.clone(), status)
                    .with_version(version.clone())
                    .with_evidence_digest(change_event::stable_digest(&(
                        &native_type,
                        &engine,
                        triggers,
                        foreign_keys,
                    )))
            })
            .collect::<Vec<_>>();
        if capabilities.is_empty() {
            capabilities.push(
                CapabilityProbeEntry::new(
                    format!("target_type:{}", data_type.to_ascii_lowercase()),
                    CapabilityProbeStatus::Detected,
                )
                .with_version(version.clone())
                .with_evidence_digest(change_event::stable_digest(&(
                    &native_type,
                    &engine,
                    triggers,
                    foreign_keys,
                ))),
            );
        }
        let constraints = [
            (!qualified && !engine.eq_ignore_ascii_case("InnoDB")).then_some("engine:not_innodb"),
            (triggers != 0).then_some("triggers:present"),
            (foreign_keys != 0).then_some("foreign_keys:present"),
            (is_nullable.eq_ignore_ascii_case("NO")).then_some("not_null"),
            (!extra.trim().is_empty()).then_some("extra_definition"),
        ]
        .into_iter()
        .flatten()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        let definition_fingerprint = change_event::stable_digest(&(
            (
                schema,
                table,
                column,
                &native_type,
                &data_type,
                &charset,
                &collation,
            ),
            (
                precision,
                scale,
                length,
                temporal_precision,
                &is_nullable,
                &extra,
                &engine,
                triggers,
                foreign_keys,
                &indexes,
            ),
        ));
        let mut metadata = TargetColumnMetadata::new(definition_fingerprint)
            .with_native_type(native_type)
            .with_constraints(constraints)
            .with_indexes(indexes);
        if let Some(precision) = precision.and_then(|value| u32::try_from(value).ok()) {
            metadata = metadata.with_precision(precision);
        }
        if let Some(scale) = scale.and_then(|value| i32::try_from(value).ok()) {
            metadata = metadata.with_scale(scale);
        }
        if let Some(length) = length {
            metadata = metadata.with_length(length);
        }
        if let Some(charset) = charset {
            metadata = metadata.with_charset(charset);
        }
        if let Some(collation) = collation {
            metadata = metadata.with_collation(collation);
        }
        let session = TargetSessionProfile::new(
            format!("mysql-{adapter_version};version={version}"),
            [
                ("time_zone", time_zone.clone()),
                ("sql_mode", sql_mode.clone()),
            ],
        );
        probes.insert(
            column.clone(),
            TargetCapabilityProbe::new(
                target_build.clone(),
                schema,
                table,
                column,
                metadata,
                capabilities,
                Vec::<CapabilityProbeEntry>::new(),
                session,
            ),
        );
    }
    Ok(probes)
}

/// Convenience entry point for a single mapped column.
pub fn probe_target_with_connection(
    conn: &mut Conn,
    adapter_version: &str,
    manifest_for_target: impl FnOnce(ServerBuildIdentity) -> TargetCapabilityManifest,
    schema: &str,
    table: &str,
    column: &str,
) -> io::Result<TargetCapabilityProbe> {
    let column_name = column.to_owned();
    probe_targets_with_connection(
        conn,
        adapter_version,
        manifest_for_target,
        schema,
        table,
        std::slice::from_ref(&column_name),
    )?
    .remove(column)
    .ok_or_else(|| io::Error::other("target column probe returned no result"))
}

fn capability_failure(message: impl Into<String>) -> io::Error {
    io::Error::other(change_event::TargetCapabilityFailure::new(message))
}
