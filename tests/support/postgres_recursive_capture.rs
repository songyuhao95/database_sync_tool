//! Live recursive PostgreSQL type capture qualification.
//! Uses isolated schema, publication, and slot names on authorized PG15/16/17 instances.

use change_event::{Datum, LogicalValue, Operation, ServerBuildIdentity};
use postgresql_15::{CancellationToken, Config};
use sqlx::{Connection, PgConnection};
use std::{collections::BTreeSet, env, time::Duration};

use super::{postgres_env, type_qualification_evidence};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn catalog_non_storable_reason(
    catalog: &postgresql_15::SourceTypeCatalog,
    root_oid: u32,
) -> Option<String> {
    fn visit(
        catalog: &postgresql_15::SourceTypeCatalog,
        oid: u32,
        visiting: &mut BTreeSet<u32>,
    ) -> Option<String> {
        if !visiting.insert(oid) {
            return None;
        }
        let Some(definition) = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
        else {
            visiting.remove(&oid);
            return Some(format!("references missing PostgreSQL type OID {oid}"));
        };
        let reason = match &definition.kind {
            postgresql_15::SourceTypeDefinitionKind::Pseudo => Some(format!(
                "depends on non-storable PostgreSQL pseudotype {}.{}",
                definition.schema, definition.name
            )),
            postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. } => {
                visit(catalog, *base_oid, visiting).map(|reason| {
                    format!("domain {}.{} {reason}", definition.schema, definition.name)
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Composite { fields } => {
                fields.iter().find_map(|field| {
                    visit(catalog, field.type_oid, visiting).map(|reason| {
                        format!(
                            "composite {}.{} field {} {reason}",
                            definition.schema, definition.name, field.name
                        )
                    })
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } => {
                let element_is_pseudo = catalog.types.iter().any(|element| {
                    element.oid == *element_oid
                        && matches!(
                            element.kind,
                            postgresql_15::SourceTypeDefinitionKind::Pseudo
                        )
                });
                if element_is_pseudo {
                    Some(format!(
                        "array {}.{} has a non-storable pseudo-type element",
                        definition.schema, definition.name
                    ))
                } else {
                    visit(catalog, *element_oid, visiting)
                        .filter(|reason| reason.contains("non-storable PostgreSQL pseudotype"))
                        .map(|reason| {
                            format!(
                                "array {}.{} element {reason}",
                                definition.schema, definition.name
                            )
                        })
                }
            }
            postgresql_15::SourceTypeDefinitionKind::Range { subtype_oid } => {
                visit(catalog, *subtype_oid, visiting).map(|reason| {
                    format!("range {}.{} {reason}", definition.schema, definition.name)
                })
            }
            postgresql_15::SourceTypeDefinitionKind::MultiRange { range_oid } => {
                visit(catalog, *range_oid, visiting).map(|reason| {
                    format!(
                        "multirange {}.{} {reason}",
                        definition.schema, definition.name
                    )
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
                if definition.schema == "pg_catalog"
                    && matches!(
                        definition.name.as_str(),
                        "pg_brin_bloom_summary" | "pg_brin_minmax_multi_summary"
                    ) =>
            {
                Some(format!(
                    "PostgreSQL internal BRIN summary {}.{} has no input/receive routine for row DML",
                    definition.schema, definition.name
                ))
            }
            postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
            | postgresql_15::SourceTypeDefinitionKind::Enum { .. }
            | postgresql_15::SourceTypeDefinitionKind::Extension { .. } => None,
        };
        visiting.remove(&oid);
        reason
    }

    visit(catalog, root_oid, &mut BTreeSet::new())
}

#[allow(dead_code)]
pub(super) struct RecursiveCapture {
    pub(super) table_name: String,
    pub(super) transactions: Vec<change_event::ValidatedTransaction>,
    pub(super) catalog: postgresql_15::SourceTypeCatalog,
    pub(super) source_build: ServerBuildIdentity,
    pub(super) fields: Vec<(String, String)>,
    pub(super) dynamic_classes: Vec<String>,
}

pub(super) async fn capture_recursive_types(major: u16) -> TestResult<RecursiveCapture> {
    capture_recursive_types_with_postgis(major, false).await
}

async fn capture_recursive_types_with_postgis(
    major: u16,
    require_postgis: bool,
) -> TestResult<RecursiveCapture> {
    let version = major.to_string();
    let password = env::var(postgres_env::env_name(&version, "TEST_PASSWORD"))?;
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = format!("cdc_pg{major}_recursive_{tag}");
    let table_name = format!("events_{major}_{tag}");
    let publication = format!("cdc_pg{major}_recursive_pub_{tag}");
    let slot = format!("cdcpg{major}recursive{tag}");
    let admin_user = postgres_env::setting(&version, "ADMIN_USER", "postgres");
    let reader_user = postgres_env::setting(&version, "READER_USER", "postgresql_reader");
    let writer_user = postgres_env::setting(&version, "WRITER_USER", "postgresql_writer");
    let mut admin =
        PgConnection::connect_with(&postgres_env::options(&version, &admin_user, &password))
            .await?;
    let removed_fixtures = cleanup_stale_type_qualification_fixtures(&mut admin, major).await?;
    if removed_fixtures > 0 {
        eprintln!(
            "PostgreSQL {major} type qualification removed {removed_fixtures} interrupted test fixtures"
        );
    }
    let mut created_hstore = false;
    let mut created_postgis = false;
    let result = async {
        execute(&mut admin, format!("CREATE SCHEMA {schema}")).await?;
        execute(&mut admin, "CREATE SCHEMA IF NOT EXISTS \"CDC_test\"".into()).await?;
        let (hstore_schema, created) = ensure_hstore(&mut admin, &schema).await?;
        created_hstore = created;
        let (postgis_schema, created) = ensure_postgis(&mut admin).await?;
        created_postgis = created;
        if require_postgis && postgis_schema.is_none() {
            return Err(std::io::Error::other(format!(
                "PostgreSQL {major} PostGIS geometry/geography live qualification requires the PostGIS server extension package; pg_available_extensions has no postgis entry"
            ))
            .into());
        }
        eprintln!(
            "PostgreSQL {major} recursive qualification extensions: hstore={}, hstore_temporary_install={}, postgis={}, postgis_temporary_install={}",
            hstore_schema.is_some(),
            created_hstore,
            postgis_schema.is_some(),
            created_postgis
        );
        execute(
            &mut admin,
            format!("CREATE TYPE {schema}.mood AS ENUM ('calm','ready')"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE DOMAIN {schema}.positive_int AS integer CHECK (VALUE > 0)"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE TYPE {schema}.address AS (street text, unit integer, note text)"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE TYPE {schema}.person AS (name text, address {schema}.address, score {schema}.positive_int)"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE TYPE {schema}.intspan AS RANGE (subtype = integer)"),
        )
        .await?;
        let extension_columns = hstore_schema
            .as_ref()
            .map(|extension_schema| {
                format!(", attributes {}.hstore", quote_ident(extension_schema))
            })
            .unwrap_or_default()
            + &postgis_schema
                .as_ref()
                .map(|extension_schema| {
                    format!(
                        ", location {}.geometry, earth_location {}.geography",
                        quote_ident(extension_schema),
                        quote_ident(extension_schema)
                    )
                })
                .unwrap_or_default();
        execute(
            &mut admin,
            format!(
                "CREATE TABLE \"CDC_test\".{table_name} (
                    id bigint PRIMARY KEY,
                    mood {schema}.mood NOT NULL,
                    score {schema}.positive_int NOT NULL,
                    person {schema}.person NOT NULL,
                    people {schema}.person[] NOT NULL,
                    matrix integer[] NOT NULL,
                    span {schema}.intspan NOT NULL,
                    spans {schema}.intspan_multirange NOT NULL,
                    catalog_internal pg_catalog.pg_node_tree NOT NULL,
                    nullable_marker text
                    {extension_columns}
                )"
            ),
        )
        .await?;
        execute(
            &mut admin,
            format!("ALTER TABLE \"CDC_test\".{table_name} REPLICA IDENTITY FULL"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE PUBLICATION {publication} FOR TABLE \"CDC_test\".{table_name}"),
        )
        .await?;
        execute(
            &mut admin,
            format!(
                "GRANT USAGE ON SCHEMA \"CDC_test\", {schema} TO {reader_user}, {writer_user}"
            ),
        )
        .await?;
        execute(
            &mut admin,
            format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON \"CDC_test\".{table_name} TO {reader_user}, {writer_user}"
            ),
        )
        .await?;

        let mut config = Config::new(
            postgres_env::setting(&version, "HOST", "192.168.0.10"),
            postgres_env::setting(&version, "PORT", "54321").parse::<u16>()?,
            "CDC_test",
            &reader_user,
            &password,
            &publication,
            &slot,
        );
        config.create_slot = true;
        let initial = postgresql_15::replication_for_version(config.clone(), major).await?;
        let source_id = initial.source().id.clone();
        drop(initial);
        config.create_slot = false;
        config.expected_source_id = Some(source_id);

        let catalog = postgresql_15::source_type_catalog(&mut admin).await?;
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&mut admin)
            .await?;
        if server_version.split('.').next() != Some(version.as_str()) {
            return Err(std::io::Error::other(format!(
                "PostgreSQL source {major} endpoint returned unexpected server version {server_version}"
            ))
            .into());
        }

        // Exercise the mapper against every defined, non-pseudotype catalog
        // declaration, not only the handful of values used by the recursive
        // DML fixture. A type missing from the mapping is a real catalog gap.
        let catalog_type_rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT t.oid::bigint, pg_catalog.format_type(t.oid, NULL)
               FROM pg_catalog.pg_type t
               JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
              WHERE t.typisdefined AND t.typtype <> 'p'
                AND n.nspname NOT LIKE 'pg_toast%'
                AND n.nspname !~ '^cdc_pg(15|16|17)_recursive_[0-9]+$'
                AND NOT EXISTS (
                    SELECT 1
                      FROM pg_catalog.pg_class fixture
                      JOIN pg_catalog.pg_namespace fixture_ns
                        ON fixture_ns.oid=fixture.relnamespace
                      JOIN pg_catalog.pg_type fixture_type
                        ON fixture_type.oid=fixture.reltype
                     WHERE fixture_ns.nspname='CDC_test'
                       AND fixture.relname=$1
                       AND t.oid IN (fixture_type.oid, fixture_type.typarray)
                )
              ORDER BY t.oid",
        )
        .bind(&table_name)
        .fetch_all(&mut admin)
        .await?;
        let mut mapped_catalog_types = Vec::new();
        let mut excluded_pseudotype_array_count = 0;
        let mut catalog_mapping_gaps = Vec::new();
        for (oid, native_type) in catalog_type_rows {
            let oid = u32::try_from(oid)?;
            let Some(definition) = catalog.types.iter().find(|definition| definition.oid == oid)
            else {
                catalog_mapping_gaps.push(format!("OID {oid} {native_type}: absent from type snapshot"));
                continue;
            };
            if matches!(definition.kind, postgresql_15::SourceTypeDefinitionKind::Pseudo) {
                continue;
            }
            if let postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } = definition.kind
                && catalog.types.iter().any(|element| {
                    element.oid == element_oid
                        && matches!(element.kind, postgresql_15::SourceTypeDefinitionKind::Pseudo)
                })
            {
                excluded_pseudotype_array_count += 1;
                continue;
            }
            let qualified_type = catalog_type_declaration(&catalog, oid, &native_type)?;
            match postgresql_15::source_type_mapping_with_catalog_for_version(
                &version,
                &qualified_type,
                &catalog,
            ) {
                Ok(mapping) => {
                    mapped_catalog_types.push((
                        oid,
                        definition.schema.clone(),
                        definition.name.clone(),
                        native_type,
                        qualified_type,
                        definition.definition_digest.clone(),
                        mapping.mapping_id,
                        mapping.evidence_digest.unwrap_or_default(),
                        change_event::stable_digest(&mapping.logical_type),
                        if matches!(
                            &mapping.logical_type,
                            change_event::LogicalType::Raw { .. }
                        ) {
                            "SOURCE_REPRESENTATION"
                        } else {
                            "SEMANTIC_CODEC"
                        },
                        serde_json::to_string(&mapping.logical_type)?,
                    ));
                }
                Err(error) => catalog_mapping_gaps.push(format!("OID {oid} {native_type}: {error}")),
            }
        }
        if !catalog_mapping_gaps.is_empty() {
            return Err(std::io::Error::other(format!(
                "PostgreSQL {major} has {} unmapped defined catalog types: {}",
                catalog_mapping_gaps.len(),
                catalog_mapping_gaps
                    .iter()
                    .take(24)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; ")
            ))
            .into());
        }
        let brin_io_rows = sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT type.typname,
                    input_proc.proname,
                    output_proc.proname,
                    receive_proc.proname,
                    send_proc.proname
               FROM pg_catalog.pg_type type
               JOIN pg_catalog.pg_namespace namespace ON namespace.oid=type.typnamespace
               JOIN pg_catalog.pg_proc input_proc ON input_proc.oid=type.typinput
               JOIN pg_catalog.pg_proc output_proc ON output_proc.oid=type.typoutput
               JOIN pg_catalog.pg_proc receive_proc ON receive_proc.oid=type.typreceive
               JOIN pg_catalog.pg_proc send_proc ON send_proc.oid=type.typsend
              WHERE namespace.nspname='pg_catalog'
                AND type.typname IN ('pg_brin_bloom_summary','pg_brin_minmax_multi_summary')
              ORDER BY type.typname",
        )
        .fetch_all(&mut admin)
        .await?;
        let expected_brin_io = BTreeSet::from([
            (
                "pg_brin_bloom_summary",
                "brin_bloom_summary_in",
                "brin_bloom_summary_out",
                "brin_bloom_summary_recv",
                "brin_bloom_summary_send",
            ),
            (
                "pg_brin_minmax_multi_summary",
                "brin_minmax_multi_summary_in",
                "brin_minmax_multi_summary_out",
                "brin_minmax_multi_summary_recv",
                "brin_minmax_multi_summary_send",
            ),
        ]);
        let actual_brin_io = brin_io_rows
            .iter()
            .map(|(name, input, output, receive, send)| {
                (
                    name.as_str(),
                    input.as_str(),
                    output.as_str(),
                    receive.as_str(),
                    send.as_str(),
                )
            })
            .collect::<BTreeSet<_>>();
        if actual_brin_io != expected_brin_io {
            return Err(std::io::Error::other(format!(
                "PostgreSQL {major} BRIN internal type I/O catalog differs from the audited non-row-DML contract: {actual_brin_io:?}"
            ))
            .into());
        }
        let brin_io_evidence = brin_io_rows
            .into_iter()
            .map(|(name, input, output, receive, send)| {
                (
                    name,
                    serde_json::json!({
                        "input_function": input,
                        "output_function": output,
                        "receive_function": receive,
                        "send_function": send,
                        "row_dml_constructible": false,
                        "reason": "PostgreSQL core input and receive functions reject construction; BRIN summaries are internal index payloads, not user-row values"
                    }),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let catalog_type_digest = change_event::stable_digest(&mapped_catalog_types);
        let non_storable_type_count = mapped_catalog_types
            .iter()
            .filter(|(oid, ..)| catalog_non_storable_reason(&catalog, *oid).is_some())
            .count();
        let catalog_type_roster = mapped_catalog_types
            .iter()
            .map(
                |(
                    oid,
                    schema,
                    name,
                    _native_type,
                    qualified_type,
                    definition_digest,
                    mapping_id,
                    mapping_evidence_digest,
                    logical_type_digest,
                    representation_mode,
                    _logical_type,
                )| {
                    let definition = catalog
                        .types
                        .iter()
                        .find(|definition| definition.oid == *oid)
                        .expect("mapped catalog type must be present in the source snapshot");
                    let catalog_class_id = match &definition.kind {
                        postgresql_15::SourceTypeDefinitionKind::Array { .. } => {
                            "postgresql.arrays"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Enum { .. } => {
                            "postgresql.enums"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Domain { .. } => {
                            "postgresql.domains"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Composite { .. } => {
                            "postgresql.composites"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Range { .. }
                        | postgresql_15::SourceTypeDefinitionKind::MultiRange { .. } => {
                            "postgresql.ranges"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Extension { .. } => {
                            "postgresql.extensions_and_custom_base_types"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
                            if schema == "pg_catalog" =>
                        {
                            "postgresql.other_defined_catalog_types"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Builtin { .. } => {
                            "postgresql.user_defined_base_types"
                        }
                        postgresql_15::SourceTypeDefinitionKind::Pseudo => unreachable!(
                            "pseudotypes and arrays of pseudotypes are excluded from the roster"
                        ),
                    };
                    let non_storable_reason = catalog_non_storable_reason(&catalog, *oid);
                    serde_json::json!({
                        "type_id": format!("dynamic:postgresql.instance.{schema}.{name}:{definition_digest}"),
                        "catalog_class_id": catalog_class_id,
                        "schema": schema,
                        "name": name,
                        "type_oid": oid,
                        "native_declaration": qualified_type,
                        "definition_digest": definition_digest,
                        "mapping_id": mapping_id,
                        "mapping_evidence_digest": mapping_evidence_digest,
                        "logical_type_digest": logical_type_digest,
                        "representation_mode": representation_mode,
                        "user_storable": non_storable_reason.is_none(),
                        "non_storable_reason": non_storable_reason,
                        "row_dml_io_evidence": brin_io_evidence.get(name).cloned()
                    })
                },
            )
            .collect::<Vec<_>>();
        type_qualification_evidence::record_exact_catalog_type_mapping_evidence(
            type_qualification_evidence::ExactCatalogTypeMappingEvidence {
                connector_id: format!("postgresql_{major}"),
                suite_id: format!("postgresql_{major}.complete_defined_catalog_type_mapping"),
                catalog_type_count: mapped_catalog_types.len(),
                excluded_pseudotype_array_count,
                catalog_scope: "all mapped defined non-pseudotype PostgreSQL catalog types; pseudotype dependencies and PostgreSQL internal BRIN summary values are explicitly classified outside user-row DML; arrays with real, nullable element types remain in coverage; transient fixture types are counted separately".into(),
                catalog_digest: catalog_type_digest.clone(),
                type_roster: catalog_type_roster,
                non_storable_type_count,
            },
        )?;
        let server_version_text: String = sqlx::query_scalar("SELECT version()")
            .fetch_one(&mut admin)
            .await?;
        let source_build = ServerBuildIdentity::new(
            "postgresql",
            "community",
            server_version,
            server_version_text,
        );

        let mut writer = PgConnection::connect_with(&postgres_env::options(
            &version,
            &writer_user,
            &password,
        ))
        .await?;
        for setting in [
            "SET client_encoding='UTF8'",
            "SET DateStyle='ISO, YMD'",
            "SET IntervalStyle='iso_8601'",
            "SET TimeZone='UTC'",
            "SET bytea_output='hex'",
            "SET extra_float_digits=3",
            "SET search_path='pg_catalog'",
        ] {
            execute(&mut writer, setting.into()).await?;
        }
        let catalog_tree_text: Option<String> = sqlx::query_scalar(
            "SELECT ev_action::text FROM pg_catalog.pg_rewrite ORDER BY oid LIMIT 1",
        )
        .fetch_one(&mut writer)
        .await?;
        let catalog_tree_text = catalog_tree_text.ok_or_else(|| {
            std::io::Error::other("selected pg_catalog.pg_rewrite row has NULL ev_action")
        })?;
        let mut insert_columns = vec![
            "id",
            "mood",
            "score",
            "person",
            "people",
            "matrix",
            "span",
            "spans",
            "catalog_internal",
            "nullable_marker",
        ];
        let mut insert_values = vec![
            "1".to_owned(),
            "'ready'".to_owned(),
            "27".to_owned(),
            format!("ROW('Ada', ROW('Main Street', 7, NULL):: {schema}.address, 27):: {schema}.person"),
            format!("ARRAY[
                ROW('Grace', ROW('Compiler Road', 3, 'Apt 2')::{schema}.address, 31)::{schema}.person,
                NULL,
                ROW('Linus', ROW('Kernel Way', 9, NULL)::{schema}.address, 42)::{schema}.person
            ]::{schema}.person[]"),
            "'[0:1][2:3]={{1,NULL},{3,4}}'::integer[]".to_owned(),
            format!("'[1,8)'::{schema}.intspan"),
            format!("'{{[1,3),[5,8)}}'::{schema}.intspan_multirange"),
            "(SELECT ev_action FROM pg_catalog.pg_rewrite ORDER BY oid LIMIT 1)".to_owned(),
            "NULL".to_owned(),
        ];
        if let Some(extension_schema) = &hstore_schema {
            insert_columns.push("attributes");
            insert_values.push(format!(
                "'\"author\"=>\"Ada\", \"nullable\"=>NULL'::{}.hstore",
                quote_ident(extension_schema)
            ));
        }
        if let Some(extension_schema) = &postgis_schema {
            insert_columns.push("location");
            insert_values.push(format!(
                "{}.st_geomfromewkt('SRID=4326;POINT(1 2)')",
                quote_ident(extension_schema)
            ));
            insert_columns.push("earth_location");
            insert_values.push(format!(
                "'SRID=4326;POINT(3 4)'::{}.geography",
                quote_ident(extension_schema)
            ));
        }
        execute(
            &mut writer,
            format!(
                "INSERT INTO \"CDC_test\".{table_name} ({}) VALUES ({})",
                insert_columns.join(", "),
                insert_values.join(", ")
            ),
        )
        .await?;
        execute(
            &mut writer,
            format!(
                "UPDATE \"CDC_test\".{table_name} SET nullable_marker='now-present' WHERE id=1"
            ),
        )
        .await?;
        execute(
            &mut writer,
            format!("DELETE FROM \"CDC_test\".{table_name} WHERE id=1"),
        )
        .await?;

        let transactions = read_transactions(config, major, 3).await?;
        let insert = transactions[0].transaction();
        assert!(matches!(insert.changes[0].operation, Operation::Insert));
        let row = insert.changes[0].after.as_ref().expect("INSERT after image");
        let get = |name: &str| -> &Datum {
            &row.iter().find(|column| column.name == name).expect(name).datum
        };
        assert!(matches!(get("mood"), Datum::Value(LogicalValue::Enum { label }) if label == "ready"));
        assert!(matches!(get("score"), Datum::Value(LogicalValue::Domain { value }) if matches!(value.as_ref(), LogicalValue::Integer { signed: true, bits: 32, value } if value == "27")));
        assert!(matches!(get("person"), Datum::Value(LogicalValue::Struct { fields }) if fields.len() == 3));
        assert!(matches!(get("people"), Datum::Value(LogicalValue::Array { elements }) if elements.len() == 3 && matches!(elements[1], LogicalValue::Null)));
        assert!(matches!(get("matrix"), Datum::Value(LogicalValue::ArrayWithMetadata { elements, dimensions: 2, lower_bounds, dimension_lengths }) if elements.len() == 4 && lower_bounds == &[0, 2] && dimension_lengths == &[2, 2] && matches!(elements[1], LogicalValue::Null)));
        assert!(matches!(get("span"), Datum::Value(LogicalValue::Range { empty: false, lower: Some(lower), upper: Some(upper), lower_inclusive: true, upper_inclusive: false }) if matches!(lower.as_ref(), LogicalValue::Integer { value, .. } if value == "1") && matches!(upper.as_ref(), LogicalValue::Integer { value, .. } if value == "8")));
        assert!(matches!(get("spans"), Datum::Value(LogicalValue::MultiRange { ranges }) if ranges.len() == 2));
        match get("catalog_internal") {
            Datum::SourceRepresentationEnvelope(envelope) => {
                assert_eq!(envelope.raw_bytes()?, catalog_tree_text.as_bytes());
                assert!(envelope.context.type_metadata["native_type"].ends_with("pg_node_tree"));
            }
            other => return Err(std::io::Error::other(format!(
                "pg_catalog.pg_node_tree did not enter ChangeEvent as a source representation: {other:?}"
            )).into()),
        }
        assert!(matches!(get("nullable_marker"), Datum::Null));
        if hstore_schema.is_some() {
            assert!(matches!(get("attributes"), Datum::Value(LogicalValue::Map { entries }) if entries.len() == 2));
        }
        if postgis_schema.is_some() {
            assert!(matches!(get("location"), Datum::Value(LogicalValue::Spatial { geometry_type, dimensions: 2, srid: Some(4326), .. }) if geometry_type == "point"));
            assert!(matches!(get("earth_location"), Datum::Value(LogicalValue::Spatial { geometry_type, dimensions: 2, srid: Some(4326), .. }) if geometry_type == "point"));
        }

        let update = transactions[1].transaction().changes.first().expect("UPDATE row");
        assert!(matches!(update.operation, Operation::Update));
        assert!(matches!(update.before.as_ref().unwrap().iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Null));
        assert!(matches!(&update.after.as_ref().unwrap().iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Value(LogicalValue::Text { text: Some(value), .. }) if value == "now-present"));
        assert!(matches!(transactions[2].transaction().changes[0].operation, Operation::Delete));

        for transaction in &transactions {
            let encoded = change_event::json(transaction)?;
            let mut reader = change_event::JsonReader::new(std::io::Cursor::new(&encoded));
            let replay = reader.next_transaction()?.expect("ChangeEvent JSON transaction");
            assert_eq!(change_event::json(&replay)?, encoded);
            reader.finish()?;
        }
        let exact_dynamic_names = [
            "mood",
            "score",
            "person",
            "people",
            "matrix",
            "span",
            "spans",
            "catalog_internal",
        ]
        .into_iter()
        .chain(hstore_schema.is_some().then_some("attributes"))
        .chain(postgis_schema.is_some().then_some("location"))
        .chain(postgis_schema.is_some().then_some("earth_location"))
        .collect::<std::collections::BTreeSet<_>>();
        let exact_catalog_columns = sqlx::query_as::<_, (String, i64, String)>(
            "SELECT a.attname, a.atttypid::bigint, pg_catalog.format_type(a.atttypid, a.atttypmod)
               FROM pg_catalog.pg_attribute a
               JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
               JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
              WHERE n.nspname='CDC_test' AND c.relname=$1
                AND a.attnum > 0 AND NOT a.attisdropped",
        )
        .bind(&table_name)
        .fetch_all(&mut admin)
        .await?;
        let mut exact_source_type_ids = Vec::new();
        let mut exact_representation_type_ids = Vec::new();
        for (column_name, oid, native_type) in exact_catalog_columns {
            if !exact_dynamic_names.contains(column_name.as_str()) {
                continue;
            }
            let oid = u32::try_from(oid)?;
            let definition = catalog
                .types
                .iter()
                .find(|definition| definition.oid == oid)
                .ok_or_else(|| {
                    std::io::Error::other(format!(
                        "captured PostgreSQL column {column_name} has unmapped catalog OID {oid}"
                    ))
                })?;
            if !type_qualification_evidence::source_native_type_ids(
                &format!("postgresql_{major}"),
                [native_type.clone()],
            )
            .is_empty()
            {
                continue;
            }
            let mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
                &version,
                &native_type,
                &catalog,
            )?;
            let type_id = format!(
                "dynamic:postgresql.instance.{}.{}:{}",
                definition.schema, definition.name, definition.definition_digest
            );
            if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                exact_representation_type_ids.push(type_id.clone());
            }
            exact_source_type_ids.push(type_id);
        }
        type_qualification_evidence::record_source_type_evidence(
            &format!("postgresql_{major}"),
            &format!("postgresql_{major}.recursive_type_fixtures"),
            exact_source_type_ids,
            exact_representation_type_ids,
        )?;
        let mut dynamic_classes = vec![
            "postgresql.other_defined_catalog_types".to_owned(),
            "postgresql.arrays".to_owned(),
            "postgresql.domains".to_owned(),
            "postgresql.enums".to_owned(),
            "postgresql.composites".to_owned(),
            "postgresql.ranges".to_owned(),
        ];
        if hstore_schema.is_some() || postgis_schema.is_some() {
            dynamic_classes.push("postgresql.extensions_and_custom_base_types".to_owned());
        }
        if hstore_schema.is_some() {
            dynamic_classes.push("postgresql.user_defined_base_types".to_owned());
        }
        let fields = row
            .iter()
            .filter(|column| column.name != "id")
            .map(|column| (column.name.clone(), column.native_type.clone()))
            .collect();
        type_qualification_evidence::record_dynamic_type_class_evidence(
            &format!("postgresql_{major}"),
            &format!("postgresql_{major}.recursive_type_fixtures"),
            dynamic_classes.clone(),
        )?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(RecursiveCapture {
            table_name: table_name.clone(),
            transactions,
            catalog,
            source_build,
            fields,
            dynamic_classes,
        })
    }
    .await;

    let _ = sqlx::query(
        "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1 AND NOT active",
    )
    .bind(&slot)
    .execute(&mut admin)
    .await;
    let _ = execute(
        &mut admin,
        format!("DROP TABLE IF EXISTS \"CDC_test\".{table_name}"),
    )
    .await;
    if created_hstore {
        let _ = execute(&mut admin, "DROP EXTENSION IF EXISTS hstore".into()).await;
    }
    let _ = execute(
        &mut admin,
        format!("DROP PUBLICATION IF EXISTS {publication}"),
    )
    .await;
    let _ = execute(
        &mut admin,
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
    )
    .await;
    if created_postgis {
        let _ = execute(&mut admin, "DROP EXTENSION IF EXISTS postgis".into()).await;
    }
    result
}

async fn installed_extension_schema(
    connection: &mut PgConnection,
    name: &str,
) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname=$1",
    )
    .bind(name)
    .fetch_optional(&mut *connection)
    .await?)
}

async fn ensure_hstore(
    connection: &mut PgConnection,
    test_schema: &str,
) -> TestResult<(Option<String>, bool)> {
    if let Some(schema) = installed_extension_schema(connection, "hstore").await? {
        return Ok((Some(schema), false));
    }
    let available = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='hstore')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !available {
        return Ok((None, false));
    }
    match execute(
        connection,
        format!(
            "CREATE EXTENSION hstore SCHEMA {}",
            quote_ident(test_schema)
        ),
    )
    .await
    {
        Ok(()) => Ok((Some(test_schema.to_owned()), true)),
        Err(error) => {
            if let Some(schema) = installed_extension_schema(connection, "hstore").await? {
                Ok((Some(schema), false))
            } else {
                Err(error)
            }
        }
    }
}

async fn ensure_postgis(connection: &mut PgConnection) -> TestResult<(Option<String>, bool)> {
    if let Some(schema) = installed_extension_schema(connection, "postgis").await? {
        return Ok((Some(schema), false));
    }
    let available = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='postgis')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !available {
        return Ok((None, false));
    }
    execute(connection, "CREATE EXTENSION postgis".into()).await?;
    let schema = installed_extension_schema(connection, "postgis")
        .await?
        .ok_or("PostGIS reports installed but has no extension schema")?;
    Ok((Some(schema), true))
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn catalog_type_declaration(
    catalog: &postgresql_15::SourceTypeCatalog,
    oid: u32,
    formatted_type: &str,
) -> TestResult<String> {
    let definition = catalog
        .types
        .iter()
        .find(|definition| definition.oid == oid)
        .ok_or_else(|| std::io::Error::other(format!("missing PostgreSQL type OID {oid}")))?;
    if let postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } = definition.kind {
        let element = catalog
            .types
            .iter()
            .find(|definition| definition.oid == element_oid)
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "missing PostgreSQL array element OID {element_oid}"
                ))
            })?;
        if element.schema == "pg_catalog" {
            return Ok(formatted_type.to_owned());
        }
        return Ok(format!(
            "{}[]",
            catalog_type_declaration(catalog, element_oid, formatted_type)?
        ));
    }
    if definition.schema == "pg_catalog" {
        Ok(formatted_type.to_owned())
    } else {
        Ok(format!(
            "{}.{}",
            quote_ident(&definition.schema),
            quote_ident(&definition.name)
        ))
    }
}

async fn read_transactions(
    config: Config,
    major: u16,
    count: usize,
) -> TestResult<Vec<change_event::ValidatedTransaction>> {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let mut worker = tokio::spawn(async move {
        let mut replication = postgresql_15::replication_for_version(config, major).await?;
        let mut transactions = Vec::with_capacity(count);
        for _ in 0..count {
            transactions.push(replication.next_transaction(&cancel).await?);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(transactions)
    });
    match tokio::time::timeout(Duration::from_secs(40), &mut worker).await {
        Ok(result) => result?,
        Err(error) => {
            cancellation.cancel();
            let _ = worker.await;
            Err(error.into())
        }
    }
}

async fn execute(connection: &mut PgConnection, sql: String) -> TestResult {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(connection)
        .await?;
    Ok(())
}

/// Remove only nonce-named relations, types, and schemas created by this
/// repository's live qualification fixtures when a prior test was interrupted.
/// The name patterns intentionally match the fixture constructors in
/// `web_ui::runtime_tests` and `capture_recursive_types`; ordinary CDC_test
/// objects and the stable `cdc_web_types_pg{major}_qualification` schema are
/// outside this cleanup scope.
async fn cleanup_stale_type_qualification_fixtures(
    connection: &mut PgConnection,
    major: u16,
) -> TestResult<usize> {
    let mut removed = 0;
    let relation_pattern = format!(
        "^(web_carrier|web_pg|web_mysql_types|web_pg_enum|web_pg{major}_types|cap_inv)_[0-9]+$|^(events|catalog_events)_{major}_[0-9]+$"
    );
    let relations = sqlx::query_scalar::<_, String>(
        "SELECT c.relname
           FROM pg_catalog.pg_class c
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='CDC_test' AND c.relkind IN ('r','p')
            AND c.relname ~ $1
          ORDER BY c.relname",
    )
    .bind(&relation_pattern)
    .fetch_all(&mut *connection)
    .await?;
    for relation in relations {
        execute(
            connection,
            format!(
                "DROP TABLE IF EXISTS {}.{} CASCADE",
                quote_ident("CDC_test"),
                quote_ident(&relation)
            ),
        )
        .await?;
        removed += 1;
    }

    let enum_types = sqlx::query_scalar::<_, String>(
        "SELECT t.typname
           FROM pg_catalog.pg_type t
           JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
          WHERE n.nspname='CDC_test' AND t.typtype='e'
            AND t.typname ~ '^web_mood_[0-9]+$'
          ORDER BY t.typname",
    )
    .fetch_all(&mut *connection)
    .await?;
    for type_name in enum_types {
        execute(
            connection,
            format!(
                "DROP TYPE IF EXISTS {}.{} CASCADE",
                quote_ident("CDC_test"),
                quote_ident(&type_name)
            ),
        )
        .await?;
        removed += 1;
    }

    let recursive_publications = sqlx::query_scalar::<_, String>(
        "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname ~ $1 ORDER BY pubname",
    )
    .bind(format!("^cdc_pg{major}_recursive_pub_[0-9]+$"))
    .fetch_all(&mut *connection)
    .await?;
    for publication in recursive_publications {
        execute(
            connection,
            format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&publication)),
        )
        .await?;
        removed += 1;
    }

    let stale_slots = sqlx::query_scalar::<_, String>(
        "SELECT slot_name FROM pg_catalog.pg_replication_slots
          WHERE database=current_database() AND NOT active AND slot_name ~ $1
          ORDER BY slot_name",
    )
    .bind(format!("^cdcpg{major}recursive[0-9]+$"))
    .fetch_all(&mut *connection)
    .await?;
    for slot in stale_slots {
        sqlx::query("SELECT pg_catalog.pg_drop_replication_slot($1)")
            .bind(slot)
            .execute(&mut *connection)
            .await?;
        removed += 1;
    }

    let schema_pattern =
        format!("^cdc_web_types_pg{major}_[0-9]+$|^cdc_pg{major}_recursive_[0-9]+$");
    let schemas = sqlx::query_scalar::<_, String>(
        "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname ~ $1 ORDER BY nspname",
    )
    .bind(&schema_pattern)
    .fetch_all(&mut *connection)
    .await?;
    for schema in schemas {
        execute(
            connection,
            format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&schema)),
        )
        .await?;
        removed += 1;
    }
    Ok(removed)
}

macro_rules! live_test {
    ($name:ident, $major:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires a configured PostgreSQL live test instance"]
        async fn $name() -> TestResult {
            let _ = capture_recursive_types($major).await?;
            Ok(())
        }
    };
}

live_test!(postgresql15_recursive_type_capture, 15);
live_test!(postgresql16_recursive_type_capture, 16);
live_test!(postgresql17_recursive_type_capture, 17);

macro_rules! postgis_live_test {
    ($name:ident, $major:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires a configured PostgreSQL live test instance with the PostGIS server extension package"]
        async fn $name() -> TestResult {
            let capture = capture_recursive_types_with_postgis($major, true).await?;
            for expected in ["location", "earth_location"] {
                assert!(
                    capture.fields.iter().any(|(name, _)| name == expected),
                    "PostgreSQL {} PostGIS capture lacks {expected}",
                    $major
                );
            }
            Ok(())
        }
    };
}

postgis_live_test!(postgresql15_postgis_geometry_geography_capture, 15);
postgis_live_test!(postgresql16_postgis_geometry_geography_capture, 16);
postgis_live_test!(postgresql17_postgis_geometry_geography_capture, 17);
