use crate::{Result, invalid};
use sqlx::{PgConnection, Row};
use std::collections::HashMap;
#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub name: String,
    pub oid: u32,
    pub modifier: i32,
    pub native_type: String,
    pub type_schema: String,
    pub type_name: String,
    pub type_definition_digest: String,
    pub type_definition_evidence: String,
    pub type_definition_closure_evidence: String,
    pub type_definition_closure_digest: String,
    pub representation_only: bool,
    pub representation_capture_allowed: bool,
    pub key: Option<usize>,
}
#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub identity: u8,
    pub source_type_catalog_digest: String,
    pub columns: Vec<Column>,
}
pub(crate) struct LoadedCatalog {
    pub tables: HashMap<u32, Table>,
    pub type_names: HashMap<u32, (String, String)>,
    pub type_catalog: crate::SourceTypeCatalog,
}
pub(crate) async fn load(
    conn: &mut PgConnection,
    publication: &str,
    version: u16,
) -> Result<LoadedCatalog> {
    let publication_info=sqlx::query("SELECT pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot FROM pg_publication WHERE pubname=$1")
        .bind(publication).fetch_optional(&mut *conn).await?.ok_or_else(||invalid("publication does not exist; prepare it with the table owner/admin"))?;
    for flag in ["pubinsert", "pubupdate", "pubdelete", "pubtruncate"] {
        if !publication_info.try_get::<bool, _>(flag)? {
            return Err(invalid(format!(
                "publication must include {flag}; omitted operations would be invisible"
            )));
        }
    }
    if publication_info.try_get::<bool, _>("pubviaroot")? {
        return Err(invalid("partition-root publication is not supported yet"));
    }
    let relations=sqlx::query("SELECT c.oid::bigint AS oid,p.schemaname,p.tablename,p.attnames::text[] AS attnames,p.rowfilter,c.relkind::text AS kind,c.relreplident::text AS identity,c.relrowsecurity,c.relpersistence::text AS persistence FROM pg_publication_tables p JOIN pg_namespace n ON n.nspname=p.schemaname JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname=$1 ORDER BY c.oid")
        .bind(publication).fetch_all(&mut *conn).await?;
    if relations.is_empty() {
        return Err(invalid("publication has no tables"));
    }
    let type_catalog = source_type_catalog(conn).await?;
    let source_type_catalog_digest = type_catalog.evidence_digest();
    let mut tables = HashMap::new();
    for row in relations {
        let oid = u32::try_from(row.try_get::<i64, _>("oid")?)?;
        let schema: String = row.try_get("schemaname")?;
        let name: String = row.try_get("tablename")?;
        let identity: String = row.try_get("identity")?;
        if row.try_get::<String, _>("kind")? != "r"
            || row.try_get::<String, _>("persistence")? != "p"
            || row.try_get::<bool, _>("relrowsecurity")?
            || row.try_get::<Option<String>, _>("rowfilter")?.is_some()
            || !["d", "f"].contains(&identity.as_str())
        {
            return Err(invalid(format!(
                "{schema}.{name}: requires an ordinary permanent primary-key table without RLS/row filters and DEFAULT or FULL replica identity"
            )));
        }
        let column_rows=sqlx::query("SELECT a.attname,a.atttypid::bigint AS type_oid,a.atttypmod,CASE WHEN t.typtype='e' THEN 'enum(' || (SELECT string_agg(quote_literal(e.enumlabel), ',' ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid=a.atttypid) || ')' ELSE format_type(a.atttypid,a.atttypmod) END AS native_type,a.attgenerated::text AS generated,(SELECT (k.ord-1)::integer FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY AS k(attnum,ord) WHERE i.indrelid=a.attrelid AND i.indisprimary AND k.attnum=a.attnum AND k.ord <= i.indnkeyatts) AS key_ordinal FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid WHERE a.attrelid=$1::bigint::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")
            .bind(i64::from(oid)).fetch_all(&mut *conn).await?;
        let mut columns = Vec::new();
        for column in column_rows {
            let column_name: String = column.try_get("attname")?;
            let oid = u32::try_from(column.try_get::<i64, _>("type_oid")?)?;
            let native_type: String = column.try_get("native_type")?;
            let definition = type_catalog
                .types
                .iter()
                .find(|definition| definition.oid == oid)
                .ok_or_else(|| {
                    invalid(format!(
                        "{schema}.{name}: catalog is missing type OID {oid}"
                    ))
                })?;
            let mapping = crate::type_mapping::source_type_mapping_with_catalog_for_version(
                &version.to_string(),
                &native_type,
                &type_catalog,
            )
            .map_err(|error| {
                invalid(format!(
                    "{schema}.{name}.{} ({native_type}): {error}",
                    column_name
                ))
            })?;
            let semantic_codec = !crate::type_mapping::captures_source_representation(
                &type_catalog,
                oid,
                &mapping.logical_type,
            );
            let representation_capture_allowed = !semantic_codec;
            let closure = type_catalog
                .definition_closure(oid)
                .map_err(|error| invalid(format!("{schema}.{name}.{column_name}: {error}")))?;
            let type_definition_closure_digest = closure.digest();
            let type_definition_closure_evidence = serde_json::to_string(&closure)?;
            if !column.try_get::<String, _>("generated")?.is_empty() {
                return Err(invalid(format!(
                    "{schema}.{name}: unsupported type/generated column {native_type} (OID {oid})"
                )));
            }
            columns.push(Column {
                name: column_name,
                oid,
                modifier: column.try_get("atttypmod")?,
                native_type,
                type_schema: definition.schema.clone(),
                type_name: definition.name.clone(),
                type_definition_digest: definition.definition_digest.clone(),
                type_definition_evidence: serde_json::to_string(definition)?,
                type_definition_closure_evidence,
                type_definition_closure_digest,
                representation_only: !semantic_codec,
                representation_capture_allowed,
                key: column
                    .try_get::<Option<i32>, _>("key_ordinal")?
                    .map(usize::try_from)
                    .transpose()?,
            });
        }
        let published: Vec<String> = row.try_get("attnames")?;
        if published != columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
            || !columns.iter().any(|c| c.key.is_some())
        {
            return Err(invalid(format!(
                "{schema}.{name}: publish all columns of a primary-key table"
            )));
        }
        tables.insert(
            oid,
            Table {
                oid,
                schema,
                name,
                identity: identity.as_bytes()[0],
                source_type_catalog_digest: source_type_catalog_digest.clone(),
                columns,
            },
        );
    }
    let type_names = type_catalog
        .types
        .iter()
        .map(|definition| {
            (
                definition.oid,
                (definition.schema.clone(), definition.name.clone()),
            )
        })
        .collect();
    Ok(LoadedCatalog {
        tables,
        type_names,
        type_catalog,
    })
}

/// Read the recursive PostgreSQL type directory once per capture activation.
/// The resulting evidence is used to validate every published column before
/// the replication stream is opened; no row value participates in mapping.
/// Load the immutable PostgreSQL type and extension directory used by
/// SourceTypeMapping. Callers should retain the returned snapshot together
/// with the server build and environment fingerprint from [`crate::Metadata`].
pub async fn source_type_catalog(conn: &mut PgConnection) -> Result<crate::SourceTypeCatalog> {
    use crate::type_mapping::{SourceTypeDefinition, SourceTypeField};

    let rows = sqlx::query(
        "SELECT t.oid::bigint AS oid,n.nspname,t.typname,t.typtype::text AS typtype,
                t.typbasetype::bigint AS base_oid,t.typelem::bigint AS elem_oid,
                t.typrelid::bigint AS relid,t.typdelim::text AS delimiter,
                (t.typsubscript='pg_catalog.array_subscript_handler'::regproc
                 AND t.typelem<>0
                 AND EXISTS (SELECT 1 FROM pg_type element WHERE element.typarray=t.oid)) AS is_array,
                t.typnotnull,
                r.rngtypid::bigint AS range_oid,r.rngsubtype::bigint AS range_subtype,
                coll.collname AS collation,
                x.extname
           FROM pg_type t
           JOIN pg_namespace n ON n.oid=t.typnamespace
           LEFT JOIN pg_range r ON r.rngtypid=t.oid OR r.rngmultitypid=t.oid
           LEFT JOIN pg_depend d ON d.classid='pg_type'::regclass AND d.objid=t.oid
                AND d.deptype='e'
           LEFT JOIN pg_extension x ON x.oid=d.refobjid
           LEFT JOIN pg_collation coll ON coll.oid=t.typcollation
          WHERE t.typisdefined
            AND t.typtype IN ('b','e','d','c','r','m','p')
            AND n.nspname NOT LIKE 'pg_toast%'
          ORDER BY t.oid",
    )
    .fetch_all(&mut *conn)
    .await?;

    // Load dependent catalog rows in batches. The type catalog contains many
    // PostgreSQL system composites; querying pg_attribute once per composite
    // made startup time grow with the number of types and could take minutes
    // on a remote server.
    let mut enum_labels_by_oid: HashMap<u32, Vec<String>> = HashMap::new();
    for row in sqlx::query(
        "SELECT enumtypid::bigint AS type_oid, enumlabel
           FROM pg_enum
          ORDER BY enumtypid, enumsortorder",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        let oid = u32::try_from(row.try_get::<i64, _>("type_oid")?)?;
        enum_labels_by_oid
            .entry(oid)
            .or_default()
            .push(row.try_get("enumlabel")?);
    }

    let mut domain_constraints_by_oid: HashMap<u32, Vec<String>> = HashMap::new();
    for row in sqlx::query(
        "SELECT contypid::bigint AS type_oid, pg_get_constraintdef(oid) AS definition
           FROM pg_constraint
          WHERE contypid <> 0
          ORDER BY contypid, oid",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        let oid = u32::try_from(row.try_get::<i64, _>("type_oid")?)?;
        domain_constraints_by_oid
            .entry(oid)
            .or_default()
            .push(row.try_get("definition")?);
    }

    let mut composite_fields_by_relid: HashMap<u32, Vec<SourceTypeField>> = HashMap::new();
    for row in sqlx::query(
        "SELECT t.typrelid::bigint AS relid, a.attname,
                a.atttypid::bigint AS type_oid, NOT a.attnotnull AS nullable
           FROM pg_type t
           JOIN pg_namespace n ON n.oid=t.typnamespace
           JOIN pg_attribute a ON a.attrelid=t.typrelid
          WHERE t.typisdefined AND t.typtype='c'
            AND n.nspname NOT LIKE 'pg_toast%'
            AND a.attnum>0 AND NOT a.attisdropped
          ORDER BY t.typrelid, a.attnum",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        let relid = u32::try_from(row.try_get::<i64, _>("relid")?)?;
        let field = SourceTypeField::new(
            row.try_get::<String, _>("attname")?,
            u32::try_from(row.try_get::<i64, _>("type_oid")?)?,
            row.try_get("nullable")?,
        );
        composite_fields_by_relid
            .entry(relid)
            .or_default()
            .push(field);
    }

    let mut definitions = Vec::with_capacity(rows.len());
    for row in rows {
        let oid = u32::try_from(row.try_get::<i64, _>("oid")?)?;
        let schema: String = row.try_get("nspname")?;
        let name: String = row.try_get("typname")?;
        let kind: String = row.try_get("typtype")?;
        let is_array: bool = row.try_get("is_array")?;
        let element_oid = u32::try_from(row.try_get::<i64, _>("elem_oid")?)?;
        let extension: Option<String> = row.try_get("extname")?;
        let definition = match kind.as_str() {
            "p" => SourceTypeDefinition::pseudo(oid, &schema, &name, row.try_get("collation")?),
            "b" if is_array && element_oid != 0 => SourceTypeDefinition::array_with_delimiter(
                oid,
                &schema,
                &name,
                element_oid,
                row.try_get::<String, _>("delimiter")?
                    .chars()
                    .next()
                    .ok_or_else(|| invalid("PostgreSQL array delimiter is empty"))?,
            ),
            "b" if let Some(extension) = extension => {
                let (codec, logical) = match (extension.as_str(), name.as_str()) {
                    ("hstore", "hstore") => (
                        "postgresql.hstore.text.v1",
                        Some(change_event::LogicalType::Map {
                            key: Box::new(change_event::LogicalType::text("UTF8", None)),
                            value: Box::new(change_event::LogicalType::text("UTF8", None)),
                        }),
                    ),
                    ("postgis", "geometry" | "geography") => (
                        "postgresql.postgis.ewkb-hex.v1",
                        Some(change_event::LogicalType::spatial("*", None, 0)),
                    ),
                    _ => ("", None),
                };
                SourceTypeDefinition::extension(
                    oid,
                    &schema,
                    &name,
                    extension,
                    codec,
                    if codec.ends_with("ewkb-hex.v1") {
                        "hex-EWKB"
                    } else {
                        "UTF-8"
                    },
                    logical,
                )
            }
            "b" => SourceTypeDefinition::builtin(oid, &schema, &name),
            "e" => SourceTypeDefinition::enum_type(
                oid,
                &schema,
                &name,
                enum_labels_by_oid.remove(&oid).unwrap_or_default(),
            ),
            "d" => {
                let base_oid = u32::try_from(row.try_get::<i64, _>("base_oid")?)?;
                SourceTypeDefinition::domain(
                    oid,
                    &schema,
                    &name,
                    base_oid,
                    domain_constraints_by_oid.remove(&oid).unwrap_or_default(),
                    row.try_get("typnotnull")?,
                    row.try_get("collation")?,
                )
            }
            "c" => {
                let relid = u32::try_from(row.try_get::<i64, _>("relid")?)?;
                let fields = composite_fields_by_relid.remove(&relid).unwrap_or_default();
                SourceTypeDefinition::composite(oid, &schema, &name, fields)
            }
            "r" => SourceTypeDefinition::range(
                oid,
                &schema,
                &name,
                u32::try_from(row.try_get::<i64, _>("range_subtype")?)?,
            ),
            "m" => SourceTypeDefinition::multi_range(
                oid,
                &schema,
                &name,
                u32::try_from(row.try_get::<i64, _>("range_oid")?)?,
            ),
            _ => {
                return Err(invalid(format!(
                    "unsupported PostgreSQL type category {kind}"
                )));
            }
        };
        definitions.push(definition);
    }
    let extensions = sqlx::query(
        "SELECT COALESCE(e.extname,a.name) AS name,
                COALESCE(e.extversion,a.default_version) AS version,
                COALESCE(n.nspname,'') AS schema,
                e.oid IS NOT NULL AS installed,
                a.name IS NOT NULL AS available
           FROM pg_available_extensions a
           FULL OUTER JOIN pg_extension e ON e.extname=a.name
           LEFT JOIN pg_namespace n ON n.oid=e.extnamespace
          ORDER BY COALESCE(e.extname,a.name)",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|row| {
        Ok(crate::SourceExtension {
            name: row.try_get("name")?,
            version: row
                .try_get::<Option<String>, _>("version")?
                .unwrap_or_default(),
            schema: row.try_get("schema")?,
            installed: row.try_get("installed")?,
            available: row.try_get("available")?,
            target_compatible: None,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(crate::SourceTypeCatalog::with_extensions(
        definitions,
        extensions,
    ))
}
