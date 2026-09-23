use crate::{Result, invalid, types};
use sqlx::{PgConnection, Row};
use std::collections::HashMap;
#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub name: String,
    pub oid: u32,
    pub modifier: i32,
    pub native_type: String,
    pub key: Option<usize>,
}
#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub identity: u8,
    pub columns: Vec<Column>,
}
pub(crate) async fn load(
    conn: &mut PgConnection,
    publication: &str,
    version: u16,
) -> Result<HashMap<u32, Table>> {
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
    let type_catalog = load_type_catalog(conn).await?;
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
            let oid = u32::try_from(column.try_get::<i64, _>("type_oid")?)?;
            let native_type: String = column.try_get("native_type")?;
            crate::type_mapping::source_type_mapping_with_catalog_for_version(
                &version.to_string(),
                &native_type,
                &type_catalog,
            )
            .map_err(|error| invalid(format!("{schema}.{name}: {error}")))?;
            if !(types::supported(oid) || native_type.to_ascii_lowercase().starts_with("enum("))
                || !column.try_get::<String, _>("generated")?.is_empty()
            {
                return Err(invalid(format!(
                    "{schema}.{name}: unsupported type/generated column {native_type} (OID {oid})"
                )));
            }
            columns.push(Column {
                name: column.try_get("attname")?,
                oid,
                modifier: column.try_get("atttypmod")?,
                native_type,
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
                columns,
            },
        );
    }
    Ok(tables)
}

/// Read the recursive PostgreSQL type directory once per capture activation.
/// The resulting evidence is used to validate every published column before
/// the replication stream is opened; no row value participates in mapping.
async fn load_type_catalog(conn: &mut PgConnection) -> Result<crate::SourceTypeCatalog> {
    use crate::type_mapping::{SourceTypeDefinition, SourceTypeField};

    let rows = sqlx::query(
        "SELECT t.oid::bigint AS oid,n.nspname,t.typname,t.typtype::text AS typtype,
                t.typbasetype::bigint AS base_oid,t.typelem::bigint AS elem_oid,
                t.typrelid::bigint AS relid,
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
          WHERE t.typtype IN ('b','e','d','c','r','m')
            AND n.nspname NOT LIKE 'pg_toast%'
          ORDER BY t.oid",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut definitions = Vec::with_capacity(rows.len());
    for row in rows {
        let oid = u32::try_from(row.try_get::<i64, _>("oid")?)?;
        let schema: String = row.try_get("nspname")?;
        let name: String = row.try_get("typname")?;
        let kind: String = row.try_get("typtype")?;
        let element_oid = u32::try_from(row.try_get::<i64, _>("elem_oid")?)?;
        let extension: Option<String> = row.try_get("extname")?;
        let definition = match kind.as_str() {
            "b" if element_oid != 0 => {
                SourceTypeDefinition::array(oid, &schema, &name, element_oid)
            }
            "b" if let Some(extension) = extension => SourceTypeDefinition::extension(
                oid,
                &schema,
                &name,
                extension,
                "",
                "postgresql.text.v1",
                None,
            ),
            "b" => SourceTypeDefinition::builtin(oid, &schema, &name),
            "e" => {
                let labels = sqlx::query_scalar::<_, String>(
                    "SELECT enumlabel FROM pg_enum WHERE enumtypid=$1::bigint::oid ORDER BY enumsortorder",
                )
                .bind(i64::from(oid))
                .fetch_all(&mut *conn)
                .await?;
                SourceTypeDefinition::enum_type(oid, &schema, &name, labels)
            }
            "d" => {
                let base_oid = u32::try_from(row.try_get::<i64, _>("base_oid")?)?;
                let constraints = sqlx::query_scalar::<_, String>(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE contypid=$1::bigint::oid ORDER BY oid",
                )
                .bind(i64::from(oid))
                .fetch_all(&mut *conn)
                .await?;
                SourceTypeDefinition::domain(
                    oid,
                    &schema,
                    &name,
                    base_oid,
                    constraints,
                    row.try_get("typnotnull")?,
                    row.try_get("collation")?,
                )
            }
            "c" => {
                let relid = u32::try_from(row.try_get::<i64, _>("relid")?)?;
                let fields = sqlx::query(
                    "SELECT attname,atttypid::bigint AS type_oid,NOT attnotnull AS nullable
                       FROM pg_attribute
                      WHERE attrelid=$1::bigint::oid AND attnum>0 AND NOT attisdropped
                      ORDER BY attnum",
                )
                .bind(i64::from(relid))
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .map(|field| {
                    Ok(SourceTypeField::new(
                        field.try_get::<String, _>("attname")?,
                        u32::try_from(field.try_get::<i64, _>("type_oid")?)?,
                        field.try_get("nullable")?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
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
        "SELECT extname,extversion,n.nspname AS schema FROM pg_extension e
          JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY extname",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|row| {
        Ok(crate::SourceExtension {
            name: row.try_get("extname")?,
            version: row.try_get("extversion")?,
            schema: row.try_get("schema")?,
            installed: true,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(crate::SourceTypeCatalog::with_extensions(
        definitions,
        extensions,
    ))
}
