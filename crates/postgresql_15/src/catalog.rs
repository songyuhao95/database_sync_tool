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
