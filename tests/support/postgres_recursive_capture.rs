//! Live recursive PostgreSQL type capture qualification.
//! Uses isolated schema, publication, and slot names on authorized PG15/16/17 instances.

use change_event::{Datum, LogicalValue, Operation, ServerBuildIdentity};
use postgresql_15::{CancellationToken, Config};
use sqlx::{Connection, PgConnection};
use std::{env, time::Duration};

use super::{postgres_env, type_qualification_evidence};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) struct RecursiveCapture {
    pub(super) table_name: String,
    pub(super) transactions: Vec<change_event::ValidatedTransaction>,
    pub(super) catalog: postgresql_15::SourceTypeCatalog,
    pub(super) source_build: ServerBuildIdentity,
    pub(super) fields: Vec<(String, String)>,
    pub(super) dynamic_classes: Vec<String>,
}

pub(super) async fn capture_recursive_types(major: u16) -> TestResult<RecursiveCapture> {
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
    let mut created_hstore = false;
    let mut created_postgis = false;
    let result = async {
        execute(&mut admin, format!("CREATE SCHEMA {schema}")).await?;
        execute(&mut admin, "CREATE SCHEMA IF NOT EXISTS \"CDC_test\"".into()).await?;
        let (hstore_schema, created) = ensure_hstore(&mut admin, &schema).await?;
        created_hstore = created;
        let (postgis_schema, created) = ensure_postgis(&mut admin).await?;
        created_postgis = created;
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
                    format!(", location {}.geometry", quote_ident(extension_schema))
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
