//! Live PostgreSQL 15/16/17 built-in type capture qualification.
//! Run through `scripts/test.ps1 -Live`; objects are isolated by a unique suffix.

use change_event::{Datum, LogicalValue, Operation};
use postgresql_15::{CancellationToken, Config, Result};
use sqlx::{Connection, PgConnection, Row};
use std::{env, time::Duration};

#[path = "../../../tests/support/postgres_env.rs"]
mod postgres_env;

struct BuiltinCase {
    name: &'static str,
    declaration: &'static str,
    expression: &'static str,
}

const BUILTINS: &[BuiltinCase] = &[
    BuiltinCase {
        name: "internal_char",
        declaration: "\"char\"",
        expression: "'x'::\"char\"",
    },
    BuiltinCase {
        name: "name_value",
        declaration: "name",
        expression: "'identifier'::name",
    },
    BuiltinCase {
        name: "boolean_value",
        declaration: "boolean",
        expression: "true",
    },
    BuiltinCase {
        name: "smallint_value",
        declaration: "smallint",
        expression: "-32768",
    },
    BuiltinCase {
        name: "integer_value",
        declaration: "integer",
        expression: "2147483647",
    },
    BuiltinCase {
        name: "bigint_value",
        declaration: "bigint",
        expression: "9223372036854775807",
    },
    BuiltinCase {
        name: "numeric_value",
        declaration: "numeric(30,6)",
        expression: "12345678901234567890.123456::numeric(30,6)",
    },
    BuiltinCase {
        name: "negative_scale_numeric",
        declaration: "numeric(10,-2)",
        expression: "123456789::numeric(10,-2)",
    },
    BuiltinCase {
        name: "real_value",
        declaration: "real",
        expression: "'NaN'::real",
    },
    BuiltinCase {
        name: "double_value",
        declaration: "double precision",
        expression: "'-Infinity'::double precision",
    },
    BuiltinCase {
        name: "money_value",
        declaration: "money",
        expression: "'1234.56'::money",
    },
    BuiltinCase {
        name: "text_value",
        declaration: "text",
        expression: "'captured text'",
    },
    BuiltinCase {
        name: "varchar_value",
        declaration: "character varying(255)",
        expression: "'varchar'",
    },
    BuiltinCase {
        name: "bpchar_value",
        declaration: "character(8)",
        expression: "'char'",
    },
    BuiltinCase {
        name: "bytea_value",
        declaration: "bytea",
        expression: "decode('00ff','hex')",
    },
    BuiltinCase {
        name: "bit_value",
        declaration: "bit(8)",
        expression: "B'101'::bit(8)",
    },
    BuiltinCase {
        name: "varbit_value",
        declaration: "bit varying(64)",
        expression: "B'10101'::bit varying(64)",
    },
    BuiltinCase {
        name: "date_value",
        declaration: "date",
        expression: "'0001-01-01 BC'::date",
    },
    BuiltinCase {
        name: "time_value",
        declaration: "time(6) without time zone",
        expression: "'24:00:00'::time(6)",
    },
    BuiltinCase {
        name: "timetz_value",
        declaration: "time(6) with time zone",
        expression: "'12:34:56.123456+05:30'::timetz",
    },
    BuiltinCase {
        name: "timestamp_value",
        declaration: "timestamp(6) without time zone",
        expression: "'0001-01-01 01:02:03 BC'::timestamp(6)",
    },
    BuiltinCase {
        name: "timestamptz_value",
        declaration: "timestamp(6) with time zone",
        expression: "'infinity'::timestamptz",
    },
    BuiltinCase {
        name: "interval_value",
        declaration: "interval",
        expression: "'2 years 3 mons 4 days 05:06:07.123456'::interval",
    },
    BuiltinCase {
        name: "uuid_value",
        declaration: "uuid",
        expression: "'12345678-1234-1234-1234-123456789abc'::uuid",
    },
    BuiltinCase {
        name: "json_value",
        declaration: "json",
        expression: "'{\"n\":1}'::json",
    },
    BuiltinCase {
        name: "jsonb_value",
        declaration: "jsonb",
        expression: "'{\"n\":1}'::jsonb",
    },
    BuiltinCase {
        name: "xml_value",
        declaration: "xml",
        expression: "XMLPARSE(DOCUMENT '<root/>')",
    },
    BuiltinCase {
        name: "point_value",
        declaration: "point",
        expression: "point(1,2)",
    },
    BuiltinCase {
        name: "line_value",
        declaration: "line",
        expression: "line(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "lseg_value",
        declaration: "lseg",
        expression: "lseg(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "box_value",
        declaration: "box",
        expression: "box(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "path_value",
        declaration: "path",
        expression: "'[(0,0),(1,1)]'::path",
    },
    BuiltinCase {
        name: "polygon_value",
        declaration: "polygon",
        expression: "'((0,0),(1,1),(2,0))'::polygon",
    },
    BuiltinCase {
        name: "circle_value",
        declaration: "circle",
        expression: "'<(0,0),1>'::circle",
    },
    BuiltinCase {
        name: "cidr_value",
        declaration: "cidr",
        expression: "'192.0.2.0/24'::cidr",
    },
    BuiltinCase {
        name: "inet_value",
        declaration: "inet",
        expression: "'2001:db8::1/64'::inet",
    },
    BuiltinCase {
        name: "macaddr_value",
        declaration: "macaddr",
        expression: "'08:00:2b:01:02:03'::macaddr",
    },
    BuiltinCase {
        name: "macaddr8_value",
        declaration: "macaddr8",
        expression: "'08:00:2b:01:02:03:04:05'::macaddr8",
    },
    BuiltinCase {
        name: "tsvector_value",
        declaration: "tsvector",
        expression: "'fat:1 rat:2'::tsvector",
    },
    BuiltinCase {
        name: "tsquery_value",
        declaration: "tsquery",
        expression: "'fat & rat'::tsquery",
    },
    BuiltinCase {
        name: "oid_value",
        declaration: "oid",
        expression: "42::oid",
    },
    BuiltinCase {
        name: "oidvector_value",
        declaration: "oidvector",
        expression: "'1 2'::oidvector",
    },
    BuiltinCase {
        name: "int2vector_value",
        declaration: "int2vector",
        expression: "'1 2'::int2vector",
    },
    BuiltinCase {
        name: "tid_value",
        declaration: "tid",
        expression: "'(0,1)'::tid",
    },
    BuiltinCase {
        name: "xid_value",
        declaration: "xid",
        expression: "'42'::xid",
    },
    BuiltinCase {
        name: "xid8_value",
        declaration: "xid8",
        expression: "'42'::xid8",
    },
    BuiltinCase {
        name: "cid_value",
        declaration: "cid",
        expression: "'42'::cid",
    },
    BuiltinCase {
        name: "lsn_value",
        declaration: "pg_lsn",
        expression: "'0/16B6C50'::pg_lsn",
    },
    BuiltinCase {
        name: "snapshot_value",
        declaration: "pg_snapshot",
        expression: "'1:5:2,3'::pg_snapshot",
    },
    BuiltinCase {
        name: "txid_snapshot_value",
        declaration: "txid_snapshot",
        expression: "'1:5:2,3'::txid_snapshot",
    },
    BuiltinCase {
        name: "regproc_value",
        declaration: "regproc",
        expression: "'abs(integer)'::regprocedure::regproc",
    },
    BuiltinCase {
        name: "regprocedure_value",
        declaration: "regprocedure",
        expression: "'abs(integer)'::regprocedure",
    },
    BuiltinCase {
        name: "regoper_value",
        declaration: "regoper",
        expression: "'=(integer,integer)'::regoperator::regoper",
    },
    BuiltinCase {
        name: "regoperator_value",
        declaration: "regoperator",
        expression: "'=(integer,integer)'::regoperator",
    },
    BuiltinCase {
        name: "regclass_value",
        declaration: "regclass",
        expression: "'pg_class'::regclass",
    },
    BuiltinCase {
        name: "regtype_value",
        declaration: "regtype",
        expression: "'int4'::regtype",
    },
    BuiltinCase {
        name: "regconfig_value",
        declaration: "regconfig",
        expression: "'english'::regconfig",
    },
    BuiltinCase {
        name: "regdictionary_value",
        declaration: "regdictionary",
        expression: "'simple'::regdictionary",
    },
    BuiltinCase {
        name: "regnamespace_value",
        declaration: "regnamespace",
        expression: "'pg_catalog'::regnamespace",
    },
    BuiltinCase {
        name: "regrole_value",
        declaration: "regrole",
        expression: "'postgres'::regrole",
    },
    BuiltinCase {
        name: "regcollation_value",
        declaration: "regcollation",
        expression: "'default'::regcollation",
    },
    BuiltinCase {
        name: "int4range_value",
        declaration: "int4range",
        expression: "int4range(1,5,'[)')",
    },
    BuiltinCase {
        name: "int8range_value",
        declaration: "int8range",
        expression: "int8range(1,5,'[)')",
    },
    BuiltinCase {
        name: "numrange_value",
        declaration: "numrange",
        expression: "numrange(1.25,5.5,'[)')",
    },
    BuiltinCase {
        name: "tsrange_value",
        declaration: "tsrange",
        expression: "tsrange('2020-01-01','2020-01-02','[)')",
    },
    BuiltinCase {
        name: "tstzrange_value",
        declaration: "tstzrange",
        expression: "tstzrange('2020-01-01+00','2020-01-02+00','[)')",
    },
    BuiltinCase {
        name: "daterange_value",
        declaration: "daterange",
        expression: "daterange('2020-01-01','2020-01-05','[)')",
    },
    BuiltinCase {
        name: "int4multirange_value",
        declaration: "int4multirange",
        expression: "'{[1,3),[5,8)}'::int4multirange",
    },
    BuiltinCase {
        name: "int8multirange_value",
        declaration: "int8multirange",
        expression: "'{[1,3),[5,8)}'::int8multirange",
    },
    BuiltinCase {
        name: "nummultirange_value",
        declaration: "nummultirange",
        expression: "'{[1.25,3.5),[5,8)}'::nummultirange",
    },
    BuiltinCase {
        name: "tsmultirange_value",
        declaration: "tsmultirange",
        expression: "'{[\"2020-01-01 00:00:00\",\"2020-01-02 00:00:00\")}'::tsmultirange",
    },
    BuiltinCase {
        name: "tstzmultirange_value",
        declaration: "tstzmultirange",
        expression: "'{[\"2020-01-01 00:00:00+00\",\"2020-01-02 00:00:00+00\")}'::tstzmultirange",
    },
    BuiltinCase {
        name: "datemultirange_value",
        declaration: "datemultirange",
        expression: "'{[2020-01-01,2020-01-05)}'::datemultirange",
    },
];

async fn capture_all_builtins(expected_major: u16) -> Result<()> {
    let version = expected_major.to_string();
    let password = env::var(postgres_env::env_name(&version, "TEST_PASSWORD"))?;
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = format!("cdc_pg{expected_major}_builtin_{tag}");
    let publication = format!("cdc_pg{expected_major}_builtin_pub_{tag}");
    let slot = format!("cdc_pg{expected_major}_builtin_slot_{tag}");
    let admin_user = postgres_env::setting(&version, "ADMIN_USER", "postgres");
    let reader_user = postgres_env::setting(&version, "READER_USER", "postgresql_reader");
    let writer_user = postgres_env::setting(&version, "WRITER_USER", "postgresql_writer");
    let mut admin =
        PgConnection::connect_with(&postgres_env::options(&version, &admin_user, &password))
            .await?;

    let result = async {
        execute(&mut admin, format!("CREATE SCHEMA {schema}")).await?;
        let array_rows = sqlx::query(
            "SELECT format_type(array_type.oid, -1) AS declaration, element_type.typname AS element_name
               FROM pg_catalog.pg_type element_type
               JOIN pg_catalog.pg_namespace namespace ON namespace.oid=element_type.typnamespace
               JOIN pg_catalog.pg_type array_type ON array_type.oid=element_type.typarray
              WHERE namespace.nspname='pg_catalog'
                AND element_type.typtype IN ('b','d','e','r','m')
                AND element_type.typisdefined
                AND array_type.typtype='b'
                AND array_type.typisdefined
                AND array_type.typelem=element_type.oid
              ORDER BY array_type.oid",
        )
        .fetch_all(&mut admin)
        .await?;
        let mut fields = BUILTINS
            .iter()
            .map(|case| (case.name.to_owned(), case.declaration.to_owned()))
            .collect::<Vec<_>>();
        for (index, row) in array_rows.iter().enumerate() {
            fields.push((
                format!("array_{index:03}"),
                row.try_get::<String, _>("declaration")?,
            ));
        }
        fields.push(("nullable_marker".into(), "text".into()));
        let definitions = fields
            .iter()
            .map(|(name, declaration)| format!("\"{name}\" {declaration}"))
            .collect::<Vec<_>>()
            .join(",");
        execute(&mut admin, "SET search_path='pg_catalog'".into()).await?;
        execute(
            &mut admin,
            format!("CREATE TABLE {schema}.events (id bigint PRIMARY KEY,{definitions})"),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("create built-in fixture table: {error}")))?;
        execute(
            &mut admin,
            format!("ALTER TABLE {schema}.events REPLICA IDENTITY FULL"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE PUBLICATION {publication} FOR TABLE {schema}.events"),
        )
        .await?;

        let mut config = Config::new(
            postgres_env::setting(&version, "HOST", "192.168.0.10"),
            postgres_env::setting(&version, "PORT", "54321")
                .parse()
                .map_err(|error| format!("invalid PostgreSQL test port: {error}"))?,
            "CDC_test",
            reader_user.clone(),
            &password,
            &publication,
            &slot,
        );
        config.create_slot = true;
        let initial = postgresql_15::replication_for_version(config.clone(), expected_major).await?;
        let source_id = initial.source().id.clone();
        drop(initial);
        config.create_slot = false;
        config.expected_source_id = Some(source_id);

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
        let mut values = vec!["1".to_owned()];
        values.extend(BUILTINS.iter().map(|case| case.expression.to_owned()));
        values.extend(array_rows.iter().map(|row| {
            format!(
                "ARRAY[NULL]::{}",
                row.try_get::<String, _>("declaration").expect("catalog declaration")
            )
        }));
        values.push("NULL".into());
        let columns = std::iter::once("id".to_owned())
            .chain(fields.iter().map(|(name, _)| format!("\"{name}\"")))
            .collect::<Vec<_>>()
            .join(",");
        let insert = format!(
            "INSERT INTO {schema}.events ({columns}) VALUES ({})",
            values.join(",")
        );
        execute(&mut writer, insert).await?;

        let mut reader = PgConnection::connect_with(&postgres_env::options(
            &version,
            &reader_user,
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
            execute(&mut reader, setting.into()).await?;
        }
        let projection = fields
            .iter()
            .map(|(name, _)| format!("\"{name}\"::text"))
            .collect::<Vec<_>>()
            .join(",");
        let expected_row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {projection} FROM {schema}.events WHERE id=1"
        )))
        .fetch_one(&mut reader)
        .await?;
        let expected_text = (0..fields.len())
            .map(|index| expected_row.try_get::<Option<String>, _>(index))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        execute(
            &mut writer,
            format!("UPDATE {schema}.events SET nullable_marker='now-present' WHERE id=1"),
        )
        .await?;
        execute(
            &mut writer,
            format!("DELETE FROM {schema}.events WHERE id=1"),
        )
        .await?;

        let transactions = read_transactions(config, expected_major, 3).await?;
        let insert_tx = transactions[0].transaction();
        assert_eq!(insert_tx.changes.len(), 1);
        assert!(matches!(insert_tx.changes[0].operation, Operation::Insert));
        let image = insert_tx.changes[0].after.as_ref().expect("INSERT after image");
        let by_name = image.iter().map(|column| (column.name.as_str(), &column.datum)).collect::<std::collections::HashMap<_, _>>();
        let mut semantic_count = 0;
        let mut envelope_count = 0;
        for (index, (name, _)) in fields.iter().enumerate() {
            let datum = by_name.get(name.as_str()).expect("captured built-in column");
            if name == "nullable_marker" {
                assert!(matches!(datum, Datum::Null));
                continue;
            }
            match datum {
                Datum::SourceRepresentationEnvelope(envelope) => {
                    envelope_count += 1;
                    assert_eq!(envelope.raw_bytes()?, expected_text[index].as_deref().unwrap().as_bytes(), "{name}");
                    assert_eq!(envelope.context.source_cursor, insert_tx.changes[0].source_cursor, "{name} cursor");
                    assert!(envelope.payload_length > 0, "{name} payload length");
                    assert_eq!(envelope.context.type_metadata["session.search_path"], "pg_catalog");
                    assert!(!envelope.context.type_metadata["environment.lc_monetary"].is_empty());
                }
                Datum::Value(value) => {
                    semantic_count += 1;
                    assert!(!matches!(value, LogicalValue::Null), "{name}");
                }
                other => panic!("{name} did not carry its inserted value: {other:?}"),
            }
        }
        assert!(semantic_count > 0, "semantic codecs must remain active");
        assert!(envelope_count > 0, "unmapped built-ins must use the evidence envelope");
        assert!(matches!(
            by_name["nullable_marker"],
            Datum::Null
        ));

        assert!(matches!(transactions[1].transaction().changes[0].operation, Operation::Update));
        let update = &transactions[1].transaction().changes[0];
        let before = update.before.as_ref().unwrap();
        let after = update.after.as_ref().unwrap();
        assert!(matches!(before.iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Null));
        assert!(matches!(&after.iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Value(LogicalValue::Text { text: Some(value), .. }) if value == "now-present"));
        assert!(matches!(transactions[2].transaction().changes[0].operation, Operation::Delete));

        for transaction in &transactions {
            let encoded = change_event::json(transaction)?;
            let mut json_reader = change_event::JsonReader::new(std::io::Cursor::new(&encoded));
            let replay = json_reader.next_transaction()?.expect("JSON transaction");
            assert_eq!(change_event::json(&replay)?, encoded);
            json_reader.finish()?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((fields.len() - 1, semantic_count, envelope_count, array_rows.len()))
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
        format!("DROP PUBLICATION IF EXISTS {publication}"),
    )
    .await;
    let _ = execute(
        &mut admin,
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
    )
    .await;
    let (type_count, semantic_count, envelope_count, array_count) = result?;
    println!(
        "PASS: PostgreSQL {expected_major} pgoutput captured {type_count} built-in types (including {array_count} catalog arrays): {semantic_count} semantic values, {envelope_count} evidence-bound exact text representations; INSERT/UPDATE/DELETE, NULL→value presence, and ChangeEvent JSON replay"
    );
    Ok(())
}

async fn read_transactions(
    config: Config,
    expected_major: u16,
    count: usize,
) -> Result<Vec<change_event::ValidatedTransaction>> {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let mut worker = tokio::spawn(async move {
        let mut capture = postgresql_15::replication_for_version(config, expected_major).await?;
        let mut transactions = Vec::with_capacity(count);
        for _ in 0..count {
            transactions.push(capture.next_transaction(&cancel).await?);
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

async fn execute(conn: &mut PgConnection, sql: String) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(sql)).execute(conn).await?;
    Ok(())
}

macro_rules! live_test {
    ($name:ident, $version:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires a configured PostgreSQL live test instance"]
        async fn $name() -> Result<()> {
            capture_all_builtins($version).await
        }
    };
}

live_test!(postgresql15_all_builtin_types_capture, 15);
live_test!(postgresql16_all_builtin_types_capture, 16);
live_test!(postgresql17_all_builtin_types_capture, 17);
