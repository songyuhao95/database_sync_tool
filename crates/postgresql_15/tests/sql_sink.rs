use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    CapabilityProbeStatus, ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonValue,
    LogicalValue, Operation, RowChange, SinkAdapter as _, Source, SpatialFormat,
    TargetCapabilityFailure, validate,
};
use postgresql_15::{SinkAdapter, TargetConfig, execute};
use sqlx::{Connection, PgConnection, Row};
use std::{
    env, io,
    time::{SystemTime, UNIX_EPOCH},
};
#[path = "../../../tests/support/postgres_env.rs"]
mod postgres_env;
#[path = "../../../tests/support/postgres_sink_fixtures.rs"]
mod sink_fixtures;

use sink_fixtures::{column, cursor, fixture, mysql_cursor, mysql_fixture, postgres_fixture, text};

#[test]
fn postgres15_sink_adapter_accepts_mysql_and_postgres_events() {
    let sink = SinkAdapter::new();
    let manifest = sink.capability_manifest();
    assert_eq!(manifest.connector, "postgresql_15");
    assert_eq!(manifest.target, "postgresql-15");
    assert!(manifest.requires_primary_key);
    assert!(manifest.supported_logical_types.contains(&"json"));
    assert!(manifest.supported_presence.contains(&"unchanged"));

    for transaction in [
        fixture(),
        postgres_fixture("16.10"),
        postgres_fixture("17.6"),
        mysql_fixture("5.7.44"),
        mysql_fixture("8.0.46"),
        mysql_fixture("8.4.8"),
    ] {
        sink.qualify(&transaction).unwrap();
        let plan = sink.plan(&transaction).unwrap();
        for statement in plan.statements() {
            assert!(statement.contains('$'));
            assert!(!statement.contains("18446744073709551615"));
            assert!(!statement.contains("E4B8ADE69687275C0A"));
        }
        assert!(plan.parameters().any(|parameters| !parameters.is_empty()));
    }
}

#[test]
fn postgres15_sink_adapter_reports_capability_failures_before_execution() {
    let mut keyless = fixture().transaction().clone();
    for change in &mut keyless.changes {
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            for column in image {
                column.primary_key_ordinal = None;
            }
        }
    }
    let error = SinkAdapter::new()
        .plan(&validate(keyless).unwrap())
        .unwrap_err();
    assert!(error.to_string().contains("Target Capability Failure"));
    assert!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<TargetCapabilityFailure>())
            .is_some()
    );

    let mut unsupported_presence = fixture().transaction().clone();
    let update = &mut unsupported_presence.changes[1];
    for image in [&mut update.before, &mut update.after]
        .into_iter()
        .flatten()
    {
        image
            .iter_mut()
            .find(|column| column.name == "message")
            .unwrap()
            .generated = true;
    }
    let error = SinkAdapter::new()
        .plan(&validate(unsupported_presence).unwrap())
        .unwrap_err();
    assert!(error.to_string().contains("Target Capability Failure"));
}

#[test]
fn postgres15_change_event_becomes_sql() {
    let plan = postgresql_15::sql(&fixture()).unwrap();
    assert_eq!(plan.source_transaction_id(), "pg:42:0/16B6C80");
    assert_eq!(
        plan.script(),
        "-- cdc transaction=pg:42:0/16B6C80 target=postgresql-15\n\
BEGIN;\n\
INSERT INTO \"public\".\"cdc_contract\" (\"id\", \"message\") VALUES (1, convert_from(decode('E4B8ADE69687275C', 'hex'), 'UTF8'));\n\
UPDATE \"public\".\"cdc_contract\" SET \"id\" = 2 WHERE \"id\" = 1;\n\
DELETE FROM \"public\".\"cdc_contract\" WHERE \"id\" = 2;\n\
COMMIT;\n"
    );
}

#[test]
fn postgres15_rejects_keyless_mysql_change_event() {
    let transaction = validate(ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
        },
        id: "430c326c-ab91-11f1-a23b-0242ac160004:42".into(),
        begin_cursor: mysql_cursor(100),
        commit_cursor: mysql_cursor(200),
        changes: vec![RowChange {
            database: None,
            schema: "public".into(),
            table: "keyless".into(),
            operation: Operation::Insert,
            source_cursor: mysql_cursor(110),
            source_timestamp: 1_700_000_000,
            schema_basis: "mysql catalog".into(),
            before: None,
            after: Some(vec![column(0, "message", None, text("value"))]),
        }],
    })
    .unwrap();
    assert!(postgresql_15::sql(&transaction).is_err());
}

fn typed_column(ordinal: usize, name: &str, native_type: &str, datum: Datum) -> ColumnDatum {
    ColumnDatum {
        ordinal,
        name: name.into(),
        native_type: native_type.into(),
        primary_key_ordinal: if ordinal == 0 { Some(0) } else { None },
        generated: false,
        collation: None,
        datum,
    }
}

fn all_types_fixture() -> change_event::ValidatedTransaction {
    let row = vec![
        typed_column(
            0,
            "id",
            "bigint",
            Datum::Value(LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "1".into(),
            }),
        ),
        typed_column(
            1,
            "flag",
            "boolean",
            Datum::Value(LogicalValue::Boolean { value: true }),
        ),
        typed_column(
            2,
            "token",
            "uuid",
            Datum::Value(LogicalValue::Uuid {
                value: "12345678-1234-1234-1234-123456789abc".into(),
            }),
        ),
        typed_column(
            3,
            "amount",
            "numeric(30,6)",
            Datum::Value(LogicalValue::Decimal {
                unscaled: "-1".into(),
                scale: 6,
            }),
        ),
        typed_column(
            4,
            "ratio",
            "double precision",
            Datum::Value(LogicalValue::Float {
                bits: 64,
                ieee754_hex: "3ff4000000000000".into(),
            }),
        ),
        typed_column(
            5,
            "payload",
            "bytea",
            Datum::Value(LogicalValue::Binary {
                bytes_base64url: "AP8".into(),
            }),
        ),
        typed_column(
            6,
            "day",
            "date",
            Datum::Value(LogicalValue::Date {
                year: 2026,
                month: 9,
                day: 14,
            }),
        ),
        typed_column(
            7,
            "local_time",
            "timestamp(6) without time zone",
            Datum::Value(LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 14,
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 123_456,
            }),
        ),
        typed_column(
            8,
            "observed_at",
            "timestamp(6) with time zone",
            Datum::Value(LogicalValue::Instant {
                unix_seconds: "1700000000".into(),
                nanoseconds: 123_456_000,
            }),
        ),
        typed_column(
            9,
            "metadata",
            "jsonb",
            Datum::Value(LogicalValue::Json {
                value: JsonValue::Object(vec![JsonEntry {
                    key: "n".into(),
                    value: JsonValue::Decimal {
                        unscaled: "12345678901234567890123456".into(),
                        scale: 6,
                    },
                }]),
            }),
        ),
        typed_column(10, "note", "text", Datum::Null),
    ];
    validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15.14".into(),
            id: "postgresql:1:1:1".into(),
        },
        id: "pg:43:0/16B6D80".into(),
        begin_cursor: cursor("0/16B6D50"),
        commit_cursor: cursor("0/16B6D80"),
        changes: vec![RowChange {
            database: Some("CDC_test".into()),
            schema: "public".into(),
            table: "typed_contract".into(),
            operation: Operation::Insert,
            source_cursor: cursor("0/16B6D80"),
            source_timestamp: 1_700_000_000,
            schema_basis: "pgoutput+catalog:2".into(),
            before: None,
            after: Some(row),
        }],
    })
    .unwrap()
}

#[test]
fn postgres15_renders_supported_logical_values() {
    assert_eq!(
        postgresql_15::sql(&all_types_fixture()).unwrap().script(),
        "-- cdc transaction=pg:43:0/16B6D80 target=postgresql-15\n\
BEGIN;\n\
INSERT INTO \"public\".\"typed_contract\" (\"id\", \"flag\", \"token\", \"amount\", \"ratio\", \"payload\", \"day\", \"local_time\", \"observed_at\", \"metadata\", \"note\") VALUES (1, TRUE, UUID '12345678-1234-1234-1234-123456789abc', -0.000001, 1.25, decode('00FF', 'hex'), DATE '2026-09-14', TIMESTAMP '2026-09-14 01:02:03.123456', (TIMESTAMPTZ 'epoch' + 1700000000 * INTERVAL '1 second' + 123456 * INTERVAL '1 microsecond'), '{\"n\":12345678901234567890.123456}'::jsonb, NULL);\n\
COMMIT;\n"
    );
}

#[test]
fn postgres15_preserves_float_sign_and_rejects_timestamp_precision_loss() {
    let mut negative_zero = all_types_fixture().transaction().clone();
    let ratio = negative_zero.changes[0]
        .after
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|column| column.name == "ratio")
        .unwrap();
    ratio.datum = Datum::Value(LogicalValue::Float {
        bits: 64,
        ieee754_hex: "8000000000000000".into(),
    });
    let rendered = postgresql_15::sql(&validate(negative_zero).unwrap())
        .unwrap()
        .script();
    assert!(rendered.contains(", -0.0, decode("));

    let mut nanoseconds = all_types_fixture().transaction().clone();
    let observed = nanoseconds.changes[0]
        .after
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|column| column.name == "observed_at")
        .unwrap();
    observed.datum = Datum::Value(LogicalValue::Instant {
        unix_seconds: "1700000000".into(),
        nanoseconds: 123_456_789,
    });
    assert!(postgresql_15::sql(&validate(nanoseconds).unwrap()).is_err());
}

fn transaction_with_changes(
    validated: &change_event::ValidatedTransaction,
    indexes: &[usize],
) -> change_event::ValidatedTransaction {
    let mut transaction = validated.transaction().clone();
    transaction.changes = indexes
        .iter()
        .map(|index| transaction.changes[*index].clone())
        .collect();
    validate(transaction).unwrap()
}

async fn writes_sql_transaction_for_version(
    target_version: &'static str,
) -> postgresql_15::Result<()> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_sink_pg{target_version}_{tag}");
    let mut setup =
        PgConnection::connect_with(&postgres_env::options(target_version, &admin, &password))
            .await?;
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&mut setup)
        .await?;
    assert_eq!(server_version.split('.').next(), Some(target_version));
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.\"{table}\" (
            id bigint PRIMARY KEY,
            message text NOT NULL
        )"
    )))
    .execute(&mut setup)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON public.\"{table}\" TO \"{}\"",
        writer.replace('"', "\"\"")
    )))
    .execute(&mut setup)
    .await?;

    let result = tokio::spawn({
        let table = table.clone();
        let password = password.clone();
        let writer = writer.clone();
        async move {
            let config = TargetConfig::new(
                postgres_env::setting(target_version, "HOST", "192.168.0.10"),
                "CDC_test",
                writer,
                password,
            )
            .with_port(
                postgres_env::setting(target_version, "PORT", "54321")
                    .parse()
                    .expect("invalid PostgreSQL test port"),
            );
            let sink = SinkAdapter::new_for_version(target_version);
            for source_fixture in [
                fixture(),
                postgres_fixture("16.10"),
                postgres_fixture("17.6"),
                mysql_fixture("5.7.44"),
                mysql_fixture("8.0.46"),
                mysql_fixture("8.4.8"),
            ] {
                let mut full = source_fixture.transaction().clone();
                for change in &mut full.changes {
                    change.table.clone_from(&table);
                }
                let full = validate(full)?;

                let insert = transaction_with_changes(&full, &[0]);
                assert_eq!(
                    execute(&config, &sink.plan(&insert)?)
                        .await?
                        .statements_executed,
                    1
                );
                let mut verify = PgConnection::connect_with(&postgres_env::options(
                    target_version,
                    &postgres_env::setting(target_version, "ADMIN_USER", "postgres"),
                    &config.password,
                ))
                .await?;
                let row = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SELECT id,message FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(row.try_get::<i64, _>("id")?, 1);
                assert_eq!(row.try_get::<String, _>("message")?, "中文'\\");

                let mut keyless = insert.transaction().clone();
                if let Some(image) = keyless.changes[0].before.as_mut() {
                    for column in image {
                        column.primary_key_ordinal = None;
                    }
                }
                if let Some(image) = keyless.changes[0].after.as_mut() {
                    for column in image {
                        column.primary_key_ordinal = None;
                    }
                }
                let keyless = validate(keyless)?;
                assert!(sink.plan(&keyless).is_err());
                let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(count, 1, "capability failure must not change target data");

                let update = transaction_with_changes(&full, &[1]);
                assert_eq!(
                    execute(&config, &sink.plan(&update)?)
                        .await?
                        .statements_executed,
                    1
                );
                let row = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SELECT id,message FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(row.try_get::<i64, _>("id")?, 2);
                assert_eq!(
                    row.try_get::<String, _>("message")?,
                    "中文'\\",
                    "unchanged TOAST value must be preserved"
                );

                let delete = transaction_with_changes(&full, &[2]);
                assert_eq!(
                    execute(&config, &sink.plan(&delete)?)
                        .await?
                        .statements_executed,
                    1
                );
                let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(count, 0);

                let duplicate = transaction_with_changes(&full, &[0, 0]);
                assert!(execute(&config, &sink.plan(&duplicate)?).await.is_err());
                let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(
                    count, 0,
                    "failed source transaction must roll back every row"
                );
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
    })
    .await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut setup)
    .await?;
    result??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_writes_sql_transaction() -> postgresql_15::Result<()> {
    writes_sql_transaction_for_version("15").await
}

async fn writes_native_type_values_for_version(
    target_version: &'static str,
) -> postgresql_15::Result<()> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_native_sink_pg{target_version}_{tag}");
    let mut setup =
        PgConnection::connect_with(&postgres_env::options(target_version, &admin, &password))
            .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.\"{table}\" (
            id bigint PRIMARY KEY,
            flag boolean,
            token uuid,
            amount numeric(30,6),
            ratio double precision,
            payload bytea,
            day date,
            local_time timestamp(6) without time zone,
            observed_at timestamp(6) with time zone,
            metadata jsonb,
            note text
        )"
    )))
    .execute(&mut setup)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON public.\"{table}\" TO \"{}\"",
        writer.replace('"', "\"\"")
    )))
    .execute(&mut setup)
    .await?;

    let result = async {
        let mut insert = all_types_fixture().transaction().clone();
        insert.changes[0].table.clone_from(&table);
        let insert = validate(insert)?;
        let config = TargetConfig::new(
            postgres_env::setting(target_version, "HOST", "192.168.0.10"),
            "CDC_test",
            writer,
            password,
        )
        .with_port(
            postgres_env::setting(target_version, "PORT", "54321")
                .parse()
                .expect("invalid PostgreSQL test port"),
        );
        let sink = SinkAdapter::new_for_version(target_version);
        let applied = execute(&config, &sink.plan(&insert)?).await?;
        assert_eq!(applied.statements_executed, 1);

        let mut verify = PgConnection::connect_with(&postgres_env::options(
            target_version,
            &admin,
            &config.password,
        ))
        .await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id, flag, token::text AS token, amount::text AS amount,
                    ratio, payload, day::text AS day, local_time::text AS local_time,
                    extract(epoch FROM observed_at)::numeric::text AS observed_epoch,
                    metadata->>'n' AS json_number, note
             FROM public.\"{table}\""
        )))
        .fetch_one(&mut verify)
        .await?;
        assert_eq!(row.try_get::<i64, _>("id")?, 1);
        assert!(row.try_get::<bool, _>("flag")?);
        assert_eq!(
            row.try_get::<String, _>("token")?,
            "12345678-1234-1234-1234-123456789abc"
        );
        assert_eq!(row.try_get::<String, _>("amount")?, "-0.000001");
        assert_eq!(row.try_get::<f64, _>("ratio")?, 1.25);
        assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [0, 255]);
        assert_eq!(row.try_get::<String, _>("day")?, "2026-09-14");
        assert_eq!(
            row.try_get::<String, _>("local_time")?,
            "2026-09-14 01:02:03.123456"
        );
        assert_eq!(
            row.try_get::<String, _>("observed_epoch")?,
            "1700000000.123456"
        );
        assert_eq!(
            row.try_get::<String, _>("json_number")?,
            "12345678901234567890.123456"
        );
        assert!(row.try_get::<Option<String>, _>("note")?.is_none());

        let mut update = insert.transaction().clone();
        let change = &mut update.changes[0];
        change.operation = Operation::Update;
        change.before = change.after.clone();
        let mut after = change.before.clone().expect("insert image");
        after
            .iter_mut()
            .find(|column| column.name == "note")
            .expect("note field")
            .datum = text("updated");
        change.after = Some(after);
        let update = validate(update)?;
        assert_eq!(
            execute(&config, &sink.plan(&update)?)
                .await?
                .statements_executed,
            1
        );
        let note: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT note FROM public.\"{table}\" WHERE id=1"
        )))
        .fetch_one(&mut verify)
        .await?;
        assert_eq!(note.as_deref(), Some("updated"));

        let mut delete = insert.transaction().clone();
        let change = &mut delete.changes[0];
        change.operation = Operation::Delete;
        change.before = change.after.clone();
        change.after = None;
        let delete = validate(delete)?;
        assert_eq!(
            execute(&config, &sink.plan(&delete)?)
                .await?
                .statements_executed,
            1
        );
        let remaining: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM public.\"{table}\""
        )))
        .fetch_one(&mut verify)
        .await?;
        assert_eq!(remaining, 0);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut setup)
    .await?;
    result?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_writes_native_type_values() -> postgresql_15::Result<()> {
    writes_native_type_values_for_version("15").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_writes_native_type_values() -> postgresql_15::Result<()> {
    writes_native_type_values_for_version("16").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_writes_native_type_values() -> postgresql_15::Result<()> {
    writes_native_type_values_for_version("17").await
}

async fn checkpoint_and_dml_for_version(target_version: &'static str) -> postgresql_15::Result<()> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer_name = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin_name = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_checkpoint_pg{target_version}_{tag}");
    let task = format!("test_checkpoint_pg{target_version}_{tag}");
    let source_uuid = "postgresql:123456:1:16384:Q0RDX3Rlc3Q";
    let binding = "a".repeat(64);
    let config = TargetConfig::new(
        postgres_env::setting(target_version, "HOST", "192.168.0.10"),
        "CDC_test",
        writer_name.clone(),
        password.clone(),
    )
    .with_port(
        postgres_env::setting(target_version, "PORT", "54321")
            .parse()
            .expect("invalid PostgreSQL test port"),
    );
    let sink = SinkAdapter::new_for_version(target_version);
    let mut admin = PgConnection::connect_with(&postgres_env::options(
        target_version,
        &admin_name,
        &password,
    ))
    .await?;
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&mut admin)
        .await?;
    assert_eq!(server_version.split('.').next(), Some(target_version));

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.\"{table}\" (id bigint PRIMARY KEY, message text NOT NULL)"
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON public.\"{table}\" TO \"{}\"",
        writer_name.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;

    let mut full = fixture().transaction().clone();
    full.source.id = source_uuid.into();
    full.id = "pg:42:0/16B6C80".into();
    for change in &mut full.changes {
        change.table = table.clone();
        change.schema = "public".into();
        change.source_cursor = cursor("0/16B6C80");
        change.schema_basis = "pgoutput+catalog:42".into();
        change.database = Some("CDC_test".into());
    }
    let full = validate(full)?;
    let transaction_at = |indexes: &[usize], lsn: &str| {
        let mut transaction = full.transaction().clone();
        transaction.id = format!("pg:42:{lsn}");
        transaction.commit_cursor = cursor(lsn);
        transaction.changes = indexes
            .iter()
            .map(|index| {
                let mut change = transaction.changes[*index].clone();
                change.source_cursor = cursor(lsn);
                change
            })
            .collect();
        validate(transaction).unwrap()
    };
    let insert = transaction_at(&[0], "0/16B6C80");
    let mut delete_transaction = full.transaction().clone();
    delete_transaction.id = "pg:42:0/16B6D80".into();
    delete_transaction.commit_cursor = cursor("0/16B6D80");
    let mut delete_change = delete_transaction.changes.remove(0);
    delete_change.operation = Operation::Delete;
    delete_change.before = delete_change.after.take();
    delete_change.source_cursor = cursor("0/16B6D80");
    delete_transaction.changes = vec![delete_change];
    let delete = validate(delete_transaction)?;
    let duplicate = transaction_at(&[0, 0], "0/16B6C80");
    let initial_position = u64::from_str_radix("16B6C50", 16).unwrap();

    let first = std::thread::spawn({
        let config = config.clone();
        let task = task.clone();
        let binding = binding.clone();
        move || -> postgresql_15::Result<u64> {
            let mut checkpoint = postgresql_15::CheckpointWriter::open_for_version(
                &config,
                &task,
                source_uuid,
                &binding,
                target_version,
            )?;
            let initialized =
                checkpoint.initialize("postgresql_lsn", "0/16B6C50", initial_position, None)?;
            assert_eq!(initialized.position, initial_position);
            assert!(checkpoint.apply(&sink.plan(&duplicate)?).is_err());
            Ok(checkpoint.checkpoint().unwrap().position)
        }
    })
    .join()
    .map_err(|_| std::io::Error::other("checkpoint worker panicked"))??;
    assert_eq!(first, initial_position);
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM public.\"{table}\""
    )))
    .fetch_one(&mut admin)
    .await?;
    assert_eq!(
        count, 0,
        "failed DML must roll back before checkpoint update"
    );

    let second = std::thread::spawn({
        let config = config.clone();
        let task = task.clone();
        let binding = binding.clone();
        move || -> postgresql_15::Result<(u64, u64)> {
            let mut checkpoint = postgresql_15::CheckpointWriter::open_for_version(
                &config,
                &task,
                source_uuid,
                &binding,
                target_version,
            )?;
            let applied = checkpoint.apply(&sink.plan(&insert)?)?;
            assert_eq!(applied.statements_executed, 1);
            assert_eq!(applied.checkpoint.applied_rows, 1);
            drop(checkpoint);

            let mut restarted = postgresql_15::CheckpointWriter::open_for_version(
                &config,
                &task,
                source_uuid,
                &binding,
                target_version,
            )?;
            assert!(restarted.apply(&sink.plan(&insert)?)?.already_applied);
            let deleted = restarted.apply(&sink.plan(&delete)?)?;
            assert_eq!(deleted.statements_executed, 1);
            let position = deleted.checkpoint.position;
            drop(restarted);

            let mut recovered = postgresql_15::CheckpointWriter::open_for_version(
                &config,
                &task,
                source_uuid,
                &binding,
                target_version,
            )?;
            assert_eq!(recovered.checkpoint().unwrap().applied_rows, 2);
            assert!(recovered.apply(&sink.plan(&delete)?)?.already_applied);
            Ok((position, recovered.checkpoint().unwrap().applied_rows))
        }
    })
    .join()
    .map_err(|_| std::io::Error::other("checkpoint worker panicked"))??;
    assert!(second.0 > initial_position);
    assert_eq!(second.1, 2);
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM public.\"{table}\""
    )))
    .fetch_one(&mut admin)
    .await?;
    assert_eq!(count, 0);

    sqlx::query("DELETE FROM cdc.log_info WHERE task_id = $1")
        .bind(&task)
        .execute(&mut admin)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut admin)
    .await?;
    Ok(())
}

async fn checkpoint_open_with_composite_row_type_dependency(
    target_version: &'static str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer_name = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin_name = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("checkpoint_log_info_rowtype_dependency_{tag}");
    let first_task = format!("checkpoint_type_dependency_first_{tag}");
    let second_task = format!("checkpoint_type_dependency_second_{tag}");
    let source_uuid = "postgresql:123456:1:16384:Q0RDX3Rlc3Q";
    let binding = "b".repeat(64);
    let config = TargetConfig::new(
        postgres_env::setting(target_version, "HOST", "192.168.0.10"),
        "CDC_test",
        writer_name,
        password.clone(),
    )
    .with_port(
        postgres_env::setting(target_version, "PORT", "54321")
            .parse()
            .expect("invalid PostgreSQL test port"),
    );
    let mut admin = PgConnection::connect_with(&postgres_env::options(
        target_version,
        &admin_name,
        &password,
    ))
    .await?;

    let open_checkpoint = |task_id: String| {
        let config = config.clone();
        let binding = binding.clone();
        std::thread::spawn(move || -> postgresql_15::Result<()> {
            let checkpoint = postgresql_15::CheckpointWriter::open_for_version(
                &config,
                &task_id,
                source_uuid,
                &binding,
                target_version,
            )?;
            drop(checkpoint);
            Ok(())
        })
        .join()
        .map_err(|_| io::Error::other("checkpoint worker panicked"))?
    };

    open_checkpoint(first_task.clone())?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.\"{table}\" (checkpoint_row cdc.log_info[])"
    )))
    .execute(&mut admin)
    .await?;

    let reopened = open_checkpoint(second_task.clone());

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query("DELETE FROM cdc.log_info WHERE task_id = ANY($1)")
        .bind(vec![first_task, second_task])
        .execute(&mut admin)
        .await?;

    reopened?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_checkpoint_open_preserves_referenced_log_info_row_type()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    checkpoint_open_with_composite_row_type_dependency("15").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_checkpoint_open_preserves_referenced_log_info_row_type()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    checkpoint_open_with_composite_row_type_dependency("16").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_checkpoint_open_preserves_referenced_log_info_row_type()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    checkpoint_open_with_composite_row_type_dependency("17").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_checkpoint_and_dml_commit_atomically_across_restart()
-> postgresql_15::Result<()> {
    checkpoint_and_dml_for_version("15").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_checkpoint_and_dml_commit_atomically_across_restart()
-> postgresql_15::Result<()> {
    checkpoint_and_dml_for_version("16").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_checkpoint_and_dml_commit_atomically_across_restart()
-> postgresql_15::Result<()> {
    checkpoint_and_dml_for_version("17").await
}

fn postgis_point(x: f64, y: f64) -> Datum {
    let mut ewkb = vec![1, 1, 0, 0, 0x20];
    ewkb.extend_from_slice(&4326_i32.to_le_bytes());
    ewkb.extend_from_slice(&x.to_le_bytes());
    ewkb.extend_from_slice(&y.to_le_bytes());
    Datum::Value(LogicalValue::Spatial {
        format: SpatialFormat::Ewkb,
        bytes_base64url: URL_SAFE_NO_PAD.encode(ewkb),
        geometry_type: "point".into(),
        dimensions: 2,
        srid: Some(4326),
        crs: Some("srid:4326".into()),
    })
}

fn postgis_row(x: f64, y: f64) -> Vec<ColumnDatum> {
    vec![
        typed_column(
            0,
            "id",
            "bigint",
            Datum::Value(LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "1".into(),
            }),
        ),
        typed_column(1, "planar", "geometry(Point,4326)", postgis_point(x, y)),
        typed_column(2, "geodetic", "geography(Point,4326)", postgis_point(x, y)),
    ]
}

fn postgis_transaction(
    table: &str,
    operation: Operation,
    x: f64,
    y: f64,
) -> change_event::ValidatedTransaction {
    let mut transaction = fixture().transaction().clone();
    let change = &mut transaction.changes[0];
    change.table = table.into();
    change.operation = operation;
    change.before = if matches!(operation, Operation::Insert) {
        None
    } else {
        Some(if matches!(operation, Operation::Delete) {
            postgis_row(x, y)
        } else {
            postgis_row(12.5, 45.25)
        })
    };
    change.after = if matches!(operation, Operation::Delete) {
        None
    } else {
        Some(postgis_row(x, y))
    };
    transaction.changes.truncate(1);
    validate(transaction).unwrap()
}

#[test]
fn postgis_geometry_and_geography_use_distinct_parameterized_target_expressions() {
    for version in ["15", "16", "17"] {
        let transaction = postgis_transaction("postgis_contract", Operation::Insert, 12.5, 45.25);
        let plan = SinkAdapter::new_for_version(version)
            .plan(&transaction)
            .unwrap();
        let statement = plan.statements().next().unwrap();
        assert!(statement.contains("ST_GeomFromEWKB($"));
        assert!(statement.contains("::geography"));
        let parameters = plan.parameters().next().unwrap();
        assert_eq!(
            parameters
                .iter()
                .filter(|parameter| matches!(parameter, postgresql_15::Parameter::Spatial { .. }))
                .count(),
            2
        );
    }
}

#[test]
fn native_spatial_sink_rejects_type_names_that_only_contain_postgis_words() {
    for misleading in ["geometry_archive", "mygeography", "\"geometry.cache\""] {
        let mut transaction =
            postgis_transaction("postgis_contract", Operation::Insert, 12.5, 45.25)
                .transaction()
                .clone();
        transaction.changes[0].after.as_mut().unwrap()[1].native_type = misleading.into();
        let transaction = validate(transaction).unwrap();
        let error = SinkAdapter::new().plan(&transaction).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("PostGIS geometry/geography target"),
            "{misleading} must not qualify as a native spatial target: {error}"
        );
    }
}

async fn postgis_native_sink_for_version(
    target_version: &'static str,
) -> postgresql_15::Result<()> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_postgis_sink_pg{target_version}_{tag}");
    let mut setup =
        PgConnection::connect_with(&postgres_env::options(target_version, &admin, &password))
            .await?;
    let available: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='postgis')",
    )
    .fetch_one(&mut setup)
    .await?;
    assert!(
        available,
        "PostgreSQL {target_version} native spatial sink qualification requires the PostGIS server extension package; pg_available_extensions has no postgis entry"
    );
    let installed: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis')")
            .fetch_one(&mut setup)
            .await?;
    if !installed {
        sqlx::query("CREATE EXTENSION postgis")
            .execute(&mut setup)
            .await?;
    }
    let setup_result = async {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE public.\"{table}\" (id bigint PRIMARY KEY, planar geometry(Point,4326), geodetic geography(Point,4326))"
        )))
        .execute(&mut setup)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON public.\"{table}\" TO \"{}\"",
            writer.replace('"', "\"\"")
        )))
        .execute(&mut setup)
        .await?;
        Ok::<_, sqlx::Error>(())
    }.await;
    if let Err(error) = setup_result {
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE IF EXISTS public.\"{table}\""
        )))
        .execute(&mut setup)
        .await;
        if !installed {
            let _ = sqlx::query("DROP EXTENSION IF EXISTS postgis")
                .execute(&mut setup)
                .await;
        }
        return Err(Box::new(error));
    }
    let result = tokio::spawn({
        let table = table.clone();
        let password = password.clone();
        let writer = writer.clone();
        let admin = admin.clone();
        async move {
        let config = TargetConfig::new(
            postgres_env::setting(target_version, "HOST", "192.168.0.10"),
            "CDC_test",
            writer,
            password,
        )
        .with_port(postgres_env::setting(target_version, "PORT", "54321").parse()?);
        let sink = SinkAdapter::new_for_version(target_version);
        let insert = postgis_transaction(&table, Operation::Insert, 12.5, 45.25);
        assert_eq!(execute(&config, &sink.plan(&insert)?).await?.statements_executed, 1);
        let mut verify = PgConnection::connect_with(&postgres_env::options(target_version, &admin, &config.password)).await?;
        let query = format!("SELECT ST_AsText(planar) AS planar, ST_AsText(geodetic::geometry) AS geodetic, ST_SRID(planar) AS planar_srid, ST_SRID(geodetic::geometry) AS geodetic_srid FROM public.\"{table}\"");
        let row = sqlx::query(sqlx::AssertSqlSafe(query.clone())).fetch_one(&mut verify).await?;
        assert_eq!(row.try_get::<String, _>("planar")?, "POINT(12.5 45.25)");
        assert_eq!(row.try_get::<String, _>("geodetic")?, "POINT(12.5 45.25)");
        assert_eq!(row.try_get::<i32, _>("planar_srid")?, 4326);
        assert_eq!(row.try_get::<i32, _>("geodetic_srid")?, 4326);

        let update = postgis_transaction(&table, Operation::Update, 13.5, 46.25);
        assert_eq!(execute(&config, &sink.plan(&update)?).await?.statements_executed, 1);
        let row = sqlx::query(sqlx::AssertSqlSafe(query)).fetch_one(&mut verify).await?;
        assert_eq!(row.try_get::<String, _>("planar")?, "POINT(13.5 46.25)");
        assert_eq!(row.try_get::<String, _>("geodetic")?, "POINT(13.5 46.25)");

        let delete = postgis_transaction(&table, Operation::Delete, 13.5, 46.25);
        assert_eq!(execute(&config, &sink.plan(&delete)?).await?.statements_executed, 1);
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM public.\"{table}\""))).fetch_one(&mut verify).await?;
        assert_eq!(count, 0);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
    }).await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut setup)
    .await?;
    if !installed {
        sqlx::query("DROP EXTENSION postgis")
            .execute(&mut setup)
            .await?;
    }
    result??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL 15 and PostGIS server package"]
async fn postgres15_postgis_native_sink() -> postgresql_15::Result<()> {
    postgis_native_sink_for_version("15").await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 16 and PostGIS server package"]
async fn postgres16_postgis_native_sink() -> postgresql_15::Result<()> {
    postgis_native_sink_for_version("16").await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 and PostGIS server package"]
async fn postgres17_postgis_native_sink() -> postgresql_15::Result<()> {
    postgis_native_sink_for_version("17").await
}

async fn user_defined_spatial_name_probe_for_version(
    target_version: &'static str,
) -> postgresql_15::Result<()> {
    let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))?;
    let writer = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
    let admin = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let schema = format!("cdc_spatial_name_pg{target_version}_{tag}");
    let mut setup =
        PgConnection::connect_with(&postgres_env::options(target_version, &admin, &password))
            .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut setup)
        .await?;
    let result = async {
        for name in ["geometry_archive", "geometry"] {
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE TYPE {schema}.{name} AS ENUM ('sample')")))
                .execute(&mut setup).await?;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.events (id bigint PRIMARY KEY, misleading {schema}.geometry_archive, same_name {schema}.geometry)"
        )))
        .execute(&mut setup).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA {schema} TO \"{}\"",
            writer.replace('"', "\"\"")
        )))
        .execute(&mut setup).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "GRANT SELECT ON {schema}.events TO \"{}\"",
            writer.replace('"', "\"\"")
        )))
        .execute(&mut setup).await?;
        let config = TargetConfig::new(
            postgres_env::setting(target_version, "HOST", "192.168.0.10"),
            "CDC_test",
            writer,
            password,
        )
        .with_port(postgres_env::setting(target_version, "PORT", "54321").parse()?);
        for column in ["misleading", "same_name"] {
            let probe = postgresql_15::probe_target_for_version(
                &config,
                &schema,
                "events",
                column,
                target_version,
            ).await?;
            if probe.capabilities[0].status != CapabilityProbeStatus::Detected
                || probe.column_metadata.extension.is_some()
                || !probe.session.settings.contains_key("search_path")
            {
                return Err(io::Error::other(format!(
                    "PostgreSQL {target_version} {column} was incorrectly classified as PostGIS: {probe:?}"
                )).into());
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&mut setup)
    .await?;
    result?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_user_defined_spatial_name_probe() -> postgresql_15::Result<()> {
    user_defined_spatial_name_probe_for_version("15").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_user_defined_spatial_name_probe() -> postgresql_15::Result<()> {
    user_defined_spatial_name_probe_for_version("16").await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_user_defined_spatial_name_probe() -> postgresql_15::Result<()> {
    user_defined_spatial_name_probe_for_version("17").await
}
