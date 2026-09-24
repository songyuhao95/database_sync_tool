use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue, Operation,
    RowChange, SinkAdapter as _, Source, SourceCursor, TargetCapabilityFailure, validate,
};
use postgresql_15::{SinkAdapter, TargetConfig, execute};
use sqlx::{
    Connection, PgConnection, Row,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn column(ordinal: usize, name: &str, key: Option<usize>, datum: Datum) -> ColumnDatum {
    ColumnDatum {
        ordinal,
        name: name.into(),
        native_type: if name == "id" { "bigint" } else { "text" }.into(),
        primary_key_ordinal: key,
        generated: false,
        collation: None,
        datum,
    }
}

fn id(value: &str) -> ColumnDatum {
    column(
        0,
        "id",
        Some(0),
        Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: value.into(),
        }),
    )
}

fn message(datum: Datum) -> ColumnDatum {
    column(1, "message", None, datum)
}

fn text(value: &str) -> Datum {
    Datum::Value(LogicalValue::Text {
        charset: "UTF8".into(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(value),
        text: Some(value.into()),
    })
}

fn fixture() -> change_event::ValidatedTransaction {
    validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15.14".into(),
            id: "postgresql:1:1:1".into(),
        },
        id: "pg:42:0/16B6C80".into(),
        begin_cursor: cursor("0/16B6C50"),
        commit_cursor: cursor("0/16B6C80"),
        changes: vec![
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Insert,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_000,
                schema_basis: "pgoutput+catalog:1".into(),
                before: None,
                after: Some(vec![id("1"), message(text("中文'\\"))]),
            },
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Update,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_001,
                schema_basis: "pgoutput+catalog:1".into(),
                before: Some(vec![id("1"), message(Datum::Unavailable)]),
                after: Some(vec![id("2"), message(Datum::Unchanged)]),
            },
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Delete,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_002,
                schema_basis: "pgoutput+catalog:1".into(),
                before: Some(vec![id("2"), message(Datum::Unavailable)]),
                after: None,
            },
        ],
    })
    .unwrap()
}

fn mysql_fixture(version: &str) -> change_event::ValidatedTransaction {
    let mut transaction = fixture().transaction().clone();
    transaction.source = Source {
        kind: "mysql".into(),
        version: version.into(),
        id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
    };
    transaction.begin_cursor = mysql_cursor(100);
    transaction.commit_cursor = mysql_cursor(200);
    for (index, change) in transaction.changes.iter_mut().enumerate() {
        change.source_cursor = mysql_cursor(110 + index as u32 * 10);
        change.schema_basis = "mysql canonical fixture".into();
    }
    validate(transaction).unwrap()
}

fn postgres_fixture(version: &str) -> change_event::ValidatedTransaction {
    let mut transaction = fixture().transaction().clone();
    transaction.source.version = version.into();
    validate(transaction).unwrap()
}

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

fn mysql_cursor(position: u32) -> SourceCursor {
    let mut bytes = b"mysql-bin.000001\0".to_vec();
    bytes.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(bytes),
        display: format!("mysql-bin.000001:{position}"),
    }
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

fn setting(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

fn options(user: &str, password: &str) -> PgConnectOptions {
    PgConnectOptions::new()
        .host(&setting("PG_CDC_HOST", "192.168.0.10"))
        .port(setting("PG_CDC_PORT", "54321").parse().unwrap())
        .database("CDC_test")
        .username(user)
        .password(password)
        .ssl_mode(PgSslMode::Prefer)
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

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 15 test database"]
async fn postgres15_writes_sql_transaction() -> postgresql_15::Result<()> {
    let password = env::var("PG_CDC_TEST_PASSWORD")?;
    let writer = setting("PG_CDC_WRITER_USER", "postgresql_writer");
    let admin = setting("PG_CDC_ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_sink_{tag}");
    let mut setup = PgConnection::connect_with(&options(&admin, &password)).await?;
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
                setting("PG_CDC_HOST", "192.168.0.10"),
                "CDC_test",
                writer,
                password,
            )
            .with_port(setting("PG_CDC_PORT", "54321").parse().unwrap());
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
                    execute(&config, &SinkAdapter::new().plan(&insert)?)
                        .await?
                        .statements_executed,
                    1
                );
                let mut verify = PgConnection::connect_with(&options(
                    &setting("PG_CDC_ADMIN_USER", "postgres"),
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
                assert!(SinkAdapter::new().plan(&keyless).is_err());
                let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.\"{table}\""
                )))
                .fetch_one(&mut verify)
                .await?;
                assert_eq!(count, 1, "capability failure must not change target data");

                let update = transaction_with_changes(&full, &[1]);
                assert_eq!(
                    execute(&config, &SinkAdapter::new().plan(&update)?)
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
                    execute(&config, &SinkAdapter::new().plan(&delete)?)
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
                assert!(
                    execute(&config, &SinkAdapter::new().plan(&duplicate)?)
                        .await
                        .is_err()
                );
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
async fn postgres15_checkpoint_and_dml_commit_atomically_across_restart()
-> postgresql_15::Result<()> {
    let password = env::var("PG_CDC_TEST_PASSWORD")?;
    let writer_name = setting("PG_CDC_WRITER_USER", "postgresql_writer");
    let admin_name = setting("PG_CDC_ADMIN_USER", "postgres");
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_checkpoint_{tag}");
    let task = format!("test_checkpoint_{tag}");
    let source_uuid = "postgresql:123456:1:16384:Q0RDX3Rlc3Q";
    let binding = "a".repeat(64);
    let config = TargetConfig::new(
        setting("PG_CDC_HOST", "192.168.0.10"),
        "CDC_test",
        writer_name.clone(),
        password.clone(),
    )
    .with_port(setting("PG_CDC_PORT", "54321").parse().unwrap());
    let mut admin = PgConnection::connect_with(&options(&admin_name, &password)).await?;

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
            let mut checkpoint =
                postgresql_15::CheckpointWriter::open(&config, &task, source_uuid, &binding)?;
            let initialized =
                checkpoint.initialize("postgresql_lsn", "0/16B6C50", initial_position, None)?;
            assert_eq!(initialized.position, initial_position);
            assert!(
                checkpoint
                    .apply(&SinkAdapter::new().plan(&duplicate)?)
                    .is_err()
            );
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
            let mut checkpoint =
                postgresql_15::CheckpointWriter::open(&config, &task, source_uuid, &binding)?;
            let applied = checkpoint.apply(&SinkAdapter::new().plan(&insert)?)?;
            assert_eq!(applied.statements_executed, 1);
            assert_eq!(applied.checkpoint.applied_rows, 1);
            drop(checkpoint);

            let mut restarted =
                postgresql_15::CheckpointWriter::open(&config, &task, source_uuid, &binding)?;
            assert!(
                restarted
                    .apply(&SinkAdapter::new().plan(&insert)?)?
                    .already_applied
            );
            let deleted = restarted.apply(&SinkAdapter::new().plan(&delete)?)?;
            assert_eq!(deleted.statements_executed, 1);
            let position = deleted.checkpoint.position;
            drop(restarted);

            let mut recovered =
                postgresql_15::CheckpointWriter::open(&config, &task, source_uuid, &binding)?;
            assert_eq!(recovered.checkpoint().unwrap().applied_rows, 2);
            assert!(
                recovered
                    .apply(&SinkAdapter::new().plan(&delete)?)?
                    .already_applied
            );
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
