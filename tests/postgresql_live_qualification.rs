use change_event::{Datum, LogicalValue, SinkAdapter as _, ValidatedTransaction, validate};
use postgresql_15::{CancellationToken, Config, Replication, TargetConfig};
use sqlx::{Connection, PgConnection};
use std::{env, error::Error, future::Future, io, time::Duration};

#[path = "support/postgres_env.rs"]
mod postgres_env;
#[path = "support/postgres_sink_fixtures.rs"]
mod sink_fixtures;

fn source_datum<'a>(transaction: &'a ValidatedTransaction, column: &str) -> &'a Datum {
    let value = transaction.transaction().changes[0]
        .after
        .as_ref()
        .expect("captured insert has an after image")
        .iter()
        .find(|value| value.name == column)
        .unwrap_or_else(|| panic!("source image has column {column}"));
    &value.datum
}

trait VersionedPublicPostgresApi {
    const MAJOR: &'static str;

    fn replication(
        config: Config,
    ) -> impl Future<Output = postgresql_15::Result<Replication>> + Send;

    fn plan(transaction: &ValidatedTransaction) -> io::Result<postgresql_15::SqlTransaction>;

    fn execute<'a>(
        config: &'a TargetConfig,
        plan: &'a postgresql_15::SqlTransaction,
    ) -> impl Future<Output = io::Result<postgresql_15::ApplyResult>> + Send + 'a;
}

struct Postgresql16Api;
struct Postgresql17Api;

impl VersionedPublicPostgresApi for Postgresql16Api {
    const MAJOR: &'static str = "16";

    fn replication(
        config: Config,
    ) -> impl Future<Output = postgresql_15::Result<Replication>> + Send {
        postgresql_16::replication(config)
    }

    fn plan(transaction: &ValidatedTransaction) -> io::Result<postgresql_15::SqlTransaction> {
        postgresql_16::SinkAdapter::new().plan(transaction)
    }

    fn execute<'a>(
        config: &'a TargetConfig,
        plan: &'a postgresql_15::SqlTransaction,
    ) -> impl Future<Output = io::Result<postgresql_15::ApplyResult>> + Send + 'a {
        postgresql_16::execute(config, plan)
    }
}

impl VersionedPublicPostgresApi for Postgresql17Api {
    const MAJOR: &'static str = "17";

    fn replication(
        config: Config,
    ) -> impl Future<Output = postgresql_15::Result<Replication>> + Send {
        postgresql_17::replication(config)
    }

    fn plan(transaction: &ValidatedTransaction) -> io::Result<postgresql_15::SqlTransaction> {
        postgresql_17::SinkAdapter::new().plan(transaction)
    }

    fn execute<'a>(
        config: &'a TargetConfig,
        plan: &'a postgresql_15::SqlTransaction,
    ) -> impl Future<Output = io::Result<postgresql_15::ApplyResult>> + Send + 'a {
        postgresql_17::execute(config, plan)
    }
}

async fn qualify_public_source<V: VersionedPublicPostgresApi>()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let major = V::MAJOR.to_owned();
    let password = env::var(postgres_env::env_name(&major, "TEST_PASSWORD"))?;
    let admin_user = postgres_env::setting(&major, "ADMIN_USER", "postgres");
    let reader_user = postgres_env::setting(&major, "READER_USER", "postgresql_reader");
    let writer_user = postgres_env::setting(&major, "WRITER_USER", "postgresql_writer");
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = format!("cdc_pg{major}_live_{tag}");
    let publication = format!("cdc_pg{major}_pub_{tag}");
    let slot = format!("cdc_pg{major}_slot_{tag}");
    let mut admin =
        PgConnection::connect_with(&postgres_env::options(&major, &admin_user, &password)).await?;
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&mut admin)
        .await?;
    assert_eq!(server_version.split('.').next(), Some(major.as_str()));
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.events (
            id bigint PRIMARY KEY,
            amount numeric(30,6) NOT NULL,
            ratio double precision NOT NULL,
            happened_at timestamptz(6) NOT NULL,
            payload jsonb NOT NULL,
            bytes bytea NOT NULL,
            note text NULL
        )"
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT USAGE ON SCHEMA {schema} TO \"{}\"",
        reader_user.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT ON {schema}.events TO \"{}\"",
        reader_user.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {schema}.events"
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT USAGE ON SCHEMA {schema} TO \"{}\"",
        writer_user.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT INSERT ON {schema}.events TO \"{}\"",
        writer_user.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;

    let source_major = major.clone();
    let source_schema = schema.clone();
    let source_publication = publication.clone();
    let source_slot = slot.clone();
    let source_password = password.clone();
    let source_writer_user = writer_user.clone();
    let result = tokio::spawn(async move {
        let mut config = Config::new(
            postgres_env::setting(&source_major, "HOST", "192.168.0.10"),
            postgres_env::setting(&source_major, "PORT", "54321")
                .parse()
                .expect("invalid PostgreSQL test port"),
            "CDC_test",
            postgres_env::setting(&source_major, "READER_USER", "postgresql_reader"),
            &source_password,
            &source_publication,
            &source_slot,
        );
        config.create_slot = true;
        let mut capture = V::replication(config).await?;
        assert!(capture
            .source()
            .version
            .starts_with(&format!("{source_major}.")));
        let cancel = CancellationToken::new();
        let mut writer = PgConnection::connect_with(&postgres_env::options(
            &source_major,
            &source_writer_user,
            &source_password,
        ))
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {source_schema}.events VALUES (
                1,
                12345678901234567890.123456,
                1.25,
                TIMESTAMPTZ '2026-09-14 01:02:03.123456+00',
                '{{\"n\":12345678901234567890.123456}}'::jsonb,
                decode('00ff', 'hex'),
                NULL
            )"
        )))
        .execute(&mut writer)
        .await?;
        let transaction = tokio::time::timeout(
            Duration::from_secs(20),
            capture.next_transaction(&cancel),
        )
        .await??;
        assert!(transaction
            .transaction()
            .source
            .version
            .starts_with(&format!("{source_major}.")));
        assert!(matches!(
            source_datum(&transaction, "amount"),
            Datum::Value(LogicalValue::Decimal { unscaled, scale: 6 })
                if unscaled == "12345678901234567890123456"
        ));
        assert!(matches!(
            source_datum(&transaction, "ratio"),
            Datum::Value(LogicalValue::Float { ieee754_hex, .. }) if ieee754_hex == "3ff4000000000000"
        ));
        assert!(matches!(
            source_datum(&transaction, "happened_at"),
            Datum::Value(LogicalValue::Instant { nanoseconds: 123456000, .. })
        ));
        assert!(matches!(
            source_datum(&transaction, "payload"),
            Datum::Value(LogicalValue::Json { .. })
        ));
        assert!(matches!(
            source_datum(&transaction, "bytes"),
            Datum::Value(LogicalValue::Binary { bytes_base64url }) if bytes_base64url == "AP8"
        ));
        assert!(matches!(source_datum(&transaction, "note"), Datum::Null));
        let encoded = change_event::json(&transaction)?;
        let mut reader = change_event::JsonReader::new(io::Cursor::new(encoded.as_bytes()));
        let replayed = reader
            .next_transaction()?
            .ok_or_else(|| io::Error::other("captured transaction was absent from JSON replay"))?;
        reader.finish()?;
        assert_eq!(change_event::json(&replayed)?, encoded);
        if let Ok(directory) = env::var("CDC_TEST_ARTIFACT_DIR") {
            std::fs::create_dir_all(&directory)?;
            std::fs::write(
                std::path::Path::new(&directory)
                    .join(format!("postgresql_{source_major}-{source_schema}.change_event.jsonl")),
                encoded,
            )?;
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => Err(Box::new(error) as Box<dyn Error + Send + Sync>),
    };

    let cleanup = async {
        sqlx::query(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active",
        )
        .bind(&slot)
        .execute(&mut admin)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP PUBLICATION IF EXISTS {publication}"
        )))
        .execute(&mut admin)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA IF EXISTS {schema} CASCADE")))
            .execute(&mut admin)
            .await?;
        Ok::<(), sqlx::Error>(())
    }
    .await;
    if let Err(error) = result {
        let _ = cleanup;
        return Err(error);
    }
    cleanup?;
    Ok(())
}

fn plan_for_public_sink<V: VersionedPublicPostgresApi>(
    transaction: &ValidatedTransaction,
) -> io::Result<postgresql_15::SqlTransaction> {
    V::plan(transaction)
}

async fn execute_with_public_sink<V: VersionedPublicPostgresApi>(
    config: &TargetConfig,
    plan: &postgresql_15::SqlTransaction,
) -> io::Result<postgresql_15::ApplyResult> {
    V::execute(config, plan).await
}

fn transaction_with_changes(
    validated: &ValidatedTransaction,
    indexes: &[usize],
) -> ValidatedTransaction {
    let mut transaction = validated.transaction().clone();
    transaction.changes = indexes
        .iter()
        .map(|index| transaction.changes[*index].clone())
        .collect();
    validate(transaction).unwrap()
}

async fn qualify_public_sink<V: VersionedPublicPostgresApi>()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let version = V::MAJOR;
    let password = env::var(postgres_env::env_name(version, "TEST_PASSWORD"))?;
    let admin_user = postgres_env::setting(version, "ADMIN_USER", "postgres");
    let writer_user = postgres_env::setting(version, "WRITER_USER", "postgresql_writer");
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let table = format!("cdc_public_sink_pg{version}_{tag}");
    let config = TargetConfig::new(
        postgres_env::setting(version, "HOST", "192.168.0.10"),
        "CDC_test",
        writer_user.clone(),
        password.clone(),
    )
    .with_port(
        postgres_env::setting(version, "PORT", "54321")
            .parse()
            .expect("invalid PostgreSQL test port"),
    );
    let mut admin =
        PgConnection::connect_with(&postgres_env::options(version, &admin_user, &password)).await?;
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&mut admin)
        .await?;
    assert_eq!(server_version.split('.').next(), Some(version));
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.\"{table}\" (id bigint PRIMARY KEY, message text NOT NULL)"
    )))
    .execute(&mut admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON public.\"{table}\" TO \"{}\"",
        writer_user.replace('"', "\"\"")
    )))
    .execute(&mut admin)
    .await?;

    let scenario_config = config.clone();
    let scenario_password = password.clone();
    let scenario_admin_user = admin_user.clone();
    let scenario_table = table.clone();
    let result = tokio::spawn(async move {
        let source_fixtures = [
            sink_fixtures::fixture(),
            sink_fixtures::postgres_fixture("16.10"),
            sink_fixtures::postgres_fixture("17.6"),
            sink_fixtures::mysql_fixture("5.7.44"),
            sink_fixtures::mysql_fixture("8.0.46"),
            sink_fixtures::mysql_fixture("8.4.8"),
        ];
        for source_fixture in source_fixtures {
            let mut full = source_fixture.transaction().clone();
            for change in &mut full.changes {
                change.table.clone_from(&scenario_table);
            }
            let full = validate(full)?;
            let mut verify = PgConnection::connect_with(&postgres_env::options(
                version,
                &scenario_admin_user,
                &scenario_password,
            ))
            .await?;
            let insert = transaction_with_changes(&full, &[0]);
            let insert_plan = plan_for_public_sink::<V>(&insert)?;
            assert_eq!(
                execute_with_public_sink::<V>(&scenario_config, &insert_plan)
                    .await?
                    .statements_executed,
                1
            );
            let row: (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,message FROM public.\"{scenario_table}\""
            )))
            .fetch_one(&mut verify)
            .await?;
            assert_eq!(row, (1, "中文'\\".into()));

            let mut keyless = insert.transaction().clone();
            let change = &mut keyless.changes[0];
            for image in change.before.iter_mut().chain(change.after.iter_mut()) {
                for column in image {
                    column.primary_key_ordinal = None;
                }
            }
            assert!(plan_for_public_sink::<V>(&validate(keyless)?).is_err());

            let update = transaction_with_changes(&full, &[1]);
            let update_plan = plan_for_public_sink::<V>(&update)?;
            assert_eq!(
                execute_with_public_sink::<V>(&scenario_config, &update_plan)
                    .await?
                    .statements_executed,
                1
            );
            let row: (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,message FROM public.\"{scenario_table}\""
            )))
            .fetch_one(&mut verify)
            .await?;
            assert_eq!(row, (2, "中文'\\".into()));

            let delete = transaction_with_changes(&full, &[2]);
            let delete_plan = plan_for_public_sink::<V>(&delete)?;
            assert_eq!(
                execute_with_public_sink::<V>(&scenario_config, &delete_plan)
                    .await?
                    .statements_executed,
                1
            );
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM public.\"{scenario_table}\""
            )))
            .fetch_one(&mut verify)
            .await?;
            assert_eq!(count, 0);

            let duplicate = transaction_with_changes(&full, &[0, 0]);
            let duplicate_plan = plan_for_public_sink::<V>(&duplicate)?;
            assert!(
                execute_with_public_sink::<V>(&scenario_config, &duplicate_plan)
                    .await
                    .is_err()
            );
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM public.\"{scenario_table}\""
            )))
            .fetch_one(&mut verify)
            .await?;
            assert_eq!(
                count, 0,
                "failed source transaction must roll back all rows"
            );
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => Err(Box::new(error) as Box<dyn Error + Send + Sync>),
    };

    let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS public.\"{table}\""
    )))
    .execute(&mut admin)
    .await;
    if let Err(error) = result {
        let _ = cleanup;
        return Err(error);
    }
    cleanup?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_public_source_capture() -> Result<(), Box<dyn Error + Send + Sync>> {
    qualify_public_source::<Postgresql16Api>().await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_public_source_capture() -> Result<(), Box<dyn Error + Send + Sync>> {
    qualify_public_source::<Postgresql17Api>().await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 16 test database"]
async fn postgres16_public_sink_apply() -> Result<(), Box<dyn Error + Send + Sync>> {
    qualify_public_sink::<Postgresql16Api>().await
}

#[tokio::test]
#[ignore = "requires the configured PostgreSQL 17 test database"]
async fn postgres17_public_sink_apply() -> Result<(), Box<dyn Error + Send + Sync>> {
    qualify_public_sink::<Postgresql17Api>().await
}
