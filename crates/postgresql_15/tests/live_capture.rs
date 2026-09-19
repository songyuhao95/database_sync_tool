//! Run explicitly with PG_CDC_TEST_PASSWORD. Uses only uniquely named objects in CDC_test.
use change_event::{Datum, LogicalValue, SinkAdapter as _, ValidatedTransaction};
use mysql_driver::prelude::Queryable;
use mysql_driver::{Conn, OptsBuilder};
use postgresql_15::{CancellationToken, Config, Result};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn test_port() -> u16 {
    env::var("PG_CDC_PORT")
        .map(|p| p.parse().expect("invalid PG_CDC_PORT"))
        .unwrap_or(54321)
}
fn test_user(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
fn options(user: &str, password: &str) -> PgConnectOptions {
    PgConnectOptions::new()
        .host(&env::var("PG_CDC_HOST").unwrap_or_else(|_| "192.168.0.10".into()))
        .port(test_port())
        .username(user)
        .password(password)
        .database("CDC_test")
        .ssl_mode(PgSslMode::Prefer)
}
async fn read_n(c: Config, n: usize) -> Result<Vec<ValidatedTransaction>> {
    // A dedicated task allows a bounded test timeout without cancelling a live read future.
    let token = CancellationToken::new();
    let cancel = token.clone();
    let mut handle = tokio::spawn(async move {
        let mut capture = postgresql_15::replication(c).await?;
        let mut result = Vec::new();
        for _ in 0..n {
            result.push(capture.next_transaction(&cancel).await?);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result)
    });
    match tokio::time::timeout(Duration::from_secs(20), &mut handle).await {
        Ok(result) => result?,
        Err(error) => {
            token.cancel();
            let _ = handle.await;
            Err(error.into())
        }
    }
}
fn datum<'a>(tx: &'a ValidatedTransaction, before: bool, name: &str) -> &'a Datum {
    let row = &tx.transaction().changes[0];
    let image = if before {
        row.before.as_ref()
    } else {
        row.after.as_ref()
    }
    .unwrap();
    &image.iter().find(|c| c.name == name).unwrap().datum
}
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL 15 test instance and explicit credentials"]
async fn postgres15_capture_and_replay() -> Result<()> {
    let password = env::var("PG_CDC_TEST_PASSWORD")?;
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let schema = format!("cdc_pg15_test_{tag}");
    let publication = format!("cdc_pub_{tag}");
    let slot = format!("cdc_slot_{tag}");
    let mut admin = PgConnection::connect_with(&options(
        &test_user("PG_CDC_ADMIN_USER", "postgres"),
        &password,
    ))
    .await?;
    test_sql(format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await?;
    let mut c = Config::new(
        env::var("PG_CDC_HOST").unwrap_or_else(|_| "192.168.0.10".into()),
        test_port(),
        "CDC_test",
        test_user("PG_CDC_READER_USER", "postgresql_reader"),
        &password,
        &publication,
        &slot,
    );
    c.create_slot = true;
    let scenario_schema = schema.clone();
    let scenario_pub = publication.clone();
    let scenario_password = password.clone();
    let result=tokio::spawn(async move {
        let schema=scenario_schema; let publication=scenario_pub; let password=scenario_password;
        let mut admin=PgConnection::connect_with(&options(&test_user("PG_CDC_ADMIN_USER","postgres"),&password)).await?;
        let mut writer=PgConnection::connect_with(&options(&test_user("PG_CDC_WRITER_USER","postgresql_writer"),&password)).await?;
        test_sql(format!("CREATE TABLE {schema}.events (tenant integer NOT NULL,id bigint NOT NULL,message text NOT NULL,note text,amount numeric(30,6),flag boolean,bytes bytea,created_at timestamp(6),observed_at timestamptz(6),day date,token uuid,metadata jsonb,ratio double precision,PRIMARY KEY(id,tenant))")).execute(&mut admin).await?;
        test_sql(format!("ALTER TABLE {schema}.events ALTER COLUMN message SET STORAGE EXTERNAL")).execute(&mut admin).await?;
        test_sql(format!("CREATE PUBLICATION {publication} FOR TABLE {schema}.events")).execute(&mut admin).await?;
        let first=postgresql_15::replication(c.clone()).await?;
        let source_id=first.source().id.clone(); let initial=first.start_lsn();
        drop(first);
        c.create_slot=false;c.expected_source_id=Some(source_id);
        let mut rollback=writer.begin().await?;
        test_sql(format!("INSERT INTO {schema}.events(tenant,id,message) VALUES(7,999,'rolled back')")).execute(&mut *rollback).await?;
        rollback.rollback().await?;
        let mut insert=writer.begin().await?;
        test_sql(format!("INSERT INTO {schema}.events VALUES(7,1,repeat('abcdefgh',16384),'note',12345678901234567890.123456,true,decode('00ff','hex'),'2026-09-13 12:13:14.123456','2026-09-13 20:13:14.123456+08','2026-09-13','12345678-1234-1234-1234-123456789abc','{{\"n\":12345678901234567890.123456}}'::jsonb,1.25)")).execute(&mut *insert).await?;
        test_sql(format!("INSERT INTO {schema}.events(tenant,id,message) VALUES(7,2,'')")).execute(&mut *insert).await?;
        insert.commit().await?;
        test_sql(format!("UPDATE {schema}.events SET note=NULL,flag=false,amount=-12.345678 WHERE id=1 AND tenant=7")).execute(&mut writer).await?;
        test_sql(format!("UPDATE {schema}.events SET id=9,tenant=8 WHERE id=1 AND tenant=7")).execute(&mut writer).await?;
        test_sql(format!("DELETE FROM {schema}.events WHERE id=9 AND tenant=8")).execute(&mut writer).await?;
        let transactions=read_n(c.clone(),4).await?;
        assert_eq!(transactions[0].transaction().changes.len(),2);
        assert!(matches!(datum(&transactions[0],false,"amount"),Datum::Value(LogicalValue::Decimal{unscaled,scale:6}) if unscaled=="12345678901234567890123456"));
        assert!(matches!(datum(&transactions[0],false,"message"),Datum::Value(LogicalValue::Text{text:Some(t),..}) if t.len()==131072));
        assert!(matches!(datum(&transactions[0],false,"flag"),Datum::Value(LogicalValue::Boolean{value:true})));
        assert!(matches!(datum(&transactions[0],false,"bytes"),Datum::Value(LogicalValue::Binary{bytes_base64url}) if bytes_base64url=="AP8"));
        assert!(matches!(datum(&transactions[0],false,"metadata"),Datum::Value(LogicalValue::Json{value:change_event::JsonValue::Object(entries)})
            if matches!(&entries[0].value,change_event::JsonValue::Decimal{unscaled,scale:6} if unscaled=="12345678901234567890123456")));
        assert!(matches!(&transactions[0].transaction().changes[1].after.as_ref().unwrap().iter().find(|c|c.name=="message").unwrap().datum,
            Datum::Value(LogicalValue::Text{text:Some(t),..}) if t.is_empty()));
        assert!(matches!(datum(&transactions[0],false,"observed_at"),Datum::Value(LogicalValue::Instant{nanoseconds:123456000,..})));

        assert!(matches!(datum(&transactions[1],true,"message"),Datum::Unavailable));
        assert!(matches!(datum(&transactions[1],false,"message"),Datum::Unchanged));
        assert!(matches!(datum(&transactions[1],false,"note"),Datum::Null));
        assert!(matches!(datum(&transactions[2],true,"id"),Datum::Value(LogicalValue::Integer{value,..}) if value=="1"));
        assert!(matches!(datum(&transactions[2],true,"tenant"),Datum::Value(LogicalValue::Integer{value,..}) if value=="7"));
        assert!(matches!(datum(&transactions[2],false,"id"),Datum::Value(LogicalValue::Integer{value,..}) if value=="9"));
        assert!(matches!(datum(&transactions[3],true,"id"),Datum::Value(LogicalValue::Integer{value,..}) if value=="9"));
        assert!(matches!(datum(&transactions[3],true,"message"),Datum::Unavailable));
        let observed:Option<String>=sqlx::query_scalar("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1")
            .bind(&c.slot).fetch_one(&mut admin).await?;
        assert_eq!(observed.as_deref(),Some(initial.as_str()),"reading/logging must never advance durable acknowledgement");
        if let Ok(dir)=env::var("CDC_TEST_ARTIFACT_DIR") {
            std::fs::create_dir_all(&dir)?;
            let json=transactions.iter().map(change_event::json).collect::<std::result::Result<Vec<_>,_>>()?.join("");
            std::fs::write(std::path::Path::new(&dir).join(format!("postgresql_15-{schema}.change_event.jsonl")),json)?;
        }
        let replay=read_n(c.clone(),4).await?;
        for (a,b) in transactions.iter().zip(&replay) {
            let json=change_event::json(a)?;
            assert_eq!(json,change_event::json(b)?);
            let mut reader=change_event::JsonReader::new(std::io::Cursor::new(json));
            assert_eq!(reader.next_transaction()?.unwrap().transaction().id,a.transaction().id);
            reader.finish()?;
        }
        c.start_lsn=Some(transactions[1].transaction().commit_cursor.value.clone());
        let resumed=read_n(c.clone(),2).await?;
        assert_eq!(resumed[0].transaction().id,transactions[2].transaction().id);
        let mut wrong=c.clone();wrong.expected_source_id=Some("postgresql:1:1:1".into());
        assert!(postgresql_15::replication(wrong).await.is_err());
        let mut missing=c.clone();missing.slot.push_str("_missing");
        assert!(postgresql_15::replication(missing).await.is_err());
        let absent:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)").bind(missing_name(&c.slot)).fetch_one(&mut admin).await?;
        assert!(absent);
        // Reject unsupported table reset and keep the slot checkpoint unchanged.
        c.start_lsn=Some(transactions[3].transaction().commit_cursor.value.clone());
        test_sql(format!("TRUNCATE {schema}.events")).execute(&mut admin).await?;
        let error=read_n(c.clone(),1).await.unwrap_err().to_string();
        assert!(error.contains("TRUNCATE"),"{error}");
        let final_lsn:Option<String>=sqlx::query_scalar("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1").bind(&c.slot).fetch_one(&mut admin).await?;
        assert_eq!(final_lsn.as_deref(),Some(initial.as_str()));

        // FULL identity supplies complete old images, including toasted values.
        test_sql(format!("ALTER TABLE {schema}.events REPLICA IDENTITY FULL")).execute(&mut admin).await?;
        let full_start:String=sqlx::query_scalar("SELECT pg_current_wal_lsn()::text").fetch_one(&mut admin).await?;
        c.start_lsn=Some(full_start);
        test_sql(format!("INSERT INTO {schema}.events(tenant,id,message,note) VALUES(7,3,repeat('abcdefgh',16384),'old note')")).execute(&mut writer).await?;
        test_sql(format!("UPDATE {schema}.events SET note='new note' WHERE id=3 AND tenant=7")).execute(&mut writer).await?;
        test_sql(format!("DELETE FROM {schema}.events WHERE id=3 AND tenant=7")).execute(&mut writer).await?;
        let full=read_n(c.clone(),3).await?;
        assert!(matches!(datum(&full[1],true,"message"),Datum::Value(LogicalValue::Text{text:Some(t),..}) if t.len()==131072));
        assert!(matches!(datum(&full[1],true,"note"),Datum::Value(LogicalValue::Text{text:Some(t),..}) if t=="old note"));
        assert!(matches!(datum(&full[1],false,"note"),Datum::Value(LogicalValue::Text{text:Some(t),..}) if t=="new note"));
        assert!(matches!(datum(&full[2],true,"message"),Datum::Value(LogicalValue::Text{text:Some(t),..}) if t.len()==131072));
        let mut limited=c.clone();limited.max_transaction_bytes=1024;
        assert!(read_n(limited,1).await.unwrap_err().to_string().contains("wire-size limit"));
        let final_lsn:Option<String>=sqlx::query_scalar("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1").bind(&c.slot).fetch_one(&mut admin).await?;
        assert_eq!(final_lsn.as_deref(),Some(initial.as_str()));
        println!("PASS: 7 committed transactions / 8 rows; DEFAULT and FULL identity; rollback excluded; exact numeric/JSONB, NULL, empty text, TOAST, composite key change; identical replay; explicit LSN resume; missing slot/source mismatch/TRUNCATE/size limit rejected; no WAL acknowledgement");

        Ok::<_,Box<dyn std::error::Error+Send+Sync>>(())
    }).await;
    // Also clean up when the spawned test panics.
    sqlx::query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active").bind(&slot).execute(&mut admin).await?;
    test_sql(format!("DROP PUBLICATION IF EXISTS {publication}"))
        .execute(&mut admin)
        .await?;
    test_sql(format!("DROP TABLE IF EXISTS {schema}.events"))
        .execute(&mut admin)
        .await?;
    test_sql(format!("DROP SCHEMA {schema}"))
        .execute(&mut admin)
        .await?;
    result??;
    Ok(())
}
fn missing_name(slot: &str) -> String {
    format!("{slot}_missing")
}

fn mysql_setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn mysql_port(name: &str, default: u16) -> u16 {
    mysql_setting(name, &default.to_string())
        .parse()
        .expect("invalid MySQL test port")
}

fn mysql_connection(port: u16) -> Conn {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(mysql_setting("CDC_MYSQL_HOST", "192.168.0.10")))
            .tcp_port(port)
            .user(Some(mysql_setting("CDC_MYSQL_WRITER_USER", "mysql_writer")))
            .pass(Some(
                env::var("CDC_MYSQL_WRITER_PASSWORD").expect("set CDC_MYSQL_WRITER_PASSWORD"),
            )),
    )
    .expect("connect to MySQL Sink")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL 15 and all three configured MySQL test instances"]
async fn postgres15_capture_and_apply_to_mysql_sinks() -> Result<()> {
    let password = env::var("PG_CDC_TEST_PASSWORD")?;
    let tag = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let schema = format!("cdc_pg_bridge_{tag}");
    let publication = format!("cdc_bridge_pub_{tag}");
    let slot = format!("cdc_bridge_slot_{tag}");
    let mut admin = PgConnection::connect_with(&options(
        &test_user("PG_CDC_ADMIN_USER", "postgres"),
        &password,
    ))
    .await?;
    let mut writer = PgConnection::connect_with(&options(
        &test_user("PG_CDC_WRITER_USER", "postgresql_writer"),
        &password,
    ))
    .await?;
    test_sql(format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await?;
    test_sql(format!(
        "CREATE TABLE {schema}.events (
            tenant integer NOT NULL,
            id bigint NOT NULL,
            message text NOT NULL,
            amount numeric(30,6),
            bytes bytea,
            day date,
            local_time timestamp(6),
            observed_at timestamptz(6),
            metadata jsonb,
            PRIMARY KEY(id,tenant)
        )"
    ))
    .execute(&mut admin)
    .await?;
    test_sql(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {schema}.events"
    ))
    .execute(&mut admin)
    .await?;

    let mut config = Config::new(
        mysql_setting("PG_CDC_HOST", "192.168.0.10"),
        test_port(),
        "CDC_test",
        test_user("PG_CDC_READER_USER", "postgresql_reader"),
        &password,
        &publication,
        &slot,
    );
    config.create_slot = true;
    let first = postgresql_15::replication(config.clone()).await?;
    let source_id = first.source().id.clone();
    drop(first);
    config.create_slot = false;
    config.expected_source_id = Some(source_id);

    test_sql(format!(
        "INSERT INTO {schema}.events VALUES
            (7,1,'bridge',12345678901234567890.123456,decode('00ff','hex'),
             '2026-09-14','2026-09-14 01:02:03.123456',
             '2026-09-14 01:02:03.123456+00','{{\"n\":12345678901234567890.123456}}'::jsonb)"
    ))
    .execute(&mut writer)
    .await?;
    test_sql(format!(
        "UPDATE {schema}.events SET id=2,message='updated',amount=-0.000001 WHERE id=1 AND tenant=7"
    ))
    .execute(&mut writer)
    .await?;
    test_sql(format!(
        "DELETE FROM {schema}.events WHERE id=2 AND tenant=7"
    ))
    .execute(&mut writer)
    .await?;
    let transactions = read_n(config.clone(), 3).await?;
    assert_eq!(transactions.len(), 3);
    assert!(matches!(
        transactions[0].transaction().changes[0].operation,
        change_event::Operation::Insert
    ));
    assert!(matches!(
        transactions[1].transaction().changes[0].operation,
        change_event::Operation::Update
    ));
    assert!(matches!(
        transactions[2].transaction().changes[0].operation,
        change_event::Operation::Delete
    ));

    macro_rules! apply_to_mysql {
        ($adapter:ident, $port_name:literal, $port:literal) => {{
            let port = mysql_port($port_name, $port);
            let mut target = mysql_connection(port);
            target
                .query_drop(format!("CREATE DATABASE `{schema}`"))
                .unwrap();
            target
                .query_drop(format!(
                    "CREATE TABLE `{schema}`.`events` (
                        tenant INT NOT NULL,
                        id BIGINT NOT NULL,
                        message TEXT NOT NULL,
                        amount DECIMAL(30,6),
                        bytes VARBINARY(32),
                        day DATE,
                        local_time DATETIME(6),
                        observed_at TIMESTAMP(6),
                        metadata JSON,
                        PRIMARY KEY(id,tenant)
                    ) ENGINE=InnoDB"
                ))
                .unwrap();
            let config = $adapter::TargetConfig {
                host: mysql_setting("CDC_MYSQL_HOST", "192.168.0.10"),
                port,
                user: mysql_setting("CDC_MYSQL_WRITER_USER", "mysql_writer"),
                password: env::var("CDC_MYSQL_WRITER_PASSWORD")
                    .expect("set CDC_MYSQL_WRITER_PASSWORD"),
            };
            let sink = $adapter::SinkAdapter::new();
            for (index, transaction) in transactions.iter().enumerate() {
                let plan = sink.plan(transaction).unwrap();
                assert_eq!(
                    $adapter::execute(&config, &plan)
                        .unwrap()
                        .statements_executed,
                    1
                );
                let count: u64 = target
                    .query_first(format!("SELECT COUNT(*) FROM `{schema}`.events"))
                    .unwrap()
                    .unwrap();
                assert_eq!(count, if index == 2 { 0 } else { 1 });
                if index == 0 {
                    let row: (i64, String) = target
                        .query_first(format!("SELECT id,message FROM `{schema}`.events"))
                        .unwrap()
                        .unwrap();
                    assert_eq!(row, (1, "bridge".into()));

                    let mut unsupported = transaction.transaction().clone();
                    unsupported.changes[0].after.as_mut().unwrap()[2].datum =
                        Datum::Value(LogicalValue::Boolean { value: true });
                    let unsupported = change_event::validate(unsupported).unwrap();
                    assert!(sink.plan(&unsupported).is_err());
                    let count_after_rejection: u64 = target
                        .query_first(format!("SELECT COUNT(*) FROM `{schema}`.events"))
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        count_after_rejection, 1,
                        "capability failure must not change an existing target row"
                    );
                }
                if index == 1 {
                    let row: (i64, String) = target
                        .query_first(format!("SELECT id,message FROM `{schema}`.events"))
                        .unwrap()
                        .unwrap();
                    assert_eq!(row, (2, "updated".into()));
                }
            }
            let mut duplicate = transactions[0].transaction().clone();
            duplicate
                .changes
                .push(transactions[0].transaction().changes[0].clone());
            let duplicate = change_event::validate(duplicate).unwrap();
            let duplicate_plan = sink.plan(&duplicate).unwrap();
            assert!($adapter::execute(&config, &duplicate_plan).is_err());
            let count: u64 = target
                .query_first(format!("SELECT COUNT(*) FROM `{schema}`.events"))
                .unwrap()
                .unwrap();
            assert_eq!(
                count, 0,
                "failed transaction must not leave a partial write"
            );
            target
                .query_drop(format!("DROP DATABASE `{schema}`"))
                .unwrap();
        }};
    }
    apply_to_mysql!(mysql_5_7, "CDC_MYSQL57_PORT", 33061);
    apply_to_mysql!(mysql_8_0, "CDC_MYSQL80_PORT", 33062);
    apply_to_mysql!(mysql_8_4, "CDC_MYSQL84_PORT", 33063);

    sqlx::query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active")
        .bind(&slot)
        .execute(&mut admin)
        .await?;
    test_sql(format!("DROP PUBLICATION IF EXISTS {publication}"))
        .execute(&mut admin)
        .await?;
    test_sql(format!("DROP TABLE IF EXISTS {schema}.events"))
        .execute(&mut admin)
        .await?;
    test_sql(format!("DROP SCHEMA {schema}"))
        .execute(&mut admin)
        .await?;
    Ok(())
}

// SQL identifiers here are fixed prefixes + numeric timestamps, never user input.
fn test_sql(
    text: String,
) -> sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(sqlx::AssertSqlSafe(text))
}
