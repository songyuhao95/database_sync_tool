use super::{instance_input, store};
use crate::{
    Error, Store,
    model::NewUser,
    tasks::{FieldPreviewInput, TableMapping, TaskInput},
};
use mysql_driver::{Conn, OptsBuilder, prelude::Queryable};
use sqlx::{
    Connection as _, Executor as _, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[path = "../../../tests/support/type_qualification_evidence.rs"]
mod type_qualification_evidence;

#[test]
#[ignore = "requires live MySQL 5.7 and PostgreSQL 15; isolated tables and local SQLite"]
fn live_web_explicit_carrier_preview_create_start_and_readback() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("web_carrier_{nonce}");
    let carrier_fields = [
        ("payload", "mysql.json"),
        ("enum_value", "mysql.enum"),
        ("set_value", "mysql.set"),
        ("bit_value", "mysql.bit"),
    ];
    let mysql_host = std::env::var("CDC_MYSQL_HOST").unwrap();
    let mysql_port = std::env::var("CDC_MYSQL57_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let mysql_reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let mysql_writer_password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let pg_host = std::env::var("PG_CDC_HOST").unwrap();
    let pg_port = std::env::var("PG_CDC_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let pg_password = std::env::var("PG_CDC_TEST_PASSWORD").unwrap();
    let pg_admin = std::env::var("PG_CDC_ADMIN_USER").unwrap();
    let pg_reader = std::env::var("PG_CDC_READER_USER").unwrap();
    let pg_writer = std::env::var("PG_CDC_WRITER_USER").unwrap();
    let mut source = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(mysql_host.clone()))
            .tcp_port(mysql_port)
            .user(Some(std::env::var("CDC_MYSQL_WRITER_USER").unwrap()))
            .pass(Some(mysql_writer_password.clone())),
    )
    .unwrap();
    source
        .query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
        .unwrap();
    source
        .query_drop(format!(
            "CREATE TABLE CDC_test.{table}(
                id BIGINT PRIMARY KEY,
                payload JSON NULL,
                enum_value ENUM('alpha','beta') NULL,
                set_value SET('a','b') NULL,
                bit_value BIT(8) NULL
             ) ENGINE=InnoDB"
        ))
        .unwrap();
    let pg_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let options = PgConnectOptions::new()
        .host(&pg_host)
        .port(pg_port)
        .database("CDC_test")
        .username(&pg_admin)
        .password(&pg_password)
        .ssl_mode(PgSslMode::Prefer);
    let mut target = pg_runtime
        .block_on(PgConnection::connect_with(&options))
        .unwrap();
    pg_runtime
        .block_on(async {
            target
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA IF NOT EXISTS \"CDC_test\" AUTHORIZATION {pg_writer};
                     GRANT USAGE ON SCHEMA \"CDC_test\" TO {pg_writer};
                     CREATE SCHEMA IF NOT EXISTS cdc AUTHORIZATION {pg_writer};
                     GRANT USAGE,CREATE ON SCHEMA cdc TO {pg_writer};
                     CREATE TABLE \"CDC_test\".\"{table}\"(
                         id bigint PRIMARY KEY,
                         payload text,
                         enum_value text,
                         set_value text,
                         bit_value text
                     );
                     GRANT SELECT,INSERT,UPDATE,DELETE ON \"CDC_test\".\"{table}\" TO {pg_writer}"
                ))))
                .await
        })
        .unwrap();
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = format!("carrier-source-{nonce}");
    source_input.host = mysql_host;
    source_input.port = mysql_port;
    source_input.version = "5.7".into();
    source_input.reader_password = Some(mysql_reader_password);
    source_input.writer_password = Some(mysql_writer_password);
    let source_instance = store.save_instance(admin, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = format!("carrier-target-{nonce}");
    sink_input.host = pg_host;
    sink_input.port = pg_port;
    sink_input.kind = "postgresql".into();
    sink_input.version = "15".into();
    sink_input.database = "CDC_test".into();
    sink_input.reader_username = pg_reader;
    sink_input.reader_password = Some(pg_password.clone());
    sink_input.writer_username = pg_writer;
    sink_input.writer_password = Some(pg_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();
    let draft_id = format!("carrier-{nonce}");
    let preview_input =
        |column: &str, parameters: BTreeMap<String, String>, confirmations| FieldPreviewInput {
            draft_id: draft_id.clone(),
            source_id: source_instance.id.clone(),
            sink_id: sink_instance.id.clone(),
            source_database: String::new(),
            sink_database: "CDC_test".into(),
            source_revision: 1,
            sink_revision: 1,
            schema: "CDC_test".into(),
            table: table.clone(),
            column: column.into(),
            parameters,
            confirmations,
        };
    let mut conversion_options = BTreeMap::new();
    let mut confirmations = Vec::new();
    let mut confirmed_plans = Vec::new();
    for (column, type_id) in carrier_fields {
        eprintln!("WEB_CARRIER_PHASE preview {type_id}");
        let discovery = store
            .preview_field(admin, preview_input(column, BTreeMap::new(), vec![]))
            .unwrap();
        let candidate = discovery
            .available_candidates
            .iter()
            .find(|candidate| {
                candidate
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("logical_value_json")
            })
            .unwrap_or_else(|| panic!("Web must offer the {type_id} value carrier"));
        let parameters = BTreeMap::from([
            ("__rule_id".into(), candidate.rule.id.clone()),
            ("__rule_version".into(), candidate.rule.version.clone()),
        ]);
        let unconfirmed = store
            .preview_field(admin, preview_input(column, parameters.clone(), vec![]))
            .unwrap();
        let pending = unconfirmed.result.unwrap();
        assert_eq!(
            pending.status,
            change_event::CompatibilityStatus::NeedsConfirmation,
            "{type_id} must require explicit risk acknowledgement"
        );
        let pending_plan = pending.plan.unwrap();
        let confirmation = change_event::RiskConfirmation {
            source_field_lineage: pending_plan.source_field.lineage_id.clone(),
            target_field_lineage: pending_plan.target_field.lineage_id.clone(),
            rule: pending_plan.rule.clone(),
            plan_digest: pending_plan.plan_digest.clone(),
            actor: "admin".into(),
            confirmed_at: "2026-09-28T00:00:00Z".into(),
            reason: Some("value carrier does not preserve the native target type".into()),
        };
        let confirmed = store
            .preview_field(
                admin,
                preview_input(column, parameters.clone(), vec![confirmation.clone()]),
            )
            .unwrap();
        let confirmed_result = confirmed.result.unwrap();
        assert_eq!(
            confirmed_result.status,
            change_event::CompatibilityStatus::Compatible,
            "{type_id} must become usable after confirmation"
        );
        conversion_options.insert(column.to_owned(), parameters);
        confirmations.push(confirmation);
        confirmed_plans.push((type_id, confirmed_result.plan.unwrap()));
    }
    let task_input = |confirmations| TaskInput {
        draft_id: Some(draft_id.clone()),
        name: format!("live Web carrier {nonce}"),
        source_id: source_instance.id.clone(),
        sink_id: sink_instance.id.clone(),
        source_database: String::new(),
        sink_database: "CDC_test".into(),
        source_revision: 1,
        sink_revision: 1,
        start_mode: "auto".into(),
        mappings: vec![TableMapping {
            source_schema: "CDC_test".into(),
            source_table: table.clone(),
            sink_schema: "CDC_test".into(),
            sink_table: table.clone(),
            columns: std::iter::once("id".to_owned())
                .chain(
                    carrier_fields
                        .iter()
                        .map(|(column, _)| (*column).to_owned()),
                )
                .collect(),
            conversion_options: conversion_options.clone(),
        }],
        confirmations,
    };
    assert!(store.create_task(admin, task_input(vec![])).is_err());
    let task = store.create_task(admin, task_input(confirmations)).unwrap();
    eprintln!("WEB_CARRIER_PHASE created");
    assert_eq!(task.plan_status, "valid");
    for (type_id, confirmed_plan) in &confirmed_plans {
        assert!(
            task.plans
                .iter()
                .any(|plan| plan.plan_digest == confirmed_plan.plan_digest),
            "{type_id} plan must be persisted"
        );
        assert!(task.risk_confirmations.iter().any(|confirmation| {
            confirmation.plan_digest == confirmed_plan.plan_digest && confirmation.actor == "admin"
        }));
    }
    let start = store.start_task(admin, task.id.clone());
    assert!(
        start.is_ok(),
        "start gate: {:?}; plan reason: {:?}",
        start.as_ref().err(),
        store.task(&task.id).unwrap().plan_invalid_reason
    );
    eprintln!("WEB_CARRIER_PHASE started");
    wait_for(&store, &task.id, |task| task.status == "running");
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table}(id,payload,enum_value,set_value,bit_value)
             VALUES(1,'{{\"message\":\"carrier\"}}','alpha','a,b',b'10100101')"
        ))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 1);
    let payload: String = pg_runtime
        .block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT payload FROM \"CDC_test\".\"{table}\" WHERE id=1"
            )))
            .fetch_one(&mut target),
        )
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<change_event::LogicalValue>(&payload).unwrap(),
        change_event::LogicalValue::Json { .. }
    ));
    for column in ["enum_value", "set_value", "bit_value"] {
        let stored: String = pg_runtime
            .block_on(
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT {column} FROM \"CDC_test\".\"{table}\" WHERE id=1"
                )))
                .fetch_one(&mut target),
            )
            .unwrap();
        let value: change_event::LogicalValue = serde_json::from_str(&stored).unwrap();
        match (column, value) {
            ("enum_value", change_event::LogicalValue::Enum { label }) => {
                assert_eq!(label, "alpha");
            }
            ("set_value", change_event::LogicalValue::Set { members }) => {
                assert_eq!(members, ["a", "b"]);
            }
            ("bit_value", change_event::LogicalValue::BitString { bit_length: 8, .. }) => {}
            (_, other) => panic!("{column} read back as the wrong value variant: {other:?}"),
        }
    }
    source
        .query_drop(format!(
            "UPDATE CDC_test.{table}
             SET payload='{{\"message\":\"carrier-updated\"}}',
                 enum_value='beta',set_value='b',bit_value=b'00000001'
             WHERE id=1"
        ))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 2);
    let updated: String = pg_runtime
        .block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT payload FROM \"CDC_test\".\"{table}\" WHERE id=1"
            )))
            .fetch_one(&mut target),
        )
        .unwrap();
    assert!(updated.contains("carrier-updated"));
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    assert_eq!(store.task(&task.id).unwrap().status, "stopped");
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table}(id,payload,enum_value,set_value,bit_value)
             VALUES(2,'{{\"message\":\"after-restart\"}}','beta','b',b'00000001')"
        ))
        .unwrap();
    drop(store);
    let reopened = Arc::new(Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap());
    reopened.start_task(admin, task.id.clone()).unwrap();
    wait_for(&reopened, &task.id, |task| task.runtime.applied_rows >= 3);
    let resumed: String = pg_runtime
        .block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT payload FROM \"CDC_test\".\"{table}\" WHERE id=2"
            )))
            .fetch_one(&mut target),
        )
        .unwrap();
    assert!(resumed.contains("after-restart"));
    source
        .query_drop(format!("DELETE FROM CDC_test.{table} WHERE id=1"))
        .unwrap();
    wait_for(&reopened, &task.id, |task| task.runtime.applied_rows >= 4);
    let deleted: Option<String> = pg_runtime
        .block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT payload FROM \"CDC_test\".\"{table}\" WHERE id=1"
            )))
            .fetch_optional(&mut target),
        )
        .unwrap();
    assert!(deleted.is_none());
    assert!(
        reopened
            .task(&task.id)
            .unwrap()
            .runtime
            .checkpoint
            .is_some()
    );
    reopened.stop_task(admin, &task.id).unwrap();
    reopened.shutdown_tasks();
    source
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    pg_runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP TABLE \"CDC_test\".\"{table}\""
            )))
            .execute(&mut target),
        )
        .unwrap();
    for (type_id, plan) in &confirmed_plans {
        type_qualification_evidence::record_web_plan_evidence(
            "mysql_5_7",
            "postgresql_15",
            "web_ui.mysql57_multitype_to_pg15_text_carrier_live",
            type_id,
            plan,
            "logical_value_json_carrier",
            true,
        )
        .unwrap();
    }
}

#[test]
fn runtime_permissions_conflicts_and_restart_state() {
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let source = store.save_instance(admin, None, instance_input()).unwrap();
    let mut target = instance_input();
    target.name = "runtime-target".into();
    target.port = 33062;
    target.version = "8.0".into();
    let sink = store.save_instance(admin, None, target).unwrap();
    let input = |name: &str| TaskInput {
        draft_id: None,
        name: name.into(),
        source_id: source.id.clone(),
        sink_id: sink.id.clone(),
        source_database: String::new(),
        sink_database: String::new(),
        source_revision: 1,
        sink_revision: 1,
        start_mode: "auto".into(),
        mappings: vec![TableMapping {
            source_schema: "CDC_test".into(),
            source_table: "orders".into(),
            sink_schema: "CDC_test".into(),
            sink_table: "orders".into(),
            columns: vec!["id".into()],
            conversion_options: std::collections::BTreeMap::new(),
        }],
        confirmations: vec![],
    };
    let first = store.insert_task(admin, input("first")).unwrap();
    let second = store.insert_task(admin, input("second")).unwrap();
    let viewer = store
        .create_user(
            admin,
            NewUser {
                owner: "test".into(),
                username: "runtime_viewer".into(),
                password: "a-long-test-password".into(),
                role: "viewer".into(),
                note: String::new(),
            },
        )
        .unwrap();
    assert!(matches!(
        store.start_task(viewer.id, first.id.clone()),
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        store.stop_task(viewer.id, &first.id),
        Err(Error::Forbidden)
    ));
    store.begin_task(admin, &first.id).unwrap();
    store
        .finish_task_with_state(&first.id, Some("blocked diagnostic"), true)
        .unwrap();
    assert_eq!(store.task(&first.id).unwrap().status, "blocked");
    // Re-activation is the recovery boundary after the saved plan is fixed.
    store.begin_task(admin, &first.id).unwrap();
    assert!(matches!(
        store.begin_task(admin, &second.id),
        Err(Error::Conflict(_))
    ));
    drop(store);
    let reopened = Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap();
    assert_eq!(reopened.task(&first.id).unwrap().status, "stopped");
}

fn mysql(port: u16) -> Conn {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some("192.168.0.10"))
            .tcp_port(port)
            .user(Some("mysql_writer"))
            .pass(Some(std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap()))
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(5))),
    )
    .unwrap()
}
fn wait_for(store: &Store, id: &str, predicate: impl Fn(&crate::tasks::ReplicationTask) -> bool) {
    let limit = Instant::now() + Duration::from_secs(60);
    loop {
        let task = store.task(id).unwrap();
        assert_ne!(
            task.status, "failed",
            "task failed: {:?}",
            task.runtime.last_error
        );
        if predicate(&task) {
            return;
        }
        assert!(
            Instant::now() < limit,
            "timed out: state={} rows={} error={:?}; logs={:?}",
            task.status,
            task.runtime.applied_rows,
            task.runtime.last_error,
            store
                .task_logs(id, 0)
                .map(|logs| logs.into_iter().map(|log| log.message).collect::<Vec<_>>())
        );
        thread::sleep(Duration::from_millis(50));
    }
}
#[test]
#[ignore = "requires live MySQL instances; isolated tables and local SQLite"]
fn live_tasks_resume_from_sink_in_both_modes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    for (src_port, src_version, dst_port, dst_version) in [
        (33061, "5.7", 33062, "8.0"),
        (33062, "8.0", 33063, "8.4"),
        (33063, "8.4", 33061, "5.7"),
    ] {
        for mode in ["gtid", "binlog"] {
            let (dir, store) = store();
            let admin = store.login("admin", "admin", None).unwrap().session.user.id;
            let mut source_input = instance_input();
            source_input.name = format!("source-{src_version}");
            source_input.port = src_port;
            source_input.version = src_version.into();
            source_input.reader_password = Some(reader_password.clone());
            source_input.writer_password = Some(password.clone());
            let source = store.save_instance(admin, None, source_input).unwrap();
            let mut sink_input = instance_input();
            sink_input.name = format!("sink-{dst_version}");
            sink_input.port = dst_port;
            sink_input.version = dst_version.into();
            sink_input.reader_password = Some(reader_password.clone());
            sink_input.writer_password = Some(password.clone());
            let sink = store.save_instance(admin, None, sink_input).unwrap();
            let table = format!("rt_{nonce}_{src_port}_{mode}");
            let mut src = mysql(src_port);
            let mut dst = mysql(dst_port);
            for conn in [&mut src, &mut dst] {
                conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
                    .unwrap();
                conn.query_drop(format!("CREATE TABLE CDC_test.{table}(id INT UNSIGNED PRIMARY KEY,message VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL) ENGINE=InnoDB")).unwrap();
            }
            src.query_drop(format!(
                "INSERT INTO CDC_test.{table} VALUES(100,'snapshot A'),(101,'snapshot B')"
            ))
            .unwrap();
            let task = store
                .create_task(
                    admin,
                    TaskInput {
                        draft_id: None,
                        name: format!("live-{mode}"),
                        source_id: source.id,
                        sink_id: sink.id,
                        source_database: String::new(),
                        sink_database: String::new(),
                        source_revision: 1,
                        sink_revision: 1,
                        start_mode: mode.into(),
                        mappings: vec![TableMapping {
                            source_schema: "CDC_test".into(),
                            source_table: table.clone(),
                            sink_schema: "CDC_test".into(),
                            sink_table: table.clone(),
                            columns: vec!["id".into(), "message".into()],
                            conversion_options: std::collections::BTreeMap::new(),
                        }],
                        confirmations: vec![],
                    },
                )
                .unwrap();
            store.start_task(admin, task.id.clone()).unwrap();
            wait_for(&store, &task.id, |t| {
                t.status == "running"
                    && t.runtime
                        .checkpoint
                        .as_ref()
                        .is_some_and(|cp| cp.phase == "incremental")
            });
            let initial = store.task(&task.id).unwrap().runtime.checkpoint.unwrap();
            assert_eq!(initial.snapshot_rows, 2);
            assert_eq!(
                dst.query_first::<u64, _>(format!("SELECT COUNT(*) FROM CDC_test.{table}"))
                    .unwrap(),
                Some(2)
            );
            src.query_drop(format!("INSERT INTO CDC_test.{table} VALUES(1,'insert')"))
                .unwrap();
            wait_for(&store, &task.id, |t| t.runtime.applied_rows == 1);
            src.query_drop(format!(
                "UPDATE CDC_test.{table} SET message='updated' WHERE id=1"
            ))
            .unwrap();
            wait_for(&store, &task.id, |t| t.runtime.applied_rows == 2);
            let value: String = dst
                .query_first(format!("SELECT message FROM CDC_test.{table} WHERE id=1"))
                .unwrap()
                .unwrap();
            assert_eq!(value, "updated");
            src.query_drop(format!("DELETE FROM CDC_test.{table} WHERE id=1"))
                .unwrap();
            wait_for(&store, &task.id, |t| t.runtime.applied_rows == 3);
            store.stop_task(admin, &task.id).unwrap();
            store.shutdown_tasks();
            assert_eq!(store.task(&task.id).unwrap().status, "stopped");
            // Simulate an obsolete local checkpoint. The Sink must remain authoritative.
            store.db().unwrap().execute("UPDATE task_runtime SET checkpoint_json=?2,applied_rows=0,applied_transactions=0 WHERE task_id=?1",
                rusqlite::params![task.id,serde_json::to_string(&initial).unwrap()]).unwrap();
            src.query_drop(format!(
                "INSERT INTO CDC_test.{table} VALUES(2,'while stopped')"
            ))
            .unwrap();
            drop(store);
            let reopened = Arc::new(Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap());
            reopened.start_task(admin, task.id.clone()).unwrap();
            wait_for(&reopened, &task.id, |t| t.runtime.applied_rows == 4);
            let rows: Vec<(u32, String)> = dst
                .query(format!(
                    "SELECT id,message FROM CDC_test.{table} ORDER BY id"
                ))
                .unwrap();
            assert_eq!(
                rows,
                vec![
                    (2, "while stopped".into()),
                    (100, "snapshot A".into()),
                    (101, "snapshot B".into())
                ]
            );
            assert!(
                reopened
                    .task_logs(&task.id, 0)
                    .unwrap()
                    .iter()
                    .any(|l| l.message.contains("恢复同步"))
            );
            reopened.stop_task(admin, &task.id).unwrap();
            reopened.shutdown_tasks();
            // Missing authoritative progress must stop, even when a local mirror exists.
            dst.exec_drop("DELETE FROM CDC.log_info WHERE task_id=?", (&task.id,))
                .unwrap();
            reopened.start_task(admin, task.id.clone()).unwrap();
            let limit = Instant::now() + Duration::from_secs(15);
            while reopened.task(&task.id).unwrap().status != "failed" {
                assert!(Instant::now() < limit);
                thread::sleep(Duration::from_millis(50));
            }
            assert!(
                reopened
                    .task(&task.id)
                    .unwrap()
                    .runtime
                    .last_error
                    .unwrap()
                    .contains("进度丢失")
            );
            reopened.shutdown_tasks();
            for conn in [&mut src, &mut dst] {
                conn.query_drop(format!("DROP TABLE CDC_test.{table}"))
                    .unwrap();
            }
            println!(
                "{src_version} -> {dst_version} {mode}: initial full copy, INSERT/UPDATE/DELETE, stop, restart with stale SQLite and missing checkpoint passed"
            );
        }
    }
}

#[test]
#[ignore = "requires live MySQL 5.7 and PostgreSQL 15; isolated tables and local SQLite"]
fn live_web_mysql57_to_postgresql15_full_incremental_and_resume() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("web_pg_{nonce}");
    let mysql_host = std::env::var("CDC_MYSQL_HOST").unwrap();
    let mysql_port = std::env::var("CDC_MYSQL57_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let mysql_reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let mysql_writer_password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let pg_host = std::env::var("PG_CDC_HOST").unwrap();
    let pg_port = std::env::var("PG_CDC_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let pg_password = std::env::var("PG_CDC_TEST_PASSWORD").unwrap();
    let pg_admin = std::env::var("PG_CDC_ADMIN_USER").unwrap();
    let pg_reader = std::env::var("PG_CDC_READER_USER").unwrap();
    let pg_writer = std::env::var("PG_CDC_WRITER_USER").unwrap();

    let mut source = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(mysql_host.clone()))
            .tcp_port(mysql_port)
            .user(Some(std::env::var("CDC_MYSQL_WRITER_USER").unwrap()))
            .pass(Some(mysql_writer_password.clone())),
    )
    .unwrap();
    source
        .query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
        .unwrap();
    source
        .query_drop(format!(
            "CREATE TABLE CDC_test.{table}(id INT PRIMARY KEY,message VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL) ENGINE=InnoDB"
        ))
        .unwrap();
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table} VALUES(100,'snapshot A'),(101,'snapshot B')"
        ))
        .unwrap();

    let pg_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let options = PgConnectOptions::new()
        .host(&pg_host)
        .port(pg_port)
        .database("CDC_test")
        .username(&pg_admin)
        .password(&pg_password)
        .ssl_mode(PgSslMode::Prefer);
    let mut target = pg_runtime
        .block_on(PgConnection::connect_with(&options))
        .unwrap();
    pg_runtime
        .block_on(async {
            target
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA IF NOT EXISTS \"CDC_test\" AUTHORIZATION {pg_writer};
                     GRANT USAGE ON SCHEMA \"CDC_test\" TO {pg_writer};
                     CREATE SCHEMA IF NOT EXISTS cdc AUTHORIZATION {pg_writer};
                     GRANT USAGE,CREATE ON SCHEMA cdc TO {pg_writer};
                     GRANT CREATE ON DATABASE \"CDC_test\" TO {pg_writer};
                     CREATE TABLE \"CDC_test\".\"{table}\"(id integer PRIMARY KEY,message character varying(40) NOT NULL);
                     GRANT SELECT,INSERT,UPDATE,DELETE ON \"CDC_test\".\"{table}\" TO {pg_writer}"
                ))))
                .await
        })
        .unwrap();
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = "web-pg-source".into();
    source_input.host = mysql_host;
    source_input.port = mysql_port;
    source_input.version = "5.7".into();
    source_input.reader_password = Some(mysql_reader_password);
    source_input.writer_password = Some(mysql_writer_password);
    let source_instance = store.save_instance(admin, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = "web-pg-target".into();
    sink_input.host = pg_host;
    sink_input.port = pg_port;
    sink_input.kind = "postgresql".into();
    sink_input.version = "15".into();
    sink_input.database = "CDC_test".into();
    sink_input.reader_username = pg_reader;
    sink_input.reader_password = Some(pg_password.clone());
    sink_input.writer_username = pg_writer;
    sink_input.writer_password = Some(pg_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();
    let task = store
        .create_task(
            admin,
            TaskInput {
                draft_id: None,
                name: "MySQL 5.7 to PostgreSQL 15".into(),
                source_id: source_instance.id,
                sink_id: sink_instance.id,
                source_database: String::new(),
                sink_database: "CDC_test".into(),
                source_revision: 1,
                sink_revision: 1,
                start_mode: "auto".into(),
                mappings: vec![TableMapping {
                    source_schema: "CDC_test".into(),
                    source_table: table.clone(),
                    sink_schema: "CDC_test".into(),
                    sink_table: table.clone(),
                    columns: vec!["id".into(), "message".into()],
                    conversion_options: std::collections::BTreeMap::new(),
                }],
                confirmations: vec![],
            },
        )
        .unwrap();
    store.start_task(admin, task.id.clone()).unwrap();
    wait_for(&store, &task.id, |task| {
        task.status == "running"
            && task
                .runtime
                .checkpoint
                .as_ref()
                .is_some_and(|cp| cp.phase == "incremental" && cp.snapshot_rows == 2)
    });
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table} VALUES(1,'incremental')"
        ))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows == 1);
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table} VALUES(2,'after restart')"
        ))
        .unwrap();
    store.start_task(admin, task.id.clone()).unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows == 2);
    let rows = pg_runtime
        .block_on(
            sqlx::query_as::<_, (i32, String)>(sqlx::AssertSqlSafe(format!(
                "SELECT id,message FROM \"CDC_test\".\"{table}\" ORDER BY id"
            )))
            .fetch_all(&mut target),
        )
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "incremental".into()),
            (2, "after restart".into()),
            (100, "snapshot A".into()),
            (101, "snapshot B".into())
        ]
    );
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    pg_runtime
        .block_on(target.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DELETE FROM cdc.log_info WHERE task_id='{}'; DROP TABLE \"CDC_test\".\"{table}\"",
            task.id
        )))))
        .unwrap();
    source
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    drop(store);
    drop(dir);
}

#[test]
#[ignore = "requires live MySQL 5.7 and PostgreSQL 15; isolated table and local SQLite"]
fn live_postgresql15_target_schema_change_invalidates_saved_plan() {
    use crate::Error;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("cap_inv_{nonce}");
    let mysql_host = std::env::var("CDC_MYSQL_HOST").unwrap();
    let mysql_port = std::env::var("CDC_MYSQL57_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let mysql_reader = std::env::var("CDC_MYSQL_READER_USER").unwrap();
    let mysql_reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let mysql_writer = std::env::var("CDC_MYSQL_WRITER_USER").unwrap();
    let mysql_writer_password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let mut source = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(mysql_host.clone()))
            .tcp_port(mysql_port)
            .user(Some(mysql_writer.clone()))
            .pass(Some(mysql_writer_password.clone())),
    )
    .unwrap();
    source
        .query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
        .unwrap();
    source
        .query_drop(format!(
            "CREATE TABLE CDC_test.{table}(id INT PRIMARY KEY,message VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL) ENGINE=InnoDB"
        ))
        .unwrap();
    source
        .query_drop(format!("INSERT INTO CDC_test.{table} VALUES(1,'planned')"))
        .unwrap();

    let pg_host = std::env::var("PG_CDC_HOST").unwrap();
    let pg_port = std::env::var("PG_CDC_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let pg_password = std::env::var("PG_CDC_TEST_PASSWORD").unwrap();
    let pg_admin = std::env::var("PG_CDC_ADMIN_USER").unwrap();
    let pg_reader = std::env::var("PG_CDC_READER_USER").unwrap();
    let pg_writer = std::env::var("PG_CDC_WRITER_USER").unwrap();
    let pg_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pg_options = PgConnectOptions::new()
        .host(&pg_host)
        .port(pg_port)
        .database("CDC_test")
        .username(&pg_admin)
        .password(&pg_password)
        .ssl_mode(PgSslMode::Prefer);
    let mut target = pg_runtime
        .block_on(PgConnection::connect_with(&pg_options))
        .unwrap();
    pg_runtime
        .block_on(async {
            target
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA IF NOT EXISTS \"CDC_test\" AUTHORIZATION \"{}\";
                     GRANT USAGE ON SCHEMA \"CDC_test\" TO \"{}\";
                     CREATE TABLE \"CDC_test\".\"{table}\"(id integer PRIMARY KEY,message character varying(40) NOT NULL);
                     GRANT SELECT ON \"CDC_test\".\"{table}\" TO \"{}\";
                     GRANT SELECT,INSERT,UPDATE,DELETE ON \"CDC_test\".\"{table}\" TO \"{}\"",
                    pg_writer.replace('"', "\"\""),
                    pg_reader.replace('"', "\"\""),
                    pg_reader.replace('"', "\"\""),
                    pg_writer.replace('"', "\"\"")
                ))))
                .await
        })
        .unwrap();

    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = "invalidation-mysql-source".into();
    source_input.host = mysql_host;
    source_input.port = mysql_port;
    source_input.version = "5.7".into();
    source_input.reader_username = mysql_reader;
    source_input.reader_password = Some(mysql_reader_password);
    source_input.writer_username = mysql_writer;
    source_input.writer_password = Some(mysql_writer_password);
    let source_instance = store.save_instance(admin, None, source_input).unwrap();

    let mut sink_input = instance_input();
    sink_input.name = "invalidation-postgresql-target".into();
    sink_input.host = pg_host;
    sink_input.port = pg_port;
    sink_input.kind = "postgresql".into();
    sink_input.version = "15".into();
    sink_input.database = "CDC_test".into();
    sink_input.reader_username = pg_reader;
    sink_input.reader_password = Some(pg_password.clone());
    sink_input.writer_username = pg_writer.clone();
    sink_input.writer_password = Some(pg_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();

    let task = store
        .create_task(
            admin,
            TaskInput {
                draft_id: None,
                name: "live-target-plan-invalidation".into(),
                source_id: source_instance.id,
                sink_id: sink_instance.id,
                source_database: String::new(),
                sink_database: "CDC_test".into(),
                source_revision: 1,
                sink_revision: 1,
                start_mode: "auto".into(),
                mappings: vec![TableMapping {
                    source_schema: "CDC_test".into(),
                    source_table: table.clone(),
                    sink_schema: "CDC_test".into(),
                    sink_table: table.clone(),
                    columns: vec!["id".into(), "message".into()],
                    conversion_options: std::collections::BTreeMap::new(),
                }],
                confirmations: vec![],
            },
        )
        .unwrap();
    let initial_plan_is_valid = task.plan_status == "valid";
    let initial_plan_count = task.plans.len();

    pg_runtime
        .block_on(async {
            target
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "ALTER TABLE \"CDC_test\".\"{table}\" DROP COLUMN message"
                ))))
                .await
        })
        .unwrap();

    let requalification_rejected = store.requalify_task(admin, &task.id, vec![]).is_err();
    let status_after_requalification = store.task(&task.id).unwrap().plan_status;
    let start_result = store.start_task(admin, task.id.clone());
    let status_after_start = store.task(&task.id).unwrap().plan_status;
    store.shutdown_tasks();

    pg_runtime
        .block_on(async {
            target
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "DROP TABLE \"CDC_test\".\"{table}\""
                ))))
                .await
        })
        .unwrap();
    source
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    drop(store);
    drop(dir);

    assert!(initial_plan_is_valid, "initial task plan must be valid");
    assert!(
        initial_plan_count > 0,
        "initial task plan must contain tables"
    );
    assert!(
        requalification_rejected,
        "schema change must make task requalification fail"
    );
    assert_eq!(status_after_requalification, "stale");
    assert!(matches!(start_result, Err(Error::Conflict(_))));
    assert_eq!(status_after_start, "stale");
}

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 8.0; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql15_enum_to_mysql80_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(15, "8.0");
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 8.0; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql16_enum_to_mysql80_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(16, "8.0");
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 8.0; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql17_enum_to_mysql80_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(17, "8.0");
}

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 5.7; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql15_enum_to_mysql57_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(15, "5.7");
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 5.7; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql16_enum_to_mysql57_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(16, "5.7");
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 5.7; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql17_enum_to_mysql57_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(17, "5.7");
}

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 8.4; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql15_enum_to_mysql84_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(15, "8.4");
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 8.4; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql16_enum_to_mysql84_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(16, "8.4");
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 8.4; creates isolated type, table, publication membership, and local SQLite"]
fn live_web_postgresql17_enum_to_mysql84_carrier() {
    live_web_postgresql_enum_to_mysql_carrier(17, "8.4");
}

fn live_web_postgresql_enum_to_mysql_carrier(major: u16, mysql_version: &str) {
    use sha2::{Digest, Sha256};

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("web_pg_enum_{nonce}");
    let type_name = format!("web_mood_{nonce}");
    let prefix = if major == 15 {
        "PG_CDC".to_owned()
    } else {
        format!("PG_CDC{major}")
    };
    let pg_env = |suffix: &str| std::env::var(format!("{prefix}_{suffix}")).unwrap();
    let publication_name = format!("cdc_pg{major}_demo");
    let pg_host = pg_env("HOST");
    let pg_port = pg_env("PORT").parse::<u16>().unwrap();
    let pg_password = pg_env("TEST_PASSWORD");
    let pg_admin = pg_env("ADMIN_USER");
    let pg_reader = pg_env("READER_USER");
    let pg_writer = pg_env("WRITER_USER");
    let mysql_host = std::env::var("CDC_MYSQL_HOST").unwrap();
    let mysql_port_key = match mysql_version {
        "5.7" => "CDC_MYSQL57_PORT",
        "8.0" => "CDC_MYSQL80_PORT",
        "8.4" => "CDC_MYSQL84_PORT",
        _ => panic!("unsupported test MySQL version"),
    };
    let mysql_port = std::env::var(mysql_port_key)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let mysql_reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let mysql_writer_password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let options = PgConnectOptions::new()
        .host(&pg_host)
        .port(pg_port)
        .database("CDC_test")
        .username(&pg_admin)
        .password(&pg_password)
        .ssl_mode(PgSslMode::Prefer);
    let mut pg = runtime
        .block_on(PgConnection::connect_with(&options))
        .unwrap();
    runtime.block_on(async {
        pg.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE SCHEMA IF NOT EXISTS \"CDC_test\";
             CREATE TYPE \"CDC_test\".{type_name} AS ENUM ('calm','ready');
             CREATE TABLE \"CDC_test\".{table}(id bigint PRIMARY KEY, mood \"CDC_test\".{type_name} NOT NULL);
             GRANT USAGE ON SCHEMA \"CDC_test\" TO {pg_reader};
             GRANT USAGE ON TYPE \"CDC_test\".{type_name} TO {pg_reader};
             GRANT SELECT ON \"CDC_test\".{table} TO {pg_reader}"
        )))).await
    }).unwrap();
    let publication: Option<bool> = runtime
        .block_on(
            sqlx::query_scalar("SELECT puballtables FROM pg_publication WHERE pubname=$1")
                .bind(&publication_name)
                .fetch_optional(&mut pg),
        )
        .unwrap();
    let created_publication = publication.is_none();
    if created_publication {
        runtime
            .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE PUBLICATION {publication_name} FOR TABLE \"CDC_test\".{table}"
            )))))
            .unwrap();
    } else if publication == Some(false) {
        let already_in_publication: bool = runtime.block_on(
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_publication_tables WHERE pubname=$1 AND schemaname='CDC_test' AND tablename=$2)"
            ).bind(&publication_name).bind(&table).fetch_one(&mut pg),
        ).unwrap();
        if !already_in_publication {
            runtime
                .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "ALTER PUBLICATION {publication_name} ADD TABLE \"CDC_test\".{table}"
                )))))
                .unwrap();
        }
    }
    let mut mysql = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(mysql_host.clone()))
            .tcp_port(mysql_port)
            .user(Some(std::env::var("CDC_MYSQL_WRITER_USER").unwrap()))
            .pass(Some(mysql_writer_password.clone())),
    )
    .unwrap();
    mysql
        .query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
        .unwrap();
    mysql
        .query_drop(format!(
            "CREATE TABLE CDC_test.{table}(id BIGINT PRIMARY KEY, mood JSON NULL) ENGINE=InnoDB"
        ))
        .unwrap();

    let (_dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = format!("pg-enum-source-{nonce}");
    source_input.host = pg_host;
    source_input.port = pg_port;
    source_input.kind = "postgresql".into();
    source_input.version = major.to_string();
    source_input.database = "CDC_test".into();
    source_input.reader_username = pg_reader;
    source_input.reader_password = Some(pg_password.clone());
    source_input.writer_username = pg_writer;
    source_input.writer_password = Some(pg_password);
    let source_instance = store.save_instance(admin, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = format!("pg-enum-target-{mysql_version}-{nonce}");
    sink_input.host = mysql_host;
    sink_input.port = mysql_port;
    sink_input.version = mysql_version.into();
    sink_input.reader_password = Some(mysql_reader_password);
    sink_input.writer_password = Some(mysql_writer_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();
    let draft_id = format!("pg-enum-{nonce}");
    let preview_input = |parameters: BTreeMap<String, String>, confirmations| FieldPreviewInput {
        draft_id: draft_id.clone(),
        source_id: source_instance.id.clone(),
        sink_id: sink_instance.id.clone(),
        source_database: "CDC_test".into(),
        sink_database: String::new(),
        source_revision: 1,
        sink_revision: 1,
        schema: "CDC_test".into(),
        table: table.clone(),
        column: "mood".into(),
        parameters,
        confirmations,
    };
    let discovered = store
        .preview_field(admin, preview_input(BTreeMap::new(), vec![]))
        .unwrap();
    let candidate = discovered
        .available_candidates
        .iter()
        .find(|candidate| {
            candidate
                .target
                .parameters
                .get("conversion_kind")
                .map(String::as_str)
                == Some("logical_value_json")
        })
        .unwrap_or_else(|| {
            panic!(
                "Web must offer a tagged value carrier for a PostgreSQL enum: {}",
                serde_json::to_string_pretty(&discovered).unwrap()
            )
        });
    let parameters = BTreeMap::from([
        ("__rule_id".into(), candidate.rule.id.clone()),
        ("__rule_version".into(), candidate.rule.version.clone()),
    ]);
    let pending = store
        .preview_field(admin, preview_input(parameters.clone(), vec![]))
        .unwrap()
        .result
        .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    let pending_plan = pending.plan.unwrap();
    let confirmation = change_event::RiskConfirmation {
        source_field_lineage: pending_plan.source_field.lineage_id.clone(),
        target_field_lineage: pending_plan.target_field.lineage_id.clone(),
        rule: pending_plan.rule.clone(),
        plan_digest: pending_plan.plan_digest.clone(),
        actor: "admin".into(),
        confirmed_at: "2026-09-29T00:00:00Z".into(),
        reason: Some("tagged value carrier is not a native MySQL enum".into()),
    };
    let confirmed = store
        .preview_field(
            admin,
            preview_input(parameters.clone(), vec![confirmation.clone()]),
        )
        .unwrap()
        .result
        .unwrap();
    assert_eq!(
        confirmed.status,
        change_event::CompatibilityStatus::Compatible
    );
    let plan = confirmed.plan.unwrap();
    let task_input = |confirmations| TaskInput {
        draft_id: Some(draft_id.clone()),
        name: format!("live PostgreSQL {major} enum to MySQL {mysql_version} carrier {nonce}"),
        source_id: source_instance.id.clone(),
        sink_id: sink_instance.id.clone(),
        source_database: "CDC_test".into(),
        sink_database: String::new(),
        source_revision: 1,
        sink_revision: 1,
        start_mode: "auto".into(),
        mappings: vec![TableMapping {
            source_schema: "CDC_test".into(),
            source_table: table.clone(),
            sink_schema: "CDC_test".into(),
            sink_table: table.clone(),
            columns: vec!["id".into(), "mood".into()],
            conversion_options: BTreeMap::from([("mood".into(), parameters.clone())]),
        }],
        confirmations,
    };
    assert!(store.create_task(admin, task_input(vec![])).is_err());
    let task = store
        .create_task(admin, task_input(vec![confirmation]))
        .unwrap();
    assert_eq!(task.plan_status, "valid");
    assert!(
        task.plans
            .iter()
            .any(|saved| saved.plan_digest == plan.plan_digest)
    );
    store.start_task(admin, task.id.clone()).unwrap();
    wait_for(&store, &task.id, |task| task.status == "running");
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"CDC_test\".{table}(id,mood) VALUES(1,'calm')"
        )))))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 1);
    let stored: Option<String> = mysql
        .query_first(format!("SELECT mood FROM CDC_test.{table} WHERE id=1"))
        .unwrap();
    let value: change_event::LogicalValue =
        serde_json::from_str(stored.as_deref().unwrap()).unwrap();
    assert!(matches!(value, change_event::LogicalValue::Enum { label } if label == "calm"));
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"CDC_test\".{table}(id,mood) VALUES(2,'ready')"
        )))))
        .unwrap();
    let restart = store.start_task(admin, task.id.clone());
    assert!(
        restart.is_ok(),
        "PostgreSQL source restart: {:?}; saved-plan reason: {:?}",
        restart.err(),
        store.task(&task.id).unwrap().plan_invalid_reason
    );
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 2);
    let resumed: Option<String> = mysql
        .query_first(format!("SELECT mood FROM CDC_test.{table} WHERE id=2"))
        .unwrap();
    let value: change_event::LogicalValue =
        serde_json::from_str(resumed.as_deref().unwrap()).unwrap();
    assert!(matches!(value, change_event::LogicalValue::Enum { label } if label == "ready"));
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"CDC_test\".{table} SET mood='ready' WHERE id=1"
        )))))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 3);
    let updated: Option<String> = mysql
        .query_first(format!("SELECT mood FROM CDC_test.{table} WHERE id=1"))
        .unwrap();
    let value: change_event::LogicalValue =
        serde_json::from_str(updated.as_deref().unwrap()).unwrap();
    assert!(matches!(value, change_event::LogicalValue::Enum { label } if label == "ready"));
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM \"CDC_test\".{table} WHERE id=2"
        )))))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 4);
    let deleted: Option<String> = mysql
        .query_first(format!("SELECT mood FROM CDC_test.{table} WHERE id=2"))
        .unwrap();
    assert!(deleted.is_none());
    assert!(store.task(&task.id).unwrap().runtime.checkpoint.is_some());
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();

    let slot_hash = format!("{:x}", Sha256::digest(task.id.as_bytes()));
    let slot_name = format!("cdc_web_{}", &slot_hash[..16]);
    runtime
        .block_on(
            sqlx::query("SELECT pg_drop_replication_slot($1)")
                .bind(slot_name)
                .execute(&mut pg),
        )
        .unwrap();
    mysql
        .exec_drop("DELETE FROM CDC.log_info WHERE task_id=?", (&task.id,))
        .unwrap();
    mysql
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE \"CDC_test\".{table}"
        )))))
        .unwrap();
    runtime
        .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TYPE \"CDC_test\".{type_name}"
        )))))
        .unwrap();
    if created_publication {
        runtime
            .block_on(pg.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP PUBLICATION {publication_name}"
            )))))
            .unwrap();
    }
    let mysql_connector_id = format!("mysql_{}", mysql_version.replace('.', "_"));
    type_qualification_evidence::record_web_plan_evidence(
        &format!("postgresql_{major}"),
        &mysql_connector_id,
        &format!("web_ui.pg{major}_enum_to_{mysql_connector_id}_json_carrier_live"),
        "dynamic:postgresql.enums",
        &plan,
        "logical_value_json_carrier",
        true,
    )
    .unwrap();
}
