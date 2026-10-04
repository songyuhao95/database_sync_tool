use super::{instance_input, store};
use crate::{
    Error, Store,
    model::NewUser,
    tasks::{FieldPreviewInput, TableMapping, TaskInput},
};
use mysql_driver::{Conn, OptsBuilder, prelude::Queryable};
use sqlx::{
    Connection as _, Executor as _, PgConnection, Row as _,
    postgres::{PgConnectOptions, PgSslMode},
};
#[path = "../../../tests/support/postgres_builtin_fixtures.rs"]
mod postgres_builtin_fixtures;
use postgres_builtin_fixtures::BUILTINS;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[path = "../../../tests/support/type_qualification_evidence.rs"]
mod type_qualification_evidence;

struct MysqlFixtureTables {
    endpoints: Vec<(String, u16)>,
    user: String,
    password: String,
    table: String,
}

impl Drop for MysqlFixtureTables {
    fn drop(&mut self) {
        for (host, port) in &self.endpoints {
            if let Ok(mut connection) = Conn::new(
                OptsBuilder::new()
                    .ip_or_hostname(Some(host.clone()))
                    .tcp_port(*port)
                    .user(Some(self.user.clone()))
                    .pass(Some(self.password.clone()))
                    .tcp_connect_timeout(Some(Duration::from_secs(5))),
            ) {
                let _ =
                    connection.query_drop(format!("DROP TABLE IF EXISTS CDC_test.{}", self.table));
            }
        }
    }
}

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
        let task_logs = if task.status == "failed" {
            store
                .task_logs(id, 0)
                .unwrap()
                .into_iter()
                .map(|entry| entry.message)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        assert_ne!(
            task.status, "failed",
            "task failed: {:?}; task logs: {:?}",
            task.runtime.last_error, task_logs
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
#[ignore = "requires live MySQL 5.7 and PostgreSQL 15; creates isolated 37-type source and target tables"]
fn live_web_mysql57_all_native_types_to_postgresql15_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("5.7", 15);
}

#[test]
#[ignore = "requires live MySQL 5.7 and PostgreSQL 16; creates isolated 37-type source and target tables"]
fn live_web_mysql57_all_native_types_to_postgresql16_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("5.7", 16);
}

#[test]
#[ignore = "requires live MySQL 5.7 and PostgreSQL 17; creates isolated 37-type source and target tables"]
fn live_web_mysql57_all_native_types_to_postgresql17_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("5.7", 17);
}

#[test]
#[ignore = "requires live MySQL 8.0 and PostgreSQL 15; creates isolated 37-type source and target tables"]
fn live_web_mysql80_all_native_types_to_postgresql15_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.0", 15);
}

#[test]
#[ignore = "requires live MySQL 8.0 and PostgreSQL 16; creates isolated 37-type source and target tables"]
fn live_web_mysql80_all_native_types_to_postgresql16_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.0", 16);
}

#[test]
#[ignore = "requires live MySQL 8.0 and PostgreSQL 17; creates isolated 37-type source and target tables"]
fn live_web_mysql80_all_native_types_to_postgresql17_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.0", 17);
}

#[test]
#[ignore = "requires live MySQL 8.4 and PostgreSQL 15; creates isolated 37-type source and target tables"]
fn live_web_mysql84_all_native_types_to_postgresql15_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.4", 15);
}

#[test]
#[ignore = "requires live MySQL 8.4 and PostgreSQL 16; creates isolated 37-type source and target tables"]
fn live_web_mysql84_all_native_types_to_postgresql16_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.4", 16);
}

#[test]
#[ignore = "requires live MySQL 8.4 and PostgreSQL 17; creates isolated 37-type source and target tables"]
fn live_web_mysql84_all_native_types_to_postgresql17_carriers() {
    live_web_mysql_all_native_types_to_postgresql_carriers("8.4", 17);
}

const MYSQL_NATIVE_FIELDS: &[(&str, &str, &str, &str)] = &[
    ("tinyint_value", "TINYINT", "mysql.tinyint", "-12"),
    ("smallint_value", "SMALLINT", "mysql.smallint", "-1234"),
    ("mediumint_value", "MEDIUMINT", "mysql.mediumint", "-12345"),
    ("integer_value", "INT", "mysql.integer", "-123456"),
    ("bigint_value", "BIGINT", "mysql.bigint", "-123456789"),
    (
        "decimal_value",
        "DECIMAL(30,6)",
        "mysql.decimal",
        "12345.125000",
    ),
    ("float_value", "FLOAT", "mysql.float", "1.25"),
    ("double_value", "DOUBLE", "mysql.double", "1.125"),
    ("bit_value", "BIT(8)", "mysql.bit", "b'10100101'"),
    ("date_value", "DATE", "mysql.date", "'2024-02-29'"),
    ("time_value", "TIME(6)", "mysql.time", "'12:30:15.123456'"),
    (
        "datetime_value",
        "DATETIME(6)",
        "mysql.datetime",
        "'2024-02-29 12:30:15.123456'",
    ),
    (
        "timestamp_value",
        "TIMESTAMP(6) NULL",
        "mysql.timestamp",
        "'2024-02-29 12:30:15.123456'",
    ),
    ("year_value", "YEAR", "mysql.year", "2024"),
    ("char_value", "CHAR(16)", "mysql.char", "'fixed'"),
    (
        "varchar_value",
        "VARCHAR(32)",
        "mysql.varchar",
        "'variable'",
    ),
    ("binary_value", "BINARY(4)", "mysql.binary", "X'01020304'"),
    (
        "varbinary_value",
        "VARBINARY(8)",
        "mysql.varbinary",
        "X'00FF'",
    ),
    ("tinytext_value", "TINYTEXT", "mysql.tinytext", "'tiny'"),
    ("text_value", "TEXT", "mysql.text", "'text'"),
    (
        "mediumtext_value",
        "MEDIUMTEXT",
        "mysql.mediumtext",
        "'medium'",
    ),
    ("longtext_value", "LONGTEXT", "mysql.longtext", "'long'"),
    ("tinyblob_value", "TINYBLOB", "mysql.tinyblob", "X'01'"),
    ("blob_value", "BLOB", "mysql.blob", "X'0203'"),
    (
        "mediumblob_value",
        "MEDIUMBLOB",
        "mysql.mediumblob",
        "X'040506'",
    ),
    ("longblob_value", "LONGBLOB", "mysql.longblob", "X'070809'"),
    ("enum_value", "ENUM('alpha','beta')", "mysql.enum", "'beta'"),
    ("set_value", "SET('a','b','c')", "mysql.set", "'a,c'"),
    (
        "json_value",
        "JSON",
        "mysql.json",
        "JSON_OBJECT('kind','web')",
    ),
    (
        "geometry_value",
        "GEOMETRY",
        "mysql.geometry",
        "ST_GeomFromText('POINT(1 2)')",
    ),
    (
        "point_value",
        "POINT",
        "mysql.point",
        "ST_GeomFromText('POINT(1 2)')",
    ),
    (
        "linestring_value",
        "LINESTRING",
        "mysql.linestring",
        "ST_GeomFromText('LINESTRING(0 0,1 1)')",
    ),
    (
        "polygon_value",
        "POLYGON",
        "mysql.polygon",
        "ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))')",
    ),
    (
        "multipoint_value",
        "MULTIPOINT",
        "mysql.multipoint",
        "ST_GeomFromText('MULTIPOINT((1 1),(2 2))')",
    ),
    (
        "multilinestring_value",
        "MULTILINESTRING",
        "mysql.multilinestring",
        "ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))')",
    ),
    (
        "multipolygon_value",
        "MULTIPOLYGON",
        "mysql.multipolygon",
        "ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))')",
    ),
    (
        "geometrycollection_value",
        "GEOMETRYCOLLECTION",
        "mysql.geometrycollection",
        "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))')",
    ),
];

#[test]
#[ignore = "requires separate source and target MySQL 5.7 instances for full Web task qualification"]
fn live_web_mysql57_same_version_all_types() {
    live_web_mysql_same_version_all_types("5.7");
}

#[test]
#[ignore = "requires separate source and target MySQL 8.0 instances for full Web task qualification"]
fn live_web_mysql80_same_version_all_types() {
    live_web_mysql_same_version_all_types("8.0");
}

#[test]
#[ignore = "requires separate source and target MySQL 8.4 instances for full Web task qualification"]
fn live_web_mysql84_same_version_all_types() {
    live_web_mysql_same_version_all_types("8.4");
}

fn live_web_mysql_same_version_all_types(version: &str) {
    live_web_mysql_all_native_types_to_mysql_carriers(version, version);
}
fn live_web_mysql_all_native_types_to_postgresql_carriers(mysql_version: &str, pg_major: u16) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("web_mysql_types_{nonce}");
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
    let pg_prefix = if pg_major == 15 {
        "PG_CDC".to_owned()
    } else {
        format!("PG_CDC{pg_major}")
    };
    let pg_env = |suffix: &str| std::env::var(format!("{pg_prefix}_{suffix}")).unwrap();
    let pg_host = pg_env("HOST");
    let pg_port = pg_env("PORT").parse::<u16>().unwrap();
    let pg_password = pg_env("TEST_PASSWORD");
    let pg_admin = pg_env("ADMIN_USER");
    let pg_reader = pg_env("READER_USER");
    let pg_writer = pg_env("WRITER_USER");
    let _mysql_fixture = MysqlFixtureTables {
        endpoints: vec![(mysql_host.clone(), mysql_port)],
        user: std::env::var("CDC_MYSQL_WRITER_USER").unwrap(),
        password: mysql_writer_password.clone(),
        table: table.clone(),
    };
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
    let mysql_columns = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, kind, _, _)| format!("{name} {kind} NULL"))
        .collect::<Vec<_>>()
        .join(",");
    source.query_drop(format!("CREATE TABLE CDC_test.{table}(id BIGINT PRIMARY KEY,{mysql_columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")).unwrap();
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
    let mut target = runtime
        .block_on(PgConnection::connect_with(&options))
        .unwrap();
    let pg_columns = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, _, _, _)| format!("{name} text"))
        .collect::<Vec<_>>()
        .join(",");
    runtime
        .block_on(target.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE SCHEMA IF NOT EXISTS \"CDC_test\";
         CREATE SCHEMA IF NOT EXISTS cdc;
         GRANT USAGE,CREATE ON SCHEMA cdc TO {pg_writer};
         GRANT USAGE ON SCHEMA \"CDC_test\" TO {pg_writer};
         CREATE TABLE \"CDC_test\".{table}(id bigint PRIMARY KEY,{pg_columns});
         GRANT SELECT,INSERT,UPDATE,DELETE ON \"CDC_test\".{table} TO {pg_writer}"
        )))))
        .unwrap();
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = format!("mysql-{mysql_version}-all-types-{nonce}");
    source_input.host = mysql_host;
    source_input.port = mysql_port;
    source_input.version = mysql_version.into();
    source_input.reader_password = Some(mysql_reader_password);
    source_input.writer_password = Some(mysql_writer_password);
    let source_instance = store.save_instance(admin, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = format!("pg-{pg_major}-all-types-{nonce}");
    sink_input.host = pg_host;
    sink_input.port = pg_port;
    sink_input.kind = "postgresql".into();
    sink_input.version = pg_major.to_string();
    sink_input.database = "CDC_test".into();
    sink_input.reader_username = pg_reader;
    sink_input.reader_password = Some(pg_password.clone());
    sink_input.writer_username = pg_writer;
    sink_input.writer_password = Some(pg_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();
    let source_identity = store
        .catalog_connection(
            admin,
            &source_instance.id,
            crate::catalog::EndpointRole::Source,
        )
        .unwrap();
    let sink_identity = store
        .catalog_connection(admin, &sink_instance.id, crate::catalog::EndpointRole::Sink)
        .unwrap();
    assert_ne!(
        source_identity.server_uuid, sink_identity.server_uuid,
        "route qualification requires independent source and target servers"
    );
    drop(sink_identity);
    drop(source_identity);
    let draft_id = format!(
        "mysql-{}-pg-{pg_major}-all-types-{nonce}",
        mysql_version.replace('.', "-")
    );
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
    for (column, _, type_id, _) in MYSQL_NATIVE_FIELDS {
        let discovered = store
            .preview_field(admin, preview_input(column, BTreeMap::new(), vec![]))
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
                    "Web lacks a target representation for {type_id}: {}",
                    serde_json::to_string_pretty(&discovered).unwrap()
                )
            });
        let parameters = BTreeMap::from([
            ("__rule_id".into(), candidate.rule.id.clone()),
            ("__rule_version".into(), candidate.rule.version.clone()),
        ]);
        let pending = store
            .preview_field(admin, preview_input(column, parameters.clone(), vec![]))
            .unwrap()
            .result
            .unwrap();
        let pending_plan = pending
            .plan
            .clone()
            .unwrap_or_else(|| panic!("Web could not plan {type_id}: {pending:?}"));
        let field_confirmations =
            if pending.status == change_event::CompatibilityStatus::NeedsConfirmation {
                let confirmation = change_event::RiskConfirmation {
                    source_field_lineage: pending_plan.source_field.lineage_id.clone(),
                    target_field_lineage: pending_plan.target_field.lineage_id.clone(),
                    rule: pending_plan.rule.clone(),
                    plan_digest: pending_plan.plan_digest.clone(),
                    actor: "admin".into(),
                    confirmed_at: "2026-09-30T00:00:00Z".into(),
                    reason: Some("tagged value carrier replaces native target behavior".into()),
                };
                confirmations.push(confirmation.clone());
                vec![confirmation]
            } else {
                assert_eq!(
                    pending.status,
                    change_event::CompatibilityStatus::Compatible,
                    "{type_id}: {}",
                    pending.explanation
                );
                vec![]
            };
        let confirmed = store
            .preview_field(
                admin,
                preview_input(column, parameters.clone(), field_confirmations),
            )
            .unwrap()
            .result
            .unwrap();
        assert_eq!(
            confirmed.status,
            change_event::CompatibilityStatus::Compatible,
            "{type_id}: {}",
            confirmed.explanation
        );
        conversion_options.insert((*column).to_owned(), parameters);
        confirmed_plans.push((*type_id, confirmed.plan.unwrap()));
    }
    let task = store
        .create_task(
            admin,
            TaskInput {
                draft_id: Some(draft_id),
                name: format!(
                    "live MySQL {mysql_version} all types to PostgreSQL {pg_major} {nonce}"
                ),
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
                    columns: std::iter::once("id".to_owned())
                        .chain(
                            MYSQL_NATIVE_FIELDS
                                .iter()
                                .map(|(name, _, _, _)| (*name).to_owned()),
                        )
                        .collect(),
                    conversion_options,
                }],
                confirmations,
            },
        )
        .unwrap();
    assert_eq!(task.plan_status, "valid");
    assert!(confirmed_plans.iter().all(|(_, plan)| {
        task.plans
            .iter()
            .any(|saved| saved.plan_digest == plan.plan_digest)
    }));
    store.start_task(admin, task.id.clone()).unwrap();
    wait_for(&store, &task.id, |task| task.status == "running");
    let names = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, _, _, _)| *name)
        .collect::<Vec<_>>()
        .join(",");
    let values = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(_, _, _, value)| *value)
        .collect::<Vec<_>>()
        .join(",");
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table}(id,{names}) VALUES(1,{values})"
        ))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 1);
    for (column, _, type_id, _) in MYSQL_NATIVE_FIELDS {
        let stored: String = runtime
            .block_on(
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT {column} FROM \"CDC_test\".{table} WHERE id=1"
                )))
                .fetch_one(&mut target),
            )
            .unwrap();
        serde_json::from_str::<change_event::LogicalValue>(&stored).unwrap_or_else(|error| {
            panic!("{type_id} target carrier is not replayable JSON: {error}")
        });
    }
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    assert!(store.task(&task.id).unwrap().runtime.checkpoint.is_some());
    source
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP TABLE \"CDC_test\".{table}"
            )))
            .execute(&mut target),
        )
        .unwrap();
    drop(dir);
    let source_connector_id = format!("mysql_{}", mysql_version.replace('.', "_"));
    let sink_connector_id = format!("postgresql_{pg_major}");
    let suite_id = format!(
        "web_ui.{source_connector_id}_all_native_types_to_{sink_connector_id}_carrier_live"
    );
    for (type_id, plan) in &confirmed_plans {
        type_qualification_evidence::record_web_plan_evidence(
            &source_connector_id,
            &sink_connector_id,
            &suite_id,
            type_id,
            plan,
            "logical_value_json_carrier",
            true,
        )
        .unwrap();
    }
}

#[test]
#[ignore = "requires live MySQL 5.7 and 8.0; creates isolated 37-type tables"]
fn live_web_mysql57_all_native_types_to_mysql80_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("5.7", "8.0");
}

#[test]
#[ignore = "requires live MySQL 5.7 and 8.4; creates isolated 37-type tables"]
fn live_web_mysql57_all_native_types_to_mysql84_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("5.7", "8.4");
}

#[test]
#[ignore = "requires live MySQL 8.0 and 5.7; creates isolated 37-type tables"]
fn live_web_mysql80_all_native_types_to_mysql57_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("8.0", "5.7");
}

#[test]
#[ignore = "requires live MySQL 8.0 and 8.4; creates isolated 37-type tables"]
fn live_web_mysql80_all_native_types_to_mysql84_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("8.0", "8.4");
}

#[test]
#[ignore = "requires live MySQL 8.4 and 5.7; creates isolated 37-type tables"]
fn live_web_mysql84_all_native_types_to_mysql57_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("8.4", "5.7");
}

#[test]
#[ignore = "requires live MySQL 8.4 and 8.0; creates isolated 37-type tables"]
fn live_web_mysql84_all_native_types_to_mysql80_carriers() {
    live_web_mysql_all_native_types_to_mysql_carriers("8.4", "8.0");
}

fn live_web_mysql_all_native_types_to_mysql_carriers(source_version: &str, sink_version: &str) {
    let port = |version: &str| {
        let key = match version {
            "5.7" => "CDC_MYSQL57_PORT",
            "8.0" => "CDC_MYSQL80_PORT",
            "8.4" => "CDC_MYSQL84_PORT",
            _ => panic!("unsupported test MySQL version"),
        };
        std::env::var(key).unwrap().parse::<u16>().unwrap()
    };
    let source_port = port(source_version);
    let sink_port = if source_version == sink_version {
        let key = match sink_version {
            "5.7" => "CDC_MYSQL57_SINK_PORT",
            "8.0" => "CDC_MYSQL80_SINK_PORT",
            "8.4" => "CDC_MYSQL84_SINK_PORT",
            _ => panic!("unsupported test MySQL version"),
        };
        std::env::var(key)
            .unwrap_or_else(|_| panic!("set {key} to a separate same-version MySQL target"))
            .parse::<u16>()
            .unwrap()
    } else {
        port(sink_version)
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("web_mysql_types_{nonce}");
    let host = std::env::var("CDC_MYSQL_HOST").unwrap();
    let sink_host_key = match source_version {
        "5.7" => "CDC_MYSQL57_SINK_HOST",
        "8.0" => "CDC_MYSQL80_SINK_HOST",
        "8.4" => "CDC_MYSQL84_SINK_HOST",
        _ => panic!("unsupported test MySQL version"),
    };
    let sink_host = if source_version == sink_version {
        std::env::var(sink_host_key).unwrap_or_else(|_| {
            panic!("set {sink_host_key} to the separate same-version MySQL target")
        })
    } else {
        host.clone()
    };
    let reader_password = std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let writer_password = std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    let writer_user = std::env::var("CDC_MYSQL_WRITER_USER").unwrap();
    let _mysql_fixture = MysqlFixtureTables {
        endpoints: vec![(host.clone(), source_port), (sink_host.clone(), sink_port)],
        user: writer_user.clone(),
        password: writer_password.clone(),
        table: table.clone(),
    };
    let connect = |version: &str, endpoint_host: &str, endpoint_port: u16| {
        for attempt in 1..=10 {
            match Conn::new(
                OptsBuilder::new()
                    .ip_or_hostname(Some(endpoint_host.to_owned()))
                    .tcp_port(endpoint_port)
                    .user(Some(writer_user.clone()))
                    .pass(Some(writer_password.clone())),
            ) {
                Ok(connection) => return connection,
                Err(_) if attempt < 10 => thread::sleep(Duration::from_secs(3)),
                Err(_) => panic!("unable to connect to configured MySQL {version} test instance"),
            }
        }
        unreachable!("connection loop returns or panics")
    };
    let mut source = connect(source_version, &host, source_port);
    let mut target = connect(sink_version, &sink_host, sink_port);
    let source_columns = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, kind, _, _)| format!("{name} {kind} NULL"))
        .collect::<Vec<_>>()
        .join(",");
    let target_columns = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, _, _, _)| format!("{name} JSON NULL"))
        .collect::<Vec<_>>()
        .join(",");
    source
        .query_drop(format!("CREATE TABLE CDC_test.{table}(id BIGINT PRIMARY KEY,{source_columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"))
        .unwrap();
    target
        .query_drop(format!("CREATE TABLE CDC_test.{table}(id BIGINT PRIMARY KEY,{target_columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"))
        .unwrap();
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = format!("mysql-{source_version}-source-types-{nonce}");
    source_input.host = host.clone();
    source_input.port = source_port;
    source_input.version = source_version.into();
    source_input.reader_password = Some(reader_password.clone());
    source_input.writer_password = Some(writer_password.clone());
    let source_instance = store.save_instance(admin, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = format!("mysql-{sink_version}-sink-types-{nonce}");
    sink_input.host = sink_host;
    sink_input.port = sink_port;
    sink_input.version = sink_version.into();
    sink_input.reader_password = Some(reader_password);
    sink_input.writer_password = Some(writer_password);
    let sink_instance = store.save_instance(admin, None, sink_input).unwrap();
    let source_identity = store
        .catalog_connection(
            admin,
            &source_instance.id,
            crate::catalog::EndpointRole::Source,
        )
        .unwrap();
    let sink_identity = store
        .catalog_connection(admin, &sink_instance.id, crate::catalog::EndpointRole::Sink)
        .unwrap();
    assert_ne!(
        source_identity.server_uuid, sink_identity.server_uuid,
        "same-version qualification requires independent MySQL servers"
    );
    drop(sink_identity);
    drop(source_identity);
    let draft_id = format!(
        "mysql-{}-to-{}-types-{nonce}",
        source_version.replace('.', "-"),
        sink_version.replace('.', "-")
    );
    let preview_input =
        |column: &str, parameters: BTreeMap<String, String>, confirmations| FieldPreviewInput {
            draft_id: draft_id.clone(),
            source_id: source_instance.id.clone(),
            sink_id: sink_instance.id.clone(),
            source_database: String::new(),
            sink_database: String::new(),
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
    for (column, _, type_id, _) in MYSQL_NATIVE_FIELDS {
        let discovered = store
            .preview_field(admin, preview_input(column, BTreeMap::new(), vec![]))
            .unwrap();
        let candidate = discovered.available_candidates.iter().find(|candidate| {
            candidate
                .target
                .parameters
                .get("conversion_kind")
                .map(String::as_str)
                == Some("logical_value_json")
        });
        let Some(candidate) = candidate else {
            let native = discovered.result.unwrap_or_else(|| {
                panic!("Web has neither a carrier nor a native plan for {type_id}")
            });
            assert_eq!(
                native.status,
                change_event::CompatibilityStatus::Compatible,
                "{type_id}: {}",
                native.explanation
            );
            confirmed_plans.push((*type_id, native.plan.unwrap(), "native_target_column"));
            continue;
        };
        let parameters = BTreeMap::from([
            ("__rule_id".into(), candidate.rule.id.clone()),
            ("__rule_version".into(), candidate.rule.version.clone()),
        ]);
        let pending = store
            .preview_field(admin, preview_input(column, parameters.clone(), vec![]))
            .unwrap()
            .result
            .unwrap();
        let pending_plan = pending
            .plan
            .clone()
            .unwrap_or_else(|| panic!("Web could not plan {type_id}: {pending:?}"));
        let field_confirmations =
            if pending.status == change_event::CompatibilityStatus::NeedsConfirmation {
                let confirmation = change_event::RiskConfirmation {
                    source_field_lineage: pending_plan.source_field.lineage_id.clone(),
                    target_field_lineage: pending_plan.target_field.lineage_id.clone(),
                    rule: pending_plan.rule.clone(),
                    plan_digest: pending_plan.plan_digest.clone(),
                    actor: "admin".into(),
                    confirmed_at: "2026-09-30T00:00:00Z".into(),
                    reason: Some("JSON carrier replaces native target behavior".into()),
                };
                confirmations.push(confirmation.clone());
                vec![confirmation]
            } else {
                assert_eq!(
                    pending.status,
                    change_event::CompatibilityStatus::Compatible
                );
                vec![]
            };
        let confirmed = store
            .preview_field(
                admin,
                preview_input(column, parameters.clone(), field_confirmations),
            )
            .unwrap()
            .result
            .unwrap();
        assert_eq!(
            confirmed.status,
            change_event::CompatibilityStatus::Compatible,
            "{type_id}: {}",
            confirmed.explanation
        );
        conversion_options.insert((*column).to_owned(), parameters);
        confirmed_plans.push((
            *type_id,
            confirmed.plan.unwrap(),
            "logical_value_json_carrier",
        ));
    }
    let task = store
        .create_task(
            admin,
            TaskInput {
                draft_id: Some(draft_id),
                name: format!("live MySQL {source_version} to {sink_version} all types {nonce}"),
                source_id: source_instance.id,
                sink_id: sink_instance.id,
                source_database: String::new(),
                sink_database: String::new(),
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
                            MYSQL_NATIVE_FIELDS
                                .iter()
                                .map(|(name, _, _, _)| (*name).to_owned()),
                        )
                        .collect(),
                    conversion_options,
                }],
                confirmations,
            },
        )
        .unwrap();
    assert_eq!(task.plan_status, "valid");
    assert!(confirmed_plans.iter().all(|(_, plan, _)| {
        task.plans
            .iter()
            .any(|saved| saved.plan_digest == plan.plan_digest)
    }));
    store.start_task(admin, task.id.clone()).unwrap();
    wait_for(&store, &task.id, |task| task.status == "running");
    let names = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(name, _, _, _)| *name)
        .collect::<Vec<_>>()
        .join(",");
    let values = MYSQL_NATIVE_FIELDS
        .iter()
        .map(|(_, _, _, value)| *value)
        .collect::<Vec<_>>()
        .join(",");
    source
        .query_drop(format!(
            "INSERT INTO CDC_test.{table}(id,{names}) VALUES(1,{values})"
        ))
        .unwrap();
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 1);
    for (column, _, type_id, _) in MYSQL_NATIVE_FIELDS {
        let stored: String = target
            .query_first(format!("SELECT {column} FROM CDC_test.{table} WHERE id=1"))
            .unwrap()
            .unwrap();
        if *type_id == "mysql.json" {
            let value: serde_json::Value = serde_json::from_str(&stored)
                .unwrap_or_else(|error| panic!("{type_id} native JSON is invalid: {error}"));
            assert_eq!(value, serde_json::json!({"kind": "web"}));
        } else {
            serde_json::from_str::<change_event::LogicalValue>(&stored)
                .unwrap_or_else(|error| panic!("{type_id} target JSON is not replayable: {error}"));
        }
    }
    store.stop_task(admin, &task.id).unwrap();
    store.shutdown_tasks();
    assert!(store.task(&task.id).unwrap().runtime.checkpoint.is_some());
    source
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    target
        .query_drop(format!("DROP TABLE CDC_test.{table}"))
        .unwrap();
    drop(dir);
    let source_connector_id = format!("mysql_{}", source_version.replace('.', "_"));
    let sink_connector_id = format!("mysql_{}", sink_version.replace('.', "_"));
    let suite_id = format!(
        "web_ui.{source_connector_id}_all_native_types_to_{sink_connector_id}_carrier_live"
    );
    for (type_id, plan, storage_mode) in &confirmed_plans {
        type_qualification_evidence::record_web_plan_evidence(
            &source_connector_id,
            &sink_connector_id,
            &suite_id,
            type_id,
            plan,
            storage_mode,
            true,
        )
        .unwrap();
    }
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
    wait_for(&store, &task.id, |task| task.runtime.applied_rows >= 2);
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

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 5.7; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql15_all_builtin_types_to_mysql57_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Mysql("5.7"));
}

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 8.0; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql15_all_builtin_types_to_mysql80_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Mysql("8.0"));
}

#[test]
#[ignore = "requires live PostgreSQL 15 and MySQL 8.4; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql15_all_builtin_types_to_mysql84_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Mysql("8.4"));
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 5.7; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql16_all_builtin_types_to_mysql57_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Mysql("5.7"));
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 8.0; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql16_all_builtin_types_to_mysql80_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Mysql("8.0"));
}

#[test]
#[ignore = "requires live PostgreSQL 16 and MySQL 8.4; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql16_all_builtin_types_to_mysql84_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Mysql("8.4"));
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 5.7; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql17_all_builtin_types_to_mysql57_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Mysql("5.7"));
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 8.0; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql17_all_builtin_types_to_mysql80_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Mysql("8.0"));
}

#[test]
#[ignore = "requires live PostgreSQL 17 and MySQL 8.4; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql17_all_builtin_types_to_mysql84_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Mysql("8.4"));
}

#[test]
#[ignore = "requires live PostgreSQL 15 and PostgreSQL 16; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql15_all_builtin_types_to_postgresql16_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Postgresql(16));
}

#[test]
#[ignore = "requires live PostgreSQL 15 and PostgreSQL 17; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql15_all_builtin_types_to_postgresql17_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Postgresql(17));
}

#[test]
#[ignore = "requires live PostgreSQL 16 and PostgreSQL 15; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql16_all_builtin_types_to_postgresql15_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Postgresql(15));
}

#[test]
#[ignore = "requires live PostgreSQL 16 and PostgreSQL 17; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql16_all_builtin_types_to_postgresql17_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Postgresql(17));
}

#[test]
#[ignore = "requires live PostgreSQL 17 and PostgreSQL 15; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql17_all_builtin_types_to_postgresql15_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Postgresql(15));
}

#[test]
#[ignore = "requires live PostgreSQL 17 and PostgreSQL 16; creates isolated all-builtins Web route fixtures"]
fn live_web_postgresql17_all_builtin_types_to_postgresql16_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Postgresql(16));
}

#[test]
#[ignore = "requires live PostgreSQL 15; creates an isolated target database and exercises every built-in type through Web preview, task save, start gate, and readback"]
fn live_web_postgresql15_all_builtin_types_to_postgresql15_carriers() {
    live_web_postgresql_all_builtin_types(15, LiveTypeSink::Postgresql(15));
}

#[test]
#[ignore = "requires live PostgreSQL 16; creates an isolated target database and exercises every built-in type through Web preview, task save, start gate, and readback"]
fn live_web_postgresql16_all_builtin_types_to_postgresql16_carriers() {
    live_web_postgresql_all_builtin_types(16, LiveTypeSink::Postgresql(16));
}

#[test]
#[ignore = "requires live PostgreSQL 17; creates an isolated target database and exercises every built-in type through Web preview, task save, start gate, and readback"]
fn live_web_postgresql17_all_builtin_types_to_postgresql17_carriers() {
    live_web_postgresql_all_builtin_types(17, LiveTypeSink::Postgresql(17));
}

#[test]
#[ignore = "requires configured PostgreSQL 15/16/17 and all six sink versions; exercises dynamic types through Web tasks"]
fn live_web_postgresql15_dynamic_types_to_all_sinks() {
    live_web_postgresql_dynamic_types(15);
}

#[test]
#[ignore = "requires configured PostgreSQL 15/16/17 and all six sink versions; exercises dynamic types through Web tasks"]
fn live_web_postgresql16_dynamic_types_to_all_sinks() {
    live_web_postgresql_dynamic_types(16);
}

#[test]
#[ignore = "requires configured PostgreSQL 15/16/17 and all six sink versions; exercises dynamic types through Web tasks"]
fn live_web_postgresql17_dynamic_types_to_all_sinks() {
    live_web_postgresql_dynamic_types(17);
}

#[test]
#[ignore = "requires live PostgreSQL 15; uses a temporary target database on the same server"]
fn live_web_postgresql15_dynamic_types_to_postgresql15() {
    cleanup_live_postgresql_dynamic_type_schema(15);
    live_web_postgresql_all_types(15, LiveTypeSink::Postgresql(15), true);
    cleanup_live_postgresql_dynamic_type_schema(15);
}

#[derive(Clone, Copy)]
enum LiveTypeSink {
    Mysql(&'static str),
    Postgresql(u16),
}

#[derive(Clone)]
struct LivePostgresqlField {
    name: String,
    declaration: String,
    expression: String,
    type_ids: Vec<String>,
}

fn live_postgresql_type_dependency_oids(
    catalog: &postgresql_15::SourceTypeCatalog,
    root_oid: u32,
) -> Vec<u32> {
    let mut pending = vec![root_oid];
    let mut discovered = BTreeMap::<u32, ()>::new();
    while let Some(oid) = pending.pop() {
        if discovered.insert(oid, ()).is_some() {
            continue;
        }
        let definition = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
            .unwrap_or_else(|| panic!("source type dependency OID {oid} is missing from catalog"));
        match &definition.kind {
            postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. } => {
                pending.push(*base_oid)
            }
            postgresql_15::SourceTypeDefinitionKind::Composite { fields } => {
                pending.extend(fields.iter().map(|field| field.type_oid));
            }
            postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } => {
                pending.push(*element_oid)
            }
            postgresql_15::SourceTypeDefinitionKind::Range { subtype_oid } => {
                pending.push(*subtype_oid)
            }
            postgresql_15::SourceTypeDefinitionKind::MultiRange { range_oid } => {
                pending.push(*range_oid)
            }
            postgresql_15::SourceTypeDefinitionKind::Pseudo
            | postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
            | postgresql_15::SourceTypeDefinitionKind::Enum { .. }
            | postgresql_15::SourceTypeDefinitionKind::Extension { .. } => {}
        }
    }
    discovered.into_keys().collect()
}

#[derive(Debug, Default)]
struct LivePostgresqlValueTypeCoverage {
    values: BTreeSet<u32>,
    null_only: BTreeSet<u32>,
}

fn live_postgresql_value_type_coverage(
    catalog: &postgresql_15::SourceTypeCatalog,
    root_oid: u32,
    datum: &change_event::Datum,
) -> LivePostgresqlValueTypeCoverage {
    fn record(
        catalog: &postgresql_15::SourceTypeCatalog,
        coverage: &mut LivePostgresqlValueTypeCoverage,
        oid: u32,
        has_value: bool,
    ) {
        let Some(definition) = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
        else {
            return;
        };
        if matches!(
            definition.kind,
            postgresql_15::SourceTypeDefinitionKind::Pseudo
        ) {
            return;
        }
        if has_value {
            coverage.null_only.remove(&oid);
            coverage.values.insert(oid);
        } else if !coverage.values.contains(&oid) {
            coverage.null_only.insert(oid);
        }
    }

    fn visit(
        catalog: &postgresql_15::SourceTypeCatalog,
        oid: u32,
        value: &change_event::LogicalValue,
        coverage: &mut LivePostgresqlValueTypeCoverage,
    ) {
        let Some(definition) = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
        else {
            return;
        };
        if matches!(
            definition.kind,
            postgresql_15::SourceTypeDefinitionKind::Pseudo
        ) {
            return;
        }
        if matches!(value, change_event::LogicalValue::Null) {
            record(catalog, coverage, oid, false);
            if let postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. } =
                &definition.kind
            {
                visit(catalog, *base_oid, value, coverage);
            }
            return;
        }
        record(catalog, coverage, oid, true);
        match (&definition.kind, value) {
            (
                postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. },
                change_event::LogicalValue::Domain { value },
            ) => visit(catalog, *base_oid, value, coverage),
            (postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. }, value) => {
                visit(catalog, *base_oid, value, coverage)
            }
            (
                postgresql_15::SourceTypeDefinitionKind::Composite {
                    fields: type_fields,
                },
                change_event::LogicalValue::Struct { fields: values },
            ) => {
                for type_field in type_fields {
                    if let Some(value_field) = values
                        .iter()
                        .find(|value_field| value_field.name == type_field.name)
                    {
                        visit(catalog, type_field.type_oid, &value_field.value, coverage);
                    }
                }
            }
            (
                postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. },
                change_event::LogicalValue::Array { elements }
                | change_event::LogicalValue::ArrayWithMetadata { elements, .. },
            ) => {
                for element in elements {
                    visit(catalog, *element_oid, element, coverage);
                }
            }
            (
                postgresql_15::SourceTypeDefinitionKind::Range { subtype_oid },
                change_event::LogicalValue::Range { lower, upper, .. },
            ) => {
                if let Some(lower) = lower {
                    visit(catalog, *subtype_oid, lower, coverage);
                }
                if let Some(upper) = upper {
                    visit(catalog, *subtype_oid, upper, coverage);
                }
            }
            (
                postgresql_15::SourceTypeDefinitionKind::MultiRange { range_oid },
                change_event::LogicalValue::MultiRange { ranges },
            ) => {
                for range in ranges {
                    visit(catalog, *range_oid, range, coverage);
                }
            }
            _ => {}
        }
    }

    let mut coverage = LivePostgresqlValueTypeCoverage::default();
    match datum {
        change_event::Datum::Null => {
            let mut oid = root_oid;
            let mut visited = BTreeSet::new();
            while visited.insert(oid) {
                let Some(definition) = catalog.types.iter().find(|item| item.oid == oid) else {
                    break;
                };
                if matches!(
                    definition.kind,
                    postgresql_15::SourceTypeDefinitionKind::Pseudo
                ) {
                    break;
                }
                record(catalog, &mut coverage, oid, false);
                let postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. } =
                    &definition.kind
                else {
                    break;
                };
                oid = *base_oid;
            }
        }
        change_event::Datum::Value(value) => visit(catalog, root_oid, value, &mut coverage),
        // An envelope preserves one opaque value of its declared type. It
        // does not expose which recursive members were present in that value.
        change_event::Datum::SourceRepresentationEnvelope(_) => {
            record(catalog, &mut coverage, root_oid, true)
        }
        change_event::Datum::Unavailable | change_event::Datum::Unchanged => {}
    }
    coverage
}

fn live_postgresql_catalog_type_id(definition: &postgresql_15::SourceTypeDefinition) -> String {
    format!(
        "dynamic:postgresql.instance.{}.{}:{}",
        definition.schema, definition.name, definition.definition_digest
    )
}

#[test]
fn postgresql_recursive_value_type_coverage_tracks_nested_values_and_nulls() {
    use change_event::{Datum, LogicalValue, StructuredField};
    use postgresql_15::{SourceTypeCatalog, SourceTypeDefinition, SourceTypeField};

    let catalog = SourceTypeCatalog::new([
        SourceTypeDefinition::builtin(23, "pg_catalog", "int4"),
        SourceTypeDefinition::domain(
            80_001,
            "fixture",
            "positive_int",
            23,
            std::iter::empty::<String>(),
            false,
            None,
        ),
        SourceTypeDefinition::enum_type(80_002, "fixture", "mood", ["calm", "ready"]),
        SourceTypeDefinition::composite(
            80_003,
            "fixture",
            "person",
            [
                SourceTypeField {
                    name: "score".into(),
                    type_oid: 80_001,
                    nullable: true,
                },
                SourceTypeField {
                    name: "mood".into(),
                    type_oid: 80_002,
                    nullable: true,
                },
            ],
        ),
        SourceTypeDefinition::array(80_004, "fixture", "mood_array", 80_002),
    ]);

    let composite = Datum::Value(LogicalValue::Struct {
        fields: vec![
            StructuredField {
                name: "score".into(),
                value: LogicalValue::Domain {
                    value: Box::new(LogicalValue::Integer {
                        signed: true,
                        bits: 32,
                        value: "7".into(),
                    }),
                },
            },
            StructuredField {
                name: "mood".into(),
                value: LogicalValue::Null,
            },
        ],
    });
    let coverage = live_postgresql_value_type_coverage(&catalog, 80_003, &composite);
    assert_eq!(coverage.values, BTreeSet::from([23, 80_001, 80_003]));
    assert_eq!(coverage.null_only, BTreeSet::from([80_002]));

    let array = Datum::Value(LogicalValue::Array {
        elements: vec![LogicalValue::Null],
    });
    let coverage = live_postgresql_value_type_coverage(&catalog, 80_004, &array);
    assert_eq!(coverage.values, BTreeSet::from([80_004]));
    assert_eq!(coverage.null_only, BTreeSet::from([80_002]));

    let domain_null = live_postgresql_value_type_coverage(&catalog, 80_001, &Datum::Null);
    assert_eq!(domain_null.values, BTreeSet::new());
    assert_eq!(domain_null.null_only, BTreeSet::from([23, 80_001]));
}

fn live_postgresql_non_storable_reason(
    catalog: &postgresql_15::SourceTypeCatalog,
    root_oid: u32,
) -> Option<String> {
    fn visit(
        catalog: &postgresql_15::SourceTypeCatalog,
        oid: u32,
        visiting: &mut BTreeSet<u32>,
    ) -> Option<String> {
        if !visiting.insert(oid) {
            return None;
        }
        let Some(definition) = catalog
            .types
            .iter()
            .find(|definition| definition.oid == oid)
        else {
            visiting.remove(&oid);
            return Some(format!("references missing PostgreSQL type OID {oid}"));
        };
        let reason = match &definition.kind {
            postgresql_15::SourceTypeDefinitionKind::Pseudo => Some(format!(
                "depends on non-storable PostgreSQL pseudotype {}.{}",
                definition.schema, definition.name
            )),
            postgresql_15::SourceTypeDefinitionKind::Domain { base_oid, .. } => {
                visit(catalog, *base_oid, visiting).map(|reason| {
                    format!("domain {}.{} {reason}", definition.schema, definition.name)
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Composite { fields } => {
                fields.iter().find_map(|field| {
                    visit(catalog, field.type_oid, visiting).map(|reason| {
                        format!(
                            "composite {}.{} field {} {reason}",
                            definition.schema, definition.name, field.name
                        )
                    })
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } => {
                let element_is_pseudo = catalog.types.iter().any(|element| {
                    element.oid == *element_oid
                        && matches!(
                            element.kind,
                            postgresql_15::SourceTypeDefinitionKind::Pseudo
                        )
                });
                if element_is_pseudo {
                    Some(format!(
                        "array {}.{} has a non-storable pseudo-type element",
                        definition.schema, definition.name
                    ))
                } else {
                    visit(catalog, *element_oid, visiting)
                        .filter(|reason| reason.contains("non-storable PostgreSQL pseudotype"))
                        .map(|reason| {
                            format!(
                                "array {}.{} element {reason}",
                                definition.schema, definition.name
                            )
                        })
                }
            }
            postgresql_15::SourceTypeDefinitionKind::Range { subtype_oid } => {
                visit(catalog, *subtype_oid, visiting).map(|reason| {
                    format!("range {}.{} {reason}", definition.schema, definition.name)
                })
            }
            postgresql_15::SourceTypeDefinitionKind::MultiRange { range_oid } => {
                visit(catalog, *range_oid, visiting).map(|reason| {
                    format!(
                        "multirange {}.{} {reason}",
                        definition.schema, definition.name
                    )
                })
            }
            postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
                if definition.schema == "pg_catalog"
                    && matches!(
                        definition.name.as_str(),
                        "pg_brin_bloom_summary" | "pg_brin_minmax_multi_summary"
                    ) =>
            {
                Some(format!(
                    "PostgreSQL internal BRIN summary {}.{} has no input/receive routine for row DML",
                    definition.schema, definition.name
                ))
            }
            postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
            | postgresql_15::SourceTypeDefinitionKind::Enum { .. }
            | postgresql_15::SourceTypeDefinitionKind::Extension { .. } => None,
        };
        visiting.remove(&oid);
        reason
    }

    visit(catalog, root_oid, &mut BTreeSet::new())
}

#[test]
fn live_postgresql_non_storable_classifier_follows_pseudo_dependencies_through_arrays() {
    use postgresql_15::{
        SourceTypeCatalog, SourceTypeDefinition, SourceTypeDefinitionKind, SourceTypeField,
    };

    let catalog = SourceTypeCatalog::new([
        SourceTypeDefinition {
            oid: 1,
            schema: "pg_catalog".into(),
            name: "anyarray".into(),
            kind: SourceTypeDefinitionKind::Pseudo,
            collation: None,
            definition_digest: "pseudo".into(),
        },
        SourceTypeDefinition {
            oid: 2,
            schema: "pg_catalog".into(),
            name: "pg_statistic".into(),
            kind: SourceTypeDefinitionKind::Composite {
                fields: vec![SourceTypeField {
                    name: "stavalues1".into(),
                    type_oid: 1,
                    nullable: true,
                }],
            },
            collation: None,
            definition_digest: "composite".into(),
        },
        SourceTypeDefinition {
            oid: 3,
            schema: "pg_catalog".into(),
            name: "_pg_statistic".into(),
            kind: SourceTypeDefinitionKind::Array {
                element_oid: 2,
                delimiter: ',',
            },
            collation: None,
            definition_digest: "array".into(),
        },
        SourceTypeDefinition {
            oid: 4,
            schema: "pg_catalog".into(),
            name: "int4".into(),
            kind: SourceTypeDefinitionKind::Builtin {
                native_type: "integer".into(),
            },
            collation: None,
            definition_digest: "integer".into(),
        },
        SourceTypeDefinition {
            oid: 5,
            schema: "pg_catalog".into(),
            name: "_int4".into(),
            kind: SourceTypeDefinitionKind::Array {
                element_oid: 4,
                delimiter: ',',
            },
            collation: None,
            definition_digest: "integer-array".into(),
        },
    ]);

    let reason = live_postgresql_non_storable_reason(&catalog, 3)
        .expect("array of a pseudo-dependent composite cannot be a user table column");
    assert!(reason.contains("pg_statistic"));
    assert!(reason.contains("anyarray"));
    assert!(
        live_postgresql_non_storable_reason(&catalog, 5).is_none(),
        "arrays of ordinary storable types remain eligible"
    );
}

fn live_postgresql_catalog_class_id(
    definition: &postgresql_15::SourceTypeDefinition,
) -> &'static str {
    match &definition.kind {
        postgresql_15::SourceTypeDefinitionKind::Array { .. } => "postgresql.arrays",
        postgresql_15::SourceTypeDefinitionKind::Enum { .. } => "postgresql.enums",
        postgresql_15::SourceTypeDefinitionKind::Domain { .. } => "postgresql.domains",
        postgresql_15::SourceTypeDefinitionKind::Composite { .. } => "postgresql.composites",
        postgresql_15::SourceTypeDefinitionKind::Range { .. }
        | postgresql_15::SourceTypeDefinitionKind::MultiRange { .. } => "postgresql.ranges",
        postgresql_15::SourceTypeDefinitionKind::Extension { .. } => {
            "postgresql.extensions_and_custom_base_types"
        }
        postgresql_15::SourceTypeDefinitionKind::Builtin { .. }
            if definition.schema == "pg_catalog" =>
        {
            "postgresql.other_defined_catalog_types"
        }
        postgresql_15::SourceTypeDefinitionKind::Builtin { .. } => {
            "postgresql.user_defined_base_types"
        }
        postgresql_15::SourceTypeDefinitionKind::Pseudo => {
            panic!("pseudotypes cannot appear as stored type dependencies")
        }
    }
}

fn live_postgresql_catalog_declaration(
    catalog: &postgresql_15::SourceTypeCatalog,
    oid: u32,
) -> String {
    let definition = catalog
        .types
        .iter()
        .find(|definition| definition.oid == oid)
        .unwrap_or_else(|| panic!("source type OID {oid} is missing from catalog"));
    match &definition.kind {
        postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } => {
            format!(
                "{}[]",
                live_postgresql_catalog_declaration(catalog, *element_oid)
            )
        }
        postgresql_15::SourceTypeDefinitionKind::Builtin { native_type }
            if definition.schema == "pg_catalog" =>
        {
            native_type.clone()
        }
        _ => format!(
            "\"{}\".\"{}\"",
            definition.schema.replace('"', "\"\""),
            definition.name.replace('"', "\"\"")
        ),
    }
}

fn live_postgresql_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn live_postgresql_constraint_literals(expression: &str) -> Vec<String> {
    let characters = expression.chars().collect::<Vec<_>>();
    let mut literals = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        if characters[index] != '\'' {
            index += 1;
            continue;
        }
        index += 1;
        let mut literal = String::new();
        while index < characters.len() {
            match characters[index] {
                '\'' if characters.get(index + 1) == Some(&'\'') => {
                    literal.push('\'');
                    index += 2;
                }
                '\'' => {
                    index += 1;
                    break;
                }
                character => {
                    literal.push(character);
                    index += 1;
                }
            }
        }
        if !literal.is_empty() {
            literals.push(literal);
        }
    }
    literals
}

fn live_postgresql_catalog_value_candidates(
    catalog: &postgresql_15::SourceTypeCatalog,
    oid: u32,
    builtin_values: &BTreeMap<i64, String>,
    visiting: &mut BTreeSet<u32>,
) -> Vec<String> {
    if !visiting.insert(oid) {
        return Vec::new();
    }
    let Some(definition) = catalog
        .types
        .iter()
        .find(|definition| definition.oid == oid)
    else {
        visiting.remove(&oid);
        return Vec::new();
    };
    let declaration = live_postgresql_catalog_declaration(catalog, oid);
    let mut candidates = match &definition.kind {
        postgresql_15::SourceTypeDefinitionKind::Builtin { .. } => builtin_values
            .get(&i64::from(oid))
            .cloned()
            .into_iter()
            .collect(),
        postgresql_15::SourceTypeDefinitionKind::Enum { labels } => labels
            .iter()
            .map(|label| format!("{}::{declaration}", live_postgresql_quote_literal(label)))
            .collect(),
        postgresql_15::SourceTypeDefinitionKind::Domain {
            base_oid,
            constraints,
            ..
        } => {
            let mut values = live_postgresql_catalog_value_candidates(
                catalog,
                *base_oid,
                builtin_values,
                visiting,
            )
            .into_iter()
            .map(|value| format!("({value})::{declaration}"))
            .collect::<Vec<_>>();
            for literal in constraints
                .iter()
                .flat_map(|constraint| live_postgresql_constraint_literals(constraint))
                .chain(
                    [
                        "1",
                        "0",
                        "-1",
                        "true",
                        "false",
                        "YES",
                        "NO",
                        "yes",
                        "no",
                        "sample",
                        "x",
                        "",
                        "2000-01-01",
                        "2000-01-01 00:00:00",
                        "{}",
                        "empty",
                    ]
                    .into_iter()
                    .map(str::to_owned),
                )
            {
                values.push(format!(
                    "{}::{declaration}",
                    live_postgresql_quote_literal(&literal)
                ));
            }
            values
        }
        _ => Vec::new(),
    };
    visiting.remove(&oid);
    let mut seen = BTreeSet::new();
    candidates.retain(|candidate| seen.insert(candidate.clone()));
    candidates
}

fn live_postgresql_type_ids(source_major: u16, field: &LivePostgresqlField) -> Vec<String> {
    let mut type_ids = type_qualification_evidence::source_native_type_ids(
        &format!("postgresql_{source_major}"),
        [field.declaration.clone()],
    );
    type_ids.extend(field.type_ids.iter().cloned());
    type_ids.sort();
    type_ids.dedup();
    assert!(
        !type_ids.is_empty(),
        "no native inventory id for PostgreSQL declaration {}",
        field.declaration
    );
    type_ids
}

#[test]
fn live_postgresql_type_ids_keep_static_and_catalog_bound_identities() {
    let exact_id = format!(
        "dynamic:postgresql.instance.pg_catalog.int8:{}",
        "a".repeat(64)
    );
    let field = LivePostgresqlField {
        name: "value".into(),
        declaration: "bigint".into(),
        expression: "1".into(),
        type_ids: vec![exact_id.clone()],
    };

    let type_ids = live_postgresql_type_ids(15, &field);

    assert!(type_ids.contains(&exact_id));
    assert!(type_ids.contains(&"postgresql.bigint".to_owned()));
}

#[test]
fn live_postgresql_domain_fixture_candidates_decode_constraint_literals() {
    assert_eq!(
        live_postgresql_constraint_literals(
            "CHECK (((VALUE)::text = ANY (ARRAY['YES'::character varying, 'NO'::character varying])))"
        ),
        ["YES", "NO"]
    );
    assert_eq!(
        live_postgresql_constraint_literals("CHECK (VALUE <> 'it''s valid')"),
        ["it's valid"]
    );
    assert_eq!(live_postgresql_quote_literal("it's valid"), "'it''s valid'");
}

fn live_server_build_identity(
    connector: &crate::registry::ConnectorDescriptor,
    metadata: &crate::model::Metadata,
) -> change_event::ServerBuildIdentity {
    if let crate::model::Metadata::Postgresql(metadata) = metadata
        && let Some(build) = &metadata.server_build
    {
        return build.clone();
    }
    let server_version = match metadata {
        crate::model::Metadata::Mysql { server_version, .. } => server_version,
        crate::model::Metadata::Postgresql(metadata) => &metadata.server_version,
    };
    change_event::ServerBuildIdentity::new(
        connector.identity.kind,
        connector.identity.kind,
        connector.identity.version,
        server_version.clone(),
    )
}

fn live_web_postgresql_all_builtin_types(source_major: u16, sink: LiveTypeSink) {
    let mut created_postgis_for_fixture = false;
    if matches!(sink, LiveTypeSink::Mysql(_)) {
        // MySQL InnoDB rows have an 8 KiB inline limit even with DYNAMIC row
        // format. Qualify the complete PostgreSQL catalog in bounded,
        // same-named source/target table groups rather than using one
        // impossible 600+ column fixture.
        const TYPE_FIELDS_PER_TABLE: usize = 180;
        for partition in 0..16 {
            if !live_web_postgresql_all_types_partition(
                source_major,
                sink,
                false,
                Some((partition, TYPE_FIELDS_PER_TABLE)),
                &mut created_postgis_for_fixture,
            ) {
                break;
            }
        }
    } else {
        assert!(live_web_postgresql_all_types_partition(
            source_major,
            sink,
            false,
            None,
            &mut created_postgis_for_fixture,
        ));
    }
}

fn live_web_postgresql_dynamic_types(source_major: u16) {
    cleanup_live_postgresql_dynamic_type_schema(source_major);
    for sink in [
        LiveTypeSink::Mysql("5.7"),
        LiveTypeSink::Mysql("8.0"),
        LiveTypeSink::Mysql("8.4"),
        LiveTypeSink::Postgresql(15),
        LiveTypeSink::Postgresql(16),
        LiveTypeSink::Postgresql(17),
    ] {
        live_web_postgresql_all_types(source_major, sink, true);
    }
    cleanup_live_postgresql_dynamic_type_schema(source_major);
}

fn cleanup_live_postgresql_dynamic_type_schema(source_major: u16) {
    let prefix = if source_major == 15 {
        "PG_CDC".to_owned()
    } else {
        format!("PG_CDC{source_major}")
    };
    let env_value = |suffix: &str| std::env::var(format!("{prefix}_{suffix}")).unwrap();
    let schema = format!("cdc_web_types_pg{source_major}_qualification");
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let options = PgConnectOptions::new()
        .host(&env_value("HOST"))
        .port(env_value("PORT").parse::<u16>().unwrap())
        .database("CDC_test")
        .username(&env_value("ADMIN_USER"))
        .password(&env_value("TEST_PASSWORD"))
        .ssl_mode(PgSslMode::Prefer);
    let mut connection = runtime
        .block_on(PgConnection::connect_with(&options))
        .unwrap();
    let extensions = runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT e.extname
                   FROM pg_catalog.pg_extension e
                   JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
                  WHERE n.nspname=$1 AND e.extname IN ('citext','hstore')",
            )
            .bind(&schema)
            .fetch_all(&mut connection),
        )
        .unwrap();
    for extension in extensions {
        let quoted_extension = format!("\"{}\"", extension.replace('"', "\"\""));
        runtime
            .block_on(connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP EXTENSION IF EXISTS {quoted_extension} CASCADE"
            )))))
            .unwrap();
    }
    runtime
        .block_on(connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"
        )))))
        .unwrap();
}

fn live_drop_fixture_type_extensions(
    runtime: &tokio::runtime::Runtime,
    connection: &mut PgConnection,
    schema: &str,
) {
    let extensions = runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT extension.extname
                   FROM pg_catalog.pg_extension extension
                   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=extension.extnamespace
                  WHERE namespace.nspname=$1
                    AND extension.extname IN ('citext','hstore','postgis')",
            )
            .bind(schema)
            .fetch_all(&mut *connection),
        )
        .unwrap();
    for extension in extensions {
        let quoted_extension = format!("\"{}\"", extension.replace('"', "\"\""));
        runtime
            .block_on(connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP EXTENSION IF EXISTS {quoted_extension} CASCADE"
            )))))
            .unwrap();
    }
}

fn live_web_postgresql_all_types(source_major: u16, sink: LiveTypeSink, dynamic_only: bool) {
    let mut created_postgis_for_fixture = false;
    assert!(live_web_postgresql_all_types_partition(
        source_major,
        sink,
        dynamic_only,
        None,
        &mut created_postgis_for_fixture,
    ));
}

fn live_web_postgresql_all_types_partition(
    source_major: u16,
    sink: LiveTypeSink,
    dynamic_only: bool,
    field_partition: Option<(usize, usize)>,
    created_postgis_for_fixture: &mut bool,
) -> bool {
    let qualification_started_at = Instant::now();
    use crate::catalog::EndpointRole;
    use sha2::{Digest, Sha256};

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("web_pg{source_major}_types_{nonce}");
    let type_schema = if dynamic_only {
        format!("cdc_web_types_pg{source_major}_qualification")
    } else {
        // Reuse one identity across bounded table partitions. The exact type
        // definition digest is schema-bound, so nonce-scoped schemas made
        // every partition invent new catalog identities that had no matching
        // source/sink/Web receipt in the other partitions.
        format!("cdc_web_types_pg{source_major}_builtin_qualification")
    };
    let preserve_type_schema = !dynamic_only && field_partition.is_some();
    let quoted_type_schema = format!("\"{}\"", type_schema.replace('"', "\"\""));
    let source_prefix = if source_major == 15 {
        "PG_CDC".to_owned()
    } else {
        format!("PG_CDC{source_major}")
    };
    let source_env = |suffix: &str| std::env::var(format!("{source_prefix}_{suffix}")).unwrap();
    let source_host = source_env("HOST");
    let source_port = source_env("PORT").parse::<u16>().unwrap();
    let source_password = source_env("TEST_PASSWORD");
    let source_admin = source_env("ADMIN_USER");
    let source_reader = source_env("READER_USER");
    let source_writer = source_env("WRITER_USER");
    let source_publication = format!("cdc_pg{source_major}_demo");
    let pg_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let source_options = PgConnectOptions::new()
        .host(&source_host)
        .port(source_port)
        .database("CDC_test")
        .username(&source_admin)
        .password(&source_password)
        .ssl_mode(PgSslMode::Prefer);
    let mut source_admin_connection = pg_runtime
        .block_on(PgConnection::connect_with(&source_options))
        .unwrap();
    pg_runtime
        .block_on(
            source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "DO $cleanup$ DECLARE stale record; BEGIN
                   FOR stale IN SELECT c.oid::regclass AS relation
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='CDC_test'
                      AND c.relname ~ '^web_pg{source_major}_types_[0-9]+(_mcv_source)?$'
                      AND c.relkind='r'
                   LOOP EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', stale.relation); END LOOP;
                 END $cleanup$"
            )))),
        )
        .unwrap();
    let stale_slots = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT slot_name FROM pg_catalog.pg_replication_slots
                 WHERE database=current_database() AND slot_name LIKE 'cdc_web_%' AND NOT active",
            )
            .fetch_all(&mut source_admin_connection),
        )
        .unwrap();
    for stale_slot in stale_slots {
        pg_runtime
            .block_on(
                sqlx::query("SELECT pg_drop_replication_slot($1)")
                    .bind(stale_slot)
                    .execute(&mut source_admin_connection),
            )
            .unwrap();
    }

    // Recover only fixture-owned schemas left by a prior interrupted run.
    // Their citext/hstore extensions are created by this fixture inside those
    // schemas, so removing them cannot affect shared or application extensions.
    let clean_shared_schema = match field_partition {
        Some((index, _)) if preserve_type_schema => index == 0,
        _ => true,
    };
    let stale_test_schema_pattern = if clean_shared_schema {
        format!("^cdc_web_types_pg{source_major}_([0-9]+|builtin_qualification)$")
    } else {
        format!("^cdc_web_types_pg{source_major}_[0-9]+$")
    };
    let stale_test_extensions = pg_runtime
        .block_on(
            sqlx::query_as::<_, (String, String)>(
                "SELECT e.extname, n.nspname
                   FROM pg_catalog.pg_extension e
                   JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
                  WHERE n.nspname ~ $1 AND e.extname IN ('citext','hstore')",
            )
            .bind(&stale_test_schema_pattern)
            .fetch_all(&mut source_admin_connection),
        )
        .unwrap();
    for (extension, _) in stale_test_extensions {
        let quoted_extension = format!("\"{}\"", extension.replace('"', "\"\""));
        pg_runtime
            .block_on(
                source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP EXTENSION IF EXISTS {quoted_extension} CASCADE"
                )))),
            )
            .unwrap();
    }
    let stale_test_schemas = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname ~ $1",
            )
            .bind(&stale_test_schema_pattern)
            .fetch_all(&mut source_admin_connection),
        )
        .unwrap();
    for stale_schema in stale_test_schemas {
        let quoted_schema = format!("\"{}\"", stale_schema.replace('"', "\"\""));
        pg_runtime
            .block_on(
                source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"
                )))),
            )
            .unwrap();
    }

    pg_runtime.block_on(async {
        source_admin_connection
            .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "CREATE SCHEMA IF NOT EXISTS {quoted_type_schema};
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_mood') THEN
                     CREATE TYPE {quoted_type_schema}.web_mood AS ENUM ('calm','ready');
                   END IF;
                 END $qual$;
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_int4_base') THEN
                     EXECUTE 'CREATE TYPE {quoted_type_schema}.web_int4_base';
                     EXECUTE 'CREATE FUNCTION {quoted_type_schema}.web_int4_base_in(cstring) RETURNS {quoted_type_schema}.web_int4_base AS ''int4in'' LANGUAGE internal IMMUTABLE STRICT';
                     EXECUTE 'CREATE FUNCTION {quoted_type_schema}.web_int4_base_out({quoted_type_schema}.web_int4_base) RETURNS cstring AS ''int4out'' LANGUAGE internal IMMUTABLE STRICT';
                     EXECUTE 'CREATE FUNCTION {quoted_type_schema}.web_int4_base_recv(internal) RETURNS {quoted_type_schema}.web_int4_base AS ''int4recv'' LANGUAGE internal IMMUTABLE STRICT';
                     EXECUTE 'CREATE FUNCTION {quoted_type_schema}.web_int4_base_send({quoted_type_schema}.web_int4_base) RETURNS bytea AS ''int4send'' LANGUAGE internal IMMUTABLE STRICT';
                     EXECUTE 'CREATE TYPE {quoted_type_schema}.web_int4_base (INPUT = {quoted_type_schema}.web_int4_base_in, OUTPUT = {quoted_type_schema}.web_int4_base_out, RECEIVE = {quoted_type_schema}.web_int4_base_recv, SEND = {quoted_type_schema}.web_int4_base_send, INTERNALLENGTH = 4, PASSEDBYVALUE, ALIGNMENT = int4)';
                   END IF;
                 END $qual$;
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_positive') THEN
                     CREATE DOMAIN {quoted_type_schema}.web_positive AS integer CHECK (VALUE > 0);
                   END IF;
                 END $qual$;
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_address') THEN
                     CREATE TYPE {quoted_type_schema}.web_address AS (street text, unit integer, note text);
                   END IF;
                 END $qual$;
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_person') THEN
                     CREATE TYPE {quoted_type_schema}.web_person AS (name text, address {quoted_type_schema}.web_address, score {quoted_type_schema}.web_positive);
                   END IF;
                 END $qual$;
                 DO $qual$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='{type_schema}' AND t.typname='web_intspan') THEN
                     CREATE TYPE {quoted_type_schema}.web_intspan AS RANGE (SUBTYPE = integer);
                   END IF;
                 END $qual$;"
            ))))
            .await
            .unwrap();
    });
    let mcv_source_table = format!("{table}_mcv_source");
    let mcv_statistics_name = format!("{table}_mcv_stats");
    let mcv_expression = if dynamic_only {
        pg_runtime
            .block_on(async {
                source_admin_connection
                    .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "CREATE SCHEMA IF NOT EXISTS \"CDC_test\";
                         CREATE TABLE \"CDC_test\".\"{mcv_source_table}\" (correlation_key integer NOT NULL, bucket text NOT NULL);
                         INSERT INTO \"CDC_test\".\"{mcv_source_table}\" (correlation_key,bucket)
                           SELECT CASE WHEN sample.i % 2 = 0 THEN 1 ELSE 2 END,
                                  CASE WHEN sample.i % 2 = 0 THEN 'even' ELSE 'odd' END
                             FROM generate_series(1,1000) AS sample(i);
                         CREATE STATISTICS \"CDC_test\".\"{mcv_statistics_name}\" (mcv)
                           ON correlation_key,bucket FROM \"CDC_test\".\"{mcv_source_table}\";
                         ANALYZE \"CDC_test\".\"{mcv_source_table}\";"
                    ))))
                    .await
            })
            .unwrap();
        let has_mcv_value = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (
                       SELECT 1
                         FROM pg_catalog.pg_statistic_ext_data data
                         JOIN pg_catalog.pg_statistic_ext stats ON stats.oid=data.stxoid
                         JOIN pg_catalog.pg_namespace namespace ON namespace.oid=stats.stxnamespace
                        WHERE namespace.nspname='CDC_test'
                          AND stats.stxname=$1
                          AND data.stxdmcv IS NOT NULL
                     )",
                )
                .bind(&mcv_statistics_name)
                .fetch_one(&mut source_admin_connection),
            )
            .unwrap();
        assert!(
            has_mcv_value,
            "PostgreSQL {source_major} did not produce pg_mcv_list data for the live fixture"
        );
        Some(format!(
            "(SELECT data.stxdmcv
                FROM pg_catalog.pg_statistic_ext_data data
                JOIN pg_catalog.pg_statistic_ext stats ON stats.oid=data.stxoid
                JOIN pg_catalog.pg_namespace namespace ON namespace.oid=stats.stxnamespace
               WHERE namespace.nspname='CDC_test'
                 AND stats.stxname='{mcv_statistics_name}')"
        ))
    } else {
        None
    };
    let mut fields = BUILTINS
        .iter()
        .map(|case| LivePostgresqlField {
            name: case.name.to_owned(),
            declaration: case.declaration.to_owned(),
            expression: case.expression.to_owned(),
            type_ids: type_qualification_evidence::source_native_type_ids(
                &format!("postgresql_{source_major}"),
                [case.declaration.to_owned()],
            ),
        })
        .collect::<Vec<_>>();
    let mut builtin_element_expressions = BTreeMap::<i64, String>::new();
    for case in BUILTINS {
        let typed_expression = format!("({})::{}", case.expression, case.declaration);
        let sql = format!("SELECT pg_catalog.pg_typeof({typed_expression})::oid::bigint");
        match pg_runtime.block_on(
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
                .fetch_one(&mut source_admin_connection),
        ) {
            Ok(oid) => {
                builtin_element_expressions
                    .entry(oid)
                    .or_insert(typed_expression);
            }
            Err(sqlx::Error::Database(_)) => {}
            Err(error) => panic!("could not resolve PostgreSQL fixture expression: {error}"),
        }
    }
    let catalog_array_rows = pg_runtime
        .block_on(
            sqlx::query(
                "SELECT pg_catalog.format_type(array_type.oid, -1) AS declaration,
                        element_type.oid::bigint AS element_oid,
                        pg_catalog.format_type(element_type.oid, NULL) AS element_declaration,
                        element_type.typname AS element_name
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
            .fetch_all(&mut source_admin_connection),
        )
        .unwrap();
    for (index, row) in catalog_array_rows.iter().enumerate() {
        let declaration: String = row.try_get("declaration").unwrap();
        let element_oid: i64 = row.try_get("element_oid").unwrap();
        let element_declaration: String = row.try_get("element_declaration").unwrap();
        let element_name: String = row.try_get("element_name").unwrap();
        let expression = if let Some(element_expression) =
            builtin_element_expressions.get(&element_oid)
        {
            format!("ARRAY[({element_expression})::{element_declaration}]::{declaration}")
        } else if element_name == "gtsvector" {
            // PostgreSQL's internal GiST text-search vector has no public
            // input function. Its array type is storable, so qualify framing
            // and NULL-element presence without inventing a value.
            format!("ARRAY[NULL]::{declaration}")
        } else {
            panic!(
                "PostgreSQL {source_major} catalog array {declaration} has no live sample for element {element_declaration} ({element_name}, OID {element_oid})"
            );
        };
        fields.push(LivePostgresqlField {
            name: format!("array_{index:03}"),
            expression,
            declaration,
            type_ids: vec!["dynamic:postgresql.arrays".into()],
        });
    }
    let dynamic_type_cases = [
        (
            "dynamic_enum",
            format!("{quoted_type_schema}.web_mood"),
            format!("'ready'::{quoted_type_schema}.web_mood"),
            "postgresql.enums",
        ),
        (
            "dynamic_domain",
            format!("{quoted_type_schema}.web_positive"),
            format!("27::{quoted_type_schema}.web_positive"),
            "postgresql.domains",
        ),
        (
            "dynamic_user_base",
            format!("{quoted_type_schema}.web_int4_base"),
            format!("'27'::{quoted_type_schema}.web_int4_base"),
            "postgresql.user_defined_base_types",
        ),
        (
            "dynamic_composite",
            format!("{quoted_type_schema}.web_person"),
            format!(
                "ROW('Ada', ROW('Main Street', 7, NULL)::{quoted_type_schema}.web_address, 27)::{quoted_type_schema}.web_person"
            ),
            "postgresql.composites",
        ),
        (
            "dynamic_address",
            format!("{quoted_type_schema}.web_address"),
            format!("ROW('Main Street', 7, 'Unit 2')::{quoted_type_schema}.web_address"),
            "postgresql.composites",
        ),
        (
            "dynamic_composite_array",
            format!("{quoted_type_schema}.web_person[]"),
            format!(
                "ARRAY[ROW('Grace', ROW('Compiler Road', 3, 'Apt 2')::{quoted_type_schema}.web_address, 31)::{quoted_type_schema}.web_person, NULL]::{quoted_type_schema}.web_person[]"
            ),
            "postgresql.arrays",
        ),
        (
            "dynamic_range",
            format!("{quoted_type_schema}.web_intspan"),
            format!("'[1,8)'::{quoted_type_schema}.web_intspan"),
            "postgresql.ranges",
        ),
        (
            "dynamic_multirange",
            format!("{quoted_type_schema}.web_intspan_multirange"),
            format!("'{{[1,3),[5,8)}}'::{quoted_type_schema}.web_intspan_multirange"),
            "postgresql.ranges",
        ),
    ];
    for (name, declaration, expression, class) in dynamic_type_cases {
        fields.push(LivePostgresqlField {
            name: name.into(),
            declaration,
            expression,
            type_ids: vec![format!("dynamic:{class}")],
        });
    }
    if let Some(expression) = mcv_expression {
        fields.push(LivePostgresqlField {
            name: "dynamic_pg_mcv_list".into(),
            declaration: "pg_catalog.pg_mcv_list".into(),
            expression,
            type_ids: vec!["dynamic:postgresql.other_defined_catalog_types".into()],
        });
    }
    fields.push(LivePostgresqlField {
        name: "catalog_internal".into(),
        declaration: "pg_catalog.pg_node_tree".into(),
        expression: "(SELECT ev_action FROM pg_catalog.pg_rewrite ORDER BY oid LIMIT 1)".into(),
        type_ids: vec!["dynamic:postgresql.other_defined_catalog_types".into()],
    });

    let mut hstore_schema = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='hstore'",
            )
            .fetch_optional(&mut source_admin_connection),
        )
        .unwrap();
    if hstore_schema.is_none() {
        let available = pg_runtime
            .block_on(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name='hstore')",
            ).fetch_one(&mut source_admin_connection))
            .unwrap();
        if available {
            pg_runtime
                .block_on(async {
                    source_admin_connection
                        .execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                            "CREATE EXTENSION hstore SCHEMA {quoted_type_schema}"
                        ))))
                        .await
                })
                .unwrap();
            hstore_schema = Some(type_schema.clone());
        }
    }
    if let Some(extension_schema) = &hstore_schema {
        let quoted_extension_schema = format!("\"{}\"", extension_schema.replace('"', "\"\""));
        fields.push(LivePostgresqlField {
            name: "dynamic_hstore".into(),
            declaration: format!("{quoted_extension_schema}.hstore"),
            expression: format!(
                "'\"author\"=>\"Ada\", \"nullable\"=>NULL'::{quoted_extension_schema}.hstore"
            ),
            type_ids: vec![
                "dynamic:postgresql.extensions_and_custom_base_types".into(),
                "dynamic:postgresql.user_defined_base_types".into(),
            ],
        });
    }
    let mut postgis_schema = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='postgis'",
            )
            .fetch_optional(&mut source_admin_connection),
        )
        .unwrap();
    if postgis_schema.is_none() && !dynamic_only {
        let available = pg_runtime
            .block_on(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name='postgis')",
            ).fetch_one(&mut source_admin_connection))
            .unwrap();
        if available {
            pg_runtime
                .block_on(
                    sqlx::query("CREATE EXTENSION postgis").execute(&mut source_admin_connection),
                )
                .unwrap();
            *created_postgis_for_fixture = true;
            postgis_schema = pg_runtime
                .block_on(
                    sqlx::query_scalar::<_, String>(
                        "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='postgis'",
                    )
                    .fetch_optional(&mut source_admin_connection),
                )
                .unwrap();
        }
    }
    if let Some(extension_schema) = &postgis_schema {
        let quoted_extension_schema = format!("\"{}\"", extension_schema.replace('"', "\"\""));
        fields.push(LivePostgresqlField {
            name: "dynamic_postgis_geometry".into(),
            declaration: format!("{quoted_extension_schema}.geometry"),
            expression: format!(
                "{quoted_extension_schema}.st_geomfromewkt('SRID=4326;POINT(1 2)')"
            ),
            type_ids: vec!["dynamic:postgresql.extensions_and_custom_base_types".into()],
        });
    }
    let mut citext_schema = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, String>(
                "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='citext'",
            )
            .fetch_optional(&mut source_admin_connection),
        )
        .unwrap();
    if citext_schema.is_none() {
        let available = pg_runtime
            .block_on(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name='citext')",
            ).fetch_one(&mut source_admin_connection))
            .unwrap();
        if available {
            pg_runtime
                .block_on(
                    source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                        "CREATE EXTENSION citext SCHEMA {quoted_type_schema}"
                    )))),
                )
                .unwrap();
            citext_schema = Some(type_schema.clone());
        }
    }
    if let Some(extension_schema) = &citext_schema {
        let quoted_extension_schema = format!("\"{}\"", extension_schema.replace('"', "\"\""));
        fields.push(LivePostgresqlField {
            name: "dynamic_extension_opaque".into(),
            declaration: format!("{quoted_extension_schema}.citext"),
            expression: format!("'MixedCase@Example.invalid'::{quoted_extension_schema}.citext"),
            type_ids: vec![
                "dynamic:postgresql.extensions_and_custom_base_types".into(),
                "dynamic:postgresql.user_defined_base_types".into(),
            ],
        });
    }
    let extension_schema_grants = hstore_schema
        .iter()
        .chain(postgis_schema.iter())
        .chain(citext_schema.iter())
        .map(|schema| {
            format!(
                "GRANT USAGE ON SCHEMA \"{}\" TO {source_reader};",
                schema.replace('"', "\"\"")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    fields.push(LivePostgresqlField {
        name: "nullable_marker".into(),
        declaration: "text".into(),
        expression: "NULL".into(),
        type_ids: vec!["postgresql.text".to_owned()],
    });

    // PostgreSQL creates a companion array type for every storable type.
    // Exercise arrays of the custom and extension types above as separate
    // live column declarations so they receive their own catalog-bound
    // Source/Sink/Web qualification receipts.
    let mut existing_type_oids = BTreeMap::<i64, ()>::new();
    for field in &fields {
        let sql = format!(
            "SELECT pg_catalog.pg_typeof(({})::{})::oid::bigint",
            field.expression, field.declaration
        );
        let oid = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
                    .fetch_one(&mut source_admin_connection),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "could not resolve live sample for {} {}: {error}",
                    field.name, field.declaration
                )
            });
        existing_type_oids.insert(oid, ());
    }
    // Catalog-discovered composites that recursively contain only storable
    // types receive their own protocol-capture and Web receipt. Keep catalog
    // row types that depend on pseudotypes in the mapping inventory, but do
    // not try to use them as user-table columns: PostgreSQL rejects those DDLs.
    let composite_catalog = pg_runtime
        .block_on(postgresql_15::source_type_catalog(
            &mut source_admin_connection,
        ))
        .unwrap();
    let mut composite_definitions = composite_catalog
        .types
        .iter()
        .filter_map(|definition| {
            let postgresql_15::SourceTypeDefinitionKind::Composite { fields } = &definition.kind
            else {
                return None;
            };
            if live_postgresql_non_storable_reason(&composite_catalog, definition.oid).is_some() {
                return None;
            }
            Some((definition.oid, fields.clone()))
        })
        .collect::<Vec<_>>();
    composite_definitions.sort_by_key(|(oid, _)| *oid);
    for (oid, attributes) in composite_definitions {
        if existing_type_oids.contains_key(&(i64::from(oid))) {
            continue;
        }
        let declaration = live_postgresql_catalog_declaration(&composite_catalog, oid);
        let values = attributes
            .iter()
            .map(|attribute| {
                format!(
                    "NULL::{}",
                    live_postgresql_catalog_declaration(&composite_catalog, attribute.type_oid)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let expression = format!("ROW({values})::{declaration}");
        let safe_name = format!("catalog_composite_{oid}");
        fields.push(LivePostgresqlField {
            name: safe_name,
            declaration,
            expression,
            type_ids: vec!["dynamic:postgresql.composites".into()],
        });
        existing_type_oids.insert(i64::from(oid), ());
    }
    let catalog_enum_domain_types = composite_catalog
        .types
        .iter()
        .filter(|definition| {
            matches!(
                &definition.kind,
                postgresql_15::SourceTypeDefinitionKind::Enum { .. }
                    | postgresql_15::SourceTypeDefinitionKind::Domain { .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    for definition in catalog_enum_domain_types {
        if existing_type_oids.contains_key(&i64::from(definition.oid))
            || live_postgresql_non_storable_reason(&composite_catalog, definition.oid).is_some()
        {
            continue;
        }
        let declaration = live_postgresql_catalog_declaration(&composite_catalog, definition.oid);
        let candidates = live_postgresql_catalog_value_candidates(
            &composite_catalog,
            definition.oid,
            &builtin_element_expressions,
            &mut BTreeSet::new(),
        );
        let mut selected_expression = None;
        for candidate in candidates {
            let sql = format!("SELECT ({candidate})::text");
            if pg_runtime
                .block_on(
                    sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql))
                        .fetch_one(&mut source_admin_connection),
                )
                .is_ok()
            {
                selected_expression = Some(candidate);
                break;
            }
        }
        let expression = selected_expression.unwrap_or_else(|| {
            panic!(
                "PostgreSQL {source_major} has no non-NULL live fixture value for catalog type {}.{} (OID {}); refusing to count a dependency-only occurrence as type qualification",
                definition.schema, definition.name, definition.oid
            )
        });
        let type_id = live_postgresql_catalog_class_id(&definition);
        fields.push(LivePostgresqlField {
            name: format!("catalog_type_{}", definition.oid),
            declaration,
            expression,
            type_ids: vec![format!("dynamic:{type_id}")],
        });
        existing_type_oids.insert(i64::from(definition.oid), ());
    }
    let statistics_seed_table = format!("cdc_type_stats_{nonce}");
    let statistics_seed_name = format!("cdc_type_stats_object_{nonce}");
    pg_runtime
        .block_on(async {
            source_admin_connection
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "CREATE TEMP TABLE \"{statistics_seed_table}\" (a integer NOT NULL,b integer NOT NULL);
                     INSERT INTO \"{statistics_seed_table}\" (a,b)
                     SELECT value, value % 17 FROM pg_catalog.generate_series(1,1000) AS series(value);
                     CREATE STATISTICS \"{statistics_seed_name}\" (ndistinct,dependencies)
                     ON a,b FROM \"{statistics_seed_table}\";
                     ANALYZE \"{statistics_seed_table}\";"
                ))))
                .await
                .unwrap();
        });
    let statistics_are_ready = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, bool>(
                "SELECT n_distinct IS NOT NULL AND dependencies IS NOT NULL
                   FROM pg_catalog.pg_stats_ext
                  WHERE statistics_name=$1 AND tablename=$2",
            )
            .bind(&statistics_seed_name)
            .bind(&statistics_seed_table)
            .fetch_optional(&mut source_admin_connection),
        )
        .unwrap()
        .unwrap_or(false);
    assert!(
        statistics_are_ready,
        "PostgreSQL {source_major} did not produce both extended statistics fixture values"
    );
    for (type_name, view_column) in [
        ("pg_ndistinct", "n_distinct"),
        ("pg_dependencies", "dependencies"),
    ] {
        let Some(definition) = composite_catalog
            .types
            .iter()
            .find(|definition| definition.schema == "pg_catalog" && definition.name == type_name)
        else {
            continue;
        };
        if existing_type_oids.contains_key(&i64::from(definition.oid)) {
            continue;
        }
        let declaration = live_postgresql_catalog_declaration(&composite_catalog, definition.oid);
        let expression = format!(
            "(SELECT {view_column} FROM pg_catalog.pg_stats_ext WHERE statistics_name={} AND tablename={})",
            live_postgresql_quote_literal(&statistics_seed_name),
            live_postgresql_quote_literal(&statistics_seed_table)
        );
        fields.push(LivePostgresqlField {
            name: format!("catalog_type_{}", definition.oid),
            declaration,
            expression,
            type_ids: vec![format!(
                "dynamic:{}",
                live_postgresql_catalog_class_id(definition)
            )],
        });
        existing_type_oids.insert(i64::from(definition.oid), ());
    }
    if let Some(definition) = composite_catalog
        .types
        .iter()
        .find(|definition| definition.schema == "pg_catalog" && definition.name == "gtsvector")
        && let std::collections::btree_map::Entry::Vacant(entry) =
            existing_type_oids.entry(i64::from(definition.oid))
    {
        let binary_formats_unavailable = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, bool>(
                    "SELECT t.typreceive=0 AND t.typsend=0
                       FROM pg_catalog.pg_type t WHERE t.oid::bigint=$1",
                )
                .bind(i64::from(definition.oid))
                .fetch_one(&mut source_admin_connection),
            )
            .unwrap();
        let text_input_rejects_values = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, String>(
                    "SELECT 'qualification-probe'::pg_catalog.gtsvector::text",
                )
                .fetch_one(&mut source_admin_connection),
            )
            .is_err();
        assert!(
            binary_formats_unavailable && text_input_rejects_values,
            "PostgreSQL {source_major} gtsvector became non-NULL writable through a public SQL format; the fixture's NULL-only boundary needs review"
        );
        let declaration = live_postgresql_catalog_declaration(&composite_catalog, definition.oid);
        fields.push(LivePostgresqlField {
            name: format!("catalog_type_{}", definition.oid),
            expression: format!("NULL::{declaration}"),
            declaration,
            type_ids: vec![format!(
                "dynamic:{}",
                live_postgresql_catalog_class_id(definition)
            )],
        });
        entry.insert(());
    }
    let custom_array_candidates = fields
        .iter()
        .filter(|field| {
            (field.name.starts_with("dynamic_")
                || field.name.starts_with("catalog_composite_")
                || field.name.starts_with("catalog_type_")
                || field.name == "catalog_internal")
                && !field.name.ends_with("_array")
                && !field.declaration.ends_with("[]")
        })
        .filter_map(|field| {
            let sql = format!(
                "SELECT t.typarray::bigint
                   FROM pg_catalog.pg_type t
                  WHERE t.oid=pg_catalog.pg_typeof(({})::{})::oid
                    AND t.typarray <> 0",
                field.expression, field.declaration
            );
            let array = pg_runtime
                .block_on(
                    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
                        .fetch_optional(&mut source_admin_connection),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "could not inspect array companion for {}: {error}",
                        field.name
                    )
                });
            array.map(|oid| {
                let declaration = live_postgresql_catalog_declaration(
                    &composite_catalog,
                    u32::try_from(oid).expect("array type OID fits u32"),
                );
                (
                    field.name.clone(),
                    field.declaration.clone(),
                    field.expression.clone(),
                    oid,
                    declaration,
                )
            })
        })
        .filter(|(_, _, _, oid, _)| !existing_type_oids.contains_key(oid))
        .collect::<Vec<_>>();
    for (index, (root_name, element_declaration, element_expression, oid, declaration)) in
        custom_array_candidates.into_iter().enumerate()
    {
        existing_type_oids.insert(oid, ());
        fields.push(LivePostgresqlField {
            name: if root_name.starts_with("catalog_composite_") {
                format!("{root_name}_array")
            } else {
                format!("dynamic_array_{index:03}")
            },
            declaration: declaration.clone(),
            expression: format!(
                "ARRAY[({})::{}]::{declaration}",
                element_expression, element_declaration
            ),
            type_ids: vec!["dynamic:postgresql.arrays".into()],
        });
    }

    if dynamic_only {
        fields.retain(|field| {
            field.name == "nullable_marker"
                || field.name == "catalog_internal"
                || field.name.starts_with("dynamic_")
        });
        assert!(
            fields
                .iter()
                .any(|field| field.name == "dynamic_extension_opaque"),
            "dynamic PostgreSQL fixtures must exercise an extension base type without a semantic codec"
        );
        assert!(
            fields.iter().any(|field| {
                field
                    .type_ids
                    .iter()
                    .any(|type_id| type_id == "dynamic:postgresql.enums")
            }),
            "dynamic PostgreSQL fixture must include an enum field"
        );
    }

    if let Some((partition_index, partition_size)) = field_partition {
        let marker = fields
            .iter()
            .find(|field| field.name == "nullable_marker")
            .cloned()
            .expect("all-type qualification keeps its NULL marker in every partition");
        let selected = fields
            .iter()
            .filter(|field| field.name != "nullable_marker")
            .skip(partition_index * partition_size)
            .take(partition_size)
            .cloned()
            .collect::<Vec<_>>();
        if selected.is_empty() {
            if dynamic_only {
                pg_runtime
                    .block_on(
                        sqlx::query(sqlx::AssertSqlSafe(format!(
                            "DROP TABLE IF EXISTS \"CDC_test\".\"{mcv_source_table}\" CASCADE"
                        )))
                        .execute(&mut source_admin_connection),
                    )
                    .unwrap();
            }
            if !dynamic_only {
                live_drop_fixture_type_extensions(
                    &pg_runtime,
                    &mut source_admin_connection,
                    &type_schema,
                );
            }
            if *created_postgis_for_fixture {
                pg_runtime
                    .block_on(
                        sqlx::query("DROP EXTENSION postgis CASCADE")
                            .execute(&mut source_admin_connection),
                    )
                    .unwrap();
                *created_postgis_for_fixture = false;
            }
            if !dynamic_only {
                pg_runtime
                    .block_on(
                        sqlx::query(sqlx::AssertSqlSafe(format!(
                            "DROP SCHEMA IF EXISTS {quoted_type_schema} CASCADE"
                        )))
                        .execute(&mut source_admin_connection),
                    )
                    .unwrap();
            }
            return false;
        }
        fields = std::iter::once(marker).chain(selected).collect();
    }

    let definitions = fields
        .iter()
        .map(|field| format!("\"{}\" {}", field.name, field.declaration))
        .collect::<Vec<_>>()
        .join(",");
    pg_runtime
        .block_on(async {
            source_admin_connection
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA IF NOT EXISTS \"CDC_test\";
                     CREATE TABLE \"CDC_test\".\"{table}\" (\"id\" bigint PRIMARY KEY,{definitions});
                     ALTER TABLE \"CDC_test\".\"{table}\" REPLICA IDENTITY FULL;
                     GRANT USAGE ON SCHEMA \"CDC_test\" TO {source_reader};
                     GRANT USAGE ON SCHEMA {quoted_type_schema} TO {source_reader};
                     {extension_schema_grants}
                     GRANT SELECT ON \"CDC_test\".\"{table}\" TO {source_reader}"
                ))))
                .await
                .unwrap();
        });
    let source_fixture_exists = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, bool>(
                "SELECT pg_catalog.to_regclass(format('%I.%I', 'CDC_test', $1)) IS NOT NULL",
            )
            .bind(&table)
            .fetch_one(&mut source_admin_connection),
        )
        .unwrap();
    assert!(
        source_fixture_exists,
        "PostgreSQL {source_major} type qualification fixture {table} was not created"
    );
    let publication_all_tables: Option<bool> = pg_runtime
        .block_on(
            sqlx::query_scalar("SELECT puballtables FROM pg_publication WHERE pubname=$1")
                .bind(&source_publication)
                .fetch_optional(&mut source_admin_connection),
        )
        .unwrap();
    let created_publication = publication_all_tables.is_none();
    let added_publication_member = match publication_all_tables {
        None => {
            pg_runtime
                .block_on(
                    source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                        "CREATE PUBLICATION {source_publication} FOR TABLE \"CDC_test\".\"{table}\""
                    )))),
                )
                .unwrap();
            false
        }
        Some(false) => {
            let member: bool = pg_runtime
                .block_on(
                    sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_publication_tables WHERE pubname=$1 AND schemaname='CDC_test' AND tablename=$2)",
                    )
                    .bind(&source_publication)
                    .bind(&table)
                    .fetch_one(&mut source_admin_connection),
                )
                .unwrap();
            if !member {
                pg_runtime
                    .block_on(source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(
                        format!(
                            "ALTER PUBLICATION {source_publication} ADD TABLE \"CDC_test\".\"{table}\""
                        ),
                    ))))
                    .unwrap();
            }
            !member
        }
        Some(true) => false,
    };
    let source_catalog = pg_runtime
        .block_on(postgresql_15::source_type_catalog(
            &mut source_admin_connection,
        ))
        .unwrap();
    let catalog_columns = pg_runtime
        .block_on(
            sqlx::query_as::<_, (String, i64)>(
                "SELECT a.attname, a.atttypid::bigint
                   FROM pg_catalog.pg_attribute a
                   JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                  WHERE n.nspname='CDC_test' AND c.relname=$1
                    AND a.attnum > 0 AND NOT a.attisdropped",
            )
            .bind(&table)
            .fetch_all(&mut source_admin_connection),
        )
        .unwrap()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut catalog_type_roster = Vec::new();
    let mut catalog_type_ids_by_oid = BTreeMap::<u32, String>::new();
    let mut catalog_raw_type_oids = BTreeSet::<u32>::new();
    for field in &mut fields {
        let Some(oid) = catalog_columns.get(&field.name).copied() else {
            panic!(
                "source catalog did not return fixture column {}",
                field.name
            );
        };
        let oid = u32::try_from(oid).unwrap();
        for dependency_oid in live_postgresql_type_dependency_oids(&source_catalog, oid) {
            let dependency = source_catalog
                .types
                .iter()
                .find(|definition| definition.oid == dependency_oid)
                .expect("dependency exists in the catalog snapshot");
            // Type qualification identities must use the same canonical
            // declaration for direct fields and recursive dependencies;
            // otherwise a quoted and unquoted spelling creates conflicting
            // mapping receipts for the same catalog definition.
            let native_declaration =
                live_postgresql_catalog_declaration(&source_catalog, dependency_oid);
            let mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
                &source_major.to_string(),
                &native_declaration,
                &source_catalog,
            )
            .unwrap_or_else(|error| {
                panic!("source mapping missing for {native_declaration}: {error}")
            });
            let type_id = live_postgresql_catalog_type_id(dependency);
            catalog_type_ids_by_oid.insert(dependency_oid, type_id.clone());
            if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                catalog_raw_type_oids.insert(dependency_oid);
            }
            if field.name != "nullable_marker"
                && dependency_oid == oid
                && !field.type_ids.contains(&type_id)
            {
                field.type_ids.push(type_id.clone());
            }
            catalog_type_roster.push(serde_json::json!({
                    "type_id": type_id,
                    "catalog_class_id": live_postgresql_catalog_class_id(dependency),
                    "schema": dependency.schema,
                    "name": dependency.name,
                    "type_oid": dependency_oid,
                    "native_declaration": native_declaration,
                    "definition_digest": dependency.definition_digest,
                    "mapping_id": mapping.mapping_id,
                    "mapping_evidence_digest": mapping.evidence_digest.unwrap_or_default(),
                    "logical_type_digest": change_event::stable_digest(&mapping.logical_type),
                    "representation_mode": if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                        "SOURCE_REPRESENTATION"
                    } else {
                        "SEMANTIC_CODEC"
                    }
                }));
        }
    }
    let target_catalog_id = match sink {
        LiveTypeSink::Mysql("5.7") => "mysql_5_7".to_owned(),
        LiveTypeSink::Mysql("8.0") => "mysql_8_0".to_owned(),
        LiveTypeSink::Mysql("8.4") => "mysql_8_4".to_owned(),
        LiveTypeSink::Mysql(version) => panic!("unsupported MySQL test target {version}"),
        LiveTypeSink::Postgresql(major) => format!("postgresql_{major}"),
    };
    type_qualification_evidence::record_catalog_type_roster_evidence(
        &format!("postgresql_{source_major}"),
        &format!(
            "web_ui.postgresql_{source_major}_all_builtin_types_to_{target_catalog_id}_catalog_type_roster"
        ),
        catalog_type_roster,
    )
    .unwrap();
    let mut source_mappings = BTreeMap::new();
    for field in &fields {
        source_mappings.insert(
            field.name.clone(),
            postgresql_15::source_type_mapping_with_catalog_for_version(
                &source_major.to_string(),
                &field.declaration,
                &source_catalog,
            )
            .unwrap_or_else(|error| {
                panic!(
                    "PostgreSQL {source_major} fixture type {} did not map: {error}",
                    field.declaration
                )
            }),
        );
    }
    if dynamic_only {
        let mapping = &source_mappings["dynamic_extension_opaque"];
        assert!(
            matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }),
            "the extension type without a dedicated codec must be captured as an explicit source representation"
        );
        assert!(
            mapping.source_representation_evidence.is_some(),
            "the source representation plan must bind the extension type definition"
        );
    }

    let isolated_same_version_postgresql_target =
        matches!(sink, LiveTypeSink::Postgresql(major) if major == source_major);
    let (
        sink_id,
        sink_connector_id,
        sink_database,
        sink_host,
        sink_port,
        sink_reader,
        sink_reader_password,
        sink_writer,
        sink_writer_password,
        sink_admin,
    ) = match sink {
        LiveTypeSink::Mysql(version) => {
            let host = std::env::var("CDC_MYSQL_HOST").unwrap();
            let port_key = match version {
                "5.7" => "CDC_MYSQL57_PORT",
                "8.0" => "CDC_MYSQL80_PORT",
                "8.4" => "CDC_MYSQL84_PORT",
                _ => panic!("unsupported MySQL test target"),
            };
            (
                format!("mysql-{version}"),
                format!("mysql_{}", version.replace('.', "_")),
                String::new(),
                host,
                std::env::var(port_key).unwrap().parse::<u16>().unwrap(),
                std::env::var("CDC_MYSQL_READER_USER").unwrap(),
                std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap(),
                std::env::var("CDC_MYSQL_WRITER_USER").unwrap(),
                std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap(),
                std::env::var("CDC_MYSQL_WRITER_USER").unwrap(),
            )
        }
        LiveTypeSink::Postgresql(major) => {
            let prefix = if major == 15 {
                "PG_CDC".to_owned()
            } else {
                format!("PG_CDC{major}")
            };
            let env_value = |suffix: &str| std::env::var(format!("{prefix}_{suffix}")).unwrap();
            let database = if isolated_same_version_postgresql_target {
                let database = format!("cdc_web_pg{major}_sink_{nonce}");
                let stale_databases = pg_runtime
                    .block_on(
                        sqlx::query_scalar::<_, String>(
                            "SELECT datname FROM pg_catalog.pg_database WHERE datname LIKE $1",
                        )
                        .bind(format!("cdc_web_pg{major}_sink_%"))
                        .fetch_all(&mut source_admin_connection),
                    )
                    .unwrap();
                for stale_database in stale_databases {
                    let quoted = format!("\"{}\"", stale_database.replace('"', "\"\""));
                    pg_runtime
                        .block_on(source_admin_connection.execute(sqlx::query(
                            sqlx::AssertSqlSafe(format!("DROP DATABASE {quoted} WITH (FORCE)")),
                        )))
                        .unwrap();
                }
                pg_runtime
                    .block_on(
                        source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                            "CREATE DATABASE \"{database}\""
                        )))),
                    )
                    .unwrap();
                let quoted_reader =
                    format!("\"{}\"", env_value("READER_USER").replace('"', "\"\""));
                let quoted_writer =
                    format!("\"{}\"", env_value("WRITER_USER").replace('"', "\"\""));
                pg_runtime
                    .block_on(source_admin_connection.execute(sqlx::query(
                        sqlx::AssertSqlSafe(format!(
                            "GRANT CONNECT ON DATABASE \"{database}\" TO {quoted_reader}, {quoted_writer}"
                        )),
                    )))
                    .unwrap();
                pg_runtime
                    .block_on(
                        source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                            "GRANT CREATE ON DATABASE \"{database}\" TO {quoted_writer}"
                        )))),
                    )
                    .unwrap();
                for role in [
                    env_value("ADMIN_USER"),
                    env_value("READER_USER"),
                    env_value("WRITER_USER"),
                ] {
                    let can_connect = pg_runtime
                        .block_on(
                            sqlx::query_scalar::<_, bool>(
                                "SELECT has_database_privilege($1, $2, 'CONNECT')",
                            )
                            .bind(&role)
                            .bind(&database)
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    assert!(
                        can_connect,
                        "PostgreSQL target role cannot connect to test database"
                    );
                }
                database
            } else {
                "CDC_test".into()
            };
            (
                format!("postgresql-{major}"),
                format!("postgresql_{major}"),
                database,
                env_value("HOST"),
                env_value("PORT").parse::<u16>().unwrap(),
                env_value("READER_USER"),
                env_value("TEST_PASSWORD"),
                env_value("WRITER_USER"),
                env_value("TEST_PASSWORD"),
                env_value("ADMIN_USER"),
            )
        }
    };
    let mut target_mysql = None;
    let mut target_postgresql = None;
    let mysql_fixture = if sink_connector_id.starts_with("mysql_") {
        let mut target = Conn::new(
            OptsBuilder::new()
                .ip_or_hostname(Some(sink_host.clone()))
                .tcp_port(sink_port)
                .user(Some(sink_writer.clone()))
                .pass(Some(sink_writer_password.clone()))
                .tcp_connect_timeout(Some(Duration::from_secs(5))),
        )
        .unwrap();
        target
            .query_drop("CREATE DATABASE IF NOT EXISTS CDC_test")
            .unwrap();
        let stale_tables = target
            .query::<String, _>(format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC_test' AND TABLE_NAME LIKE 'web_pg{source_major}_types_%'"
            ))
            .unwrap();
        for stale in stale_tables {
            let prefix = format!("web_pg{source_major}_types_");
            if let Some(suffix) = stale.strip_prefix(&prefix)
                && !suffix.is_empty()
                && suffix.chars().all(|character| character.is_ascii_digit())
            {
                let stale_task_id =
                    format!("pg{source_major}-{sink_connector_id}-all-types-{suffix}");
                let checkpoint_table_exists: Option<String> = target
                    .query_first(
                        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='CDC' AND TABLE_NAME='log_info'",
                    )
                    .unwrap();
                if checkpoint_table_exists.is_some() {
                    target
                        .exec_drop(
                            "DELETE FROM CDC.log_info WHERE task_id=?",
                            (&stale_task_id,),
                        )
                        .unwrap();
                }
                target
                    .query_drop(format!("DROP TABLE IF EXISTS CDC_test.`{stale}`"))
                    .unwrap();
            }
        }
        let columns = fields
            .iter()
            .map(|field| {
                let mapping = &source_mappings[&field.name];
                let native =
                    if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                        "LONGBLOB"
                    } else {
                        "JSON"
                    };
                format!("`{}` {native} NULL", field.name)
            })
            .collect::<Vec<_>>()
            .join(",");
        target
            .query_drop(format!(
                "CREATE TABLE CDC_test.`{table}` (`id` BIGINT NOT NULL PRIMARY KEY,{columns}) ENGINE=InnoDB ROW_FORMAT=DYNAMIC"
            ))
            .unwrap();
        target_mysql = Some(target);
        Some(MysqlFixtureTables {
            endpoints: vec![(sink_host.clone(), sink_port)],
            user: sink_writer.clone(),
            password: sink_writer_password.clone(),
            table: table.clone(),
        })
    } else {
        let target_options = PgConnectOptions::new()
            .host(&sink_host)
            .port(sink_port)
            .database(&sink_database)
            .username(&sink_admin)
            .password(&sink_writer_password)
            .ssl_mode(PgSslMode::Prefer);
        let mut target = pg_runtime
            .block_on(PgConnection::connect_with(&target_options))
            .unwrap();
        let stale_tables = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, String>(
                    "SELECT c.relname
                       FROM pg_catalog.pg_class c
                       JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                      WHERE n.nspname='CDC_test'
                        AND c.relname ~ $1
                        AND c.relkind='r'",
                )
                .bind(format!("^web_pg{source_major}_types_[0-9]+$"))
                .fetch_all(&mut target),
            )
            .unwrap();
        let checkpoint_table_exists = pg_runtime
            .block_on(
                sqlx::query_scalar::<_, bool>("SELECT to_regclass('cdc.log_info') IS NOT NULL")
                    .fetch_one(&mut target),
            )
            .unwrap();
        for stale in stale_tables {
            let prefix = format!("web_pg{source_major}_types_");
            if let Some(suffix) = stale.strip_prefix(&prefix)
                && !suffix.is_empty()
                && suffix.chars().all(|character| character.is_ascii_digit())
            {
                if checkpoint_table_exists {
                    let stale_task_id =
                        format!("pg{source_major}-{sink_connector_id}-all-types-{suffix}");
                    pg_runtime
                        .block_on(
                            sqlx::query("DELETE FROM cdc.log_info WHERE task_id=$1")
                                .bind(stale_task_id)
                                .execute(&mut target),
                        )
                        .unwrap();
                }
                let quoted_stale = format!("\"{}\"", stale.replace('"', "\"\""));
                pg_runtime
                    .block_on(
                        sqlx::query(sqlx::AssertSqlSafe(format!(
                            "DROP TABLE IF EXISTS \"CDC_test\".{quoted_stale} CASCADE"
                        )))
                        .execute(&mut target),
                    )
                    .unwrap();
            }
        }
        let columns = fields
            .iter()
            .map(|field| {
                let mapping = &source_mappings[&field.name];
                let native =
                    if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                        "bytea"
                    } else {
                        "text"
                    };
                format!("\"{}\" {native} NULL", field.name)
            })
            .collect::<Vec<_>>()
            .join(",");
        pg_runtime
            .block_on(async {
                target
                    .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "CREATE SCHEMA IF NOT EXISTS \"CDC_test\" AUTHORIZATION {sink_writer};
                         GRANT USAGE ON SCHEMA \"CDC_test\" TO {sink_writer};
                         CREATE SCHEMA IF NOT EXISTS cdc AUTHORIZATION {sink_writer};
                         GRANT USAGE,CREATE ON SCHEMA cdc TO {sink_writer};
                         CREATE TABLE \"CDC_test\".\"{table}\" (\"id\" bigint PRIMARY KEY,{columns});
                         GRANT SELECT,INSERT,UPDATE,DELETE ON \"CDC_test\".\"{table}\" TO {sink_writer}"
                    ))))
                    .await
                    .unwrap();
            });
        target_postgresql = Some(target);
        None
    };

    let (directory, store) = store();
    let admin_id = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = format!("pg{source_major}-web-all-types-source-{nonce}");
    source_input.host = source_host.clone();
    source_input.port = source_port;
    source_input.kind = "postgresql".into();
    source_input.version = source_major.to_string();
    source_input.database = "CDC_test".into();
    source_input.databases = vec!["CDC_test".into()];
    source_input.reader_username = source_reader.clone();
    source_input.reader_password = Some(source_password.clone());
    source_input.writer_username = source_writer;
    source_input.writer_password = Some(source_password.clone());
    let source_instance = store.save_instance(admin_id, None, source_input).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = format!("{sink_id}-web-all-types-target-{nonce}");
    sink_input.host = sink_host.clone();
    sink_input.port = sink_port;
    sink_input.kind = if sink_connector_id.starts_with("mysql_") {
        "mysql".into()
    } else {
        "postgresql".into()
    };
    sink_input.version = match sink {
        LiveTypeSink::Mysql(version) => version.into(),
        LiveTypeSink::Postgresql(major) => major.to_string(),
    };
    sink_input.database = sink_database.clone();
    sink_input.databases = if sink_database.is_empty() {
        Vec::new()
    } else {
        vec![sink_database.clone()]
    };
    sink_input.reader_username = sink_reader;
    sink_input.reader_password = Some(sink_reader_password);
    sink_input.writer_username = sink_writer;
    sink_input.writer_password = Some(sink_writer_password);
    let sink_instance = store.save_instance(admin_id, None, sink_input).unwrap();
    let mut source_catalog_connection = store
        .catalog_connection(admin_id, &source_instance.id, EndpointRole::Source)
        .unwrap();
    let source_capture_catalog_digest = source_catalog.evidence_digest();
    let source_web_catalog_digest = source_catalog_connection
        .source_type_catalog
        .as_ref()
        .expect("PostgreSQL Web source connection retains its type catalog")
        .evidence_digest();
    if source_web_catalog_digest != source_capture_catalog_digest {
        let web_catalog = source_catalog_connection
            .source_type_catalog
            .as_ref()
            .expect("Web source catalog is available");
        let capture_by_oid = source_catalog
            .types
            .iter()
            .map(|definition| (definition.oid, definition))
            .collect::<BTreeMap<_, _>>();
        let web_by_oid = web_catalog
            .types
            .iter()
            .map(|definition| (definition.oid, definition))
            .collect::<BTreeMap<_, _>>();
        let mut differences = capture_by_oid
            .keys()
            .chain(web_by_oid.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(
                |oid| match (capture_by_oid.get(&oid), web_by_oid.get(&oid)) {
                    (Some(capture), Some(web)) if capture == web => None,
                    (Some(capture), Some(web)) => Some(format!(
                        "oid={oid} type={}.{} capture_digest={} web_digest={}",
                        capture.schema,
                        capture.name,
                        capture.definition_digest,
                        web.definition_digest
                    )),
                    (Some(capture), None) => Some(format!(
                        "oid={oid} type={}.{} missing_from_web_catalog",
                        capture.schema, capture.name
                    )),
                    (None, Some(web)) => Some(format!(
                        "oid={oid} type={}.{} missing_from_capture_catalog",
                        web.schema, web.name
                    )),
                    (None, None) => unreachable!(),
                },
            )
            .take(5)
            .collect::<Vec<_>>();
        let capture_extensions = source_catalog
            .extensions
            .iter()
            .map(|extension| (extension.name.as_str(), extension.version.as_str()))
            .collect::<BTreeSet<_>>();
        let web_extensions = web_catalog
            .extensions
            .iter()
            .map(|extension| (extension.name.as_str(), extension.version.as_str()))
            .collect::<BTreeSet<_>>();
        if capture_extensions != web_extensions {
            differences.push(format!(
                "extension set capture={capture_extensions:?} web={web_extensions:?}"
            ));
        }
        eprintln!(
            "PostgreSQL {source_major} catalog digest mismatch: capture_types={} web_types={} differences={differences:?}",
            source_catalog.types.len(),
            web_catalog.types.len()
        );
    }
    assert_eq!(
        source_web_catalog_digest, source_capture_catalog_digest,
        "independent PostgreSQL catalog reads must bind the same source type snapshot before qualification"
    );
    let source_table = source_catalog_connection
        .table("CDC_test", &table)
        .unwrap()
        .expect("isolated source table");
    let mut sink_catalog_connection = store
        .catalog_connection(admin_id, &sink_instance.id, EndpointRole::Sink)
        .unwrap();
    let sink_table = sink_catalog_connection
        .table("CDC_test", &table)
        .unwrap()
        .expect("isolated target table");
    let source_connector = store
        .connector(&source_instance.id, EndpointRole::Source)
        .unwrap();
    let sink_connector = store
        .connector(&sink_instance.id, EndpointRole::Sink)
        .unwrap();
    let source_build =
        live_server_build_identity(source_connector, &source_catalog_connection.metadata);
    let target_build =
        live_server_build_identity(sink_connector, &sink_catalog_connection.metadata);
    let source_environment_fingerprint = match &source_catalog_connection.metadata {
        crate::model::Metadata::Postgresql(metadata) => metadata.environment_fingerprint.as_deref(),
        crate::model::Metadata::Mysql { .. } => None,
    };
    let draft_id = format!(
        "pg{source_major}-{}-all-types-{nonce}",
        sink_connector_id.replace('.', "_")
    );
    let web_preview = store
        .preview_field(
            admin_id,
            FieldPreviewInput {
                draft_id: draft_id.clone(),
                source_id: source_instance.id.clone(),
                sink_id: sink_instance.id.clone(),
                source_database: "CDC_test".into(),
                sink_database: sink_database.clone(),
                source_revision: 1,
                sink_revision: 1,
                schema: "CDC_test".into(),
                table: table.clone(),
                column: "id".into(),
                parameters: BTreeMap::new(),
                confirmations: vec![],
            },
        )
        .unwrap()
        .result
        .unwrap_or_else(|| panic!("real Web preview failed for route {draft_id}"));
    assert_eq!(
        web_preview.status,
        change_event::CompatibilityStatus::Compatible,
        "Web preview failed for the route row locator: {}",
        web_preview.explanation
    );
    let web_preview_digest = web_preview
        .plan
        .as_ref()
        .expect("Web preview returns a conversion plan")
        .plan_digest
        .clone();
    let mut conversion_options = BTreeMap::new();
    let mut confirmations = Vec::new();
    let mut confirmed_plans = Vec::new();
    let mut storage_modes = BTreeMap::new();
    let probe_columns = source_table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let target_probes = sink_catalog_connection
        .probe_targets("CDC_test", &table, &probe_columns)
        .unwrap_or_else(|error| panic!("target capability batch probe failed: {error}"));
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: target probe completed in {:.1}s for {} fields",
        qualification_started_at.elapsed().as_secs_f64(),
        source_table.columns.len()
    );
    for (column_index, column) in source_table.columns.iter().enumerate() {
        if column_index % 10 == 0 {
            eprintln!(
                "PostgreSQL {source_major} → {sink_id}: planning field {}/{} after {:.1}s",
                column_index + 1,
                source_table.columns.len(),
                qualification_started_at.elapsed().as_secs_f64()
            );
        }
        let sink_column = sink_table
            .columns
            .iter()
            .find(|candidate| candidate.name == column.name)
            .unwrap_or_else(|| panic!("target fixture is missing column {}", column.name));
        let target_probe = target_probes
            .get(&column.name)
            .unwrap_or_else(|| panic!("target probe omitted column {}", column.name));
        let source_mapping = if let Some(mapping) =
            source_mappings.get(&column.name).filter(|mapping| {
                mapping
                    .native_type
                    .eq_ignore_ascii_case(&column.column_type)
            }) {
            let mut mapping = mapping.clone();
            if let Some(environment_fingerprint) = source_environment_fingerprint {
                let definition_fingerprint = mapping
                    .source_definition_fingerprint
                    .clone()
                    .unwrap_or_else(|| source_capture_catalog_digest.clone());
                mapping = mapping.with_source_evidence(
                    definition_fingerprint,
                    source_build.clone(),
                    environment_fingerprint,
                );
            }
            mapping
        } else {
            source_connector
                .source_type_mapping_with_evidence(
                    column,
                    source_catalog_connection.source_type_catalog.as_ref(),
                    Some(source_build.clone()),
                    source_environment_fingerprint,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "PostgreSQL {source_major} key field {} did not map: {error}",
                        column.name
                    )
                })
        };
        let plan = |parameters: &BTreeMap<String, String>,
                    risk_confirmations: &[change_event::RiskConfirmation]| {
            crate::registry::field_compatibility_with_precomputed_source_mapping_and_target_probe(
                source_connector,
                sink_connector,
                &source_table,
                &sink_table,
                column,
                sink_column,
                source_mapping.clone(),
                &draft_id,
                &format!("{draft_id}:r1"),
                Some(source_build.clone()),
                Some(target_build.clone()),
                parameters,
                risk_confirmations,
                Some(target_probe),
            )
        };
        if column.name == "id" {
            let exact = plan(&BTreeMap::new(), &[])
                .unwrap_or_else(|error| panic!("BIGINT key planning failed: {error}"));
            assert_eq!(
                exact.status,
                change_event::CompatibilityStatus::Compatible,
                "BIGINT key must remain directly writable: {}",
                exact.explanation
            );
            let plan = exact.plan.unwrap();
            if plan.plan_digest != web_preview_digest {
                let web_plan = web_preview
                    .plan
                    .as_ref()
                    .expect("Web preview plan remains available");
                let web_json = serde_json::to_value(web_plan).unwrap();
                let task_json = serde_json::to_value(&plan).unwrap();
                let keys = web_json
                    .as_object()
                    .expect("plan serializes as an object")
                    .keys()
                    .chain(
                        task_json
                            .as_object()
                            .expect("plan serializes as an object")
                            .keys(),
                    )
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let differences = keys
                    .into_iter()
                    .filter_map(|key| {
                        let web = &web_json[&key];
                        let task = &task_json[&key];
                        (web != task).then(|| format!("{key}: web={web} task={task}"))
                    })
                    .collect::<Vec<_>>();
                eprintln!(
                    "PostgreSQL {source_major}→{sink_id} preview/task plan differences: {differences:?}"
                );
            }
            assert_eq!(
                plan.plan_digest, web_preview_digest,
                "batch qualification and the Web preview must use the same plan"
            );
            confirmed_plans.push((
                vec!["postgresql.bigint".to_owned()],
                plan,
                "native_target_column",
                false,
            ));
            storage_modes.insert(column.name.clone(), "native_target_column");
            continue;
        }
        let mapping = &source_mappings[&column.name];
        let conversion_kind =
            if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
                "source_representation"
            } else {
                "logical_value_json"
            };
        let discovered = plan(&BTreeMap::new(), &[]).unwrap_or_else(|error| {
            panic!("PostgreSQL {} planning failed: {error}", column.column_type)
        });
        // Prefer a qualified direct target mapping when one exists. Explicit
        // carriers remain the fallback for types with no native target form.
        if discovered.status == change_event::CompatibilityStatus::Compatible {
            let exact = discovered.plan.expect("compatible plan is present");
            let type_ids = live_postgresql_type_ids(
                source_major,
                fields
                    .iter()
                    .find(|field| field.name == column.name)
                    .unwrap(),
            );
            storage_modes.insert(column.name.clone(), "native_target_column");
            confirmed_plans.push((type_ids, exact, "native_target_column", false));
            continue;
        }
        // A directly planned target can still require an explicit risk
        // acknowledgement (for example, when comparison semantics differ).
        // Exercise that same confirmation flow instead of incorrectly treating
        // every non-Compatible preview as a missing carrier.
        let (parameters, pending, explicit_carrier) = if discovered.plan.is_some() {
            (BTreeMap::new(), discovered, false)
        } else {
            let candidate = discovered.candidates.iter().find(|candidate| {
                candidate
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some(conversion_kind)
            });
            let Some(candidate) = candidate else {
                panic!(
                    "Web has neither a compatible native plan nor an explicit {conversion_kind} carrier for PostgreSQL {}.{}: {}",
                    column.name, column.column_type, discovered.explanation
                );
            };
            let parameters = BTreeMap::from([
                ("__rule_id".into(), candidate.rule.id.clone()),
                ("__rule_version".into(), candidate.rule.version.clone()),
            ]);
            let pending = plan(&parameters, &[]).unwrap_or_else(|error| {
                panic!("no plan for PostgreSQL {}: {error}", column.column_type)
            });
            (parameters, pending, true)
        };
        let pending_plan = pending
            .plan
            .clone()
            .unwrap_or_else(|| panic!("PostgreSQL field plan missing: {pending:?}"));
        let confirmation = if pending.status == change_event::CompatibilityStatus::NeedsConfirmation
        {
            Some(change_event::RiskConfirmation {
                source_field_lineage: pending_plan.source_field.lineage_id.clone(),
                target_field_lineage: pending_plan.target_field.lineage_id.clone(),
                rule: pending_plan.rule.clone(),
                plan_digest: pending_plan.plan_digest.clone(),
                actor: "admin".into(),
                confirmed_at: "2026-09-30T00:00:00Z".into(),
                reason: Some(if explicit_carrier {
                    format!(
                        "qualify PostgreSQL {} using its explicit {conversion_kind} target representation",
                        column.column_type
                    )
                } else {
                    format!(
                        "qualify PostgreSQL {} after explicitly accepting the planned conversion risk",
                        column.column_type
                    )
                }),
            })
        } else {
            assert_eq!(
                pending.status,
                change_event::CompatibilityStatus::Compatible,
                "PostgreSQL {} compatibility preview failed: {}",
                column.column_type,
                pending.explanation
            );
            None
        };
        let confirmation_verified = confirmation.is_some();
        if let Some(confirmation) = &confirmation {
            confirmations.push(confirmation.clone());
        }
        let final_result = if let Some(confirmation) = confirmation {
            plan(&parameters, &[confirmation])
                .unwrap_or_else(|error| panic!("confirmed plan failed: {error}"))
        } else {
            pending
        };
        assert_eq!(
            final_result.status,
            change_event::CompatibilityStatus::Compatible,
            "PostgreSQL {} plan is not confirmed: {}",
            column.column_type,
            final_result.explanation
        );
        let plan = final_result.plan.unwrap();
        if !parameters.is_empty() {
            conversion_options.insert(column.name.clone(), parameters);
        }
        let field = fields
            .iter()
            .find(|field| field.name == column.name)
            .expect("source field originates in the built-in fixture");
        let type_ids = live_postgresql_type_ids(source_major, field);
        let storage_mode = if !explicit_carrier {
            "native_target_column"
        } else if conversion_kind == "source_representation" {
            "source_representation_blob_carrier"
        } else {
            "logical_value_json_carrier"
        };
        storage_modes.insert(column.name.clone(), storage_mode);
        confirmed_plans.push((type_ids, plan, storage_mode, confirmation_verified));
    }
    let columns = std::iter::once("id".to_owned())
        .chain(fields.iter().map(|field| field.name.clone()))
        .collect::<Vec<_>>();
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: planned {} fields in {:.1}s; creating task",
        source_table.columns.len(),
        qualification_started_at.elapsed().as_secs_f64()
    );
    let task = store
        .create_task(
            admin_id,
            TaskInput {
                draft_id: Some(draft_id),
                name: format!("PostgreSQL {source_major} all built-ins to {sink_id} {nonce}"),
                source_id: source_instance.id.clone(),
                sink_id: sink_instance.id.clone(),
                source_database: "CDC_test".into(),
                sink_database: sink_database.clone(),
                source_revision: 1,
                sink_revision: 1,
                start_mode: "auto".into(),
                mappings: vec![TableMapping {
                    source_schema: "CDC_test".into(),
                    source_table: table.clone(),
                    sink_schema: "CDC_test".into(),
                    sink_table: table.clone(),
                    columns,
                    conversion_options,
                }],
                confirmations,
            },
        )
        .unwrap();
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: task created in {:.1}s; starting",
        qualification_started_at.elapsed().as_secs_f64()
    );
    assert_eq!(task.plan_status, "valid");
    assert!(confirmed_plans.iter().all(|(_, plan, _, _)| {
        task.plans
            .iter()
            .any(|saved| saved.plan_digest == plan.plan_digest)
    }));
    if isolated_same_version_postgresql_target {
        assert_eq!(task.sink_database, sink_database);
        let endpoint = store
            .endpoint_for_database(&sink_instance.id, 1, "writer", Some(&sink_database))
            .unwrap();
        assert_eq!(endpoint.database, sink_database);
        let target_config = postgresql_15::TargetConfig::new(
            endpoint.host,
            endpoint.database,
            endpoint.user,
            endpoint.password,
        )
        .with_port(endpoint.port);
        let probe_id = format!("qualprobe_{nonce}");
        let source_identity = format!("qual-source-{nonce}");
        let binding = format!("{:x}", Sha256::digest(probe_id.as_bytes()));
        let target_version = source_major.to_string();
        let checkpoint_writer = postgresql_15::CheckpointWriter::open_for_version(
            &target_config,
            &probe_id,
            &source_identity,
            &binding,
            &target_version,
        )
        .unwrap();
        drop(checkpoint_writer);
    }
    store.start_task(admin_id, task.id.clone()).unwrap();
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: start requested at {:.1}s",
        qualification_started_at.elapsed().as_secs_f64()
    );
    wait_for(&store, &task.id, |task| task.status == "running");
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: task running at {:.1}s",
        qualification_started_at.elapsed().as_secs_f64()
    );
    let source_fixture_exists = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, bool>(
                "SELECT pg_catalog.to_regclass(format('%I.%I', 'CDC_test', $1)) IS NOT NULL",
            )
            .bind(&table)
            .fetch_one(&mut source_admin_connection),
        )
        .unwrap();
    assert!(
        source_fixture_exists,
        "starting PostgreSQL {source_major} → {sink_connector_id} removed source fixture {table}"
    );

    let audit_slot = format!("cdc_web_q{source_major}{nonce}");
    let mut audit_config = postgresql_15::Config::new(
        source_host.clone(),
        source_port,
        "CDC_test",
        &source_reader,
        &source_password,
        &source_publication,
        &audit_slot,
    );
    audit_config.create_slot = true;
    let mut audit_replication = pg_runtime
        .block_on(postgresql_15::replication_for_version(
            audit_config,
            source_major,
        ))
        .unwrap_or_else(|error| panic!("cannot open independent type evidence stream: {error}"));

    // Keep each heap tuple comfortably below PostgreSQL's 8 KiB tuple limit.
    // All batches still belong to one source transaction so the route proves
    // transaction capture/apply for the complete catalog fixture.
    const FIELD_BATCH_SIZE: usize = 24;
    let mut fields_by_row_id = BTreeMap::<i64, Vec<&LivePostgresqlField>>::new();
    let mut field_row_ids = BTreeMap::<String, i64>::new();
    for (batch_index, field_batch) in fields.chunks(FIELD_BATCH_SIZE).enumerate() {
        let row_id = i64::try_from(batch_index + 1).expect("fixture row id fits i64");
        for field in field_batch {
            fields_by_row_id.entry(row_id).or_default().push(field);
            field_row_ids.insert(field.name.clone(), row_id);
        }
    }
    let initial_insert_rows = u64::try_from(fields_by_row_id.len())
        .expect("fixture batch count fits applied row counter");
    let source_fixture_exists = pg_runtime
        .block_on(
            sqlx::query_scalar::<_, bool>(
                "SELECT pg_catalog.to_regclass(format('%I.%I', 'CDC_test', $1)) IS NOT NULL",
            )
            .bind(&table)
            .fetch_one(&mut source_admin_connection),
        )
        .unwrap();
    assert!(
        source_fixture_exists,
        "source fixture {table} disappeared before qualification INSERT for PostgreSQL {source_major} → {sink_connector_id}"
    );
    pg_runtime.block_on(async {
        let mut insert_transaction = source_admin_connection.begin().await.unwrap();
        for (row_id, row_fields) in &fields_by_row_id {
            let insert_columns = std::iter::once("\"id\"".to_owned())
                .chain(row_fields.iter().map(|field| format!("\"{}\"", field.name)))
                .collect::<Vec<_>>()
                .join(",");
            let insert_values = std::iter::once(row_id.to_string())
                .chain(row_fields.iter().map(|field| field.expression.clone()))
                .collect::<Vec<_>>()
                .join(",");
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO \"CDC_test\".\"{table}\" ({insert_columns}) VALUES ({insert_values})"
            )))
            .execute(&mut *insert_transaction)
            .await
            .unwrap();
        }
        insert_transaction.commit().await.unwrap();
    });
    let audit_cancel = postgresql_15::CancellationToken::new();
    let captured_transaction = pg_runtime
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(40),
                audit_replication.next_transaction(&audit_cancel),
            )
            .await
        })
        .unwrap_or_else(|error| panic!("timed out capturing qualification row: {error}"))
        .unwrap_or_else(|error| {
            panic!("source adapter failed capturing qualification row: {error}")
        });
    drop(audit_replication);
    pg_runtime
        .block_on(
            sqlx::query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name)
                   FROM pg_catalog.pg_replication_slots
                  WHERE slot_name=$1 AND NOT active",
            )
            .bind(&audit_slot)
            .execute(&mut source_admin_connection),
        )
        .unwrap();
    let captured_json = change_event::json(&captured_transaction).unwrap();
    let mut replay_reader = change_event::JsonReader::new(std::io::Cursor::new(&captured_json));
    let replayed_transaction = replay_reader
        .next_transaction()
        .unwrap()
        .expect("captured type qualification transaction replays");
    assert_eq!(
        change_event::json(&replayed_transaction).unwrap(),
        captured_json
    );
    replay_reader.finish().unwrap();
    let captured_changes = captured_transaction
        .transaction()
        .changes
        .iter()
        .filter(|change| change.schema == "CDC_test" && change.table == table)
        .collect::<Vec<_>>();
    assert_eq!(
        captured_changes.len(),
        fields_by_row_id.len(),
        "one ChangeEvent INSERT is required for each bounded fixture row"
    );
    let mut captured_datums = BTreeMap::new();
    for captured_change in captured_changes {
        assert!(matches!(
            captured_change.operation,
            change_event::Operation::Insert
        ));
        let captured_row = captured_change.after.as_ref().expect("INSERT after image");
        let row_id = captured_row
            .iter()
            .find(|column| column.name == "id")
            .unwrap_or_else(|| panic!("ChangeEvent omitted the fixture row locator"));
        let row_id = match &row_id.datum {
            change_event::Datum::Value(change_event::LogicalValue::Integer { value, .. }) => {
                value.parse::<i64>().unwrap_or_else(|error| {
                    panic!("captured fixture row id is not an integer: {error}")
                })
            }
            other => panic!("captured fixture row id has unexpected value: {other:?}"),
        };
        let row_fields = fields_by_row_id
            .get(&row_id)
            .unwrap_or_else(|| panic!("ChangeEvent returned unexpected fixture row id {row_id}"));
        for field in row_fields {
            let column = captured_row
                .iter()
                .find(|column| column.name == field.name)
                .unwrap_or_else(|| panic!("ChangeEvent omitted source column {}", field.name));
            captured_datums.insert(field.name.as_str(), &column.datum);
        }
    }
    assert_eq!(
        captured_datums.len(),
        fields.len(),
        "every fixture field must be selected from its non-NULL source row"
    );
    let mut exact_source_type_ids = BTreeMap::<String, String>::new();
    let mut exact_representation_type_ids = Vec::new();
    let mut exact_null_only_type_ids = BTreeSet::new();
    for field in &fields {
        let datum = *captured_datums
            .get(field.name.as_str())
            .unwrap_or_else(|| panic!("ChangeEvent omitted source column {}", field.name));
        assert!(
            !matches!(
                datum,
                change_event::Datum::Unavailable | change_event::Datum::Unchanged
            ),
            "INSERT source field {} has no transferable value: {datum:?}",
            field.name
        );
        let root_oid = u32::try_from(catalog_columns[&field.name]).unwrap();
        let coverage = live_postgresql_value_type_coverage(&source_catalog, root_oid, datum);
        for oid in coverage.values {
            let Some(type_id) = catalog_type_ids_by_oid.get(&oid) else {
                continue;
            };
            let outcome = if catalog_raw_type_oids.contains(&oid) {
                exact_representation_type_ids.push(type_id.clone());
                "SOURCE_REPRESENTATION_PRESERVED"
            } else {
                "VALUE_PRESERVED"
            };
            exact_source_type_ids.insert(type_id.clone(), outcome.to_owned());
        }
        for oid in coverage.null_only {
            if let Some(type_id) = catalog_type_ids_by_oid.get(&oid)
                && !exact_source_type_ids.contains_key(type_id)
            {
                exact_null_only_type_ids.insert(type_id.clone());
            }
        }
    }
    exact_null_only_type_ids.retain(|type_id| !exact_source_type_ids.contains_key(type_id));
    type_qualification_evidence::record_source_type_evidence(
        &format!("postgresql_{source_major}"),
        &format!("web_ui.postgresql_{source_major}_dynamic_types_to_{sink_connector_id}_live"),
        exact_source_type_ids.keys().cloned(),
        exact_representation_type_ids,
    )
    .unwrap();
    type_qualification_evidence::record_source_null_only_type_evidence(
        &format!("postgresql_{source_major}"),
        &format!("web_ui.postgresql_{source_major}_all_builtin_types_to_{sink_connector_id}_null_only_live"),
        exact_null_only_type_ids.iter().cloned(),
    )
    .unwrap();
    wait_for(&store, &task.id, |task| {
        task.runtime.applied_rows >= initial_insert_rows
    });
    eprintln!(
        "PostgreSQL {source_major} → {sink_id}: initial rows applied at {:.1}s",
        qualification_started_at.elapsed().as_secs_f64()
    );
    let mut verified_dynamic_type_ids = BTreeMap::<String, (String, String)>::new();
    for field in &fields {
        if field.name == "nullable_marker" {
            continue;
        }
        let mapping = &source_mappings[&field.name];
        let is_raw = matches!(mapping.logical_type, change_event::LogicalType::Raw { .. });
        let expected_datum = *captured_datums
            .get(field.name.as_str())
            .unwrap_or_else(|| panic!("ChangeEvent omitted source column {}", field.name));
        let root_oid = u32::try_from(catalog_columns[&field.name]).unwrap();
        let coverage =
            live_postgresql_value_type_coverage(&source_catalog, root_oid, expected_datum);
        let has_exact_type_ids = coverage
            .values
            .iter()
            .chain(coverage.null_only.iter())
            .any(|oid| catalog_type_ids_by_oid.contains_key(oid));
        let storage_mode = storage_modes[&field.name];
        let row_id = field_row_ids[&field.name];
        match (
            &mut target_mysql,
            &mut target_postgresql,
            is_raw,
            storage_mode,
        ) {
            (Some(target), None, false, "native_target_column")
                if matches!(mapping.logical_type, change_event::LogicalType::Json { .. }) =>
            {
                let value: Option<String> = target
                    .query_first(format!(
                        "SELECT `{}` FROM CDC_test.`{table}` WHERE id={row_id}",
                        field.name
                    ))
                    .unwrap();
                let target_json: serde_json::Value = serde_json::from_str(
                    value
                        .as_deref()
                        .expect("native JSON target contains the value"),
                )
                .unwrap_or_else(|error| panic!("native JSON value is invalid: {error}"));
                let source_json: String = pg_runtime
                    .block_on(
                        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                            field.name
                        )))
                        .fetch_one(&mut source_admin_connection),
                    )
                    .unwrap();
                assert_eq!(
                    target_json,
                    serde_json::from_str::<serde_json::Value>(&source_json).unwrap(),
                    "native JSON changed value for PostgreSQL {}",
                    field.declaration
                );
                if let change_event::Datum::Value(expected) = expected_datum {
                    let expected_fixture_value = change_event::LogicalValue::Json {
                        value: change_event::JsonValue::Object(vec![change_event::JsonEntry {
                            key: "n".into(),
                            value: change_event::JsonValue::Decimal {
                                unscaled: "1".into(),
                                scale: 0,
                            },
                        }]),
                    };
                    assert_eq!(
                        expected, &expected_fixture_value,
                        "native JSON ChangeEvent value differs from the source fixture"
                    );
                }
            }
            (Some(target), None, false, "native_target_column") => {
                let value: Option<String> = target
                    .query_first(format!(
                        "SELECT `{}` FROM CDC_test.`{table}` WHERE id={row_id}",
                        field.name
                    ))
                    .unwrap();
                assert!(
                    value.is_some(),
                    "native value is NULL for {}",
                    field.declaration
                );
                if has_exact_type_ids {
                    let source_text: String = pg_runtime
                        .block_on(
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                                field.name
                            )))
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    let target_text: Option<String> = target
                        .query_first(format!(
                            "SELECT CAST(`{}` AS CHAR) FROM CDC_test.`{table}` WHERE id={row_id}",
                            field.name
                        ))
                        .unwrap();
                    let target_text = target_text
                        .unwrap_or_else(|| panic!("native sink value missing for {}", field.name));
                    assert_eq!(
                        target_text, source_text,
                        "native dynamic type value changed"
                    );
                }
            }
            (Some(target), None, false, _) => {
                let value: Option<String> = target
                    .query_first(format!(
                        "SELECT `{}` FROM CDC_test.`{table}` WHERE id={row_id}",
                        field.name
                    ))
                    .unwrap();
                let logical: change_event::LogicalValue =
                    serde_json::from_str(value.as_deref().expect("logical carrier stores a value"))
                        .unwrap_or_else(|error| {
                            panic!(
                                "PostgreSQL {} JSON carrier cannot replay: {error}",
                                field.declaration
                            )
                        });
                match expected_datum {
                    change_event::Datum::Value(expected) => assert_eq!(
                        logical, *expected,
                        "MySQL logical carrier differs from captured ChangeEvent value for {}",
                        field.declaration
                    ),
                    other => panic!("logical carrier source datum was not a value: {other:?}"),
                }
            }
            (Some(target), None, true, _) => {
                let value: Option<Vec<u8>> = target
                    .query_first::<Option<Vec<u8>>, _>(format!(
                        "SELECT `{}` FROM CDC_test.`{table}` WHERE id={row_id}",
                        field.name
                    ))
                    .unwrap()
                    .expect("target row exists even when its source value is SQL NULL");
                if matches!(expected_datum, change_event::Datum::Null) {
                    assert!(
                        value.is_none(),
                        "SQL NULL for PostgreSQL {} must stay NULL in the target carrier",
                        field.declaration
                    );
                } else {
                    assert!(
                        value.as_ref().is_some_and(|bytes| !bytes.is_empty()),
                        "PostgreSQL {} representation carrier was not stored",
                        field.declaration
                    );
                }
                if is_raw && !matches!(expected_datum, change_event::Datum::Null) {
                    let source_text: String = pg_runtime
                        .block_on(
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                                field.name
                            )))
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    let stored = value.as_deref().expect("source representation is stored");
                    let envelope: change_event::SourceRepresentationEnvelope =
                        serde_json::from_slice(stored).expect("carrier stores a valid envelope");
                    if let change_event::Datum::SourceRepresentationEnvelope(expected) =
                        expected_datum
                    {
                        assert_eq!(
                            &envelope, expected,
                            "MySQL carrier changed the captured envelope"
                        );
                    } else {
                        panic!("raw source type did not produce an envelope");
                    }
                    assert_eq!(
                        envelope.raw_bytes().unwrap(),
                        source_text.as_bytes(),
                        "MySQL carrier envelope must preserve the exact pgoutput text for {}",
                        field.declaration
                    );
                }
            }
            (None, Some(target), false, "native_target_column") => {
                let value: String = pg_runtime
                    .block_on(
                        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT \"{}\" FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                            field.name
                        )))
                        .fetch_one(&mut *target),
                    )
                    .unwrap();
                assert!(!value.is_empty(), "native text value is empty");
                if has_exact_type_ids {
                    let source_text: String = pg_runtime
                        .block_on(
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                                field.name
                            )))
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    assert_eq!(value, source_text, "native dynamic type value changed");
                }
                if matches!(mapping.logical_type, change_event::LogicalType::Text { .. }) {
                    let source_text: String = pg_runtime
                        .block_on(
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                                field.name
                            )))
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    assert_eq!(value, source_text, "native text value changed");
                }
            }
            (None, Some(target), false, _) => {
                let value: String = pg_runtime
                    .block_on(
                        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT \"{}\" FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                            field.name
                        )))
                        .fetch_one(&mut *target),
                    )
                    .unwrap();
                let logical: change_event::LogicalValue = serde_json::from_str(&value)
                    .unwrap_or_else(|error| {
                        panic!(
                            "PostgreSQL {} text carrier cannot replay: {error}",
                            field.declaration
                        )
                    });
                match expected_datum {
                    change_event::Datum::Value(expected) => assert_eq!(
                        logical, *expected,
                        "PostgreSQL logical carrier differs from captured ChangeEvent value for {}",
                        field.declaration
                    ),
                    other => panic!("logical carrier source datum was not a value: {other:?}"),
                }
            }
            (None, Some(target), true, _) => {
                let value: Option<Vec<u8>> = pg_runtime
                    .block_on(
                        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT \"{}\" FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                            field.name
                        )))
                        .fetch_one(&mut *target),
                    )
                    .unwrap();
                if matches!(expected_datum, change_event::Datum::Null) {
                    assert!(
                        value.is_none(),
                        "SQL NULL for PostgreSQL {} must stay NULL in the target carrier",
                        field.declaration
                    );
                } else {
                    assert!(
                        value.as_ref().is_some_and(|bytes| !bytes.is_empty()),
                        "PostgreSQL {} representation carrier was not stored",
                        field.declaration
                    );
                }
                if is_raw && !matches!(expected_datum, change_event::Datum::Null) {
                    let source_text: String = pg_runtime
                        .block_on(
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT \"{}\"::text FROM \"CDC_test\".\"{table}\" WHERE id={row_id}",
                                field.name
                            )))
                            .fetch_one(&mut source_admin_connection),
                        )
                        .unwrap();
                    let envelope: change_event::SourceRepresentationEnvelope =
                        serde_json::from_slice(value.as_deref().unwrap())
                            .expect("carrier stores a valid envelope");
                    if let change_event::Datum::SourceRepresentationEnvelope(expected) =
                        expected_datum
                    {
                        assert_eq!(
                            &envelope, expected,
                            "PostgreSQL carrier changed the captured envelope"
                        );
                    } else {
                        panic!("raw source type did not produce an envelope");
                    }
                    assert_eq!(
                        envelope.raw_bytes().unwrap(),
                        source_text.as_bytes(),
                        "PostgreSQL carrier envelope must preserve the exact pgoutput text for {}",
                        field.declaration
                    );
                }
            }
            _ => unreachable!("exactly one live sink connection exists"),
        }
        for oid in coverage.values {
            let Some(type_id) = catalog_type_ids_by_oid.get(&oid) else {
                continue;
            };
            verified_dynamic_type_ids.insert(
                type_id.clone(),
                (
                    if catalog_raw_type_oids.contains(&oid) {
                        "SOURCE_REPRESENTATION_PRESERVED".to_owned()
                    } else {
                        "VALUE_PRESERVED".to_owned()
                    },
                    storage_mode.to_owned(),
                ),
            );
        }
        for oid in coverage.null_only {
            if let Some(type_id) = catalog_type_ids_by_oid.get(&oid)
                && !verified_dynamic_type_ids.contains_key(type_id)
            {
                verified_dynamic_type_ids.insert(
                    type_id.clone(),
                    ("NULL_PRESERVED".to_owned(), storage_mode.to_owned()),
                );
            }
        }
    }
    assert_eq!(
        verified_dynamic_type_ids
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        exact_source_type_ids
            .keys()
            .cloned()
            .chain(exact_null_only_type_ids.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>(),
        "every catalog type instance in the live fixture must have a verified sink readback"
    );
    let mut verified_by_outcome = BTreeMap::<(String, String), Vec<String>>::new();
    for (type_id, (outcome, storage_mode)) in verified_dynamic_type_ids {
        verified_by_outcome
            .entry((outcome, storage_mode))
            .or_default()
            .push(type_id);
    }
    for ((outcome, storage_mode), type_ids) in verified_by_outcome {
        type_qualification_evidence::record_sink_type_evidence(
            &format!("postgresql_{source_major}"),
            &sink_connector_id,
            &format!(
                "web_ui.postgresql_{source_major}_dynamic_types_to_{sink_connector_id}_{outcome}"
            ),
            type_ids,
            &outcome,
            &storage_mode,
        )
        .unwrap();
    }
    let marker_row_id = field_row_ids["nullable_marker"];
    let marker_is_null = match (&mut target_mysql, &mut target_postgresql) {
        (Some(target), None) => target
            .query_first::<bool, _>(format!(
                "SELECT `nullable_marker` IS NULL FROM CDC_test.`{table}` WHERE id={marker_row_id}"
            ))
            .unwrap()
            .unwrap(),
        (None, Some(target)) => pg_runtime
            .block_on(
                sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(format!(
                    "SELECT \"nullable_marker\" IS NULL FROM \"CDC_test\".\"{table}\" WHERE id={marker_row_id}"
                )))
                .fetch_one(&mut *target),
            )
            .unwrap(),
        _ => unreachable!("exactly one live sink connection exists"),
    };
    assert!(marker_is_null, "INSERT NULL presence was not preserved");
    pg_runtime
        .block_on(
            source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE \"CDC_test\".\"{table}\" SET \"nullable_marker\"='web-updated' WHERE id={marker_row_id}"
            )))),
        )
        .unwrap();
    wait_for(&store, &task.id, |task| {
        task.runtime.applied_rows > initial_insert_rows
    });
    let marker_is_present = match (&mut target_mysql, &mut target_postgresql) {
        (Some(target), None) => target
            .query_first::<bool, _>(format!(
                "SELECT `nullable_marker` IS NOT NULL FROM CDC_test.`{table}` WHERE id={marker_row_id}"
            ))
            .unwrap()
            .unwrap(),
        (None, Some(target)) => pg_runtime
            .block_on(
                sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(format!(
                    "SELECT \"nullable_marker\" IS NOT NULL FROM \"CDC_test\".\"{table}\" WHERE id={marker_row_id}"
                )))
                .fetch_one(&mut *target),
            )
            .unwrap(),
        _ => unreachable!("exactly one live sink connection exists"),
    };
    assert!(marker_is_present, "UPDATE value did not reach the target");
    pg_runtime
        .block_on(
            source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM \"CDC_test\".\"{table}\" WHERE id={marker_row_id}"
            )))),
        )
        .unwrap();
    wait_for(&store, &task.id, |task| {
        task.runtime.applied_rows >= initial_insert_rows + 2
    });
    let remaining_rows = match (&mut target_mysql, &mut target_postgresql) {
        (Some(target), None) => target
            .query_first::<i64, _>(format!(
                "SELECT COUNT(*) FROM CDC_test.`{table}` WHERE id={marker_row_id}"
            ))
            .unwrap()
            .unwrap(),
        (None, Some(target)) => pg_runtime
            .block_on(
                sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                    "SELECT COUNT(*) FROM \"CDC_test\".\"{table}\" WHERE id={marker_row_id}"
                )))
                .fetch_one(&mut *target),
            )
            .unwrap(),
        _ => unreachable!("exactly one live sink connection exists"),
    };
    assert_eq!(remaining_rows, 0, "DELETE did not remove the target row");
    store.stop_task(admin_id, &task.id).unwrap();
    store.shutdown_tasks();

    let slot_hash = format!("{:x}", Sha256::digest(task.id.as_bytes()));
    let slot_name = format!("cdc_web_{}", &slot_hash[..16]);
    pg_runtime
        .block_on(
            sqlx::query(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active",
            )
            .bind(&slot_name)
            .execute(&mut source_admin_connection),
        )
        .unwrap();
    match (&mut target_mysql, &mut target_postgresql) {
        (Some(target), None) => {
            target
                .exec_drop("DELETE FROM CDC.log_info WHERE task_id=?", (&task.id,))
                .unwrap();
            target
                .query_drop(format!("DROP TABLE CDC_test.`{table}`"))
                .unwrap();
        }
        (None, Some(target)) => {
            pg_runtime
                .block_on(
                    sqlx::query("DELETE FROM cdc.log_info WHERE task_id=$1")
                        .bind(&task.id)
                        .execute(&mut *target),
                )
                .unwrap();
            pg_runtime
                .block_on(
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP TABLE \"CDC_test\".\"{table}\""
                    )))
                    .execute(&mut *target),
                )
                .unwrap();
        }
        _ => unreachable!("exactly one live sink connection exists"),
    }
    if added_publication_member {
        pg_runtime
            .block_on(
                source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "ALTER PUBLICATION {source_publication} DROP TABLE \"CDC_test\".\"{table}\""
                )))),
            )
            .unwrap();
    }
    pg_runtime
        .block_on(
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP TABLE \"CDC_test\".\"{table}\""
            )))
            .execute(&mut source_admin_connection),
        )
        .unwrap();
    if dynamic_only {
        pg_runtime
            .block_on(
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP TABLE \"CDC_test\".\"{mcv_source_table}\" CASCADE"
                )))
                .execute(&mut source_admin_connection),
            )
            .unwrap();
    }
    if created_publication {
        pg_runtime
            .block_on(
                source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP PUBLICATION {source_publication}"
                )))),
            )
            .unwrap();
    }
    if !dynamic_only && !preserve_type_schema {
        live_drop_fixture_type_extensions(&pg_runtime, &mut source_admin_connection, &type_schema);
        pg_runtime
            .block_on(
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP SCHEMA {quoted_type_schema} CASCADE"
                )))
                .execute(&mut source_admin_connection),
            )
            .unwrap();
    }
    if !dynamic_only && !preserve_type_schema && *created_postgis_for_fixture {
        pg_runtime
            .block_on(
                sqlx::query("DROP EXTENSION postgis CASCADE").execute(&mut source_admin_connection),
            )
            .unwrap();
        *created_postgis_for_fixture = false;
    }
    if isolated_same_version_postgresql_target {
        drop(sink_catalog_connection);
        drop(target_postgresql.take());
        pg_runtime
            .block_on(
                source_admin_connection.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP DATABASE \"{sink_database}\" WITH (FORCE)"
                )))),
            )
            .unwrap();
    }
    drop(mysql_fixture);
    drop(directory);

    let suite_id = if dynamic_only {
        format!("web_ui.postgresql_{source_major}_dynamic_types_to_{sink_connector_id}_live")
    } else {
        format!(
            "web_ui.postgresql_{source_major}_all_builtin_types_to_{sink_connector_id}_carrier_live"
        )
    };
    for (type_ids, plan, storage_mode, confirmation_verified) in &confirmed_plans {
        for type_id in type_ids {
            type_qualification_evidence::record_web_plan_evidence(
                &format!("postgresql_{source_major}"),
                &sink_connector_id,
                &suite_id,
                type_id,
                plan,
                storage_mode,
                *confirmation_verified,
            )
            .unwrap();
        }
    }
    true
}
