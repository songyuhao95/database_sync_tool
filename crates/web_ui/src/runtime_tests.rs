use super::{instance_input, store};
use crate::{
    Error, Store,
    model::NewUser,
    tasks::{TableMapping, TaskInput},
};
use mysql_driver::{Conn, OptsBuilder, prelude::Queryable};
use sqlx::{
    Connection as _, Executor as _, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

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
            "timed out: state={} rows={} error={:?}",
            task.status,
            task.runtime.applied_rows,
            task.runtime.last_error
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
            "CREATE TABLE CDC_test.{table}(id INT UNSIGNED PRIMARY KEY,message VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL) ENGINE=InnoDB"
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
                     CREATE TABLE \"CDC_test\".\"{table}\"(id bigint PRIMARY KEY,message character varying(40) NOT NULL);
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
            sqlx::query_as::<_, (i64, String)>(sqlx::AssertSqlSafe(format!(
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
