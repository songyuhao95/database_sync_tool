use super::*;
use crate::catalog::{CatalogQuery, EndpointRole};

#[tokio::test]
async fn postgres_instance_api_accepts_pg15_and_preserves_credentials() {
    let (dir, store) = store();
    let login = store.login("admin", PASSWORD, None).unwrap();
    let app = router(
        store.clone(),
        WebConfig {
            origin: "http://127.0.0.1:8080".into(),
        },
    )
    .unwrap();
    let payload = json!({
        "name":"pg15", "host":"192.168.0.10", "port":54321,
        "kind":"postgresql", "version":"15", "database":"CDC_test",
        "reader_username":"postgresql_reader", "reader_password":"reader-test-secret",
        "writer_username":"postgresql_writer", "writer_password":"writer-test-secret"
    });
    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/instances")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::COOKIE, format!("cdc_session={}", login.token))
                .header("x-csrf-token", &login.session.csrf_token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["kind"], "postgresql");
    assert_eq!(body["database"], "CDC_test");
    assert!(!body.to_string().contains("test-secret"));
    let id = body["id"].as_str().unwrap();
    let mut input = payload.clone();
    input["name"] = json!("pg15-edited");
    input["database"] = json!("another_db");
    input["reader_password"] = Value::Null;
    input["writer_password"] = Value::Null;
    let saved = store
        .save_instance(
            login.session.user.id,
            Some(id.into()),
            serde_json::from_value(input).unwrap(),
        )
        .unwrap();
    assert!(saved.has_reader_password && saved.has_writer_password);
    // PostgreSQL instances must not enter the currently MySQL-only task worker.
    assert!(matches!(
        store.catalog_connection(
            login.session.user.id,
            id,
            crate::catalog::EndpointRole::Source
        ),
        Err(Error::Invalid(_))
    ));
    drop(app);
    drop(store);
    let store = Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap();
    let saved = serde_json::to_value(store.instance(id).unwrap()).unwrap();
    assert_eq!(saved["name"], "pg15-edited");
    assert_eq!(saved["database"], "another_db");
    assert_eq!(saved["kind"], "postgresql");
    assert!(
        store
            .db()
            .unwrap()
            .query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))
            .unwrap()
    );
}

#[test]
fn version_six_migration_preserves_tasks_secrets_and_mysql_metadata() {
    let dir = TestDirectory::new();
    let path = dir.path().join("web.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(include_str!("schema.sql")).unwrap();
    conn.execute_batch(include_str!("migration_5.sql")).unwrap();
    conn.execute_batch(include_str!("migration_6.sql")).unwrap();
    conn.execute("INSERT INTO users(id,owner,username,password_hash,role) VALUES(1,'admin','admin','unused','admin')",[]).unwrap();
    let secret = crate::secrets::seal(
        &crate::secrets::cipher(&[7; 32]),
        "preserved-reader",
        "source:reader",
    )
    .unwrap();
    let metadata = r#"{"server_version":"5.7.44","log_bin":true,"binlog_format":"ROW","binlog_row_image":"FULL","gtid_mode":"ON"}"#;
    conn.execute("INSERT INTO instances(id,name,host,port,version,reader_username,reader_secret,writer_username,metadata_json,revision) VALUES('source','source','127.0.0.1',3306,'5.7','reader',?1,'',?2,4)",rusqlite::params![secret,metadata]).unwrap();
    conn.execute("INSERT INTO instances(id,name,host,port,version,reader_username,writer_username,revision) VALUES('sink','sink','127.0.0.1',3307,'8.0','','',2)",[]).unwrap();
    conn.execute("INSERT INTO replication_tasks(id,name,source_id,sink_id,source_revision,sink_revision,start_mode,mappings_json,created_at,auto_start) VALUES('task','task','source','sink',4,2,'auto','[]',1,1)",[]).unwrap();
    let checkpoint = r#"{"source_uuid":"s","sink_uuid":"d","mode":"binlog","file":"binlog.000001","position":42,"gtid_set":null}"#;
    conn.execute(
        "INSERT INTO task_runtime(task_id,state,checkpoint_json) VALUES('task','stopped',?1)",
        [checkpoint],
    )
    .unwrap();
    drop(conn);
    let store = Store::open(&path, [7; 32]).unwrap();
    let source = store.instance("source").unwrap();
    assert_eq!(source.kind, "mysql");
    assert_eq!(source.database, "");
    assert!(matches!(
        source.metadata,
        Some(crate::model::Metadata::Mysql { log_bin: true, .. })
    ));
    assert_eq!(
        store.endpoint("source", 4, "reader").unwrap().password,
        "preserved-reader"
    );
    let task = store.task("task").unwrap();
    assert!(!task.configuration_changed);
    assert!(task.auto_start);
    assert_eq!(task.runtime.checkpoint.unwrap().position, 42);
    assert!(store.delete_instance(1, "source").is_err());
    assert!(
        !store
            .db()
            .unwrap()
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
    let input:InstanceInput=serde_json::from_value(json!({"name":"pg","host":"127.0.0.1","port":5432,"kind":"postgresql","version":"15","database":"CDC_test"})).unwrap();
    store.save_instance(1, None, input).unwrap();
}

#[test]
fn instance_versions_database_names_and_permissions_are_validated() {
    let (_dir, store) = store();
    let actor = store
        .login("admin", PASSWORD, None)
        .unwrap()
        .session
        .user
        .id;
    for (kind, version, database) in [
        ("mysql", "15", ""),
        ("postgresql", "5.7", "CDC_test"),
        ("postgresql", "16", "CDC_test"),
        ("postgresql", "15", ""),
        ("postgresql", "15", "bad\0database"),
        ("unknown", "15", "CDC_test"),
    ] {
        let input=serde_json::from_value(json!({"name":"invalid","host":"127.0.0.1","port":5432,"kind":kind,"version":version,"database":database})).unwrap();
        assert!(matches!(
            store.save_instance(actor, None, input),
            Err(Error::Invalid(_))
        ));
    }
    // Omitted type/database retains the historical MySQL API behavior.
    let legacy: InstanceInput = serde_json::from_value(
        json!({"name":"legacy","host":"127.0.0.1","port":3306,"version":"5.7"}),
    )
    .unwrap();
    assert_eq!(
        store.save_instance(actor, None, legacy).unwrap().kind,
        "mysql"
    );
    let viewer = store
        .create_user(
            actor,
            NewUser {
                owner: "admin".into(),
                username: "viewer".into(),
                password: "long-test-password".into(),
                role: "viewer".into(),
                note: String::new(),
            },
        )
        .unwrap();
    let input=serde_json::from_value(json!({"name":"forbidden","host":"127.0.0.1","port":5432,"kind":"postgresql","version":"15","database":"CDC_test"})).unwrap();
    assert!(matches!(
        store.save_instance(viewer.id, None, input),
        Err(Error::Forbidden)
    ));
}

#[test]
fn postgres_instance_persists_multiple_selected_databases() {
    let (_dir, store) = store();
    let actor = store
        .login("admin", PASSWORD, None)
        .unwrap()
        .session
        .user
        .id;
    let input = serde_json::from_value(json!({
        "name": "pg15-multi",
        "host": "127.0.0.1",
        "port": 54321,
        "kind": "postgresql",
        "version": "15",
        "databases": ["orders", "CDC_test"],
        "reader_username": "reader",
        "reader_password": "reader-secret"
    }))
    .unwrap();
    let saved = store.save_instance(actor, None, input).unwrap();
    assert_eq!(saved.databases, vec!["CDC_test", "orders"]);
    assert_eq!(saved.database, "CDC_test");
    let persisted = serde_json::to_value(store.instance(&saved.id).unwrap()).unwrap();
    assert_eq!(persisted["databases"], json!(["CDC_test", "orders"]));
}

#[test]
#[ignore = "requires the authorized PostgreSQL 15 test instance; read-only"]
fn live_postgres_instance_probe_uses_reader_and_persists_metadata() {
    let (_dir, store) = store();
    let actor = store
        .login("admin", PASSWORD, None)
        .unwrap()
        .session
        .user
        .id;
    let password = std::env::var("PG_CDC_TEST_PASSWORD").expect("set PG_CDC_TEST_PASSWORD");
    let input = serde_json::from_value(
        json!({"name":"live-pg15","host":"192.168.0.10","port":54321,
        "kind":"postgresql","version":"15","database":"CDC_test",
        "reader_username":"postgresql_reader","reader_password":password}),
    )
    .unwrap();
    let saved = store.save_instance(actor, None, input).unwrap();
    let inspected = store.probe_instance(actor, &saved.id).unwrap();
    assert!(
        inspected.probe_error.is_none(),
        "{:?}",
        inspected.probe_error
    );
    let Some(crate::model::Metadata::Postgresql(meta)) = inspected.metadata else {
        panic!("expected PostgreSQL metadata")
    };
    assert!(meta.server_version.starts_with("15."));
    assert_eq!(meta.database, "CDC_test");
    assert_eq!(meta.wal_level, "logical");
    assert!(meta.can_replicate);
    assert!(store.instance(&saved.id).unwrap().metadata.is_some());
    println!(
        "PASS: PostgreSQL 15 reader connection, CDC_test, wal_level=logical, replication permission and persisted metadata"
    );
}

#[test]
#[ignore = "requires the authorized PostgreSQL 15 test instance; read-only"]
fn live_postgres_database_discovery_lists_connectable_databases() {
    let host = std::env::var("PG_CDC_HOST").expect("set PG_CDC_HOST");
    let port = std::env::var("PG_CDC_PORT")
        .expect("set PG_CDC_PORT")
        .parse()
        .unwrap();
    let user = std::env::var("PG_CDC_READER_USER").expect("set PG_CDC_READER_USER");
    let password = std::env::var("PG_CDC_TEST_PASSWORD").expect("set PG_CDC_TEST_PASSWORD");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let databases = runtime
        .block_on(postgresql_15::databases(&host, port, &user, &password))
        .unwrap();
    assert!(databases.iter().any(|name| name == "CDC_test"));
    assert!(!databases.is_empty());
    println!(
        "PASS: PostgreSQL database discovery returned {} connectable databases",
        databases.len()
    );
}

#[test]
#[ignore = "requires the authorized PostgreSQL 15 test instance; read-only"]
fn live_postgres_sink_catalog_uses_writer_for_configured_database() {
    let (_dir, store) = store();
    let actor = store
        .login("admin", PASSWORD, None)
        .unwrap()
        .session
        .user
        .id;
    let password = std::env::var("PG_CDC_TEST_PASSWORD").expect("set PG_CDC_TEST_PASSWORD");
    let writer = std::env::var("PG_CDC_WRITER_USER").unwrap_or_else(|_| "postgresql_writer".into());
    let input = serde_json::from_value(json!({
        "name": "live-pg15-sink-catalog",
        "host": std::env::var("PG_CDC_HOST").unwrap_or_else(|_| "192.168.0.10".into()),
        "port": std::env::var("PG_CDC_PORT").unwrap_or_else(|_| "54321".into()).parse::<u16>().unwrap(),
        "kind": "postgresql",
        "version": "15",
        "databases": ["CDC_test"],
        "writer_username": writer,
        "writer_password": password,
    }))
    .unwrap();
    let instance = store.save_instance(actor, None, input).unwrap();
    let catalog = store
        .catalog(
            actor,
            &instance.id,
            CatalogQuery {
                role: EndpointRole::Sink,
                schema: None,
                database: Some("CDC_test".into()),
            },
        )
        .unwrap();
    assert!(catalog.schemas.iter().any(|schema| schema == "public"));
    println!("PASS: PostgreSQL sink catalog loaded with writer account");
}
