use super::{TestDirectory, instance_input, request, response_json, store};
use crate::{
    Error, Store, WebConfig,
    catalog::{CatalogColumn, CatalogTable},
    model::NewUser,
    router,
    tasks::{TableMapping, TaskInput, validate_input, validate_pair, validate_selected_pair},
};
use axum::{
    body::Body,
    http::{StatusCode, header},
};
use serde_json::json;
use std::collections::BTreeMap;
use tower::ServiceExt;

fn mapping() -> TableMapping {
    TableMapping {
        source_schema: "CDC_test".into(),
        source_table: "orders".into(),
        sink_schema: "CDC_test".into(),
        sink_table: "orders".into(),
        columns: vec!["id".into()],
        conversion_options: BTreeMap::new(),
    }
}
fn input(source: &str, sink: &str) -> TaskInput {
    TaskInput {
        draft_id: None,
        name: "orders replication".into(),
        source_id: source.into(),
        sink_id: sink.into(),
        source_database: String::new(),
        sink_database: String::new(),
        source_revision: 1,
        sink_revision: 1,
        start_mode: "auto".into(),
        mappings: vec![mapping()],
        confirmations: vec![],
    }
}
fn table() -> CatalogTable {
    CatalogTable {
        schema: "CDC_test".into(),
        name: "orders".into(),
        engine: "InnoDB".into(),
        primary_key: vec!["id".into()],
        columns: vec![CatalogColumn {
            name: "id".into(),
            column_type: "int(11) unsigned".into(),
            nullable: false,
            extra: String::new(),
            collation: None,
            default_value: None,
        }],
    }
}

#[test]
fn mysql_char_columns_reach_the_common_compatibility_planner() {
    let mut source = table();
    source.columns.push(CatalogColumn {
        name: "fixed_name".into(),
        column_type: "char(10)".into(),
        nullable: false,
        extra: String::new(),
        collation: Some("utf8mb4_bin".into()),
        default_value: None,
    });
    let sink = source.clone();
    assert!(validate_pair(&source, &sink).is_ok());
}

#[test]
fn table_preflight_preserves_integer_semantics_and_keys() {
    let source = table();
    let mut sink = source.clone();
    sink.columns[0].column_type = "int unsigned".into();
    assert!(validate_pair(&source, &sink).is_ok());
    sink.columns[0].column_type = "int".into();
    assert!(validate_pair(&source, &sink).is_err());
    sink = source.clone();
    sink.primary_key.clear();
    assert!(validate_pair(&source, &sink).is_err());
    sink = source.clone();
    sink.engine = "MyISAM".into();
    assert!(validate_pair(&source, &sink).is_err());
    sink = source.clone();
    sink.columns[0].nullable = true;
    assert!(validate_pair(&source, &sink).is_err());
    sink = source.clone();
    sink.columns[0].collation = Some("utf8mb4_0900_ai_ci".into());
    assert!(validate_pair(&source, &sink).is_err());
}

#[test]
fn mysql_columns_map_to_postgresql_15_sink_types() {
    let mut source = table();
    source.columns.push(CatalogColumn {
        name: "message".into(),
        column_type: "varchar(255)".into(),
        nullable: false,
        extra: String::new(),
        collation: Some("utf8mb4_bin".into()),
        default_value: None,
    });
    source.columns.push(CatalogColumn {
        name: "metadata".into(),
        column_type: "json".into(),
        nullable: true,
        extra: String::new(),
        collation: None,
        default_value: None,
    });
    let mut sink = source.clone();
    sink.engine = "PostgreSQL".into();
    sink.columns[0].column_type = "bigint".into();
    sink.columns[1].column_type = "character varying(255)".into();
    sink.columns[1].collation = None;
    sink.columns[2].column_type = "jsonb".into();
    assert!(validate_pair(&source, &sink).is_ok());
    sink.columns[0].column_type = "integer".into();
    assert!(validate_pair(&source, &sink).is_err());
}

#[test]
fn task_validation_rejects_unusable_or_forged_mapping() {
    let mut task = input("source", "sink");
    assert!(validate_input(&task).is_ok());
    task.mappings.push(mapping());
    assert!(validate_input(&task).is_err());
    task.mappings.clear();
    assert!(validate_input(&task).is_err());
    task = input("same", "same");
    assert!(validate_input(&task).is_err());
    task = input("source", "sink");
    task.mappings[0].sink_table = "renamed".into();
    assert!(validate_input(&task).is_err());
    task = input("source", "sink");
    let mut second = mapping();
    second.source_schema = "audit".into();
    second.sink_schema = "audit".into();
    second.source_table = "events".into();
    second.sink_table = "events".into();
    task.mappings.push(second);
    assert!(validate_input(&task).is_ok());
    task = input("source", "sink");
    task.start_mode = "invalid".into();
    assert!(validate_input(&task).is_err());
    task = input("source", "sink");
    task.mappings[0].source_schema = "mysql".into();
    task.mappings[0].sink_schema = "mysql".into();
    assert!(validate_input(&task).is_err());
}

#[test]
fn tasks_persist_and_protect_instance_references_and_revisions() {
    let (dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let source = store.save_instance(admin, None, instance_input()).unwrap();
    let mut sink_input = instance_input();
    sink_input.name = "mysql-sink-84".into();
    sink_input.port = 33063;
    sink_input.version = "8.4".into();
    let sink = store.save_instance(admin, None, sink_input).unwrap();
    let task = store
        .insert_task(admin, input(&source.id, &sink.id))
        .unwrap();
    assert_eq!(task.status, "configured");
    assert!(!task.configuration_changed);
    assert_eq!(task.mappings, vec![mapping()]);
    assert!(matches!(
        store.insert_task(admin, input(&source.id, &sink.id)),
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        store.delete_instance(admin, &source.id),
        Err(Error::Conflict(_))
    ));
    store
        .save_instance(admin, Some(source.id.clone()), instance_input())
        .unwrap();
    assert!(store.task(&task.id).unwrap().configuration_changed);
    assert!(matches!(
        store.insert_task(admin, input(&source.id, &sink.id)),
        Err(Error::Conflict(_))
    ));
    drop(store);
    let store = Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap();
    assert_eq!(store.tasks().unwrap().len(), 1);
    assert_eq!(store.task(&task.id).unwrap().source_name, source.name);
    assert_eq!(store.task(&task.id).unwrap().start_mode, "auto");
}

#[test]
fn postgresql_source_can_be_persisted_with_each_mysql_sink_version() {
    let (_dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap().session.user.id;
    let mut source_input = instance_input();
    source_input.name = "postgresql-source-15".into();
    source_input.kind = "postgresql".into();
    source_input.version = "15".into();
    source_input.database = "CDC_test".into();
    source_input.port = 54315;
    let source = store.save_instance(admin, None, source_input).unwrap();

    for (version, port) in [("5.7", 33057), ("8.0", 33080), ("8.4", 33084)] {
        let mut sink_input = instance_input();
        sink_input.name = format!("mysql-sink-{version}");
        sink_input.version = version.into();
        sink_input.port = port;
        let sink = store.save_instance(admin, None, sink_input).unwrap();
        let mut task_input = input(&source.id, &sink.id);
        task_input.name = format!("orders replication {version}");
        let task = store.insert_task(admin, task_input).unwrap();
        assert_eq!(task.source_id, source.id);
        assert_eq!(task.sink_id, sink.id);
    }
}

#[test]
fn postgresql_source_columns_map_to_mysql_sink_types() {
    let mut source = table();
    source.engine = "PostgreSQL".into();
    source.columns[0].column_type = "integer".into();
    source.columns.push(CatalogColumn {
        name: "message".into(),
        column_type: "character varying(255)".into(),
        nullable: false,
        extra: String::new(),
        collation: None,
        default_value: None,
    });
    let mut sink = table();
    sink.columns[0].column_type = "int".into();
    sink.columns.push(CatalogColumn {
        name: "message".into(),
        column_type: "varchar(255)".into(),
        nullable: false,
        extra: String::new(),
        collation: Some("utf8mb4_bin".into()),
        default_value: None,
    });
    assert!(validate_pair(&source, &sink).is_ok());
}

#[test]
fn old_task_mapping_defaults_to_all_columns() {
    let mapping: TableMapping = serde_json::from_value(json!({
        "source_schema": "CDC_test",
        "source_table": "orders",
        "sink_schema": "CDC_test",
        "sink_table": "orders"
    }))
    .unwrap();
    assert!(mapping.columns.is_empty());
}

#[test]
fn selected_columns_require_primary_keys_and_safe_sink_defaults() {
    let mut source = table();
    source.columns.push(CatalogColumn {
        name: "message".into(),
        column_type: "varchar(255)".into(),
        nullable: false,
        extra: String::new(),
        collation: Some("utf8mb4_general_ci".into()),
        default_value: None,
    });
    let mut sink = source.clone();

    assert!(validate_selected_pair(&source, &sink, &["id".into()]).is_err());
    sink.columns[1].default_value = Some(String::new());
    assert!(validate_selected_pair(&source, &sink, &["id".into()]).is_ok());
    assert!(validate_selected_pair(&source, &sink, &["message".into()]).is_err());
    sink = source.clone();
    assert!(validate_selected_pair(&source, &sink, &["id".into(), "message".into()]).is_ok());
}

#[test]
fn version_three_migration_preserves_accounts_and_instances() {
    let dir = TestDirectory::new();
    let path = dir.path().join("web.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            owner TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            theme TEXT NOT NULL DEFAULT 'dark',
            note TEXT NOT NULL DEFAULT ''
         );
         CREATE TABLE secrets (name TEXT PRIMARY KEY, value BLOB NOT NULL);
         CREATE TABLE instances (
            id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
            host TEXT NOT NULL, port INTEGER NOT NULL,
            version TEXT NOT NULL,
            reader_username TEXT NOT NULL, reader_secret BLOB,
            writer_username TEXT NOT NULL, writer_secret BLOB,
            metadata_json TEXT, checked_at INTEGER, probe_error TEXT,
            revision INTEGER NOT NULL DEFAULT 1
         );
         INSERT INTO users(owner,username,password_hash,role,note)
         VALUES('owner','admin','unused','admin','preserved');
         PRAGMA user_version=3;",
    )
    .unwrap();
    drop(conn);
    let store = Store::open(path, [7; 32]).unwrap();
    assert_eq!(store.users(1).unwrap()[0].note, "preserved");
    assert!(store.tasks().unwrap().is_empty());
}

#[tokio::test]
async fn task_endpoints_enforce_auth_csrf_and_admin_role() {
    let (_dir, store) = store();
    let admin = store.login("admin", "admin", None).unwrap();
    let source = store
        .save_instance(admin.session.user.id, None, instance_input())
        .unwrap();
    let mut sink_input = instance_input();
    sink_input.name = "mysql-sink-84".into();
    sink_input.port = 33063;
    sink_input.version = "8.4".into();
    let sink = store
        .save_instance(admin.session.user.id, None, sink_input)
        .unwrap();
    let task = store
        .insert_task(admin.session.user.id, input(&source.id, &sink.id))
        .unwrap();
    store
        .create_user(
            admin.session.user.id,
            NewUser {
                owner: "admin".into(),
                username: "observer".into(),
                password: "observer-test-password".into(),
                role: "viewer".into(),
                note: String::new(),
            },
        )
        .unwrap();
    let viewer = store
        .login("observer", "observer-test-password", None)
        .unwrap();
    let app = router(
        store,
        WebConfig {
            origin: "http://127.0.0.1:8080".into(),
        },
    )
    .unwrap();
    for path in [
        "/api/tasks",
        "/api/tasks/missing",
        "/api/tasks/missing/logs",
        "/api/instances/missing/catalog?role=source",
        "/assets/tasks.js",
    ] {
        let response = app
            .clone()
            .oneshot(request("GET", path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let payload=json!({"name":"test","source_id":"source","sink_id":"sink","source_revision":1,"sink_revision":1,"start_mode":"auto","mappings":[{"source_schema":"CDC_test","source_table":"orders","sink_schema":"CDC_test","sink_table":"orders"}]}).to_string();
    for action in ["start", "stop"] {
        let path = format!("/api/tasks/{}/{action}", task.id);
        for (login, csrf, expected) in [
            (None, false, StatusCode::UNAUTHORIZED),
            (Some(&admin), false, StatusCode::FORBIDDEN),
            (Some(&viewer), true, StatusCode::FORBIDDEN),
        ] {
            let mut req = request("POST", &path).header(header::ORIGIN, "http://127.0.0.1:8080");
            if let Some(login) = login {
                req = req.header(header::COOKIE, format!("cdc_session={}", login.token));
                if csrf {
                    req = req.header("x-csrf-token", &login.session.csrf_token);
                }
            }
            assert_eq!(
                app.clone()
                    .oneshot(req.body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status(),
                expected
            );
        }
    }
    for (login, csrf, expected) in [
        (&admin, false, StatusCode::FORBIDDEN),
        (&viewer, true, StatusCode::FORBIDDEN),
    ] {
        let mut req = request("POST", "/api/tasks")
            .header(header::COOKIE, format!("cdc_session={}", login.token))
            .header(header::ORIGIN, "http://127.0.0.1:8080")
            .header(header::CONTENT_TYPE, "application/json");
        if csrf {
            req = req.header("x-csrf-token", &login.session.csrf_token);
        }
        assert_eq!(
            app.clone()
                .oneshot(req.body(Body::from(payload.clone())).unwrap())
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    let response = app
        .clone()
        .oneshot(
            request("GET", "/api/instances/missing/catalog?role=sink")
                .header(header::COOKIE, format!("cdc_session={}", viewer.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = app
        .clone()
        .oneshot(
            request("GET", "/api/tasks")
                .header(header::COOKIE, format!("cdc_session={}", viewer.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body.as_array().unwrap().len(), 1);
    assert_eq!(body[0]["id"], task.id);

    let response = app
        .oneshot(
            request("GET", &format!("/api/tasks/{}", task.id))
                .header(header::COOKIE, format!("cdc_session={}", viewer.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["source_name"], "mysql-source-57");
    assert_eq!(body["sink_name"], "mysql-sink-84");
    assert_eq!(body["mappings"][0]["source_table"], "orders");
}
