use super::{request, response_json, task_ui_tests::fixture};
use crate::{Store, WebConfig, model::NewUser, router, tasks::TaskInput};
use axum::{
    body::Body,
    http::{StatusCode, header},
};
use rusqlite::params;
use std::sync::Arc;
use tower::ServiceExt;

const CHECKPOINT: &str = r#"{"source_uuid":"source","sink_uuid":"sink","mode":"binlog","file":"mysql-bin.000001","position":456,"gtid_set":null}"#;

#[test]
fn auto_start_migration_persistence_and_stop_preserve_checkpoint() {
    let (dir, store, actor, task) = fixture();
    assert!(!task.auto_start);
    store.begin_task(actor, &task.id).unwrap();
    store
        .db()
        .unwrap()
        .execute(
            "UPDATE task_runtime SET checkpoint_json=?2 WHERE task_id=?1",
            params![task.id, CHECKPOINT],
        )
        .unwrap();
    // Recreate a real version-5 database with a task and progress, then upgrade.
    store
        .db()
        .unwrap()
        .execute_batch(
            "ALTER TABLE replication_tasks DROP COLUMN auto_start; PRAGMA user_version=5;",
        )
        .unwrap();
    drop(store);
    let store = Arc::new(Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap());
    let task = store.task(&task.id).unwrap();
    assert!(!task.auto_start);
    assert_eq!(task.status, "stopped");
    assert_eq!(task.runtime.checkpoint.unwrap().position, 456);
    let saved = store.set_task_auto_start(actor, &task.id, true).unwrap();
    assert!(saved.auto_start);
    assert!(!saved.configuration_changed);
    assert!(store.workers.lock().unwrap().is_empty());
    // Saving a setting and pressing Stop do not dispatch or erase the preference.
    assert!(store.stop_task(actor, &task.id).unwrap().auto_start);
    drop(store);
    let store = Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap();
    assert!(store.task(&task.id).unwrap().auto_start);
    assert!(store.tasks().unwrap()[0].auto_start);
    assert!(
        !store
            .set_task_auto_start(actor, &task.id, false)
            .unwrap()
            .auto_start
    );
    let checkpoint: String = store
        .db()
        .unwrap()
        .query_row(
            "SELECT checkpoint_json FROM task_runtime WHERE task_id=?1",
            [task.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(checkpoint, CHECKPOINT);
}

#[test]
fn startup_dispatches_only_enabled_tasks_and_isolates_failures() {
    let (_dir, store, actor, changed) = fixture();
    let input = |name: &str| TaskInput {
        name: name.into(),
        source_id: changed.source_id.clone(),
        sink_id: changed.sink_id.clone(),
        source_database: String::new(),
        sink_database: String::new(),
        source_revision: 1,
        sink_revision: 1,
        start_mode: "auto".into(),
        mappings: changed.mappings.clone(),
        confirmations: vec![],
    };
    let enabled = store
        .insert_task(actor, input("Enabled startup test"))
        .unwrap();
    let manual = store
        .insert_task(actor, input("Manual startup test"))
        .unwrap();
    store.begin_task(actor, &changed.id).unwrap();
    store.finish_task(&changed.id, None).unwrap();
    {
        let conn = store.db().unwrap();
        // The dispatched worker fails before connecting: no real MySQL is touched.
        conn.execute(
            "UPDATE instances SET reader_secret=NULL WHERE id=?1",
            [&changed.source_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE replication_tasks SET auto_start=1,source_revision=0,created_at=0 WHERE id=?1",
            [&changed.id],
        )
        .unwrap();
        conn.execute(
            "UPDATE task_runtime SET checkpoint_json=?2 WHERE task_id=?1",
            params![changed.id, CHECKPOINT],
        )
        .unwrap();
    }
    store.set_task_auto_start(actor, &enabled.id, true).unwrap();
    assert_eq!(store.start_auto_tasks().unwrap(), 0);
    store.shutdown_tasks();
    let failed = store.task(&changed.id).unwrap();
    assert_eq!(failed.status, "failed");
    assert!(
        failed
            .runtime
            .last_error
            .unwrap()
            .contains("自动开始任务失败")
    );
    assert_eq!(failed.runtime.checkpoint.unwrap().position, 456);
    assert_eq!(store.task(&enabled.id).unwrap().status, "failed");
    assert!(!store.task_logs(&enabled.id, 0).unwrap().is_empty());
    assert_eq!(store.task(&manual.id).unwrap().status, "configured");
    assert!(store.task_logs(&manual.id, 0).unwrap().is_empty());
    store
        .set_task_auto_start(actor, &enabled.id, false)
        .unwrap();
    store
        .set_task_auto_start(actor, &changed.id, false)
        .unwrap();
    assert_eq!(store.start_auto_tasks().unwrap(), 0);
}

#[tokio::test]
async fn auto_start_endpoint_requires_admin_session_csrf_and_boolean() {
    let (_dir, store, actor, task) = fixture();
    let admin = store.login("admin", "admin", None).unwrap();
    store
        .create_user(
            actor,
            NewUser {
                owner: "test".into(),
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
        store.clone(),
        WebConfig {
            origin: "http://127.0.0.1:8080".into(),
        },
    )
    .unwrap();
    let path = format!("/api/tasks/{}/auto-start", task.id);
    for (login, csrf, body, expected) in [
        (None, false, r#"{"enabled":true}"#, StatusCode::UNAUTHORIZED),
        (
            Some(&admin),
            false,
            r#"{"enabled":true}"#,
            StatusCode::FORBIDDEN,
        ),
        (
            Some(&viewer),
            true,
            r#"{"enabled":true}"#,
            StatusCode::FORBIDDEN,
        ),
        (
            Some(&admin),
            true,
            r#"{"enabled":"true"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            Some(&admin),
            true,
            r#"{}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let mut req = request("PUT", &path)
            .header(header::ORIGIN, "http://127.0.0.1:8080")
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(login) = login {
            req = req.header(header::COOKIE, format!("cdc_session={}", login.token));
            if csrf {
                req = req.header("x-csrf-token", &login.session.csrf_token);
            }
        }
        assert_eq!(
            app.clone()
                .oneshot(req.body(Body::from(body)).unwrap())
                .await
                .unwrap()
                .status(),
            expected
        );
        assert!(!store.task(&task.id).unwrap().auto_start);
    }
    for enabled in [true, false] {
        let response = app
            .clone()
            .oneshot(
                request("PUT", &path)
                    .header(header::ORIGIN, "http://127.0.0.1:8080")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::COOKIE, format!("cdc_session={}", admin.token))
                    .header("x-csrf-token", &admin.session.csrf_token)
                    .body(Body::from(
                        serde_json::json!({"enabled":enabled}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["auto_start"], enabled);
    }
    let response = app
        .oneshot(
            request("PUT", "/api/tasks/missing/auto-start")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::COOKIE, format!("cdc_session={}", admin.token))
                .header("x-csrf-token", &admin.session.csrf_token)
                .body(Body::from(r#"{"enabled":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
