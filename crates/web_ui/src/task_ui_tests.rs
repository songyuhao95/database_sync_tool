use super::{TestDirectory, instance_input, request, response_json, store};
use crate::{
    Error, Store, WebConfig,
    model::NewUser,
    router,
    source_logs::SourceLogQuery,
    tasks::{ReplicationTask, TableMapping, TaskInput},
};
use axum::{
    body::Body,
    http::{StatusCode, header},
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::Arc,
};
use tower::ServiceExt;
pub(super) fn fixture() -> (TestDirectory, Arc<Store>, i64, ReplicationTask) {
    let (dir, store) = store();
    let actor = store.login("admin", "admin", None).unwrap().session.user.id;
    let source = store.save_instance(actor, None, instance_input()).unwrap();
    let mut config = instance_input();
    config.name = "sink".into();
    config.port = 33062;
    config.version = "8.0".into();
    let sink = store.save_instance(actor, None, config).unwrap();
    let task = store
        .insert_task(
            actor,
            TaskInput {
                name: "UI test task".into(),
                source_id: source.id,
                sink_id: sink.id,
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
            },
        )
        .unwrap();
    (dir, store, actor, task)
}

#[test]
fn task_tree_controls_keep_hidden_database_selector_and_error_text_visible() {
    let css = include_str!("../assets/style.css");
    assert!(css.contains(".task-tree-controls>label[hidden]{display:none!important}"));
    assert!(css.contains(".task-tree-controls{min-height:132px;height:auto;"));
    let tasks = include_str!("../assets/tasks.js");
    assert!(tasks.contains("function syncControlHeights()"));
    assert!(tasks.contains("checkbox.title=incompatibility"));
    assert!(css.contains(
        ".task-tree-controls .task-endpoint-meta,.task-tree-controls .task-error{min-height:18px;margin:0;font-size:11px;line-height:1.45;white-space:normal;overflow-wrap:anywhere;overflow:hidden;text-overflow:clip}"
    ));
    assert!(css.contains(".task-tree-controls .task-error{max-height:36px;overflow:auto}"));
}
#[test]
fn deleting_stopped_tasks_preserves_data_and_rejects_active_workers() {
    let (_dir, store, actor, task) = fixture();
    store
        .append_task_file(&task.id, "write.log", "audit log retained\n")
        .unwrap();
    store.begin_task(actor, &task.id).unwrap();
    assert!(matches!(
        store.delete_task(actor, &task.id),
        Err(Error::Conflict(_))
    ));
    store.finish_task(&task.id, None).unwrap();
    // Runtime state alone is insufficient: a worker may still be exiting.
    let (release, wait) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        wait.recv().unwrap();
    });
    store.workers.lock().unwrap().insert(
        task.id.clone(),
        crate::runtime::Worker {
            cancel: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            handle,
        },
    );
    assert!(matches!(
        store.delete_task(actor, &task.id),
        Err(Error::Conflict(_))
    ));
    release.send(()).unwrap();
    let worker = store.workers.lock().unwrap().remove(&task.id).unwrap();
    worker.handle.join().unwrap();
    store.delete_task(actor, &task.id).unwrap();
    assert!(store.tasks().unwrap().is_empty());
    assert!(matches!(store.task(&task.id), Err(Error::NotFound)));
    for table in ["task_runtime", "task_logs"] {
        assert_eq!(
            store
                .db()
                .unwrap()
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE task_id=?1"),
                    [&task.id],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }
    assert_eq!(
        fs::read_to_string(store.log_dir.join(&task.id).join("write.log")).unwrap(),
        "audit log retained\n"
    );
    // Deleting the task releases the instance references, without contacting MySQL.
    store.delete_instance(actor, &task.source_id).unwrap();
}
#[test]
fn source_log_tail_increment_utf8_partial_lines_and_reset() {
    let (_dir, store, _, task) = fixture();
    assert!(
        store
            .source_logs(&task.id, SourceLogQuery::default())
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(matches!(
        store.source_logs("missing", SourceLogQuery::default()),
        Err(Error::NotFound)
    ));
    let file = "mysql-5.7-binlog.log";
    let text = (1..=250)
        .map(|i| format!("源端事件 {i}\n"))
        .collect::<String>();
    store.append_task_file(&task.id, file, &text).unwrap();
    let page = store
        .source_logs(&task.id, SourceLogQuery::default())
        .unwrap();
    assert_eq!(page.entries.len(), 200);
    assert_eq!(page.entries[0].message, "源端事件 51");
    assert_eq!(page.next, text.len() as u64);
    let path = store.log_dir.join(&task.id).join(file);
    let partial = "下一条\n".as_bytes();
    let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
    writer.write_all(&partial[..2]).unwrap();
    writer.flush().unwrap();
    let query = || SourceLogQuery {
        after: page.next,
        generation: Some(page.generation.clone()),
    };
    let pending = store.source_logs(&task.id, query()).unwrap();
    assert!(pending.entries.is_empty());
    assert_eq!(pending.next, page.next);
    writer.write_all(&partial[2..]).unwrap();
    writer.flush().unwrap();
    drop(writer);
    let appended = store.source_logs(&task.id, query()).unwrap();
    assert_eq!(appended.entries.len(), 1);
    assert_eq!(appended.entries[0].message, "下一条");
    fs::write(path, "重新开始\n").unwrap();
    let reset = store.source_logs(&task.id, query()).unwrap();
    assert!(reset.reset);
    assert_eq!(reset.entries[0].message, "重新开始");
}
#[tokio::test]
async fn task_delete_and_source_logs_enforce_session_csrf_and_role() {
    let (_dir, store, actor, task) = fixture();
    let admin = store.login("admin", "admin", None).unwrap();
    store
        .create_user(
            actor,
            NewUser {
                owner: "test".into(),
                username: "task_observer".into(),
                password: "observer-test-password".into(),
                role: "viewer".into(),
                note: String::new(),
            },
        )
        .unwrap();
    let viewer = store
        .login("task_observer", "observer-test-password", None)
        .unwrap();
    store
        .append_task_file(
            &task.id,
            "mysql-5.7-binlog.log",
            "SOURCE TABLE_MAP CDC_test.orders\n",
        )
        .unwrap();
    let app = router(
        store.clone(),
        WebConfig {
            origin: "http://127.0.0.1:8080".into(),
        },
    )
    .unwrap();
    let path = format!("/api/tasks/{}", task.id);
    for (login, csrf, expected) in [
        (None, false, StatusCode::UNAUTHORIZED),
        (Some(&admin), false, StatusCode::FORBIDDEN),
        (Some(&viewer), true, StatusCode::FORBIDDEN),
    ] {
        let mut req = request("DELETE", &path).header(header::ORIGIN, "http://127.0.0.1:8080");
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
    let source_path = format!("{path}/source-logs");
    assert_eq!(
        app.clone()
            .oneshot(request("GET", &source_path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app
        .clone()
        .oneshot(
            request("GET", &source_path)
                .header(header::COOKIE, format!("cdc_session={}", viewer.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await["entries"][0]["message"],
        "SOURCE TABLE_MAP CDC_test.orders"
    );
    store.begin_task(actor, &task.id).unwrap();
    let delete = || {
        request("DELETE", &path)
            .header(header::ORIGIN, "http://127.0.0.1:8080")
            .header(header::COOKIE, format!("cdc_session={}", admin.token))
            .header("x-csrf-token", &admin.session.csrf_token)
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone().oneshot(delete()).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    store.finish_task(&task.id, None).unwrap();
    assert_eq!(
        app.clone().oneshot(delete()).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        app.clone().oneshot(delete()).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.oneshot(
            request("GET", &source_path)
                .header(header::COOKIE, format!("cdc_session={}", admin.token))
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::NOT_FOUND
    );
}
