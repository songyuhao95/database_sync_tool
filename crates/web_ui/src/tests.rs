use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    Store, WebConfig,
    error::Error,
    model::{InstanceInput, NewUser, UserUpdate},
    router,
};

const PASSWORD: &str = "admin";
#[path = "auto_start_tests.rs"]
mod auto_start_tests;
#[path = "registry_tests.rs"]
mod registry_tests;
#[path = "runtime_tests.rs"]
mod runtime_tests;
#[path = "task_plan_tests.rs"]
mod task_plan_tests;
#[path = "task_tests.rs"]
mod task_tests;
#[path = "task_ui_tests.rs"]
mod task_ui_tests;

#[test]
fn home_empty_state_uses_the_instance_editor_route() {
    let script = include_str!("../assets/app.js");
    assert!(script.contains("box.append(instanceAddLink('添加第一个实例'))"));
    assert!(script.contains("link.href='/instances/new'"));
    assert!(!script.contains("添加第一个实例');link.href='/add'"));
}

#[test]
fn settings_account_management_uses_table_and_edit_dialog() {
    let script = include_str!("../assets/app.js");
    assert!(script.contains("node('h2','','账号管理')"));
    assert!(script.contains("['所有者','账号','角色','备注','操作']"));
    assert!(script.contains("class=\"account-create-form\""));
    assert!(script.contains("备注（可留空）"));
    assert!(script.contains("class=\"btn primary\">确定"));
    assert!(script.contains("data-dialog-close>取消</button>"));
    assert!(script.contains("dialog.close();toast('账号已添加')"));
    assert!(script.contains("dialog.showModal()"));
    assert!(script.contains("class=\"input\" disabled"));
    assert!(script.contains("method:'DELETE'"));
    assert!(!script.contains("class=\"account-form\""));
    assert!(!script.contains("当前密码"));
    assert!(!script.contains("确定删除这个登录账号"));
}

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cdc-web-ui-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let expected_parent = std::env::temp_dir();
        if self.0.parent() == Some(expected_parent.as_path())
            && self
                .0
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("cdc-web-ui-test-"))
        {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn store() -> (TestDirectory, Arc<Store>) {
    let directory = TestDirectory::new();
    let store = Arc::new(Store::open(directory.path().join("web.sqlite"), [7; 32]).unwrap());
    store.bootstrap_default_admin().unwrap();
    (directory, store)
}

fn instance_input() -> InstanceInput {
    InstanceInput {
        name: "mysql-source-57".into(),
        host: "192.168.0.10".into(),
        port: 33061,
        kind: "mysql".into(),
        database: String::new(),
        databases: Vec::new(),
        version: "5.7".into(),
        reader_username: "mysql_reader".into(),
        reader_password: Some("reader-secret".into()),
        writer_username: "mysql_writer".into(),
        writer_password: Some("writer-secret".into()),
    }
}

#[test]
fn accounts_sessions_and_instance_secrets_are_persistent() {
    let (directory, store) = store();
    let admin = store.login("admin", PASSWORD, None).unwrap();
    assert_eq!(store.session(&admin.token).unwrap().user.username, "admin");

    let viewer = store
        .create_user(
            admin.session.user.id,
            NewUser {
                owner: "operations".into(),
                username: "operator".into(),
                password: "another-long-password".into(),
                role: "viewer".into(),
                note: "夜班值守账号".into(),
            },
        )
        .unwrap();
    assert_eq!(viewer.owner, "operations");
    assert_eq!(viewer.note, "夜班值守账号");
    let viewer_login = store
        .login("operator", "another-long-password", None)
        .unwrap();
    assert!(matches!(
        store.update_user(
            viewer.id,
            viewer.id,
            UserUpdate {
                owner: "operator".into(),
                password: None,
                note: String::new(),
            },
        ),
        Err(Error::Forbidden)
    ));
    let viewer = store
        .update_user(
            admin.session.user.id,
            viewer.id,
            UserUpdate {
                owner: "database-team".into(),
                password: Some("updated-long-password".into()),
                note: "生产同步观察员".into(),
            },
        )
        .unwrap();
    assert_eq!(viewer.owner, "database-team");
    assert_eq!(viewer.username, "operator");
    assert_eq!(viewer.role, "viewer");
    assert_eq!(viewer.note, "生产同步观察员");
    assert!(matches!(
        store.session(&viewer_login.token),
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        store.login("operator", "another-long-password", None),
        Err(Error::Unauthorized)
    ));
    assert!(
        store
            .login("operator", "updated-long-password", None)
            .is_ok()
    );
    assert!(matches!(
        store.save_instance(viewer.id, None, instance_input()),
        Err(Error::Forbidden)
    ));

    let instance = store
        .save_instance(admin.session.user.id, None, instance_input())
        .unwrap();
    assert!(instance.has_reader_password);
    assert!(instance.has_writer_password);
    let (reader, writer): (Vec<u8>, Vec<u8>) = store
        .db()
        .unwrap()
        .query_row(
            "SELECT reader_secret, writer_secret FROM instances WHERE id=?1",
            [&instance.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        !reader
            .windows(b"reader-secret".len())
            .any(|part| part == b"reader-secret")
    );
    assert!(
        !writer
            .windows(b"writer-secret".len())
            .any(|part| part == b"writer-secret")
    );

    assert!(matches!(
        store.delete_user(admin.session.user.id, admin.session.user.id),
        Err(Error::Conflict(_))
    ));
    store
        .change_password(admin.session.user.id, PASSWORD, "changed-admin-password")
        .unwrap();
    assert!(matches!(
        store.session(&admin.token),
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        store.login("admin", PASSWORD, None),
        Err(Error::Unauthorized)
    ));
    let changed = store
        .login("admin", "changed-admin-password", None)
        .unwrap();
    store.logout(changed.session.token_hash).unwrap();

    drop(store);
    let reopened = Store::open(directory.path().join("web.sqlite"), [7; 32]).unwrap();
    assert_eq!(reopened.instances().unwrap().len(), 1);
    assert!(
        reopened
            .login("operator", "updated-long-password", None)
            .is_ok()
    );
    assert!(matches!(
        Store::open(directory.path().join("web.sqlite"), [8; 32]),
        Err(Error::Internal)
    ));
}

#[test]
fn version_one_database_adds_owner_and_note_during_open() {
    let directory = TestDirectory::new();
    let path = directory.path().join("web.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            theme TEXT NOT NULL DEFAULT 'dark'
         );
         CREATE TABLE secrets (name TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE instances (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    host TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
    version TEXT NOT NULL CHECK(version IN ('5.7','8.0','8.4')),
    reader_username TEXT NOT NULL, reader_secret BLOB,
    writer_username TEXT NOT NULL, writer_secret BLOB,
    metadata_json TEXT, checked_at INTEGER, probe_error TEXT,
    revision INTEGER NOT NULL DEFAULT 1
);

         INSERT INTO users(username,password_hash,role) VALUES('admin','unused','admin');
         PRAGMA user_version=1;",
    )
    .unwrap();
    drop(conn);

    let store = Store::open(&path, [7; 32]).unwrap();
    let (owner, note, version): (String, String, i64) = store
        .db()
        .unwrap()
        .query_row(
            "SELECT owner, note, (SELECT user_version FROM pragma_user_version) FROM users WHERE username='admin'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(owner, "admin");
    assert!(note.is_empty());
    assert_eq!(version, 10);
}

#[test]
fn version_two_database_adds_empty_note_during_open() {
    let directory = TestDirectory::new();
    let path = directory.path().join("web.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            owner TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            theme TEXT NOT NULL DEFAULT 'dark'
         );
         CREATE TABLE secrets (name TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE instances (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    host TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
    version TEXT NOT NULL CHECK(version IN ('5.7','8.0','8.4')),
    reader_username TEXT NOT NULL, reader_secret BLOB,
    writer_username TEXT NOT NULL, writer_secret BLOB,
    metadata_json TEXT, checked_at INTEGER, probe_error TEXT,
    revision INTEGER NOT NULL DEFAULT 1
);

         INSERT INTO users(owner,username,password_hash,role) VALUES('admin','admin','unused','admin');
         PRAGMA user_version=2;",
    )
    .unwrap();
    drop(conn);

    let store = Store::open(&path, [7; 32]).unwrap();
    let (note, version): (String, i64) = store
        .db()
        .unwrap()
        .query_row(
            "SELECT note, (SELECT user_version FROM pragma_user_version) FROM users WHERE username='admin'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(note.is_empty());
    assert_eq!(version, 10);
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn request(method: &str, uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::HOST, "127.0.0.1:8080")
}

#[tokio::test]
async fn http_routes_require_session_and_csrf() {
    let (_directory, store) = store();
    let app = router(
        store,
        WebConfig {
            origin: "http://127.0.0.1:8080".into(),
        },
    )
    .unwrap();

    let response = app
        .clone()
        .oneshot(request("GET", "/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::LOCATION], "/login?next=/");

    let response = app
        .clone()
        .oneshot(
            request("GET", "/api/instances")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/auth/login")
                .header(header::ORIGIN, "http://evil.example")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"username":"admin","password":PASSWORD}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/auth/login")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"username":"admin","password":PASSWORD}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Strict"));
    assert!(!set_cookie.contains("Secure"));
    let cookie = set_cookie.split(';').next().unwrap().to_owned();
    let body = response_json(response).await;
    let csrf = body["csrf_token"].as_str().unwrap().to_owned();

    let payload = json!({
        "name":"mysql-source-57","host":"192.168.0.10","port":33061,
        "version":"5.7","reader_username":"mysql_reader",
        "reader_password":"reader-secret","writer_username":"mysql_writer",
        "writer_password":"writer-secret"
    })
    .to_string();
    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/instances")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/instances")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::COOKIE, &cookie)
                .header("x-csrf-token", &csrf)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let serialized = String::from_utf8(
        to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!serialized.contains("reader-secret"));
    assert!(!serialized.contains("writer-secret"));

    let response = app
        .clone()
        .oneshot(
            request("GET", "/api/instances")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await.as_array().unwrap().len(), 1);

    let response = app
        .clone()
        .oneshot(
            request("GET", "/api/connectors")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let connectors = response_json(response).await;
    assert_eq!(connectors["sources"].as_array().unwrap().len(), 6);
    assert_eq!(connectors["sinks"].as_array().unwrap().len(), 4);
    assert_eq!(connectors["sources"][3]["identity"]["kind"], "postgresql");
    assert_eq!(connectors["sources"][5]["identity"]["version"], "17");
    assert_eq!(connectors["sinks"][3]["identity"]["version"], "15");

    let response = app
        .clone()
        .oneshot(
            request("POST", "/api/auth/logout")
                .header(header::ORIGIN, "http://127.0.0.1:8080")
                .header(header::COOKIE, &cookie)
                .header("x-csrf-token", &csrf)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            request("GET", "/api/me")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[path = "instance_tests.rs"]
mod instance_tests;
