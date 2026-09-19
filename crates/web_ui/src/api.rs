use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::{
    Store,
    auth::{SESSION_SECONDS, Session},
    catalog::CatalogQuery,
    error::{Error, Result},
    frontend,
    model::{DatabaseDiscoveryInput, InstanceInput, NewUser, UserUpdate},
    tasks::TaskInput,
};

#[derive(Clone)]
pub struct WebConfig {
    /// Exact browser origin, for example `http://127.0.0.1:8080`.
    pub origin: String,
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    origin: String,
    authority: String,
    secure: bool,
    jobs: Arc<Semaphore>,
    passwords: Arc<Semaphore>,
}

impl AppState {
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::RateLimited)?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&store)
        })
        .await
        .map_err(|_| Error::Internal)?
    }
}

pub fn router(store: Arc<Store>, config: WebConfig) -> Result<Router> {
    let uri: Uri = config
        .origin
        .parse()
        .map_err(|_| Error::Invalid("Web origin 无效"))?;
    let scheme = uri
        .scheme_str()
        .ok_or(Error::Invalid("Web origin 需要 http 或 https"))?;
    if !["http", "https"].contains(&scheme) || uri.path() != "/" || uri.query().is_some() {
        return Err(Error::Invalid("Web origin 只能包含协议、主机与端口"));
    }
    let authority = uri
        .authority()
        .ok_or(Error::Invalid("Web origin 缺少主机"))?
        .as_str()
        .to_owned();
    if authority.contains('@') {
        return Err(Error::Invalid("Web origin 不能包含账号"));
    }
    let state = AppState {
        store,
        origin: config.origin.trim_end_matches('/').to_owned(),
        authority,
        secure: scheme == "https",
        jobs: Arc::new(Semaphore::new(16)),
        passwords: Arc::new(Semaphore::new(2)),
    };

    let private = Router::new()
        .route("/", get(frontend::app))
        .route("/tasks", get(frontend::app))
        .route("/tasks/{id}", get(frontend::app))
        .route("/add", get(frontend::app))
        .route("/instances/new", get(frontend::app))
        .route("/instances/{id}/edit", get(frontend::app))
        .route("/settings", get(frontend::app))
        .route("/assets/app.js", get(frontend::app_js))
        .route("/assets/tasks.js", get(frontend::tasks_js))
        .route("/api/me", get(me))
        .route("/api/me/theme", post(set_theme))
        .route("/api/me/password", post(change_password))
        .route("/api/auth/logout", post(logout))
        .route("/api/instances", get(instances).post(create_instance))
        .route("/api/connectors", get(connectors))
        .route(
            "/api/instances/{id}",
            get(instance).put(update_instance).delete(remove_instance),
        )
        .route("/api/instances/{id}/probe", post(probe_instance))
        .route(
            "/api/postgresql/databases",
            post(discover_postgresql_databases),
        )
        .route("/api/instances/{id}/catalog", get(catalog))
        .route("/api/tasks", get(tasks).post(create_task))
        .route("/api/tasks/{id}", get(task).delete(delete_task))
        .route("/api/tasks/{id}/requalify", post(requalify_task))
        .route("/api/tasks/{id}/preflight", post(requalify_task))
        .route(
            "/api/tasks/{id}/auto-start",
            axum::routing::put(set_task_auto_start),
        )
        .route("/api/tasks/{id}/start", post(start_task))
        .route("/api/tasks/{id}/stop", post(stop_task))
        .route("/api/tasks/{id}/logs", get(task_logs))
        .route("/api/tasks/{id}/source-logs", get(source_logs))
        .route("/api/users", get(users).post(create_user))
        .route("/api/users/{id}", delete(remove_user).put(update_user))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));

    Ok(Router::new()
        .merge(private)
        .route("/login", get(frontend::login))
        .route("/assets/login.js", get(frontend::login_js))
        .route("/assets/style.css", get(frontend::css))
        .route("/assets/icons.css", get(frontend::icons))
        .route("/assets/Phosphor.woff2", get(frontend::font))
        .route("/api/auth/login", post(login))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), security))
        .with_state(state))
}

fn cookie(headers: &HeaderMap) -> Option<String> {
    let mut found = None;
    for value in headers.get_all(header::COOKIE) {
        for part in value.to_str().ok()?.split(';') {
            if let Some(("cdc_session", value)) = part.trim().split_once('=') {
                if found.is_some() {
                    return None;
                }
                found = Some(value.to_owned());
            }
        }
    }
    found
}

fn session_cookie(token: &str, secure: bool, delete: bool) -> HeaderValue {
    format!(
        "cdc_session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        if delete { 0 } else { SESSION_SECONDS },
        if secure { "; Secure" } else { "" }
    )
    .parse()
    .expect("static cookie attributes are valid")
}

async fn security(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let cross_site = headers
        .get("sec-fetch-site")
        .is_some_and(|value| value == "cross-site");
    let bad_origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        != Some(state.origin.as_str());
    let unsafe_method = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let mut response = if host != Some(state.authority.as_str())
        || (unsafe_method && (bad_origin || cross_site))
    {
        Error::Forbidden.into_response()
    } else {
        next.run(request).await
    };
    for (name, value) in [
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        ("x-frame-options", "DENY"),
        (
            "content-security-policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}

async fn authenticate(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let Some(token) = cookie(request.headers()) else {
        return unauthenticated(&request);
    };
    let session = match state.run(move |store| store.session(&token)).await {
        Ok(session) => session,
        Err(Error::Unauthorized) => return unauthenticated(&request),
        Err(error) => return error.into_response(),
    };
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        let valid = request
            .headers()
            .get("x-csrf-token")
            .is_some_and(|value| bool::from(value.as_bytes().ct_eq(session.csrf_token.as_bytes())));
        if !valid {
            return Error::Forbidden.into_response();
        }
    }
    request.extensions_mut().insert(session);
    next.run(request).await
}

fn unauthenticated(request: &Request) -> Response {
    if request.uri().path().starts_with("/api/") || request.uri().path().starts_with("/assets/") {
        Error::Unauthorized.into_response()
    } else {
        Redirect::to(&format!("/login?next={}", request.uri().path())).into_response()
    }
}

async fn not_found() -> Response {
    Error::NotFound.into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginInput {
    username: String,
    password: String,
}

async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<LoginInput>,
) -> Result<Response> {
    let permit = state
        .passwords
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::RateLimited)?;
    let old = cookie(&headers);
    let login = state
        .run(move |store| {
            let _permit = permit;
            store.login(&input.username, &input.password, old.as_deref())
        })
        .await?;
    let mut response = Json(json!({
        "user": login.session.user,
        "csrf_token": login.session.csrf_token
    }))
    .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        session_cookie(&login.token, state.secure, false),
    );
    Ok(response)
}

async fn me(Extension(session): Extension<Session>) -> Json<serde_json::Value> {
    Json(json!({
        "user": session.user,
        "csrf_token": session.csrf_token,
        "version": env!("CARGO_PKG_VERSION"),
        "task_service_connected": true
    }))
}

async fn logout(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
) -> Result<Response> {
    state
        .run(move |store| store.logout(session.token_hash))
        .await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, session_cookie("", state.secure, true));
    Ok(response)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ThemeInput {
    theme: String,
}

async fn set_theme(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<ThemeInput>,
) -> Result<Json<crate::model::User>> {
    state
        .run(move |store| store.theme(session.user.id, &input.theme))
        .await
        .map(Json)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PasswordInput {
    old_password: String,
    new_password: String,
}

async fn change_password(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<PasswordInput>,
) -> Result<Response> {
    let permit = state
        .passwords
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::RateLimited)?;
    state
        .run(move |store| {
            let _permit = permit;
            store.change_password(session.user.id, &input.old_password, &input.new_password)
        })
        .await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, session_cookie("", state.secure, true));
    Ok(response)
}

async fn users(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
) -> Result<Json<Vec<crate::model::User>>> {
    state
        .run(move |store| store.users(session.user.id))
        .await
        .map(Json)
}

async fn create_user(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<NewUser>,
) -> Result<impl IntoResponse> {
    let permit = state
        .passwords
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::RateLimited)?;
    let user = state
        .run(move |store| {
            let _permit = permit;
            store.create_user(session.user.id, input)
        })
        .await?;
    Ok((StatusCode::CREATED, Json(user)))
}

async fn remove_user(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<i64>,
) -> Result<StatusCode> {
    state
        .run(move |store| store.delete_user(session.user.id, id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_user(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<i64>,
    Json(input): Json<UserUpdate>,
) -> Result<Response> {
    let actor = session.user.id;
    let password_changed = input.password.is_some();
    let permit = state
        .passwords
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::RateLimited)?;
    let user = state
        .run(move |store| {
            let _permit = permit;
            store.update_user(actor, id, input)
        })
        .await?;
    let mut response = Json(user).into_response();
    if password_changed && actor == id {
        response
            .headers_mut()
            .insert(header::SET_COOKIE, session_cookie("", state.secure, true));
    }
    Ok(response)
}

async fn instances(State(state): State<AppState>) -> Result<Json<Vec<crate::model::Instance>>> {
    state.run(|store| store.instances()).await.map(Json)
}
async fn connectors() -> Json<crate::registry::ConnectorCatalog> {
    Json(crate::registry::catalog())
}

async fn instance(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<crate::model::Instance>> {
    state.run(move |store| store.instance(&id)).await.map(Json)
}

async fn create_instance(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<InstanceInput>,
) -> Result<impl IntoResponse> {
    let instance = state
        .run(move |store| store.save_instance(session.user.id, None, input))
        .await?;
    Ok((StatusCode::CREATED, Json(instance)))
}

async fn update_instance(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
    Json(input): Json<InstanceInput>,
) -> Result<Json<crate::model::Instance>> {
    state
        .run(move |store| store.save_instance(session.user.id, Some(id), input))
        .await
        .map(Json)
}

async fn remove_instance(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
) -> Result<StatusCode> {
    state
        .run(move |store| store.delete_instance(session.user.id, &id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn probe_instance(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
) -> Result<Json<crate::model::Instance>> {
    state
        .run(move |store| store.probe_instance(session.user.id, &id))
        .await
        .map(Json)
}

async fn discover_postgresql_databases(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<DatabaseDiscoveryInput>,
) -> Result<Json<Vec<String>>> {
    state
        .run(move |store| store.discover_postgresql_databases(session.user.id, input))
        .await
        .map(Json)
}

async fn catalog(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
    Query(query): Query<CatalogQuery>,
) -> Result<Json<crate::catalog::Catalog>> {
    state
        .run(move |store| store.catalog(session.user.id, &id, query))
        .await
        .map(Json)
}
async fn tasks(State(state): State<AppState>) -> Result<Json<Vec<crate::tasks::ReplicationTask>>> {
    state.run(|store| store.tasks()).await.map(Json)
}
async fn task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<crate::tasks::ReplicationTask>> {
    state.run(move |store| store.task(&id)).await.map(Json)
}
async fn create_task(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Json(input): Json<TaskInput>,
) -> Result<impl IntoResponse> {
    let task = state
        .run(move |store| store.create_task(session.user.id, input))
        .await?;
    Ok((StatusCode::CREATED, Json(task)))
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RequalifyInput {
    #[serde(default)]
    confirmations: Vec<change_event::RiskConfirmation>,
}

async fn requalify_task(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
    Json(input): Json<RequalifyInput>,
) -> Result<Json<crate::tasks::ReplicationTask>> {
    state
        .run(move |store| store.requalify_task(session.user.id, &id, input.confirmations))
        .await
        .map(Json)
}

async fn start_task(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
) -> Result<Json<crate::tasks::ReplicationTask>> {
    let store = state.store.clone();
    state
        .run(move |_| store.start_task(session.user.id, id))
        .await
        .map(Json)
}
async fn stop_task(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
) -> Result<Json<crate::tasks::ReplicationTask>> {
    state
        .run(move |store| store.stop_task(session.user.id, &id))
        .await
        .map(Json)
}
#[derive(Deserialize, Default)]
struct LogQuery {
    #[serde(default)]
    after: i64,
}
async fn task_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<LogQuery>,
) -> Result<Json<Vec<crate::runtime_store::TaskLog>>> {
    state
        .run(move |store| store.task_logs(&id, query.after))
        .await
        .map(Json)
}

async fn delete_task(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
) -> Result<StatusCode> {
    state
        .run(move |store| store.delete_task(session.user.id, &id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn source_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<crate::source_logs::SourceLogQuery>,
) -> Result<Json<crate::source_logs::SourceLogPage>> {
    state
        .run(move |store| store.source_logs(&id, query))
        .await
        .map(Json)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AutoStartInput {
    enabled: bool,
}
async fn set_task_auto_start(
    State(state): State<AppState>,
    Extension(session): Extension<Session>,
    Path(id): Path<String>,
    Json(input): Json<AutoStartInput>,
) -> Result<Json<crate::tasks::ReplicationTask>> {
    state
        .run(move |store| store.set_task_auto_start(session.user.id, &id, input.enabled))
        .await
        .map(Json)
}
