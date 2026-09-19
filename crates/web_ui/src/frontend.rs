use axum::{
    http::header,
    response::{Html, IntoResponse},
};

pub(crate) async fn login() -> Html<&'static str> {
    Html(include_str!("../assets/login.html"))
}
pub(crate) async fn app() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}
pub(crate) async fn login_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../assets/login.js"),
    )
}
pub(crate) async fn app_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../assets/app.js"),
    )
}
pub(crate) async fn tasks_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../assets/tasks.js"),
    )
}
pub(crate) async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../assets/style.css"),
    )
}
pub(crate) async fn icons() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../assets/icons.css"),
    )
}
pub(crate) async fn font() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "font/woff2")],
        include_bytes!("../assets/Phosphor.woff2").as_slice(),
    )
}
