use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Validation(String),
    /// The route reached a durable source boundary but cannot continue
    /// without an operator changing its saved plan or resolving an unknown
    /// commit outcome.
    Blocked(String),
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict(&'static str),
    RateLimited,
    Internal,
}
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn message(&self) -> &str {
        match self {
            Self::Invalid(m) | Self::Conflict(m) => m,
            Self::Validation(m) | Self::Blocked(m) => m,
            Self::Unauthorized => "请重新登录，或检查账号密码",
            Self::Forbidden => "没有操作权限，或请求校验失败",
            Self::NotFound => "记录不存在",
            Self::RateLimited => "请求过于频繁，请稍后重试",
            Self::Internal => "服务暂时不可用，请检查服务端配置",
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for Error {}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_input"),
            Self::Validation(_) => (StatusCode::BAD_REQUEST, "invalid_input"),
            Self::Blocked(_) => (StatusCode::CONFLICT, "blocked_route"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        let mut response = (
            status,
            Json(json!({"error":{"code":code,"message":self.message()}})),
        )
            .into_response();
        if status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert("retry-after", "60".parse().unwrap());
        }
        response
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        if let rusqlite::Error::SqliteFailure(ref code, _) = e
            && code.code == rusqlite::ErrorCode::ConstraintViolation
        {
            return Self::Conflict("名称重复，或记录仍有关联数据");
        }
        eprintln!("[web] SQLite operation failed");
        Self::Internal
    }
}
