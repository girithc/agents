use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Uniform error type for handlers: maps to an HTTP status + JSON body
/// `{"error": "..."}`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into() }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: msg.into() }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into() }
    }
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::SERVICE_UNAVAILABLE, message: msg.into() }
    }
}

impl From<agenthub_storage::StorageError> for ApiError {
    fn from(e: agenthub_storage::StorageError) -> Self {
        match e {
            agenthub_storage::StorageError::SecretNotFound(name) => {
                Self::not_found(format!("secret '{name}' not found"))
            }
            other => Self::internal(other.to_string()),
        }
    }
}

impl From<agenthub_providers::ProviderError> for ApiError {
    fn from(e: agenthub_providers::ProviderError) -> Self {
        match e {
            agenthub_providers::ProviderError::Unknown(_, names) => {
                Self::bad_request(format!("unknown provider; registered: {names:?}"))
            }
            other => Self::bad_request(format!("provider error: {other}")),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
