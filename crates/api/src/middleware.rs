//! Request-id middleware: every HTTP request gets a unique id (honors an
//! incoming `x-request-id`), echoed back in the response header, recorded
//! in the request's trace span, and made available to handlers — runs
//! created by a request store it, so run ↔ HTTP-request are correlatable
//! end to end.

use axum::extract::Request;
use axum::http::{header::HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Extractor for the current request's id.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl<S> axum::extract::FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .unwrap_or_else(|| RequestId("unattributed".into())))
    }
}

pub async fn middleware(mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let span = tracing::info_span!(
        "http.request",
        request_id = %request_id,
        http_method = %req.method(),
        http_path = req.uri().path(),
    );

    req.extensions_mut().insert(RequestId(request_id.clone()));

    let mut res = next.run(req).instrument(span).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        res.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    res
}

use tracing::Instrument;
