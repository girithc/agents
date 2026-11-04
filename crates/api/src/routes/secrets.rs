//! Secret management endpoints. Values are encrypted at rest and are
//! masked on read unless `?reveal=true` is passed explicitly.

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

pub fn router() -> Router<std::sync::Arc<AppState>> {
    Router::new()
        .route("/v1/secrets", get(list_secrets))
        .route(
            "/v1/secrets/{name}",
            get(get_secret).put(put_secret).delete(delete_secret),
        )
}

#[derive(Deserialize)]
pub struct PutSecretBody {
    pub value: String,
}

async fn put_secret(
    State(st): State<std::sync::Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<PutSecretBody>,
) -> ApiResult<Json<serde_json::Value>> {
    if body.value.is_empty() {
        return Err(ApiError::bad_request("value must not be empty"));
    }
    st.store.put_secret(&name, &body.value).await?;
    tracing::info!(secret = %name, "secret stored (value never logged)");
    Ok(Json(json!({ "stored": name })))
}

async fn list_secrets(State(st): State<std::sync::Arc<AppState>>) -> ApiResult<Json<serde_json::Value>> {
    let secrets = st.store.list_secrets().await?;
    Ok(Json(json!({ "secrets": secrets, "count": secrets.len() })))
}

#[derive(Deserialize, Default)]
pub struct GetSecretQuery {
    pub reveal: Option<bool>,
}

#[derive(Serialize)]
struct SecretValue {
    name: String,
    value: String,
}

async fn get_secret(
    State(st): State<std::sync::Arc<AppState>>,
    Path(name): Path<String>,
    Query(q): Query<GetSecretQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let value = st.store.get_secret(&name).await?;
    if q.reveal.unwrap_or(false) {
        tracing::warn!(secret = %name, "secret value revealed over API");
        return Ok(Json(json!(SecretValue { name, value })));
    }
    Ok(Json(json!({ "name": name, "value": "********" })))
}

async fn delete_secret(
    State(st): State<std::sync::Arc<AppState>>,
    Path(name): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    if !st.store.delete_secret(&name).await? {
        return Err(ApiError::not_found(format!("secret '{name}' not found")));
    }
    tracing::info!(secret = %name, "secret deleted");
    Ok(Json(json!({ "deleted": name })))
}
