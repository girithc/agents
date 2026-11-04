//! HTTP API for the agent platform.
//!
//! Layering: `api` → `storage` (the loop engine lives in `runtime` and will
//! be wired into these routes as it grows). The web UI (later) will consume
//! the same routes.

pub mod error;
pub mod middleware;
pub mod routes;
pub mod state;

use std::path::PathBuf;
use std::sync::Arc;

use agenthub_storage::{Store, Vault};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

/// Configuration for [`serve`].
pub struct ServeConfig {
    pub host: String,
    pub port: u16,
    /// Directory holding `agenthub.db` and `master.key`.
    pub data_dir: PathBuf,
}

/// Build the full application state (storage).
pub fn build_state(data_dir: &PathBuf) -> anyhow::Result<Arc<AppState>> {
    std::fs::create_dir_all(data_dir)?;
    let vault = Vault::open(&data_dir.join("master.key"))?;
    let store = Arc::new(Store::open(&data_dir.join("agenthub.db"), vault)?);
    Ok(Arc::new(AppState::new(store)))
}

/// Build the axum router for the given state.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(routes::agents::router())
        .merge(routes::secrets::router())
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(middleware::middleware))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Bind and serve. Initialises tracing with DB persistence first.
pub async fn serve(cfg: ServeConfig) -> anyhow::Result<()> {
    let state = build_state(&cfg.data_dir)?;

    let _trace_guard = agenthub_telemetry::init_with_db(state.store.clone());

    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(
        %addr,
        data_dir = %cfg.data_dir.display(),
        "agenthub api listening"
    );
    axum::serve(listener, build_router(state)).await?;
    Ok(())
}
