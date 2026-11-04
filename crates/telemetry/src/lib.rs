//! Tracing setup: console output + a custom layer that persists every span
//! and event into SQLite, keyed by the `run_id` field when present.
//!
//! Usage:
//! ```ignore
//! let guard = agenthub_telemetry::init_with_db(store.clone());
//! ```
//! Any code that uses `tracing::info!`, `info_span!`, etc. is captured
//! automatically — API handlers, the agent runtime, and providers alike.

pub mod db_layer;

use std::sync::Arc;

use agenthub_storage::Store;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub use db_layer::{spawn_trace_writer, DbLayer};

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,agenthub=debug"))
}

fn json_logs_enabled() -> bool {
    std::env::var("AGENTHUB_LOG_JSON")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Initialise tracing with console output + DB persistence.
/// Returns a guard whose task drains trace messages for the process lifetime.
pub fn init_with_db(store: Arc<Store>) -> tokio::task::JoinHandle<()> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let writer = spawn_trace_writer(store, rx);
    let filter = env_filter();
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(DbLayer::new(tx));

    if json_logs_enabled() {
        registry
            .with(tracing_subscriber::fmt::layer().json().boxed())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer().boxed()).init();
    }
    writer
}

/// Console-only tracing (used by CLI commands that don't serve).
pub fn init_console() {
    let filter = env_filter();
    let registry = tracing_subscriber::registry().with(filter);
    if json_logs_enabled() {
        registry
            .with(tracing_subscriber::fmt::layer().json().boxed())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer().boxed()).init();
    }
}
