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
use tracing_subscriber::fmt::format::Writer as FmtWriter;
use tracing_subscriber::fmt::{FormatEvent, FormatFields, FmtContext};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing::Event;

/// Simple mode: `LEVEL message field=value ...` — no span chains, no
/// targets, no UUID noise. The full detail still goes to SQLite.
struct SimpleFormat;

struct FieldString(String, bool);

impl tracing::field::Visit for FieldString {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0.push_str(&format!("{value:?}"));
            self.1 = true;
        } else {
            self.0.push_str(&format!("  {}={value:?}", field.name()));
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
            self.1 = true;
        } else {
            self.0.push_str(&format!("  {}={value}", field.name()));
        }
    }
}

impl<S, N> FormatEvent<S, N> for SimpleFormat
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: FmtWriter<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Clock time only: HH:MM:SS.
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let tod = secs.rem_euclid(86_400);
        write!(
            writer,
            "{:02}:{:02}:{:02}",
            tod / 3600,
            (tod % 3600) / 60,
            tod % 60
        )?;
        let level = event.metadata().level().to_string();
        write!(writer, " {level:<5} ")?;
        let mut v = FieldString(String::new(), false);
        event.record(&mut v);
        writeln!(writer, "{}", v.0)
    }
}

use std::fmt;

pub use db_layer::{spawn_trace_writer, DbLayer};

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
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
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(SimpleFormat)
                    .boxed(),
            )
            .init();
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
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(SimpleFormat)
                    .boxed(),
            )
            .init();
    }
}

/// Like [`init_with_db`] but safe to call multiple times in one process:
/// the first call wins, later calls return `None` (already initialized).
pub fn try_init_with_db(store: Arc<Store>) -> Option<tokio::task::JoinHandle<()>> {
    static INITED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    if INITED.get().is_some() {
        return None;
    }
    INITED.set(()).ok()?;
    Some(init_with_db(store))
}
