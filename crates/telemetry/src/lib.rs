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

/// Governance mode: `HH:MM:SS agent[model] did-what[why]` — no INFO/DEBUG
/// noise, no field dumps. WARN/ERROR keep a level tag. Everything dropped
/// here (kind, full fields) still goes to SQLite verbatim.
struct SimpleFormat;

const THOUGHT_MAX: usize = 160;

/// Collected event fields for one console line. `message`, `agent`, `model`
/// and `thought` shape the line; `kind` is structural (SQLite only); the
/// rest prints only on fallback lines and warnings.
#[derive(Default)]
struct LineParts {
    message: Option<String>,
    agent: Option<String>,
    model: Option<String>,
    thought: Option<String>,
    kind: Option<String>,
    rest: Vec<(String, String)>,
}

impl LineParts {
    fn put(&mut self, name: &str, value: String) {
        match name {
            "message" => self.message = Some(value),
            "agent" => self.agent = Some(value),
            "model" => self.model = Some(value),
            "thought" => self.thought = Some(value),
            "kind" => self.kind = Some(value),
            other => self.rest.push((other.to_string(), value)),
        }
    }

    fn excerpt(s: &str) -> String {
        if s.chars().count() > THOUGHT_MAX {
            format!("{}…", s.chars().take(THOUGHT_MAX).collect::<String>())
        } else {
            s.to_string()
        }
    }
}

impl tracing::field::Visit for LineParts {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.put(field.name(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.put(field.name(), value.to_string());
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
        let level = event.metadata().level();
        let severe = matches!(*level, tracing::Level::ERROR | tracing::Level::WARN);
        let mut p = LineParts::default();
        event.record(&mut p);

        // Full model I/O lives in SQLite only — never on the console.
        if p.kind.as_deref() == Some("model_seen") || p.kind.as_deref() == Some("model_said") {
            return Ok(());
        }

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

        // Correlation id: binds every line from one think.
        let tag = p.rest.iter().find(|(k, _)| k == "t").map(|(_, v)| v.clone()).unwrap_or_default();
        match (&p.agent, &p.model) {
            (Some(agent), _) => {
                if tag.is_empty() {
                    write!(writer, " {agent}")?;
                } else {
                    write!(writer, " {agent}[{tag}]")?;
                }
                if let Some(m) = &p.message {
                    write!(writer, " {}", LineParts::excerpt(m))?;
                }
                if severe {
                    write!(writer, " [{level}]")?;
                }
            }
            _ => {
                // Non-agent events: keep the old shape, minus INFO/DEBUG.
                if severe {
                    write!(writer, " {level}")?;
                }
                if let Some(m) = &p.message {
                    write!(writer, " {m}")?;
                }
                for (k, v) in &p.rest {
                    write!(writer, "  {k}={v}")?;
                }
            }
        }
        writeln!(writer)
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
