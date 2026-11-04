//! A `tracing` layer that captures spans and events into SQLite.
//!
//! Spans carrying a `run_id` field are attributed to that agent run, so
//! `GET /v1/runs/{id}/traces` returns the full execution tree for a run.
//! Spans/events without a `run_id` (e.g. plain HTTP middleware) are still
//! persisted with a null run id for global inspection.
//!
//! The layer never blocks the caller: messages are pushed onto an
//! unbounded channel and written by a background task in batches.

use std::sync::{Arc, Mutex};

use agenthub_storage::store::traces::TraceMsg;
use agenthub_storage::{now_ms, Store};
use serde_json::{Map, Value};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::field::{Field, Visit};
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

pub struct DbLayer {
    tx: UnboundedSender<TraceMsg>,
    /// Unique per process lifetime; prefix for every span id so rows from
    /// different server restarts never collide in SQLite.
    boot_id: String,
}

impl DbLayer {
    pub fn new(tx: UnboundedSender<TraceMsg>) -> Self {
        Self {
            tx,
            boot_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    fn sid(&self, id: &Id) -> String {
        format!("{}:{}", self.boot_id, id.into_u64())
    }
}

/// Fields collected from span/event attributes.
#[derive(Debug, Default)]
struct FieldVisitor {
    map: Map<String, Value>,
}

impl FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.map.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_debug(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.map
            .insert(field.name().to_string(), Value::String(value.into()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.map
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.map
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.map
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.map
            .insert(field.name().to_string(), Value::from(value));
    }
}

#[derive(Clone)]
struct SpanMeta {
    id: String,
    start_ts: i64,
    run_id: Option<String>,
    fields: Arc<Mutex<Map<String, Value>>>,
}

fn fields_to_json(map: &Map<String, Value>) -> Value {
    Value::Object(map.clone())
}

impl<S> Layer<S> for DbLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &Id,
        ctx: Context<'_, S>,
    ) {
        let mut vis = FieldVisitor::default();
        attrs.record(&mut vis);
        let own_run_id = vis
            .map
            .get("run_id")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_matches('"').to_string());

        let span = match ctx.span(id) {
            Some(s) => s,
            None => return,
        };
        // Inherit run_id + capture parent linkage from the parent span.
        let parent_meta = span
            .parent()
            .and_then(|p| p.extensions().get::<SpanMeta>().cloned());
        let parent_id = parent_meta
            .as_ref()
            .map(|m| m.id.clone())
            .or_else(|| span.parent().map(|p| self.sid(&p.id())));
        let run_id = own_run_id.or_else(|| parent_meta.as_ref().and_then(|m| m.run_id.clone()));

        let meta = SpanMeta {
            id: self.sid(id),
            start_ts: now_ms(),
            run_id: run_id.clone(),
            fields: Arc::new(Mutex::new(vis.map.clone())),
        };
        span.extensions_mut().insert(meta.clone());

        let _ = self.tx.send(TraceMsg::SpanStart {
            span_id: meta.id.clone(),
            parent_id,
            run_id,
            name: span.name().to_string(),
            target: attrs.metadata().target().to_string(),
            start_ts: meta.start_ts,
            fields: fields_to_json(&vis.map),
        });
    }

    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let Some(meta) = span.extensions().get::<SpanMeta>().cloned() else {
            return;
        };
        let mut vis = FieldVisitor::default();
        values.record(&mut vis);
        let mut fields = meta.fields.lock().expect("span fields mutex");
        for (k, v) in vis.map {
            fields.insert(k, v);
        }
        let snapshot = fields.clone();
        drop(fields);
        let _ = self.tx.send(TraceMsg::SpanFields {
            span_id: meta.id,
            fields: fields_to_json(&snapshot),
        });
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut vis = FieldVisitor::default();
        event.record(&mut vis);
        let message = vis
            .map
            .get("message")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();

        // Resolve the current span via the registry's span stack (NOT via
        // `tracing::Span::current()`, which is blocked by the dispatcher's
        // re-entrancy guard inside `on_event`). That gives us both the span
        // id and, transitively, the run id for attribution.
        let mut span_id = None;
        let mut run_id = None;
        if let Some(id) = ctx.current_span().id().cloned() {
            if let Some(span) = ctx.span(&id) {
                if let Some(meta) = span.extensions().get::<SpanMeta>().cloned() {
                    // Walk up parents to inherit a run_id if this span
                    // doesn't carry one itself.
                    run_id = meta.run_id.clone().or_else(|| {
                        find_run_id_upwards::<S>(
                            &ctx,
                            span.parent().map(|p| p.id()),
                        )
                    });
                    span_id = Some(meta.id);
                }
            }
        }

        let _ = self.tx.send(TraceMsg::Event {
            span_id,
            run_id,
            message,
            fields: fields_to_json(&vis.map),
            ts: now_ms(),
        });
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(meta) = span.extensions().get::<SpanMeta>().cloned() else {
            return;
        };
        let fields = meta
            .fields
            .lock()
            .map(|f| f.clone())
            .unwrap_or_default();
        let _ = self.tx.send(TraceMsg::SpanEnd {
            span_id: meta.id,
            end_ts: now_ms(),
            fields: fields_to_json(&fields),
        });
    }
}

fn find_run_id_upwards<S>(ctx: &Context<'_, S>, parent: Option<Id>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut current = parent;
    while let Some(id) = current {
        let span = ctx.span(&id)?;
        if let Some(meta) = span.extensions().get::<SpanMeta>() {
            if let Some(run_id) = &meta.run_id {
                return Some(run_id.clone());
            }
        }
        current = span.parent().map(|p| p.id());
    }
    None
}

/// Spawn the background writer that drains trace messages into storage.
pub fn spawn_trace_writer(
    store: Arc<Store>,
    mut rx: UnboundedReceiver<TraceMsg>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let first = match rx.recv().await {
                Some(m) => m,
                None => break,
            };
            let mut batch = vec![first];
            while batch.len() < 256 {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            store.apply_trace_batch(&batch).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agenthub_storage::Vault;
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn spans_and_events_persist_with_run_id() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(&dir.path().join("master.key")).unwrap();
        let store = Arc::new(
            Store::open(&dir.path().join("db.sqlite"), vault).unwrap(),
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let writer = spawn_trace_writer(store.clone(), rx);
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(DbLayer::new(tx)),
        );

        {
            let parent = tracing::info_span!("test.parent", run_id = "run-123", agent_id = "a1");
            let _p = parent.enter();
            tracing::info!("top level event");
            {
                let child = tracing::info_span!("test.child");
                let _c = child.enter();
                // Nested event inherits run_id from the parent span.
                tracing::info!("nested event");
            }
        }

        drop(_guard); // close the trace channel
        writer.await.unwrap(); // drain everything deterministically

        let spans = store.list_spans("run-123").await.unwrap();
        assert_eq!(spans.len(), 2, "spans: {spans:?}");
        assert_eq!(spans[0].name, "test.parent");
        assert_eq!(spans[1].name, "test.child");
        assert_eq!(spans[1].parent_id.as_deref(), Some(spans[0].span_id.as_str()));
        assert!(spans[0].end_ts.is_some(), "closed spans have end_ts");

        let events = store.list_events("run-123").await.unwrap();
        let messages: Vec<&str> = events.iter().map(|e| e.message.as_str()).collect();
        assert!(messages.contains(&"top level event"), "events: {messages:?}");
        assert!(messages.contains(&"nested event"), "events: {messages:?}");
    }
}
