use rusqlite::params;

use super::Store;
use crate::error::Result;
use crate::models::{TraceEvent, TraceSpan};

/// A message sent from the tracing layer into the background trace writer.
/// Defined here (storage) so telemetry can depend on storage, and never the
/// other way around.
///
/// `span_id` is `{boot_id}:{raw_id}` — unique across server restarts, so
/// spans from an old process are never clobbered by a new one.
#[derive(Debug, Clone)]
pub enum TraceMsg {
    SpanStart {
        span_id: String,
        parent_id: Option<String>,
        run_id: Option<String>,
        name: String,
        target: String,
        start_ts: i64,
        fields: serde_json::Value,
    },
    SpanFields {
        span_id: String,
        fields: serde_json::Value,
    },
    SpanEnd {
        span_id: String,
        end_ts: i64,
        fields: serde_json::Value,
    },
    Event {
        span_id: Option<String>,
        run_id: Option<String>,
        message: String,
        fields: serde_json::Value,
        ts: i64,
    },
}

#[derive(Debug, Clone)]
pub struct SpanRow {
    pub span_id: i64,
    pub parent_id: Option<i64>,
    pub run_id: Option<String>,
    pub name: String,
    pub target: String,
    pub start_ts: i64,
    pub fields: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub span_id: Option<i64>,
    pub run_id: Option<String>,
    pub message: String,
    pub fields: serde_json::Value,
    pub ts: i64,
}

impl Store {
    /// Persist a batch of trace messages produced by the telemetry layer.
    /// Called on a background task; failures are logged, never fatal.
    pub async fn apply_trace_batch(&self, batch: &[TraceMsg]) {
        let batch: Vec<TraceMsg> = batch.to_vec();
        let res = self
            .db
            .exec(move |c| {
                for msg in &batch {
                    let r: rusqlite::Result<()> = match msg {
                        TraceMsg::SpanStart {
                            span_id,
                            parent_id,
                            run_id,
                            name,
                            target,
                            start_ts,
                            fields,
                        } => c
                            .execute(
                                "INSERT OR REPLACE INTO trace_spans \
                                 (span_id, parent_id, run_id, name, target, start_ts, fields) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                params![
                                    span_id,
                                    parent_id,
                                    run_id,
                                    name,
                                    target,
                                    start_ts,
                                    fields.to_string()
                                ],
                            )
                            .map(|_| ()),
                        TraceMsg::SpanFields { span_id, fields } => c
                            .execute(
                                "UPDATE trace_spans SET fields = ?1 WHERE span_id = ?2",
                                params![fields.to_string(), span_id],
                            )
                            .map(|_| ()),
                        TraceMsg::SpanEnd {
                            span_id,
                            end_ts,
                            fields,
                        } => c
                            .execute(
                                "UPDATE trace_spans SET end_ts = ?1, fields = ?2 WHERE span_id = ?3",
                                params![end_ts, fields.to_string(), span_id],
                            )
                            .map(|_| ()),
                        TraceMsg::Event {
                            span_id,
                            run_id,
                            message,
                            fields,
                            ts,
                        } => c
                            .execute(
                                "INSERT INTO trace_events (span_id, run_id, message, fields, ts) \
                                 VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![span_id, run_id, message, fields.to_string(), ts],
                            )
                            .map(|_| ()),
                    };
                    if let Err(e) = r {
                        tracing::warn!(error = %e, "failed to persist trace message");
                    }
                }
                Ok(())
            })
            .await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "failed to persist trace batch");
        }
    }

    pub async fn list_spans(&self, run_id: &str) -> Result<Vec<TraceSpan>> {        let run_id = run_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT span_id, parent_id, run_id, name, target, start_ts, end_ts, fields \
                     FROM trace_spans WHERE run_id = ?1 ORDER BY start_ts ASC, span_id ASC",
                )?;
                let rows = stmt.query_map(params![run_id], |r| {
                    Ok(TraceSpan {
                        span_id: r.get(0)?,
                        parent_id: r.get(1)?,
                        run_id: r.get(2)?,
                        name: r.get(3)?,
                        target: r.get(4)?,
                        start_ts: r.get(5)?,
                        end_ts: r.get(6)?,
                        fields: serde_json::from_str::<serde_json::Value>(
                            &r.get::<usize, String>(7).unwrap_or_default(),
                        )
                        .unwrap_or(serde_json::Value::Null),
                    })
                })?;
                rows.collect()
            })
            .await
    }

    /// Debug helper: every span row regardless of run attribution.
    pub async fn debug_all_spans(&self) -> Result<Vec<TraceSpan>> {
        self.db
            .exec(|c| {
                let mut stmt = c.prepare(
                    "SELECT span_id, parent_id, run_id, name, target, start_ts, end_ts, fields \
                     FROM trace_spans ORDER BY start_ts ASC, span_id ASC",
                )?;
                let rows = stmt.query_map([], |r| {
                    Ok(TraceSpan {
                        span_id: r.get(0)?,
                        parent_id: r.get(1)?,
                        run_id: r.get(2)?,
                        name: r.get(3)?,
                        target: r.get(4)?,
                        start_ts: r.get(5)?,
                        end_ts: r.get(6)?,
                        fields: serde_json::from_str::<serde_json::Value>(
                            &r.get::<usize, String>(7).unwrap_or_default(),
                        )
                        .unwrap_or(serde_json::Value::Null),
                    })
                })?;
                rows.collect()
            })
            .await
    }

    pub async fn list_events(&self, run_id: &str) -> Result<Vec<TraceEvent>> {
        let run_id = run_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, span_id, run_id, message, fields, ts FROM trace_events \
                     WHERE run_id = ?1 ORDER BY ts ASC, id ASC",
                )?;
                let rows = stmt.query_map(params![run_id], |r| {
                    Ok(TraceEvent {
                        id: r.get(0)?,
                        span_id: r.get(1)?,
                        run_id: r.get(2)?,
                        message: r.get(3)?,
                        fields: serde_json::from_str::<serde_json::Value>(
                            &r.get::<usize, String>(4).unwrap_or_default(),
                        )
                        .unwrap_or(serde_json::Value::Null),
                        ts: r.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }
}
