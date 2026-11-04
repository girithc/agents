//! Run execution + inspection: start runs (plain + SSE streaming), list
//! and fetch runs with transcripts, per-run traces, and run feedback.

use std::convert::Infallible;

use agenthub_runtime::{HistoryTurn, RunOptions};
use agenthub_storage::{Run, RunMessage};
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, KeepAliveStream, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::{ApiError, ApiResult};
use crate::middleware::RequestId;
use crate::routes::agents::find_agent;
use crate::state::AppState;

pub fn router() -> Router<std::sync::Arc<AppState>> {
    Router::new()
        .route("/v1/agents/{id}/runs", post(create_run))
        .route("/v1/agents/{id}/runs/stream", post(create_run_stream))
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/traces", get(get_run_traces))
        .route("/v1/runs/{id}/feedback", post(add_run_feedback))
}

#[derive(Deserialize)]
pub struct CreateRunBody {
    pub input: String,
    #[serde(default)]
    pub history: Vec<HistoryTurn>,
}

#[derive(Serialize)]
pub struct RunWithMessages {
    #[serde(flatten)]
    pub run: Run,
    pub messages: Vec<RunMessage>,
}

async fn create_run(
    State(st): State<std::sync::Arc<AppState>>,
    Path(agent_id): Path<String>,
    RequestId(request_id): RequestId,
    Json(body): Json<CreateRunBody>,
) -> ApiResult<Json<RunWithMessages>> {
    let agent = find_agent(&st, &agent_id).await?;
    let run = st
        .runtime
        .execute(
            &agent,
            &body.input,
            RunOptions {
                history: body.history,
                request_id: Some(request_id),
                ..Default::default()
            },
        )
        .await?;
    let messages = st.store.list_messages(&run.id).await?;
    Ok(Json(RunWithMessages { run, messages }))
}

// ------------------------------------------------------------ streaming

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SsePayload {
    Run { run_id: String },
    Delta { text: String },
    Reasoning { text: String },
    Done { run: Run, messages: Vec<RunMessage> },
    Error { error: String },
}

fn sse_event(name: &str, payload: &SsePayload) -> Event {
    let data = serde_json::to_string(payload).unwrap_or_else(|_| "{}".into());
    Event::default().event(name).data(data)
}

/// Executes the run in the background and streams provider text deltas as
/// SSE. Events: `run` (run id), `delta` (text chunk), `done` (final run +
/// messages), `error`.
async fn create_run_stream(
    State(st): State<std::sync::Arc<AppState>>,
    Path(agent_id): Path<String>,
    RequestId(request_id): RequestId,
    Json(body): Json<CreateRunBody>,
) -> ApiResult<Sse<KeepAliveStream<UnboundedReceiverStream<Result<Event, Infallible>>>>> {
    let agent = find_agent(&st, &agent_id).await?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<Event, Infallible>>();

    tokio::spawn(async move {
        let (delta_tx, delta_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (reasoning_tx, reasoning_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (start_tx, start_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Forward provider deltas → SSE `delta` events as they arrive.
        let delta_fwd = tokio::spawn({
            let tx = tx.clone();
            async move {
                let mut delta_rx = delta_rx;
                while let Some(d) = delta_rx.recv().await {
                    let _ = tx.send(Ok(sse_event("delta", &SsePayload::Delta { text: d })));
                }
            }
        });
        // Forward reasoning deltas → SSE `reasoning` events.
        let reasoning_fwd = tokio::spawn({
            let tx = tx.clone();
            async move {
                let mut reasoning_rx = reasoning_rx;
                while let Some(d) = reasoning_rx.recv().await {
                    let _ = tx.send(Ok(sse_event("reasoning", &SsePayload::Reasoning { text: d })));
                }
            }
        });
        // Forward run-id → SSE `run` event.
        let start_fwd = tokio::spawn({
            let tx = tx.clone();
            async move {
                let mut start_rx = start_rx;
                if let Some(run_id) = start_rx.recv().await {
                    let _ = tx.send(Ok(sse_event("run", &SsePayload::Run { run_id })));
                }
            }
        });

        let opts = RunOptions {
            history: body.history,
            delta_tx: Some(delta_tx),
            reasoning_tx: Some(reasoning_tx),
            run_start_tx: Some(start_tx),
            request_id: Some(request_id),
        };
        let result = st.runtime.execute(&agent, &body.input, opts).await;

        let _ = delta_fwd.await;
        let _ = reasoning_fwd.await;
        let _ = start_fwd.await;

        match result {
            Ok(run) => {
                let messages = st.store.list_messages(&run.id).await.unwrap_or_default();
                let _ = tx.send(Ok(sse_event("done", &SsePayload::Done { run, messages })));
            }
            Err(e) => {
                let _ = tx.send(Ok(sse_event(
                    "error",
                    &SsePayload::Error { error: e.to_string() },
                )));
            }
        }
    });

    Ok(Sse::new(UnboundedReceiverStream::new(rx)).keep_alive(KeepAlive::default()))
}

// ------------------------------------------------------------ inspection

#[derive(Deserialize, Default)]
pub struct ListRunsQuery {
    pub agent_id: Option<String>,
    /// Filter by run type: `regular` or `improve`.
    pub kind: Option<String>,
    pub limit: Option<u32>,
}

async fn list_runs(
    State(st): State<std::sync::Arc<AppState>>,
    Query(q): Query<ListRunsQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let kind = match q.kind.as_deref() {
        Some("regular") => Some(agenthub_storage::RunKind::Regular),
        Some("improve") => Some(agenthub_storage::RunKind::Improve),
        _ => None,
    };
    let runs = st
        .store
        .list_runs(
            q.agent_id.as_deref(),
            kind,
            q.limit.unwrap_or(100).min(1000),
        )
        .await?;
    Ok(Json(json!({ "runs": runs, "count": runs.len() })))
}

async fn get_run(
    State(st): State<std::sync::Arc<AppState>>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<RunWithMessages>> {
    let run = st
        .store
        .get_run(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("run '{run_id}' not found")))?;
    let messages = st.store.list_messages(&run_id).await?;
    Ok(Json(RunWithMessages { run, messages }))
}

async fn get_run_traces(
    State(st): State<std::sync::Arc<AppState>>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    if st.store.get_run(&run_id).await?.is_none() {
        return Err(ApiError::not_found(format!("run '{run_id}' not found")));
    }
    let spans = st.store.list_spans(&run_id).await?;
    let events = st.store.list_events(&run_id).await?;
    Ok(Json(json!({ "run_id": run_id, "spans": spans, "events": events })))
}

#[derive(Deserialize)]
pub struct FeedbackBody {
    pub rating: i64,
    #[serde(default)]
    pub comment: Option<String>,
}

async fn add_run_feedback(
    State(st): State<std::sync::Arc<AppState>>,
    Path(run_id): Path<String>,
    Json(body): Json<FeedbackBody>,
) -> ApiResult<Json<agenthub_storage::Feedback>> {
    if !(1..=5).contains(&body.rating) {
        return Err(ApiError::bad_request("rating must be 1..=5"));
    }
    let run = st
        .store
        .get_run(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("run '{run_id}' not found")))?;
    let feedback = st
        .store
        .add_feedback(&run_id, &run.agent_id, body.rating, body.comment.as_deref())
        .await?;
    tracing::info!(run_id = %run_id, rating = body.rating, "feedback recorded");
    Ok(Json(feedback))
}
