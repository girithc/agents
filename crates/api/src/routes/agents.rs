//! Agent CRUD + feedback + the improve (feedback-loop) endpoint.

use std::collections::HashMap;

use agenthub_providers::{ChatMessage, CompletionRequest};
use agenthub_storage::{Agent, NewAgent};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::Instrument;

use crate::error::{ApiError, ApiResult};
use crate::middleware::RequestId;
use crate::state::AppState;

pub fn router() -> Router<std::sync::Arc<AppState>> {
    Router::new()
        .route(
            "/v1/agents",
            get(list_agents).post(create_agent),
        )
        .route(
            "/v1/agents/{id}",
            get(get_agent).patch(update_agent).delete(delete_agent),
        )
        .route("/v1/agents/{id}/versions", get(list_agent_versions))
        .route("/v1/agents/{id}/feedback", get(get_agent_feedback))
        .route("/v1/agents/{id}/improve", post(improve_agent))
}

pub(crate) async fn find_agent(st: &AppState, id_or_name: &str) -> ApiResult<Agent> {
    st.store
        .get_agent(id_or_name)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("agent '{id_or_name}' not found")))
}

#[derive(Deserialize)]
pub struct CreateAgentBody {
    pub name: String,
    pub system_prompt: String,
    #[serde(default = "d_provider")]
    pub provider: String,
    #[serde(default = "d_model")]
    pub model: String,
    #[serde(default = "d_temp")]
    pub temperature: f64,
    #[serde(default = "d_max_tokens")]
    pub max_output_tokens: i64,
    #[serde(default = "d_max_iters")]
    pub max_iterations: i64,
    #[serde(default = "d_effort")]
    pub reasoning_effort: String,
}

fn d_provider() -> String { "meta".into() }
fn d_model() -> String { "muse-spark-1.3-contributor".into() }
fn d_temp() -> f64 { 0.7 }
fn d_max_tokens() -> i64 { 4096 }
fn d_max_iters() -> i64 { 6 }
fn d_effort() -> String { "medium".into() }

impl From<CreateAgentBody> for NewAgent {
    fn from(b: CreateAgentBody) -> Self {
        NewAgent {
            name: b.name,
            system_prompt: b.system_prompt,
            provider: b.provider,
            model: b.model,
            temperature: b.temperature,
            max_output_tokens: b.max_output_tokens,
            max_iterations: b.max_iterations,
            reasoning_effort: b.reasoning_effort,
        }
    }
}

async fn create_agent(
    State(st): State<std::sync::Arc<AppState>>,
    Json(body): Json<CreateAgentBody>,
) -> ApiResult<Json<Agent>> {
    let agent = st.store.create_agent(body.into()).await?;
    tracing::info!(agent_id = %agent.id, name = %agent.name, "agent created");
    Ok(Json(agent))
}

async fn list_agents(State(st): State<std::sync::Arc<AppState>>) -> ApiResult<Json<serde_json::Value>> {
    let agents = st.store.list_agents().await?;
    Ok(Json(json!({ "agents": agents, "count": agents.len() })))
}

async fn get_agent(
    State(st): State<std::sync::Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Agent>> {
    Ok(Json(find_agent(&st, &id).await?))
}

async fn update_agent(
    State(st): State<std::sync::Arc<AppState>>,
    Path(id): Path<String>,
    Json(upd): Json<agenthub_storage::AgentUpdate>,
) -> ApiResult<Json<Agent>> {
    let agent = st
        .store
        .update_agent(&id, upd)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("agent '{id}' not found")))?;
    tracing::info!(agent_id = %agent.id, "agent updated");
    Ok(Json(agent))
}

async fn delete_agent(
    State(st): State<std::sync::Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    if !st.store.delete_agent(&id).await? {
        return Err(ApiError::not_found(format!("agent '{id}' not found")));
    }
    tracing::info!(agent_id = %id, "agent deleted");
    Ok(Json(json!({ "deleted": id })))
}

async fn list_agent_versions(
    State(st): State<std::sync::Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    find_agent(&st, &id).await?;
    let versions = st.store.list_agent_versions(&id).await?;
    Ok(Json(json!({ "versions": versions, "count": versions.len() })))
}

async fn get_agent_feedback(
    State(st): State<std::sync::Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let agent = find_agent(&st, &id).await?;
    let summary = st.store.feedback_summary(&agent.id).await?;
    let feedback = st.store.list_feedback_for_agent(&agent.id, 100).await?;
    Ok(Json(json!({ "summary": summary, "feedback": feedback })))
}

// ------------------------------------------------------------ improve loop

#[derive(Deserialize, Default)]
pub struct ImproveBody {
    /// When true, only propose the new prompt without applying it.
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize)]
pub struct ImproveResult {
    /// The improve run's id — inspect it like any run
    /// (`GET /v1/runs/{run_id}`, kind = "improve").
    pub run_id: String,
    pub applied: bool,
    pub feedback_count: i64,
    pub avg_rating: f64,
    pub previous_system_prompt: String,
    pub proposed_system_prompt: String,
    pub agent: Option<Agent>,
}

/// The feedback loop: aggregate human feedback + recent run transcripts,
/// ask the model to propose an improved system prompt, and (unless
/// dry_run) version-bump the agent with the new prompt.
///
/// The improvement is itself recorded as a run (`kind = "improve"`) with
/// its own run id, transcript, and trace tree — just typed differently
/// from a regular run.
#[tracing::instrument(name = "api.improve_agent", skip_all, fields(agent_id = %agent_id))]
async fn improve_agent(
    State(st): State<std::sync::Arc<AppState>>,
    Path(agent_id): Path<String>,
    RequestId(request_id): RequestId,
    body: Option<Json<ImproveBody>>,
) -> ApiResult<Json<ImproveResult>> {
    let agent = find_agent(&st, &agent_id).await?;
    let dry_run = body.and_then(|Json(b)| Some(b.dry_run)).unwrap_or(false);

    let summary = st.store.feedback_summary(&agent.id).await?;
    let feedback = st.store.list_feedback_for_agent(&agent.id, 50).await?;
    // Transcripts come from regular runs only; other improve runs aren't
    // useful inputs for proposing a better prompt.
    let runs = st
        .store
        .list_runs(Some(&agent.id), Some(agenthub_storage::RunKind::Regular), 20)
        .await?;

    let mut ratings: HashMap<String, (i64, Option<String>)> = HashMap::new();
    for f in &feedback {
        ratings.insert(f.run_id.clone(), (f.rating, f.comment.clone()));
    }

    let mut transcript = String::new();
    for r in runs.iter().take(10) {
        let (rating, comment) = ratings
            .get(&r.id)
            .map(|(rating, comment)| (rating.to_string(), comment.clone().unwrap_or_default()))
            .unwrap_or_else(|| ("n/a".into(), String::new()));
        transcript.push_str(&format!(
            "--- run {} [{}] rating={} comment={}\nINPUT: {}\nOUTPUT: {}\nERROR: {}\n\n",
            truncate(&r.id, 12),
            r.status.as_str(),
            rating,
            comment,
            r.input,
            r.output.as_deref().unwrap_or(""),
            r.error.as_deref().unwrap_or("-"),
        ));
    }
    if transcript.is_empty() {
        transcript.push_str("(no runs yet)\n");
    }

    let prompt = format!(
        "You are an expert prompt engineer improving an AI agent.\n\n\
         CURRENT SYSTEM PROMPT:\n{current}\n\n\
         FEEDBACK SUMMARY: {count} ratings, average {avg:.2}/5. Individual comments are in the transcripts below.\n\n\
         RECENT RUN TRANSCRIPTS:\n{transcript}\n\
         TASK: Rewrite the system prompt so the agent addresses the criticisms in the feedback \
         and avoids the failure patterns visible in the transcripts, while keeping everything \
         that already works. Keep the prompt focused and self-contained.\n\n\
         Respond with ONLY the new system prompt text — no explanations, no markdown fences.",
        current = agent.system_prompt,
        count = summary.count,
        avg = summary.avg_rating,
        transcript = transcript,
    );

    // The improvement is a first-class run: kind = "improve".
    let run = st
        .store
        .create_run(
            &agent.id,
            agent.version,
            &agent.provider,
            &agent.model,
            &format!(
                "improve system prompt from feedback loop (dry_run={dry_run})"
            ),
            agenthub_storage::RunKind::Improve,
            Some(&request_id),
        )
        .await?;
    let run_id = run.id.clone();
    st.store
        .add_message(&run_id, "user", &prompt)
        .await?;

    let span = tracing::info_span!(
        "agent.improve",
        run_id = %run_id,
        request_id = %request_id,
        agent_id = %agent.id,
        dry_run,
    );

    let outcome = improve_llm_call(&st, &agent, prompt, run_id.clone())
        .instrument(span)
        .await;

    match outcome {
        Ok(proposed) => {
            if let Err(e) = st
                .store
                .add_message(&run_id, "assistant", &proposed)
                .await
            {
                tracing::warn!(error = %e, "failed to persist improve response");
            }
            st.store
                .finish_run(&run_id, &proposed, 1)
                .await
                .map_err(ApiError::from)?;

            let applied = !dry_run;
            let mut updated_agent = None;
            if applied {
                let new_version = agent.version + 1;
                st.store
                    .save_agent_version(
                        &agent.id,
                        new_version,
                        &proposed,
                        Some(&format!(
                            "auto-improved from feedback loop ({} ratings, avg {:.2})",
                            summary.count, summary.avg_rating
                        )),
                    )
                    .await?;
                updated_agent = Some(
                    st.store
                        .set_agent_prompt_version(&agent.id, &proposed, new_version)
                        .await?
                        .ok_or_else(|| ApiError::internal("agent vanished during improve"))?,
                );
                tracing::info!(agent_id = %agent.id, new_version, "agent improved via feedback loop");
            }

            Ok(Json(ImproveResult {
                run_id,
                applied,
                feedback_count: summary.count,
                avg_rating: summary.avg_rating,
                previous_system_prompt: agent.system_prompt.clone(),
                proposed_system_prompt: proposed,
                agent: updated_agent,
            }))
        }
        Err(e) => {
            st.store
                .fail_run(&run_id, &e.to_string(), 1)
                .await
                .map_err(ApiError::from)?;
            Err(e)
        }
    }
}

/// The LLM call at the heart of the improve loop — executed under the
/// improve run's span so its traces are attributed to that run.
async fn improve_llm_call(
    st: &AppState,
    agent: &Agent,
    prompt: String,
    run_id: String,
) -> ApiResult<String> {
    let provider = st.runtime.provider(&agent.provider)?;
    let api_key = match provider.secret_name() {
        Some(name) => Some(st.store.get_secret(&name).await?),
        None => None,
    };

    let resp = st
        .runtime
        .call_provider(
            provider.as_ref(),
            CompletionRequest {
                model: agent.model.clone(),
                messages: vec![ChatMessage::user(prompt)],
                temperature: 0.2,
                max_output_tokens: 2048,
                top_p: 1.0,
                reasoning_effort: Some("medium".into()),
                tools: vec![],
                run_id: Some(run_id),
                api_key,
            },
        )
        .await
        .map_err(ApiError::from)?;

    Ok(strip_fences(&resp.text))
}

fn truncate(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn strip_fences(s: &str) -> String {
    let mut lines: Vec<&str> = s.trim().lines().collect();
    if lines.first().map(|l| l.trim_start().starts_with("```")).unwrap_or(false) {
        lines.remove(0);
    }
    if lines.last().map(|l| l.trim() == "```").unwrap_or(false) {
        lines.pop();
    }
    lines.join("\n").trim().to_string()
}
