//! Agent CRUD + feedback.

use agenthub_storage::{Agent, NewAgent};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::error::{ApiError, ApiResult};
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
