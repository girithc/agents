use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------- agents

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    /// What this agent is in the world (the roster shows it); never what
    /// it owns or at what price — that's private.
    pub role: String,
    pub system_prompt: String,
    pub provider: String,
    pub model: String,
    pub temperature: f64,
    pub max_output_tokens: i64,
    pub max_iterations: i64,
    pub reasoning_effort: String,
    pub version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewAgent {
    pub name: String,
    #[serde(default)]
    pub role: String,
    pub system_prompt: String,
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_temperature")]
    pub temperature: f64,
    #[serde(default = "default_max_output_tokens")]
    pub max_output_tokens: i64,
    #[serde(default = "default_max_iterations")]
    pub max_iterations: i64,
    #[serde(default = "default_reasoning_effort")]
    pub reasoning_effort: String,
}

fn default_provider() -> String {
    "meta".into()
}
fn default_model() -> String {
    "muse-spark-1.3-contributor".into()
}
fn default_temperature() -> f64 {
    0.7
}
fn default_max_output_tokens() -> i64 {
    4096
}
fn default_max_iterations() -> i64 {
    6
}
fn default_reasoning_effort() -> String {
    "medium".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgentUpdate {
    pub name: Option<String>,
    pub role: Option<String>,
    pub system_prompt: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<i64>,
    pub max_iterations: Option<i64>,
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentVersion {
    pub id: String,
    pub agent_id: String,
    pub version: i64,
    pub system_prompt: String,
    pub note: Option<String>,
    pub created_at: i64,
}

// ---------------------------------------------------------------- feedback

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feedback {
    pub id: String,
    pub run_id: String,
    pub agent_id: String,
    pub rating: i64,
    pub comment: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeedbackSummary {
    pub agent_id: String,
    pub count: i64,
    pub avg_rating: f64,
}

// ---------------------------------------------------------------- secrets

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretMeta {
    pub name: String,
    pub created_at: i64,
    pub updated_at: i64,
}

// ---------------------------------------------------------------- traces

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    /// Globally unique: `{boot_id}:{raw_span_id}` — safe across restarts.
    pub span_id: String,
    pub parent_id: Option<String>,
    pub run_id: Option<String>,
    pub name: String,
    pub target: String,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
    pub fields: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub id: i64,
    pub span_id: Option<String>,
    pub run_id: Option<String>,
    pub message: String,
    pub fields: Value,
    pub ts: i64,
}
