use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------- agents

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub system_prompt: String,
    pub provider: String,
    pub model: String,
    pub temperature: f64,
    pub max_output_tokens: i64,
    pub max_iterations: i64,
    pub reasoning_effort: String,
    /// Whether the agent uses long-term memory (recall + auto-extraction).
    pub memory_enabled: bool,
    pub version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewAgent {
    pub name: String,
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
    #[serde(default = "default_memory_enabled")]
    pub memory_enabled: bool,
}

fn default_memory_enabled() -> bool {
    true
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
    pub system_prompt: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<i64>,
    pub max_iterations: Option<i64>,
    pub reasoning_effort: Option<String>,
    pub memory_enabled: Option<bool>,
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

// ---------------------------------------------------------------- runs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A normal agent run (user input -> response).
    Regular,
    /// A feedback-loop improvement of the agent's system prompt.
    Improve,
}

impl RunKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunKind::Regular => "regular",
            RunKind::Improve => "improve",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub agent_id: String,
    pub agent_version: i64,
    pub kind: RunKind,
    /// HTTP request that originated this run (`x-request-id`).
    pub request_id: Option<String>,
    pub status: RunStatus,
    pub input: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub provider: String,
    pub model: String,
    pub iterations: i64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMessage {
    pub id: i64,
    pub run_id: String,
    pub role: String,
    pub content: String,
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
