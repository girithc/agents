//! Common abstraction over AI providers.
//!
//! # Isolation guarantees
//!
//! Every provider lives in its own module (see [`meta`] and [`mock`]) and owns:
//!
//! * its own `reqwest::Client` (connection pools are never shared),
//! * its own wire-format mapping (request building + response parsing),
//! * its own error mapping into the shared [`ProviderError`] type.
//!
//! Callers get an `Arc<dyn Provider>` from the [`registry::Registry`]. The
//! runtime additionally wraps provider calls in a timeout and a panic
//! barrier, so one misbehaving provider cannot wedge or crash the rest of
//! the app.
//!
//! Adding a provider = add a module + implement [`Provider`] + register it.

pub mod meta;
pub mod registry;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

// ---------------------------------------------------------------- errors

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider '{provider}' returned http {status}: {body}")]
    Api {
        provider: &'static str,
        status: u16,
        body: String,
    },

    #[error("provider '{0}' transport error: {1}")]
    Transport(&'static str, #[source] reqwest::Error),

    #[error("provider '{0}' returned malformed payload: {1}")]
    Malformed(&'static str, String),

    #[error("provider '{0}' panicked: {1}")]
    Panic(&'static str, String),

    #[error("provider '{0}' stream error: {1}")]
    Stream(&'static str, String),

    #[error("provider '{0}' unknown (registered: {1:?})")]
    Unknown(&'static str, Vec<String>),

    #[error("provider '{0}' timed out")]
    Timeout(&'static str),
}

pub type Result<T> = std::result::Result<T, ProviderError>;

// ---------------------------------------------------------------- models

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    pub content: String,
    /// Tool role: which call this message answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Assistant role: tool calls requested by the model.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self { role: Role::System, content: content.into(), tool_call_id: None, tool_calls: vec![] }
    }
    pub fn user(content: impl Into<String>) -> Self {
        Self { role: Role::User, content: content.into(), tool_call_id: None, tool_calls: vec![] }
    }
    pub fn assistant(content: impl Into<String>) -> Self {
        Self { role: Role::Assistant, content: content.into(), tool_call_id: None, tool_calls: vec![] }
    }
    pub fn tool(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: content.into(),
            tool_call_id: Some(call_id.into()),
            tool_calls: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// JSON object with the call arguments.
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// JSON schema for the arguments.
    pub parameters: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone)]
pub struct CompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: f64,
    pub max_output_tokens: i64,
    pub top_p: f64,
    pub reasoning_effort: Option<String>,
    pub tools: Vec<ToolSpec>,
    /// Attributed in traces; providers include it in their spans.
    pub run_id: Option<String>,
    /// Plaintext API key resolved by the caller from the secrets vault.
    pub api_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompletionResponse {
    pub text: String,
    /// The model's reasoning/thinking summary, when the provider exposes it.
    pub reasoning: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<Usage>,
    pub raw: Value,
}

/// Events emitted while a completion streams in.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Delta(String),
    /// Reasoning/thinking token delta.
    Reasoning(String),
    Done(CompletionResponse),
}

// ---------------------------------------------------------------- trait

#[async_trait]
pub trait Provider: Send + Sync {
    /// Stable registry key, e.g. `"meta"`, `"mock"`.
    fn id(&self) -> &'static str;

    /// Name of the secret in the vault that holds this provider's API key,
    /// if the provider requires one. Resolved by the caller.
    fn secret_name(&self) -> Option<String> {
        None
    }

    fn supports_stream(&self) -> bool {
        true
    }

    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse>;

    async fn complete_stream(
        &self,
        req: CompletionRequest,
    ) -> Result<BoxStream<'static, Result<StreamEvent>>>;
}
