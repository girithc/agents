//! Agent runtime: executes agent runs against a provider, with a
//! tool-calling loop, persistent run transcript, and full tracing.
//!
//! Isolation: every provider call is wrapped in a timeout and a panic
//! barrier, so one provider misbehaving cannot wedge or crash a run (or
//! any other code).

pub mod tools;

use std::sync::Arc;
use std::time::Duration;

use agenthub_providers::{
    registry::Registry, ChatMessage, CompletionRequest, CompletionResponse, Provider,
    ProviderError, Role, StreamEvent,
};
use agenthub_storage::{Agent, Run, Store};
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::panic::AssertUnwindSafe;
use tokio::sync::mpsc::UnboundedSender;
use tracing::Instrument;

use crate::tools::ToolRegistry;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("provider error: {0}")]
    Provider(#[from] ProviderError),

    #[error("storage error: {0}")]
    Storage(#[from] agenthub_storage::StorageError),

    #[error("missing secret '{0}' (set it via `agentctl secret set {0}` or PUT /v1/secrets/{0})")]
    MissingSecret(String),

    #[error("run exceeded max_iterations ({0}) without a final answer")]
    MaxIterations(i64),

    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("provider call timed out")]
    Timeout,

    #[error("provider panicked: {0}")]
    ProviderPanic(String),
}

/// One previous conversation turn supplied by the caller (e.g. the REPL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryTurn {
    pub role: String, // "user" | "assistant"
    pub content: String,
}

#[derive(Default)]
pub struct RunOptions {
    /// Prior turns to include before the new input.
    pub history: Vec<HistoryTurn>,
    /// If set, incremental text deltas are forwarded as the model streams.
    pub delta_tx: Option<UnboundedSender<String>>,
    /// If set, receives the run id as soon as the run row is created.
    pub run_start_tx: Option<UnboundedSender<String>>,
    /// If set, reasoning/thinking token deltas are forwarded here.
    pub reasoning_tx: Option<UnboundedSender<String>>,
    /// Originating HTTP request id (`x-request-id`); stamped on the run row
    /// and on the run's trace span for correlation.
    pub request_id: Option<String>,
}

pub struct Runtime {
    store: Arc<Store>,
    registry: Arc<Registry>,
    tools: Arc<ToolRegistry>,
}

impl Runtime {
    pub fn new(store: Arc<Store>, registry: Arc<Registry>) -> Self {
        Self {
            store,
            registry,
            tools: Arc::new(ToolRegistry::with_builtins()),
        }
    }

    pub fn tools(&self) -> &ToolRegistry {
        &self.tools
    }

    /// Look up a registered provider (used by the API for one-off calls
    /// like the improve loop, outside of a full run).
    pub fn provider(&self, id: &str) -> Result<Arc<dyn Provider>, ProviderError> {
        self.registry.get(id)
    }

    pub fn registry_names(&self) -> Vec<String> {
        self.registry.names()
    }

    /// Execute a full agent run end-to-end. The run row, transcript
    /// messages, and traces are persisted as it progresses.
    pub async fn execute(
        &self,
        agent: &Agent,
        input: &str,
        opts: RunOptions,
    ) -> Result<Run, RuntimeError> {
        let run = self
            .store
            .create_run(
                &agent.id,
                agent.version,
                &agent.provider,
                &agent.model,
                input,
                agenthub_storage::RunKind::Regular,
                opts.request_id.as_deref(),
            )
            .await?;
        let run_id = run.id.clone();
        if let Some(tx) = &opts.run_start_tx {
            let _ = tx.send(run_id.clone());
        }

        // Everything inside this span is attributed to the run in traces.
        let span = tracing::info_span!(
            "agent.run",
            run_id = %run_id,
            request_id = opts.request_id.clone().unwrap_or_default(),
            agent_id = %agent.id,
            provider = %agent.provider,
            model = %agent.model,
        );

        self.execute_inner(agent, input, opts, run)
            .instrument(span)
            .await
    }

    async fn execute_inner(
        &self,
        agent: &Agent,
        input: &str,
        opts: RunOptions,
        run: Run,
    ) -> Result<Run, RuntimeError> {
        let run_id = run.id.clone();
        tracing::info!(input = %input, "agent run started");

        // Transcript: persist the user's input.
        if let Err(e) = self.store.add_message(&run_id, "user", input).await {
            tracing::warn!(error = %e, "failed to persist user message");
        }

        // Resolve provider + api key.
        let provider = match self.registry.get(&agent.provider) {
            Ok(p) => p,
            Err(e) => return Err(self.fail(&run_id, e.into(), 0).await),
        };
        let api_key = match provider.secret_name() {
            Some(secret_name) => match self.store.get_secret(&secret_name).await {
                Ok(key) => Some(key),
                Err(_) => {
                    let err = RuntimeError::MissingSecret(secret_name);
                    return Err(self.fail(&run_id, err, 0).await);
                }
            },
            None => None,
        };

        // Build the conversation.
        let mut messages = vec![ChatMessage::system(agent.system_prompt.clone())];
        for turn in &opts.history {
            match turn.role.as_str() {
                "assistant" => messages.push(ChatMessage::assistant(turn.content.clone())),
                _ => messages.push(ChatMessage::user(turn.content.clone())),
            }
        }
        messages.push(ChatMessage::user(input.to_string()));

        let mut iteration: i64 = 0;
        let mut first_iteration = true;

        let outcome: Result<CompletionResponse, RuntimeError> = loop {
            iteration += 1;
            if iteration > agent.max_iterations {
                break Err(RuntimeError::MaxIterations(agent.max_iterations));
            }

            let req = CompletionRequest {
                model: agent.model.clone(),
                messages: messages.clone(),
                temperature: agent.temperature,
                max_output_tokens: agent.max_output_tokens,
                top_p: 1.0,
                reasoning_effort: Some(agent.reasoning_effort.clone()),
                tools: self.tools.specs(),
                run_id: Some(run_id.clone()),
                api_key: api_key.clone(),
            };

            // First iteration may stream (deltas forwarded to the caller);
            // tool iterations always use the plain request path.
            let resp = if first_iteration && opts.delta_tx.is_some() {
                self.call_streaming(provider.as_ref(), req, &opts.delta_tx, &opts.reasoning_tx)
                    .await
            } else {
                self.call_provider(provider.as_ref(), req).await
            };
            first_iteration = false;

            match resp {
                Ok(resp) => {
                    if resp.tool_calls.is_empty() {
                        break Ok(resp);
                    }
                    // Record the model's tool request…
                    let calls_json =
                        serde_json::to_string(&serde_json::json!({ "tool_calls": resp.tool_calls }))?;
                    if let Err(e) = self.store.add_message(&run_id, "assistant", &calls_json).await {
                        tracing::warn!(error = %e, "failed to persist tool-call message");
                    }
                    // …keep the assistant's function_call item in the
                    // conversation — providers require each tool output to
                    // pair with its function_call item.
                    messages.push(ChatMessage {
                        role: Role::Assistant,
                        content: resp.text.clone(),
                        tool_call_id: None,
                        tool_calls: resp.tool_calls.clone(),
                    });
                    // …execute each tool, feed results back to the model.
                    for call in resp.tool_calls {
                        let result = self.tools.call(&call).await;
                        tracing::info!(tool = %call.name, result = %result, "tool executed");
                        messages.push(ChatMessage::tool(call.id.clone(), result.clone()));
                        if let Err(e) = self.store.add_message(&run_id, "tool", &result).await {
                            tracing::warn!(error = %e, "failed to persist tool message");
                        }
                    }
                }
                Err(e) => break Err(RuntimeError::Provider(e)),
            }
        };

        match outcome {
            Ok(resp) => {
                // Thinking/reasoning tokens become part of the transcript
                // (role "reasoning") and are trace-visible.
                if let Some(reasoning) = resp.reasoning.as_deref().filter(|r| !r.trim().is_empty()) {
                    if let Err(e) = self.store.add_message(&run_id, "reasoning", reasoning).await {
                        tracing::warn!(error = %e, "failed to persist reasoning message");
                    }
                }
                if let Err(e) = self
                    .store
                    .add_message(&run_id, "assistant", &resp.text)
                    .await
                {
                    tracing::warn!(error = %e, "failed to persist assistant message");
                }
                let run = self
                    .store
                    .finish_run(&run_id, &resp.text, iteration)
                    .await
                    .unwrap_or(run);
                tracing::info!(run_id = %run.id, iterations = iteration, "agent run finished");
                Ok(run)
            }
            Err(e) => Err(self.fail(&run_id, e, iteration).await),
        }
    }

    /// Provider call wrapped in a timeout + panic barrier: a hung or
    /// panicking provider becomes a normal error, never a wedged run.
    pub async fn call_provider(
        &self,
        provider: &dyn Provider,
        req: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        let fut = provider.complete(req);
        match tokio::time::timeout(
            Duration::from_secs(240),
            AssertUnwindSafe(fut).catch_unwind(),
        )
        .await
        {
            Ok(Ok(Ok(resp))) => Ok(resp),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(panic)) => {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".into());
                Err(ProviderError::Panic(provider.id(), msg))
            }
            Err(_) => Err(ProviderError::Timeout(provider.id())),
        }
    }

    /// Streaming variant: forwards text deltas while aggregating the final
    /// response. Falls back to the non-streaming path if the provider does
    /// not support streaming.
    async fn call_streaming(
        &self,
        provider: &dyn Provider,
        req: CompletionRequest,
        delta_tx: &Option<UnboundedSender<String>>,
        reasoning_tx: &Option<UnboundedSender<String>>,
    ) -> Result<CompletionResponse, ProviderError> {
        if !provider.supports_stream() {
            return self.call_provider(provider, req).await;
        }

        let stream = match tokio::time::timeout(
            Duration::from_secs(60),
            AssertUnwindSafe(provider.complete_stream(req.clone())).catch_unwind(),
        )
        .await
        {
            Ok(Ok(Ok(s))) => s,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(panic)) => {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown panic".into());
                return Err(ProviderError::Panic(provider.id(), msg));
            }
            Err(_) => return Err(ProviderError::Timeout(provider.id())),
        };

        let mut final_resp: Option<CompletionResponse> = None;
        let mut acc = String::new();
        let mut reasoning_acc = String::new();
        let mut result = Ok(());

        let mut stream = std::pin::pin!(stream);
        while let Some(item) = tokio::time::timeout(Duration::from_secs(240), stream.next()).await.unwrap_or(None) {
            match item {
                Ok(StreamEvent::Delta(d)) => {
                    acc.push_str(&d);
                    if let Some(tx) = delta_tx {
                        let _ = tx.send(d);
                    }
                }
                Ok(StreamEvent::Reasoning(d)) => {
                    reasoning_acc.push_str(&d);
                    if let Some(tx) = reasoning_tx {
                        let _ = tx.send(d);
                    }
                }
                Ok(StreamEvent::Done(resp)) => {
                    final_resp = Some(resp);
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }

        match (result, final_resp) {
            (Ok(()), Some(mut resp)) => {
                // Prefer the aggregated text if the provider's final event
                // came back empty.
                if resp.text.trim().is_empty() && !acc.is_empty() {
                    resp.text = acc;
                }
                if resp.reasoning.as_deref().map(str::is_empty).unwrap_or(true)
                    && !reasoning_acc.is_empty()
                {
                    resp.reasoning = Some(reasoning_acc);
                }
                Ok(resp)
            }
            (Ok(()), None) => Ok(CompletionResponse {
                text: acc,
                reasoning: if reasoning_acc.is_empty() { None } else { Some(reasoning_acc) },
                tool_calls: vec![],
                usage: None,
                raw: serde_json::Value::Null,
            }),
            (Err(e), _) => Err(e),
        }
    }

    async fn fail(&self, run_id: &str, err: RuntimeError, iterations: i64) -> RuntimeError {
        let msg = err.to_string();
        tracing::error!(run_id = %run_id, error = %msg, "agent run failed");
        if let Err(e) = self.store.fail_run(run_id, &msg, iterations).await {
            tracing::warn!(error = %e, "failed to persist run failure");
        }
        err
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agenthub_providers::meta::MetaProvider;
    use agenthub_providers::mock::MockProvider;
    use agenthub_providers::registry::Registry;
    use agenthub_storage::Vault;

    async fn test_store() -> Arc<Store> {
        let dir = tempfile::tempdir().unwrap();
        // Leak the tempdir for the lifetime of the test process — simple
        // and fine for tests.
        let dir = std::sync::Mutex::new(dir);
        std::mem::forget(dir);
        let path = std::env::temp_dir().join(format!("agenthub-test-{}", uuid::Uuid::new_v4()));
        let vault = Vault::open(&path.join("master.key")).unwrap();
        Arc::new(Store::open(&path.join("db.sqlite"), vault).unwrap())
    }

    #[tokio::test]
    async fn run_with_mock_provider() {
        let store = test_store().await;
        let registry = Registry::new();
        registry.register(Arc::new(MockProvider::new()));
        let runtime = Runtime::new(store.clone(), Arc::new(registry));

        let agent = store
            .create_agent(agenthub_storage::NewAgent {
                name: "tester".into(),
                system_prompt: "test prompt".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                temperature: 0.5,
                max_output_tokens: 256,
                max_iterations: 3,
                reasoning_effort: "low".into(),
            })
            .await
            .unwrap();

        let run = runtime
            .execute(&agent, "hello world", RunOptions::default())
            .await
            .unwrap();

        assert_eq!(run.status, agenthub_storage::RunStatus::Succeeded);
        assert!(run.output.clone().unwrap().contains("hello world"));

        let messages = store.list_messages(&run.id).await.unwrap();
        assert_eq!(messages[0].role, "user");
        assert_eq!(*messages.last().unwrap().role, "assistant".to_string());
    }

    #[tokio::test]
    async fn run_failure_is_persisted() {
        let store = test_store().await;
        let registry = Registry::new();
        registry.register(Arc::new(MockProvider::new()));
        let runtime = Runtime::new(store.clone(), Arc::new(registry));

        let agent = store
            .create_agent(agenthub_storage::NewAgent {
                name: "failing".into(),
                system_prompt: "x".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                temperature: 0.5,
                max_output_tokens: 256,
                max_iterations: 3,
                reasoning_effort: "low".into(),
            })
            .await
            .unwrap();

        let err = runtime
            .execute(&agent, "#fail now", RunOptions::default())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("mock failure"));

        let runs = store
            .list_runs(Some(&agent.id), None, 10)
            .await
            .unwrap();
        assert_eq!(runs[0].status, agenthub_storage::RunStatus::Failed);
        assert!(runs[0].error.is_some());
    }

    // Referenced so MetaProvider stays exercised in the type graph even if
    // not used in tests directly.
    #[allow(dead_code)]
    fn _meta_compiles(p: &MetaProvider) {
        let _ = p.id();
    }
}
