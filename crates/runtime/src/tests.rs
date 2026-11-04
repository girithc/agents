//! Engine tests. Real brain (LlmBrain → MetaProvider, api.meta.ai).
//!
//! Tests that need the model read the key from `MODEL_API_KEY` (env or
//! `./.env`) and seed it into the vault; without a key they skip.
//! Offline-path tests (queue mechanics, failure, dispatch) never touch
//! the network and always run.

use std::sync::Arc;
use std::time::Duration;

use agenthub_providers::meta::MetaProvider;
use agenthub_providers::registry::Registry;
use agenthub_providers::{CompletionRequest, CompletionResponse, Provider, ProviderError};
use agenthub_storage::store::agent::AgentTaskStatus;
use agenthub_storage::{NewAgent, Store, Vault};

use crate::env::EnvHandle;
use crate::llm_brain::{LlmBrain, dispatch_tool};
use crate::process::{spawn_agent_loop, AgentDeps};
use crate::supervisor::Supervisor;

// ------------------------------------------------------------ helpers

/// Deterministic offline provider for loop-mechanics tests: replies with
/// plain text, no tool calls — an idle think costs nothing.
struct MockProvider;

#[async_trait::async_trait]
impl Provider for MockProvider {
    fn id(&self) -> &'static str {
        "mock"
    }

    async fn complete(
        &self,
        _req: CompletionRequest,
    ) -> agenthub_providers::Result<CompletionResponse> {
        Ok(CompletionResponse {
            text: "nothing to do right now".into(),
            reasoning: None,
            tool_calls: vec![],
            usage: None,
            raw: serde_json::json!({ "mock": true }),
        })
    }

    async fn complete_stream(
        &self,
        _req: CompletionRequest,
    ) -> agenthub_providers::Result<
        futures::stream::BoxStream<
            'static,
            agenthub_providers::Result<agenthub_providers::StreamEvent>,
        >,
    > {
        Err(ProviderError::Stream("mock", "does not stream".into()))
    }
}

fn mock_brain(s: Arc<Store>) -> Arc<LlmBrain> {
    let registry = Registry::new();
    registry.register(Arc::new(MockProvider));
    Arc::new(LlmBrain::new(s, Arc::new(registry)))
}

fn register_key(s: &Store) -> bool {
    match crate::model_api_key() {
        Some(k) => futures::executor::block_on(s.put_secret("meta.api_key", &k)).is_ok(),
        None => false,
    }
}

fn meta_brain(s: Arc<Store>) -> Arc<LlmBrain> {
    let registry = Registry::new();
    registry.register(Arc::new(MetaProvider::new(None)));
    Arc::new(LlmBrain::new(s, Arc::new(registry)))
}

async fn store() -> Arc<Store> {
    let dir = std::env::temp_dir().join(format!("agenthub-test-{}", uuid::Uuid::new_v4()));
    Arc::new(
        Store::open(&dir.join("test.db"), Vault::open(&dir.join("master.key")).unwrap())
            .expect("open store"),
    )
}

async fn agent_row(s: &Store, name: &str, provider: &str) -> String {
    let a = s
        .create_agent(NewAgent {
            name: name.to_string(),
            role: "test".into(),
            system_prompt: "you are a test agent".into(),
            provider: provider.into(),
            model: "muse-spark-1.3-contributor".into(),
            temperature: 0.5,
            max_output_tokens: 512,
            max_iterations: 4,
            reasoning_effort: "low".into(),
        })
        .await
        .unwrap();
    a.id
}

async fn wait_task_status(
    s: &Store,
    agent_id: &str,
    want: AgentTaskStatus,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if let Ok(tasks) = s.list_tasks(agent_id, None).await {
            if tasks.first().map(|t| t.status) == Some(want) {
                return true;
            }
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ------------------------------------------------------------ loop mechanics
// (no model calls: the queue stays empty or the brain errors offline)

#[tokio::test]
async fn empty_queue_triggers_idle_think() {
    let s = store().await;
    let env_row = s.create_environment("t", 20).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let agent_id = agent_row(&s, "idle", "mock").await;
    let brain = mock_brain(s.clone());

    let handle = spawn_agent_loop(
        AgentDeps { store: s.clone(), env, agent_id: agent_id.clone() },
        brain,
    );

    // The queue is empty, so the loop hands the microphone to the soul:
    // one idle think (mock provider), and because it neither acted nor
    // requested sleep, the loop settles into the 30s backoff nap — not
    // the environment's 20ms tick.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut backed_off = false;
    while tokio::time::Instant::now() < deadline {
        if let Some(st) = s.get_state(&agent_id).await.unwrap() {
            if let Some(wake_at) = st.next_wake_at {
                if wake_at >= agenthub_storage::now_ms() + 25_000 {
                    backed_off = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(backed_off, "empty queue should end in an idle think + 30s backoff nap");

    handle.stop();
    handle.finished().await;
}

#[tokio::test]
async fn unknown_provider_fails_task_offline() {
    let s = store().await;
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let agent_id = agent_row(&s, "klutz", "no-such-provider").await;
    let brain = meta_brain(s.clone());

    let handle = spawn_agent_loop(
        AgentDeps { store: s.clone(), env, agent_id: agent_id.clone() },
        brain,
    );

    s.enqueue_task(&agent_id, "impossible", "do anything").await.unwrap();
    assert!(
        wait_task_status(&s, &agent_id, AgentTaskStatus::Failed, Duration::from_secs(5)).await,
        "task should be marked failed (unknown provider)"
    );

    handle.stop();
    handle.finished().await;
}

#[tokio::test]
async fn supervisor_resume_and_stop() {
    let s = store().await;
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);

    let sup = Supervisor::new(s.clone(), env.clone(), Arc::new({
        let s2 = s.clone();
        move |_agent| meta_brain(s2.clone())
    }));

    // Mark agents live in the DB, as if they ran before a restart.
    s.set_state("b1", agenthub_storage::store::agent::AgentStatus::Idle, None, None)
        .await
        .unwrap();
    s.set_state(
        "b2",
        agenthub_storage::store::agent::AgentStatus::Sleeping,
        None,
        Some(agenthub_storage::now_ms() + 60_000),
    )
    .await
    .unwrap();
    s.set_state("b3", agenthub_storage::store::agent::AgentStatus::Stopped, None, None)
        .await
        .unwrap();

    let resumed = sup.resume_all().await;
    assert!(resumed.contains(&"b1".to_string()));
    assert!(resumed.contains(&"b2".to_string()));
    assert!(!resumed.contains(&"b3".to_string()), "stopped agents stay down");

    for id in ["b1", "b2"] {
        sup.get(id).unwrap().stop();
    }
    sup.get("b1").unwrap().finished().await;
    sup.get("b2").unwrap().finished().await;
}

// ------------------------------------------------------------ tool dispatch
// (unit level, offline)

#[tokio::test]
async fn dispatch_sleep_tool_requests_sleep() {
    let s = store().await;
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let tools = crate::brain::AgentTools::new(s.clone(), env, "a1").await;

    let call = agenthub_providers::ToolCall {
        id: "c1".into(),
        name: "sleep".into(),
        arguments: serde_json::json!({ "seconds": 50 }),
    };
    let out = dispatch_tool(&call, &tools, &[]).await;
    assert!(out.contains("\"ok\":true"));
    assert_eq!(tools.take_sleep_request(), Some(50_000));
}

#[tokio::test]
async fn dispatch_remember_then_recall() {
    let s = store().await;
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let tools = crate::brain::AgentTools::new(s.clone(), env, "a2").await;

    let remember = agenthub_providers::ToolCall {
        id: "c1".into(),
        name: "remember".into(),
        arguments: serde_json::json!({ "content": "the vault code is 1234" }),
    };
    let out = dispatch_tool(&remember, &tools, &[]).await;
    assert!(out.contains("\"ok\":true"));

    let recall = agenthub_providers::ToolCall {
        id: "c2".into(),
        name: "recall".into(),
        arguments: serde_json::json!({ "query": "vault code" }),
    };
    let out = dispatch_tool(&recall, &tools, &[]).await;
    assert!(out.contains("vault code"));
}

// ------------------------------------------------------------ live brain
// (real model calls; skip without MODEL_API_KEY)

#[tokio::test]
async fn live_think_completes_task_and_remembers() {
    let s = store().await;
    if !register_key(&s) {
        eprintln!("skipping: MODEL_API_KEY not set");
        return;
    }
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let agent_id = agent_row(&s, "live", "meta").await;
    let brain = meta_brain(s.clone());

    s.upsert_soul(&agent_id, "You are Ripley, a careful warehouse clerk.", "Keep tidy records.")
        .await
        .unwrap();

    let handle = spawn_agent_loop(
        AgentDeps { store: s.clone(), env, agent_id: agent_id.clone() },
        brain,
    );

    s.enqueue_task(
        &agent_id,
        "record the delivery",
        "A crate of 12 wrenches arrived from Acme. Remember this delivery, then confirm.",
    )
    .await
    .unwrap();
    assert!(
        wait_task_status(&s, &agent_id, AgentTaskStatus::Done, Duration::from_secs(120)).await,
        "live think should complete the task"
    );

    let mem = s.recent_memory(&agent_id, 10).await.unwrap();
    assert!(
        mem.iter().any(|m| m.content.to_lowercase().contains("wrench")
            || m.content.to_lowercase().contains("acme")),
        "the think should leave a trace in memory"
    );

    handle.stop();
    handle.finished().await;
}

#[tokio::test]
async fn live_message_wakes_agent() {
    let s = store().await;
    if !register_key(&s) {
        eprintln!("skipping: MODEL_API_KEY not set");
        return;
    }
    let env_row = s.create_environment("t", 60_000).await.unwrap();
    let env = EnvHandle::new(s.clone(), &env_row.id);
    let agent_id = agent_row(&s, "awake", "meta").await;
    let brain = meta_brain(s.clone());

    let handle = spawn_agent_loop(
        AgentDeps { store: s.clone(), env, agent_id: agent_id.clone() },
        brain,
    );

    // A direct message is a think on its own — no task needed.
    handle.send_message("boss", "Please remember: the register code is 47-47.");
    let mut remembered = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(120) {
        let mem = s.recent_memory(&agent_id, 20).await.unwrap_or_default();
        if mem.iter().any(|m| m.content.contains("47-47")) {
            remembered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(remembered, "agent should wake on mail and record the message");

    handle.stop();
    handle.finished().await;
}
