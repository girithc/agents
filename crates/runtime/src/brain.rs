//! The brain: what runs inside one "think". Today a stub can be anything;
//! the LLM runtime's run loop becomes the real brain later.

use agenthub_storage::store::agent::AgentTask;
use async_trait::async_trait;

/// Everything the brain sees for one think: who it is, what it wants,
/// the rules, its to-do, and the departures board.
pub struct ThinkCtx {
    pub agent_id: String,
    /// This think's trace id — spans and provider calls share it.
    pub run_id: String,
    /// Title/body of the popped task, or the incoming message that woke it.
    pub task_title: String,
    pub task_body: String,
    pub soul: String,
    pub goal: String,
    pub rules: Vec<String>,
    pub todo: Vec<AgentTask>,
    pub board: Vec<(String, serde_json::Value)>,
}

/// The tools a brain may use while thinking. Read-only views + personal
/// memory; board writes are absent by design (departures-board rule).
pub struct AgentTools {
    store: std::sync::Arc<agenthub_storage::Store>,
    env: crate::env::EnvHandle,
    agent_id: String,
    /// Display name for human-readable logs.
    name: String,
    /// Sleep requested by the brain during this think (via the sleep tool).
    sleep_request: std::sync::Mutex<Option<u64>>,
}

impl AgentTools {
    pub async fn new(
        store: std::sync::Arc<agenthub_storage::Store>,
        env: crate::env::EnvHandle,
        agent_id: impl Into<String>,
    ) -> Self {
        let agent_id = agent_id.into();
        let name = store
            .get_agent(&agent_id)
            .await
            .ok()
            .flatten()
            .map(|a| a.name)
            .unwrap_or_else(|| agent_id.clone());
        Self {
            store,
            env,
            agent_id,
            name,
            sleep_request: std::sync::Mutex::new(None),
        }
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Human-readable name (falls back to the id).
    pub fn agent_name(&self) -> &str {
        &self.name
    }

    /// For the domain-tool layer: direct store + env access.
    pub fn store(&self) -> &std::sync::Arc<agenthub_storage::Store> {
        &self.store
    }

    pub fn env(&self) -> &crate::env::EnvHandle {
        &self.env
    }

    pub async fn remember(
        &self,
        kind: agenthub_storage::store::agent::MemoryKind,
        content: &str,
    ) {
        let _ = self.store.remember(&self.agent_id, kind, content).await;
    }

    pub async fn recall(&self, query: &str, limit: i64) -> Vec<agenthub_storage::store::agent::MemoryRow> {
        self.store
            .recall(&self.agent_id, query, limit)
            .await
            .unwrap_or_default()
    }

    pub async fn todo(&self) -> Vec<AgentTask> {
        self.store
            .list_tasks(&self.agent_id, None)
            .await
            .unwrap_or_default()
    }

    /// Add work to its own queue.
    pub async fn todo_add(&self, title: &str, body: &str) -> Option<AgentTask> {
        self.store.enqueue_task(&self.agent_id, title, body).await.ok()
    }

    /// Close one of its own tasks.
    pub async fn todo_finish(&self, task_id: &str, ok: bool) {
        let _ = self.store.finish_task(task_id, ok).await;
    }

    /// The brain's sleep tool: "don't wake me for this long".
    pub fn request_sleep_ms(&self, ms: u64) {
        *self.sleep_request.lock().unwrap() = Some(ms);
    }

    /// The loop takes (and clears) the request after the think ends.
    pub fn take_sleep_request(&self) -> Option<u64> {
        self.sleep_request.lock().unwrap().take()
    }

    pub async fn board_get(&self, key: &str) -> Option<serde_json::Value> {
        self.env.board_get(key).await
    }

    pub async fn board_snapshot(&self) -> Vec<(String, serde_json::Value)> {
        self.env.board_snapshot().await
    }
}

#[derive(Debug, Clone, Default)]
pub struct ThinkOutcome {
    /// true → task marked done, false → failed.
    pub ok: bool,
    /// Short note; stored as memory and on the task record.
    pub note: String,
    /// If set, the agent goes to sleep for this long after the think.
    pub sleep_ms: Option<u64>,
}

#[async_trait]
pub trait Brain: Send + Sync {
    async fn think(&self, ctx: &ThinkCtx, tools: &AgentTools) -> Result<ThinkOutcome, String>;
}
