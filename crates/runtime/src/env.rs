//! [`EnvHandle`] — an agent's read-only view of the environment: tick
//! cadence, the rules it must obey, and the departures board (read-only).

use std::sync::Arc;

use agenthub_storage::Store;
use serde_json::Value;

#[derive(Clone)]
pub struct EnvHandle {
    store: Arc<Store>,
    environment_id: String,
}

impl EnvHandle {
    pub fn new(store: Arc<Store>, environment_id: impl Into<String>) -> Self {
        Self { store, environment_id: environment_id.into() }
    }

    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    /// Passive-wake cadence from the environment row.
    pub async fn tick_interval(&self) -> std::time::Duration {
        let id = self.environment_id.clone();
        let ms = self
            .store
            .get_environment(&id)
            .await
            .ok()
            .flatten()
            .map(|e| e.tick_interval_ms)
            .unwrap_or(30_000);
        std::time::Duration::from_millis(ms.max(1) as u64)
    }

    /// Global rules + this agent's scoped rules, as plain text.
    pub async fn rules_for(&self, agent_id: &str) -> Vec<String> {
        self.store
            .rules_for(&self.environment_id, agent_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.rule)
            .collect()
    }

    // ------------------------------------------------------------ the board
    // Agents read the board; only trusted tool side-effects write it
    // (`set_board` is not exposed to brains).

    pub async fn board_get(&self, key: &str) -> Option<Value> {
        let raw = self
            .store
            .get_env_state(&self.environment_id, key)
            .await
            .ok()
            .flatten()?;
        serde_json::from_str(&raw).ok()
    }

    pub async fn board_snapshot(&self) -> Vec<(String, Value)> {
        self.store
            .env_snapshot(&self.environment_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| serde_json::from_str(&row.value).ok().map(|v| (row.key, v)))
            .collect()
    }

    /// Board write path — for domain tools and the system only.
    pub async fn set_board(&self, key: &str, value: &Value) {
        let _ = self
            .store
            .set_env_state(&self.environment_id, key, &value.to_string())
            .await;
    }
}

impl std::fmt::Debug for EnvHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvHandle")
            .field("environment_id", &self.environment_id)
            .finish()
    }
}
