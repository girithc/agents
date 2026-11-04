//! [`EnvHandle`] — an agent's view of the environment: tick cadence, the
//! rules it must obey, and the departures board. Reads are free; writes
//! belong to domain tools and announce themselves (pub/sub).

use std::sync::Arc;

use agenthub_storage::Store;
use serde_json::Value;

use crate::phonebook::Phonebook;
use crate::process::Mail;

#[derive(Clone)]
pub struct EnvHandle {
    store: Arc<Store>,
    environment_id: String,
    phonebook: Phonebook,
}

impl EnvHandle {
    pub fn new(store: Arc<Store>, environment_id: impl Into<String>) -> Self {
        Self {
            store,
            environment_id: environment_id.into(),
            phonebook: Phonebook::new(),
        }
    }

    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    /// The live-inbox registry: the spawn path lists every agent here so
    /// board changes can knock instead of waiting for the next tick.
    pub fn phonebook(&self) -> Phonebook {
        self.phonebook.clone()
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

    /// Board write path — for domain tools and the system only. The board
    /// is a projection for **human auditing** of the world's state; agents
    /// have no read access to it (their information flows through
    /// conversation and their own memories). The one push it does: agents
    /// who explicitly `subscribe` to a key are woken with a note when that
    /// key changes — stake-based, never broadcast.
    pub async fn set_board(&self, key: &str, value: &Value) {
        let _ = self
            .store
            .set_env_state(&self.environment_id, key, &value.to_string())
            .await;
        self.announce(key, value).await;
    }

    /// Deliver one key change to its subscribers' inboxes, knocking the
    /// live ones awake now.
    async fn announce(&self, key: &str, value: &Value) {
        let targets = self.store.subscribers_of(key).await.unwrap_or_default();
        if targets.is_empty() {
            return;
        }
        let title = format!("{key} changed");
        let body = format!("{key} changed: it is now {value}. You follow this key.");
        for agent_id in targets {
            if let Ok(task) = self.store.enqueue_task(&agent_id, &title, &body).await {
                self.phonebook
                    .knock(&agent_id, Mail::Task(task.id));
            }
        }
    }
}

impl std::fmt::Debug for EnvHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvHandle")
            .field("environment_id", &self.environment_id)
            .finish()
    }
}
