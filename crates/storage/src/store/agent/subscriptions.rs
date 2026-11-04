//! `subscriptions` — the board keys an agent chose to follow.
//!
//! Pub/sub over environment-state keys: the agent subscribes itself
//! (explicitly, via its `subscribe` tool); any later change to that key is
//! delivered to the subscriber's inbox by the runtime's fan-out.

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

impl Store {
    /// Follow a board key. Idempotent.
    pub async fn subscribe(&self, agent_id: &str, key: &str) -> Result<()> {
        let (aid, k, now) = (agent_id.to_string(), key.to_string(), now_ms());
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO subscriptions (agent_id, key, created_at) \
                     VALUES (?1, ?2, ?3)",
                    params![aid, k, now],
                )
            })
            .await?;
        Ok(())
    }

    /// Stop following a board key. Returns whether a subscription existed.
    pub async fn unsubscribe(&self, agent_id: &str, key: &str) -> Result<bool> {
        let (aid, k) = (agent_id.to_string(), key.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "DELETE FROM subscriptions WHERE agent_id = ?1 AND key = ?2",
                    params![aid, k],
                )
            })
            .await
            .map(|n| n > 0)
    }

    /// Everyone following a key — the fan-out list for a change.
    pub async fn subscribers_of(&self, key: &str) -> Result<Vec<String>> {
        let k = key.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT agent_id FROM subscriptions WHERE key = ?1 ORDER BY created_at",
                )?;
                let rows = stmt.query_map([k], |r| r.get(0))?;
                rows.collect()
            })
            .await
    }

    /// Keys an agent follows (its own subscription list).
    pub async fn subscriptions_of(&self, agent_id: &str) -> Result<Vec<String>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT key FROM subscriptions WHERE agent_id = ?1 ORDER BY created_at",
                )?;
                let rows = stmt.query_map([aid], |r| r.get(0))?;
                rows.collect()
            })
            .await
    }
}
