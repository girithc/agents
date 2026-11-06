//! `agent_facts` — semantic memory: keyed facts kept CURRENT.
//!
//! The diary (`agent_memory`) is "what happened" — append-only prose.
//! Facts are "what I know" — one row per key, UPSERTED: `watch_my_offer`
//! becomes $70, then $75 — an update, never a duplicate. This is the
//! missing organ that makes the echo problem dissolve: state changes are
//! edits, not new diary lines.
//!
//! Facts are always pushed into the agent's think prompt (pinned, like
//! the roster and rules) — the agent never spends a recall round to know
//! its own state. Adding and removing are the agent's explicit tools.

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

/// One keyed fact.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Fact {
    pub key: String,
    pub value: String,
    pub updated_at: i64,
}

impl Store {
    /// Set a fact — create or overwrite (the only way state changes).
    pub async fn fact_put(&self, agent_id: &str, key: &str, value: &str) -> Result<()> {
        let (aid, k, v, now) = (
            agent_id.to_string(),
            key.to_string(),
            value.to_string(),
            now_ms(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_facts (agent_id, key, value, updated_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(agent_id, key) DO UPDATE SET value = ?3, updated_at = ?4",
                    params![aid, k, v, now],
                )
            })
            .await?;
        Ok(())
    }

    /// Remove a fact. Returns whether it existed.
    pub async fn fact_forget(&self, agent_id: &str, key: &str) -> Result<bool> {
        let (aid, k) = (agent_id.to_string(), key.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "DELETE FROM agent_facts WHERE agent_id = ?1 AND key = ?2",
                    params![aid, k],
                )
            })
            .await
            .map(|n| n > 0)
    }

    /// All of an agent's facts, oldest edit first (stable prompt order).
    pub async fn facts_of(&self, agent_id: &str) -> Result<Vec<Fact>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT key, value, updated_at FROM agent_facts \
                     WHERE agent_id = ?1 ORDER BY updated_at",
                )?;
                let rows = stmt.query_map(params![aid], |r| {
                    Ok(Fact {
                        key: r.get(0)?,
                        value: r.get(1)?,
                        updated_at: r.get(2)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> Store {
        let dir = std::env::temp_dir().join(format!("agenthub-test-{}", uuid::Uuid::new_v4()));
        Store::open(&dir.join("test.db"), crate::Vault::open(&dir.join("master.key")).unwrap())
            .expect("open store")
    }

    #[tokio::test]
    async fn facts_upsert_and_forget() {
        let s = store().await;

        // Set, overwrite, forget — the fact is CURRENT, never duplicated.
        s.fact_put("a1", "watch_offer", "$70").await.unwrap();
        s.fact_put("a1", "watch_offer", "$75").await.unwrap();
        s.fact_put("a1", "budget", "$100").await.unwrap();

        let facts = s.facts_of("a1").await.unwrap();
        assert_eq!(facts.len(), 2, "upsert must not pile up rows");
        let offer = facts.iter().find(|f| f.key == "watch_offer").unwrap();
        assert_eq!(offer.value, "$75", "the newest write wins");

        assert!(s.fact_forget("a1", "watch_offer").await.unwrap());
        assert!(!s.fact_forget("a1", "watch_offer").await.unwrap(), "second forget: nothing to forget");
        assert_eq!(s.facts_of("a1").await.unwrap().len(), 1);
    }
}
