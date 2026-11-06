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

/// One keyed fact. `about` anchors it to a record ("item:12",
/// "thread:abc") — the same key can exist per thing; '' = the agent itself.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Fact {
    pub key: String,
    pub value: String,
    pub about: String,
    pub updated_at: i64,
}

impl Store {
    /// Set a fact — create or overwrite (the only way state changes).
    /// Scoped: '' = about me; "item:12" / "thread:abc" = about a record.
    pub async fn fact_put(&self, agent_id: &str, key: &str, value: &str, about: &str) -> Result<()> {
        let (aid, k, v, ab, now) = (
            agent_id.to_string(),
            key.to_string(),
            value.to_string(),
            about.to_string(),
            now_ms(),
        );
        self.db
            .exec(move |c| {
                // Check-then-write inside one connection lock — atomic
                // enough (single writer), and old PKs can't do ON CONFLICT
                // with `about` in the target.
                let hit: Option<String> = c
                    .query_row(
                        "SELECT value FROM agent_facts \
                         WHERE agent_id = ?1 AND key = ?2 AND about = ?3",
                        params![aid, k, ab],
                        |r| r.get(0),
                    )
                    .ok();
                match hit {
                    Some(_) => {
                        c.execute(
                            "UPDATE agent_facts SET value = ?3, updated_at = ?4 \
                             WHERE agent_id = ?1 AND key = ?2 AND about = ?5",
                            params![aid, k, v, now, ab],
                        )?;
                    }
                    None => {
                        c.execute(
                            "INSERT INTO agent_facts (agent_id, key, value, about, updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![aid, k, v, ab, now],
                        )?;
                    }
                }
                Ok(())
            })
            .await
    }

    /// Remove a fact (within its scope). Returns whether it existed.
    pub async fn fact_forget(&self, agent_id: &str, key: &str, about: &str) -> Result<bool> {
        let (aid, k, ab) = (agent_id.to_string(), key.to_string(), about.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "DELETE FROM agent_facts WHERE agent_id = ?1 AND key = ?2 AND about = ?3",
                    params![aid, k, ab],
                )
            })
            .await
            .map(|n| n > 0)
    }

    /// My own agent-scope facts (about = '') — pinned into every think.
    pub async fn facts_of(&self, agent_id: &str) -> Result<Vec<Fact>> {
        self.facts_about(agent_id, "").await
    }

    /// All facts anchored to one record — the sheet for a thread or thing.
    pub async fn facts_about(&self, agent_id: &str, about: &str) -> Result<Vec<Fact>> {
        let (aid, ab) = (agent_id.to_string(), about.to_string());
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT key, value, about, updated_at FROM agent_facts \
                     WHERE agent_id = ?1 AND about = ?2 ORDER BY updated_at",
                )?;
                let rows = stmt.query_map(params![aid, ab], |r| {
                    Ok(Fact {
                        key: r.get(0)?,
                        value: r.get(1)?,
                        about: r.get(2)?,
                        updated_at: r.get(3)?,
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
        s.fact_put("a1", "watch_offer", "$70", "").await.unwrap();
        s.fact_put("a1", "watch_offer", "$75", "").await.unwrap();
        s.fact_put("a1", "budget", "$100", "").await.unwrap();

        let facts = s.facts_of("a1").await.unwrap();
        assert_eq!(facts.len(), 2, "upsert must not pile up rows");
        let offer = facts.iter().find(|f| f.key == "watch_offer").unwrap();
        assert_eq!(offer.value, "$75", "the newest write wins");

        assert!(s.fact_forget("a1", "watch_offer", "").await.unwrap());
        assert!(!s.fact_forget("a1", "watch_offer", "").await.unwrap(), "second forget: nothing to forget");
        assert_eq!(s.facts_of("a1").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn same_key_per_thing_is_separate_facts() {
        let s = store().await;

        // Two watches, two floors: same key, different about — both live.
        s.fact_put("s1", "floor", "$70", "item:aaa").await.unwrap();
        s.fact_put("s1", "floor", "$65", "item:bbb").await.unwrap();
        assert_eq!(s.facts_about("s1", "item:aaa").await.unwrap()[0].value, "$70");
        assert_eq!(s.facts_about("s1", "item:bbb").await.unwrap()[0].value, "$65");

        // And agent-scope facts don't see the anchored ones.
        assert!(s.facts_of("s1").await.unwrap().is_empty());
    }
}
