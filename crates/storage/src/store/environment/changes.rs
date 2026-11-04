//! `environment_changes` — the ledger. One append-only row per world
//! mutation. Generic on purpose: `kind` names what happened ("deal",
//! "sale", "item_listed", "rule_added", ...), `payload` carries the details
//! as JSON (item, price, counterparty, ...). Item statuses on the board
//! are a projection of this log.

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct EnvChange {
    pub id: i64,
    pub environment_id: String,
    /// Which agent caused it (NULL = the system itself).
    pub actor: Option<String>,
    pub kind: String,
    pub payload: String, // JSON
    pub created_at: i64,
}

impl Store {
    /// Append a world mutation. Domain tools call this; agents never do.
    pub async fn append_change(
        &self,
        environment_id: &str,
        actor: Option<&str>,
        kind: &str,
        payload: &str,
    ) -> Result<i64> {
        let (eid, actor, kind, payload) = (
            environment_id.to_string(),
            actor.map(|s| s.to_string()),
            kind.to_string(),
            payload.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO environment_changes (environment_id, actor, kind, payload, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![eid, actor, kind, payload, now_ms()],
                )?;
                Ok(c.last_insert_rowid())
            })
            .await
    }

    /// Recent changes, newest first. Optional filters: kind, item (payload
    /// must contain `"item":"<x>"` — kept simple; richer indexing later if
    /// a domain needs it).
    pub async fn list_changes(
        &self,
        environment_id: &str,
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<EnvChange>> {
        let (eid, kind) = (
            environment_id.to_string(),
            kind.map(|s| s.to_string()),
        );
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, environment_id, actor, kind, payload, created_at \
                     FROM environment_changes WHERE environment_id = ?1 \
                     AND (?2 IS NULL OR kind = ?2) ORDER BY id DESC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![eid, kind, limit], |r| {
                    Ok(EnvChange {
                        id: r.get(0)?,
                        environment_id: r.get(1)?,
                        actor: r.get(2)?,
                        kind: r.get(3)?,
                        payload: r.get(4)?,
                        created_at: r.get(5)?,
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
    async fn ledger_appends_and_filters() {
        let s = store().await;
        let env = s.create_environment("market", 1000).await.unwrap();

        let p1 = r#"{"item":"watch","buyer":"b1","seller":"s1","price":85}"#;
        let p2 = r#"{"item":"mince","buyer":"b2","seller":"s2","price":7}"#;
        s.append_change(&env.id, Some("buyer-1"), "deal", p1).await.unwrap();
        s.append_change(&env.id, Some("butcher-1"), "sale", p2).await.unwrap();
        s.append_change(&env.id, None, "item_listed", r#"{"item":"coat"}"#).await.unwrap();

        let all = s.list_changes(&env.id, None, 100).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].kind, "item_listed"); // newest first
        assert!(all[0].actor.is_none()); // system row
        assert_eq!(all[2].kind, "deal"); // oldest

        let deals = s.list_changes(&env.id, Some("deal"), 100).await.unwrap();
        assert_eq!(deals.len(), 1);
        assert!(deals[0].payload.contains("watch"));
    }
}
