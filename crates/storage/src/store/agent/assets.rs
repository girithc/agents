//! `agent_assets` — what an agent OWNS: money balances, items, anything
//! countable. Private to the agent (only its own rows are readable).
//!
//! [`Store::exchange`] is the base's value-communication primitive: move
//! assets between two parties in ONE atomic transaction, with one ledger
//! row. Scene tools (accept_deal, ...) call it; agents never touch it
//! directly.

use rusqlite::params;
use serde::Serialize;

use super::super::Store;
use crate::error::{Result, StorageError};
use crate::now_ms;

#[derive(Debug, Clone, Serialize)]
pub struct Asset {
    pub agent_id: String,
    pub kind: String, // "money" | "item" | ...
    pub name: String, // "usd" | "watch" | ...
    pub qty: f64,
}

/// One side of an exchange: what this party gives (+qty moves FROM them).
#[derive(Debug, Clone)]
pub struct AssetGive {
    pub agent_id: String,
    pub kind: String,
    pub name: String,
    pub qty: f64,
}

impl Store {
    /// Read one agent's own assets (privacy: only ever your own rows).
    pub async fn assets_of(&self, agent_id: &str) -> Result<Vec<Asset>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT agent_id, kind, name, qty FROM agent_assets \
                     WHERE agent_id = ?1 ORDER BY kind, name",
                )?;
                let rows = stmt.query_map(params![aid], |r| {
                    Ok(Asset {
                        agent_id: r.get(0)?,
                        kind: r.get(1)?,
                        name: r.get(2)?,
                        qty: r.get(3)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }

    /// Apply a delta to one asset. Fails if the result would be negative.
    pub async fn adjust_asset(
        &self,
        agent_id: &str,
        kind: &str,
        name: &str,
        delta: f64,
    ) -> Result<f64> {
        let (aid, kind, name) = (
            agent_id.to_string(),
            kind.to_string(),
            name.to_string(),
        );
        self.db
            .exec_any(move |c| {
                let cur: f64 = c
                    .query_row(
                        "SELECT qty FROM agent_assets WHERE agent_id=?1 AND kind=?2 AND name=?3",
                        params![aid, kind, name],
                        |r| r.get(0),
                    )
                    .unwrap_or(0.0);
                let next = cur + delta;
                if next < 0.0 {
                    return Err(StorageError::Insufficient(format!(
                        "{aid} has {cur} {kind}:{name}, needs {}", -delta
                    )));
                }
                c.execute(
                    "INSERT INTO agent_assets (agent_id, kind, name, qty) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(agent_id, kind, name) DO UPDATE SET qty=?4",
                    params![aid, kind, name, next],
                )?;
                Ok(next)
            })
            .await
    }

    /// THE exchange: `a` gives `a_give` to `b`, `b` gives `b_give` to `a`,
    /// atomically. One ledger row records the world mutation. If either
    /// side can't afford their part, NOTHING moves and the error names it.
    /// For trades involving a UNIQUE item, pass
    /// `item = (item_id, current_owner)` — ownership flips to the other
    /// party inside the same transaction (the whole trade fails if the
    /// item isn't where expected or is already sold).
    pub async fn exchange(
        &self,
        environment_id: &str,
        actor: Option<&str>,
        ledger_kind: &str,
        payload: &str,
        a_give: AssetGive,
        b_give: AssetGive,
        item: Option<(&str, &str)>,
    ) -> Result<()> {
        let (eid, actor, lkind, payload) = (
            environment_id.to_string(),
            actor.map(|s| s.to_string()),
            ledger_kind.to_string(),
            payload.to_string(),
        );
        let item: Option<(String, String)> = item.map(|(i, o)| (i.to_string(), o.to_string()));
        self.db
            .exec_any(move |c| {
                // One SQLite transaction: all checks + moves, or none.
                let tx = c.unchecked_transaction()?;
                let moves = [
                    (&a_give, &b_give.agent_id), // a loses → b gains
                    (&b_give, &a_give.agent_id), // b loses → a gains
                ];
                for (give, receiver) in moves.iter() {
                    let (g_agent, g_kind, g_name, g_qty) =
                        (&give.agent_id, &give.kind, &give.name, give.qty);
                    let cur: f64 = c
                        .query_row(
                            "SELECT qty FROM agent_assets WHERE agent_id=?1 AND kind=?2 AND name=?3",
                            params![g_agent, g_kind, g_name],
                            |r| r.get(0),
                        )
                        .unwrap_or(0.0);
                    if cur < g_qty {
                        return Err(StorageError::Insufficient(format!(
                            "{g_agent} has {cur} {g_kind}:{g_name}, needs {g_qty}"
                        )));
                    }
                    let from = cur - g_qty;
                    c.execute(
                        "INSERT INTO agent_assets (agent_id, kind, name, qty) VALUES (?1, ?2, ?3, ?4) \
                         ON CONFLICT(agent_id, kind, name) DO UPDATE SET qty=?4",
                        params![g_agent, g_kind, g_name, from],
                    )?;
                    let cur_r: f64 = c
                        .query_row(
                            "SELECT qty FROM agent_assets WHERE agent_id=?1 AND kind=?2 AND name=?3",
                            params![receiver, g_kind, g_name],
                            |r| r.get(0),
                        )
                        .unwrap_or(0.0);
                    c.execute(
                        "INSERT INTO agent_assets (agent_id, kind, name, qty) VALUES (?1, ?2, ?3, ?4) \
                         ON CONFLICT(agent_id, kind, name) DO UPDATE SET qty=?4",
                        params![receiver, g_kind, g_name, cur_r + g_qty],
                    )?;
                }
                // The unique thing changes hands — same transaction, so a
                // failed item flip aborts the money too.
                if let Some((item_id, current_owner)) = &item {
                    let to = if *current_owner == a_give.agent_id {
                        &b_give.agent_id
                    } else {
                        &a_give.agent_id
                    };
                    let n = c.execute(
                        "UPDATE environment_items SET owner = ?2, status = 'sold', updated_at = ?3 \
                         WHERE id = ?1 AND owner = ?4 AND status != 'sold'",
                        params![item_id, to, now_ms(), current_owner],
                    )?;
                    if n != 1 {
                        return Err(StorageError::Insufficient(format!(
                            "item {item_id} is not transferable from '{current_owner}' (already sold, or not the owner)"
                        )));
                    }
                }
                c.execute(
                    "INSERT INTO environment_changes (environment_id, actor, kind, payload, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![eid, actor, lkind, payload, now_ms()],
                )?;
                tx.commit()?;
                Ok(())
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
    async fn exchange_moves_both_sides_atomically() {
        let s = store().await;
        let env = s.create_environment("t", 1000).await.unwrap();
        s.adjust_asset("buyer", "money", "usd", 100.0).await.unwrap();
        s.adjust_asset("seller", "item", "watch", 1.0).await.unwrap();

        s.exchange(
            &env.id, Some("buyer"), "deal",
            r#"{"price":85}"#,
            AssetGive { agent_id: "buyer".into(), kind: "money".into(), name: "usd".into(), qty: 85.0 },
            AssetGive { agent_id: "seller".into(), kind: "item".into(), name: "watch".into(), qty: 1.0 },
            None,
        ).await.unwrap();

        let b = s.assets_of("buyer").await.unwrap();
        let sl = s.assets_of("seller").await.unwrap();
        assert_eq!(b.iter().find(|a| a.name == "usd").unwrap().qty, 15.0);
        assert_eq!(b.iter().find(|a| a.name == "watch").unwrap().qty, 1.0);
        assert_eq!(sl.iter().find(|a| a.name == "usd").unwrap().qty, 85.0);
        assert_eq!(sl.iter().find(|a| a.name == "watch").unwrap().qty, 0.0);

        // Ledger row exists.
        let ledger = s.list_changes(&env.id, Some("deal"), 10).await.unwrap();
        assert_eq!(ledger.len(), 1);
    }

    #[tokio::test]
    async fn exchange_is_all_or_nothing() {
        let s = store().await;
        let env = s.create_environment("t", 1000).await.unwrap();
        s.adjust_asset("broke-buyer", "money", "usd", 10.0).await.unwrap();
        s.adjust_asset("seller", "item", "watch", 1.0).await.unwrap();

        let err = s.exchange(
            &env.id, Some("broke-buyer"), "deal", "{}",
            AssetGive { agent_id: "broke-buyer".into(), kind: "money".into(), name: "usd".into(), qty: 85.0 },
            AssetGive { agent_id: "seller".into(), kind: "item".into(), name: "watch".into(), qty: 1.0 },
            None,
        ).await;

        assert!(err.is_err(), "unaffordable exchange must fail");
        // NOTHING moved: buyer still has $10, seller still has the watch,
        // no ledger row.
        let b = s.assets_of("broke-buyer").await.unwrap();
        assert_eq!(b.iter().find(|a| a.name == "usd").unwrap().qty, 10.0);
        let sl = s.assets_of("seller").await.unwrap();
        assert_eq!(sl.iter().find(|a| a.name == "watch").unwrap().qty, 1.0);
        assert!(s.list_changes(&env.id, Some("deal"), 10).await.unwrap().is_empty());
    }
}
