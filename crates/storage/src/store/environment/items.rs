//! `environment_items` — the world's registry of UNIQUE things.
//!
//! One row per individual (watch #12, watch #13), ownership as a field
//! (NULL = unowned stock). This is world state, not agent state and not a
//! public ledger: only the owner reads their rows, non-owners get
//! not-found (even existence stays private), and every transfer of
//! ownership is audited in the ledger by the caller (the handshake).
//! Fungibles (money, grain) stay in `agent_assets` as quantities.

use rusqlite::params;
use uuid::Uuid;

use crate::error::Result;
use crate::now_ms;

/// One individual thing in the world.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Item {
    pub id: String,
    pub environment_id: String,
    pub kind: String,
    pub name: String,
    pub owner: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

fn row_to_item(r: &rusqlite::Row<'_>) -> rusqlite::Result<Item> {
    Ok(Item {
        id: r.get("id")?,
        environment_id: r.get("environment_id")?,
        kind: r.get("kind")?,
        name: r.get("name")?,
        owner: r.get("owner")?,
        status: r.get("status")?,
        created_at: r.get("created_at")?,
        updated_at: r.get("updated_at")?,
    })
}

const ITEM_COLS: &str =
    "id, environment_id, kind, name, owner, status, created_at, updated_at";

impl super::super::Store {
    /// Bring a new individual into the world (optionally already owned).
    pub async fn mint_item(
        &self,
        environment_id: &str,
        kind: &str,
        name: &str,
        owner: Option<&str>,
    ) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        let get_id = id.clone();
        let (eid, k, n, o, now) = (
            environment_id.to_string(),
            kind.to_string(),
            name.to_string(),
            owner.map(|s| s.to_string()),
            now_ms(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO environment_items (id, environment_id, kind, name, owner, status, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 'available', ?6, ?6)",
                    params![id, eid, k, n, o, now],
                )
            })
            .await?;
        Ok(get_id)
    }

    /// MY things in this world — the only item lookup an agent gets.
    pub async fn items_of(&self, environment_id: &str, agent_id: &str) -> Result<Vec<Item>> {
        let (eid, aid) = (environment_id.to_string(), agent_id.to_string());
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT {ITEM_COLS} FROM environment_items \
                     WHERE environment_id = ?1 AND owner = ?2 ORDER BY created_at",
                ))?;
                let rows = stmt.query_map(rusqlite::params![eid, aid], row_to_item)?;
                rows.collect()
            })
            .await
    }

    /// Look one item up — but only if you own it. Non-owners get None:
    /// not "denied", so even existence stays private.
    pub async fn item_get_for(&self, item_id: &str, caller: &str) -> Result<Option<Item>> {
        let (iid, who) = (item_id.to_string(), caller.to_string());
        self.db
            .exec(move |c| {
                c.query_row(
                    &format!(
                        "SELECT {ITEM_COLS} FROM environment_items \
                         WHERE id = ?1 AND owner = ?2"
                    ),
                    rusqlite::params![iid, who],
                    row_to_item,
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    /// The current owner of an item (base-internal routing: the verbs
    /// resolve who receives an offer; the model never browses this).
    pub async fn item_owner(&self, item_id: &str) -> Result<Option<Option<String>>> {
        let iid = item_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT owner FROM environment_items WHERE id = ?1",
                    params![iid],
                    |r| r.get::<_, Option<String>>(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    /// Flip an item's status (available → pending → sold); verbs only.
    pub async fn set_item_status(&self, item_id: &str, status: &str) -> Result<()> {
        let (iid, st, now) = (item_id.to_string(), status.to_string(), now_ms());
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE environment_items SET status = ?2, updated_at = ?3 WHERE id = ?1",
                    params![iid, st, now],
                )
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::agent::AssetGive;
    use crate::store::Store;

    async fn store() -> Store {
        let dir = std::env::temp_dir().join(format!("agenthub-test-{}", uuid::Uuid::new_v4()));
        Store::open(&dir.join("test.db"), crate::Vault::open(&dir.join("master.key")).unwrap())
            .expect("open store")
    }

    #[tokio::test]
    async fn items_are_private_and_transferable() {
        let s = store().await;
        let env = s.create_environment("t", 1000).await.unwrap();

        // Two individual watches — two ids, same name.
        let w1 = s.mint_item(&env.id, "item", "watch", Some("seller")).await.unwrap();
        let w2 = s.mint_item(&env.id, "item", "watch", Some("seller")).await.unwrap();
        assert_ne!(w1, w2);

        // Owner sees both; a stranger sees NOTHING (not even existence).
        let mine = s.items_of(&env.id, "seller").await.unwrap();
        assert_eq!(mine.len(), 2);
        assert!(s.item_get_for(&w1, "buyer").await.unwrap().is_none(), "non-owner must not see the item");
        assert!(s.item_get_for(&w1, "seller").await.unwrap().is_some());

        // A trade moves ownership atomically: buyer pays $75, item flips.
        s.adjust_asset("buyer", "money", "usd", 100.0).await.unwrap();
        s.exchange(
            &env.id,
            Some("buyer"),
            "deal",
            &format!(r#"{{"item":"{w1}","price":75}}"#),
            AssetGive { agent_id: "buyer".into(), kind: "money".into(), name: "usd".into(), qty: 75.0 },
            AssetGive { agent_id: "seller".into(), kind: "nothing".into(), name: "nothing".into(), qty: 0.0 },
            Some((&w1, "seller")),
        )
        .await
        .unwrap();

        let buyers = s.items_of(&env.id, "buyer").await.unwrap();
        assert_eq!(buyers.len(), 1);
        assert_eq!(buyers[0].id, w1);
        assert_eq!(buyers[0].status, "sold");
        // The seller keeps only watch #2.
        assert_eq!(s.items_of(&env.id, "seller").await.unwrap().iter().map(|i| i.id.clone()).collect::<Vec<_>>(), vec![w2.clone()]);

        // A second trade on the SAME item must fail (already sold).
        let again = s.exchange(
            &env.id,
            Some("seller"),
            "deal",
            "{}",
            AssetGive { agent_id: "seller".into(), kind: "nothing".into(), name: "nothing".into(), qty: 0.0 },
            AssetGive { agent_id: "buyer".into(), kind: "nothing".into(), name: "nothing".into(), qty: 0.0 },
            Some((&w1, "seller")),
        )
        .await;
        assert!(again.is_err(), "a sold item cannot trade again");
        let _ = w2;
    }
}
