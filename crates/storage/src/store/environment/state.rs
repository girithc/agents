//! `environment_state` — the departures board. Agents read it; writes happen
//! only inside trusted tool side-effects (never a raw agent write).

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct EnvStateRow {
    pub environment_id: String,
    pub key: String,
    pub value: String, // JSON
    pub updated_at: i64,
}

impl Store {
    /// System/board-side write. Domain tools call this as a side effect;
    /// agents never get this directly.
    pub async fn set_env_state(&self, environment_id: &str, key: &str, value: &str) -> Result<()> {
        let (eid, key, value) = (
            environment_id.to_string(),
            key.to_string(),
            value.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO environment_state (environment_id, key, value, updated_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(environment_id, key) DO UPDATE SET value=?3, updated_at=?4",
                    params![eid, key, value, now_ms()],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn get_env_state(&self, environment_id: &str, key: &str) -> Result<Option<String>> {
        let (eid, key) = (environment_id.to_string(), key.to_string());
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT value FROM environment_state WHERE environment_id=?1 AND key=?2",
                    params![eid, key],
                    |r| r.get::<_, String>(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    /// Whole board (for think context snapshots).
    pub async fn env_snapshot(&self, environment_id: &str) -> Result<Vec<EnvStateRow>> {
        let eid = environment_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT environment_id, key, value, updated_at FROM environment_state \
                     WHERE environment_id = ?1 ORDER BY key",
                )?;
                let rows = stmt.query_map(params![eid], |r| {
                    Ok(EnvStateRow {
                        environment_id: r.get(0)?,
                        key: r.get(1)?,
                        value: r.get(2)?,
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
    async fn board_set_get_snapshot() {
        let s = store().await;
        let env = s.create_environment("butcher", 1000).await.unwrap();
        s.set_env_state(&env.id, "cash", "1208.50").await.unwrap();
        s.set_env_state(&env.id, "inventory:mince", "49.5").await.unwrap();
        s.set_env_state(&env.id, "cash", "1217.00").await.unwrap(); // overwrite

        let cash = s.get_env_state(&env.id, "cash").await.unwrap().unwrap();
        assert_eq!(cash, "1217.00");
        assert!(s.get_env_state(&env.id, "nope").await.unwrap().is_none());

        let board = s.env_snapshot(&env.id).await.unwrap();
        assert_eq!(board.len(), 2);
        assert_eq!(board[0].key, "cash");
    }
}
