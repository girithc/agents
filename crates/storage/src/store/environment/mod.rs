//! Environment tables: the governing world, its rules, and the shared board.
//! All environment-pertaining tables live in this subfolder.

mod changes;
mod items;
mod rules;
mod state;

pub use changes::EnvChange;
pub use items::Item;
pub use rules::EnvRule;
pub use state::EnvStateRow;

use rusqlite::params;
use uuid::Uuid;

use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Environment {
    pub id: String,
    pub name: String,
    pub tick_interval_ms: i64,
    pub created_at: i64,
}

impl super::super::Store {
    pub async fn create_environment(&self, name: &str, tick_interval_ms: i64) -> Result<Environment> {
        let id = Uuid::new_v4().to_string();
        let now = now_ms();
        let (env_id, env_name, tick) = (id.clone(), name.to_string(), tick_interval_ms);
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO environment (id, name, tick_interval_ms, created_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![env_id, env_name, tick, now],
                )
            })
            .await?;
        Ok(Environment {
            id,
            name: name.to_string(),
            tick_interval_ms: tick,
            created_at: now,
        })
    }

    pub async fn get_environment(&self, id_or_name: &str) -> Result<Option<Environment>> {
        let key = id_or_name.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT id, name, tick_interval_ms, created_at FROM environment \
                     WHERE id = ?1 OR name = ?1",
                    params![key],
                    |r| {
                        Ok(Environment {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            tick_interval_ms: r.get(2)?,
                            created_at: r.get(3)?,
                        })
                    },
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }
}
