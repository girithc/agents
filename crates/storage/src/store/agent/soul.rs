//! `soul` — who the agent is (soul text) and what its end goal is (goal text).

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Soul {
    pub agent_id: String,
    pub soul: String,
    pub goal: String,
    pub updated_at: i64,
}

impl Store {
    pub async fn upsert_soul(&self, agent_id: &str, soul: &str, goal: &str) -> Result<Soul> {
        let now = now_ms();
        let (aid, soul_txt, goal_txt) = (
            agent_id.to_string(),
            soul.to_string(),
            goal.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO soul (agent_id, soul, goal, updated_at) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(agent_id) DO UPDATE SET soul=?2, goal=?3, updated_at=?4",
                    params![aid, soul_txt, goal_txt, now],
                )
            })
            .await?;
        Ok(Soul {
            agent_id: agent_id.to_string(),
            soul: soul.to_string(),
            goal: goal.to_string(),
            updated_at: now,
        })
    }

    pub async fn get_soul(&self, agent_id: &str) -> Result<Option<Soul>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT agent_id, soul, goal, updated_at FROM soul WHERE agent_id = ?1",
                    params![aid],
                    |r| {
                        Ok(Soul {
                            agent_id: r.get(0)?,
                            soul: r.get(1)?,
                            goal: r.get(2)?,
                            updated_at: r.get(3)?,
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
