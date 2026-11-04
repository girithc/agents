//! `agent_state` — one row per agent: what it's doing right now.

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Idle,
    Thinking,
    Sleeping,
    Stopped,
}

impl AgentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentStatus::Idle => "idle",
            AgentStatus::Thinking => "thinking",
            AgentStatus::Sleeping => "sleeping",
            AgentStatus::Stopped => "stopped",
        }
    }
    fn from_str(s: &str) -> Self {
        match s {
            "thinking" => AgentStatus::Thinking,
            "sleeping" => AgentStatus::Sleeping,
            "stopped" => AgentStatus::Stopped,
            _ => AgentStatus::Idle,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AgentState {
    pub agent_id: String,
    pub status: AgentStatus,
    pub current_task: Option<String>,
    pub next_wake_at: Option<i64>,
    pub updated_at: i64,
}

impl Store {
    /// Upsert the whole state row for an agent.
    pub async fn set_state(
        &self,
        agent_id: &str,
        status: AgentStatus,
        current_task: Option<&str>,
        next_wake_at: Option<i64>,
    ) -> Result<AgentState> {
        let now = now_ms();
        let aid = agent_id.to_string();
        let task = current_task.map(|s| s.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_state (agent_id, status, current_task, next_wake_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(agent_id) DO UPDATE SET status=?2, current_task=?3, \
                     next_wake_at=?4, updated_at=?5",
                    params![aid, status.as_str(), task, next_wake_at, now],
                )
            })
            .await?;
        Ok(AgentState {
            agent_id: agent_id.to_string(),
            status,
            current_task: current_task.map(|s| s.to_string()),
            next_wake_at,
            updated_at: now,
        })
    }

    pub async fn get_state(&self, agent_id: &str) -> Result<Option<AgentState>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT agent_id, status, current_task, next_wake_at, updated_at \
                     FROM agent_state WHERE agent_id = ?1",
                    params![aid],
                    |r| {
                        Ok(AgentState {
                            agent_id: r.get(0)?,
                            status: AgentStatus::from_str(&r.get::<_, String>(1)?),
                            current_task: r.get(2)?,
                            next_wake_at: r.get(3)?,
                            updated_at: r.get(4)?,
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

    /// All live (non-stopped) agents — used to respawn loops after restart.
    pub async fn list_states(&self) -> Result<Vec<AgentState>> {
        self.db
            .exec(|c| {
                let mut stmt = c.prepare(
                    "SELECT agent_id, status, current_task, next_wake_at, updated_at \
                     FROM agent_state WHERE status != 'stopped'",
                )?;
                let rows = stmt.query_map([], |r| {
                    Ok(AgentState {
                        agent_id: r.get(0)?,
                        status: AgentStatus::from_str(&r.get::<_, String>(1)?),
                        current_task: r.get(2)?,
                        next_wake_at: r.get(3)?,
                        updated_at: r.get(4)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }

    /// Agents whose sleep deadline has passed (or never got one) — the loop
    /// uses this to wake sleepers after a restart.
    pub async fn due_sleepers(&self, now: i64) -> Result<Vec<AgentState>> {
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT agent_id, status, current_task, next_wake_at, updated_at \
                     FROM agent_state WHERE status = 'sleeping' AND \
                     (next_wake_at IS NULL OR next_wake_at <= ?1)",
                )?;
                let rows = stmt.query_map(params![now], |r| {
                    Ok(AgentState {
                        agent_id: r.get(0)?,
                        status: AgentStatus::from_str(&r.get::<_, String>(1)?),
                        current_task: r.get(2)?,
                        next_wake_at: r.get(3)?,
                        updated_at: r.get(4)?,
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
    async fn state_roundtrip() {
        let s = store().await;
        s.set_state("a1", AgentStatus::Sleeping, None, Some(999))
            .await
            .unwrap();
        let st = s.get_state("a1").await.unwrap().unwrap();
        assert_eq!(st.status, AgentStatus::Sleeping);
        assert_eq!(st.next_wake_at, Some(999));

        s.set_state("a1", AgentStatus::Thinking, Some("t1"), None)
            .await
            .unwrap();
        let st = s.get_state("a1").await.unwrap().unwrap();
        assert_eq!(st.status, AgentStatus::Thinking);
        assert_eq!(st.current_task.as_deref(), Some("t1"));
    }
}
