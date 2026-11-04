//! `agent_tasks` — the work queue per agent.

use rusqlite::params;
use uuid::Uuid;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskStatus {
    Queued,
    Doing,
    Done,
    Failed,
}

impl AgentTaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentTaskStatus::Queued => "queued",
            AgentTaskStatus::Doing => "doing",
            AgentTaskStatus::Done => "done",
            AgentTaskStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AgentTask {
    pub id: String,
    pub agent_id: String,
    pub title: String,
    pub body: String,
    pub status: AgentTaskStatus,
    pub created_at: i64,
    pub done_at: Option<i64>,
}

impl Store {
    pub async fn enqueue_task(&self, agent_id: &str, title: &str, body: &str) -> Result<AgentTask> {
        let now = now_ms();
        let (task_id, aid, title_txt, body_txt) = (
            Uuid::new_v4().to_string(),
            agent_id.to_string(),
            title.to_string(),
            body.to_string(),
        );
        let id = task_id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_tasks (id, agent_id, title, body, status, created_at) \
                     VALUES (?1, ?2, ?3, ?4, 'queued', ?5)",
                    params![task_id, aid, title_txt, body_txt, now],
                )
            })
            .await?;
        Ok(AgentTask {
            id,
            agent_id: agent_id.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            status: AgentTaskStatus::Queued,
            created_at: now,
            done_at: None,
        })
    }

    /// Pop the oldest queued task and mark it `doing`.
    pub async fn pop_task(&self, agent_id: &str) -> Result<Option<AgentTask>> {
        let aid = agent_id.to_string();
        let popped = self
            .db
            .exec(move |c| {
                let id: Option<String> = c
                    .query_row(
                        "SELECT id FROM agent_tasks WHERE agent_id = ?1 AND status = 'queued' \
                         ORDER BY created_at LIMIT 1",
                        params![aid],
                        |r| r.get(0),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                match id {
                    Some(id) => {
                        c.execute(
                            "UPDATE agent_tasks SET status='doing' WHERE id=?1",
                            params![id],
                        )?;
                        Ok(id)
                    }
                    None => Ok(String::new()), // nothing queued
                }
            })
            .await?;
        if popped.is_empty() {
            return Ok(None);
        }
        self.get_task(&popped).await
    }

    pub async fn finish_task(&self, task_id: &str, ok: bool) -> Result<()> {
        let (id, status, now) = (
            task_id.to_string(),
            if ok { "done" } else { "failed" },
            now_ms(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE agent_tasks SET status=?2, done_at=?3 WHERE id=?1",
                    params![id, status, now],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<AgentTask>> {
        let id = task_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT id, agent_id, title, body, status, created_at, done_at \
                     FROM agent_tasks WHERE id = ?1",
                    params![id],
                    |r| {
                        let status: String = r.get(4)?;
                        Ok(AgentTask {
                            id: r.get(0)?,
                            agent_id: r.get(1)?,
                            title: r.get(2)?,
                            body: r.get(3)?,
                            status: match status.as_str() {
                                "doing" => AgentTaskStatus::Doing,
                                "done" => AgentTaskStatus::Done,
                                "failed" => AgentTaskStatus::Failed,
                                _ => AgentTaskStatus::Queued,
                            },
                            created_at: r.get(5)?,
                            done_at: r.get(6)?,
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

    /// The agent's to-do list: queued + doing by default, or any status filter.
    pub async fn list_tasks(
        &self,
        agent_id: &str,
        status: Option<AgentTaskStatus>,
    ) -> Result<Vec<AgentTask>> {
        let (aid, status) = (agent_id.to_string(), status.map(|s| s.as_str().to_string()));
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, agent_id, title, body, status, created_at, done_at \
                     FROM agent_tasks WHERE agent_id = ?1 \
                     AND (?2 IS NULL OR status = ?2) \
                     ORDER BY created_at",
                )?;
                let rows = stmt.query_map(params![aid, status], |r| {
                    let status: String = r.get(4)?;
                    Ok(AgentTask {
                        id: r.get(0)?,
                        agent_id: r.get(1)?,
                        title: r.get(2)?,
                        body: r.get(3)?,
                        status: match status.as_str() {
                            "doing" => AgentTaskStatus::Doing,
                            "done" => AgentTaskStatus::Done,
                            "failed" => AgentTaskStatus::Failed,
                            _ => AgentTaskStatus::Queued,
                        },
                        created_at: r.get(5)?,
                        done_at: r.get(6)?,
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
    async fn queue_fifo() {
        let s = store().await;
        let t1 = s.enqueue_task("a1", "first", "b").await.unwrap();
        s.enqueue_task("a1", "second", "b").await.unwrap();

        let popped = s.pop_task("a1").await.unwrap().unwrap();
        assert_eq!(popped.id, t1.id);
        assert_eq!(popped.status, AgentTaskStatus::Doing);

        s.finish_task(&t1.id, true).await.unwrap();
        let done = s.get_task(&t1.id).await.unwrap().unwrap();
        assert_eq!(done.status, AgentTaskStatus::Done);

        let next = s.pop_task("a1").await.unwrap().unwrap();
        assert_eq!(next.title, "second");

        let open = s.list_tasks("a1", None).await.unwrap();
        assert_eq!(open.len(), 2);
        let queued = s.list_tasks("a1", Some(AgentTaskStatus::Queued)).await.unwrap();
        assert_eq!(queued.len(), 0);
        let doing = s.list_tasks("a1", Some(AgentTaskStatus::Doing)).await.unwrap();
        assert_eq!(doing.len(), 1);
    }
}
