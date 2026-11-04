use rusqlite::params;

use super::Store;
use crate::error::Result;
use crate::models::{Feedback, FeedbackSummary, SecretMeta};
use crate::now_ms;

impl Store {
    // ------------------------------------------------------------ secrets

    pub async fn put_secret(&self, name: &str, value: &str) -> Result<()> {
        let blob = self.vault.encrypt(value)?;
        let now = now_ms();
        let name = name.to_string();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO secrets (name, value, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?3) \
                     ON CONFLICT(name) DO UPDATE SET value = excluded.value, \
                     updated_at = excluded.updated_at",
                    params![name, blob, now],
                )
            })
            .await?;
        Ok(())
    }

    /// Fetch and decrypt a secret. Returns `StorageError::SecretNotFound`
    /// if the name is unknown.
    pub async fn get_secret(&self, name: &str) -> Result<String> {
        let name = name.to_string();
        let blob: Option<Vec<u8>> = self
            .db
            .exec({
                let name = name.clone();
                move |c| {
                    c.query_row(
                        "SELECT value FROM secrets WHERE name = ?1",
                        params![name],
                        |r| r.get(0),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })
                }
            })
            .await?;
        match blob {
            Some(blob) => self.vault.decrypt(&blob),
            None => Err(crate::error::StorageError::SecretNotFound(name)),
        }
    }

    pub async fn list_secrets(&self) -> Result<Vec<SecretMeta>> {
        self.db
            .exec(|c| {
                let mut stmt =
                    c.prepare("SELECT name, created_at, updated_at FROM secrets ORDER BY name")?;
                let rows = stmt.query_map([], |r| {
                    Ok(SecretMeta {
                        name: r.get(0)?,
                        created_at: r.get(1)?,
                        updated_at: r.get(2)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }

    pub async fn delete_secret(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.db
            .exec(move |c| {
                c.execute("DELETE FROM secrets WHERE name = ?1", params![name])
                    .map(|n| n > 0)
            })
            .await
    }

    // ------------------------------------------------------------ feedback

    pub async fn add_feedback(
        &self,
        run_id: &str,
        agent_id: &str,
        rating: i64,
        comment: Option<&str>,
    ) -> Result<Feedback> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();
        let run_id = run_id.to_string();
        let agent_id = agent_id.to_string();
        let comment = comment.map(|s| s.to_string());
        let fid = id.clone();
        self.db
            .exec({
                let run_id = run_id.clone();
                let agent_id = agent_id.clone();
                let comment = comment.clone();
                move |c| {
                    c.execute(
                        "INSERT INTO feedback (id, run_id, agent_id, rating, comment, created_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![fid, run_id, agent_id, rating, comment, now],
                    )
                }
            })
            .await?;
        Ok(Feedback {
            id,
            run_id: run_id.to_string(),
            agent_id: agent_id.to_string(),
            rating,
            comment,
            created_at: now,
        })
    }

    pub async fn list_feedback_for_agent(&self, agent_id: &str, limit: u32) -> Result<Vec<Feedback>> {
        let agent_id = agent_id.to_string();
        let limit = limit as i64;
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, run_id, agent_id, rating, comment, created_at FROM feedback \
                     WHERE agent_id = ?1 ORDER BY created_at DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![agent_id, limit], |r| {
                    Ok(Feedback {
                        id: r.get(0)?,
                        run_id: r.get(1)?,
                        agent_id: r.get(2)?,
                        rating: r.get(3)?,
                        comment: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }

    pub async fn feedback_summary(&self, agent_id: &str) -> Result<FeedbackSummary> {
        let agent_id = agent_id.to_string();
        let out_agent = agent_id.clone();
        let (count, avg): (i64, Option<f64>) = self
            .db
            .exec(move |c| {
                c.query_row(
                    "SELECT COUNT(*), AVG(rating) FROM feedback WHERE agent_id = ?1",
                    params![agent_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
            })
            .await?;
        Ok(FeedbackSummary {
            agent_id: out_agent,
            count,
            avg_rating: avg.unwrap_or(0.0),
        })
    }
}
