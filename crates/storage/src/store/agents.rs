use rusqlite::{params, Row};
use uuid::Uuid;

use super::Store;
use crate::error::Result;
use crate::models::{Agent, AgentUpdate, AgentVersion};
use crate::now_ms;

fn row_to_agent(row: &Row<'_>) -> rusqlite::Result<Agent> {
    Ok(Agent {
        id: row.get("id")?,
        name: row.get("name")?,
        role: row.get("role")?,
        system_prompt: row.get("system_prompt")?,
        provider: row.get("provider")?,
        model: row.get("model")?,
        temperature: row.get("temperature")?,
        max_output_tokens: row.get("max_output_tokens")?,
        max_iterations: row.get("max_iterations")?,
        reasoning_effort: row.get("reasoning_effort")?,
        version: row.get("version")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

const AGENT_COLS: &str = "id, name, role, system_prompt, provider, model, temperature, \
     max_output_tokens, max_iterations, reasoning_effort, version, created_at, updated_at";

impl Store {
    pub async fn create_agent(&self, new: crate::models::NewAgent) -> Result<Agent> {
        let id = Uuid::new_v4().to_string();
        let now = now_ms();
        let get_id = id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agents (id, name, role, system_prompt, provider, model, temperature, \
                     max_output_tokens, max_iterations, reasoning_effort, version, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 1, ?11, ?11)",
                    params![
                        id,
                        new.name,
                        new.role,
                        new.system_prompt,
                        new.provider,
                        new.model,
                        new.temperature,
                        new.max_output_tokens,
                        new.max_iterations,
                        new.reasoning_effort,
                        now
                    ],
                )
            })
            .await?;
        self.get_agent(&get_id).await?.ok_or_else(|| crate::error::StorageError::NotFound("agent".into()))
    }

    pub async fn get_agent(&self, id_or_name: &str) -> Result<Option<Agent>> {
        let key = id_or_name.to_string();
        self.db
            .exec(move |c| {
                let by_id: Option<Agent> = c
                    .query_row(
                        &format!("SELECT {AGENT_COLS} FROM agents WHERE id = ?1"),
                        params![key],
                        row_to_agent,
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                if by_id.is_some() {
                    return Ok(by_id);
                }
                c.query_row(
                    &format!("SELECT {AGENT_COLS} FROM agents WHERE name = ?1"),
                    params![key],
                    row_to_agent,
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    pub async fn list_agents(&self) -> Result<Vec<Agent>> {
        self.db
            .exec(|c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT {AGENT_COLS} FROM agents ORDER BY created_at DESC"
                ))?;
                let rows = stmt.query_map([], row_to_agent)?;
                rows.collect()
            })
            .await
    }

    pub async fn update_agent(&self, id: &str, upd: AgentUpdate) -> Result<Option<Agent>> {
        let existing = match self.get_agent(id).await? {
            Some(a) => a,
            None => return Ok(None),
        };
        let a = Agent {
            name: upd.name.unwrap_or(existing.name),
            role: upd.role.unwrap_or(existing.role),
            system_prompt: upd.system_prompt.unwrap_or(existing.system_prompt),
            provider: upd.provider.unwrap_or(existing.provider),
            model: upd.model.unwrap_or(existing.model),
            temperature: upd.temperature.unwrap_or(existing.temperature),
            max_output_tokens: upd
                .max_output_tokens
                .unwrap_or(existing.max_output_tokens),
            max_iterations: upd.max_iterations.unwrap_or(existing.max_iterations),
            reasoning_effort: upd.reasoning_effort.unwrap_or(existing.reasoning_effort),
            ..existing
        };
        let now = now_ms();
        let id_owned = a.id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE agents SET name=?1, role=?2, system_prompt=?3, provider=?4, model=?5, \
                     temperature=?6, max_output_tokens=?7, max_iterations=?8, reasoning_effort=?9, \
                     updated_at=?10 WHERE id=?11",
                    params![
                        a.name,
                        a.role,
                        a.system_prompt,
                        a.provider,
                        a.model,
                        a.temperature,
                        a.max_output_tokens,
                        a.max_iterations,
                        a.reasoning_effort,
                        now,
                        id_owned
                    ],
                )
            })
            .await?;
        self.get_agent(id).await
    }

    pub async fn delete_agent(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.db
            .exec(move |c| {
                c.execute("DELETE FROM agents WHERE id = ?1", params![id])
                    .map(|n| n > 0)
            })
            .await
    }

    /// Snapshot the current system prompt as an immutable version row and
    /// bump the agent's version pointer. Used by the improve feedback loop.
    pub async fn save_agent_version(
        &self,
        agent_id: &str,
        version: i64,
        system_prompt: &str,
        note: Option<&str>,
    ) -> Result<AgentVersion> {
        let vid = Uuid::new_v4().to_string();
        let created = now_ms();
        let agent_id = agent_id.to_string();
        let prompt = system_prompt.to_string();
        let note = note.map(|s| s.to_string());
        let out_id = vid.clone();
        let out_agent = agent_id.clone();
        let out_note = note.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_versions (id, agent_id, version, system_prompt, note, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![out_id, agent_id, version, prompt, note, created],
                )
            })
            .await?;
        Ok(AgentVersion {
            id: vid,
            agent_id: out_agent,
            version,
            system_prompt: system_prompt.to_string(),
            note: out_note,
            created_at: created,
        })
    }

    pub async fn set_agent_prompt_version(
        &self,
        agent_id: &str,
        system_prompt: &str,
        version: i64,
    ) -> Result<Option<Agent>> {
        let now = now_ms();
        let agent_id = agent_id.to_string();
        let prompt = system_prompt.to_string();
        let get_id = agent_id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE agents SET system_prompt = ?1, version = ?2, updated_at = ?3 WHERE id = ?4",
                    params![prompt, version, now, agent_id],
                )
            })
            .await?;
        self.get_agent(&get_id).await
    }

    pub async fn list_agent_versions(&self, agent_id: &str) -> Result<Vec<AgentVersion>> {
        let agent_id = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, agent_id, version, system_prompt, note, created_at \
                     FROM agent_versions WHERE agent_id = ?1 ORDER BY version DESC",
                )?;
                let rows = stmt.query_map(params![agent_id], |r| {
                    Ok(AgentVersion {
                        id: r.get(0)?,
                        agent_id: r.get(1)?,
                        version: r.get(2)?,
                        system_prompt: r.get(3)?,
                        note: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }
}
