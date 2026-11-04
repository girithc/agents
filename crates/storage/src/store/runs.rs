use rusqlite::{params, Row};
use uuid::Uuid;

use super::Store;
use crate::error::Result;
use crate::models::{Run, RunKind, RunMessage, RunStatus};
use crate::now_ms;

fn row_to_run(row: &Row<'_>) -> rusqlite::Result<Run> {
    Ok(Run {
        id: row.get("id")?,
        agent_id: row.get("agent_id")?,
        agent_version: row.get("agent_version")?,
        kind: match row.get::<usize, String>(3)?.as_str() {
            "improve" => RunKind::Improve,
            _ => RunKind::Regular,
        },
        request_id: row.get(4)?,
        status: match row.get::<usize, String>(5)?.as_str() {
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            _ => RunStatus::Running,
        },
        input: row.get("input")?,
        output: row.get("output")?,
        error: row.get("error")?,
        provider: row.get("provider")?,
        model: row.get("model")?,
        iterations: row.get("iterations")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
    })
}

const RUN_COLS: &str = "id, agent_id, agent_version, kind, request_id, status, input, output, \
     error, provider, model, iterations, started_at, finished_at";

impl Store {
    pub async fn create_run(
        &self,
        agent_id: &str,
        agent_version: i64,
        provider: &str,
        model: &str,
        input: &str,
        kind: RunKind,
        request_id: Option<&str>,
    ) -> Result<Run> {
        let id = Uuid::new_v4().to_string();
        let started = now_ms();
        self.db
            .exec({
                let id = id.clone();
                let agent_id = agent_id.to_string();
                let provider = provider.to_string();
                let model = model.to_string();
                let input = input.to_string();
                let kind = kind.as_str().to_string();
                let request_id = request_id.map(|s| s.to_string());
                move |c| {
                    c.execute(
                        "INSERT INTO runs (id, agent_id, agent_version, kind, request_id, status, \
                         input, provider, model, started_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, 'running', ?6, ?7, ?8, ?9)",
                        params![id, agent_id, agent_version, kind, request_id, input, provider, model, started],
                    )
                }
            })
            .await?;
        Ok(Run {
            id,
            agent_id: agent_id.to_string(),
            agent_version,
            kind,
            request_id: request_id.map(|s| s.to_string()),
            status: RunStatus::Running,
            input: input.to_string(),
            output: None,
            error: None,
            provider: provider.to_string(),
            model: model.to_string(),
            iterations: 0,
            started_at: started,
            finished_at: None,
        })
    }

    pub async fn finish_run(&self, run_id: &str, output: &str, iterations: i64) -> Result<Run> {
        let now = now_ms();
        let run_id = run_id.to_string();
        let output = output.to_string();
        let get_id = run_id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE runs SET status='succeeded', output=?1, iterations=?2, finished_at=?3 \
                     WHERE id=?4",
                    params![output, iterations, now, run_id],
                )
            })
            .await?;
        self.get_run(get_id).await.map(|o| o.expect("run exists"))
    }

    pub async fn fail_run(&self, run_id: &str, error: &str, iterations: i64) -> Result<Run> {
        let now = now_ms();
        let run_id = run_id.to_string();
        let error = error.to_string();
        let get_id = run_id.clone();
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE runs SET status='failed', error=?1, iterations=?2, finished_at=?3 \
                     WHERE id=?4",
                    params![error, iterations, now, run_id],
                )
            })
            .await?;
        self.get_run(get_id).await.map(|o| o.expect("run exists"))
    }

    pub async fn get_run(&self, run_id: impl Into<String>) -> Result<Option<Run>> {
        let run_id = run_id.into();
        self.db
            .exec(move |c| {
                c.query_row(
                    &format!("SELECT {RUN_COLS} FROM runs WHERE id = ?1"),
                    params![run_id],
                    row_to_run,
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    pub async fn list_runs(
        &self,
        agent_id: Option<&str>,
        kind: Option<crate::models::RunKind>,
        limit: u32,
    ) -> Result<Vec<Run>> {
        let agent_id = agent_id.map(|s| s.to_string());
        let kind = kind.map(|k| k.as_str().to_string());
        let limit = limit as i64;
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT {RUN_COLS} FROM runs \
                     WHERE (?1 IS NULL OR agent_id = ?1) \
                     AND (?2 IS NULL OR kind = ?2) \
                     ORDER BY started_at DESC LIMIT ?3"
                ))?;
                let rows = stmt.query_map(params![agent_id, kind, limit], row_to_run)?;
                rows.collect()
            })
            .await
    }

    pub async fn add_message(&self, run_id: &str, role: &str, content: &str) -> Result<()> {
        let now = now_ms();
        let run_id = run_id.to_string();
        let role = role.to_string();
        let content = content.to_string();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO run_messages (run_id, role, content, created_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![run_id, role, content, now],
                )
            })
            .await?;
        Ok(())
    }

    pub async fn list_messages(&self, run_id: &str) -> Result<Vec<RunMessage>> {
        let run_id = run_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, run_id, role, content, created_at FROM run_messages \
                     WHERE run_id = ?1 ORDER BY id ASC",
                )?;
                let rows = stmt.query_map(params![run_id], |r| {
                    Ok(RunMessage {
                        id: r.get(0)?,
                        run_id: r.get(1)?,
                        role: r.get(2)?,
                        content: r.get(3)?,
                        created_at: r.get(4)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }
}
