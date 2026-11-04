//! Thin async wrapper around a single SQLite connection.
//!
//! All query execution happens through [`Db::exec`] which hops onto a
//! blocking thread, so the async runtime is never stalled by SQLite work.

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{Result, StorageError};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS agents (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    system_prompt     TEXT NOT NULL,
    provider          TEXT NOT NULL DEFAULT 'meta',
    model             TEXT NOT NULL DEFAULT 'muse-spark-1.3-contributor',
    temperature       REAL NOT NULL DEFAULT 0.7,
    max_output_tokens INTEGER NOT NULL DEFAULT 4096,
    max_iterations    INTEGER NOT NULL DEFAULT 6,
    reasoning_effort  TEXT NOT NULL DEFAULT 'medium',
    memory_enabled    INTEGER NOT NULL DEFAULT 1,
    version           INTEGER NOT NULL DEFAULT 1,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_versions (
    id            TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL,
    version       INTEGER NOT NULL,
    system_prompt TEXT NOT NULL,
    note          TEXT,
    created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_versions_agent ON agent_versions(agent_id);

CREATE TABLE IF NOT EXISTS runs (
    id            TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL,
    agent_version INTEGER NOT NULL,
    kind          TEXT NOT NULL DEFAULT 'regular',
    request_id    TEXT,
    status        TEXT NOT NULL DEFAULT 'running',
    input         TEXT NOT NULL,
    output        TEXT,
    error         TEXT,
    provider      TEXT NOT NULL,
    model         TEXT NOT NULL,
    iterations    INTEGER NOT NULL DEFAULT 0,
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runs_agent ON runs(agent_id);

CREATE TABLE IF NOT EXISTS run_messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_messages_run ON run_messages(run_id);

CREATE TABLE IF NOT EXISTS feedback (
    id         TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL,
    agent_id   TEXT NOT NULL,
    rating     INTEGER NOT NULL,
    comment    TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feedback_agent ON feedback(agent_id);

CREATE TABLE IF NOT EXISTS secrets (
    name       TEXT PRIMARY KEY,
    value      BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS trace_spans (
    span_id   TEXT PRIMARY KEY,
    parent_id TEXT,
    run_id    TEXT,
    name      TEXT NOT NULL,
    target    TEXT NOT NULL,
    start_ts  INTEGER,
    end_ts    INTEGER,
    fields    TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_trace_spans_run ON trace_spans(run_id);

CREATE TABLE IF NOT EXISTS trace_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    span_id  TEXT,
    run_id   TEXT,
    message  TEXT NOT NULL,
    fields   TEXT NOT NULL DEFAULT '{}',
    ts       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trace_events_run ON trace_events(run_id);
"#;

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)?;
            }
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Self::migrate_trace_tables(&conn)?;
        Self::migrate_runs_columns(&conn)?;
        Self::migrate_agents_columns(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Older databases lack the `agents.memory_enabled` column; add it.
    /// (runs.kind / runs.request_id / trace tables have their own migrations.)
    fn migrate_agents_columns(conn: &Connection) -> Result<()> {
        let mut stmt = conn.prepare("PRAGMA table_info(agents)")?;
        let cols: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        if !cols.iter().any(|c| c == "memory_enabled") {
            tracing::warn!("migrating agents table: adding 'memory_enabled' column");
            conn.execute_batch(
                "ALTER TABLE agents ADD COLUMN memory_enabled INTEGER NOT NULL DEFAULT 1",
            )?;
        }
        Ok(())
    }

    /// Older databases lack the `runs.kind` / `runs.request_id` columns;
    /// add them in place.
    fn migrate_runs_columns(conn: &Connection) -> Result<()> {
        let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
        let cols: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        if !cols.iter().any(|c| c == "kind") {
            tracing::warn!("migrating runs table: adding 'kind' column");
            conn.execute_batch(
                "ALTER TABLE runs ADD COLUMN kind TEXT NOT NULL DEFAULT 'regular'",
            )?;
        }
        if !cols.iter().any(|c| c == "request_id") {
            tracing::warn!("migrating runs table: adding 'request_id' column");
            conn.execute_batch("ALTER TABLE runs ADD COLUMN request_id TEXT")?;
        }
        Ok(())
    }

    /// v0.1 of the trace tables keyed spans by the per-process tracing
    /// counter (INTEGER), which collides across server restarts and silently
    /// clobbers old runs' spans. Detect that schema and recreate with
    /// boot-scoped TEXT ids.
    fn migrate_trace_tables(conn: &Connection) -> Result<()> {
        let mut stmt = conn.prepare("PRAGMA table_info(trace_spans)")?;
        let cols: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let span_id_old = cols
            .iter()
            .any(|(name, ty)| name == "span_id" && !ty.to_uppercase().contains("TEXT"));
        if !span_id_old {
            return Ok(());
        }
        tracing::warn!("migrating trace tables: INTEGER span ids -> boot-scoped TEXT ids");
        conn.execute_batch(
            "DROP TABLE IF EXISTS trace_events;
             DROP TABLE IF EXISTS trace_spans;",
        )?;
        conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    /// Run a closure against the connection on a blocking thread.
    pub async fn exec<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        let res = tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|_| StorageError::Crypto("db connection mutex poisoned".into()))?;
            f(&guard).map_err(StorageError::from)
        })
        .await;
        match res {
            Ok(inner) => inner,
            Err(join_err) => Err(StorageError::Crypto(format!("db task join: {join_err}"))),
        }
    }
}
