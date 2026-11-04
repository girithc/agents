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
    role              TEXT NOT NULL DEFAULT '',
    system_prompt     TEXT NOT NULL,
    provider          TEXT NOT NULL DEFAULT 'meta',
    model             TEXT NOT NULL DEFAULT 'muse-spark-1.3-contributor',
    temperature       REAL NOT NULL DEFAULT 0.7,
    max_output_tokens INTEGER NOT NULL DEFAULT 4096,
    max_iterations    INTEGER NOT NULL DEFAULT 6,
    reasoning_effort  TEXT NOT NULL DEFAULT 'medium',
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

-- ------------------------------------------------------------- agent process
-- Long-horizon agent state. All of it lives in the DB so an agent's loop
-- task can die and be respawned without losing anything.

CREATE TABLE IF NOT EXISTS soul (
    agent_id   TEXT PRIMARY KEY,
    soul       TEXT NOT NULL DEFAULT '',
    goal       TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_state (
    agent_id     TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'idle',
    current_task TEXT,
    next_wake_at INTEGER,
    updated_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_tasks (
    id         TEXT PRIMARY KEY,
    agent_id   TEXT NOT NULL,
    title      TEXT NOT NULL,
    body       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'queued',
    created_at INTEGER NOT NULL,
    done_at    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_agent ON agent_tasks(agent_id, status);

CREATE TABLE IF NOT EXISTS agent_memory (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id   TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'event',
    content    TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_memory_agent ON agent_memory(agent_id);

CREATE TABLE IF NOT EXISTS tools (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL DEFAULT 'builtin',
    config     TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_tools (
    agent_id TEXT NOT NULL,
    tool_id  TEXT NOT NULL,
    PRIMARY KEY (agent_id, tool_id)
);

-- Pub/sub: which board keys each agent chose to follow. Explicit only —
-- touching a key is not following it; the agent must decide.
CREATE TABLE IF NOT EXISTS subscriptions (
    agent_id   TEXT NOT NULL,
    key        TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (agent_id, key)
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_key ON subscriptions(key);

-- ------------------------------------------------------------- environment
-- The governing world agents live in: cadence, rules (global + per-agent),
-- and a read-only shared board of live facts (inventory, cash, ...).

CREATE TABLE IF NOT EXISTS environment (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    tick_interval_ms INTEGER NOT NULL DEFAULT 30000,
    created_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS environment_rules (
    id             TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    agent_id       TEXT,
    rule           TEXT NOT NULL,
    created_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_env_rules_env ON environment_rules(environment_id);
CREATE INDEX IF NOT EXISTS idx_env_rules_agent ON environment_rules(agent_id);

CREATE TABLE IF NOT EXISTS environment_state (
    environment_id TEXT NOT NULL,
    key            TEXT NOT NULL,
    value          TEXT NOT NULL DEFAULT '{}',
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (environment_id, key)
);

-- The ledger: one append-only row per world mutation (deal, sale, listing,
-- rule change, ...). Item statuses on the board are a projection of this.
CREATE TABLE IF NOT EXISTS environment_changes (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    environment_id TEXT NOT NULL,
    actor          TEXT,
    kind           TEXT NOT NULL,
    payload        TEXT NOT NULL DEFAULT '{}',
    created_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_env_changes_env ON environment_changes(environment_id, created_at);
CREATE INDEX IF NOT EXISTS idx_env_changes_kind ON environment_changes(environment_id, kind);
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
        // Databases created before agents had a role: add the column. A
        // duplicate-column error means it's already there — fine.
        let _ = conn.execute_batch("ALTER TABLE agents ADD COLUMN role TEXT NOT NULL DEFAULT ''");
        Self::drop_orphaned_tables(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// The one-shot run engine was removed; its tables are orphaned.
    fn drop_orphaned_tables(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "DROP TABLE IF EXISTS run_messages;
             DROP TABLE IF EXISTS runs;",
        )?;
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
