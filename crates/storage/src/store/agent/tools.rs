//! `tools` + `agent_tools` — the tool registry as data, and which agent
//! can use which tool.

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    Builtin,
    Http,
    Mcp,
}

impl ToolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ToolKind::Builtin => "builtin",
            ToolKind::Http => "http",
            ToolKind::Mcp => "mcp",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Tool {
    pub id: String,
    pub kind: ToolKind,
    pub config: String,
    pub created_at: i64,
}

impl Store {
    /// Register (or update) a tool in the master registry.
    pub async fn register_tool(&self, id: &str, kind: ToolKind, config: &str) -> Result<()> {
        let (id, kind, config) = (
            id.to_string(),
            kind.as_str().to_string(),
            config.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO tools (id, kind, config, created_at) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(id) DO UPDATE SET kind=?2, config=?3",
                    params![id, kind, config, now_ms()],
                )?;
                Ok(())
            })
            .await
    }

    /// Grant an agent access to a tool.
    pub async fn grant_tool(&self, agent_id: &str, tool_id: &str) -> Result<()> {
        let (aid, tid) = (agent_id.to_string(), tool_id.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO agent_tools (agent_id, tool_id) VALUES (?1, ?2)",
                    params![aid, tid],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn revoke_tool(&self, agent_id: &str, tool_id: &str) -> Result<()> {
        let (aid, tid) = (agent_id.to_string(), tool_id.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "DELETE FROM agent_tools WHERE agent_id=?1 AND tool_id=?2",
                    params![aid, tid],
                )?;
                Ok(())
            })
            .await
    }

    /// Tools an agent is allowed to use.
    pub async fn tools_for_agent(&self, agent_id: &str) -> Result<Vec<Tool>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT t.id, t.kind, t.config, t.created_at \
                     FROM tools t JOIN agent_tools at ON at.tool_id = t.id \
                     WHERE at.agent_id = ?1 ORDER BY t.id",
                )?;
                let rows = stmt.query_map(params![aid], |r| {
                    let kind: String = r.get(1)?;
                    Ok(Tool {
                        id: r.get(0)?,
                        kind: match kind.as_str() {
                            "http" => ToolKind::Http,
                            "mcp" => ToolKind::Mcp,
                            _ => ToolKind::Builtin,
                        },
                        config: r.get(2)?,
                        created_at: r.get(3)?,
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
    async fn grant_and_list() {
        let s = store().await;
        s.register_tool("echo", ToolKind::Builtin, "{}").await.unwrap();
        s.register_tool("http_fetch", ToolKind::Http, "{}").await.unwrap();
        s.grant_tool("a1", "echo").await.unwrap();

        let tools = s.tools_for_agent("a1").await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, "echo");

        s.grant_tool("a1", "http_fetch").await.unwrap();
        s.revoke_tool("a1", "echo").await.unwrap();
        let tools = s.tools_for_agent("a1").await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, "http_fetch");
    }
}
