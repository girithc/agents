//! `agent_memory` — what the agent remembers. Everything it learns goes here
//! as a row; recall is keyword search over its own rows (embeddings later).

use rusqlite::params;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Event,
    Fact,
    Reflection,
}

impl MemoryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Event => "event",
            MemoryKind::Fact => "fact",
            MemoryKind::Reflection => "reflection",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MemoryRow {
    pub id: i64,
    pub agent_id: String,
    pub kind: MemoryKind,
    pub content: String,
    pub created_at: i64,
}

impl Store {
    pub async fn remember(&self, agent_id: &str, kind: MemoryKind, content: &str) -> Result<i64> {
        let (aid, kind, content) = (
            agent_id.to_string(),
            kind.as_str().to_string(),
            content.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_memory (agent_id, kind, content, created_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![aid, kind, content, now_ms()],
                )?;
                Ok(c.last_insert_rowid())
            })
            .await
    }

    /// Most recent memories, newest first.
    pub async fn recent_memory(&self, agent_id: &str, limit: i64) -> Result<Vec<MemoryRow>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, agent_id, kind, content, created_at FROM agent_memory \
                     WHERE agent_id = ?1 ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![aid, limit], row_to_memory)?;
                rows.collect()
            })
            .await
    }

    /// Keyword recall: every word must appear (case-insensitive); rows are
    /// ranked by how many words hit. Good enough to start; embeddings later.
    pub async fn recall(
        &self,
        agent_id: &str,
        query: &str,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        let words: Vec<String> = query
            .to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| w.len() > 1)
            .map(|w| w.to_string())
            .collect();
        if words.is_empty() {
            return self.recent_memory(agent_id, limit).await;
        }
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, agent_id, kind, content, created_at FROM agent_memory \
                     WHERE agent_id = ?1 ORDER BY id DESC LIMIT 2000",
                )?;
                let all: Vec<MemoryRow> = {
                    let rows = stmt.query_map(params![aid], row_to_memory)?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                let mut hits: Vec<(usize, MemoryRow)> = all
                    .into_iter()
                    .map(|m| {
                        let lower = m.content.to_lowercase();
                        let score = words.iter().filter(|w| lower.contains(w.as_str())).count();
                        (score, m)
                    })
                    .filter(|(score, _)| *score > 0)
                    .collect();
                hits.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.id.cmp(&a.1.id)));
                Ok(hits
                    .into_iter()
                    .take(limit as usize)
                    .map(|(_, m)| m)
                    .collect())
            })
            .await
    }
}

fn row_to_memory(r: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRow> {
    let kind: String = r.get(2)?;
    Ok(MemoryRow {
        id: r.get(0)?,
        agent_id: r.get(1)?,
        kind: match kind.as_str() {
            "fact" => MemoryKind::Fact,
            "reflection" => MemoryKind::Reflection,
            _ => MemoryKind::Event,
        },
        content: r.get(3)?,
        created_at: r.get(4)?,
    })
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
    async fn remember_and_recall() {
        let s = store().await;
        s.remember("a1", MemoryKind::Fact, "The Acme deal closes in Q3.")
            .await
            .unwrap();
        s.remember("a1", MemoryKind::Event, "Sent follow-up email to Bob.")
            .await
            .unwrap();
        s.remember("a2", MemoryKind::Fact, "Unrelated memory.").await.unwrap();

        let hits = s.recall("a1", "acme deal", 5).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].content.contains("Acme"));

        let recent = s.recent_memory("a1", 10).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].content, "Sent follow-up email to Bob.");
    }
}
