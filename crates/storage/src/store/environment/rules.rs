//! `environment_rules` — house rules. `agent_id IS NULL` = global rule,
//! otherwise scoped to that one agent.

use rusqlite::params;
use uuid::Uuid;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct EnvRule {
    pub id: String,
    pub environment_id: String,
    pub agent_id: Option<String>,
    pub rule: String,
    pub created_at: i64,
}

impl Store {
    pub async fn add_rule(
        &self,
        environment_id: &str,
        agent_id: Option<&str>,
        rule: &str,
    ) -> Result<EnvRule> {
        let id = Uuid::new_v4().to_string();
        let now = now_ms();
        let (rule_id, eid, aid, txt) = (
            id.clone(),
            environment_id.to_string(),
            agent_id.map(|s| s.to_string()),
            rule.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO environment_rules (id, environment_id, agent_id, rule, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![rule_id, eid, aid, txt, now],
                )
            })
            .await?;
        Ok(EnvRule {
            id,
            environment_id: environment_id.to_string(),
            agent_id: agent_id.map(|s| s.to_string()),
            rule: rule.to_string(),
            created_at: now,
        })
    }

    pub async fn remove_rule(&self, rule_id: &str) -> Result<bool> {
        let id = rule_id.to_string();
        self.db
            .exec(move |c| {
                c.execute("DELETE FROM environment_rules WHERE id = ?1", params![id])
                    .map(|n| n > 0)
            })
            .await
    }

    /// The rules an agent must obey: global rules + its own scoped rules.
    pub async fn rules_for(&self, environment_id: &str, agent_id: &str) -> Result<Vec<EnvRule>> {
        let (eid, aid) = (environment_id.to_string(), agent_id.to_string());
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, environment_id, agent_id, rule, created_at \
                     FROM environment_rules WHERE environment_id = ?1 \
                     AND (agent_id IS NULL OR agent_id = ?2) ORDER BY created_at",
                )?;
                let rows = stmt.query_map(params![eid, aid], |r| {
                    Ok(EnvRule {
                        id: r.get(0)?,
                        environment_id: r.get(1)?,
                        agent_id: r.get(2)?,
                        rule: r.get(3)?,
                        created_at: r.get(4)?,
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
    async fn rules_scoping() {
        let s = store().await;
        let env = s.create_environment("butcher", 1000).await.unwrap();
        s.add_rule(&env.id, None, "Be polite to customers.").await.unwrap();
        s.add_rule(&env.id, Some("butcher-1"), "Never sell expired meat.").await.unwrap();

        let for_butcher = s.rules_for(&env.id, "butcher-1").await.unwrap();
        assert_eq!(for_butcher.len(), 2);

        let for_cashier = s.rules_for(&env.id, "cashier-1").await.unwrap();
        assert_eq!(for_cashier.len(), 1);
        assert!(for_cashier[0].agent_id.is_none());
    }
}
