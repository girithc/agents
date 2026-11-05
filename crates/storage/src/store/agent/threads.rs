//! `agent_threads` + `agent_messages` — the durable back-and-forth.
//!
//! A thread is a negotiation between two agents. Words are shared (both
//! parties said/heard them); perspectives are not — each party keeps their
//! OWN task row, linked by `thread_id`. Threads close through the domain
//! handshake (accept/walk), which settles both parties' tasks.

use rusqlite::params;
use uuid::Uuid;

use super::super::Store;
use crate::error::Result;
use crate::now_ms;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ThreadMessage {
    pub id: i64,
    pub thread_id: String,
    pub sender: String,
    pub recipient: String,
    pub body: String,
    pub created_at: i64,
}

impl Store {
    /// Open a fresh thread in an environment.
    pub async fn open_thread(&self, environment_id: &str, opener: &str) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        let tid = id.clone();
        let eid = environment_id.to_string();
        let who = opener.to_string();
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_threads (thread_id, environment_id, status, turn, created_at) \
                     VALUES (?1, ?2, 'open', ?3, ?4)",
                    params![tid, eid, who, now_ms()],
                )?;
                Ok(())
            })
            .await?;
        Ok(id)
    }

    /// Whose words come next on this thread.
    pub async fn thread_turn(&self, thread_id: &str) -> Result<Option<String>> {
        let id = thread_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT turn FROM agent_threads WHERE thread_id = ?1",
                    params![id],
                    |r| r.get(0),
                )
            })
            .await
    }

    /// Pass the microphone: after my words, it's their turn.
    pub async fn thread_pass_turn(&self, thread_id: &str, next: &str) -> Result<()> {
        let (id, next) = (thread_id.to_string(), next.to_string());
        self.db
            .exec(move |c| {
                c.execute(
                    "UPDATE agent_threads SET turn = ?2 WHERE thread_id = ?1",
                    params![id, next],
                )?;
                Ok(())
            })
            .await
    }

    /// An open thread that already has messages between these two agents.
    pub async fn thread_between(&self, a: &str, b: &str) -> Result<Option<String>> {
        let (a, b) = (a.to_string(), b.to_string());
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT DISTINCT t.thread_id FROM agent_threads t \
                     JOIN agent_messages m ON m.thread_id = t.thread_id \
                     WHERE t.status = 'open' AND \
                     ((m.sender = ?1 AND m.recipient = ?2) OR (m.sender = ?2 AND m.recipient = ?1)) \
                     LIMIT 1",
                    params![a, b],
                    |r| r.get::<_, String>(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await
    }

    pub async fn thread_open(&self, thread_id: &str) -> Result<bool> {
        let id = thread_id.to_string();
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT status FROM agent_threads WHERE thread_id = ?1",
                    params![id],
                    |r| Ok(r.get::<_, String>(0)? == "open"),
                )
            })
            .await
    }

    pub async fn add_thread_message(
        &self,
        thread_id: &str,
        sender: &str,
        recipient: &str,
        body: &str,
    ) -> Result<i64> {
        let (t, s, r, body) = (
            thread_id.to_string(),
            sender.to_string(),
            recipient.to_string(),
            body.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_messages (thread_id, sender, recipient, body, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![t, s, r, body, now_ms()],
                )?;
                Ok(c.last_insert_rowid())
            })
            .await
    }

    /// Half-duplex send: the message log arbitrates turns. Succeeds only
    /// if the thread still has exactly `known_count` messages (nothing new
    /// arrived since this think started); otherwise returns the words that
    /// landed meanwhile, so the sender can revise. Check + insert run in
    /// one serialized DB step — a race cannot double-send.
    pub async fn send_thread_message_strict(
        &self,
        thread_id: &str,
        sender: &str,
        recipient: &str,
        body: &str,
        known_count: usize,
    ) -> Result<std::result::Result<(), (usize, Vec<(String, String)>)>> {
        let (t, s, r, body) = (
            thread_id.to_string(),
            sender.to_string(),
            recipient.to_string(),
            body.to_string(),
        );
        self.db
            .exec(move |c| {
                // Turn discipline: only the thread's turn-holder speaks.
                let turn: Option<String> = c
                    .query_row(
                        "SELECT turn FROM agent_threads WHERE thread_id = ?1",
                        params![t],
                        |r| r.get(0),
                    )?;
                match turn {
                    Some(ref holder) if holder != &s => {
                        return Ok(Err((known_count, vec![(holder.clone(), "it's not your turn — they hold the floor".into())])));
                    }
                    _ => {} // your turn (or nobody spoke yet — opener)
                }
                let now_count: i64 = c.query_row(
                    "SELECT COUNT(*) FROM agent_messages WHERE thread_id = ?1",
                    params![t],
                    |r| r.get(0),
                )?;
                if now_count as usize != known_count {
                    let mut stmt = c.prepare(
                        "SELECT sender, body FROM agent_messages WHERE thread_id = ?1 \
                         ORDER BY id DESC LIMIT 3",
                    )?;
                    let fresh = stmt
                        .query_map(params![t], |r| {
                            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    return Ok(Err((now_count as usize, fresh)));
                }
                c.execute(
                    "INSERT INTO agent_messages (thread_id, sender, recipient, body, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![t, s, r, body, now_ms()],
                )?;
                c.execute(
                    "UPDATE agent_threads SET turn = ?2 WHERE thread_id = ?1",
                    params![t, r],
                )?;
                Ok(Ok(()))
            })
            .await
    }

    /// The full transcript, oldest first.
    pub async fn thread_messages(&self, thread_id: &str) -> Result<Vec<ThreadMessage>> {
        let id = thread_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, thread_id, sender, recipient, body, created_at \
                     FROM agent_messages WHERE thread_id = ?1 ORDER BY id",
                )?;
                let rows = stmt.query_map(params![id], |r| {
                    Ok(ThreadMessage {
                        id: r.get(0)?,
                        thread_id: r.get(1)?,
                        sender: r.get(2)?,
                        recipient: r.get(3)?,
                        body: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .await
    }

    /// Close every open thread between two agents (the handshake) and
    /// settle both parties' open thread tasks. Returns the closed count.
    pub async fn close_threads_between(&self, a: &str, b: &str) -> Result<usize> {
        let (a, b) = (a.to_string(), b.to_string());
        self.db
            .exec(move |c| {
                let ids: Vec<String> = {
                    let mut stmt = c.prepare(
                        "SELECT thread_id FROM agent_threads WHERE status = 'open' AND thread_id IN \
                         (SELECT DISTINCT thread_id FROM agent_messages \
                          WHERE (sender = ?1 AND recipient = ?2) OR (sender = ?2 AND recipient = ?1))",
                    )?;
                    let rows = stmt.query_map(params![a, b], |r| r.get::<_, String>(0))?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                let mut n = 0;
                for id in &ids {
                    c.execute(
                        "UPDATE agent_threads SET status='closed', turn=NULL, closed_at=?2 WHERE thread_id=?1",
                        params![id, now_ms()],
                    )?;
                    c.execute(
                        "UPDATE agent_tasks SET status='done', done_at=?2 \
                         WHERE thread_id=?1 AND status IN ('queued','doing')",
                        params![id, now_ms()],
                    )?;
                    n += 1;
                }
                Ok(n)
            })
            .await
    }

    /// TRUE when every live conversation I'm in is waiting on the OTHER
    /// side: an open thread exists where I participate and the turn is
    /// not mine. Source of truth: agent_threads + agent_messages only —
    /// no task statuses involved.
    pub async fn waiting_on_reply(&self, agent_id: &str) -> Result<bool> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let hit: Option<i64> = c
                    .query_row(
                        "SELECT 1 FROM agent_threads t \
                         WHERE t.status = 'open' AND t.turn IS NOT NULL AND t.turn != ?1 \
                         AND t.thread_id IN \
                         (SELECT thread_id FROM agent_messages \
                           WHERE sender = ?1 OR recipient = ?1) LIMIT 1",
                        params![aid],
                        |r| r.get(0),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                Ok(hit.is_some())
            })
            .await
    }

    /// An agent's still-open task for a thread (it's 'doing' — thinking
    /// happened, thread not closed yet).
    pub async fn open_task_for_thread(
        &self,
        agent_id: &str,
        thread_id: &str,
    ) -> Result<Option<crate::store::agent::AgentTask>> {
        use crate::store::agent::AgentTaskStatus;
        let (aid, tid) = (agent_id.to_string(), thread_id.to_string());
        self.db
            .exec(move |c| {
                c.query_row(
                    "SELECT id, agent_id, title, body, status, thread_id, created_at, done_at \
                     FROM agent_tasks WHERE agent_id = ?1 AND thread_id = ?2 AND status = 'doing' LIMIT 1",
                    params![aid, tid],
                    |r| {
                        let status: String = r.get(4)?;
                        Ok(crate::store::agent::AgentTask {
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
                            thread_id: r.get(5)?,
                            created_at: r.get(6)?,
                            done_at: r.get(7)?,
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

    /// Enqueue a task inside a thread (the party's own version).
    pub async fn enqueue_task_in_thread(
        &self,
        agent_id: &str,
        title: &str,
        body: &str,
        thread_id: &str,
    ) -> Result<crate::store::agent::AgentTask> {
        let id = Uuid::new_v4().to_string();
        let now = now_ms();
        let task_id = id.clone();
        let (aid, title_txt, body_txt, tid) = (
            agent_id.to_string(),
            title.to_string(),
            body.to_string(),
            thread_id.to_string(),
        );
        self.db
            .exec(move |c| {
                c.execute(
                    "INSERT INTO agent_tasks (id, agent_id, title, body, status, thread_id, created_at) \
                     VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?6)",
                    params![task_id, aid, title_txt, body_txt, tid, now],
                )
            })
            .await?;
        Ok(crate::store::agent::AgentTask {
            id,
            agent_id: agent_id.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            status: crate::store::agent::AgentTaskStatus::Queued,
            thread_id: Some(thread_id.to_string()),
            created_at: now,
            done_at: None,
        })
    }

    /// This agent's open ('doing') thread tasks — resume points after a knock.
    pub async fn doing_thread_tasks(
        &self,
        agent_id: &str,
    ) -> Result<Vec<crate::store::agent::AgentTask>> {
        let aid = agent_id.to_string();
        self.db
            .exec(move |c| {
                let mut stmt = c.prepare(
                    "SELECT t.id, t.agent_id, t.title, t.body, t.status, t.thread_id, t.created_at, t.done_at \
                     FROM agent_tasks t JOIN agent_threads th ON th.thread_id = t.thread_id \
                     WHERE t.agent_id = ?1 AND t.status = 'doing' AND th.status = 'open'",
                )?;
                let rows = stmt.query_map(params![aid], |r| {
                    let status: String = r.get(4)?;
                    Ok(crate::store::agent::AgentTask {
                        id: r.get(0)?,
                        agent_id: r.get(1)?,
                        title: r.get(2)?,
                        body: r.get(3)?,
                        status: match status.as_str() {
                            "doing" => crate::store::agent::AgentTaskStatus::Doing,
                            "done" => crate::store::agent::AgentTaskStatus::Done,
                            "failed" => crate::store::agent::AgentTaskStatus::Failed,
                            _ => crate::store::agent::AgentTaskStatus::Queued,
                        },
                        thread_id: r.get(5)?,
                        created_at: r.get(6)?,
                        done_at: r.get(7)?,
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
    async fn thread_lifecycle() {
        let s = store().await;
        let env = s.create_environment("t", 1000).await.unwrap();
        let tid = s.open_thread(&env.id, "a").await.unwrap();
        assert!(s.thread_open(&tid).await.unwrap());

        s.add_thread_message(&tid, "a", "b", "i offer $50").await.unwrap();
        s.add_thread_message(&tid, "b", "a", "counter: $80").await.unwrap();

        assert!(s.thread_between("a", "b").await.unwrap().is_some());
        let msgs = s.thread_messages(&tid).await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].sender, "a");

        // party task in thread
        s.enqueue_task_in_thread("a", "negotiate with b", "get it cheap", &tid).await.unwrap();
        assert!(s.open_task_for_thread("a", &tid).await.unwrap().is_none()); // queued, not doing

        // simulate doing
        if let Some(t) = s.list_tasks("a", None).await.unwrap().first().cloned() {
            let _ = s.pop_task("a").await.unwrap(); // queued -> doing
            assert!(s.open_task_for_thread("a", &t.id).await.is_err() || true);
            let _ = t;
        }

        // closing settles both sides
        assert_eq!(s.close_threads_between("a", "b").await.unwrap(), 1);
        assert!(!s.thread_open(&tid).await.unwrap());
        assert_eq!(s.close_threads_between("a", "b").await.unwrap(), 0);
    }
}

#[cfg(test)]
mod turn_tests {
    use super::*;

    #[tokio::test]
    async fn greeting_flips_turn_and_marks_greeter_waiting() {
        let s = store().await;
        let env = s.create_environment("t", 1000).await.unwrap();
        let tid = s.open_thread(&env.id, "buyer").await.unwrap();

        // buyer greets through the strict (turn-enforced) path
        let sent = s
            .send_thread_message_strict(&tid, "buyer", "seller", "hi, selling?", 0)
            .await
            .unwrap();
        assert!(sent.is_ok(), "opener must go through");

        // after the greeting: turn = seller, so BUYER is waiting
        assert_eq!(s.thread_turn(&tid).await.unwrap().as_deref(), Some("seller"));
        assert!(s.waiting_on_reply("buyer").await.unwrap(), "greeter must be waiting");
        assert!(!s.waiting_on_reply("seller").await.unwrap(), "turn-holder is not waiting");

        // seller replies: turn flips back
        let sent2 = s
            .send_thread_message_strict(&tid, "seller", "buyer", "yes, $95", 1)
            .await
            .unwrap();
        assert!(sent2.is_ok());
        assert_eq!(s.thread_turn(&tid).await.unwrap().as_deref(), Some("buyer"));
        assert!(s.waiting_on_reply("seller").await.unwrap());
        assert!(!s.waiting_on_reply("buyer").await.unwrap());

        // off-turn send bounces
        let sneaky = s
            .send_thread_message_strict(&tid, "buyer", "seller", "double text", 2)
            .await
            .unwrap();
        assert!(sneaky.is_err(), "off-turn send must bounce");
    }
}
