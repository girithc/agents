//! The brain: what runs inside one "think". Today a stub can be anything;
//! the LLM runtime's run loop becomes the real brain later.

use agenthub_storage::store::agent::AgentTask;
use async_trait::async_trait;

use crate::process::Mail;

/// Everything the brain sees for one think: who it is, what it wants,
/// the rules, its to-do, and the departures board.
#[derive(Debug, Clone)]
pub struct ThreadView {
    pub id: String,
    /// open | closed — is this conversation still live?
    pub status: String,
    /// Whose words come next (name), if anyone's.
    pub turn: Option<String>,
    /// The transcript so far: (sender name, words), oldest first.
    pub messages: Vec<(String, String)>,
}

pub struct ThinkCtx {
    pub agent_id: String,
    /// This think's trace id — spans and provider calls share it.
    pub run_id: String,
    /// Who exists in this world (name, role) — pushed, so the agent
    /// never wastes a tool call to re-check it. The third flag marks
    /// agents the agent ALREADY has an open conversation with: those
    /// must not be re-approached.
    pub roster: Vec<(String, String, bool)>,
    /// The conversation this task belongs to (when it's a thread task).
    pub thread: Option<ThreadView>,
    /// What I own (money, items) — mine only, never others'.
    pub assets: Vec<agenthub_storage::store::agent::Asset>,
    /// Title/body of the popped task, or the incoming message that woke it.
    pub task_title: String,
    pub task_body: String,
    pub soul: String,
    pub goal: String,
    pub rules: Vec<String>,
    pub todo: Vec<AgentTask>,
    pub board: Vec<(String, serde_json::Value)>,
}

/// The tools a brain may use while thinking. Read-only views + personal
/// memory; board writes are absent by design (departures-board rule).
pub struct AgentTools {
    store: std::sync::Arc<agenthub_storage::Store>,
    env: crate::env::EnvHandle,
    agent_id: String,
    /// Display name for human-readable logs.
    name: String,
    /// Model id for human-readable governance logs.
    model: String,
    /// Sleep requested by the brain during this think (via the sleep tool).
    sleep_request: std::sync::Mutex<Option<u64>>,
    /// The thread the CURRENT think belongs to (set by the loop per think),
    /// plus how many messages existed when the think's context was built —
    /// the half-duplex guard: if the thread moved since, the reply must be
    /// revised, not sent.
    current_thread: std::sync::Mutex<Option<(String, usize)>>,
    /// Short id of the current think — binds its log lines together.
    think_tag: std::sync::Mutex<String>,
}

impl AgentTools {
    pub async fn new(
        store: std::sync::Arc<agenthub_storage::Store>,
        env: crate::env::EnvHandle,
        agent_id: impl Into<String>,
    ) -> Self {
        let agent_id = agent_id.into();
        let (name, model) = match store.get_agent(&agent_id).await.ok().flatten() {
            Some(a) => (a.name, a.model),
            None => (agent_id.clone(), "unknown".to_string()),
        };
        Self {
            store,
            env,
            agent_id,
            name,
            model,
            sleep_request: std::sync::Mutex::new(None),
            current_thread: std::sync::Mutex::new(None),
            think_tag: std::sync::Mutex::new(String::new()),
        }
    }

    /// Bind this think's log lines with a short shared id.
    pub fn set_think_tag(&self, tag: &str) {
        *self.think_tag.lock().unwrap() = tag.to_string();
    }

    pub fn think_tag(&self) -> String {
        self.think_tag.lock().unwrap().clone()
    }

    /// The loop pins the thread of the task being thought about, and the
    /// message count its context was built from.
    pub fn set_current_thread(&self, thread_id: Option<&str>, known_count: usize) {
        *self.current_thread.lock().unwrap() =
            thread_id.map(|s| (s.to_string(), known_count));
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Human-readable name (falls back to the id).
    pub fn agent_name(&self) -> &str {
        &self.name
    }

    /// Model id backing this agent (falls back to "unknown").
    pub fn agent_model(&self) -> &str {
        &self.model
    }

    /// For the domain-tool layer: direct store + env access.
    pub fn store(&self) -> &std::sync::Arc<agenthub_storage::Store> {
        &self.store
    }

    pub fn env(&self) -> &crate::env::EnvHandle {
        &self.env
    }

    pub async fn remember(
        &self,
        kind: agenthub_storage::store::agent::MemoryKind,
        content: &str,
    ) {
        let _ = self.store.remember(&self.agent_id, kind, content).await;
    }

    pub async fn recall(&self, query: &str, limit: i64) -> Vec<agenthub_storage::store::agent::MemoryRow> {
        self.store
            .recall(&self.agent_id, query, limit)
            .await
            .unwrap_or_default()
    }

    pub async fn todo(&self) -> Vec<AgentTask> {
        self.store
            .list_tasks(&self.agent_id, None)
            .await
            .unwrap_or_default()
    }

    /// Add work to its own queue.
    pub async fn todo_add(&self, title: &str, body: &str) -> Option<AgentTask> {
        self.store.enqueue_task(&self.agent_id, title, body).await.ok()
    }

    /// Close one of its own tasks.
    pub async fn todo_finish(&self, task_id: &str, ok: bool) {
        let _ = self.store.finish_task(task_id, ok).await;
    }

    /// The brain's sleep tool: "don't wake me for this long".
    pub fn request_sleep_ms(&self, ms: u64) {
        *self.sleep_request.lock().unwrap() = Some(ms);
    }

    /// The loop takes (and clears) the request after the think ends.
    pub fn take_sleep_request(&self) -> Option<u64> {
        self.sleep_request.lock().unwrap().take()
    }

    /// My own wallet + possessions.
    pub async fn my_assets(&self) -> Vec<agenthub_storage::store::agent::Asset> {
        self.store.assets_of(&self.agent_id).await.unwrap_or_default()
    }

    pub async fn board_get(&self, key: &str) -> Option<serde_json::Value> {
        self.env.board_get(key).await
    }

    pub async fn board_snapshot(&self) -> Vec<(String, serde_json::Value)> {
        self.env.board_snapshot().await
    }

    /// Follow a board key: every future change to it lands in this agent's
    /// inbox and wakes it. The agent's explicit choice — touching a key is
    /// not following it.
    pub async fn subscribe(&self, key: &str) {
        let _ = self.store.subscribe(&self.agent_id, key).await;
    }

    /// Stop following a board key.
    pub async fn unsubscribe(&self, key: &str) {
        let _ = self.store.unsubscribe(&self.agent_id, key).await;
    }

    /// Speak with another agent. Your words join the open thread between
    /// you two (or open one), land in their durable inbox, and a knock
    /// wakes them now if they're live. Both parties keep their own task
    /// row for the thread — words are shared, perspectives are not.
    pub async fn talk_to(&self, who: &str, what: &str) -> Result<(), String> {
        let other = self
            .store
            .get_agent(who)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no agent '{who}'"))?;
        if other.id == self.agent_id {
            return Err("that's you — talk_to someone else".into());
        }

        // No re-approaching: if we already have an open conversation and
        // the floor is THEIRS (I answered, waiting on reply), refuse — the
        // model must go to someone else or sleep.
        if let Some(existing) = self
            .store
            .thread_between(&self.agent_id, &other.id)
            .await
            .map_err(|e| e.to_string())?
        {
            if self.store.thread_open(&existing).await.map_err(|e| e.to_string())? {
                let turn = self.store.thread_turn(&existing).await.map_err(|e| e.to_string())?;
                if turn.as_deref() != Some(self.agent_id.as_str())
                    && self.current_thread.lock().unwrap().is_none()
                {
                    return Err(format!(
                        "you already have an open conversation with {who} and are waiting \
                         on their reply — do not approach them again. Sleep, or talk to \
                         someone else on your roster."
                    ));
                }
            }
        }

        // Find or open the thread between us (current think's thread wins).
        let cur = self.current_thread.lock().unwrap().clone();
        let (thread_id, known_count) = match cur {
            Some(pair) => pair,
            None => match self.store.thread_between(&self.agent_id, &other.id).await.map_err(|e| e.to_string())? {
                Some(t) => (t, 0),
                None => {
                    let t = self
                        .store
                        .open_thread(&self.env.environment_id().to_string(), &self.agent_id)
                        .await
                        .map_err(|e| e.to_string())?;
                    (t, 0)
                }
            },
        };

        // Half-duplex: if anything arrived while this think was running,
        // the pre-generated words are stale — the sender must revise.
        let sent = self
            .store
            .send_thread_message_strict(&thread_id, &self.agent_id, &other.id, what, known_count)
            .await
            .map_err(|e| e.to_string())?;
        if let Err((fresh_count, fresh)) = sent {
            // The sender has now SEEN the fresh words (they're in this
            // error) — advance their pin, so a revised resend goes through
            // unless yet another message lands mid-revision.
            self.set_current_thread(Some(&thread_id), fresh_count);
            let mut lines = Vec::new();
            for (sender_id, said) in fresh.iter().rev() {
                let who = if *sender_id == other.id {
                    other.name.clone()
                } else {
                    self.store
                        .get_agent(sender_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|a| a.name)
                        .unwrap_or_else(|| sender_id.clone())
                };
                lines.push(format!("{who} said: \"{said}\""));
            }
            return Err(format!(
                "channel moved while you were thinking — {}. \
                 Revise your reply with their words and try talk_to again.",
                lines.join(" | ")
            ));
        }
        self.set_current_thread(Some(&thread_id), known_count + 1);

        // My own thread task (my version): keep it open while we talk.
        if self
            .store
            .open_task_for_thread(&self.agent_id, &thread_id)
            .await
            .map_err(|e| e.to_string())?
            .is_none()
        {
            self.store
                .enqueue_task_in_thread(
                    &self.agent_id,
                    &format!("talking with {}", other.name),
                    what,
                    &thread_id,
                )
                .await
                .map_err(|e| e.to_string())?;
        }

        // Their thread task (their version): one open task per thread.
        let task = match self
            .store
            .open_task_for_thread(&other.id, &thread_id)
            .await
            .map_err(|e| e.to_string())?
        {
            Some(t) => t,
            None => self
                .store
                .enqueue_task_in_thread(
                    &other.id,
                    &format!("talking with {}", self.name),
                    what,
                    &thread_id,
                )
                .await
                .map_err(|e| e.to_string())?,
        };

        self.env.phonebook().knock(&other.id, Mail::Task(task.id));
        Ok(())
    }

    /// Leave the thread you're in: your task closes, the counterpart is
    /// notified IN the thread ("left the negotiation") and knocked awake.
    /// What they do next — leave too, or try to re-engage — is theirs.
    pub async fn leave_thread(&self) -> Result<(), String> {
        let cur = self.current_thread.lock().unwrap().clone();
        let Some((thread_id, _)) = cur else {
            return Err("no thread open right now".into());
        };

        // Who's on the other side (from the last message in the thread).
        let msgs = self
            .store
            .thread_messages(&thread_id)
            .await
            .map_err(|e| e.to_string())?;
        let other = msgs
            .last()
            .map(|m| {
                if m.sender == self.agent_id {
                    m.recipient.clone()
                } else {
                    m.sender.clone()
                }
            });

        // My own version of the task: settled as done (I chose to leave).
        if let Some(t) = self
            .store
            .open_task_for_thread(&self.agent_id, &thread_id)
            .await
            .map_err(|e| e.to_string())?
        {
            self.store.finish_task(&t.id, true).await.map_err(|e| e.to_string())?;
        }

        // The notification is a thread message — visible in their transcript.
        if let Some(other_id) = &other {
            self.store
                .add_thread_message(
                    &thread_id,
                    &self.agent_id,
                    other_id,
                    "left the negotiation",
                )
                .await
                .map_err(|e| e.to_string())?;
            // Wake them so they see it now; their open task, their choice.
            if let Some(t) = self
                .store
                .open_task_for_thread(other_id, &thread_id)
                .await
                .map_err(|e| e.to_string())?
            {
                self.env.phonebook().knock(other_id, Mail::Task(t.id));
            }
        }
        self.set_current_thread(None, 0);
        Ok(())
    }

    /// Close the thread you're in: "we're done here." Settles BOTH
    /// parties' tasks, closes the thread, and tells the other side in
    /// the transcript + a knock. The words (goodbyes, deal talk) ride
    /// the conversation as usual — this is just the door.
    pub async fn close_thread(&self) -> Result<(), String> {
        let cur = self.current_thread.lock().unwrap().clone();
        let Some((thread_id, _)) = cur else {
            return Err("no thread open right now".into());
        };
        let msgs = self
            .store
            .thread_messages(&thread_id)
            .await
            .map_err(|e| e.to_string())?;
        let other = msgs
            .last()
            .map(|m| {
                if m.sender == self.agent_id {
                    m.recipient.clone()
                } else {
                    m.sender.clone()
                }
            });

        if let Some(other_id) = &other {
            self.store
                .add_thread_message(
                    &thread_id,
                    &self.agent_id,
                    other_id,
                    "closed the conversation",
                )
                .await
                .map_err(|e| e.to_string())?;
            self.store
                .close_threads_between(&self.agent_id, other_id)
                .await
                .map_err(|e| e.to_string())?;
            self.env.phonebook().knock(other_id, Mail::Task(String::new()));
        }
        self.set_current_thread(None, 0);
        Ok(())
    }

    /// The roster: who exists in this world, by name and role. Names and
    /// roles only — never anyone's inventory, budget, or prices.
    pub async fn directory(&self) -> Vec<(String, String)> {
        self.store
            .list_agents()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.name, a.role))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ThinkOutcome {
    /// true → task marked done, false → failed.
    pub ok: bool,
    /// Short note; stored as memory and on the task record.
    pub note: String,
    /// If set, the agent goes to sleep for this long after the think.
    pub sleep_ms: Option<u64>,
}

#[async_trait]
pub trait Brain: Send + Sync {
    async fn think(&self, ctx: &ThinkCtx, tools: &AgentTools) -> Result<ThinkOutcome, String>;
}
