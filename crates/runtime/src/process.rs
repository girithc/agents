//! The loop: one tokio task per agent.
//!
//! Simple contract: wake → look for work → think → sleep → repeat. The
//! agent is always either thinking or sleeping; sleep length is pulled
//! from the environment (or requested by the brain). Mail — a task hint,
//! a direct message, or stop — interrupts any sleep. All state transitions
//! land in `agent_state`; notes land in `agent_memory`.

use std::sync::Arc;
use std::time::Duration;

use agenthub_storage::store::agent::{AgentStatus, AgentTask, MemoryKind};
use agenthub_storage::Store;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::brain::{AgentTools, Brain, ThinkCtx};
use crate::env::EnvHandle;

/// Anything that can wake an agent mid-wait.
pub enum Mail {
    /// Hint that a task is queued for this agent (the loop pops it itself).
    Task(String),
    /// Someone called the agent directly.
    Message { from: String, body: String },
    Stop,
}

pub struct AgentDeps {
    pub store: Arc<Store>,
    pub env: EnvHandle,
    pub agent_id: String,
}

#[derive(Clone)]
pub struct AgentHandle {
    pub agent_id: String,
    tx: UnboundedSender<Mail>,
    join: Arc<JoinHandle<()>>,
}

impl AgentHandle {
    pub fn send_task(&self, task_id: &str) {
        let _ = self.tx.send(Mail::Task(task_id.to_string()));
    }

    pub fn send_message(&self, from: &str, body: &str) {
        let _ = self.tx.send(Mail::Message {
            from: from.to_string(),
            body: body.to_string(),
        });
    }

    pub fn stop(&self) {
        let _ = self.tx.send(Mail::Stop);
    }

    /// Resolves when the loop has exited.
    pub async fn finished(&self) {
        while !self.join.is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Start an agent's loop on the current tokio runtime.
pub fn spawn_agent_loop(deps: AgentDeps, brain: Arc<dyn Brain>) -> AgentHandle {
    let agent_id = deps.agent_id.clone();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    // List this agent in the phonebook so board changes can knock it awake
    // instantly instead of waiting for the next tick.
    deps.env.phonebook().register(&agent_id, tx.clone());
    let join = tokio::spawn(run(deps, brain, rx));
    AgentHandle {
        agent_id,
        tx,
        join: Arc::new(join),
    }
}

async fn run(
    deps: AgentDeps,
    brain: Arc<dyn Brain>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Mail>,
) {
    let AgentDeps { store, env, agent_id } = deps;
    let tools = AgentTools::new(store.clone(), env.clone(), agent_id.clone()).await;
    let who = tools.agent_name().to_string();
    let model = tools.agent_model().to_string();

    let _ = store.set_state(&agent_id, AgentStatus::Idle, None, None).await;
    tracing::info!(agent = %who, model = %model, "starts working");

    // The agent is always either thinking or sleeping. `sleep_until` is the
    // current sleep's deadline; mail can interrupt any sleep.
    let mut first_pass = true;
    //
    // The first sleep is "zero length" so the loop's own deadline branch
    // does the initial peek — one wake-and-peek path, not two.

    // Honor a durable sleep deadline from a previous life, if still pending.
    let durable_wake = store
        .get_state(&agent_id)
        .await
        .ok()
        .flatten()
        .filter(|st| st.status == AgentStatus::Sleeping)
        .and_then(|st| st.next_wake_at);
    let mut sleep_until = match durable_wake {
        Some(wake_at) if wake_at > agenthub_storage::now_ms() => {
            let now = agenthub_storage::now_ms();
            Some(tokio::time::Instant::now() + Duration::from_millis((wake_at - now) as u64))
        }
        _ => Some(tokio::time::Instant::now()), // immediate first peek
    };

    loop {
        let wait_deadline = async {
            match sleep_until {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        // Brain may override the next sleep length (None = use env cadence).
        let mut requested_sleep: Option<u64> = None;

        tokio::select! {
            mail = rx.recv() => {
                match mail {
                    None | Some(Mail::Stop) => break,
                    Some(Mail::Task(_)) => {
                        tracing::info!(agent = %who, model = %model, "got a task hint");
                        let _ = store.set_state(&agent_id, AgentStatus::Idle, None, None).await;
                        while let Some(task) = pop(&store, &agent_id).await {
                            let out = run_task(&store, brain.as_ref(), &tools, &env, &task).await;
                            requested_sleep = out.and_then(|o| o.sleep_ms);
                        }
                    }
                    Some(Mail::Message { from, body }) => {
                        tracing::info!(agent = %who, model = %model, "got a message from {from}");
                        let _ = store.remember(
                            &agent_id,
                            MemoryKind::Event,
                            &format!("message from {from}: {body}"),
                        ).await;
                        let think_id = uuid::Uuid::new_v4().to_string();
                        let span = tracing::info_span!(
                            "agent.think",
                            run_id = %think_id,
                            agent = %agent_id,
                            task_id = tracing::field::Empty,
                            task_title = "message",
                        );
                        let ctx = think_ctx(&store, &env, &agent_id, &think_id, "message", &body).await;
                        let outcome = run_think(&store, brain.as_ref(), &tools, &ctx, None)
                            .instrument(span)
                            .await;
                        // The mail path uses the outcome's sleep request below.
                        requested_sleep = outcome.and_then(|o| o.sleep_ms);
                    }
                }
            }
            _ = wait_deadline => {
                // Sleep ended on its own: wake, peek for work.
                if !first_pass {
                    tracing::debug!(agent = %who, "{who} woke up");
                }
                first_pass = false;
                let _ = store.set_state(&agent_id, AgentStatus::Idle, None, None).await;
                if let Some(task) = pop(&store, &agent_id).await {
                    let out = run_task(&store, brain.as_ref(), &tools, &env, &task).await;
                    requested_sleep = out.and_then(|o| o.sleep_ms);
                }
            }
        }

        // Sleep again: brain's request wins, otherwise pull the environment's
        // cadence ourselves right before sleeping.
        let ms = match requested_sleep {
            Some(ms) => ms,
            None => {
                tracing::debug!(agent = %who, "no work left, napping");
                env.tick_interval().await.as_millis() as u64
            }
        };
        sleep_until = apply_sleep(&store, &who, &agent_id, ms).await;
    }

    let _ = store.set_state(&agent_id, AgentStatus::Stopped, None, None).await;
    env.phonebook().remove(&agent_id); // delist: no more knocks
    tracing::info!(agent = %who, model = %model, "stopped");
}

async fn pop(store: &Arc<Store>, agent_id: &str) -> Option<AgentTask> {
    store.pop_task(agent_id).await.unwrap_or(None)
}

/// Enter `sleeping` state with a durable wake time, and return the loop's
/// in-memory deadline for it.
async fn apply_sleep(store: &Arc<Store>, who: &str, agent_id: &str, ms: u64) -> Option<tokio::time::Instant> {
    let wake_at = agenthub_storage::now_ms() + ms as i64;
    tracing::debug!(agent = %who, "{who} is going to sleep for {}s", ms / 1000);
    let _ = store
        .set_state(agent_id, AgentStatus::Sleeping, None, Some(wake_at))
        .await;
    Some(tokio::time::Instant::now() + Duration::from_millis(ms))
}

/// Pop-to-done for one task: mark thinking → think → mark done/failed.
/// Returns the brain's outcome (for the sleep request).
async fn run_task(
    store: &Arc<Store>,
    brain: &dyn Brain,
    tools: &AgentTools,
    env: &EnvHandle,
    task: &AgentTask,
) -> Option<crate::brain::ThinkOutcome> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!(
        "agent.think",
        run_id = %run_id,
        agent = %tools.agent_id(),
        task_id = %task.id,
        task_title = %task.title,
    );
    async move {
        let _ = store
            .set_state(tools.agent_id(), AgentStatus::Thinking, Some(&task.id), None)
            .await;
        let ctx = think_ctx(store, env, tools.agent_id(), &run_id, &task.title, &task.body).await;
        let out = run_think(store, brain, tools, &ctx, Some(&task.id)).await;
        let note = out.as_ref().map(|o| o.note.clone()).unwrap_or_default();
        let note = if note.chars().count() > 100 {
            format!("{}...", note.chars().take(100).collect::<String>())
        } else {
            note
        };
        tracing::info!(
            agent = %tools.agent_name(),
            model = %tools.agent_model(),
            kind = "finished",
            thought = %note,
            "finished"
        );
        out
    }
    .instrument(span)
    .await
}

async fn run_think(
    store: &Arc<Store>,
    brain: &dyn Brain,
    tools: &AgentTools,
    ctx: &ThinkCtx,
    task_id: Option<&str>,
) -> Option<crate::brain::ThinkOutcome> {
    let outcome = match brain.think(ctx, tools).await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(agent = %tools.agent_name(), error = %e, "thinking failed");
            let _ = store
                .remember(tools.agent_id(), MemoryKind::Reflection, &format!("think failed: {e}"))
                .await;
            // A failed think still closes its task (failed), so the queue
            // never stalls on a stuck `doing` row.
            if let Some(id) = task_id {
                let _ = store.finish_task(id, false).await;
            }
            return None;
        }
    };
    let _ = store
        .remember(tools.agent_id(), MemoryKind::Reflection, &outcome.note)
        .await;
    if let Some(id) = task_id {
        let _ = store.finish_task(id, outcome.ok).await;
    }
    Some(outcome)
}

async fn think_ctx(
    store: &Arc<Store>,
    env: &EnvHandle,
    agent_id: &str,
    run_id: &str,
    task_title: &str,
    task_body: &str,
) -> ThinkCtx {
    let (soul, goal) = match store.get_soul(agent_id).await.ok().flatten() {
        Some(s) => (s.soul, s.goal),
        None => (String::new(), String::new()),
    };
    ThinkCtx {
        agent_id: agent_id.to_string(),
        run_id: run_id.to_string(),
        task_title: task_title.to_string(),
        task_body: task_body.to_string(),
        soul,
        goal,
        rules: env.rules_for(agent_id).await,
        todo: store.list_tasks(agent_id, None).await.unwrap_or_default(),
        board: env.board_snapshot().await,
    }
}
