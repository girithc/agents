//! The loop: one tokio task per agent.
//!
//! Simple contract: wake → look for work → think → sleep → repeat. An
//! empty queue is not rest: the agent gets one think on its own
//! initiative — the soul decides whether to act or sleep. The agent is
//! always either thinking or sleeping; sleep length is pulled from the
//! environment (or requested by the brain). Mail — a task hint, a direct
//! message, or stop — interrupts any sleep. All state transitions
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

/// Naps this long after an idle think that neither acted nor requested a
/// sleep — an eager world stays responsive, a resting one idles cheaply.
const IDLE_BACKOFF_MS: u64 = 30_000;

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

    let _ = store.set_state(&agent_id, AgentStatus::Idle, None, None).await;
    tracing::info!(agent = %who, "starts");

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
        let mut idle_backoff = false;

        tokio::select! {
            mail = rx.recv() => {
                match mail {
                    None | Some(Mail::Stop) => break,
                    Some(Mail::Task(_)) => {
                        tracing::info!(agent = %who, "{who} received a message");
                        let _ = store.set_state(&agent_id, AgentStatus::Idle, None, None).await;
                        // An incoming message may be a reply inside an OPEN
                        // thread — continue that negotiation (its task is
                        // 'doing', not queued, and stays open until the
                        // handshake closes it).
                        let open = store.doing_thread_tasks(&agent_id).await.unwrap_or_default();
                        for task in &open {
                            let out = run_task(&store, brain.as_ref(), &tools, &env, task).await;
                            requested_sleep = out.and_then(|o| o.sleep_ms);
                        }
                        while let Some(task) = pop(&store, &agent_id).await {
                            let out = run_task(&store, brain.as_ref(), &tools, &env, &task).await;
                            requested_sleep = out.and_then(|o| o.sleep_ms);
                        }
                    }
                    Some(Mail::Message { from, body }) => {
                        let think_id = uuid::Uuid::new_v4().to_string();
                        tools.set_think_tag(&think_id[..8]);
                        tracing::info!(agent = %who, t = %think_id[..8].to_string(), "received a message");
                        let _ = store.remember(
                            &agent_id,
                            MemoryKind::Event,
                            &format!("message from {from}: {body}"),
                        ).await;
                        let span = tracing::info_span!(
                            "agent.think",
                            run_id = %think_id,
                            agent = %agent_id,
                            task_id = tracing::field::Empty,
                            task_title = "message",
                        );
                        let ctx = think_ctx(&store, &env, &agent_id, &think_id, None, "message", &body).await;
                        tools.set_current_thread(None, 0);
                        let outcome = run_think(&store, brain.as_ref(), &tools, &ctx, None, true)
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
                match pop(&store, &agent_id).await {
                    Some(task) => {
                        let out = run_task(&store, brain.as_ref(), &tools, &env, &task).await;
                        requested_sleep = out.and_then(|o| o.sleep_ms);
                    }
                    None => {
                        // Waiting on a counterpart's reply anywhere? The mic
                        // is theirs — nap, no model call. (Truth: threads,
                        // not task statuses.)
                        if store.waiting_on_reply(&agent_id).await.unwrap_or(false) {
                            tracing::debug!(agent = %who, "waiting on their reply — mic stays theirs");
                        } else {
                            // An open thread with MY turn: continue the
                            // conversation — the same think path a knock
                            // uses. There is no "reconnect" move in the base.
                            let open =
                                store.doing_thread_tasks(&agent_id).await.unwrap_or_default();
                            match open.first() {
                                Some(t) => {
                                    let out =
                                        run_task(&store, brain.as_ref(), &tools, &env, t).await;
                                    requested_sleep = out.and_then(|o| o.sleep_ms);
                                }
                                None => {
                                    // Truly idle: first contact lives here.
                                    let out = run_idle_think(
                                        &store,
                                        brain.as_ref(),
                                        &tools,
                                        &env,
                                    )
                                    .await;
                                    idle_backoff = out
                                        .as_ref()
                                        .map(|o| o.sleep_ms.is_none())
                                        .unwrap_or(true);
                                    requested_sleep = out.and_then(|o| o.sleep_ms);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sleep again: brain's request wins, otherwise the idle guard's
        // long nap, otherwise the environment's cadence.
        let ms = match requested_sleep {
            Some(ms) => ms,
            None => {
                tracing::debug!(agent = %who, "no work left, napping");
                if idle_backoff {
                    tracing::debug!(agent = %who, "idle think had no action, longer nap");
                    IDLE_BACKOFF_MS
                } else {
                    env.tick_interval().await.as_millis() as u64
                }
            }
        };
        sleep_until = apply_sleep(&store, &who, &agent_id, ms).await;
    }

    let _ = store.set_state(&agent_id, AgentStatus::Stopped, None, None).await;
    env.phonebook().remove(&agent_id); // delist: no more knocks
    tracing::info!(agent = %who, "stopped");
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
        let tag = &run_id[..8];
        tools.set_think_tag(tag);
        let ctx = think_ctx(store, env, tools.agent_id(), &run_id, Some(&task), &task.title, &task.body).await;
        let count = ctx.thread.as_ref().map(|t| t.messages.len()).unwrap_or(0);
        tools.set_current_thread(task.thread_id.as_deref(), count);
        let out = run_think(store, brain, tools, &ctx, Some(&task.id), true).await;
        tracing::info!(agent = %tools.agent_name(), t = %tag, "finished");
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
    remember_note: bool,
) -> Option<crate::brain::ThinkOutcome> {
    let outcome = match brain.think(ctx, tools).await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(agent = %tools.agent_name(), error = %e, "thinking failed");
            let _ = store
                .remember(tools.agent_id(), MemoryKind::Reflection, &format!("think failed: {e}"))
                .await;
            // A failed think closes its task (failed) so the queue never
            // stalls — EXCEPT open-thread tasks, which stay open to retry.
            if let Some(id) = task_id {
                let task = store.get_task(id).await.ok().flatten();
                let thread_open = match task.as_ref().and_then(|t| t.thread_id.as_deref()) {
                    Some(tid) => store.thread_open(tid).await.unwrap_or(false),
                    None => false,
                };
                if !thread_open {
                    let _ = store.finish_task(id, false).await;
                }
            }
            return None;
        }
    };
    if remember_note {
        let _ = store
            .remember(tools.agent_id(), MemoryKind::Reflection, &outcome.note)
            .await;
    }
    if let Some(id) = task_id {
        // Thread tasks stay open while their thread is open — the agents
        // (close_thread/leave_thread/handshake) end them, not the loop.
        let task = store.get_task(id).await.ok().flatten();
        let thread_open = match task.as_ref().and_then(|t| t.thread_id.as_deref()) {
            Some(tid) => store.thread_open(tid).await.unwrap_or(false),
            None => false,
        };
        if !thread_open {
            let _ = store.finish_task(id, outcome.ok).await;
        }
    }
    Some(outcome)
}

/// One think on the agent's own initiative: the queue was empty, so the
/// loop hands the microphone to the soul. Nothing is finished (no task)
/// and nothing is remembered (a resting agent must not spam its own
/// notebook) — the trace keeps the governance record either way.
async fn run_idle_think(
    store: &Arc<Store>,
    brain: &dyn Brain,
    tools: &AgentTools,
    env: &EnvHandle,
) -> Option<crate::brain::ThinkOutcome> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!(
        "agent.think",
        run_id = %run_id,
        agent = %tools.agent_id(),
        task_id = tracing::field::Empty,
        task_title = "self-started",
    );
    async move {
        // If the agent is mid-negotiation, the initiative think sees that
        // thread's transcript — any follow-up continues it; it never
        // re-pitches from scratch.
        let open_thread_task = store
            .doing_thread_tasks(tools.agent_id())
            .await
            .unwrap_or_default()
            .first()
            .cloned();
        tools.set_think_tag(&run_id[..8]);
        let (tid, cnt) = match &open_thread_task {
            Some(t) => match &t.thread_id {
                Some(id) => (Some(id.as_str()), store.thread_messages(id).await.unwrap_or_default().len()),
                None => (None, 0),
            },
            None => (None, 0),
        };
        tools.set_current_thread(tid, cnt);
        let (title, body) = match &open_thread_task {
            Some(t) => (
                t.title.as_str(),
                format!(
                    "You are mid-negotiation (thread open). Last thing you knew: {}. \
                     If waiting on their reply is right, call sleep. Otherwise follow up ONCE, \
                     with the full conversation so far in mind — do not start over.",
                    t.body
                ),
            ),
            None => (
                "self-started",
                "Nothing is queued for you right now. Consult your soul and goal: \
                 if there is anything you should be doing — start work, approach \
                 someone, follow up — act now with your tools. If there is truly \
                 nothing to do, call sleep."
                    .into(),
            ),
        };
        let ctx = think_ctx(
            store,
            env,
            tools.agent_id(),
            &run_id,
            open_thread_task.as_ref(),
            title,
            &body,
        )
        .await;
        match brain.think(&ctx, tools).await {
            Err(e) => {
                tracing::warn!(agent = %tools.agent_name(), error = %e, "idle think failed");
                None
            }
            Ok(out) => {
                tracing::info!(
                    agent = %tools.agent_name(),
                    kind = "finished",
                    thought = %out.note,
                    "finished"
                );
                Some(out)
            }
        }
    }
    .instrument(span)
    .await
}

async fn think_ctx(
    store: &Arc<Store>,
    env: &EnvHandle,
    agent_id: &str,
    run_id: &str,
    task: Option<&AgentTask>,
    task_title: &str,
    task_body: &str,
) -> ThinkCtx {
    let (soul, goal) = match store.get_soul(agent_id).await.ok().flatten() {
        Some(s) => (s.soul, s.goal),
        None => (String::new(), String::new()),
    };
    // The roster is world data — pushed into every think, never re-polled.
    // The flag marks who they ALREADY talk to (open thread between us):
    // no re-approaching them.
    let mut roster = Vec::new();
    for a in store.list_agents().await.unwrap_or_default() {
        if a.id == agent_id {
            continue;
        }
        let talking = store
            .thread_between(agent_id, &a.id)
            .await
            .map(|t| t.is_some())
            .unwrap_or(false);
        roster.push((a.name, a.role, talking));
    }
    // The thread transcript, when this task belongs to a conversation.
    let thread = match task.and_then(|t| t.thread_id.clone()) {
        Some(tid) => {
            let status = if store.thread_open(&tid).await.unwrap_or(false) {
                "open".to_string()
            } else {
                "closed".to_string()
            };
            let turn_id = store.thread_turn(&tid).await.ok().flatten();
            let turn = match turn_id {
                Some(id) => {
                    let name = store
                        .get_agent(&id)
                        .await
                        .ok()
                        .flatten()
                        .map(|a| a.name)
                        .unwrap_or_else(|| id.clone());
                    Some(name)
                }
                None => None,
            };
            let msgs = store.thread_messages(&tid).await.unwrap_or_default();
            let mut view = Vec::new();
            for m in msgs {
                let who = store
                    .get_agent(&m.sender)
                    .await
                    .ok()
                    .flatten()
                    .map(|a| a.name)
                    .unwrap_or_else(|| m.sender.clone());
                view.push((who, m.body));
            }
            Some(crate::brain::ThreadView { id: tid, status, turn, messages: view })
        }
        None => None,
    };
    let assets = store.assets_of(agent_id).await.unwrap_or_default();
    ThinkCtx {
        agent_id: agent_id.to_string(),
        run_id: run_id.to_string(),
        roster,
        thread,
        assets,
        task_title: task_title.to_string(),
        task_body: task_body.to_string(),
        soul,
        goal,
        rules: env.rules_for(agent_id).await,
        todo: store.open_tasks(agent_id).await.unwrap_or_default(),
        board: env.board_snapshot().await,
    }
}
