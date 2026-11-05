//! [`LlmBrain`] — the real brain: an LLM that sees the think context,
//! may call personal tools across several rounds, then answers.
//!
//! System message  = soul + goal + environment rules (who I am, what I
//! want, what the house demands). User message = the task + my to-do +
//! the departures board. Tool results feed back until the model answers
//! with plain text.

use std::sync::Arc;
use std::time::Duration;

use agenthub_providers::{
    registry::Registry, ChatMessage, CompletionRequest, Provider, ToolCall, ToolSpec,
};
use agenthub_storage::store::agent::MemoryKind;
use agenthub_storage::Store;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::panic::AssertUnwindSafe;

use crate::brain::{AgentTools, Brain, ThinkCtx, ThinkOutcome};
use crate::scene::{DomainCtx, DomainTool};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct LlmBrain {
    store: Arc<Store>,
    registry: Arc<Registry>,
    /// Tool rounds per think, hard cap so a confused model can't spin.
    pub max_iterations: u32,
    pub timeout: Duration,
    /// Scene verbs, tried before the personal toolset.
    domain_tools: Vec<Arc<dyn DomainTool>>,
}

impl LlmBrain {
    pub fn new(store: Arc<Store>, registry: Arc<Registry>) -> Self {
        Self {
            store,
            registry,
            max_iterations: 8,
            timeout: DEFAULT_TIMEOUT,
            domain_tools: Vec::new(),
        }
    }

    /// Give this brain the scene's domain tools.
    pub fn with_domain_tools(mut self, tools: Vec<Arc<dyn DomainTool>>) -> Self {
        self.domain_tools = tools;
        self
    }

    /// One think = a full tool-calling conversation with the model.
    async fn complete_with_tools(
        &self,
        provider: Arc<dyn Provider>,
        mut req: CompletionRequest,
        tools: &AgentTools,
        domain: &[Arc<dyn DomainTool>],
    ) -> Result<String, String> {
        for _ in 0..self.max_iterations {
            let resp = {
                let p = provider.clone();
                // Timeout + panic barrier: a hung or panicking provider call
                // becomes a normal error, never a wedged agent.
                let fut = AssertUnwindSafe(p.complete(req.clone()));
                match tokio::time::timeout(self.timeout, futures::FutureExt::catch_unwind(fut)).await
                {
                    Err(_) => Err("provider call timed out".to_string()),
                    Ok(Err(panic)) => {
                        let msg = panic
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown panic".into());
                        Err(format!("provider panicked: {msg}"))
                    }
                    Ok(Ok(r)) => r.map_err(|e| e.to_string()),
                }
            }?;

            if resp.tool_calls.is_empty() {
                return Ok(resp.text);
            }

            // Feed every tool result back and continue the conversation.
            let mut assistant = ChatMessage::assistant(String::new());
            assistant.tool_calls = resp.tool_calls.clone();
            let mut messages = req.messages.clone();
            messages.push(assistant);
            for call in &resp.tool_calls {
                let result = dispatch_tool(call, tools, domain).await;
                tracing::debug!(agent = %tools.agent_id(), tool = %call.name, "tool result: {result}");
                // One governance line per tool use: who[model] did what[why].
                // The reasoning summary is the model's stated "why" this round.
                let said = friendly_line(call, &result);
                let thought = resp.reasoning.clone().unwrap_or_default();
                tracing::info!(
                    agent = %tools.agent_name(),
                    model = %req.model,
                    kind = "action",
                    thought = %thought,
                    "{said}"
                );
                messages.push(ChatMessage::tool(call.id.clone(), result));

                // `sleep` is terminal: going to sleep ends the turn —
                // no further model round, otherwise the model loops.
                if call.name == "sleep" {
                    let note = if resp.text.is_empty() { "[going to sleep]".to_string() } else { resp.text };
                    return Ok(note);
                }
            }
            req.messages = messages;
        }
        Err(format!("no final answer after {} tool rounds", self.max_iterations))
    }
}

// ---------------------------------------------------------------- toolset

/// The personal toolset every LLM brain gets. Domain tools (cut_meat,
/// charge_customer, ...) are added per agent later.
pub fn tool_specs() -> Vec<ToolSpec> {
    let obj = |props: Value, required: &[&str]| {
        json!({ "type": "object", "properties": props, "required": required })
    };
    vec![
        ToolSpec {
            name: "remember".into(),
            description: "Store a memory for later recall.".into(),
            parameters: obj(
                json!({
                    "kind": { "type": "string", "enum": ["event", "fact", "reflection"] },
                    "content": { "type": "string" }
                }),
                &["content"],
            ),
        },
        ToolSpec {
            name: "recall".into(),
            description: "Search your own memories by keywords.".into(),
            parameters: obj(
                json!({
                    "query": { "type": "string" },
                    "limit": { "type": "integer" }
                }),
                &["query"],
            ),
        },
        ToolSpec {
            name: "todo_add".into(),
            description: "Add a task to your own to-do queue.".into(),
            parameters: obj(
                json!({
                    "title": { "type": "string" },
                    "body": { "type": "string" }
                }),
                &["title"],
            ),
        },
        ToolSpec {
            name: "todo_finish".into(),
            description: "Close one of your queued tasks.".into(),
            parameters: obj(
                json!({
                    "task_id": { "type": "string" },
                    "ok": { "type": "boolean" }
                }),
                &["task_id"],
            ),
        },
        ToolSpec {
            name: "talk_to".into(),
            description: "Speak with another agent by name — greet them, ask what they \
                          have or what they want, negotiate, say anything. Your words \
                          land in their inbox and wake them; they reply with their own \
                          talk_to. This is how the world runs: conversation."
                .into(),
            parameters: obj(
                json!({
                    "who": { "type": "string", "description": "agent name (see who_is_here)" },
                    "what": { "type": "string", "description": "what you say" }
                }),
                &["who", "what"],
            ),
        },
        ToolSpec {
            name: "subscribe".into(),
            description: "Follow a board key: every future change to it is delivered \
                          to your inbox and wakes you (including changes you cause \
                          yourself). Subscribe only to things you must follow up on — \
                          casual reads don't need a subscription."
                .into(),
            parameters: obj(
                json!({ "key": { "type": "string", "description": "board key, e.g. item:watch" } }),
                &["key"],
            ),
        },
        ToolSpec {
            name: "unsubscribe".into(),
            description: "Stop following a board key — no more notifications from it."
                .into(),
            parameters: obj(
                json!({ "key": { "type": "string" } }),
                &["key"],
            ),
        },
        ToolSpec {
            name: "leave_thread".into(),
            description: "Leave the negotiation you're in: your task closes and \
                          the other side is notified in the thread. Use it when \
                          the deal is off for you (price out of reach, time up)."
                .into(),
            parameters: json!({ "type": "object", "properties": {} }),
        },
        ToolSpec {
            name: "close_thread".into(),
            description: "End the conversation you're in — 'we're done here'. \
                          Settles the task for both sides and tells them. Use it \
                          when the matter is truly finished (deal closed, chat over)."
                .into(),
            parameters: json!({ "type": "object", "properties": {} }),
        },
        ToolSpec {
            name: "sleep".into(),
            description: "After this think, sleep for the given duration \
                          instead of the default interval.".into(),
            parameters: obj(
                json!({ "seconds": { "type": "number" } }),
                &["seconds"],
            ),
        },
    ]
}

/// Run one tool call from the model. Scene/domain tools are tried first,
/// then the personal toolset.
#[tracing::instrument(name = "tool.call", skip_all, fields(tool = %call.name, agent = %tools.agent_id()))]
pub async fn dispatch_tool(
    call: &ToolCall,
    tools: &AgentTools,
    domain_tools: &[Arc<dyn DomainTool>],
) -> String {
    for dt in domain_tools {
        if dt.spec().name == call.name {
            let ctx = DomainCtx {
                store: tools.store(),
                env: tools.env(),
                actor: tools.agent_id(),
            };
            return match dt.call(&call.arguments, &ctx).await {
                Ok(v) => v.to_string(),
                Err(e) => json!({ "error": e }).to_string(),
            };
        }
    }

    let args = call.arguments.clone();
    let out: Result<Value, String> = match call.name.as_str() {
        "remember" => {
            let content = arg_str(&args, "content");
            let kind = match args.get("kind").and_then(Value::as_str) {
                Some("event") => MemoryKind::Event,
                Some("reflection") => MemoryKind::Reflection,
                _ => MemoryKind::Fact,
            };
            match content {
                Some(c) => {
                    tools.remember(kind, &c).await;
                    Ok(json!({ "ok": true }))
                }
                None => Err("missing 'content'".into()),
            }
        }
        "recall" => {
            let query = arg_str(&args, "query").unwrap_or_default();
            let limit = args.get("limit").and_then(Value::as_i64).unwrap_or(5);
            let rows = tools.recall(&query, limit).await;
            Ok(json!(rows.iter().map(|r| r.content.clone()).collect::<Vec<_>>()))
        }
        "todo_add" => {
            let title = arg_str(&args, "title").unwrap_or_else(|| "(untitled)".into());
            let body = arg_str(&args, "body").unwrap_or_default();
            match tools.todo_add(&title, &body).await {
                Some(t) => Ok(json!({ "task_id": t.id })),
                None => Err("could not enqueue task".into()),
            }
        }
        "todo_finish" => match arg_str(&args, "task_id") {
            Some(id) => {
                let ok = args.get("ok").and_then(Value::as_bool).unwrap_or(true);
                tools.todo_finish(&id, ok).await;
                Ok(json!({ "ok": true }))
            }
            None => Err("missing 'task_id'".to_string()),
        }
        "talk_to" => match (arg_str(&args, "who"), arg_str(&args, "what")) {
            (Some(who), Some(what)) => match tools.talk_to(&who, &what).await {
                Ok(()) => Ok(json!({ "ok": true, "to": who })),
                Err(e) => Err(e),
            },
            (None, _) => Err("missing 'who'".into()),
            (_, None) => Err("missing 'what'".into()),
        },
        "unsubscribe" => match arg_str(&args, "key") {
            Some(key) => {
                tools.unsubscribe(&key).await;
                Ok(json!({ "ok": true, "following": null }))
            }
            None => Err("missing 'key'".into()),
        },
        "leave_thread" => match tools.leave_thread().await {
            Ok(()) => Ok(json!({ "ok": true })),
            Err(e) => Err(e),
        },
        "sleep" => {
            let secs = args.get("seconds").and_then(Value::as_f64).unwrap_or(0.0);
            if !(secs.is_finite()) || secs < 0.0 || secs > 86_400.0 {
                Err("sleep seconds out of range (0..=86400)".into())
            } else {
                tools.request_sleep_ms((secs * 1000.0) as u64);
                Ok(json!({ "ok": true, "sleep_seconds": secs }))
            }
        }
        other => Err(format!("unknown tool '{other}'")),
    };
    let out = match out {
        Ok(v) => v.to_string(),
        Err(e) => json!({ "error": e }).to_string(),
    };
    out
}

/// Verb phrase for the common tools; falls back to "used <tool>".
fn friendly_line(call: &ToolCall, out: &str) -> String {
    let v: Value = serde_json::from_str(out).unwrap_or_default();
    let err = v.get("error").and_then(Value::as_str).map(|e| format!(" (failed: {e})"))
        .unwrap_or_default();
    match call.name.as_str() {
        "send_offer" => format!("offered ${:.0}{err}",
            v.get("offered").and_then(Value::as_f64).unwrap_or(0.0)),
        "accept_deal" => format!("closed the deal at ${:.0}{err}",
            v.get("deal_price").and_then(Value::as_f64).unwrap_or(0.0)),
        "remember" => format!("remembered something{err}"),
        "recall" => format!("checked their memory{err}"),
        "todo_add" => format!("added a task to their list{err}"),
        "todo_finish" => format!("ticked off a task{err}"),
        "board_read" => format!("looked at the board{err}"),
        "talk_to" => format!(
            "spoke with {}{err}",
            v.get("to").and_then(Value::as_str).unwrap_or("someone")
        ),
        "subscribe" => format!(
            "started following {}{err}",
            v.get("following").and_then(Value::as_str).unwrap_or("a key")
        ),
        "unsubscribe" => format!("stopped following {}{err}",
            v.get("following").and_then(Value::as_str).unwrap_or("a key")),
        "leave_thread" => format!("left the negotiation{err}"),
        "close_thread" => format!("closed the conversation{err}"),
        "sleep" => format!("went to sleep{err}"),
        other => format!("used {other}{err}"),
    }
}

fn arg_str(args: &Value, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(|s| s.to_string())
}

// ---------------------------------------------------------------- impl

#[async_trait]
impl Brain for LlmBrain {
    async fn think(&self, ctx: &ThinkCtx, tools: &AgentTools) -> Result<ThinkOutcome, String> {
        // Agent config decides provider/model/temperature.
        let agent = self.store.get_agent(&ctx.agent_id).await.map_err(|e| e.to_string())?;

        // System: who I am, what I want, what the house demands.
        let mut system = String::new();
        if !ctx.soul.is_empty() {
            system.push_str("SOUL:\n");
            system.push_str(&ctx.soul);
            system.push_str("\n\n");
        }
        if !ctx.goal.is_empty() {
            system.push_str("GOAL:\n");
            system.push_str(&ctx.goal);
            system.push_str("\n\n");
        }
        if !ctx.rules.is_empty() {
            system.push_str("HOUSE RULES:\n");
            for r in &ctx.rules {
                system.push_str(&format!("- {r}\n"));
            }
        }
        system.push_str(
            "\nHOW TO WORK: use the tools you need (usually 1-3 calls), then ALWAYS \
             finish by replying with a short plain-text summary of what you did — \
             a reply with NO tool call. Never repeat a tool call with the same \
             arguments. NEVER call `sleep` while your current task still needs an \
             action — finish the task first. `sleep` immediately ends your turn and \
             is only for when the task is done (or impossible) and nothing else is \
             queued. The people (roster) and the conversation so far are ALREADY in \
             your context — never re-check them. If you are mid-negotiation, CONTINUE \
             it: reply with your next counter or close; do not start over.",
        );

        // User: the task + my to-do + the board.
        let todo_lines: Vec<String> = ctx
            .todo
            .iter()
            .map(|t| format!("- [{}] {} ({})", short_status(&t.status), t.title, t.id))
            .collect();
        let board: std::collections::BTreeMap<String, &Value> =
            ctx.board.iter().map(|(k, v)| (k.clone(), v)).collect();
        let roster_lines: Vec<String> = ctx
            .roster
            .iter()
            .map(|(n, r, talking)| {
                if *talking {
                    format!("- {n} (role: {r}) — you ALREADY have an open conversation with them; never re-approach, wait for their reply")
                } else {
                    format!("- {n} (role: {r})")
                }
            })
            .collect();
        let thread_lines: Vec<String> = ctx
            .thread
            .as_ref()
            .map(|t| t.messages.iter().map(|(who, said)| format!("{who} said: {said}")).collect())
            .unwrap_or_default();
        let asset_lines: Vec<String> = ctx
            .assets
            .iter()
            .map(|a| format!("- {} {}: {}", a.kind, a.name, a.qty))
            .collect();
        let user = format!(
            "TASK: {title}\n{body}\n\nWHAT YOU OWN:\n{assets}\n\nCONVERSATION SO FAR:\n{thread}\n\nYOUR TO-DO:\n{todo}\n\nPEOPLE IN THIS WORLD (name — role, already known; do NOT re-check):\n{roster}\n\nENVIRONMENT BOARD:\n{board}",
            title = ctx.task_title,
            body = ctx.task_body,
            assets = if asset_lines.is_empty() { "(nothing)".into() } else { asset_lines.join("\n") },
            thread = if thread_lines.is_empty() { "(nothing yet — you start)".into() } else { thread_lines.join("\n") },
            todo = if todo_lines.is_empty() { "(empty)".into() } else { todo_lines.join("\n") },
            roster = roster_lines.join("\n"),
            board = serde_json::to_string_pretty(&board).unwrap_or_default(),
        );

        // Scope enforcement: the agent's granted tools (agent_tools table)
        // decide which scene verbs it may call. Personal tools (memory,
        // todo, board read, sleep) are organs every agent has.
        let granted: Option<Vec<String>> = self
            .store
            .tools_for_agent(&ctx.agent_id)
            .await
            .ok()
            .map(|ts| ts.iter().map(|t| t.id.clone()).collect());
        let domain: Vec<Arc<dyn DomainTool>> = match &granted {
            Some(g) => self
                .domain_tools
                .iter()
                .filter(|t| g.contains(&t.spec().name))
                .cloned()
                .collect(),
            None => self.domain_tools.clone(),
        };

        let mut req = CompletionRequest {
            model: agent.as_ref().map(|a| a.model.clone()).unwrap_or_else(|| "mock".into()),
            messages: vec![ChatMessage::system(system), ChatMessage::user(user)],
            temperature: agent.as_ref().map(|a| a.temperature).unwrap_or(0.7),
            max_output_tokens: agent.as_ref().map(|a| a.max_output_tokens).unwrap_or(2048),
            top_p: 1.0,
            reasoning_effort: agent
                .as_ref()
                .map(|a| Some(a.reasoning_effort.clone()))
                .unwrap_or_else(|| Some("medium".into())),
            tools: {
                let mut specs = tool_specs();
                specs.extend(domain.iter().map(|t| t.spec()));
                specs
            },
            // Same think id as the agent.think span: one trace tree.
            run_id: Some(ctx.run_id.clone()),
            api_key: None,
        };

        let provider_id = agent
            .as_ref()
            .map(|a| a.provider.clone())
            .unwrap_or_else(|| "mock".into());
        let provider = self.registry.get(&provider_id).map_err(|e| e.to_string())?;

        // Resolve the provider's API key from the vault, when it needs one.
        
        req.api_key = match provider.secret_name() {
            Some(name) => Some(self.store.get_secret(&name).await.map_err(|e| e.to_string())?),
            None => None,
        };
        if let Some(a) = &agent {
            req.model = a.model.clone();
        }

        let text = self.complete_with_tools(provider, req, tools, &domain).await?;

        Ok(ThinkOutcome {
            ok: true,
            note: text,
            sleep_ms: tools.take_sleep_request(),
        })
    }
}

fn short_status(s: &agenthub_storage::store::agent::AgentTaskStatus) -> &'static str {
    use agenthub_storage::store::agent::AgentTaskStatus::*;
    match s {
        Queued => " ",
        Doing => "~",
        Done => "x",
        Failed => "!",
    }
}
