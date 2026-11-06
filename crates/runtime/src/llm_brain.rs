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
use crate::scene::Tool;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct LlmBrain {
    store: Arc<Store>,
    registry: Arc<Registry>,
    /// Tool rounds per think, hard cap so a confused model can't spin.
    pub max_iterations: u32,
    pub timeout: Duration,
    /// Scene verbs, tried before the personal toolset.
    domain_tools: Vec<Arc<dyn Tool>>,
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
    pub fn with_domain_tools(mut self, tools: Vec<Arc<dyn Tool>>) -> Self {
        self.domain_tools = tools;
        self
    }

    /// One think = a full tool-calling conversation with the model.
    async fn complete_with_tools(
        &self,
        provider: Arc<dyn Provider>,
        mut req: CompletionRequest,
        tools: &AgentTools,
        all: &[Arc<dyn Tool>],
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

            // Full model I/O — persisted as trace events on this think's
            // run_id: what the model SAW and what it DID with it.
            let rid = req.run_id.clone().unwrap_or_default();
            tracing::info!(run_id = %rid, kind = "model_seen", messages = %serde_json::to_string(&req.messages).unwrap_or_default(), "model input");
            tracing::info!(run_id = %rid, kind = "model_said",
                text = %resp.text,
                tools = %serde_json::to_string(&resp.tool_calls).unwrap_or_default(),
                "model output");

            if resp.tool_calls.is_empty() {
                return Ok(resp.text);
            }

            // Feed every tool result back and continue the conversation.
            let mut assistant = ChatMessage::assistant(String::new());
            assistant.tool_calls = resp.tool_calls.clone();
            let mut messages = req.messages.clone();
            messages.push(assistant);
            for call in &resp.tool_calls {
                let result = dispatch_tool(call, tools, all).await;
                tracing::debug!(agent = %tools.agent_id(), tool = %call.name, "tool result: {result}");
                messages.push(ChatMessage::tool(call.id.clone(), result));

                // Terminal tools (sleep) end the turn — no further round.
                if all.iter().any(|t| t.spec().name == call.name && t.is_terminal()) {
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

/// The base organ tools — every agent has them. Each owns its spec, its
/// effect, and its own display phrase. Scene tools implement the same
/// standard trait; the base knows none of them by name.
pub fn base_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(Remember),
        Arc::new(Recall),
        Arc::new(TodoAdd),
        Arc::new(TodoFinish),
        Arc::new(TalkTo),
        Arc::new(Subscribe),
        Arc::new(Unsubscribe),
        Arc::new(LeaveThread),
        Arc::new(CloseThread),
        Arc::new(BoardRead),
        Arc::new(Sleep),
    ]
}

fn obj(props: Value, required: &[&str]) -> Value {
    json!({ "type": "object", "properties": props, "required": required })
}

/// Error suffix for a display line (errors may be long; actions stay short).
fn err_suffix(v: &Value) -> String {
    v.get("error")
        .and_then(Value::as_str)
        .map(|e| format!(" (failed: {e})"))
        .unwrap_or_default()
}

macro_rules! impl_tool {
    ($name:ident, $tool_name:literal, $desc:expr, $params:expr, describe($v:ident) $describe:block, call($args:ident, $tools:ident) $call:block, terminal($term:expr)) => {
        struct $name;
        #[async_trait]
        #[allow(unused_variables)]
        impl Tool for $name {
            fn spec(&self) -> ToolSpec {
                ToolSpec { name: $tool_name.into(), description: $desc.into(), parameters: $params }
            }
            async fn call(&self, args: &Value, ctx: &AgentTools) -> Result<Value, String> {
                let $args = args;
                let $tools = ctx;
                $call
            }
            #[allow(unused_braces)]
            fn describe(&self, $v: &Value) -> String {
                $describe
            }
            fn is_terminal(&self) -> bool {
                $term
            }
        }
    };
}

impl_tool!(Remember, "remember",
    "Store a memory for later recall.",
    obj(json!({
        "kind": { "type": "string", "enum": ["event", "fact", "reflection"] },
        "content": { "type": "string" }
    }), &["content"]),
    describe(v) { format!("remembered something{}", err_suffix(v)) },
    call(args, tools) {
        let kind = match args.get("kind").and_then(Value::as_str) {
            Some("event") => MemoryKind::Event,
            Some("reflection") => MemoryKind::Reflection,
            _ => MemoryKind::Fact,
        };
        match arg_str(args, "content") {
            Some(c) => {
                tools.remember(kind, &c).await;
                Ok(json!({ "ok": true }))
            }
            None => Err("missing 'content'".into()),
        }
    },
    terminal(false));

impl_tool!(Recall, "recall",
    "Search your own memories by keywords.",
    obj(json!({
        "query": { "type": "string" },
        "limit": { "type": "integer" }
    }), &["query"]),
    describe(v) { format!("checked their memory{}", err_suffix(v)) },
    call(args, tools) {
        let query = arg_str(args, "query").unwrap_or_default();
        let limit = args.get("limit").and_then(Value::as_i64).unwrap_or(5);
        let rows = tools.recall(&query, limit).await;
        Ok(json!(rows.iter().map(|r| r.content.clone()).collect::<Vec<_>>()))
    },
    terminal(false));

impl_tool!(FactPut, "fact_put",
    "Set a fact you know — create or overwrite. This is how state changes: \
     'watch_offer = $70' becomes 'watch_offer = $75' by writing again, never \
     by piling up notes. Your facts are always in your context.",
    obj(json!({
        "key": { "type": "string", "description": "short name, e.g. watch_offer" },
        "value": { "type": "string", "description": "the current truth" }
    }), &["key", "value"]),
    describe(v) {
        format!(
            "wrote down {} = {}{}",
            v.get("key").and_then(Value::as_str).unwrap_or("a fact"),
            v.get("value").and_then(Value::as_str).unwrap_or(""),
            err_suffix(v)
        )
    },
    call(args, tools) {
        match (arg_str(args, "key"), arg_str(args, "value")) {
            (Some(key), Some(value)) => {
                tools.fact_put(&key, &value).await;
                Ok(json!({ "ok": true, "key": key, "value": value }))
            }
            (None, _) => Err("missing 'key'".into()),
            (_, None) => Err("missing 'value'".into()),
        }
    },
    terminal(false));

impl_tool!(FactForget, "fact_forget",
    "Drop a fact that is no longer true or no longer yours to track.",
    obj(json!({ "key": { "type": "string" } }), &["key"]),
    describe(v) {
        format!(
            "forgot {}{}",
            v.get("key").and_then(Value::as_str).unwrap_or("a fact"),
            err_suffix(v)
        )
    },
    call(args, tools) {
        match arg_str(args, "key") {
            Some(key) => {
                tools.fact_forget(&key).await;
                Ok(json!({ "ok": true, "key": key }))
            }
            None => Err("missing 'key'".into()),
        }
    },
    terminal(false));

impl_tool!(TodoAdd, "todo_add",
    "Add a task to your own to-do queue.",
    obj(json!({
        "title": { "type": "string" },
        "body": { "type": "string" }
    }), &["title"]),
    describe(v) { format!("added a task{}", err_suffix(v)) },
    call(args, tools) {
        let title = arg_str(args, "title").unwrap_or_else(|| "(untitled)".into());
        let body = arg_str(args, "body").unwrap_or_default();
        match tools.todo_add(&title, &body).await {
            Some(t) => Ok(json!({ "task_id": t.id })),
            None => Err("could not enqueue task".into()),
        }
    },
    terminal(false));

impl_tool!(TodoFinish, "todo_finish",
    "Close one of your queued tasks.",
    obj(json!({
        "task_id": { "type": "string" },
        "ok": { "type": "boolean" }
    }), &["task_id"]),
    describe(v) { format!("completed a task{}", err_suffix(v)) },
    call(args, tools) {
        match arg_str(args, "task_id") {
            Some(id) => {
                let ok = args.get("ok").and_then(Value::as_bool).unwrap_or(true);
                tools.todo_finish(&id, ok).await;
                Ok(json!({ "ok": true }))
            }
            None => Err("missing 'task_id'".to_string()),
        }
    },
    terminal(false));

impl_tool!(TalkTo, "talk_to",
    "Speak with another agent by name — greet them, ask what they have or \
     what they want, negotiate, say anything. Your words land in their \
     inbox and wake them; they reply with their own talk_to. This is how \
     the world runs: conversation.",
    obj(json!({
        "who": { "type": "string", "description": "agent name (roster is in your context)" },
        "what": { "type": "string", "description": "what you say" }
    }), &["who", "what"]),
    describe(v) {
        if v.get("deferred").and_then(Value::as_bool) == Some(true) {
            format!(
                "held the floor for {} (their words arrived mid-think — revise when they return)",
                v.get("to").and_then(Value::as_str).unwrap_or("someone")
            )
        } else {
            format!(
                "spoke with {}{}",
                v.get("to").and_then(Value::as_str).unwrap_or("someone"),
                err_suffix(v)
            )
        }
    },
    call(args, tools) {
        match (arg_str(args, "who"), arg_str(args, "what")) {
            (Some(who), Some(what)) => match tools.talk_to(&who, &what).await {
                Ok(true) => Ok(json!({ "ok": true, "to": who })),
                Ok(false) => Ok(json!({
                    "ok": false, "deferred": true, "to": who,
                    "note": "their words arrived while you were thinking — your words \
                             were NOT sent. The floor returns to you: their fresh words \
                             are in your inbox — revise your reply and send then."
                })),
                Err(e) => Err(e),
            },
            (None, _) => Err("missing 'who'".into()),
            (_, None) => Err("missing 'what'".into()),
        }
    },
    terminal(false));

impl_tool!(Subscribe, "subscribe",
    "Follow a board key: every future change to it is delivered to your \
     inbox and wakes you (including changes you cause yourself). Subscribe \
     only to things you must follow up on.",
    obj(json!({ "key": { "type": "string" } }), &["key"]),
    describe(v) { format!("followed a key{}", err_suffix(v)) },
    call(args, tools) {
        match arg_str(args, "key") {
            Some(key) => {
                tools.subscribe(&key).await;
                Ok(json!({ "ok": true, "key": key }))
            }
            None => Err("missing 'key'".into()),
        }
    },
    terminal(false));

impl_tool!(Unsubscribe, "unsubscribe",
    "Stop following a board key — no more notifications from it.",
    obj(json!({ "key": { "type": "string" } }), &["key"]),
    describe(v) { format!("unfollowed a key{}", err_suffix(v)) },
    call(args, tools) {
        match arg_str(args, "key") {
            Some(key) => {
                tools.unsubscribe(&key).await;
                Ok(json!({ "ok": true }))
            }
            None => Err("missing 'key'".into()),
        }
    },
    terminal(false));

impl_tool!(LeaveThread, "leave_thread",
    "End YOUR part in the conversation you're in: your task closes and the \
     other side is told in the thread. Use it when the matter is off for you.",
    json!({ "type": "object", "properties": {} }),
    describe(v) { format!("left the conversation{}", err_suffix(v)) },
    call(args, tools) {
        tools.leave_thread().await?;
        Ok(json!({ "ok": true }))
    },
    terminal(false));

impl_tool!(CloseThread, "close_thread",
    "End the conversation you're in — 'we're done here'. Settles the task \
     for both sides and tells them. Use it when the matter is truly finished.",
    json!({ "type": "object", "properties": {} }),
    describe(v) { format!("closed the thread{}", err_suffix(v)) },
    call(args, tools) {
        tools.close_thread().await?;
        Ok(json!({ "ok": true }))
    },
    terminal(false));

impl_tool!(BoardRead, "board_read",
    "Read the shared audit board. Omit 'key' to read the whole board.",
    obj(json!({ "key": { "type": "string" } }), &[]),
    describe(v) { format!("read the board{}", err_suffix(v)) },
    call(args, tools) {
        match arg_str(args, "key") {
            Some(key) => {
                let v = tools.board_get(&key).await;
                Ok(json!({ "key": key, "value": v }))
            }
            None => {
                let snap = tools.board_snapshot().await;
                Ok(json!(snap.into_iter().collect::<std::collections::BTreeMap<_, _>>()))
            }
        }
    },
    terminal(false));

struct Sleep;
#[async_trait]
impl Tool for Sleep {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "sleep".into(),
            description: "After this think, sleep for the given duration instead \
                          of the default interval."
                .into(),
            parameters: obj(json!({ "seconds": { "type": "number" } }), &["seconds"]),
        }
    }
    async fn call(&self, args: &Value, ctx: &AgentTools) -> Result<Value, String> {
        let secs = args.get("seconds").and_then(Value::as_f64).unwrap_or(0.0);
        if !secs.is_finite() || secs < 0.0 || secs > 86_400.0 {
            Err("sleep seconds out of range (0..=86400)".into())
        } else {
            ctx.request_sleep_ms((secs * 1000.0) as u64);
            Ok(json!({ "ok": true, "sleep_seconds": secs }))
        }
    }
    fn describe(&self, v: &Value) -> String {
        format!("went to sleep{}", err_suffix(v))
    }
    fn is_terminal(&self) -> bool {
        true
    }
}

/// Run one tool call from the model against the standard registry.
#[tracing::instrument(name = "tool.call", skip_all, fields(tool = %call.name, agent = %tools.agent_id()))]
pub async fn dispatch_tool(
    call: &ToolCall,
    tools: &AgentTools,
    all_tools: &[Arc<dyn Tool>],
) -> String {
    let tool = all_tools.iter().find(|t| t.spec().name == call.name);
    let out = match tool {
        Some(t) => match t.call(&call.arguments, tools).await {
            Ok(v) => v.to_string(),
            Err(e) => json!({ "error": e }).to_string(),
        },
        None => json!({ "error": format!("unknown tool '{}'", call.name) }).to_string(),
    };
    // The tool's own ≤3-word line — base never names scene tools.
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    let said = match tool {
        Some(t) => t.describe(&v),
        None => format!("used {}", call.name),
    };
    tracing::info!(agent = %tools.agent_name(), t = %tools.think_tag(), "{said}");
    out
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
        // Semantic memory: my facts, always current, always here — never
        // re-polled. Update them with fact_put when the world moves; drop
        // them with fact_forget when they stop being true.
        if !ctx.facts.is_empty() {
            system.push_str("\nMY FACTS (keyed, always current — keep them so):\n");
            for (k, v) in &ctx.facts {
                system.push_str(&format!("- {k} = {v}\n"));
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
        let thread_state = match &ctx.thread {
            Some(t) => format!(
                "the conversation is {} — next to speak: {}",
                t.status,
                t.turn.as_deref().unwrap_or("nobody yet")
            ),
            None => "no conversation".into(),
        };
        let user = format!(
            "TASK: {title}\n{body}\n\nCONVERSATION STATUS: {thread_state}\n\nWHAT YOU OWN:\n{assets}\n\nCONVERSATION SO FAR:\n{thread}\n\nYOUR TO-DO:\n{todo}\n\nPEOPLE IN THIS WORLD (name — role, already known; do NOT re-check):\n{roster}\n\nENVIRONMENT BOARD:\n{board}",
            title = ctx.task_title,
            body = ctx.task_body,
            assets = if asset_lines.is_empty() { "(nothing)".into() } else { asset_lines.join("\n") },
            thread_state = thread_state,
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
        let granted_domain: Vec<Arc<dyn Tool>> = match &granted {
            Some(g) => self
                .domain_tools
                .iter()
                .filter(|t| g.contains(&t.spec().name))
                .cloned()
                .collect(),
            None => self.domain_tools.clone(),
        };
        let mut all = base_tools();
        all.extend(granted_domain);

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
            tools: all.iter().map(|t| t.spec()).collect(),
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

        let text = self.complete_with_tools(provider, req, tools, &all).await?;

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
