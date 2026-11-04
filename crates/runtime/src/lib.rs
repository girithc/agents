//! runtime: durable agent processes (the engine).
//!
//! Each agent is one tokio task (the loop) + one brain (the LLM, plugged in
//! later). The loop is mechanical: pop a task → think → apply outcome →
//! wait again. Agents wake three ways: mail/task arrives (active), the
//! environment tick fires (passive peek), or an explicit sleep deadline
//! passes. All state lives in SQLite; a dead loop is respawnable.

pub mod brain;
pub mod env;
pub mod llm_brain;
pub mod phonebook;
pub mod process;
pub mod scene;
pub mod supervisor;
#[cfg(test)]
mod tests;

pub use brain::{AgentTools, Brain, ThinkCtx, ThinkOutcome};
pub use env::EnvHandle;
pub use llm_brain::{dispatch_tool, tool_specs, LlmBrain};
pub use phonebook::Phonebook;
pub use process::{spawn_agent_loop, AgentDeps, AgentHandle, Mail};
pub use scene::{DomainCtx, DomainTool, Scene, SceneWorld};
pub use supervisor::Supervisor;

/// Read MODEL_API_KEY from env or ./.env (first match wins).
/// Convention for the API server, CLI, and tests.
pub fn model_api_key() -> Option<String> {
    if let Ok(k) = std::env::var("MODEL_API_KEY") {
        if !k.trim().is_empty() {
            return Some(k);
        }
    }
    // Works from any crate/test cwd: resolve the workspace-root .env.
    let workspace_env = concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env");
    let dotenv = std::fs::read_to_string(workspace_env)
        .or_else(|_| std::fs::read_to_string(".env"))
        .ok()?;
    for line in dotenv.lines() {
        if let Some(v) = line.strip_prefix("MODEL_API_KEY=") {
            let v = v.trim().trim_matches('"').to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}
