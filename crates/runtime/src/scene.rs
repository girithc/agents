//! Scenes: finished worlds built on the base.
//!
//! The base knows loops, brains, memory, the board, the ledger. A scene
//! defines a world: who exists, what exists, what's allowed, which verbs
//! (domain tools), how it starts. Every verb is a tool — that is what
//! keeps scenes decoupled from the engine.

use std::sync::Arc;

use agenthub_storage::Store;
use async_trait::async_trait;
use serde_json::Value;

use crate::env::EnvHandle;
use agenthub_providers::ToolSpec;

/// What a domain tool may touch: the store, the environment, and who is
/// calling. Domain tools are the ONLY things allowed to write the board
/// and append to the ledger.
pub struct DomainCtx<'a> {
    pub store: &'a Store,
    pub env: &'a EnvHandle,
    /// Calling agent's id.
    pub actor: &'a str,
}

#[async_trait]
pub trait DomainTool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    async fn call(&self, args: &Value, ctx: &DomainCtx<'_>) -> Result<Value, String>;
}

/// A finished product: a world definition. `setup` writes all rows
/// (environment, rules, items, agents, souls, grants, seed tasks) and is
/// idempotent-safe to call once per world.
#[async_trait]
pub trait Scene {
    fn name(&self) -> &str;

    /// Build the world in the DB; returns the env handle and agent ids.
    async fn setup(&self, store: &Arc<Store>) -> Result<SceneWorld, String>;

    /// The verbs available in this world (granted per-agent in setup).
    fn domain_tools(&self) -> Vec<Arc<dyn DomainTool>>;
}

#[derive(Debug, Clone)]
pub struct SceneWorld {
    pub env: EnvHandle,
    /// Agent ids created by the scene, in scene-defined order.
    pub agents: Vec<String>,
}
