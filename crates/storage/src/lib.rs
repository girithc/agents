//! Persistent storage for agents, environments, secrets, feedback, and traces.
//!
//! SQLite (WAL mode) is the single source of truth; secret values are
//! encrypted at rest with AES-256-GCM via [`crate::vault::Vault`].

mod db;
pub mod error;
pub mod store;
mod vault;

pub mod models;

pub use db::Db;
pub use error::{Result, StorageError};
pub use models::{
    Agent, AgentUpdate, AgentVersion, Feedback, FeedbackSummary, NewAgent, SecretMeta, TraceEvent,
    TraceSpan,
};
pub use store::environment::Item;
pub use store::Store;
pub use vault::Vault;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
