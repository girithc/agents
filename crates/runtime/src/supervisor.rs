//! [`Supervisor`] — owns all running agent loops: spawn, lookup, and
//! respawn-after-restart from `agent_state` rows.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agenthub_storage::Store;

use crate::brain::Brain;
use crate::env::EnvHandle;
use crate::process::{spawn_agent_loop, AgentDeps, AgentHandle};

pub type BrainFactory = Arc<dyn Fn(&str) -> Arc<dyn Brain> + Send + Sync>;

#[derive(Clone)]
pub struct Supervisor {
    pub store: Arc<Store>,
    pub env: EnvHandle,
    brain_factory: BrainFactory,
    handles: Arc<Mutex<HashMap<String, AgentHandle>>>,
}

impl Supervisor {
    pub fn new(store: Arc<Store>, env: EnvHandle, brain_factory: BrainFactory) -> Self {
        Self {
            store,
            env,
            brain_factory,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start the loop for one agent (no-op if already running).
    pub fn spawn_agent(&self, agent_id: &str) -> Option<AgentHandle> {
        {
            let handles = self.handles.lock().unwrap();
            if handles.contains_key(agent_id) {
                return handles.get(agent_id).cloned();
            }
        }
        let brain = (self.brain_factory)(agent_id);
        let deps = AgentDeps {
            store: self.store.clone(),
            env: self.env.clone(),
            agent_id: agent_id.to_string(),
        };
        let handle = spawn_agent_loop(deps, brain);
        self.handles
            .lock()
            .unwrap()
            .insert(agent_id.to_string(), handle.clone());
        Some(handle)
    }

    pub fn get(&self, agent_id: &str) -> Option<AgentHandle> {
        self.handles.lock().unwrap().get(agent_id).cloned()
    }

    /// Persist every span/event (thinks, tool calls, mail, sleep/wake)
    /// into SQLite trace tables. First call in a process wins.
    pub fn init_trace_persistence(&self) -> Option<tokio::task::JoinHandle<()>> {
        agenthub_telemetry::try_init_with_db(self.store.clone())
    }

    /// Respawn loops for every live (non-stopped) agent after a restart.
    /// Sleepers come back with their remaining deadline honored by the loop.
    pub async fn resume_all(&self) -> Vec<String> {
        let states = self.store.list_states().await.unwrap_or_default();
        let mut resumed = Vec::new();
        for st in states {
            if self.spawn_agent(&st.agent_id).is_some() {
                resumed.push(st.agent_id);
            }
        }
        resumed
    }
}
