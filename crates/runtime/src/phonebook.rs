//! [`Phonebook`] — live inboxes for instant knocks.
//!
//! When a board key a subscriber follows changes, the change must wake the
//! agent *now*, not on its next tick. Waking now means writing into that
//! agent's mail channel — whose sender end is created in
//! [`spawn_agent_loop`](crate::process::spawn_agent_loop) and parked here at
//! spawn time. Entries for dead loops are harmless: sending fails and the
//! notification still waits in the agent's durable DB inbox for the next
//! tick.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use crate::process::Mail;

#[derive(Clone, Default)]
pub struct Phonebook {
    senders: Arc<Mutex<HashMap<String, UnboundedSender<Mail>>>>,
}

impl Phonebook {
    pub fn new() -> Self {
        Self::default()
    }

    /// The Supervisor (via the spawn path) lists every agent here.
    pub fn register(&self, agent_id: &str, tx: UnboundedSender<Mail>) {
        self.senders
            .lock()
            .unwrap()
            .insert(agent_id.to_string(), tx);
    }

    /// Delist an agent whose loop has ended.
    pub fn remove(&self, agent_id: &str) {
        self.senders.lock().unwrap().remove(agent_id);
    }

    /// Knock now: deliver mail to a live agent's loop, interrupting any
    /// sleep. Returns false if the agent isn't running (or its channel
    /// closed) — the durable inbox covers that case on the next tick.
    pub fn knock(&self, agent_id: &str, mail: Mail) -> bool {
        self.senders
            .lock()
            .unwrap()
            .get(agent_id)
            .map(|tx| tx.send(mail).is_ok())
            .unwrap_or(false)
    }

    /// Ids of agents with live loops right now.
    pub fn live(&self) -> Vec<String> {
        self.senders.lock().unwrap().keys().cloned().collect()
    }
}
