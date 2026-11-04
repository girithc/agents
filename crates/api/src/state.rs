use agenthub_runtime::Runtime;
use agenthub_storage::Store;
use std::sync::Arc;

/// Shared application state handed to every handler.
pub struct AppState {
    pub store: Arc<Store>,
    pub runtime: Arc<Runtime>,
}

impl AppState {
    pub fn new(store: Arc<Store>, runtime: Arc<Runtime>) -> Self {
        Self { store, runtime }
    }

    pub fn registry_names(&self) -> Vec<String> {
        self.runtime.registry_names()
    }
}
