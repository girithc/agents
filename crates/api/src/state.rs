use agenthub_storage::Store;
use std::sync::Arc;

/// Shared application state handed to every handler.
pub struct AppState {
    pub store: Arc<Store>,
}

impl AppState {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}
