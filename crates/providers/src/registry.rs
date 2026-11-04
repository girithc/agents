use std::collections::HashMap;
use std::sync::RwLock;

use crate::{Provider, ProviderError, Result};

/// Thread-safe provider registry. Providers are registered at startup and
/// looked up by id; one provider failing never affects the others.
#[derive(Default)]
pub struct Registry {
    providers: RwLock<HashMap<String, std::sync::Arc<dyn Provider>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, provider: std::sync::Arc<dyn Provider>) {
        self.providers
            .write()
            .expect("provider registry lock")
            .insert(provider.id().to_string(), provider);
    }

    pub fn get(&self, id: &str) -> Result<std::sync::Arc<dyn Provider>> {
        let names = self.names();
        self.providers
            .read()
            .expect("provider registry lock")
            .get(id)
            .cloned()
            .ok_or_else(|| ProviderError::Unknown(id_leak(id), names))
    }

    pub fn names(&self) -> Vec<String> {
        self.providers
            .read()
            .expect("provider registry lock")
            .keys()
            .cloned()
            .collect()
    }
}

fn id_leak(id: &str) -> &'static str {
    // Registry ids are tiny startup-time strings; leaking keeps the
    // error type simple ('static provider tag like the other variants).
    Box::leak(id.to_string().into_boxed_str())
}
