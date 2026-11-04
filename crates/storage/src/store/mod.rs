//! Repositories over the SQLite database + secrets vault.

use std::path::Path;
use std::sync::Arc;

use crate::db::Db;
use crate::vault::Vault;

mod agents;
pub mod agent;
pub mod environment;
mod secrets;
pub mod traces;

#[derive(Clone)]
pub struct Store {
    pub(crate) db: Db,
    pub(crate) vault: Arc<Vault>,
}

impl Store {
    pub fn open(db_path: &Path, vault: Vault) -> crate::error::Result<Self> {
        let db = Db::open(db_path)?;
        Ok(Self {
            db,
            vault: Arc::new(vault),
        })
    }
}
