//! Read authorization from the sync state's page inventory.
use notion_knowledge_core::{
    inventory::InventoryEntry,
    root_scope::{Membership, ScopeError},
};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use std::{path::PathBuf, sync::Arc};

/// Opens the shared state file read-only for each lookup, so a page placed
/// or removed by another process's walk is seen at once. No state file, or
/// one not yet created, authorizes nothing.
pub(crate) struct StateInventory(Option<PathBuf>);

impl Membership for StateInventory {
    fn parent_chain(&self, page: &str) -> Result<Option<Vec<InventoryEntry>>, ScopeError> {
        let Some(path) = &self.0 else {
            return Ok(None);
        };
        SqliteSyncStateStore::open_read_only(path)
            .and_then(|store| store.inventory_lineage(page))
            .map_err(|_| ScopeError::Unverifiable)
    }
}

pub(crate) fn from_config(config: &crate::config::Config) -> Arc<dyn Membership> {
    Arc::new(StateInventory(config.webhook_state_file.clone()))
}
