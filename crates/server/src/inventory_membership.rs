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

/// Reads are authorized from the shared state file, so serving without one
/// would fail every read at request time. Refuse to start instead.
pub(crate) fn from_config(
    config: &crate::config::Config,
) -> Result<Arc<dyn Membership>, &'static str> {
    let path = config
        .webhook_state_file
        .clone()
        .ok_or("read authorization needs the shared sync state file")?;
    Ok(Arc::new(StateInventory(Some(path))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::inventory::InventoryParent;

    fn entry(id: &str, parent: InventoryParent) -> InventoryEntry {
        InventoryEntry {
            page_id: id.into(),
            parent,
            root_id: "root".into(),
            last_edited_time: "2026-11-17T00:00:00Z".into(),
            in_trash: false,
        }
    }

    #[test]
    fn sees_each_walk_another_process_records_and_denies_without_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let writer = SqliteSyncStateStore::open(&path).unwrap();
        let reader = StateInventory(Some(path));
        assert_eq!(reader.parent_chain("page").unwrap(), None);

        writer
            .replace_inventory(
                "walk-1",
                &[
                    entry("root", InventoryParent::Root),
                    entry("page", InventoryParent::Page("root".into())),
                ],
            )
            .unwrap();
        let chain = reader.parent_chain("page").unwrap().unwrap();
        assert_eq!(
            chain.iter().map(|e| e.page_id.as_str()).collect::<Vec<_>>(),
            ["page", "root"]
        );

        // The next walk no longer reaches the page: the same reader sees it.
        writer
            .replace_inventory("walk-2", &[entry("root", InventoryParent::Root)])
            .unwrap();
        assert_eq!(reader.parent_chain("page").unwrap(), None);

        assert_eq!(StateInventory(None).parent_chain("page").unwrap(), None);
        assert_eq!(
            StateInventory(Some(dir.path().join("absent.sqlite"))).parent_chain("page"),
            Err(ScopeError::Unverifiable)
        );
    }
}
