use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use notion_knowledge_core::sync_state::{PageSyncState, SyncStateStore};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;

fn state(
    page_id: &str,
    hash: Option<&str>,
    tombstone: Option<i64>,
    synced_at_ms: i64,
) -> PageSyncState {
    PageSyncState {
        page_id: page_id.to_owned(),
        content_hash: hash.map(str::to_owned),
        notion_last_edited_ms: Some(synced_at_ms - 1),
        synced_at_ms,
        tombstoned_at_ms: tombstone,
    }
}

fn temp_db() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "notion-knowledge-sync-state-{}-{nonce}.sqlite",
        std::process::id()
    ))
}

fn remove_db(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
}

#[test]
fn migrations_are_versioned_idempotent_and_rebuild_from_an_empty_file() {
    let path = temp_db();

    {
        let store = SqliteSyncStateStore::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }
    {
        let store = SqliteSyncStateStore::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }

    remove_db(&path);

    let rebuilt = SqliteSyncStateStore::open(&path).unwrap();
    assert_eq!(rebuilt.schema_version().unwrap(), 1);
    assert_eq!(rebuilt.page("missing").unwrap(), None);

    drop(rebuilt);
    remove_db(&path);
}

#[test]
fn page_upsert_atomically_replaces_complete_state_and_represents_tombstones() {
    let mut store = SqliteSyncStateStore::open_in_memory().unwrap();

    store
        .upsert_page(&state("page-1", Some("hash-a"), None, 100))
        .unwrap();
    let tombstone = PageSyncState {
        page_id: "page-1".into(),
        content_hash: None,
        notion_last_edited_ms: None,
        synced_at_ms: 200,
        tombstoned_at_ms: Some(199),
    };
    store.upsert_page(&tombstone).unwrap();

    assert_eq!(store.page("page-1").unwrap(), Some(tombstone));
}

#[test]
fn webhook_event_ids_are_deduplicated_durably() {
    let path = temp_db();

    {
        let mut store = SqliteSyncStateStore::open(&path).unwrap();
        assert!(store.record_webhook_event("evt-1", 10).unwrap());
        assert!(!store.record_webhook_event("evt-1", 11).unwrap());
    }

    {
        let mut reopened = SqliteSyncStateStore::open(&path).unwrap();
        assert!(!reopened.record_webhook_event("evt-1", 12).unwrap());
        assert!(reopened.record_webhook_event("evt-2", 13).unwrap());
    }

    remove_db(&path);
}

#[test]
fn checkpoints_and_index_versions_can_be_replaced_without_notion() {
    let mut store = SqliteSyncStateStore::open_in_memory().unwrap();

    assert_eq!(store.checkpoint("crawl").unwrap(), None);
    store.put_checkpoint("crawl", "cursor-a", 1).unwrap();
    store.put_checkpoint("crawl", "cursor-b", 2).unwrap();
    assert_eq!(
        store.checkpoint("crawl").unwrap().as_deref(),
        Some("cursor-b")
    );

    assert_eq!(store.index_version("chunks").unwrap(), None);
    store.set_index_version("chunks", "v1", 3).unwrap();
    store.set_index_version("chunks", "v2", 4).unwrap();
    assert_eq!(
        store.index_version("chunks").unwrap().as_deref(),
        Some("v2")
    );
}
