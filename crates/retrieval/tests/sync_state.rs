use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use notion_knowledge_core::sync_state::{
    CrawlCheckpoint, IndexVersion, PageSyncState, PageSyncStatus, SyncStateError, SyncStateStore,
};
use notion_knowledge_retrieval::sync_state::{LATEST_SCHEMA_VERSION, SqliteSyncStateStore};
use rusqlite::Connection;

static NEXT_DB: AtomicU64 = AtomicU64::new(1);

struct TestDb {
    path: PathBuf,
}

impl TestDb {
    fn new(name: &str) -> Self {
        let id = NEXT_DB.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "notion-knowledge-sync-state-{}-{name}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&directory).unwrap();
        Self {
            path: directory.join("state.sqlite3"),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn reset_directory(&self) {
        if let Some(directory) = self.path.parent() {
            fs::remove_dir_all(directory).unwrap();
        }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if let Some(directory) = self.path.parent() {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

#[test]
fn state_survives_reopen_with_versioned_schema() {
    let db = TestDb::new("durable");
    let store = SqliteSyncStateStore::open(db.path()).unwrap();
    assert_eq!(store.schema_version().unwrap(), LATEST_SCHEMA_VERSION);

    let page = PageSyncState::present(
        "page-1".into(),
        "sha256:abc".into(),
        Some("2026-10-05T18:00:00Z".into()),
    )
    .unwrap();
    let checkpoint =
        CrawlCheckpoint::new("workspace-root".into(), Some("cursor-42".into())).unwrap();
    let index_version = IndexVersion::new("chunks".into(), "schema-1".into()).unwrap();

    store.put_page_state(&page).unwrap();
    store.put_checkpoint(&checkpoint).unwrap();
    store.put_index_version(&index_version).unwrap();
    assert!(store.register_webhook_event("event-1").unwrap());
    drop(store);

    let reopened = SqliteSyncStateStore::open(db.path()).unwrap();
    assert_eq!(reopened.page_state("page-1").unwrap(), Some(page));
    assert_eq!(
        reopened.checkpoint("workspace-root").unwrap(),
        Some(checkpoint)
    );
    assert_eq!(
        reopened.index_version("chunks").unwrap(),
        Some(index_version)
    );
    assert!(!reopened.register_webhook_event("event-1").unwrap());
}

#[test]
fn page_upsert_replaces_hash_and_tombstone_atomically() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();

    store
        .put_page_state(&PageSyncState::present("page-1".into(), "hash-1".into(), None).unwrap())
        .unwrap();
    store
        .put_page_state(&PageSyncState::present("page-1".into(), "hash-2".into(), None).unwrap())
        .unwrap();

    let updated = store.page_state("page-1").unwrap().unwrap();
    assert_eq!(updated.content_hash(), Some("hash-2"));
    assert!(matches!(
        updated.status(),
        PageSyncStatus::Present { content_hash } if content_hash == "hash-2"
    ));

    store
        .put_page_state(&PageSyncState::tombstone("page-1".into(), None).unwrap())
        .unwrap();

    let tombstone = store.page_state("page-1").unwrap().unwrap();
    assert!(tombstone.is_tombstone());
    assert_eq!(tombstone.content_hash(), None);
}

#[test]
fn webhook_ids_are_deduplicated_without_storing_payloads() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();

    assert!(store.register_webhook_event("event-42").unwrap());
    assert!(!store.register_webhook_event("event-42").unwrap());
    assert!(store.register_webhook_event("event-43").unwrap());
}

#[test]
fn deleting_the_database_rebuilds_an_empty_store_without_notion() {
    let db = TestDb::new("rebuild");
    let store = SqliteSyncStateStore::open(db.path()).unwrap();
    store
        .put_page_state(&PageSyncState::present("page-1".into(), "hash-1".into(), None).unwrap())
        .unwrap();
    drop(store);

    db.reset_directory();

    let rebuilt = SqliteSyncStateStore::open(db.path()).unwrap();
    assert_eq!(rebuilt.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(rebuilt.page_state("page-1").unwrap(), None);
}

#[test]
fn altered_migration_history_is_rejected_as_corrupt_state() {
    let db = TestDb::new("altered-history");
    let connection = Connection::open(db.path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                applied_at_unix INTEGER NOT NULL
            );
            INSERT INTO schema_migrations (version, name, applied_at_unix)
            VALUES (1, 'different_migration', 0);",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteSyncStateStore::open(db.path()),
        Err(SyncStateError::CorruptState)
    ));
}

#[test]
fn newer_unknown_schema_is_rejected_instead_of_reinterpreted() {
    let db = TestDb::new("future-schema");
    let connection = Connection::open(db.path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                applied_at_unix INTEGER NOT NULL
            );
            INSERT INTO schema_migrations (version, name, applied_at_unix)
            VALUES (99, 'future', 0);",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteSyncStateStore::open(db.path()),
        Err(SyncStateError::UnsupportedSchema)
    ));
}
