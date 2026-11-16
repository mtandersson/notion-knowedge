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

#[cfg(unix)]
#[test]
fn database_creation_is_private_and_reopen_preserves_operator_permissions_and_content() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let db = TestDb::new("private-creation");
    let page = PageSyncState::present("page".into(), "hash".into(), None).unwrap();
    let store = SqliteSyncStateStore::open(db.path()).unwrap();
    store.put_page_state(&page).unwrap();
    drop(store);
    let created = fs::metadata(db.path()).unwrap();
    assert_eq!(created.mode() & 0o777, 0o600);
    assert_eq!(
        created.uid(),
        fs::metadata(db.path().parent().unwrap()).unwrap().uid()
    );
    // Existing permissions are never silently changed to make read activation
    // appear safe; its separate private-storage validation still owns that gate.
    fs::set_permissions(db.path(), fs::Permissions::from_mode(0o640)).unwrap();
    let reopened = SqliteSyncStateStore::open(db.path()).unwrap();
    assert_eq!(reopened.page_state("page").unwrap(), Some(page));
    let after = fs::metadata(db.path()).unwrap();
    assert_eq!(after.mode() & 0o777, 0o640);
    assert_eq!(
        (after.dev(), after.ino(), after.uid()),
        (created.dev(), created.ino(), created.uid())
    );
}

#[cfg(unix)]
#[test]
fn database_creation_rejects_symlinks_and_non_file_targets_without_changing_them() {
    use std::os::unix::fs::symlink;
    let db = TestDb::new("creation-target");
    let target = db.path().with_extension("target");
    let alias = db.path().with_extension("alias");
    let directory = db.path().with_extension("directory");
    fs::write(&target, "untouched").unwrap();
    symlink(&target, &alias).unwrap();
    fs::create_dir(&directory).unwrap();
    assert!(SqliteSyncStateStore::open(&alias).is_err());
    assert!(SqliteSyncStateStore::open(&directory).is_err());
    assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
    assert_eq!(fs::read_link(&alias).unwrap(), target);
    assert!(fs::read_dir(&directory).unwrap().next().is_none());
}

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

#[test]
fn metadata_identity_migration_preserves_checkpoints_without_fabricating_identity() {
    let db = TestDb::new("metadata-migration");
    let store = SqliteSyncStateStore::open(db.path()).unwrap();
    let page = PageSyncState::present("legacy-page".into(), "legacy-hash".into(), None).unwrap();
    store.put_page_state(&page).unwrap();
    drop(store);
    let connection = Connection::open(db.path()).unwrap();
    connection
        .execute_batch(include_str!("fixtures/remove_schedule_schema.sql"))
        .unwrap();
    connection
        .execute_batch(
            "DROP TABLE reconciliation_retry_proposals; DROP TABLE reconciliation_plan_run_failures; DROP TABLE reconciliation_plan_progress; DROP TABLE reconciliation_plans; DROP TABLE page_metadata_identity; DELETE FROM schema_migrations WHERE version>=9;",
        )
        .unwrap();
    drop(connection);
    let reopened = SqliteSyncStateStore::open(db.path()).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(reopened.page_state("legacy-page").unwrap(), Some(page));
    let connection = Connection::open(db.path()).unwrap();
    let identities: i64 = connection
        .query_row("SELECT COUNT(*) FROM page_metadata_identity", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(identities, 0);
}

mod inventory {
    use super::TestDb;
    use notion_knowledge_core::{
        discovery::SkipReason,
        inventory::{InventoryEntry, InventoryOutcome, InventoryParent},
        reconciliation::FailureClass,
    };
    use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;

    fn entry(id: &str, parent: InventoryParent, edited: &str, in_trash: bool) -> InventoryEntry {
        InventoryEntry {
            page_id: id.into(),
            parent,
            root_id: "A".into(),
            last_edited_time: edited.into(),
            in_trash,
        }
    }
    fn page(id: &str) -> InventoryParent {
        InventoryParent::Page(id.into())
    }

    #[test]
    fn inventory_replace_classifies_added_changed_moved_removed_trashed() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        let seeded = store
            .replace_inventory(
                "run-1",
                &[
                    entry("A", InventoryParent::Root, "t1", false),
                    entry("B", page("A"), "t1", false),
                    entry("D", page("A"), "t1", false),
                    entry("E", page("A"), "t1", false),
                ],
            )
            .unwrap();
        assert_eq!(seeded.added, ["A", "B", "D", "E"]);
        let diff = store
            .replace_inventory(
                "run-2",
                &[
                    entry("A", InventoryParent::Root, "t2", false),
                    entry("B", InventoryParent::Root, "t1", false),
                    entry("C", page("A"), "t1", false),
                    entry("D", page("A"), "t1", true),
                ],
            )
            .unwrap();
        assert_eq!(diff.added, ["C"]);
        assert_eq!(diff.changed, ["A"]);
        assert_eq!(diff.moved, ["B"]);
        assert_eq!(diff.trashed, ["D"]);
        assert_eq!(diff.removed, ["E"]);
        assert_eq!(diff.unchanged, 0);
        assert_eq!(store.inventory_entry("E").unwrap(), None);
        assert_eq!(
            store.inventory_entry("B").unwrap(),
            Some(entry("B", InventoryParent::Root, "t1", false))
        );
    }

    #[test]
    fn inventory_counts_add_up_to_total() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        let rows = ["A", "B", "C", "D"].map(|id| entry(id, InventoryParent::Root, "t1", false));
        store.replace_inventory("run", &rows).unwrap();
        store
            .record_inventory_outcome(
                "A",
                &InventoryOutcome::Indexed {
                    revision: "t1".into(),
                },
                1,
            )
            .unwrap();
        store
            .record_inventory_outcome("B", &InventoryOutcome::Skipped(SkipReason::Inaccessible), 1)
            .unwrap();
        store
            .record_inventory_outcome("C", &InventoryOutcome::Failed(FailureClass::Source), 1)
            .unwrap();
        let counts = store.inventory_counts().unwrap();
        assert_eq!(counts.total, 4);
        assert_eq!((counts.indexed, counts.skipped, counts.failed), (1, 1, 1));
        assert_eq!(counts.pending, 1);
        assert_eq!(
            counts.indexed + counts.skipped + counts.failed + counts.pending,
            counts.total
        );
        assert_eq!(
            store.out_of_sync_pages(10).unwrap(),
            [
                (
                    "B".to_owned(),
                    InventoryOutcome::Skipped(SkipReason::Inaccessible)
                ),
                (
                    "C".to_owned(),
                    InventoryOutcome::Failed(FailureClass::Source)
                ),
            ]
        );
    }

    #[test]
    fn inventory_parent_chain_ends_at_root_or_is_none() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        store
            .replace_inventory(
                "run",
                &[
                    entry("A", InventoryParent::Root, "t", false),
                    entry("B", page("A"), "t", false),
                    entry("C", page("B"), "t", false),
                    entry("U", page("missing"), "t", false),
                    entry("X", page("Y"), "t", false),
                    entry("Y", page("X"), "t", false),
                ],
            )
            .unwrap();
        assert_eq!(
            store.inventory_parent_chain("C").unwrap(),
            Some(vec!["B".to_owned(), "A".to_owned()])
        );
        assert_eq!(store.inventory_parent_chain("A").unwrap(), Some(vec![]));
        assert_eq!(store.inventory_parent_chain("U").unwrap(), None);
        assert_eq!(store.inventory_parent_chain("X").unwrap(), None);
        assert_eq!(store.inventory_parent_chain("unknown").unwrap(), None);
    }

    #[test]
    fn upsert_keeps_unreached_entries_and_reports_its_own_difference() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        store
            .replace_inventory("run-1", &[entry("A", InventoryParent::Root, "t1", false)])
            .unwrap();
        let diff = store
            .upsert_inventory_entry(
                "run-2",
                &entry(
                    "B",
                    InventoryParent::DataSource {
                        data_source_id: "S".into(),
                        database_id: "D".into(),
                        owner_page_id: "A".into(),
                    },
                    "t1",
                    false,
                ),
            )
            .unwrap();
        assert_eq!(diff.added, ["B"]);
        assert!(store.inventory_entry("A").unwrap().is_some());
        assert_eq!(
            store.inventory_parent_chain("B").unwrap(),
            Some(vec!["A".to_owned()])
        );
        assert_eq!(store.inventory_counts().unwrap().total, 2);
    }

    #[test]
    fn dropping_inventory_table_forces_full_walk() {
        let db = TestDb::new("inventory-drop");
        let store = SqliteSyncStateStore::open(db.path()).unwrap();
        store
            .replace_inventory("run", &[entry("A", InventoryParent::Root, "t", false)])
            .unwrap();
        drop(store);
        rusqlite::Connection::open(db.path())
            .unwrap()
            .execute_batch("DROP TABLE page_inventory")
            .unwrap();
        let reopened = SqliteSyncStateStore::open(db.path()).unwrap();
        assert_eq!(reopened.inventory_counts().unwrap().total, 0);
        assert_eq!(
            reopened.schema_version().unwrap(),
            notion_knowledge_retrieval::sync_state::LATEST_SCHEMA_VERSION
        );
    }
}
