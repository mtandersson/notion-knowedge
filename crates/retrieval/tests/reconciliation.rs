use notion_knowledge_core::{reconciliation::*, sync_state::*};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use rusqlite::Connection;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Db(PathBuf);
impl Db {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "nk-journal-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn open(&self) -> SqliteSyncStateStore {
        SqliteSyncStateStore::open(&self.0).unwrap()
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn scope() -> ReconciliationScope {
    ReconciliationScope::new(
        vec!["root-a".into(), "root-b".into()],
        vec!["exclude".into()],
        "policy-v1",
        "index-v1",
    )
    .unwrap()
}
fn page(id: &str, action: WorkAction) -> InventoryPage {
    InventoryPage {
        page_id: id.into(),
        revision: "revision-v1".into(),
        action,
    }
}
#[test]
fn version_one_data_survives_upgrade_and_reopen() {
    let db = Db::new();
    let raw = Connection::open(&db.0).unwrap();
    raw.execute_batch(include_str!("../migrations/0001_initial.sql"))
        .unwrap();
    raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL); INSERT INTO schema_migrations VALUES(1,'initial_sync_state',0); INSERT INTO page_sync_state VALUES('page','hash','edited',0,0); INSERT INTO crawl_checkpoints VALUES('root','cursor',0); INSERT INTO webhook_events VALUES('event',0); INSERT INTO index_versions VALUES('chunks','v1',0);").unwrap();
    drop(raw);
    let store = db.open();
    assert_eq!(store.schema_version().unwrap(), 2);
    assert_eq!(
        store.page_state("page").unwrap().unwrap().content_hash(),
        Some("hash")
    );
    assert_eq!(
        store.checkpoint("root").unwrap().unwrap().cursor(),
        Some("cursor")
    );
    assert!(!store.register_webhook_event("event").unwrap());
    assert_eq!(
        store.index_version("chunks").unwrap().unwrap().version(),
        "v1"
    );
    drop(store);
    assert_eq!(db.open().schema_version().unwrap(), 2);
}
#[test]
fn scope_sets_are_canonical_and_resume_rejects_policy_generation_and_root_changes() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let lease = store.acquire_lease(10, 10).unwrap();
    store.start_or_resume(&lease, 10, "run", &scope()).unwrap();
    let equivalent = ReconciliationScope::new(
        vec!["root-b".into(), " root-a ".into(), "root-a".into()],
        vec!["exclude".into(), "exclude".into()],
        "policy-v1",
        "index-v1",
    )
    .unwrap();
    assert_eq!(equivalent, scope());
    store
        .start_or_resume(&lease, 10, "run", &equivalent)
        .unwrap();
    for changed in [
        ReconciliationScope::new(
            vec!["root-a".into()],
            vec!["exclude".into()],
            "policy-v1",
            "index-v1",
        ),
        ReconciliationScope::new(
            vec!["root-a".into(), "root-b".into()],
            vec![],
            "policy-v1",
            "index-v1",
        ),
        ReconciliationScope::new(
            vec!["root-a".into(), "root-b".into()],
            vec!["exclude".into()],
            "policy-v2",
            "index-v1",
        ),
        ReconciliationScope::new(
            vec!["root-a".into(), "root-b".into()],
            vec!["exclude".into()],
            "policy-v1",
            "index-v2",
        ),
    ] {
        assert_eq!(
            store.start_or_resume(&lease, 10, "run", &changed.unwrap()),
            Err(JournalError::ScopeMismatch)
        );
    }
}
#[test]
fn leases_contend_across_connections_expire_and_fence_every_old_holder_mutation() {
    let db = Db::new();
    let one = db.open();
    let two = db.open();
    let old = one.acquire_lease(10, 2).unwrap();
    one.start_or_resume(&old, 10, "run", &scope()).unwrap();
    assert_eq!(two.acquire_lease(11, 2), Err(JournalError::Busy));
    let renewed = one.renew_lease(&old, 11, 2).unwrap();
    assert_eq!(two.acquire_lease(12, 2), Err(JournalError::Busy));
    let new = two.acquire_lease(13, 2).unwrap();
    assert!(new.fence > old.fence);
    assert_eq!(
        one.renew_lease(&renewed, 13, 2),
        Err(JournalError::LeaseLost)
    );
    assert_eq!(
        one.inventory(&old, 13, "run", &[page("a", WorkAction::Refresh)], None),
        Err(JournalError::LeaseLost)
    );
    assert_eq!(one.release_lease(&old, 13), Err(JournalError::LeaseLost));
    assert_eq!(
        two.inventory(&new, 13, "run", &[], None),
        Err(JournalError::InvalidTransition)
    );
    two.start_or_resume(&new, 13, "run", &scope()).unwrap();
    two.inventory(&new, 13, "run", &[], None).unwrap();
    two.release_lease(&new, 13).unwrap();
    assert!(two.acquire_lease(13, 1).unwrap().fence > new.fence);
}
#[test]
fn conflicting_batch_and_sql_failure_roll_back_inventory_and_checkpoint() {
    let db = Db::new();
    let store = db.open();
    let lease = store.acquire_lease(1, 100).unwrap();
    store.start_or_resume(&lease, 1, "run", &scope()).unwrap();
    store
        .inventory(
            &lease,
            1,
            "run",
            &[page("a", WorkAction::Refresh)],
            Some("first"),
        )
        .unwrap();
    let mut conflict = page("a", WorkAction::Refresh);
    conflict.revision = "different".into();
    assert_eq!(
        store.inventory(
            &lease,
            2,
            "run",
            &[page("b", WorkAction::Refresh), conflict],
            Some("second")
        ),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(store.work("run").unwrap().len(), 1);
    assert_eq!(
        store.run("run", &scope()).unwrap().checkpoint.as_deref(),
        Some("first")
    );
    let raw = Connection::open(&db.0).unwrap();
    raw.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON reconciliation_runs BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        store.inventory(
            &lease,
            3,
            "run",
            &[page("c", WorkAction::Refresh)],
            Some("third")
        ),
        Err(JournalError::Unavailable)
    );
    assert_eq!(store.work("run").unwrap().len(), 1);
}
#[test]
fn restart_replays_applied_before_ack_once_and_retains_failures_for_explicit_retry() {
    let db = Db::new();
    let store = db.open();
    let lease = store.acquire_lease(10, 2).unwrap();
    store.start_or_resume(&lease, 10, "run", &scope()).unwrap();
    let pages = [
        page("refresh", WorkAction::Refresh),
        page("gone", WorkAction::Delete),
        page("same", WorkAction::Unchanged),
    ];
    store
        .inventory(&lease, 10, "run", &pages, Some("end"))
        .unwrap();
    store.seal_inventory(&lease, 10, "run").unwrap();
    // An idempotent index fixture commits before the journal acknowledgment, then crashes.
    let mut index = BTreeMap::new();
    index.insert("refresh", "revision-v1");
    drop(store);
    let store = db.open();
    let lease = store.acquire_lease(12, 10).unwrap();
    store.start_or_resume(&lease, 12, "run", &scope()).unwrap();
    assert_eq!(store.run("run", &scope()).unwrap().pending_count, 3);
    index.insert("refresh", "revision-v1");
    assert_eq!(index.len(), 1);
    store
        .acknowledge(&lease, 12, "run", "refresh", WorkStatus::Applied)
        .unwrap();
    store
        .acknowledge(&lease, 12, "run", "refresh", WorkStatus::Applied)
        .unwrap();
    store
        .acknowledge(&lease, 12, "run", "gone", WorkStatus::Failed)
        .unwrap();
    store
        .record_failure(&lease, 12, "run", FailureClass::Index, Some(30))
        .unwrap();
    assert_eq!(
        store.complete(&lease, 12, "run", Some(30)),
        Err(JournalError::InvalidTransition)
    );
    drop(store);
    let store = db.open();
    let snapshot = store.run("run", &scope()).unwrap();
    assert_eq!(
        (
            snapshot.pending_count,
            snapshot.applied_count,
            snapshot.failed_count,
            snapshot.refreshed_count
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(snapshot.failure, Some(FailureClass::Index));
    assert_eq!(snapshot.next_deadline, Some(30));
    assert_eq!(
        store.work("run").unwrap()[0].failure,
        Some(FailureClass::Index)
    );
    assert_eq!(
        store.acknowledge(&lease, 13, "run", "gone", WorkStatus::Applied),
        Err(JournalError::InvalidTransition)
    );
    store.retry_failed(&lease, 13, "run", "gone").unwrap();
    store
        .acknowledge(&lease, 13, "run", "gone", WorkStatus::Applied)
        .unwrap();
    store
        .acknowledge(&lease, 13, "run", "same", WorkStatus::Applied)
        .unwrap();
    store.complete(&lease, 13, "run", Some(40)).unwrap();
    let snapshot = store.run("run", &scope()).unwrap();
    assert_eq!(snapshot.phase, RunPhase::Completed);
    assert_eq!(
        (
            snapshot.inventory_count,
            snapshot.applied_count,
            snapshot.refreshed_count,
            snapshot.deleted_count,
            snapshot.unchanged_count
        ),
        (3, 3, 1, 1, 1)
    );
    assert_eq!(snapshot.failure, None);
    assert_eq!(snapshot.next_deadline, Some(40));
}

#[test]
fn expired_holder_cannot_ack_retry_seal_complete_or_record_failure_after_takeover() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let old = store.acquire_lease(1, 1).unwrap();
    store.start_or_resume(&old, 1, "run", &scope()).unwrap();
    store
        .inventory(&old, 1, "run", &[page("a", WorkAction::Refresh)], None)
        .unwrap();
    store.seal_inventory(&old, 1, "run").unwrap();
    store
        .acknowledge(&old, 1, "run", "a", WorkStatus::Failed)
        .unwrap();
    let new = store.acquire_lease(2, 10).unwrap();
    let mismatch =
        ReconciliationScope::new(vec!["other".into()], vec![], "policy-v1", "index-v1").unwrap();
    assert_eq!(
        store.start_or_resume(&new, 2, "run", &mismatch),
        Err(JournalError::ScopeMismatch)
    );
    assert_eq!(
        store.retry_failed(&new, 2, "run", "a"),
        Err(JournalError::InvalidTransition)
    );
    store.start_or_resume(&new, 2, "run", &scope()).unwrap();
    for result in [
        store.acknowledge(&old, 2, "run", "a", WorkStatus::Applied),
        store.retry_failed(&old, 2, "run", "a"),
        store.seal_inventory(&old, 2, "run"),
        store.complete(&old, 2, "run", None),
        store.record_failure(&old, 2, "run", FailureClass::Source, Some(5)),
    ] {
        assert_eq!(result, Err(JournalError::LeaseLost));
    }
    assert_eq!(store.run("run", &scope()).unwrap().failed_count, 1);
}

#[test]
fn inventory_failure_preserves_checkpoint_and_deadline_and_empty_inventory_can_finish() {
    let db = Db::new();
    let store = db.open();
    let lease = store.acquire_lease(1, 10).unwrap();
    store.start_or_resume(&lease, 1, "run", &scope()).unwrap();
    store
        .inventory(&lease, 1, "run", &[], Some("cursor"))
        .unwrap();
    store
        .record_failure(&lease, 1, "run", FailureClass::Source, Some(20))
        .unwrap();
    drop(store);
    let store = db.open();
    let state = store.start_or_resume(&lease, 2, "run", &scope()).unwrap();
    assert_eq!(state.phase, RunPhase::Inventory);
    assert_eq!(state.failure, Some(FailureClass::Source));
    assert_eq!(state.checkpoint.as_deref(), Some("cursor"));
    assert_eq!(state.next_deadline, Some(20));
    assert_eq!(state.inventory_count, 0);
    assert_eq!(
        store.complete(&lease, 2, "run", None),
        Err(JournalError::InvalidTransition)
    );
    store.seal_inventory(&lease, 2, "run").unwrap();
    store.complete(&lease, 2, "run", Some(30)).unwrap();
    assert_eq!(
        store.run("run", &scope()).unwrap().phase,
        RunPhase::Completed
    );
    store.start_or_resume(&lease, 2, "next", &scope()).unwrap();
    assert_eq!(
        store.start_or_resume(&lease, 2, "overlap", &scope()),
        Err(JournalError::Busy)
    );
}
