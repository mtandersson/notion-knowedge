use notion_knowledge_core::{sync_state::SyncStateStore, webhook::*};
use notion_knowledge_retrieval::sync_state::{LATEST_SCHEMA_VERSION, SqliteSyncStateStore};
const ID: &str = "13950b26-c203-4f3b-b97d-93ec06319565";
const OTHER: &str = "367cba44-b6f3-4c92-81e7-6a2e9659efd4";
fn event() -> WebhookEvent {
    WebhookEvent {
        id: ID.into(),
        timestamp: "2026-10-08T00:00:00Z".into(),
        workspace_id: ID.into(),
        subscription_id: ID.into(),
        integration_id: ID.into(),
        event_type: "page.content_updated".into(),
        entity_id: ID.into(),
        entity_type: "page".into(),
        attempt_number: 1,
    }
}
fn scope() -> InboxScope {
    InboxScope {
        workspace_id: ID.into(),
        subscription_id: ID.into(),
    }
}
fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "nk53-inbox-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
struct Database(std::path::PathBuf);
impl Database {
    fn new() -> Self {
        Self(path())
    }
    fn open(&self) -> SqliteSyncStateStore {
        SqliteSyncStateStore::open(&self.0).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
#[test]
fn receipt_survives_reopen_and_duplicate_attempts_leave_one_pending_hint() {
    let db = Database::new();
    let original = event();
    {
        let store = db.open();
        assert_eq!(store.receive(&original), Ok(Receipt::Inserted));
    }
    let store = db.open();
    let mut duplicate = original.clone();
    duplicate.attempt_number = 9;
    duplicate.id = duplicate.id.to_ascii_uppercase();
    assert_eq!(store.receive(&duplicate), Ok(Receipt::Duplicate));
    assert_eq!(
        store.event(&original.key()).unwrap().unwrap().event,
        original
    );
    let (claim, work) = store.claim(&scope(), 10, 5).unwrap().unwrap();
    assert_eq!(work.state, EventState::Running);
    assert!(store.claim(&scope(), 11, 5).unwrap().is_none());
    store.finish(&claim, 11, None).unwrap();
    assert_eq!(store.receive(&duplicate), Ok(Receipt::Duplicate));
    assert_eq!(
        store.event(&original.key()).unwrap().unwrap().state,
        EventState::Succeeded
    );
    assert!(store.claim(&scope(), 100, 5).unwrap().is_none());
}
#[test]
fn expired_claim_is_recovered_after_restart_and_old_owner_cannot_complete_it() {
    let db = Database::new();
    let old = {
        let store = db.open();
        store.receive(&event()).unwrap();
        store.claim(&scope(), 10, 5).unwrap().unwrap().0
    };
    let store = db.open();
    assert_eq!(store.finish(&old, 15, None), Err(InboxError::ClaimLost));
    let (new, work) = store.claim(&scope(), 15, 5).unwrap().unwrap();
    assert!(new.generation > old.generation);
    assert_eq!(work.state, EventState::Running);
    assert_eq!(store.finish(&old, 16, None), Err(InboxError::ClaimLost));
    store.finish(&new, 16, Some(EventFailure::Source)).unwrap();
    assert_eq!(store.finish(&new, 17, None), Err(InboxError::ClaimLost));
    drop(store);
    let failed = db.open().event(&event().key()).unwrap().unwrap();
    assert_eq!(failed.state, EventState::Failed);
    assert_eq!(failed.failure, Some(EventFailure::Source));
    assert_eq!(db.open().receive(&event()), Ok(Receipt::Duplicate));
    assert!(db.open().claim(&scope(), 100, 5).unwrap().is_none());
}
#[test]
fn conflicting_hints_do_not_overwrite_and_claims_respect_subscription_scope() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    store.receive(&event()).unwrap();
    let mut changed = event();
    changed.entity_id = OTHER.into();
    assert_eq!(store.receive(&changed), Err(InboxError::Conflict));
    assert_eq!(store.event(&event().key()).unwrap().unwrap().event, event());
    changed = event();
    changed.subscription_id = OTHER.into();
    assert_eq!(store.receive(&changed), Ok(Receipt::Inserted));
    let other_scope = InboxScope {
        workspace_id: ID.into(),
        subscription_id: OTHER.into(),
    };
    let (_, work) = store.claim(&other_scope, 1, 10).unwrap().unwrap();
    assert_eq!(work.event, changed);
    assert!(store.claim(&other_scope, 2, 10).unwrap().is_none());
    assert_eq!(
        store.event(&event().key()).unwrap().unwrap().state,
        EventState::Pending
    );
}
#[test]
fn concurrent_delivery_and_claims_are_atomic_across_sqlite_connections() {
    let db = Database::new();
    let one = db.open();
    let two = db.open();
    let (a, b) = std::thread::scope(|s| {
        let a = s.spawn(|| one.receive(&event()));
        let b = s.spawn(|| two.receive(&event()));
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    assert!(matches!(
        (a, b),
        (Receipt::Inserted, Receipt::Duplicate) | (Receipt::Duplicate, Receipt::Inserted)
    ));
    let (a, b) = std::thread::scope(|s| {
        let a = s.spawn(|| one.claim(&scope(), 1, 10));
        let b = s.spawn(|| two.claim(&scope(), 1, 10));
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    assert_ne!(a.is_some(), b.is_some());
}
#[test]
fn migration_preserves_legacy_receipts_and_rolls_back_failed_new_receipts() {
    let db = Database::new();
    {
        let raw = rusqlite::Connection::open(&db.0).unwrap();
        raw.execute_batch(include_str!("../migrations/0001_initial.sql"))
            .unwrap();
        raw.execute_batch(include_str!("../migrations/0002_reconciliation.sql"))
            .unwrap();
        raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL); INSERT INTO schema_migrations VALUES(1,'initial_sync_state',0),(2,'reconciliation_journal',0); INSERT INTO webhook_events(event_id) VALUES ('13950b26-c203-4f3b-b97d-93ec06319565');").unwrap();
    }
    let store = db.open();
    assert_eq!(store.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(store.receive(&event()), Err(InboxError::Conflict));
    assert!(store.event(&event().key()).unwrap().is_none());
    assert!(!store.register_webhook_event(ID).unwrap());
    let mut fresh = event();
    fresh.id = OTHER.into();
    let raw = rusqlite::Connection::open(&db.0).unwrap();
    raw.execute_batch("CREATE TRIGGER reject_receipt BEFORE INSERT ON webhook_inbox BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert_eq!(store.receive(&fresh), Err(InboxError::Unavailable));
    assert!(store.event(&fresh.key()).unwrap().is_none());
    raw.execute_batch("DROP TRIGGER reject_receipt;").unwrap();
    assert_eq!(store.receive(&fresh), Ok(Receipt::Inserted));
}
#[test]
fn invalid_inputs_cannot_create_work_or_overflow_claims() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    for mutate in [0, 1, 2, 3] {
        let mut bad = event();
        match mutate {
            0 => bad.attempt_number = 0,
            1 => bad.timestamp = "invalid".into(),
            2 => bad.id = "invalid".into(),
            _ => bad.event_type = "bad\nname".into(),
        };
        assert_eq!(store.receive(&bad), Err(InboxError::InvalidInput));
    }
    for (now, duration) in [(-1, 1), (0, 0), (0, 3601), (i64::MAX, 1)] {
        assert_eq!(
            store.claim(&scope(), now, duration),
            Err(InboxError::InvalidInput)
        );
    }
}
