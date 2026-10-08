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
#[test]
fn delayed_retries_preserve_reason_and_do_not_block_healthy_work() {
    let db = Database::new();
    let store = db.open();
    store.receive(&event()).unwrap();
    let (first, _) = store.claim(&scope(), 10, 5).unwrap().unwrap();
    store
        .complete(
            &first,
            11,
            ProcessingOutcome::Retryable(EventFailure::Source),
        )
        .unwrap();
    drop(store);
    let store = db.open();
    let delayed = store.event(&event().key()).unwrap().unwrap();
    assert_eq!(delayed.retry_at, Some(16));
    assert_eq!(delayed.last_failure, Some(EventFailure::Source));
    assert_eq!(delayed.cycle_attempts, 1);
    assert!(store.claim(&scope(), 15, 5).unwrap().is_none());
    let mut healthy = event();
    healthy.id = OTHER.into();
    healthy.entity_id = OTHER.into();
    store.receive(&healthy).unwrap();
    let (good, work) = store.claim(&scope(), 15, 5).unwrap().unwrap();
    assert_eq!(work.event.id, OTHER);
    store.finish(&good, 15, None).unwrap();
    let (second, work) = store.claim(&scope(), 16, 5).unwrap().unwrap();
    assert_eq!(work.cycle_attempts, 2);
    assert_eq!(work.lifetime_attempts, 2);
    assert_eq!(
        store.complete(&first, 17, ProcessingOutcome::Succeeded),
        Err(InboxError::ClaimLost)
    );
    store
        .complete(
            &second,
            17,
            ProcessingOutcome::Permanent(EventFailure::Conflict),
        )
        .unwrap();
    let failed = store.event(&event().key()).unwrap().unwrap();
    assert_eq!(failed.failure, Some(EventFailure::Conflict));
    assert!(store.claim(&scope(), 1000, 5).unwrap().is_none());
    store
        .requeue(
            &event().key(),
            failed.generation,
            1000,
            RetryPolicy {
                max_attempts: 2,
                base_seconds: 1,
                max_seconds: 2,
            },
        )
        .unwrap();
    assert_eq!(
        store.requeue(
            &event().key(),
            failed.generation,
            1000,
            RetryPolicy::default()
        ),
        Err(InboxError::Conflict)
    );
    let (third, work) = store.claim(&scope(), 1000, 5).unwrap().unwrap();
    assert_eq!(work.cycle_attempts, 1);
    assert_eq!(work.lifetime_attempts, 3);
    assert_eq!(
        store.finish(&second, 1001, None),
        Err(InboxError::ClaimLost)
    );
    store
        .complete(
            &third,
            1001,
            ProcessingOutcome::Retryable(EventFailure::Index),
        )
        .unwrap();
    let (fourth, _) = store.claim(&scope(), 1002, 5).unwrap().unwrap();
    store
        .complete(
            &fourth,
            1003,
            ProcessingOutcome::Retryable(EventFailure::Index),
        )
        .unwrap();
    assert_eq!(
        store.event(&event().key()).unwrap().unwrap().state,
        EventState::Failed
    );
    assert_eq!(store.receive(&event()), Ok(Receipt::Duplicate));
    assert_eq!(
        store
            .event(&event().key())
            .unwrap()
            .unwrap()
            .lifetime_attempts,
        4
    );
}
#[test]
fn repeated_crashes_are_bounded_and_retry_time_overflow_keeps_the_claim() {
    let db = Database::new();
    db.open().receive(&event()).unwrap();
    for attempt in 1..=5 {
        let store = db.open();
        let (_, work) = store.claim(&scope(), attempt * 5, 5).unwrap().unwrap();
        assert_eq!(work.cycle_attempts, attempt);
    }
    let store = db.open();
    assert!(store.claim(&scope(), 30, 5).unwrap().is_none());
    assert_eq!(store.failed(&scope(), 10).unwrap()[0].lifetime_attempts, 5);
    let failed = store.event(&event().key()).unwrap().unwrap();
    store
        .requeue(
            &event().key(),
            failed.generation,
            i64::MAX - 3,
            RetryPolicy::default(),
        )
        .unwrap();
    let (claim, _) = store.claim(&scope(), i64::MAX - 3, 2).unwrap().unwrap();
    assert_eq!(
        store.complete(
            &claim,
            i64::MAX - 2,
            ProcessingOutcome::Retryable(EventFailure::Source)
        ),
        Err(InboxError::InvalidInput)
    );
    assert_eq!(
        store.event(&event().key()).unwrap().unwrap().state,
        EventState::Running
    );
    for policy in [
        RetryPolicy {
            max_attempts: 0,
            ..Default::default()
        },
        RetryPolicy {
            base_seconds: 0,
            ..Default::default()
        },
        RetryPolicy {
            max_seconds: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(policy.validate(), Err(InboxError::InvalidInput));
    }
}
#[test]
fn concurrent_operator_requeue_has_one_winner_and_retains_failed_duplicate_state() {
    let db = Database::new();
    let one = db.open();
    let two = db.open();
    one.receive(&event()).unwrap();
    let (claim, _) = one.claim(&scope(), 1, 10).unwrap().unwrap();
    one.finish(&claim, 2, Some(EventFailure::Source)).unwrap();
    assert_eq!(one.receive(&event()), Ok(Receipt::Duplicate));
    assert_eq!(
        one.event(&event().key()).unwrap().unwrap().state,
        EventState::Failed
    );
    let (a, b) = std::thread::scope(|s| {
        let a =
            s.spawn(|| one.requeue(&event().key(), claim.generation, 3, RetryPolicy::default()));
        let b =
            s.spawn(|| two.requeue(&event().key(), claim.generation, 3, RetryPolicy::default()));
        (a.join().unwrap(), b.join().unwrap())
    });
    assert!(matches!(
        (a, b),
        (Ok(()), Err(InboxError::Conflict)) | (Err(InboxError::Conflict), Ok(()))
    ));
    let (new, _) = two.claim(&scope(), 3, 10).unwrap().unwrap();
    assert_eq!(one.finish(&claim, 4, None), Err(InboxError::ClaimLost));
    two.finish(&new, 4, None).unwrap();
    let policy = RetryPolicy {
        max_attempts: 10,
        base_seconds: 5,
        max_seconds: 12,
    };
    assert_eq!(policy.deadline(100, 1), Ok(105));
    assert_eq!(policy.deadline(100, 2), Ok(110));
    assert_eq!(policy.deadline(100, 3), Ok(112));
    assert_eq!(policy.deadline(100, 100), Ok(112));
}
#[test]
fn v3_upgrade_preserves_running_failed_receipts_and_other_tables() {
    let db = Database::new();
    {
        let raw = rusqlite::Connection::open(&db.0).unwrap();
        raw.execute_batch(include_str!("../migrations/0001_initial.sql"))
            .unwrap();
        raw.execute_batch(include_str!("../migrations/0002_reconciliation.sql"))
            .unwrap();
        raw.execute_batch(include_str!("../migrations/0003_webhook_inbox.sql"))
            .unwrap();
        raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL); INSERT INTO schema_migrations VALUES(1,'initial_sync_state',0),(2,'reconciliation_journal',0),(3,'webhook_inbox',0); INSERT INTO webhook_events(event_id) VALUES ('legacy'); INSERT INTO page_sync_state(page_id,content_hash,tombstoned) VALUES ('retained-page','retained-hash',0); INSERT INTO crawl_checkpoints(checkpoint_key,cursor) VALUES ('crawl','next'); INSERT INTO index_versions(index_name,version) VALUES ('chunks','v1'); INSERT INTO reconciliation_runs(run_id,scope,fence,phase,checkpoint,next_deadline) VALUES ('retained-run','scope',1,0,'checkpoint',123);").unwrap();
        for (id, state, generation, lease, failure) in
            [(ID, 1, 4, Some(100), None), (OTHER, 3, 2, None, Some(0))]
        {
            raw.execute("INSERT INTO webhook_inbox(workspace_id,subscription_id,event_id,integration_id,event_timestamp,event_type,entity_id,entity_type,attempt_number,state,generation,lease_until,failure) VALUES (?1,?1,?2,?1,'2026-10-08T00:00:00Z','page.content_updated',?1,'page',9,?3,?4,?5,?6)",rusqlite::params![ID,id,state,generation,lease,failure]).unwrap();
        }
    }
    let store = db.open();
    let running = store.event(&event().key()).unwrap().unwrap();
    assert_eq!(running.lifetime_attempts, 4);
    assert_eq!(running.cycle_attempts, 1);
    assert_eq!(running.generation, 4);
    assert_eq!(running.lease_until, Some(100));
    assert!(store.claim(&scope(), 99, 5).unwrap().is_none());
    let (_, recovered) = store.claim(&scope(), 100, 5).unwrap().unwrap();
    assert_eq!(recovered.lifetime_attempts, 5);
    let failed = store.failed(&scope(), 10).unwrap();
    assert_eq!(failed[0].failure, Some(EventFailure::Source));
    assert_eq!(failed[0].last_failure, Some(EventFailure::Source));
    assert_eq!(failed[0].lifetime_attempts, 2);
    assert!(!store.register_webhook_event("legacy").unwrap());
    let raw = rusqlite::Connection::open(&db.0).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT content_hash FROM page_sync_state WHERE page_id='retained-page'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "retained-hash"
    );
    assert_eq!(
        raw.query_row(
            "SELECT checkpoint FROM reconciliation_runs WHERE run_id='retained-run'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "checkpoint"
    );
    assert_eq!(
        raw.query_row(
            "SELECT cursor FROM crawl_checkpoints WHERE checkpoint_key='crawl'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "next"
    );
    assert_eq!(
        raw.query_row(
            "SELECT version FROM index_versions WHERE index_name='chunks'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "v1"
    );
}
