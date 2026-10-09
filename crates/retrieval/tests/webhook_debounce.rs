use notion_knowledge_core::webhook::*;
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
const W: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const S: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const P: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
fn scope() -> InboxScope {
    InboxScope {
        workspace_id: W.into(),
        subscription_id: S.into(),
    }
}
fn event(n: u32, second: u32) -> WebhookEvent {
    WebhookEvent {
        id: format!("00000000-0000-0000-0000-{n:012x}"),
        timestamp: format!("2026-10-08T12:00:{second:02}Z"),
        workspace_id: W.into(),
        subscription_id: S.into(),
        integration_id: W.into(),
        event_type: if n.is_multiple_of(2) {
            "page.properties_updated"
        } else {
            "page.content_updated"
        }
        .into(),
        entity_id: P.into(),
        entity_type: "page".into(),
        attempt_number: 1,
    }
}
fn window() -> DebounceWindow {
    DebounceWindow {
        quiet_ms: 1000,
        max_delay_ms: 3000,
    }
}
#[test]
fn rapid_content_and_property_bursts_have_one_latest_page_refresh_after_quiet_period() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    for n in 1..=3 {
        db.receive_debounced(&event(n, n), i64::from(n - 1) * 400, window())
            .unwrap();
    }
    assert!(db.claim(&scope(), 2, 10).unwrap().is_none());
    assert!(db.claim_page(&scope(), 1799, 10).unwrap().is_none());
    let claim = db.claim_page(&scope(), 1800, 10).unwrap().unwrap();
    assert_eq!(claim.events.len(), 3);
    assert_eq!(claim.newest, event(3, 3));
    assert_eq!(
        db.finish(&claim.events[0], 2, None),
        Err(InboxError::ClaimLost)
    );
    db.complete_page(&claim, 1801, ProcessingOutcome::Succeeded)
        .unwrap();
    for n in 1..=3 {
        assert_eq!(
            db.event(&event(n, n).key()).unwrap().unwrap().state,
            EventState::Succeeded
        );
    }
    assert!(db.claim_page(&scope(), 20000, 10).unwrap().is_none());
}
#[test]
fn duplicate_and_out_of_order_hints_do_not_postpone_deadline_and_offsets_order_by_instant() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    let mut recent = event(1, 10);
    recent.timestamp = "2026-10-08T13:00:10+01:00".into();
    db.receive_debounced(&recent, 0, window()).unwrap();
    db.receive_debounced(&event(2, 9), 900, window()).unwrap();
    let mut duplicate = recent.clone();
    duplicate.attempt_number = 9;
    assert_eq!(
        db.receive_debounced(&duplicate, 999, window()).unwrap(),
        Receipt::Duplicate
    );
    let claim = db.claim_page(&scope(), 1000, 10).unwrap().unwrap();
    assert_eq!(claim.newest, recent);
}
#[test]
fn sustained_burst_has_a_cap_and_other_pages_are_independent() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    for n in 1..=4 {
        db.receive_debounced(&event(n, n), i64::from(n - 1) * 900, window())
            .unwrap();
    }
    let mut other = event(9, 9);
    other.entity_id = S.into();
    db.receive_debounced(&other, 0, window()).unwrap();
    let a = db.claim_page(&scope(), 1000, 10).unwrap().unwrap();
    assert_eq!(a.key.page_id, S);
    assert!(db.claim_page(&scope(), 2999, 10).unwrap().is_none());
    let b = db.claim_page(&scope(), 3000, 10).unwrap().unwrap();
    assert_eq!(b.events.len(), 4);
    assert_eq!(b.key.page_id, P);
}
#[test]
fn arrivals_during_refresh_survive_completion_and_snapshot_tampering_fails() {
    let path = std::env::temp_dir().join(format!(
        "nk54-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = SqliteSyncStateStore::open(&path).unwrap();
    let other = SqliteSyncStateStore::open(&path).unwrap();
    db.receive_debounced(&event(1, 1), 0, window()).unwrap();
    let first = db.claim_page(&scope(), 1000, 10).unwrap().unwrap();
    other
        .receive_debounced(&event(2, 2), 1100, window())
        .unwrap();
    assert!(other.claim_page(&scope(), 2100, 10).unwrap().is_none());
    let mut bad = first.clone();
    bad.events.clear();
    assert_eq!(
        db.complete_page(&bad, 2200, ProcessingOutcome::Succeeded),
        Err(InboxError::ClaimLost)
    );
    db.complete_page(&first, 2200, ProcessingOutcome::Succeeded)
        .unwrap();
    drop(db);
    drop(other);
    let db = SqliteSyncStateStore::open(&path).unwrap();
    let second = db.claim_page(&scope(), 2200, 10).unwrap().unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.newest, event(2, 2));
    drop(db);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn subsecond_lease_completion_and_exact_expiry_are_fenced() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    db.receive_debounced(&event(1, 1), 0, window()).unwrap();
    let first = db.claim_page(&scope(), 1500, 1).unwrap().unwrap();
    db.complete_page(&first, 2499, ProcessingOutcome::Succeeded)
        .unwrap();
    db.receive_debounced(&event(2, 2), 2500, window()).unwrap();
    let next = db.claim_page(&scope(), 3501, 1).unwrap().unwrap();
    assert_eq!(
        db.complete_page(&next, 4501, ProcessingOutcome::Succeeded),
        Err(InboxError::ClaimLost)
    );
    let recovered = db.claim_page(&scope(), 4501, 1).unwrap().unwrap();
    assert_eq!(
        db.complete_page(&next, 4501, ProcessingOutcome::Succeeded),
        Err(InboxError::ClaimLost)
    );
    db.complete_page(&recovered, 4502, ProcessingOutcome::Succeeded)
        .unwrap();
}
#[test]
fn delayed_retry_does_not_block_new_work_or_bypass_budget_and_crashes_dead_letter() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    db.receive_debounced(&event(1, 1), 0, window()).unwrap();
    let first = db.claim_page(&scope(), 1000, 1).unwrap().unwrap();
    db.complete_page(
        &first,
        1100,
        ProcessingOutcome::Retryable(EventFailure::Source),
    )
    .unwrap();
    db.receive_debounced(&event(2, 2), 1200, window()).unwrap();
    let fresh = db.claim_page(&scope(), 2200, 1).unwrap().unwrap();
    assert_eq!(fresh.events.len(), 1);
    assert_eq!(fresh.newest, event(2, 2));
    db.complete_page(&fresh, 2201, ProcessingOutcome::Succeeded)
        .unwrap();
    assert!(db.claim_page(&scope(), 5999, 1).unwrap().is_none());
    let retry = db.claim_page(&scope(), 6000, 1).unwrap().unwrap();
    assert_eq!(retry.newest, event(1, 1));
    assert_eq!(
        db.event(&event(1, 1).key())
            .unwrap()
            .unwrap()
            .cycle_attempts,
        2
    );
    for now in [7000, 8000, 9000] {
        assert!(db.claim_page(&scope(), now, 1).unwrap().is_some());
    }
    assert!(db.claim_page(&scope(), 10000, 1).unwrap().is_none());
    assert_eq!(
        db.event(&event(1, 1).key()).unwrap().unwrap().state,
        EventState::Failed
    );
}

fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "nk54-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
#[test]
fn concurrent_claim_has_one_winner_and_failed_membership_insert_rolls_back_receipt() {
    let path = path();
    let one = SqliteSyncStateStore::open(&path).unwrap();
    let two = SqliteSyncStateStore::open(&path).unwrap();
    one.receive_debounced(&event(1, 1), 0, window()).unwrap();
    let (a, b) = std::thread::scope(|s| {
        let a = s.spawn(|| one.claim_page(&scope(), 1000, 10));
        let b = s.spawn(|| two.claim_page(&scope(), 1000, 10));
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("CREATE TRIGGER reject_membership BEFORE INSERT ON webhook_page_members BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert_eq!(
        two.receive_debounced(&event(2, 2), 2000, window()),
        Err(InboxError::Unavailable)
    );
    assert!(one.event(&event(2, 2).key()).unwrap().is_none());
    let current = a.or(b).unwrap();
    one.complete_page(&current, 2000, ProcessingOutcome::Succeeded)
        .unwrap();
    drop(raw);
    drop(one);
    drop(two);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn equal_instant_tie_is_deterministic_canonical_ids_coalesce_and_other_kinds_stay_inbox() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    let mut upper = event(2, 1);
    upper.entity_id.make_ascii_uppercase();
    upper.workspace_id.make_ascii_uppercase();
    upper.subscription_id.make_ascii_uppercase();
    db.receive_debounced(&upper, 0, window()).unwrap();
    db.receive_debounced(&event(1, 1), 100, window()).unwrap();
    let mut ignored = event(3, 1);
    ignored.entity_type = "block".into();
    db.receive_debounced(&ignored, 100, window()).unwrap();
    let normal = db.claim(&scope(), 1, 10).unwrap().unwrap();
    assert_eq!(normal.1.event.entity_type, "block");
    let batch = db.claim_page(&scope(), 1000, 10).unwrap().unwrap();
    assert_eq!(batch.events.len(), 2);
    assert_eq!(batch.newest, event(2, 1));
    assert_eq!(batch.key.page_id, P);
    for invalid in [
        DebounceWindow {
            quiet_ms: 0,
            max_delay_ms: 1000,
        },
        DebounceWindow {
            quiet_ms: 1000,
            max_delay_ms: 999,
        },
    ] {
        assert_eq!(
            db.receive_debounced(&event(9, 9), 0, invalid),
            Err(InboxError::InvalidInput)
        );
    }
    assert_eq!(
        db.receive_debounced(&event(9, 9), i64::MAX, window()),
        Err(InboxError::InvalidInput)
    );
    assert!(db.event(&event(9, 9).key()).unwrap().is_none());
}
#[test]
fn v4_upgrade_preserves_pending_retry_failed_budget_and_existing_operational_state() {
    let path = path();
    let raw = rusqlite::Connection::open(&path).unwrap();
    for migration in [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_reconciliation.sql"),
        include_str!("../migrations/0003_webhook_inbox.sql"),
        include_str!("../migrations/0004_webhook_recovery.sql"),
    ] {
        raw.execute_batch(migration).unwrap();
    }
    raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL); INSERT INTO schema_migrations VALUES(1,'initial_sync_state',0),(2,'reconciliation_journal',0),(3,'webhook_inbox',0),(4,'webhook_recovery',0); INSERT INTO page_sync_state(page_id,content_hash,tombstoned) VALUES ('retained-page','hash',0); INSERT INTO reconciliation_runs(run_id,scope,fence,phase,checkpoint,next_deadline) VALUES ('run','scope',1,0,'next',123);").unwrap();
    for (n, state, retry, failure) in [(1, 0, Some(100), None), (2, 3, None, Some(0))] {
        let e = event(n, n);
        raw.execute("INSERT INTO webhook_inbox(workspace_id,subscription_id,event_id,integration_id,event_timestamp,event_type,entity_id,entity_type,attempt_number,state,generation,failure,last_failure,cycle_attempts,lifetime_attempts,retry_at,max_attempts,base_seconds,max_seconds) VALUES(?1,?2,?3,?1,?4,?5,?6,'page',9,?7,4,?8,0,2,4,?9,3,10,20)",rusqlite::params![W,S,e.id,e.timestamp,e.event_type,P,state,failure,retry]).unwrap();
    }
    drop(raw);
    let db = SqliteSyncStateStore::open(&path).unwrap();
    let retry = db.event(&event(1, 1).key()).unwrap().unwrap();
    assert_eq!(retry.retry_at, Some(100));
    assert_eq!(retry.policy.max_attempts, 3);
    assert_eq!(retry.cycle_attempts, 2);
    assert_eq!(retry.lifetime_attempts, 4);
    assert_eq!(retry.event.attempt_number, 9);
    let failed = db.event(&event(2, 2).key()).unwrap().unwrap();
    assert_eq!(failed.state, EventState::Failed);
    assert_eq!(failed.last_failure, Some(EventFailure::Source));
    assert!(db.claim_page(&scope(), 200000, 10).unwrap().is_none());
    assert!(db.claim(&scope(), 99, 10).unwrap().is_none());
    assert!(db.claim(&scope(), 100, 10).unwrap().is_some());
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT content_hash FROM page_sync_state WHERE page_id='retained-page'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "hash"
    );
    assert_eq!(
        raw.query_row(
            "SELECT checkpoint FROM reconciliation_runs WHERE run_id='run'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "next"
    );
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
