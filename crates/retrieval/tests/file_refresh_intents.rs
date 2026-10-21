use notion_knowledge_core::write_refresh::{
    GrantRefreshAuthority, MutationRefreshQueue, RefreshKey, RefreshQueueError,
};
use notion_knowledge_retrieval::{
    file_refresh_intents::{FileIntentStatus, MAX_HELD_PER_WORKSPACE},
    mutation_refresh::GrantMutationRefreshQueue,
    sync_state::SqliteSyncStateStore,
};
use std::sync::Arc;
struct Temp(std::path::PathBuf);
impl Temp {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn temp() -> Temp {
    use std::os::unix::fs::PermissionsExt;
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "nk481-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    Temp(path)
}
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn key(n: usize) -> RefreshKey {
    RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: format!("{n:064x}"),
        page_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
    }
}
fn authority() -> GrantRefreshAuthority {
    GrantRefreshAuthority::new(
        WORKSPACE.into(),
        "accepted-original-grant".into(),
        1,
        "owner".into(),
        "c".repeat(64),
    )
    .unwrap()
}
#[test]
fn expiry_is_quarantine_not_quiescence_and_recovery_keeps_original_authority() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    let key = key(1);
    let auth = authority();
    let intent = db.begin_file_refresh_intent(&key, &auth, 0, 10).unwrap();
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 1, 10)
            .unwrap()
            .is_none()
    );
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 1, 10)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.file_refresh_intent_status(&key, &auth, 10).unwrap(),
        Some(FileIntentStatus::RecoveryRequired)
    );
    assert!(!db.file_source_owner_is_current(&intent, 10).unwrap());
    assert_eq!(
        db.renew_file_source_owner(&intent, 10, 10),
        Err(RefreshQueueError::ClaimLost)
    );
    assert_eq!(
        db.settle_file_refresh_intent(&intent, 10, true),
        Err(RefreshQueueError::ClaimLost)
    );
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 1_000_000, 10)
            .unwrap()
            .is_none()
    );
    let replacement = GrantRefreshAuthority::new(
        WORKSPACE.into(),
        "replacement".into(),
        2,
        "owner".into(),
        "c".repeat(64),
    )
    .unwrap();
    assert!(
        db.reconcile_quiescent_file_intent(&key, &replacement, 20)
            .is_err()
    );
    db.reconcile_quiescent_file_intent(&key, &auth, 20).unwrap();
    assert!(db.settle_file_refresh_intent(&intent, 20, false).is_err());
    let claim = db
        .claim_grant_mutation_refresh(WORKSPACE, 20, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claim.key, key);
    assert_eq!(claim.authority, Some(auth));
    db.complete_mutation_refresh(&claim, 21, true).unwrap();
}
#[test]
fn settlement_is_idempotent_discard_is_terminal_and_keys_cannot_steal_ownership() {
    let db = Arc::new(SqliteSyncStateStore::open_in_memory().unwrap());
    let auth = authority();
    for possible in [true, false] {
        let key = key(if possible { 1 } else { 2 });
        let owner = db.begin_file_refresh_intent(&key, &auth, 0, 10).unwrap();
        assert!(matches!(
            db.begin_file_refresh_intent(&key, &auth, 1, 10),
            Err(RefreshQueueError::Conflict)
        ));
        let mut wrong = key.clone();
        wrong.page_id = "dddddddd-dddd-dddd-dddd-dddddddddddd".into();
        assert!(db.begin_file_refresh_intent(&wrong, &auth, 1, 10).is_err());
        db.settle_file_refresh_intent(&owner, 1, possible).unwrap();
        db.settle_file_refresh_intent(&owner, 2, possible).unwrap();
        assert!(db.settle_file_refresh_intent(&owner, 2, !possible).is_err());
        assert!(!db.file_source_owner_is_current(&owner, 2).unwrap());
        if !possible {
            assert!(
                GrantMutationRefreshQueue::new(db.clone(), auth.clone())
                    .unwrap()
                    .retry(&key)
                    .is_err()
            );
            assert_eq!(
                db.file_refresh_intent_status(&key, &auth, 2).unwrap(),
                Some(FileIntentStatus::Discarded)
            );
        }
    }
    let claim = db
        .claim_grant_mutation_refresh(WORKSPACE, 3, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claim.key, key(1));
    db.complete_mutation_refresh(&claim, 4, true).unwrap();
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 4, 10)
            .unwrap()
            .is_none()
    );
}
#[test]
fn renewal_is_bounded_and_races_reconciliation_with_one_cas_winner() {
    let dir = temp();
    let path = dir.path().join("state.sqlite");
    let db = Arc::new(SqliteSyncStateStore::open(&path).unwrap());
    let auth = authority();
    let key = key(1);
    let owner = db.begin_file_refresh_intent(&key, &auth, 0, 10).unwrap();
    let other = Arc::new(SqliteSyncStateStore::open(&path).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let a = db.clone();
    let token = owner.clone();
    let ready = barrier.clone();
    let renewal = std::thread::spawn(move || {
        ready.wait();
        a.renew_file_source_owner(&token, 9, 10)
    });
    let ready = barrier.clone();
    let k = key.clone();
    let grant = auth.clone();
    let recovery = std::thread::spawn(move || {
        ready.wait();
        other.reconcile_quiescent_file_intent(&k, &grant, 10)
    });
    barrier.wait();
    let renewed = renewal.join().unwrap().is_ok();
    let recovered = recovery.join().unwrap().is_ok();
    assert_ne!(renewed, recovered);
    if renewed {
        assert!(db.file_source_owner_is_current(&owner, 18).unwrap());
        assert!(!db.file_source_owner_is_current(&owner, 19).unwrap());
    }
    assert!(db.renew_file_source_owner(&owner, 4000, 10).is_err());
    assert!(db.settle_file_refresh_intent(&owner, 4000, false).is_err());
}
#[test]
fn intent_insert_failure_rolls_back_held_queue_and_preserves_other_admissions() {
    let dir = temp();
    let path = dir.path().join("state.sqlite");
    let db = Arc::new(SqliteSyncStateStore::open(&path).unwrap());
    let auth = authority();
    let key = key(1);
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("CREATE TRIGGER fail_intent BEFORE INSERT ON file_refresh_intents BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert!(db.begin_file_refresh_intent(&key, &auth, 0, 10).is_err());
    assert_eq!(
        GrantMutationRefreshQueue::new(db.clone(), auth.clone())
            .unwrap()
            .status(&key)
            .unwrap(),
        None
    );
    assert_eq!(
        raw.query_row("SELECT count(*) FROM file_refresh_intents", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    raw.execute_batch("DROP TRIGGER fail_intent").unwrap();
    db.begin_file_refresh_intent(&key, &auth, 0, 10).unwrap();
    for n in 2..=MAX_HELD_PER_WORKSPACE as usize {
        db.begin_file_refresh_intent(&crate::key(n), &auth, 0, 10)
            .unwrap();
    }
    assert!(
        db.begin_file_refresh_intent(&crate::key(100), &auth, 0, 10)
            .is_err()
    );
    assert_eq!(
        raw.query_row("SELECT count(*) FROM mutation_refresh", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        MAX_HELD_PER_WORKSPACE
    );
}
#[test]
fn drop_reopen_preserves_quarantine_original_grant_and_no_payload_columns() {
    let dir = temp();
    let path = dir.path().join("state.sqlite");
    let auth = authority();
    let key = key(1);
    {
        let db = SqliteSyncStateStore::open(&path).unwrap();
        let owner = db.begin_file_refresh_intent(&key, &auth, 0, 10).unwrap();
        db.renew_file_source_owner(&owner, 9, 10).unwrap();
    }
    let db = SqliteSyncStateStore::open_existing(&path).unwrap();
    assert_eq!(
        db.file_refresh_intent_status(&key, &auth, 19).unwrap(),
        Some(FileIntentStatus::RecoveryRequired)
    );
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 20, 10)
            .unwrap()
            .is_none()
    );
    db.reconcile_quiescent_file_intent(&key, &auth, 20).unwrap();
    let claim = db
        .claim_grant_mutation_refresh(WORKSPACE, 20, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claim.authority, Some(auth));
    let raw = rusqlite::Connection::open(&path).unwrap();
    let mut query = raw
        .prepare("PRAGMA table_info(file_refresh_intents)")
        .unwrap();
    let columns: Vec<String> = query
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        columns,
        vec![
            "workspace_id",
            "mutation_digest",
            "owner",
            "lease_until",
            "deadline",
            "phase"
        ]
    );
}
