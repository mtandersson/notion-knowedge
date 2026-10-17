use notion_knowledge_core::write_refresh::{
    IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError,
};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn key() -> RefreshKey {
    RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "b".repeat(64),
        page_id: "cccccccc-cccc-cccc-cccc-cccccccccccc".into(),
    }
}

#[test]
fn admission_is_durable_deduplicated_and_bound_to_one_page() {
    let path = std::env::temp_dir().join(format!(
        "nk-mutation-queue-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let db = SqliteSyncStateStore::open(&path).unwrap();
        assert_eq!(db.admit(&key()).unwrap(), IndexStatus::Queued);
        assert_eq!(db.admit(&key()).unwrap(), IndexStatus::Queued);
        let mut collision = key();
        collision.page_id = "dddddddd-dddd-dddd-dddd-dddddddddddd".into();
        assert_eq!(db.admit(&collision), Err(RefreshQueueError::Conflict));
    }
    let db = SqliteSyncStateStore::open_existing(&path).unwrap();
    let claim = db
        .claim_mutation_refresh(WORKSPACE, 100, 10)
        .unwrap()
        .unwrap();
    assert_eq!(db.status(&key()).unwrap(), Some(IndexStatus::Running));
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 109, 10)
            .unwrap()
            .is_none()
    );
    db.complete_mutation_refresh(&claim, 109, true).unwrap();
    assert_eq!(db.admit(&key()).unwrap(), IndexStatus::Completed);
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 200, 10)
            .unwrap()
            .is_none()
    );
    drop(db);
    let db = SqliteSyncStateStore::open_existing(&path).unwrap();
    assert_eq!(db.status(&key()).unwrap(), Some(IndexStatus::Completed));
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn expired_claims_are_fenced_and_recovery_is_bounded_without_write_replay() {
    let db = SqliteSyncStateStore::open_in_memory().unwrap();
    db.admit(&key()).unwrap();
    let first = db
        .claim_mutation_refresh(WORKSPACE, 100, 10)
        .unwrap()
        .unwrap();
    let second = db
        .claim_mutation_refresh(WORKSPACE, 110, 10)
        .unwrap()
        .unwrap();
    assert!(!db.mutation_refresh_is_current(&first, 110).unwrap());
    assert_eq!(
        db.complete_mutation_refresh(&first, 110, true),
        Err(RefreshQueueError::ClaimLost)
    );
    assert!(db.mutation_refresh_is_current(&second, 119).unwrap());
    let third = db
        .claim_mutation_refresh(WORKSPACE, 120, 10)
        .unwrap()
        .unwrap();
    assert_eq!(third.generation, 3);
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 130, 10)
            .unwrap()
            .is_none()
    );
    assert_eq!(db.admit(&key()).unwrap(), IndexStatus::Failed);
    assert_eq!(
        db.complete_mutation_refresh(&third, 130, true),
        Err(RefreshQueueError::ClaimLost)
    );
}
