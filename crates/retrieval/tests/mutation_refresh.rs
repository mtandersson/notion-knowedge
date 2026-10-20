use notion_knowledge_core::write_refresh::{
    IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError,
};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn grant(epoch: u64) -> notion_knowledge_core::write_refresh::GrantRefreshAuthority {
    notion_knowledge_core::write_refresh::GrantRefreshAuthority::new(
        WORKSPACE.into(),
        "fixture-grant".into(),
        epoch,
        "fixture-owner".into(),
        "e".repeat(64),
    )
    .unwrap()
}

#[test]
fn oauth_refresh_cannot_be_claimed_or_reinterpreted_by_another_authority() {
    use notion_knowledge_retrieval::mutation_refresh::GrantMutationRefreshQueue;
    let db = std::sync::Arc::new(SqliteSyncStateStore::open_in_memory().unwrap());
    let queue = GrantMutationRefreshQueue::new(db.clone(), grant(1)).unwrap();
    queue.admit(&key()).unwrap();
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 100, 10)
            .unwrap()
            .is_none()
    );
    assert_eq!(db.admit(&key()), Err(RefreshQueueError::Conflict));
    let replacement = GrantMutationRefreshQueue::new(db.clone(), grant(2)).unwrap();
    assert_eq!(replacement.admit(&key()), Err(RefreshQueueError::Conflict));
    assert_eq!(replacement.status(&key()), Err(RefreshQueueError::Conflict));
    let claim = db
        .claim_grant_mutation_refresh(WORKSPACE, 100, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claim.authority, Some(grant(1)));
    queue.cancel(&key()).unwrap();
    assert!(!db.mutation_refresh_is_current(&claim, 101).unwrap());
    assert_eq!(
        db.complete_mutation_refresh(&claim, 101, true),
        Err(RefreshQueueError::ClaimLost)
    );
    assert_eq!(queue.status(&key()).unwrap(), Some(IndexStatus::Failed));
    assert_eq!(replacement.retry(&key()), Err(RefreshQueueError::Conflict));
    queue.retry(&key()).unwrap();
    let retry = db
        .claim_grant_mutation_refresh(WORKSPACE, 102, 10)
        .unwrap()
        .unwrap();
    assert!(retry.generation > claim.generation);
    assert_eq!(retry.authority, Some(grant(1)));
}

#[test]
fn integration_refresh_is_never_enrolled_into_an_oauth_grant() {
    use notion_knowledge_retrieval::mutation_refresh::GrantMutationRefreshQueue;
    let db = std::sync::Arc::new(SqliteSyncStateStore::open_in_memory().unwrap());
    db.admit(&key()).unwrap();
    let queue = GrantMutationRefreshQueue::new(db.clone(), grant(1)).unwrap();
    assert_eq!(queue.admit(&key()), Err(RefreshQueueError::Conflict));
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 100, 10)
            .unwrap()
            .is_none()
    );
    let claim = db
        .claim_mutation_refresh(WORKSPACE, 100, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claim.authority, None);
    db.complete_mutation_refresh(&claim, 101, true).unwrap();
    assert_eq!(db.status(&key()).unwrap(), Some(IndexStatus::Completed));
}

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
    db.retry_mutation_refresh(&key()).unwrap();
    assert_eq!(db.status(&key()).unwrap(), Some(IndexStatus::Queued));
    let retry = db
        .claim_mutation_refresh(WORKSPACE, 131, 10)
        .unwrap()
        .unwrap();
    assert_eq!(retry.generation, 4);
    db.complete_mutation_refresh(&retry, 132, true).unwrap();
    assert_eq!(
        db.complete_mutation_refresh(&third, 130, true),
        Err(RefreshQueueError::ClaimLost)
    );
}
