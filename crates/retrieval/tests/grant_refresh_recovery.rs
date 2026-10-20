use notion_knowledge_core::write_refresh::{
    GrantRefreshAuthority, IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError,
};
use notion_knowledge_retrieval::{
    mutation_refresh::GrantMutationRefreshQueue, sync_state::SqliteSyncStateStore,
};
use std::sync::Arc;

const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

#[test]
fn every_grant_identity_component_is_immutable_after_admission() {
    let db = Arc::new(SqliteSyncStateStore::open_in_memory().unwrap());
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "f".repeat(64),
        page_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
    };
    GrantMutationRefreshQueue::new(db.clone(), authority('d'))
        .unwrap()
        .admit(&key)
        .unwrap();
    for changed in [
        GrantRefreshAuthority::new(
            WORKSPACE.into(),
            "other-grant".into(),
            7,
            "fixture-owner".into(),
            "d".repeat(64),
        )
        .unwrap(),
        GrantRefreshAuthority::new(
            WORKSPACE.into(),
            "durable-fixture-grant".into(),
            8,
            "fixture-owner".into(),
            "d".repeat(64),
        )
        .unwrap(),
        GrantRefreshAuthority::new(
            WORKSPACE.into(),
            "durable-fixture-grant".into(),
            7,
            "other-owner".into(),
            "d".repeat(64),
        )
        .unwrap(),
        authority('e'),
    ] {
        let queue = GrantMutationRefreshQueue::new(db.clone(), changed).unwrap();
        assert_eq!(queue.admit(&key), Err(RefreshQueueError::Conflict));
        assert_eq!(queue.status(&key), Err(RefreshQueueError::Conflict));
        assert_eq!(queue.cancel(&key), Err(RefreshQueueError::Conflict));
        assert_eq!(queue.retry(&key), Err(RefreshQueueError::Conflict));
    }
}

fn authority(policy: char) -> GrantRefreshAuthority {
    GrantRefreshAuthority::new(
        WORKSPACE.into(),
        "durable-fixture-grant".into(),
        7,
        "fixture-owner".into(),
        policy.to_string().repeat(64),
    )
    .unwrap()
}

#[test]
fn restart_recovers_the_same_grant_claim_and_never_adopts_changed_scope() {
    let path = std::env::temp_dir().join(format!(
        "nk-grant-refresh-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "c".repeat(64),
        page_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
    };
    let old_claim = {
        let db = Arc::new(SqliteSyncStateStore::open(&path).unwrap());
        let queue = GrantMutationRefreshQueue::new(db.clone(), authority('d')).unwrap();
        assert_eq!(queue.admit(&key).unwrap(), IndexStatus::Queued);
        db.claim_grant_mutation_refresh(WORKSPACE, 100, 10)
            .unwrap()
            .unwrap()
    };
    let db = Arc::new(SqliteSyncStateStore::open_existing(&path).unwrap());
    let changed_scope = GrantMutationRefreshQueue::new(db.clone(), authority('e')).unwrap();
    assert_eq!(changed_scope.admit(&key), Err(RefreshQueueError::Conflict));
    assert_eq!(changed_scope.retry(&key), Err(RefreshQueueError::Conflict));
    let queue = GrantMutationRefreshQueue::new(db.clone(), authority('d')).unwrap();
    assert_eq!(queue.status(&key).unwrap(), Some(IndexStatus::Running));
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 109, 10)
            .unwrap()
            .is_none()
    );
    let recovered = db
        .claim_grant_mutation_refresh(WORKSPACE, 110, 10)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.authority, Some(authority('d')));
    assert!(recovered.generation > old_claim.generation);
    assert_eq!(
        db.complete_mutation_refresh(&old_claim, 111, true),
        Err(RefreshQueueError::ClaimLost)
    );
    db.complete_mutation_refresh(&recovered, 111, true).unwrap();
    drop(queue);
    drop(changed_scope);
    drop(db);
    let db = Arc::new(SqliteSyncStateStore::open_existing(&path).unwrap());
    let queue = GrantMutationRefreshQueue::new(db.clone(), authority('d')).unwrap();
    assert_eq!(queue.admit(&key).unwrap(), IndexStatus::Completed);
    assert!(
        db.claim_grant_mutation_refresh(WORKSPACE, 200, 10)
            .unwrap()
            .is_none()
    );
    assert!(
        db.claim_mutation_refresh(WORKSPACE, 200, 10)
            .unwrap()
            .is_none()
    );
    drop(queue);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn populated_pre_authority_schema_migrates_without_enrolling_legacy_work() {
    let path = std::env::temp_dir().join(format!(
        "nk-grant-migration-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "a".repeat(64),
        page_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
    };
    let db = SqliteSyncStateStore::open(&path).unwrap();
    db.admit(&key).unwrap();
    drop(db);
    let legacy = rusqlite::Connection::open(&path).unwrap();
    // Undo only the additive authority migration to reconstruct a populated
    // version-17 database, retaining its actual prior tables and migration names.
    legacy.execute_batch("DROP INDEX mutation_refresh_authority_pending; ALTER TABLE mutation_refresh DROP COLUMN authority; DELETE FROM schema_migrations WHERE version=18;").unwrap();
    drop(legacy);
    let db = Arc::new(SqliteSyncStateStore::open(&path).unwrap());
    assert_eq!(db.schema_version().unwrap(), 18);
    assert_eq!(
        notion_knowledge_retrieval::sync_state::LATEST_SCHEMA_VERSION,
        18
    );
    let queue = GrantMutationRefreshQueue::new(db.clone(), authority('d')).unwrap();
    assert_eq!(queue.admit(&key), Err(RefreshQueueError::Conflict));
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
    drop(queue);
    drop(db);
    // A second normal open must recognize the newly applied version.
    let reopened = SqliteSyncStateStore::open(&path).unwrap();
    assert_eq!(reopened.status(&key).unwrap(), Some(IndexStatus::Completed));
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
