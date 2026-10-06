use notion_knowledge_core::sync_state::{
    CrawlCheckpoint, IndexVersion, PageSyncState, PageSyncStatus, SyncStateError,
};

#[test]
fn page_state_distinguishes_content_from_tombstones() {
    let present = PageSyncState::present(
        "page-1".into(),
        "sha256:abc".into(),
        Some("2026-10-05T18:00:00Z".into()),
    )
    .unwrap();

    assert_eq!(present.page_id(), "page-1");
    assert_eq!(present.content_hash(), Some("sha256:abc"));
    assert!(!present.is_tombstone());
    assert!(matches!(
        present.status(),
        PageSyncStatus::Present { content_hash } if content_hash == "sha256:abc"
    ));

    let tombstone =
        PageSyncState::tombstone("page-1".into(), Some("2026-10-05T19:00:00Z".into())).unwrap();
    assert_eq!(tombstone.content_hash(), None);
    assert!(tombstone.is_tombstone());
    assert!(matches!(tombstone.status(), PageSyncStatus::Tombstone));
}

#[test]
fn operational_keys_reject_empty_or_control_values() {
    assert_eq!(
        PageSyncState::present(" ".into(), "hash".into(), None),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(
        CrawlCheckpoint::new("crawl\nkey".into(), None),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(
        IndexVersion::new("chunks".into(), "".into()),
        Err(SyncStateError::InvalidInput)
    );
}
