#![cfg(all(unix, feature = "local-lancedb"))]
//! Credential-free, real Notion HTTP -> real SQLite/LanceDB integration.
use notion_knowledge_core::{
    backend::PageId,
    chunking::ChunkConfig,
    discovery::ExclusionRules,
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    lifecycle::LifecycleScope,
    reconciliation::{Lease, ReconciliationJournal, ReconciliationScope},
    sync_state::SyncStateStore,
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{
    chunks::{GuardedChunkTable, LanceChunkTable, VectorIndexConfig},
    commit::{CommitBinding, CommitOutcome, IndexCommitCoordinator},
    refresh::{AuthoritativePageRefresh, RefreshError},
};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use notion_fake::{
    FOREIGN, FakeNotion as HttpNotion, Model, OTHER, PAGE, ROOT, RequestPause, WORKSPACE,
};
static NEXT: AtomicU64 = AtomicU64::new(0);

struct CountingProvider {
    metadata: EmbeddingMetadata,
    count: AtomicUsize,
    race: Mutex<Option<Arc<Mutex<Model>>>>,
    target_race: Mutex<Option<Arc<Mutex<Model>>>>,
    boundary_pause: Mutex<Option<Arc<RequestPause>>>,
}
impl CountingProvider {
    fn new(metadata: EmbeddingMetadata) -> Self {
        Self {
            metadata,
            count: AtomicUsize::new(0),
            race: Mutex::new(None),
            target_race: Mutex::new(None),
            boundary_pause: Mutex::new(None),
        }
    }
    fn embeddings(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}
impl EmbeddingProvider for CountingProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.count.fetch_add(texts.len(), Ordering::SeqCst);
        if let Some(pause) = self.boundary_pause.lock().unwrap().take() {
            pause.armed.store(true, Ordering::SeqCst);
        }
        if let Some(state) = self.target_race.lock().unwrap().take() {
            state.lock().unwrap().inaccessible_other = true;
        }
        if let Some(state) = self.race.lock().unwrap().take() {
            let mut model = state.lock().unwrap();
            model.title = "Raced title".into();
            model.revision = "2026-10-10T12:00:09Z".into();
        }
        Box::pin(async move {
            Ok(texts
                .iter()
                .map(|text| vec![text.len() as f32, 1.0, 0.0])
                .collect())
        })
    }
}
fn chunk_config() -> ChunkConfig {
    ChunkConfig {
        target_chars: 48,
        overlap_chars: 0,
    }
}
fn clock() -> Arc<dyn Fn() -> i64 + Send + Sync> {
    Arc::new(|| 10)
}
fn authority() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}
struct Fixture {
    root: PathBuf,
    index: PathBuf,
    db: PathBuf,
    coordinator: Arc<IndexCommitCoordinator>,
    journal_scope: ReconciliationScope,
    scope: LifecycleScope,
    embed: EmbeddingMetadata,
    lease: Lease,
}
impl Fixture {
    fn new() -> Self {
        Self::with_scope(authority())
    }
    fn with_scope(scope: LifecycleScope) -> Self {
        Self::with_physical_generation(scope, "1", 1)
    }
    fn with_physical_generation(scope: LifecycleScope, generation: &str, epoch: u64) -> Self {
        notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Off);
        let root = std::env::temp_dir().join(format!(
            "nk-refresh-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = root.join("index");
        let db = root.join("state.sqlite");
        fs::create_dir_all(&index).unwrap();
        let journal_scope = notion_knowledge_retrieval::planner::production_scope(&scope).unwrap();
        let binding =
            CommitBinding::new("chunks", WORKSPACE, &journal_scope, generation, epoch).unwrap();
        let coordinator = IndexCommitCoordinator::initialize(&index, &db, binding).unwrap();
        let lease = coordinator.state().acquire_lease(10, 100).unwrap();
        let embed =
            EmbeddingMetadata::new("count".into(), "fixture".into(), "v1".into(), 3).unwrap();
        Self {
            root,
            index,
            db,
            coordinator,
            journal_scope,
            scope,
            embed,
            lease,
        }
    }
    fn guarded(&self) -> GuardedChunkTable {
        GuardedChunkTable::bind(
            &self.index,
            "chunks",
            self.embed.clone(),
            self.coordinator.clone(),
        )
        .unwrap()
    }
    fn handler(&self, client: Arc<NotionClient>) -> AuthoritativePageRefresh {
        AuthoritativePageRefresh::new(
            client,
            self.guarded(),
            self.coordinator.clone(),
            self.scope.clone(),
            &self.journal_scope,
            chunk_config(),
        )
        .unwrap()
    }
    async fn create(&self) {
        self.guarded()
            .create_empty(self.lease.clone(), clock())
            .await
            .unwrap();
        // What a walk of the root tree records; relation and link targets
        // are placed through this inventory.
        self.place(ROOT, None, true);
        self.place(PAGE, Some(ROOT), true);
        self.place(OTHER, Some(ROOT), true);
    }
    /// Record `page` in the inventory as a walk would: under `parent`, or as
    /// a root, and unreadable (no revision) unless `readable`. Targets the
    /// fake walk never reaches (FOREIGN) are placed by hand: the placement
    /// stands in for an earlier walk that reached them, since relation and
    /// link targets resolve only from the inventory.
    fn place(&self, page: &str, parent: Option<&str>, readable: bool) {
        use notion_knowledge_core::inventory::{InventoryEntry, InventoryParent};
        self.coordinator
            .state()
            .upsert_inventory_entry(
                "fixture-walk",
                &InventoryEntry {
                    page_id: page.into(),
                    parent: parent
                        .map_or(InventoryParent::Root, |p| InventoryParent::Page(p.into())),
                    root_id: ROOT.into(),
                    last_edited_time: if readable {
                        "2026-10-10T10:10:00Z".into()
                    } else {
                        String::new()
                    },
                    in_trash: false,
                },
            )
            .unwrap();
    }
    async fn table(&self) -> LanceChunkTable {
        LanceChunkTable::open(&self.index, "chunks", self.embed.clone())
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_media_rotation_commits_without_reembedding_and_asset_changes_refresh() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let url = "https://prod-files-secure.s3.us-west-2.amazonaws.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/synthetic.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AUTH_GENERATION&X-Amz-Date=AUTH_GENERATION&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=AUTH_GENERATION&x-id=GetObject";
    source.change(|model| {
        model.rotate_media = true;
        model.markdown =
            format!("![Synthetic image]({url})\n\n<file src=\"{url}\">Synthetic document</file>\n");
    });
    let client = source.client();
    // Public authoritative reads remain lossless and expose current URLs.
    let raw_first = client.read_content(PAGE).await.unwrap();
    let raw_second = client.read_content(PAGE).await.unwrap();
    assert_eq!(raw_first.page, raw_second.page);
    assert_ne!(raw_first.markdown, raw_second.markdown);
    assert!(raw_second.markdown.contains("X-Amz-Signature="));
    let handler = index.handler(client);
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    let first = handler
        .refresh_page(
            &page,
            &root,
            "media-first",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(first.outcome, CommitOutcome::Applied);
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    assert!(!chunks.is_empty());
    let count = provider.embeddings();
    let second = handler
        .refresh_page(
            &page,
            &root,
            "media-second",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(second.diff.added, 0);
    assert_eq!(second.diff.removed, 0);
    assert_eq!(provider.embeddings(), count);
    assert_eq!(chunks, index.guarded().page_chunks(PAGE).await.unwrap());
    source.change(|model| {
        model.markdown = model.markdown.replace("synthetic.png", "different.png");
    });
    let third = handler
        .refresh_page(
            &page,
            &root,
            "media-changed",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(third.outcome, CommitOutcome::Applied);
    assert!(third.diff.added > 0);
    assert!(provider.embeddings() > count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_metadata_content_delete_duplicate_and_persistent_restart() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let client = source.client();
    let refresh = index.handler(client.clone());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());

    refresh
        .refresh_page(
            &PageId(OTHER.into()),
            &root,
            "event-other",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let other_chunks = index.guarded().page_chunks(OTHER).await.unwrap();
    let other_embeds = provider.embeddings();

    let first = refresh
        .refresh_page(
            &page,
            &root,
            "event-create-001",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(first.outcome, CommitOutcome::Applied);
    assert!(first.diff.added > 0);
    let initial_embeds = provider.embeddings();
    assert_eq!(initial_embeds - other_embeds, first.diff.added);
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    assert!(!chunks.is_empty());
    let original_ids: Vec<_> = chunks.iter().map(|c| c.chunk_id.clone()).collect();
    let table = index.table().await;
    assert_eq!(
        table.fts_query("text", "Saffron", 10).await.unwrap().len(),
        1
    );
    assert!(source.body_reads.load(Ordering::SeqCst) > 0);
    assert!(source.attempts.load(Ordering::SeqCst) >= 2);
    index
        .guarded()
        .ensure_vector_index(
            VectorIndexConfig {
                num_partitions: Some(1),
                sample_rate: 8,
                max_iterations: 20,
                ..VectorIndexConfig::default()
            },
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        !index
            .table()
            .await
            .vector_query(&[30.0, 1.0, 0.0], 10, 1,)
            .await
            .unwrap()
            .is_empty()
    );

    // No changed rows, no embedding, no index rewrite, no new receipt.
    let duplicate = refresh
        .refresh_page(
            &page,
            &root,
            "event-create-001",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.outcome, CommitOutcome::AlreadyApplied);
    assert_eq!(provider.embeddings(), initial_embeds);

    // Property/title update, same markdown => stable chunk IDs & zero embeds.
    source.change(|m| {
        m.title = "Renamed".into();
        m.revision = "2026-10-10T12:00:01Z".into();
    });
    // An already acknowledged ID is harmless after later source edits; a
    // distinct update event owns the new refresh and citations stay unchanged.
    assert_eq!(
        refresh
            .refresh_page(
                &page,
                &root,
                "event-create-001",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap()
            .outcome,
        CommitOutcome::AlreadyApplied
    );
    assert!(
        index
            .guarded()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .all(|chunk| chunk.metadata.title == "First")
    );

    let updated = refresh
        .refresh_page(
            &page,
            &root,
            "event-title-002",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(updated.outcome, CommitOutcome::Applied);
    assert_eq!(updated.diff.added, 0);
    assert_eq!(provider.embeddings(), initial_embeds);
    let rows = index.guarded().page_chunks(PAGE).await.unwrap();
    assert_eq!(
        rows.iter().map(|c| c.chunk_id.clone()).collect::<Vec<_>>(),
        original_ids
    );
    assert!(rows.iter().all(|c| c.metadata.title == "Renamed"));

    source.change(|m| {
        m.markdown = "# Intro\n\nSaffron ravioli.\n\n## Brand new\n\nViolet soups.\n".into();
        m.revision = "2026-10-10T12:00:02Z".into();
    });
    let change = refresh
        .refresh_page(
            &page,
            &root,
            "event-body-003",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(change.outcome, CommitOutcome::Applied);
    assert!(change.diff.changed > 0 || change.diff.added > 0);
    assert_eq!(
        provider.embeddings() - initial_embeds,
        change.diff.changed + change.diff.added
    );
    let refreshed_chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    let unchanged = chunks
        .iter()
        .find(|chunk| chunk.text.contains("Saffron"))
        .unwrap();
    assert!(
        refreshed_chunks
            .iter()
            .any(|chunk| chunk.chunk_id == unchanged.chunk_id
                && chunk.content_hash == unchanged.content_hash)
    );
    let table = index.table().await;
    assert_eq!(
        table
            .fts_query("text", "evergreen", 10)
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        table.fts_query("text", "Violet", 10).await.unwrap().len(),
        1
    );

    assert_eq!(
        index.guarded().page_chunks(OTHER).await.unwrap(),
        other_chunks
    );
    assert_eq!(
        index
            .table()
            .await
            .fts_query("text", "cinnamon", 10)
            .await
            .unwrap()
            .len(),
        1
    );

    // Reopen both operational DB and the actual Lance table after restart.
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let refreshed = AuthoritativePageRefresh::new(
        client,
        GuardedChunkTable::bind(
            &index.index,
            "chunks",
            index.embed.clone(),
            reopened.clone(),
        )
        .unwrap(),
        reopened.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    let before = provider.embeddings();
    let result = refreshed
        .refresh_page(
            &page,
            &root,
            "event-after-restart-004",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome, CommitOutcome::AlreadyApplied);
    assert_eq!(provider.embeddings(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_scope_recovery_archive_and_failed_source_do_not_corrupt_index() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    source.change(|model| model.parent = FOREIGN.into());
    let denied = handler
        .refresh_page(
            &page,
            &root,
            "out-of-scope",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await;
    assert_eq!(denied.unwrap_err(), RefreshError::OutOfScope);
    assert_eq!(provider.embeddings(), 0);
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);

    source.change(|model| model.parent = ROOT.into());
    let applied = handler
        .refresh_page(
            &page,
            &root,
            "moved-inside",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(applied.outcome, CommitOutcome::Applied);
    let count = index.table().await.count_rows().await.unwrap();
    assert!(count > 0);

    source.change(|m| {
        m.archived = true;
        m.revision = "2026-10-10T12:00:03Z".into();
    });
    let denied = handler
        .refresh_page(
            &page,
            &root,
            "archived",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await;
    assert!(denied.is_err());
    assert_eq!(index.table().await.count_rows().await.unwrap(), count);
    source.change(|m| {
        m.archived = false;
        m.revision = "2026-10-10T12:00:04Z".into();
    });
    assert_eq!(
        handler
            .refresh_page(
                &page,
                &root,
                "restored",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap()
            .outcome,
        CommitOutcome::Applied
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_identity_or_scope_generation_mismatch_blocks_index_effects() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let mut wrong_scope = authority();
    wrong_scope.generation = 2;
    assert!(matches!(
        AuthoritativePageRefresh::new(
            source.client(),
            index.guarded(),
            index.coordinator.clone(),
            wrong_scope,
            &index.journal_scope,
            chunk_config(),
        ),
        Err(RefreshError::InvalidConfiguration)
    ));

    let provider = CountingProvider::new(
        EmbeddingMetadata::new(
            "different-provider".into(),
            "fixture".into(),
            "v1".into(),
            3,
        )
        .unwrap(),
    );
    let result = index
        .handler(source.client())
        .refresh_page(
            &PageId(PAGE.into()),
            &PageId(ROOT.into()),
            "bad-vector-space",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await;
    assert_eq!(result.unwrap_err(), RefreshError::Index);
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_event_recovers_after_index_apply_before_ack_and_changed_source() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'injected ack failure');END;").unwrap();
    assert_eq!(
        index
            .handler(source.client())
            .refresh_page(
                &page,
                &root,
                "interrupted-event",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Unavailable
    );
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_none()
    );
    assert!(index.coordinator.metadata_identity(PAGE).unwrap().is_none());
    assert_eq!(
        index
            .table()
            .await
            .fts_query("text", "Saffron", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    sql.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    drop(sql);

    // Restart with durable SQLite + index effects intact, then retry the same
    // event even when Notion has changed while the original ack was missing.
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let handler = AuthoritativePageRefresh::new(
        source.client(),
        GuardedChunkTable::bind(
            &index.index,
            "chunks",
            index.embed.clone(),
            reopened.clone(),
        )
        .unwrap(),
        reopened.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    source.change(|model| {
        model.title = "After restart".into();
        model.revision = "2026-10-10T12:00:05Z".into();
    });
    let embeddings = provider.embeddings();
    assert_eq!(
        handler
            .refresh_page(
                &page,
                &root,
                "interrupted-event",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap()
            .outcome,
        CommitOutcome::Applied
    );
    assert_eq!(provider.embeddings(), embeddings);
    assert!(
        index
            .guarded()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .all(|chunk| chunk.metadata.title == "After restart")
    );
    assert_eq!(
        reopened
            .receipt("interrupted-event")
            .unwrap()
            .unwrap()
            .status,
        notion_knowledge_retrieval::commit::ReceiptStatus::Applied
    );
    assert_eq!(
        reopened
            .receipt("interrupted-event")
            .unwrap()
            .unwrap()
            .attempts,
        2
    );
    // Unchanged content is insufficient to acknowledge an event identity
    // owned by an older scope generation, even when its earlier effect applied.
    let mut lifecycle_scope = authority();
    lifecycle_scope.generation = 2;
    let next_scope =
        notion_knowledge_retrieval::planner::production_scope(&lifecycle_scope).unwrap();
    let next_binding = CommitBinding::new("chunks", WORKSPACE, &next_scope, "2", 2).unwrap();
    let next = reopened
        .change_scope(next_binding, &index.lease, 10)
        .unwrap();
    let next_handler = AuthoritativePageRefresh::new(
        source.client(),
        GuardedChunkTable::bind(&index.index, "chunks", index.embed.clone(), next.clone()).unwrap(),
        next,
        lifecycle_scope,
        &next_scope,
        chunk_config(),
    )
    .unwrap();
    assert_eq!(
        next_handler
            .refresh_page(
                &page,
                &root,
                "interrupted-event",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Conflict
    );
    // New generation establishes its own atomic metadata identity/ownership.
    assert_eq!(
        next_handler
            .refresh_page(
                &page,
                &root,
                "legitimate-new-scope-event",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap()
            .outcome,
        CommitOutcome::Applied
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_later_event_does_not_leave_original_retry_receipt_unresolved() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'injected ack failure');END;").unwrap();
    assert!(
        handler
            .refresh_page(
                &page,
                &root,
                "original",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .is_err()
    );
    sql.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    handler
        .refresh_page(
            &page,
            &root,
            "later",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let embeddings = provider.embeddings();
    handler
        .refresh_page(
            &page,
            &root,
            "original",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(provider.embeddings(), embeddings);
    assert_eq!(
        index
            .coordinator
            .receipt("original")
            .unwrap()
            .unwrap()
            .status,
        notion_knowledge_retrieval::commit::ReceiptStatus::Applied
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relation_graph_and_final_ack_failures_preserve_checkpoint_and_replay_after_restart() {
    use notion_knowledge_core::graph::GraphEdgeStore;
    use notion_knowledge_retrieval::commit::ReceiptStatus;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.relation_name = Some("Before".into());
        m.relation_targets = vec![OTHER.into()];
    });
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    index
        .handler(source.client())
        .refresh_page(
            &page,
            &root,
            "baseline",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let checkpoint = index.coordinator.state().page_state(PAGE).unwrap().unwrap();
    let edges = index.coordinator.state().edges_from(PAGE).unwrap();
    for (id, trigger) in [
        (
            "graph-interruption",
            "CREATE TRIGGER reject_effect BEFORE INSERT ON graph_edges BEGIN SELECT RAISE(ABORT,'graph unavailable'); END;",
        ),
        (
            "ack-interruption",
            "CREATE TRIGGER reject_effect BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'ack unavailable'); END;",
        ),
    ] {
        source.change(|m| {
            m.relation_name = Some(id.into());
            m.markdown.push_str("\nA new authoritative paragraph.\n");
        });
        let before = index.coordinator.state().page_state(PAGE).unwrap().unwrap();
        let old_edges = index.coordinator.state().edges_from(PAGE).unwrap();
        let sql = rusqlite::Connection::open(&index.db).unwrap();
        sql.execute_batch(trigger).unwrap();
        assert!(
            index
                .handler(source.client())
                .refresh_page(&page, &root, id, &provider, index.lease.clone(), clock())
                .await
                .is_err()
        );
        assert_eq!(
            index.coordinator.state().page_state(PAGE).unwrap().unwrap(),
            before
        );
        assert_eq!(
            index.coordinator.state().edges_from(PAGE).unwrap(),
            old_edges
        );
        assert_ne!(
            index.coordinator.receipt(id).unwrap().unwrap().status,
            ReceiptStatus::Applied
        );
        sql.execute_batch("DROP TRIGGER reject_effect").unwrap();
        drop(sql);
        let reopened = IndexCommitCoordinator::open(
            &index.index,
            &index.db,
            index.coordinator.binding().clone(),
        )
        .unwrap();
        let handler = AuthoritativePageRefresh::new(
            source.client(),
            GuardedChunkTable::bind(
                &index.index,
                "chunks",
                index.embed.clone(),
                reopened.clone(),
            )
            .unwrap(),
            reopened.clone(),
            authority(),
            &index.journal_scope,
            chunk_config(),
        )
        .unwrap();
        handler
            .refresh_page(&page, &root, id, &provider, index.lease.clone(), clock())
            .await
            .unwrap();
        assert_eq!(
            reopened.receipt(id).unwrap().unwrap().status,
            ReceiptStatus::Applied
        );
        assert!(reopened.current_relation_projection_complete(PAGE).unwrap());
        assert!(
            reopened
                .state()
                .edges_from(PAGE)
                .unwrap()
                .iter()
                .all(|e| e.display_name() == Some(id))
        );
    }
    assert_ne!(
        index.coordinator.state().page_state(PAGE).unwrap().unwrap(),
        checkpoint
    );
    assert_ne!(index.coordinator.state().edges_from(PAGE).unwrap(), edges);
    // A source revert can make the previous graph complete while partial Lance
    // work and its repair receipt still need replay. Never hide that work.
    let snapshot = source.state.lock().unwrap().clone();
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_repair BEFORE INSERT ON graph_edges BEGIN SELECT RAISE(ABORT,'repair interrupted'); END;").unwrap();
    source.change(|m| {
        m.relation_name = Some("Transient".into());
        m.markdown.push_str("\nTransient content.\n");
    });
    assert!(
        index
            .handler(source.client())
            .refresh_page(
                &page,
                &root,
                "baseline",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .is_err()
    );
    let pending = index
        .coordinator
        .unfinished_relation_repair_id("baseline")
        .unwrap()
        .unwrap();
    sql.execute_batch("DROP TRIGGER reject_repair").unwrap();
    source.change(|m| *m = snapshot);
    assert!(
        index
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    index
        .handler(source.client())
        .refresh_page(
            &page,
            &root,
            "baseline",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(
        index.coordinator.receipt(&pending).unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert!(
        index
            .coordinator
            .unfinished_relation_repair_id("baseline")
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .guarded()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .all(|chunk| !chunk.text.contains("Transient content"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_graph_bounds_source_projection_through_low_degree_target_and_keeps_snapshot_fence()
 {
    use notion_knowledge_core::graph::GraphEdgeStore;
    use notion_knowledge_retrieval::graph::CommittedGraph;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    for degree in [256, 257] {
        source.change(|m| {
            // ROOT has only one incoming edge while its source has a large
            // projection that must be validated in full, never truncated.
            m.markdown = format!("[root](https://www.notion.so/{ROOT})\n");
            for i in 1..degree {
                // Distinct unresolved targets still consume projection budget;
                // repeated links to one page are intentionally deduplicated.
                m.markdown.push_str(&format!(
                    "[other](https://www.notion.so/66666666-6666-6666-6666-{i:012x})\n"
                ));
            }
            m.revision = format!("2026-10-10T12:00:{:02}Z", degree - 250);
        });
        handler
            .refresh_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &format!("degree-{degree}"),
                &provider,
                index.lease.clone(),
                clock(),
            )
            .await
            .unwrap();
        assert!(index.coordinator.current_body_links_complete(PAGE).unwrap());
        assert_eq!(
            index
                .coordinator
                .state()
                .edges_to_bounded(ROOT, 1)
                .unwrap()
                .len(),
            1
        );
        let graph = CommittedGraph::new(index.coordinator.clone());
        let snapshot = graph.read_snapshot().unwrap().unwrap();
        let writer = std::fs::File::open(&index.index).unwrap();
        assert!(matches!(
            writer.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        let result = snapshot.edges_to_bounded(ROOT, 1);
        if degree == 256 {
            assert_eq!(result.unwrap().len(), 1);
            assert_eq!(snapshot.edges_from_bounded(PAGE, 256).unwrap().len(), 256);
        } else {
            assert!(result.is_err());
            assert!(snapshot.edges_from_bounded(PAGE, 256).is_err());
        }
        drop(snapshot);
        assert!(writer.try_lock().is_ok());
    }
}

#[tokio::test]
async fn owned_drain_retains_failures_even_when_cancelled_observers_never_consume_them() {
    use notion_knowledge_core::reconciliation::FailureClass;
    use notion_knowledge_retrieval::commit::CommitError;
    for failure in [
        FailureClass::Index,
        FailureClass::Unavailable,
        FailureClass::Conflict,
    ] {
        for queued_before_cancel in [false, true] {
            let index = Fixture::new();
            index.create().await;
            let (started, entered) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let expected_failure = failure.clone();
            let failure = failure.clone();
            let work =
                index
                    .coordinator
                    .maintain(index.lease.clone(), clock(), move |_| async move {
                        started.send(()).unwrap();
                        resume.await.unwrap();
                        Err(failure)
                    });
            entered.await.unwrap();
            let mut observer = Some(work);
            if !queued_before_cancel {
                drop(observer.take());
            }
            release.send(()).unwrap();
            // Joining while the observer still exists forces the successful-send
            // race: the error is queued but the outer future never consumes it.
            let errors = index.coordinator.drain_owned_outcomes().await.unwrap();
            drop(observer);
            assert_eq!(errors, vec![CommitError::Operation(expected_failure)]);
            assert!(!index.coordinator.has_unfinished_commits().unwrap());
            let guard = fs::File::open(&index.index).unwrap();
            guard.try_lock().unwrap();
        }
    }
    let index = Fixture::new();
    index.create().await;
    let observer =
        index
            .coordinator
            .maintain(index.lease.clone(), Arc::new(|| i64::MAX), |_| async {
                Ok(())
            });
    drop(observer);
    assert_eq!(
        index.coordinator.drain_owned_outcomes().await.unwrap(),
        vec![CommitError::LeaseLost]
    );
    index
        .coordinator
        .state()
        .renew_lease(&index.lease, 10, 100)
        .unwrap();
}

/// Every page one walk reached, as the sync consumer receives them.
async fn walked_pages(
    source: &HttpNotion,
    scope: &LifecycleScope,
) -> std::collections::BTreeMap<String, notion_knowledge_notion::walk::WalkedPage> {
    let (send, mut receive) = tokio::sync::mpsc::channel(64);
    let client = source.client();
    let walk = async move { client.walk_tree_with_search(scope, Some(&send)).await };
    let collect = async {
        let mut pages = std::collections::BTreeMap::new();
        while let Some(page) = receive.recv().await {
            pages.insert(page.entry.page_id.clone(), page);
        }
        pages
    };
    let (walk, pages) = tokio::join!(walk, collect);
    walk.unwrap();
    pages
}

#[tokio::test]
async fn walked_page_is_prepared_and_committed_without_reading_it_again() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let pages = walked_pages(&source, &index.scope).await;
    let walked = source.requests().len();
    let prepared = handler
        .prepare_walked(&pages[PAGE], "walked")
        .await
        .unwrap();
    let result = handler
        .refresh_prefetched(prepared, &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    assert_eq!(result.outcome, CommitOutcome::Applied);
    assert_eq!(source.requests().len(), walked, "no request after the walk");
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    assert!(chunks.iter().any(|c| c.text.contains("Saffron")));
}

#[tokio::test]
async fn source_change_after_the_walk_read_commits_that_read_without_a_recheck() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let pages = walked_pages(&source, &index.scope).await;
    let prepared = handler.prepare_walked(&pages[PAGE], "stale").await.unwrap();
    // Edited and moved out after its single read: the next walk reconciles it.
    source.change(|m| {
        m.markdown = "# Intro\n\nJuniper gnocchi.\n".into();
        m.parent = FOREIGN.into();
    });
    let walked = source.requests().len();
    let result = handler
        .refresh_prefetched(prepared, &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    assert_eq!(result.outcome, CommitOutcome::Applied);
    assert_eq!(source.requests().len(), walked);
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    assert!(chunks.iter().any(|c| c.text.contains("Saffron")));
    assert!(!chunks.iter().any(|c| c.text.contains("Juniper")));
}

#[tokio::test]
async fn unreached_page_is_removed_without_reading_it() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let pages = walked_pages(&source, &index.scope).await;
    let prepared = handler.prepare_walked(&pages[PAGE], "index").await.unwrap();
    handler
        .refresh_prefetched(prepared, &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    let before = source.requests().len();
    let removed = handler
        .remove_unreached(
            &PageId(PAGE.into()),
            "walk-removal",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(removed.outcome, CommitOutcome::Applied);
    assert_eq!(source.requests().len(), before);
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_some_and(|state| state.is_tombstone())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_authority_conflict_during_preparation_is_never_retried_as_a_source_change() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let root = PageId(ROOT.into());
    handler
        .refresh_page(
            &PageId(PAGE.into()),
            &root,
            "shared-receipt",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let embedded = provider.embeddings();

    // The receipt belongs to another page, so this work item has no authority.
    let pages = walked_pages(&source, &index.scope).await;
    let prepared = handler
        .prepare_walked(&pages[OTHER], "shared-receipt")
        .await;
    assert_eq!(prepared.err(), Some(RefreshError::Conflict));
    assert_eq!(provider.embeddings(), embedded);
    assert!(index.guarded().page_chunks(OTHER).await.unwrap().is_empty());
}

/// Sync one page as the walk consumer does: one walk, then the page it reached
/// is prepared from that single read and committed.
async fn sync_walked(
    index: &Fixture,
    handler: &AuthoritativePageRefresh,
    source: &HttpNotion,
    page: &str,
    operation: &str,
    provider: &CountingProvider,
) -> Result<notion_knowledge_retrieval::refresh::RefreshResult, RefreshError> {
    let pages = walked_pages(source, &index.scope).await;
    let walked = pages.get(page).expect("the walk reached the page");
    let prepared = handler.prepare_walked(walked, operation).await?;
    handler
        .refresh_prefetched(prepared, provider, index.lease.clone(), clock())
        .await
}

fn relation_edges(index: &Fixture) -> Vec<notion_knowledge_core::graph::GraphEdge> {
    use notion_knowledge_core::graph::GraphEdgeStore;
    index
        .coordinator
        .state()
        .edges_from(PAGE)
        .unwrap()
        .into_iter()
        .filter(|e| e.relation_type().starts_with("relation:"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_relations_replace_only_their_family_and_repair_both_noop_paths() {
    use notion_knowledge_core::graph::{GraphEdge, GraphEdgeStore, GraphTarget};
    use notion_knowledge_retrieval::commit::{ReceiptStatus, RelationProjection};
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.markdown = format!("[root](https://www.notion.so/{ROOT})");
        m.relation_name = Some("Ingredients".into());
        m.relation_targets = vec![OTHER.into(), FOREIGN.into()];
    });
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let link = GraphEdge::new(
        PAGE.into(),
        GraphTarget::Page {
            page_id: ROOT.into(),
        },
        "link:page".into(),
        "markdown:0".into(),
    )
    .unwrap();
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-initial",
        &provider,
    )
    .await
    .unwrap();
    let first = index
        .coordinator
        .receipt("relation-initial")
        .unwrap()
        .unwrap();
    assert_eq!(first.status, ReceiptStatus::Applied);
    let edges = index.coordinator.state().edges_from(PAGE).unwrap();
    assert!(edges.contains(&link));
    let relations = relation_edges(&index);
    assert_eq!(relations.len(), 2);
    assert!(
        relations
            .iter()
            .all(|e| e.display_name() == Some("Ingredients")
                && e.provenance() == "property:related-stable")
    );
    // OTHER is placed by the inventory; FOREIGN is not, so it stays unresolved.
    assert!(relations.iter().any(
        |e| matches!(e.target(), GraphTarget::Unresolved { reference } if reference == FOREIGN)
    ));
    assert!(
        relations
            .iter()
            .any(|e| matches!(e.target(), GraphTarget::Page { page_id } if page_id == OTHER))
    );
    assert!(index.guarded().page_chunks(OTHER).await.unwrap().is_empty());
    assert!(
        index
            .guarded()
            .page_chunks(FOREIGN)
            .await
            .unwrap()
            .is_empty()
    );
    let projection =
        RelationProjection::new(index.coordinator.binding(), PAGE, false, relations).unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&projection)
            .unwrap()
    );
    let repeat = sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-repeat",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(repeat.outcome, CommitOutcome::AlreadyApplied);
    assert!(
        index
            .coordinator
            .receipt("relation-repeat")
            .unwrap()
            .is_none()
    );
    let embeds = provider.embeddings();
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    // Missing graph rows despite a compatible marker repairs the unchanged shortcut.
    sql.execute(
        "DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'",
        [PAGE],
    )
    .unwrap();
    let repaired = sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-repair",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(repaired.outcome, CommitOutcome::Applied);
    assert!(
        index
            .coordinator
            .relation_projection_complete(&projection)
            .unwrap()
    );
    // An immutable applied receipt gets a NEW repair operation after explicit invalidation.
    sql.execute(
        "DELETE FROM page_relation_projections WHERE page_id=?1",
        [PAGE],
    )
    .unwrap();
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-initial",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(
        index
            .coordinator
            .receipt("relation-initial")
            .unwrap()
            .unwrap(),
        first
    );
    assert!(
        index
            .coordinator
            .relation_projection_complete(&projection)
            .unwrap()
    );
    sql.execute("UPDATE graph_edges SET display_name='stale label' WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'",[PAGE]).unwrap();
    let (left, right) = tokio::join!(
        sync_walked(
            &index,
            &handler,
            &source,
            PAGE,
            "relation-initial",
            &provider
        ),
        sync_walked(
            &index,
            &handler,
            &source,
            PAGE,
            "relation-initial",
            &provider
        )
    );
    assert!(left.is_ok() || right.is_ok());
    for result in [left, right] {
        if let Err(error) = result {
            assert_eq!(error, RefreshError::Conflict);
        }
    }
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-initial",
        &provider,
    )
    .await
    .unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&projection)
            .unwrap()
    );
    assert_eq!(
        index
            .coordinator
            .receipt("relation-initial")
            .unwrap()
            .unwrap(),
        first
    );
    assert_eq!(provider.embeddings(), embeds);
    source.change(|m| m.relation_name = Some("Renamed ingredients".into()));
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-rename",
        &provider,
    )
    .await
    .unwrap();
    assert!(
        relation_edges(&index)
            .iter()
            .all(|e| e.display_name() == Some("Renamed ingredients"))
    );
    // A target the inventory no longer places in scope resolves to nothing.
    index.place(OTHER, Some(ROOT), false);
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-inaccessible",
        &provider,
    )
    .await
    .unwrap();
    assert!(
        relation_edges(&index)
            .iter()
            .all(|e| matches!(e.target(), GraphTarget::Unresolved { .. }))
    );
    assert!(index.guarded().page_chunks(OTHER).await.unwrap().is_empty());
    index.place(OTHER, Some(ROOT), true);
    source.change(|m| m.relation_name = None);
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-remove",
        &provider,
    )
    .await
    .unwrap();
    let zero = RelationProjection::new(index.coordinator.binding(), PAGE, false, vec![]).unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&zero)
            .unwrap()
    );
    assert_eq!(
        index.coordinator.state().edges_from(PAGE).unwrap(),
        vec![link.clone()]
    );
    let repeat = sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "valid-zero-repeat",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(repeat.outcome, CommitOutcome::AlreadyApplied);
    assert!(
        index
            .coordinator
            .receipt("valid-zero-repeat")
            .unwrap()
            .is_none()
    );
    sql.execute(
        "DELETE FROM page_relation_projections WHERE page_id=?1",
        [PAGE],
    )
    .unwrap();
    sync_walked(&index, &handler, &source, PAGE, "zero-repair", &provider)
        .await
        .unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&zero)
            .unwrap()
    );
    // A page the next walk no longer reaches (trashed, then moved out) is
    // removed without being read, and takes its relations with it.
    source.change(|m| m.archived = true);
    handler
        .remove_unreached(
            &page,
            "relation-delete",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let tombstone =
        RelationProjection::new(index.coordinator.binding(), PAGE, true, vec![]).unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&tombstone)
            .unwrap()
    );
    assert_eq!(index.coordinator.state().edges_from(PAGE).unwrap(), vec![]);
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    source.change(|m| {
        m.archived = false;
        m.relation_name = Some("Restored".into());
        m.relation_targets = vec![OTHER.into()];
    });
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "relation-restored",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(relation_edges(&index).len(), 1);
    source.change(|m| m.parent = FOREIGN.into());
    handler
        .remove_unreached(
            &page,
            "relation-scope-removal",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        index
            .coordinator
            .relation_projection_complete(&tombstone)
            .unwrap()
    );
    assert!(
        index
            .coordinator
            .state()
            .edges_from(PAGE)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_recovers_missing_applied_projection_with_durable_repair_before_ack() {
    use notion_knowledge_core::graph::GraphEdgeStore;
    use notion_knowledge_retrieval::commit::ReceiptStatus;
    let mut index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.relation_name = Some("Recovered".into());
        m.relation_targets = vec![OTHER.into()];
    });
    let provider = CountingProvider::new(index.embed.clone());
    let original = "relation-recovery";
    sync_walked(
        &index,
        &index.handler(source.client()),
        &source,
        PAGE,
        original,
        &provider,
    )
    .await
    .unwrap();
    let receipt = index.coordinator.receipt(original).unwrap().unwrap();
    assert_eq!(receipt.status, ReceiptStatus::Applied);
    let checkpoint = index.coordinator.state().page_state(PAGE).unwrap();
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute_batch("DELETE FROM page_relation_projections; DELETE FROM graph_edges; CREATE TRIGGER reject_graph BEFORE INSERT ON graph_edges BEGIN SELECT RAISE(ABORT,'repair graph failure'); END;").unwrap();
    assert!(
        sync_walked(
            &index,
            &index.handler(source.client()),
            &source,
            PAGE,
            original,
            &provider
        )
        .await
        .is_err()
    );
    assert_eq!(
        index.coordinator.state().page_state(PAGE).unwrap(),
        checkpoint
    );
    assert_eq!(
        index.coordinator.receipt(original).unwrap().unwrap(),
        receipt
    );
    let repair = index.coordinator.relation_repair_id(original).unwrap();
    assert_ne!(repair, original);
    assert_eq!(
        index.coordinator.receipt(&repair).unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    sql.execute_batch("DROP TRIGGER reject_graph").unwrap();
    drop(sql);
    index.coordinator =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    assert_eq!(
        index.coordinator.relation_repair_id(original).unwrap(),
        repair
    );
    sync_walked(
        &index,
        &index.handler(source.client()),
        &source,
        PAGE,
        original,
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(
        index.coordinator.receipt(original).unwrap().unwrap(),
        receipt
    );
    let repaired = index.coordinator.receipt(&repair).unwrap().unwrap();
    assert_eq!(
        (repaired.status, repaired.attempts),
        (ReceiptStatus::Applied, 2)
    );
    assert!(
        index
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    assert_eq!(
        index.coordinator.state().edges_from(PAGE).unwrap()[0].display_name(),
        Some("Recovered")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn target_only_excluded_and_broken_ancestry_converge_without_source_changes() {
    use notion_knowledge_core::graph::{GraphEdgeStore, GraphTarget};
    let mut scope = authority();
    scope.exclusions.descendants_of.insert(OTHER.into());
    let index = Fixture::with_scope(scope);
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.relation_name = Some("Related".into());
        m.relation_targets = vec![FOREIGN.into()];
    });
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    // The target is placed under the root by an earlier walk.
    index.place(FOREIGN, Some(ROOT), true);
    assert!(handler.any_resolvable(&[FOREIGN.into()]).await.unwrap());
    sync_walked(&index, &handler, &source, PAGE, "target-seed", &provider)
        .await
        .unwrap();
    let checkpoint = index.coordinator.state().page_state(PAGE).unwrap();
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    let edges = index.coordinator.state().edges_from(PAGE).unwrap();
    assert!(matches!(edges[0].target(), GraphTarget::Page { .. }));
    for (run, parent, resolved) in [
        ("excluded-chain", OTHER, false),
        ("reentry-exclusion", ROOT, true),
        (
            "broken-chain",
            "99999999-9999-9999-9999-999999999999",
            false,
        ),
        ("reentry-broken", ROOT, true),
    ] {
        // Only the target's placement changes: the page itself is untouched.
        index.place(FOREIGN, Some(parent), true);
        assert_eq!(
            handler.any_resolvable(&[FOREIGN.into()]).await.unwrap(),
            resolved
        );
        handler
            .resolve_relations(
                &page,
                notion_knowledge_core::inventory::InventoryParent::Page(ROOT.into()),
                &root,
                run,
                &provider,
                index.lease.clone(),
                clock(),
            )
            .await
            .unwrap();
        let edges = index.coordinator.state().edges_from(PAGE).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(
            matches!(edges[0].target(), GraphTarget::Page { .. }),
            resolved
        );
        assert_eq!(
            index.coordinator.state().page_state(PAGE).unwrap(),
            checkpoint
        );
        assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), chunks);
        assert!(
            index
                .guarded()
                .page_chunks(FOREIGN)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(source.foreign_body_reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_body_links_follow_authorized_snapshot_cleanup_and_durable_repair() {
    use notion_knowledge_core::{
        aliases::{AliasSources, PageAliasStore},
        graph::{GraphEdgeStore, GraphTarget},
    };
    use notion_knowledge_retrieval::commit::ReceiptStatus;
    let aliases = AliasSources::new(true, vec![]).unwrap();
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.relation_name = Some("Related".into());
        m.relation_targets = vec![OTHER.into()];
        m.markdown = format!(
            concat!(
                "[page](https://www.notion.so/{0}) [duplicate](https://www.notion.so/{0})\n",
                "<mention-page url=\"https://www.notion.so/{0}\"/>\n",
                "[block](https://www.notion.so/{0}#22222222222222222222222222222222)\n",
                "[denied](https://www.notion.so/{1})\n",
                "[broken](https://www.notion.so/malformed?token=fixture-secret)\n",
                "[external](https://example.org/{0}) [lookalike](https://notion.so.evil.org/{0})\n",
                "`[code](https://www.notion.so/{2})`\n",
                "```\n<page url=\"https://www.notion.so/{2}\"/>\n```\n",
                "\\<page url=\"https://www.notion.so/{2}\"/>\n"
            ),
            OTHER, FOREIGN, ROOT
        );
    });
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let handler = index
        .handler(source.client())
        .with_alias_sources(aliases.clone());
    sync_walked(&index, &handler, &source, PAGE, "links-first", &provider)
        .await
        .unwrap();
    let first = index.coordinator.receipt("links-first").unwrap().unwrap();
    let edges = index.coordinator.state().edges_from(PAGE).unwrap();
    let links: Vec<_> = edges
        .iter()
        .filter(|edge| edge.relation_type().starts_with("link:"))
        .cloned()
        .collect();
    assert_eq!(
        links.len(),
        4,
        "page duplicate/mention collapse, block, denied, invalid"
    );
    assert_eq!(
        links
            .iter()
            .filter(
                |edge| matches!(edge.target(), GraphTarget::Page { page_id } if page_id == OTHER)
            )
            .count(),
        2
    );
    assert!(links.iter().any(|edge| matches!(edge.target(), GraphTarget::Unresolved { reference } if reference == FOREIGN)));
    assert!(links.iter().any(|edge| matches!(edge.target(), GraphTarget::Unresolved { reference } if reference.starts_with("invalid-page-link:"))));
    assert!(!format!("{links:?}").contains("fixture-secret"));
    assert!(
        links
            .iter()
            .all(|edge| edge.provenance().starts_with("markdown:"))
    );
    // Targets are placed from the inventory and never indexed through a link.
    assert!(index.guarded().page_chunks(OTHER).await.unwrap().is_empty());
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute(
        "DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,5)='link:'",
        [PAGE],
    )
    .unwrap();
    sql.execute_batch("CREATE TRIGGER reject_link BEFORE INSERT ON graph_edges WHEN substr(NEW.relation_type,1,5)='link:' BEGIN SELECT RAISE(ABORT,'link failure'); END;").unwrap();
    assert!(
        sync_walked(&index, &handler, &source, PAGE, "links-first", &provider)
            .await
            .is_err()
    );
    assert_eq!(
        index.coordinator.receipt("links-first").unwrap().unwrap(),
        first
    );
    assert!(!index.coordinator.current_body_links_complete(PAGE).unwrap());
    let repair = index
        .coordinator
        .unfinished_alias_repair_id("links-first", PAGE)
        .unwrap()
        .unwrap();
    assert_ne!(
        index.coordinator.receipt(&repair).unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    sql.execute_batch("DROP TRIGGER reject_link").unwrap();
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let restarted = AuthoritativePageRefresh::new(
        source.client(),
        GuardedChunkTable::bind(
            &index.index,
            "chunks",
            index.embed.clone(),
            reopened.clone(),
        )
        .unwrap(),
        reopened.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap()
    .with_alias_sources(aliases);
    sync_walked(&index, &restarted, &source, PAGE, "links-first", &provider)
        .await
        .unwrap();
    assert_eq!(
        reopened.receipt(&repair).unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert_eq!(reopened.state().edges_from(PAGE).unwrap(), edges);
    // A target the inventory stops placing changes the link effect without
    // indexing target content.
    index.place(OTHER, Some(ROOT), false);
    sync_walked(&index, &restarted, &source, PAGE, "links-denied", &provider)
        .await
        .unwrap();
    assert!(
        reopened
            .state()
            .edges_from(PAGE)
            .unwrap()
            .iter()
            .all(|edge| matches!(edge.target(), GraphTarget::Unresolved { .. }))
    );
    index.place(OTHER, Some(ROOT), true);
    source.change(|m| {
        m.markdown = format!("<page url=\"https://www.notion.so/{OTHER}\"/>");
    });
    sync_walked(
        &index,
        &restarted,
        &source,
        PAGE,
        "links-mention",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(reopened.state().edges_from(PAGE).unwrap().len(), 2);
    source.change(|m| m.markdown.clear());
    sync_walked(&index, &restarted, &source, PAGE, "links-empty", &provider)
        .await
        .unwrap();
    let remaining = reopened.state().edges_from(PAGE).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].relation_type(), "relation:related-stable");
    assert_eq!(
        reopened
            .state()
            .lookup_alias(WORKSPACE, ROOT, "First")
            .unwrap(),
        vec![PAGE.to_owned()]
    );
    source.change(|m| m.markdown = format!("[again](https://www.notion.so/{OTHER})"));
    sync_walked(&index, &restarted, &source, PAGE, "links-again", &provider)
        .await
        .unwrap();
    source.change(|m| m.archived = true);
    restarted
        .remove_unreached(
            &page,
            "links-tombstone",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(reopened.state().edges_from(PAGE).unwrap().is_empty());
    assert!(reopened.current_body_links_complete(PAGE).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_commits_body_links_and_never_acknowledges_missing_link_rows() {
    use notion_knowledge_core::graph::{GraphEdgeStore, GraphTarget};
    use notion_knowledge_retrieval::commit::ReceiptStatus;
    let index = Fixture::new();
    index.create().await;
    index.place(FOREIGN, Some(ROOT), true);
    let source = HttpNotion::start().await;
    source.change(|m| {
        m.markdown = format!("<mention-page url=\"https://www.notion.so/{FOREIGN}\"/>");
        m.relation_name = Some("Related".into());
        m.relation_targets = vec![OTHER.into()];
    });
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "body-link-inventory",
        &provider,
    )
    .await
    .unwrap();
    let edges = index.coordinator.state().edges_from(PAGE).unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().any(|edge| edge.relation_type() == "link:page"
        && matches!(edge.target(), GraphTarget::Page { page_id } if page_id == FOREIGN)));
    // This target is referenced exclusively by the body: the unchanged page
    // must still update the graph when the inventory stops placing it.
    index.place(FOREIGN, Some(ROOT), false);
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "body-only-target-denied",
        &provider,
    )
    .await
    .unwrap();
    assert!(
        index
            .coordinator
            .state()
            .edges_from(PAGE)
            .unwrap()
            .iter()
            .filter(|edge| edge.relation_type().starts_with("link:"))
            .all(|edge| matches!(edge.target(), GraphTarget::Unresolved { .. }))
    );
    index.place(FOREIGN, Some(ROOT), true);
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "body-only-target-restored",
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(index.coordinator.state().edges_from(PAGE).unwrap(), edges);
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute(
        "DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,5)='link:'",
        [PAGE],
    )
    .unwrap();
    sql.execute_batch(&format!("CREATE TRIGGER fail_link BEFORE INSERT ON graph_edges WHEN substr(NEW.relation_type,1,5)='link:' AND NEW.source_page_id='{PAGE}' BEGIN SELECT RAISE(ABORT,'link persistence'); END;")).unwrap();
    assert!(
        sync_walked(
            &index,
            &handler,
            &source,
            PAGE,
            "body-link-repair",
            &provider
        )
        .await
        .is_err()
    );
    assert!(!index.coordinator.current_body_links_complete(PAGE).unwrap());
    assert!(
        index
            .coordinator
            .receipt("body-link-repair")
            .unwrap()
            .is_none_or(|receipt| receipt.status != ReceiptStatus::Applied)
    );
    sql.execute_batch("DROP TRIGGER fail_link").unwrap();
    sync_walked(
        &index,
        &handler,
        &source,
        PAGE,
        "body-link-repair",
        &provider,
    )
    .await
    .unwrap();
    assert!(index.coordinator.current_body_links_complete(PAGE).unwrap());
    assert_eq!(index.coordinator.state().edges_from(PAGE).unwrap(), edges);
    source.change(|m| {
        m.markdown.clear();
        m.revision = "2026-10-11T12:00:00Z".into();
    });
    sync_walked(&index, &handler, &source, PAGE, "body-link-edit", &provider)
        .await
        .unwrap();
    let remaining = index.coordinator.state().edges_from(PAGE).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].relation_type().starts_with("relation:"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_owned_source_poll_releases_guard_after_cancelled_observer_and_lease_loss() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = Arc::new(CountingProvider::new(index.embed.clone()));
    let handler = index.handler(source.client());
    handler
        .refresh_page(
            &PageId(PAGE.into()),
            &PageId(ROOT.into()),
            "noop-seed",
            provider.as_ref(),
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let checkpoint = index.coordinator.state().page_state(PAGE).unwrap();
    *source.pause.owned_directory.lock().unwrap() = Some(index.index.clone());
    pause_owned_commits(&index.index, source.pause.clone());
    source.pause.armed.store(true, Ordering::SeqCst);
    let now = Arc::new(std::sync::atomic::AtomicI64::new(10));
    let owned_now = now.clone();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> =
        Arc::new(move || owned_now.load(Ordering::SeqCst));
    let lease = index.lease.clone();
    let owned_provider = provider.clone();
    let task = tokio::spawn(async move {
        handler
            .refresh_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                "unchanged-cancelled",
                owned_provider.as_ref(),
                lease,
                clock,
            )
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        source.pause.started.notified(),
    )
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    now.store(110, Ordering::SeqCst);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        index.coordinator.drain_owned(),
    )
    .await
    .unwrap()
    .unwrap();
    let probe = fs::File::open(&index.index).unwrap();
    probe.try_lock().unwrap();
    assert_eq!(
        index.coordinator.state().page_state(PAGE).unwrap(),
        checkpoint
    );
    assert!(
        index
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    assert!(
        index
            .coordinator
            .receipt("unchanged-cancelled")
            .unwrap()
            .is_none()
    );
    source.pause.proceed.notify_one();
}

/// Hold the owned commit (guard and fence held, before effects) whenever
/// `pause` is armed: the boundary the removed in-guard source read used to give.
fn pause_owned_commits(index: &std::path::Path, pause: Arc<RequestPause>) {
    notion_knowledge_retrieval::test_hooks::set_commit_pause(
        index,
        Arc::new(move || {
            let pause = pause.clone();
            Box::pin(async move { pause.hold().await })
        }),
    );
}
