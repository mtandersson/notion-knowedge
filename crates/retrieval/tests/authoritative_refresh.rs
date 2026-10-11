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
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const OTHER: &str = "55555555-5555-5555-5555-555555555555";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const FOREIGN: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct Model {
    markdown: String,
    title: String,
    revision: String,
    parent: String,
    archived: bool,
    root_revision: String,
}
impl Default for Model {
    fn default() -> Self {
        Self {
            markdown: "# Intro\n\nSaffron ravioli.\n\n## Other\n\nAn evergreen section.\n".into(),
            title: "First".into(),
            revision: "2026-10-10T12:00:00Z".into(),
            parent: ROOT.into(),
            archived: false,
            root_revision: "2026-10-10T11:00:00Z".into(),
        }
    }
}
struct HttpNotion {
    state: Arc<Mutex<Model>>,
    attempts: Arc<AtomicUsize>,
    body_reads: Arc<AtomicUsize>,
    fail_once: Arc<AtomicBool>,
    server: tokio::task::JoinHandle<()>,
    port: u16,
}
impl HttpNotion {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(Model::default()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let body_reads = Arc::new(AtomicUsize::new(0));
        let fail_once = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let values = state.clone();
        let counts = attempts.clone();
        let bodies = body_reads.clone();
        let fail = fail_once.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut part = [0u8; 2048];
                    let size = stream.read(&mut part).await.unwrap();
                    if size == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&part[..size]);
                    if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                        break;
                    }
                }
                let req = String::from_utf8(bytes).unwrap();
                assert!(req.contains("authorization: Bearer fixture-credential"));
                assert!(req.contains("notion-version: 2026-03-11"));
                assert!(req.starts_with("GET /v1/"));
                counts.fetch_add(1, Ordering::SeqCst);
                let route = req
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let snapshot = values.lock().unwrap().clone();
                let response = if route == format!("/v1/pages/{PAGE}/markdown") {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    json!({
                        "object": "page_markdown",
                        "id": PAGE,
                        "markdown": snapshot.markdown,
                        "truncated": false,
                        "unknown_block_ids": []
                    })
                } else if route == format!("/v1/pages/{PAGE}") {
                    page_metadata(
                        PAGE,
                        &snapshot.parent,
                        &snapshot.title,
                        &snapshot.revision,
                        snapshot.archived,
                    )
                } else if route == format!("/v1/pages/{OTHER}/markdown") {
                    json!({"object":"page_markdown", "id":OTHER,
                        "markdown":"# Separate\n\nUntouched cinnamon.\n", "truncated":false,
                        "unknown_block_ids":[]})
                } else if route == format!("/v1/pages/{OTHER}") {
                    page_metadata(OTHER, ROOT, "Separate", "2026-10-10T11:00:00Z", false)
                } else if route == format!("/v1/pages/{ROOT}") {
                    page_metadata(ROOT, "workspace", "Root", &snapshot.root_revision, false)
                } else if route == format!("/v1/pages/{FOREIGN}") {
                    page_metadata(
                        FOREIGN,
                        "workspace",
                        "Foreign",
                        "2026-10-10T11:00:00Z",
                        false,
                    )
                } else {
                    json!({"object":"error", "code":"object_not_found"})
                };
                let bad = fail.swap(false, Ordering::SeqCst);
                let status = if bad {
                    503
                } else if response.get("code").is_some() {
                    404
                } else {
                    200
                };
                let body = if bad { json!({}) } else { response }.to_string();
                stream.write_all(format!("HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        Self {
            state,
            attempts,
            body_reads,
            fail_once,
            server,
            port,
        }
    }
    fn client(&self) -> Arc<NotionClient> {
        Arc::new(
            NotionClient::integration("fixture-credential")
                .unwrap()
                .with_loopback_fixture(self.port),
        )
    }
    fn change(&self, change: impl FnOnce(&mut Model)) {
        change(&mut self.state.lock().unwrap());
    }
}
impl Drop for HttpNotion {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn page_metadata(id: &str, parent: &str, title: &str, revision: &str, archived: bool) -> Value {
    json!({
        "object": "page",
        "id": id,
        "url": format!("https://www.notion.so/{id}"),
        "last_edited_time": revision,
        "in_trash": archived,
        "parent": if parent == "workspace" {
            json!({"type":"workspace","workspace":true})
        } else {
            json!({"type":"page_id","page_id":parent})
        },
        "properties": {
            "Title": {
                "id":"title", "type":"title",
                "title":[{"plain_text":title}]
            },
            "level": {
                "id":"level", "type":"number", "number":3
            }
        }
    })
}

struct CountingProvider {
    metadata: EmbeddingMetadata,
    count: AtomicUsize,
    race: Mutex<Option<Arc<Mutex<Model>>>>,
}
impl CountingProvider {
    fn new(metadata: EmbeddingMetadata) -> Self {
        Self {
            metadata,
            count: AtomicUsize::new(0),
            race: Mutex::new(None),
        }
    }
    fn embeddings(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
    fn race_on_next_embedding(&self, source: Arc<Mutex<Model>>) {
        *self.race.lock().unwrap() = Some(source);
    }
}
impl EmbeddingProvider for CountingProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.count.fetch_add(texts.len(), Ordering::SeqCst);
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
    embed: EmbeddingMetadata,
    lease: Lease,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "nk-refresh-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = root.join("index");
        let db = root.join("state.sqlite");
        fs::create_dir_all(&index).unwrap();
        let journal_scope =
            ReconciliationScope::new(vec![ROOT.into()], vec![], "phase3", "1").unwrap();
        let binding = CommitBinding::new("chunks", WORKSPACE, &journal_scope, "1", 1).unwrap();
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
            authority(),
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
    assert!(source.attempts.load(Ordering::SeqCst) >= 20);
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
    source.fail_once.store(true, Ordering::SeqCst);
    let failed = handler
        .refresh_page(
            &page,
            &root,
            "source-failed",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await;
    assert_eq!(failed.unwrap_err(), RefreshError::Source);
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);

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
async fn changed_source_during_embedding_fails_commit_without_ack_or_index_mutation() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    provider.race_on_next_embedding(source.state.clone());
    let page = PageId(PAGE.into());
    let root = PageId(ROOT.into());
    let failure = handler
        .refresh_page(
            &page,
            &root,
            "raced-source",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await;
    assert_eq!(failure.unwrap_err(), RefreshError::Conflict);
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_none()
    );
    // Retry the original durable work item after the authoritative source
    // changes. No replacement webhook/event ID is required for recovery.
    let redo = handler
        .refresh_page(
            &page,
            &root,
            "raced-source",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(redo.outcome, CommitOutcome::Applied);
    assert!(index.table().await.count_rows().await.unwrap() > 0);
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
    let next_scope = ReconciliationScope::new(vec![ROOT.into()], vec![], "phase3", "2").unwrap();
    let next_binding = CommitBinding::new("chunks", WORKSPACE, &next_scope, "2", 2).unwrap();
    let next = reopened
        .change_scope(next_binding, &index.lease, 10)
        .unwrap();
    let mut lifecycle_scope = authority();
    lifecycle_scope.generation = 2;
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
        CommitOutcome::AlreadyApplied
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
async fn lifecycle_current_authority_removes_restores_and_preserves_unrelated_page() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    source.change(|m| m.archived = true);
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "unowned-delete",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::OutOfScope
    );
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_none()
    );
    source.change(|m| m.archived = false);
    handler
        .apply_lifecycle(
            &PageId(OTHER.into()),
            "other",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    handler
        .apply_lifecycle(
            &page,
            "delete-hint-but-active",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let unaffected = index.guarded().page_chunks(OTHER).await.unwrap();
    let embeddings = provider.embeddings();
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
    for (id, inactive) in [("inactive", true), ("outside", false)] {
        source.change(|m| {
            m.archived = inactive;
            m.parent = FOREIGN.into();
        });
        handler
            .apply_lifecycle(&page, id, &provider, index.lease.clone(), clock())
            .await
            .unwrap();
        assert!(
            index
                .coordinator
                .state()
                .page_state(PAGE)
                .unwrap()
                .unwrap()
                .is_tombstone()
        );
        assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
        assert!(
            index
                .table()
                .await
                .fts_query("text", "Saffron", 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            index
                .table()
                .await
                .vector_query(&[30.0, 1.0, 0.0], 10, 1)
                .await
                .unwrap()
                .iter()
                .all(|hit| hit.page_id != PAGE)
        );
        assert_eq!(
            index.guarded().page_chunks(OTHER).await.unwrap(),
            unaffected
        );
        assert_eq!(provider.embeddings(), embeddings);
    }
    source.change(|m| {
        m.archived = false;
        m.parent = ROOT.into();
    });
    handler
        .apply_lifecycle(
            &page,
            "move-into-or-restored",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        !index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
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
    // Old acknowledged delete is harmless even after restore; a new hint owns
    // any subsequent transition.
    source.change(|m| m.archived = true);
    assert_eq!(
        handler
            .apply_lifecycle(&page, "inactive", &provider, index.lease.clone(), clock())
            .await
            .unwrap()
            .outcome,
        CommitOutcome::AlreadyApplied
    );
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_move_between_allowed_roots_preserves_ids_and_vectors() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let mut scope = authority();
    scope.roots.push(PageId(FOREIGN.into()));
    let handler = AuthoritativePageRefresh::new(
        source.client(),
        index.guarded(),
        index.coordinator.clone(),
        scope,
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    let page = PageId(PAGE.into());
    handler
        .apply_lifecycle(&page, "initial", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    let old = index.guarded().page_chunks(PAGE).await.unwrap();
    let count = provider.embeddings();
    source.change(|m| {
        m.parent = FOREIGN.into();
        m.title = "Moved title".into();
        m.revision = "2026-10-10T12:00:02Z".into();
    });
    handler
        .apply_lifecycle(
            &page,
            "allowed-move",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let moved = index.guarded().page_chunks(PAGE).await.unwrap();
    assert_eq!(provider.embeddings(), count);
    assert_eq!(
        old.iter().map(|c| &c.chunk_id).collect::<Vec<_>>(),
        moved.iter().map(|c| &c.chunk_id).collect::<Vec<_>>()
    );
    assert!(
        moved
            .iter()
            .all(|c| c.metadata.source.root_page_id == FOREIGN
                && c.metadata.source.workspace_id == WORKSPACE
                && c.metadata.title == "Moved title")
    );
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_reopen_recomputes_unacknowledged_action_in_both_directions() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    let fail = "CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'injected ack failure');END;";
    sql.execute_batch(fail).unwrap();
    assert_eq!(
        index
            .handler(source.client())
            .apply_lifecycle(
                &page,
                "refresh-interrupted",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Unavailable
    );
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    source.change(|m| m.archived = true);
    // A second interruption removes every chunk before the first checkpoint
    // exists. The page-bound pending receipt must survive as ownership evidence.
    assert_eq!(
        index
            .handler(source.client())
            .apply_lifecycle(
                &page,
                "refresh-interrupted",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Unavailable
    );
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_none()
    );
    sql.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    source.change(|m| m.archived = true);
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
    handler
        .apply_lifecycle(
            &page,
            "refresh-interrupted",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        reopened
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    source.change(|m| m.archived = false);
    handler
        .apply_lifecycle(&page, "restore", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    source.change(|m| m.archived = true);
    sql.execute_batch(fail).unwrap();
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "delete-interrupted",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Unavailable
    );
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        !reopened
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    sql.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    source.change(|m| m.archived = false);
    let reopened_again =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let handler = AuthoritativePageRefresh::new(
        source.client(),
        GuardedChunkTable::bind(
            &index.index,
            "chunks",
            index.embed.clone(),
            reopened_again.clone(),
        )
        .unwrap(),
        reopened_again.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    handler
        .apply_lifecycle(
            &page,
            "delete-interrupted",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
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
    assert!(
        !reopened_again
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_failures_never_authorize_removal_or_acknowledgement() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    handler
        .apply_lifecycle(&page, "seed", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    let original = index.guarded().page_chunks(PAGE).await.unwrap();
    source.change(|m| m.parent = "66666666-6666-6666-6666-666666666666".into());
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "inaccessible-ancestor",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Source
    );
    assert!(
        index
            .coordinator
            .receipt("inaccessible-ancestor")
            .unwrap()
            .is_none()
    );
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    source.change(|m| m.parent = "invalid-parent-id".into());
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "incomplete-ancestor",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Source
    );
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    source.change(|m| m.parent = ROOT.into());
    source.fail_once.store(true, Ordering::SeqCst);
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "source-unavailable",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Source
    );
    assert!(
        index
            .coordinator
            .receipt("source-unavailable")
            .unwrap()
            .is_none()
    );
    let table_path = index.index.join("chunks.lance");
    let held_path = index.index.join("held.lance");
    fs::rename(&table_path, &held_path).unwrap();
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "index-unavailable",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Index
    );
    fs::rename(&held_path, &table_path).unwrap();
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    source.change(|m| {
        m.parent = ROOT.into();
        m.archived = true;
    });
    let incompatible = CountingProvider::new(
        EmbeddingMetadata::new("foreign".into(), "fixture".into(), "v1".into(), 3).unwrap(),
    );
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "wrong-provider",
                &incompatible,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Index
    );
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    let newer = index.coordinator.state().acquire_lease(111, 100).unwrap();
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "stale-owner-delete",
                &provider,
                index.lease.clone(),
                Arc::new(|| 111)
            )
            .await
            .unwrap_err(),
        RefreshError::Conflict
    );
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    // A source restore after removal preparation but inside the owned commit
    // must reject the obsolete delete before touching either index family.
    let state = source.state.clone();
    let fired = Arc::new(AtomicBool::new(false));
    let race_clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
        if !fired.swap(true, Ordering::SeqCst) {
            state.lock().unwrap().archived = false;
        }
        111
    });
    assert_eq!(
        handler
            .apply_lifecycle(&page, "raced-delete", &provider, newer.clone(), race_clock)
            .await
            .unwrap_err(),
        RefreshError::Conflict
    );
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), original);
    assert_eq!(
        index
            .coordinator
            .receipt("raced-delete")
            .unwrap()
            .unwrap()
            .failure,
        Some(notion_knowledge_core::reconciliation::FailureClass::Conflict)
    );
    source.change(|m| m.archived = true);
    handler
        .apply_lifecycle(
            &page,
            "raced-delete",
            &provider,
            newer.clone(),
            Arc::new(|| 111),
        )
        .await
        .unwrap();
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
}

struct PausedProvider {
    metadata: EmbeddingMetadata,
    started: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}
impl EmbeddingProvider for PausedProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.started.notify_one();
            self.proceed.notified().await;
            Ok(texts
                .iter()
                .map(|text| vec![text.len() as f32, 1.0, 0.0])
                .collect())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_canonical_refresh_cannot_resurrect_authoritatively_removed_page() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    handler
        .apply_lifecycle(&page, "seed", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    source.change(|m| {
        m.markdown = "# Changed\n\nStale resurrection content.\n".into();
        m.revision = "2026-10-10T12:00:08Z".into();
    });
    let paused = Arc::new(PausedProvider {
        metadata: index.embed.clone(),
        started: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    let refresh_handler = handler.clone();
    let refresh_provider = paused.clone();
    let lease = index.lease.clone();
    let competing_refresh = tokio::spawn(async move {
        refresh_handler
            .refresh_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                "competing-refresh",
                refresh_provider.as_ref(),
                lease,
                clock(),
            )
            .await
    });
    paused.started.notified().await;
    source.change(|m| m.archived = true);
    handler
        .apply_lifecycle(
            &page,
            "concurrent-delete",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    paused.proceed.notify_one();
    assert_eq!(
        competing_refresh.await.unwrap().unwrap_err(),
        RefreshError::Conflict
    );
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    assert!(
        index
            .table()
            .await
            .fts_query("text", "resurrection", 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_excluded_ancestry_removes_and_changed_scope_generation_fences_old_handler() {
    use notion_knowledge_core::lifecycle::{LifecycleStatus, PageLifecycle};
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let mut scope = authority();
    scope.exclusions.descendants_of.insert(FOREIGN.into());
    let client = source.client();
    let handler = AuthoritativePageRefresh::new(
        client.clone(),
        index.guarded(),
        index.coordinator.clone(),
        scope.clone(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    let page = PageId(PAGE.into());
    handler
        .apply_lifecycle(&page, "seed", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    source.change(|m| m.parent = FOREIGN.into());
    assert!(matches!(
        client.lifecycle(&page, &scope).await.unwrap().status,
        LifecycleStatus::Excluded { .. }
    ));
    handler
        .apply_lifecycle(
            &page,
            "excluded-move",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    assert!(
        index
            .table()
            .await
            .fts_query("text", "Saffron", 10)
            .await
            .unwrap()
            .is_empty()
    );
    source.change(|m| m.parent = ROOT.into());
    handler
        .apply_lifecycle(&page, "restore", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    let next_scope = ReconciliationScope::new(vec![ROOT.into()], vec![], "phase3", "2").unwrap();
    index
        .coordinator
        .change_scope(
            CommitBinding::new("chunks", WORKSPACE, &next_scope, "2", 2).unwrap(),
            &index.lease,
            10,
        )
        .unwrap();
    source.change(|m| m.archived = true);
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "stale-generation-delete",
                &provider,
                index.lease.clone(),
                clock()
            )
            .await
            .unwrap_err(),
        RefreshError::Conflict
    );
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        !index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_refresh_rejects_same_root_ancestry_change_at_commit() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    let state = source.state.clone();
    let fired = Arc::new(AtomicBool::new(false));
    let race_clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
        if !fired.swap(true, Ordering::SeqCst) {
            state.lock().unwrap().root_revision = "2026-10-10T12:00:09Z".into();
        }
        10
    });
    assert_eq!(
        handler
            .apply_lifecycle(
                &page,
                "ancestor-changed",
                &provider,
                index.lease.clone(),
                race_clock
            )
            .await
            .unwrap_err(),
        RefreshError::Conflict
    );
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .is_none()
    );
    handler
        .apply_lifecycle(
            &page,
            "ancestor-changed",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_empty_page_can_be_tombstoned_without_chunks_or_embeddings() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| m.markdown = String::new());
    let handler = index.handler(source.client());
    let provider = CountingProvider::new(index.embed.clone());
    let page = PageId(PAGE.into());
    handler
        .apply_lifecycle(&page, "empty-page", &provider, index.lease.clone(), clock())
        .await
        .unwrap();
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        !index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    source.change(|m| m.archived = true);
    handler
        .apply_lifecycle(
            &page,
            "empty-delete",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    assert_eq!(provider.embeddings(), 0);
}
