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
struct ExtraPage {
    title: String,
    visible: bool,
    archived: bool,
}
#[derive(Clone)]
struct Model {
    markdown: String,
    title: String,
    revision: String,
    parent: String,
    archived: bool,
    root_revision: String,
    hide_page: bool,
    inaccessible_other: bool,
    inaccessible_root: bool,
    inaccessible_page: bool,
    race_metadata: bool,
    race_move: bool,
    level: u64,
    extra: std::collections::BTreeMap<String, ExtraPage>,
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
            hide_page: false,
            inaccessible_other: false,
            inaccessible_root: false,
            inaccessible_page: false,
            race_metadata: false,
            race_move: false,
            level: 3,
            extra: Default::default(),
        }
    }
}
#[derive(Default)]
struct RequestPause {
    armed: AtomicBool,
    started: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}
struct HttpNotion {
    state: Arc<Mutex<Model>>,
    attempts: Arc<AtomicUsize>,
    body_reads: Arc<AtomicUsize>,
    fail_once: Arc<AtomicBool>,
    server: tokio::task::JoinHandle<()>,
    port: u16,
    pause: Arc<RequestPause>,
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
        let pause = Arc::new(RequestPause::default());
        let boundary = pause.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut part = [0u8; 2048];
                    let size = stream.read(&mut part).await.unwrap();
                    if size == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&part[..size]);
                    if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                        break;
                    }
                }
                if bytes.is_empty() {
                    continue;
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
                if route == format!("/v1/pages/{PAGE}")
                    && boundary.armed.swap(false, Ordering::SeqCst)
                {
                    boundary.started.notify_one();
                    boundary.proceed.notified().await;
                }
                if route.ends_with("/markdown") {
                    bodies.fetch_add(1, Ordering::SeqCst);
                }
                let snapshot = values.lock().unwrap().clone();
                if snapshot.race_metadata && route == format!("/v1/pages/{PAGE}") {
                    values.lock().unwrap().title.push('!');
                }
                if snapshot.race_move && route == format!("/v1/pages/{PAGE}") {
                    values.lock().unwrap().parent = if snapshot.parent == ROOT {
                        FOREIGN.into()
                    } else {
                        ROOT.into()
                    };
                }
                let response = if (snapshot.inaccessible_root
                    && route == format!("/v1/pages/{ROOT}"))
                    || (snapshot.inaccessible_other && route == format!("/v1/pages/{OTHER}"))
                    || (snapshot.inaccessible_page && route == format!("/v1/pages/{PAGE}"))
                {
                    json!({"object":"error", "code":"object_not_found"})
                } else if route == format!("/v1/blocks/{ROOT}/children?page_size=100") {
                    let mut children = vec![
                        json!({"object":"block","id":OTHER,"type":"child_page","archived":false,"has_children":false,"parent":{"type":"page_id","page_id":ROOT},"child_page":{"title":"Separate"}}),
                    ];
                    for (id, page) in &snapshot.extra {
                        if page.visible {
                            children.push(json!({"object":"block","id":id,"type":"child_page","archived":false,"has_children":false,"parent":{"type":"page_id","page_id":ROOT},"child_page":{"title":page.title}}));
                        }
                    }
                    if !snapshot.hide_page {
                        children.push(json!({"object":"block","id":PAGE,"type":"child_page","archived":false,"has_children":false,"parent":{"type":"page_id","page_id":ROOT},"child_page":{"title":"First"}}));
                    }
                    json!({"object":"list","results":children,"has_more":false,"next_cursor":null})
                } else if route.starts_with("/v1/blocks/") && route.contains("/children") {
                    json!({"object":"list","results":[],"has_more":false,"next_cursor":null})
                } else if route == format!("/v1/pages/{PAGE}/markdown") {
                    json!({
                        "object": "page_markdown",
                        "id": PAGE,
                        "markdown": snapshot.markdown,
                        "truncated": false,
                        "unknown_block_ids": []
                    })
                } else if route == format!("/v1/pages/{PAGE}") {
                    let mut page = page_metadata(
                        PAGE,
                        &snapshot.parent,
                        &snapshot.title,
                        &snapshot.revision,
                        snapshot.archived,
                    );
                    page["properties"]["level"]["number"] = json!(snapshot.level);
                    page
                } else if route == format!("/v1/pages/{OTHER}/markdown") {
                    json!({"object":"page_markdown", "id":OTHER,
                        "markdown":"# Separate\n\nUntouched cinnamon.\n", "truncated":false,
                        "unknown_block_ids":[]})
                } else if route == format!("/v1/pages/{OTHER}") {
                    page_metadata(OTHER, ROOT, "Separate", "2026-10-10T11:00:00Z", false)
                } else if let Some((id, page)) = snapshot
                    .extra
                    .iter()
                    .find(|(id, _)| route == format!("/v1/pages/{id}"))
                {
                    page_metadata(id, ROOT, &page.title, "2026-10-10T12:00:00Z", page.archived)
                } else if let Some((id, page)) = snapshot
                    .extra
                    .iter()
                    .find(|(id, _)| route == format!("/v1/pages/{id}/markdown"))
                {
                    json!({"object":"page_markdown", "id":id,
                        "markdown":format!("# {}\n\nExtra source contents.\n",page.title), "truncated":false,
                        "unknown_block_ids":[]})
                } else if route == format!("/v1/pages/{ROOT}/markdown") {
                    json!({"object":"page_markdown", "id":ROOT,
                        "markdown":"# Root\n\nRoot overview.\n", "truncated":false,
                        "unknown_block_ids":[]})
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
                let _ = stream.write_all(format!("HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
            }
        });
        Self {
            state,
            attempts,
            body_reads,
            fail_once,
            server,
            port,
            pause,
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
    boundary_pause: Mutex<Option<Arc<RequestPause>>>,
}
impl CountingProvider {
    fn new(metadata: EmbeddingMetadata) -> Self {
        Self {
            metadata,
            count: AtomicUsize::new(0),
            race: Mutex::new(None),
            boundary_pause: Mutex::new(None),
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
        if let Some(pause) = self.boundary_pause.lock().unwrap().take() {
            pause.armed.store(true, Ordering::SeqCst);
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
            ReconciliationScope::new(vec![ROOT.into()], vec![], "webhook-v1", "1").unwrap();
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

fn planner(
    index: &Fixture,
    source: &HttpNotion,
) -> notion_knowledge_retrieval::planner::AuthoritativeReconciliationPlanner {
    notion_knowledge_retrieval::planner::AuthoritativeReconciliationPlanner::new(
        source.client(),
        index.guarded(),
        index.coordinator.clone(),
        authority(),
    )
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planner_dry_run_missing_metadata_stale_unchanged_restored_and_empty_pages() {
    use notion_knowledge_retrieval::planner::Difference;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let handler = index.handler(source.client());
    let initial = planner(&index, &source).plan().await.unwrap();
    assert!(initial.errors.is_empty(), "{initial:?}");
    assert_eq!(initial.scanned, 3);
    assert_eq!(initial.planned_refresh(), 3);
    assert_eq!(initial.changed(), 0);
    assert_eq!(source.body_reads.load(Ordering::SeqCst), 0);
    handler
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "initial",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let before = index.guarded().page_chunks(PAGE).await.unwrap();
    let before_state = index.coordinator.state().page_state(PAGE).unwrap();
    let reads = source.body_reads.load(Ordering::SeqCst);
    let embeddings = provider.embeddings();
    let unchanged = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        unchanged
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Unchanged
    );
    assert_eq!(source.body_reads.load(Ordering::SeqCst), reads);
    assert_eq!(provider.embeddings(), embeddings);
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
    assert_eq!(
        index.coordinator.state().page_state(PAGE).unwrap(),
        before_state
    );
    source.change(|m| m.level = 9);
    let properties = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        properties
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Stale
    );
    source.change(|m| {
        m.level = 3;
        m.root_revision = "2026-10-10T11:01:00Z".into();
    });
    let ancestry = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        ancestry
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Stale
    );
    source.change(|m| m.title = "Metadata only".into());
    let stale = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        stale
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Stale
    );
    // Dry-run leaves no terminal receipt that suppresses the subsequent refresh.
    handler
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "metadata",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(
        index
            .guarded()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .all(|c| c.metadata.title == "Metadata only")
    );
    source.change(|m| {
        m.archived = true;
        m.hide_page = true;
    });
    let deleted = planner(&index, &source).plan().await.unwrap();
    assert_eq!(deleted.planned_delete(), 1);
    assert_eq!(deleted.deleted(), 0);
    handler
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "deleted",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let tombstone = planner(&index, &source).plan().await.unwrap();
    assert!(
        tombstone
            .local
            .get(PAGE)
            .unwrap()
            .checkpoint
            .as_ref()
            .unwrap()
            .is_tombstone()
    );
    assert_eq!(tombstone.planned_delete(), 0);
    source.change(|m| {
        m.archived = false;
        m.hide_page = false;
        m.markdown.clear();
    });
    let restored = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        restored
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Restored
    );
    handler
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "empty-restored",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    let empty = planner(&index, &source).plan().await.unwrap();
    assert!(empty.local.contains_key(PAGE));
    assert_eq!(
        empty
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Unchanged
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planner_absence_access_failures_and_races_never_authorize_deletion() {
    use notion_knowledge_retrieval::planner::{Difference, PlanError};
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    index
        .handler(source.client())
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "seed",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let before = index.guarded().page_chunks(PAGE).await.unwrap();
    source.change(|m| m.hide_page = true);
    let absent = planner(&index, &source).plan().await.unwrap();
    assert_eq!(absent.planned_delete(), 0);
    assert_eq!(
        absent
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Unchanged
    );
    source.change(|m| {
        m.archived = true;
        m.inaccessible_other = true;
    });
    let inaccessible = planner(&index, &source).plan().await.unwrap();
    assert!(!inaccessible.traversal_complete);
    assert_eq!(inaccessible.planned_delete(), 0);
    source.change(|m| {
        m.inaccessible_other = false;
        m.inaccessible_root = true;
    });
    let failed = planner(&index, &source).plan().await.unwrap();
    assert_eq!(failed.planned_delete(), 0);
    assert_eq!(
        failed.run_errors,
        vec![PlanError::Traversal(
            notion_knowledge_core::backend::BackendErrorKind::NotFound
        )]
    );
    source.change(|m| {
        m.archived = false;
        m.hide_page = false;
        m.inaccessible_root = false;
        m.inaccessible_page = true;
    });
    let bad = planner(&index, &source).plan().await.unwrap();
    assert!(bad.errors.contains_key(PAGE));
    assert!(bad.pages.iter().any(|p| p.page_id == OTHER));
    source.change(|m| {
        m.inaccessible_page = false;
        m.race_metadata = true;
    });
    let race = planner(&index, &source).plan().await.unwrap();
    assert_eq!(race.errors.get(PAGE), Some(&PlanError::Conflict));
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
    source.change(|m| {
        m.race_metadata = false;
        m.race_move = true;
    });
    let move_race = planner(&index, &source).plan().await.unwrap();
    assert_eq!(move_race.planned_delete(), 0);
    assert_eq!(move_race.errors.get(PAGE), Some(&PlanError::Conflict));
    source.change(|m| {
        m.race_move = false;
        m.hide_page = true;
        m.parent = FOREIGN.into();
    });
    let moved = planner(&index, &source).plan().await.unwrap();
    assert_eq!(moved.planned_delete(), 1);
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planner_rejects_foreign_binding_unowned_index_and_refreshes_legacy_identity() {
    use notion_knowledge_retrieval::planner::{
        AuthoritativeReconciliationPlanner, Difference, PlanError,
    };
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    index
        .handler(source.client())
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "owned",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let different_directory = Fixture::new();
    assert!(matches!(
        AuthoritativeReconciliationPlanner::new(
            source.client(),
            different_directory.guarded(),
            index.coordinator.clone(),
            authority()
        ),
        Err(PlanError::Ownership)
    ));
    for scope in [
        LifecycleScope {
            workspace_id: FOREIGN.into(),
            ..authority()
        },
        LifecycleScope {
            generation: 2,
            ..authority()
        },
        LifecycleScope {
            exclusions: ExclusionRules {
                page_ids: [PAGE.into()].into_iter().collect(),
                ..ExclusionRules::default()
            },
            ..authority()
        },
    ] {
        assert!(matches!(
            AuthoritativeReconciliationPlanner::new(
                source.client(),
                index.guarded(),
                index.coordinator.clone(),
                scope
            ),
            Err(PlanError::Ownership)
        ));
    }
    let sqlite = rusqlite::Connection::open(&index.db).unwrap();
    sqlite
        .execute(
            "DELETE FROM page_metadata_identity WHERE page_id=?1",
            [PAGE],
        )
        .unwrap();
    let legacy = planner(&index, &source).plan().await.unwrap();
    assert_eq!(
        legacy
            .pages
            .iter()
            .find(|p| p.page_id == PAGE)
            .unwrap()
            .difference,
        Difference::Stale
    );
    let chunks = index.guarded().page_chunks(PAGE).await.unwrap();
    let mut foreign = chunks.clone();
    for chunk in &mut foreign {
        chunk.metadata.source.workspace_id = FOREIGN.into();
    }
    index
        .table()
        .await
        .upsert(
            &index.embed,
            &foreign
                .into_iter()
                .map(|c| {
                    notion_knowledge_retrieval::chunks::EmbeddedChunk::new(c, vec![1.0, 1.0, 0.0])
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    assert_eq!(
        planner(&index, &source)
            .local_inventory()
            .await
            .unwrap_err(),
        PlanError::Ownership
    );
    index
        .table()
        .await
        .upsert(
            &index.embed,
            &chunks
                .into_iter()
                .map(|c| {
                    notion_knowledge_retrieval::chunks::EmbeddedChunk::new(c, vec![1.0, 1.0, 0.0])
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    sqlite
        .execute("DELETE FROM index_commit_receipts WHERE page_id=?1", [PAGE])
        .unwrap();
    assert_eq!(
        planner(&index, &source)
            .local_inventory()
            .await
            .unwrap_err(),
        PlanError::Ownership
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_read_only_planner_plan_roundtrips_without_apply_acknowledgments() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let plan = planner(&index, &source).plan().await.unwrap();
    assert_eq!(plan.inspected_ids.len(), plan.scanned);
    assert_eq!(
        plan.inspected_ids,
        std::collections::BTreeSet::from([PAGE.into(), OTHER.into(), ROOT.into()])
    );
    // Dry-run did not persist operational plan metadata or suppress the real run.
    assert!(index.coordinator.reconciliation_plan("run").is_err());
    assert_eq!(source.body_reads.load(Ordering::SeqCst), 0);
    index
        .coordinator
        .state()
        .start_or_resume(&index.lease, 10, "run", &index.journal_scope)
        .unwrap();
    index
        .coordinator
        .record_reconciliation_plan(&index.lease, 10, "run", &plan)
        .unwrap();
    let durable = index.coordinator.reconciliation_plan("run").unwrap();
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    assert_eq!(reopened.reconciliation_plan("run").unwrap(), durable);
    let counts = reopened.reconciliation_progress("run").unwrap();
    assert_eq!(
        (
            counts.scanned,
            counts.planned_refresh,
            counts.changed,
            counts.deleted
        ),
        (3, 3, 0, 0)
    );
    assert_eq!(counts.pending.len(), 3);
    assert!(reopened.state().page_state(PAGE).unwrap().is_none());
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    assert_eq!(source.body_reads.load(Ordering::SeqCst), 0);
}

fn runner(
    index: &Fixture,
    source: &HttpNotion,
) -> notion_knowledge_retrieval::runner::AuthoritativeReconciliationRunner {
    notion_knowledge_retrieval::runner::AuthoritativeReconciliationRunner::new(
        planner(index, source),
        index.handler(source.client()),
        index.coordinator.clone(),
        index.journal_scope.clone(),
    )
    .unwrap()
}
fn record_plan(
    index: &Fixture,
    run: &str,
    plan: &notion_knowledge_retrieval::planner::ReconciliationPlan,
) {
    index
        .coordinator
        .state()
        .start_or_resume(&index.lease, 10, run, &index.journal_scope)
        .unwrap();
    index
        .coordinator
        .record_reconciliation_plan(&index.lease, 10, run, plan)
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_dry_run_real_apply_and_unchanged_resume_have_exact_receipts_and_counts() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let dry = service.dry_run().await.unwrap();
    assert_eq!(dry.scanned, 3);
    assert_eq!(dry.planned_refresh(), 3);
    assert_eq!(provider.embeddings(), 0);
    assert_eq!(source.body_reads.load(Ordering::SeqCst), 0);
    assert!(index.coordinator.owned_page_ids().unwrap().is_empty());
    assert!(index.coordinator.reconciliation_plan("dry").is_err());
    let first = service
        .apply("first", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        (
            first.scanned,
            first.planned_refresh,
            first.changed,
            first.deleted,
            first.failed_pages()
        ),
        (3, 3, 3, 0, 0)
    );
    assert!(first.pending.is_empty());
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    let body = source.body_reads.load(Ordering::SeqCst);
    let embeddings = provider.embeddings();
    assert_eq!(
        service
            .apply("first", index.lease.clone(), clock(), &provider)
            .await
            .unwrap(),
        first
    );
    let unchanged = service
        .apply("second", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        (
            unchanged.changed,
            unchanged.unchanged,
            unchanged.failed_pages()
        ),
        (0, 3, 0)
    );
    assert_eq!(source.body_reads.load(Ordering::SeqCst), body);
    assert_eq!(provider.embeddings(), embeddings);
    service.complete("first", &index.lease, 10).unwrap();
    assert_eq!(
        service
            .apply("first", index.lease.clone(), clock(), &provider)
            .await
            .unwrap(),
        first
    );
    for page in index
        .coordinator
        .reconciliation_plan("second")
        .unwrap()
        .pages
        .values()
    {
        assert_eq!(
            index
                .coordinator
                .receipt(&page.operation_id)
                .unwrap()
                .unwrap()
                .status,
            notion_knowledge_retrieval::commit::ReceiptStatus::Applied
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_stale_archive_move_restore_and_incomplete_traversal_apply_only_safe_differences()
 {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    service
        .apply("seed", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    source.change(|m| {
        m.title = "Changed metadata".into();
    });
    let stale = service
        .apply("stale", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!((stale.changed, stale.unchanged), (1, 2));
    let embeddings = provider.embeddings();
    source.change(|m| {
        m.archived = true;
        m.hide_page = true;
        m.inaccessible_other = true;
    });
    let incomplete = service
        .apply("incomplete", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(incomplete.deleted, 0);
    assert!(incomplete.failed_pages() > 0);
    assert!(!incomplete.run_failures.is_empty());
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    source.change(|m| m.inaccessible_other = false);
    let archived = service
        .apply("archive", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        (archived.deleted, archived.changed, archived.failed_pages()),
        (1, 0, 0)
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
    assert_eq!(provider.embeddings(), embeddings);
    source.change(|m| {
        m.archived = false;
        m.hide_page = false;
    });
    let restored = service
        .apply("restored", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!((restored.changed, restored.deleted), (1, 0));
    assert!(
        !index
            .coordinator
            .state()
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    source.change(|m| {
        m.parent = FOREIGN.into();
        m.hide_page = true;
    });
    let moved = service
        .apply("move", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(moved.deleted, 1);
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_planned_delete_restoration_and_webhook_write_conflict_require_explicit_replan()
 {
    use notion_knowledge_retrieval::durable_plan::DurableFailure;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    service
        .apply("seed", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    source.change(|m| {
        m.archived = true;
        m.hide_page = true;
    });
    let plan = service.dry_run().await.unwrap();
    assert_eq!(plan.planned_delete(), 1);
    record_plan(&index, "restoration-race", &plan);
    source.change(|m| {
        m.archived = false;
        m.hide_page = false;
        m.title = "Newest restored".into();
    });
    let conflict = service
        .apply("restoration-race", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(conflict.deleted, 0);
    assert_eq!(conflict.failed.get(PAGE), Some(&DurableFailure::Conflict));
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    let retried = service
        .retry_page(
            "restoration-race",
            PAGE,
            index.lease.clone(),
            clock(),
            &provider,
        )
        .await
        .unwrap();
    assert_eq!(
        (retried.changed, retried.deleted, retried.failed_pages()),
        (1, 0, 0)
    );
    source.change(|m| {
        m.title = "Proposal".into();
    });
    let proposal = service.dry_run().await.unwrap();
    record_plan(&index, "webhook-race", &proposal);
    source.change(|m| {
        m.title = "Webhook newest".into();
        m.revision = "2026-10-10T12:01:00Z".into();
    });
    index
        .handler(source.client())
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "webhook-newest",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let before = index.guarded().page_chunks(PAGE).await.unwrap();
    let raced = service
        .apply("webhook-race", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(raced.failed.get(PAGE), Some(&DurableFailure::Conflict));
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
    let retry = service
        .retry_page(
            "webhook-race",
            PAGE,
            index.lease.clone(),
            clock(),
            &provider,
        )
        .await
        .unwrap();
    assert_eq!(retry.failed_pages(), 0);
    assert_eq!(retry.changed, 0);
    assert_eq!(retry.unchanged, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_bad_page_isolated_source_and_embedding_races_are_durable_explicit_failures()
{
    use notion_knowledge_retrieval::durable_plan::DurableFailure;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "source-failure", &plan);
    source.change(|m| m.inaccessible_page = true);
    let failed = service
        .apply("source-failure", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(failed.changed, 2);
    assert_eq!(failed.failed.get(PAGE), Some(&DurableFailure::Source));
    source.change(|m| m.inaccessible_page = false);
    assert_eq!(
        service
            .apply("source-failure", index.lease.clone(), clock(), &provider)
            .await
            .unwrap(),
        failed
    );
    let retry = service
        .retry_page(
            "source-failure",
            PAGE,
            index.lease.clone(),
            clock(),
            &provider,
        )
        .await
        .unwrap();
    assert_eq!((retry.changed, retry.failed_pages()), (3, 0));
    source.change(|m| {
        m.markdown.push_str("\nA fresh section.\n");
        m.revision = "2026-10-10T12:02:00Z".into();
    });
    let before = index.guarded().page_chunks(PAGE).await.unwrap();
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "owned-source-race", &plan);
    provider.race_on_next_embedding(source.state.clone());
    let race = service
        .apply("owned-source-race", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(race.failed.get(PAGE), Some(&DurableFailure::Conflict));
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
    let retry = service
        .retry_page(
            "owned-source-race",
            PAGE,
            index.lease.clone(),
            clock(),
            &provider,
        )
        .await
        .unwrap();
    assert_eq!((retry.changed, retry.failed_pages()), (1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_applied_before_journal_ack_reopens_with_new_fence_and_preserves_historical_target()
 {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "crash", &plan);
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    sql.execute_batch("CREATE TRIGGER stop_journal_ack BEFORE UPDATE OF status ON reconciliation_plan_progress WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'crash before journal ack');END;").unwrap();
    assert!(
        service
            .apply("crash", index.lease.clone(), clock(), &provider)
            .await
            .is_err()
    );
    let progress = index.coordinator.reconciliation_progress("crash").unwrap();
    assert_eq!((progress.changed, progress.pending.len()), (0, 3));
    let page = index
        .coordinator
        .reconciliation_plan("crash")
        .unwrap()
        .pages
        .values()
        .find(|p| {
            index
                .coordinator
                .receipt(&p.operation_id)
                .unwrap()
                .is_some()
        })
        .unwrap()
        .clone();
    assert_eq!(
        index
            .coordinator
            .receipt(&page.operation_id)
            .unwrap()
            .unwrap()
            .status,
        notion_knowledge_retrieval::commit::ReceiptStatus::Applied
    );
    let prepared = index
        .coordinator
        .prepared_reconciliation_operation("crash", &page.page_id)
        .unwrap();
    sql.execute_batch("DROP TRIGGER stop_journal_ack;").unwrap();
    drop(sql);
    index
        .coordinator
        .state()
        .release_lease(&index.lease, 10)
        .unwrap();
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let lease = reopened.state().acquire_lease(11, 100).unwrap();
    assert_ne!(lease.fence, index.lease.fence);
    let guard = GuardedChunkTable::bind(
        &index.index,
        "chunks",
        index.embed.clone(),
        reopened.clone(),
    )
    .unwrap();
    let planner = notion_knowledge_retrieval::planner::AuthoritativeReconciliationPlanner::new(
        source.client(),
        guard.clone(),
        reopened.clone(),
        authority(),
    )
    .unwrap();
    let refresh = AuthoritativePageRefresh::new(
        source.client(),
        guard,
        reopened.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    let resumed = notion_knowledge_retrieval::runner::AuthoritativeReconciliationRunner::new(
        planner,
        refresh,
        reopened.clone(),
        index.journal_scope.clone(),
    )
    .unwrap();
    source.change(|m| m.title = "Edited after crash".into());
    let progress = resumed
        .apply("crash", lease.clone(), Arc::new(|| 11), &provider)
        .await
        .unwrap();
    assert!(progress.successful.contains_key(&page.page_id));
    assert_eq!(
        reopened
            .prepared_reconciliation_operation("crash", &page.page_id)
            .unwrap(),
        prepared
    );
    assert_eq!(
        resumed
            .apply("crash", lease.clone(), Arc::new(|| 11), &provider)
            .await
            .unwrap(),
        progress
    );
    let newest = resumed
        .apply("new-source", lease, Arc::new(|| 11), &provider)
        .await
        .unwrap();
    assert_eq!(newest.failed_pages(), 0);
    assert!(newest.changed > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_index_failure_retains_separate_failed_work_until_explicit_retry() {
    use notion_knowledge_retrieval::durable_plan::DurableFailure;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let wrong = CountingProvider::new(
        EmbeddingMetadata::new("wrong".into(), "fixture".into(), "v1".into(), 3).unwrap(),
    );
    let service = runner(&index, &source);
    let failed = service
        .apply("index-failure", index.lease.clone(), clock(), &wrong)
        .await
        .unwrap();
    assert_eq!((failed.changed, failed.failed.len()), (0, 3));
    assert!(failed.failed.values().all(|v| *v == DurableFailure::Index));
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
    let good = CountingProvider::new(index.embed.clone());
    assert_eq!(
        service
            .apply("index-failure", index.lease.clone(), clock(), &good)
            .await
            .unwrap(),
        failed
    );
    for id in failed.failed.keys() {
        service
            .retry_page("index-failure", id, index.lease.clone(), clock(), &good)
            .await
            .unwrap();
    }
    let progress = index
        .coordinator
        .reconciliation_progress("index-failure")
        .unwrap();
    assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_cancelled_observer_retains_owned_commit_then_recovers_real_receipt() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = Arc::new(CountingProvider::new(index.embed.clone()));
    *provider.boundary_pause.lock().unwrap() = Some(source.pause.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "cancelled", &plan);
    let operation = index
        .coordinator
        .reconciliation_plan("cancelled")
        .unwrap()
        .pages[PAGE]
        .operation_id
        .clone();
    let owned = service.clone();
    let embedding = provider.clone();
    let lease = index.lease.clone();
    let task = tokio::spawn(async move {
        owned
            .apply("cancelled", lease, clock(), embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        source.pause.started.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        index
            .coordinator
            .receipt(&operation)
            .unwrap()
            .unwrap()
            .status,
        notion_knowledge_retrieval::commit::ReceiptStatus::Pending
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    source.pause.proceed.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if index
                .coordinator
                .receipt(&operation)
                .unwrap()
                .is_some_and(|r| {
                    r.status == notion_knowledge_retrieval::commit::ReceiptStatus::Applied
                })
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        index
            .coordinator
            .reconciliation_progress("cancelled")
            .unwrap()
            .pending
            .contains(PAGE)
    );
    assert!(!index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    let progress = service
        .apply("cancelled", index.lease.clone(), clock(), provider.as_ref())
        .await
        .unwrap();
    assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_owned_source_poll_expires_lease_and_new_fence_resumes_without_effects() {
    use notion_knowledge_core::reconciliation::JournalError;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = Arc::new(CountingProvider::new(index.embed.clone()));
    *provider.boundary_pause.lock().unwrap() = Some(source.pause.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "expiry", &plan);
    let operation = index
        .coordinator
        .reconciliation_plan("expiry")
        .unwrap()
        .pages[PAGE]
        .operation_id
        .clone();
    let now = Arc::new(std::sync::atomic::AtomicI64::new(10));
    let observed = now.clone();
    let timed_clock: Arc<dyn Fn() -> i64 + Send + Sync> =
        Arc::new(move || observed.load(Ordering::SeqCst));
    let owned = service.clone();
    let embedding = provider.clone();
    let lease = index.lease.clone();
    let timer = timed_clock.clone();
    let task = tokio::spawn(async move {
        owned
            .apply("expiry", lease, timer, embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        source.pause.started.notified(),
    )
    .await
    .unwrap();
    now.store(110, Ordering::SeqCst);
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(notion_knowledge_retrieval::runner::RunnerError::Journal(
            JournalError::LeaseLost
        ))
    ));
    assert_eq!(
        index
            .coordinator
            .receipt(&operation)
            .unwrap()
            .unwrap()
            .status,
        notion_knowledge_retrieval::commit::ReceiptStatus::Failed
    );
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
    source.pause.proceed.notify_one();
    let lease = index.coordinator.state().acquire_lease(110, 100).unwrap();
    let resumed = service
        .apply("expiry", lease, timed_clock, provider.as_ref())
        .await
        .unwrap();
    assert_eq!((resumed.changed, resumed.failed_pages()), (3, 0));
    assert!(
        service
            .apply("expiry", index.lease.clone(), clock(), provider.as_ref())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_generation_policy_and_expired_fence_reject_pending_plan_without_effects() {
    use notion_knowledge_retrieval::runner::{AuthoritativeReconciliationRunner, RunnerError};
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "authority-change", &plan);
    let mut changed = authority();
    changed.exclusions.page_ids.insert(PAGE.into());
    let incompatible = AuthoritativePageRefresh::new(
        source.client(),
        index.guarded(),
        index.coordinator.clone(),
        changed,
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    assert!(matches!(
        AuthoritativeReconciliationRunner::new(
            planner(&index, &source),
            incompatible,
            index.coordinator.clone(),
            index.journal_scope.clone()
        ),
        Err(RunnerError::InvalidConfiguration)
    ));
    let sql = rusqlite::Connection::open(&index.db).unwrap();
    for statement in [
        "UPDATE index_commit_binding SET epoch=2",
        "UPDATE index_commit_binding SET scope='changed-policy'",
        "UPDATE index_commit_binding SET generation='2'",
    ] {
        sql.execute_batch(statement).unwrap();
        assert!(
            service
                .apply("authority-change", index.lease.clone(), clock(), &provider)
                .await
                .is_err()
        );
        assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
        sql.execute(
            "UPDATE index_commit_binding SET epoch=1,scope=?1,generation='1'",
            [index.journal_scope.identity()],
        )
        .unwrap();
    }
    assert!(
        service
            .apply(
                "authority-change",
                index.lease.clone(),
                Arc::new(|| 110),
                &provider
            )
            .await
            .is_err()
    );
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
    assert_eq!(provider.embeddings(), 0);
    let next = index.coordinator.state().acquire_lease(110, 100).unwrap();
    let progress = service
        .apply("authority-change", next, Arc::new(|| 110), &provider)
        .await
        .unwrap();
    assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_inflight_webhook_and_authoritative_move_cannot_overwrite_new_local_state() {
    use notion_knowledge_retrieval::durable_plan::DurableFailure;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let service = runner(&index, &source);
    let provider = Arc::new(PausedProvider {
        metadata: index.embed.clone(),
        started: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "inflight", &plan);
    let owned = service.clone();
    let embedding = provider.clone();
    let lease = index.lease.clone();
    let task = tokio::spawn(async move {
        owned
            .apply("inflight", lease, clock(), embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        provider.started.notified(),
    )
    .await
    .unwrap();
    let webhook = CountingProvider::new(index.embed.clone());
    index
        .handler(source.client())
        .apply_lifecycle(
            &PageId(PAGE.into()),
            "inflight-webhook",
            &webhook,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    let before = index.guarded().page_chunks(PAGE).await.unwrap();
    source.change(|m| {
        m.parent = FOREIGN.into();
        m.hide_page = true;
    });
    // Other pages may embed too; allow all batches while recording their starts.
    let release = provider.clone();
    let notifier = tokio::spawn(async move {
        loop {
            release.proceed.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });
    let progress = tokio::time::timeout(std::time::Duration::from_secs(30), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    notifier.abort();
    assert_eq!(progress.failed.get(PAGE), Some(&DurableFailure::Conflict));
    assert_eq!(index.guarded().page_chunks(PAGE).await.unwrap(), before);
    let retry = service
        .retry_page("inflight", PAGE, index.lease.clone(), clock(), &webhook)
        .await
        .unwrap();
    assert_eq!((retry.deleted, retry.failed_pages()), (1, 0));
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_owned_source_outage_is_source_failure_without_index_mutation() {
    use notion_knowledge_retrieval::durable_plan::DurableFailure;
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let service = runner(&index, &source);
    let provider = Arc::new(CountingProvider::new(index.embed.clone()));
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "owned-outage", &plan);
    *provider.boundary_pause.lock().unwrap() = Some(source.pause.clone());
    let owned = service.clone();
    let embedding = provider.clone();
    let lease = index.lease.clone();
    let task = tokio::spawn(async move {
        owned
            .apply("owned-outage", lease, clock(), embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        source.pause.started.notified(),
    )
    .await
    .unwrap();
    source.change(|m| m.inaccessible_page = true);
    source.pause.proceed.notify_one();
    let progress = task.await.unwrap().unwrap();
    assert_eq!(progress.failed.get(PAGE), Some(&DurableFailure::Source));
    assert_eq!(progress.changed, 2);
    assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_cancelled_explicit_retry_reopens_its_persisted_fresh_proposal() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let service = runner(&index, &source);
    let provider = CountingProvider::new(index.embed.clone());
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "retry-cancel", &plan);
    source.change(|m| m.inaccessible_page = true);
    let failed = service
        .apply("retry-cancel", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(failed.failed.len(), 1);
    source.change(|m| {
        m.inaccessible_page = false;
        m.title = "Fresh explicit retry".into();
    });
    let paused = Arc::new(PausedProvider {
        metadata: index.embed.clone(),
        started: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    let owned = service.clone();
    let embedding = paused.clone();
    let lease = index.lease.clone();
    let task = tokio::spawn(async move {
        owned
            .retry_page("retry-cancel", PAGE, lease, clock(), embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        paused.started.notified(),
    )
    .await
    .unwrap();
    let (fresh, replan) = index
        .coordinator
        .reconciliation_page_proposal("retry-cancel", PAGE)
        .unwrap();
    assert!(replan);
    assert_ne!(
        fresh.metadata_identity,
        index
            .coordinator
            .reconciliation_plan("retry-cancel")
            .unwrap()
            .pages[PAGE]
            .metadata_identity
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    index
        .coordinator
        .state()
        .release_lease(&index.lease, 10)
        .unwrap();
    let reopened =
        IndexCommitCoordinator::open(&index.index, &index.db, index.coordinator.binding().clone())
            .unwrap();
    let lease = reopened.state().acquire_lease(11, 100).unwrap();
    let guard = GuardedChunkTable::bind(
        &index.index,
        "chunks",
        index.embed.clone(),
        reopened.clone(),
    )
    .unwrap();
    let plan = notion_knowledge_retrieval::planner::AuthoritativeReconciliationPlanner::new(
        source.client(),
        guard.clone(),
        reopened.clone(),
        authority(),
    )
    .unwrap();
    let refresh = AuthoritativePageRefresh::new(
        source.client(),
        guard,
        reopened.clone(),
        authority(),
        &index.journal_scope,
        chunk_config(),
    )
    .unwrap();
    let resumed = notion_knowledge_retrieval::runner::AuthoritativeReconciliationRunner::new(
        plan,
        refresh,
        reopened.clone(),
        index.journal_scope.clone(),
    )
    .unwrap();
    assert_eq!(
        reopened
            .reconciliation_page_proposal("retry-cancel", PAGE)
            .unwrap(),
        (fresh, true)
    );
    let progress = resumed
        .apply("retry-cancel", lease, Arc::new(|| 11), &provider)
        .await
        .unwrap();
    assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
    assert!(
        index
            .guarded()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .all(|c| c.metadata.title == "Fresh explicit retry")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_owned_lock_wait_expires_without_releasing_another_live_owner() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "lock-wait", &plan);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let begun = started.clone();
    let held = release.clone();
    let maintenance =
        index
            .coordinator
            .maintain(index.lease.clone(), clock(), move |_| async move {
                begun.notify_one();
                held.notified().await;
                Ok(())
            });
    let maintenance = tokio::spawn(maintenance);
    started.notified().await;
    let provider = Arc::new(PausedProvider {
        metadata: index.embed.clone(),
        started: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    let now = Arc::new(std::sync::atomic::AtomicI64::new(10));
    let observed = now.clone();
    let timed_clock: Arc<dyn Fn() -> i64 + Send + Sync> =
        Arc::new(move || observed.load(Ordering::SeqCst));
    let owned = service.clone();
    let embedding = provider.clone();
    let lease = index.lease.clone();
    let timer = timed_clock.clone();
    let task = tokio::spawn(async move {
        owned
            .apply("lock-wait", lease, timer, embedding.as_ref())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        provider.started.notified(),
    )
    .await
    .unwrap();
    provider.proceed.notify_one();
    // Expiry is authoritative regardless of whether submit has reached lock wait.
    now.store(110, Ordering::SeqCst);
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(!maintenance.is_finished());
    assert_eq!(index.table().await.count_rows().await.unwrap(), 0);
    release.notify_one();
    maintenance.await.unwrap().unwrap();
    let lease = index.coordinator.state().acquire_lease(110, 100).unwrap();
    let good = CountingProvider::new(index.embed.clone());
    let progress = service
        .apply("lock-wait", lease, timed_clock, &good)
        .await
        .unwrap();
    assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_binding_change_during_owned_precommit_rejects_effect_and_preserves_resume()
{
    for change in [
        "UPDATE index_commit_binding SET epoch=2",
        "UPDATE index_commit_binding SET scope='new-policy'",
    ] {
        let index = Fixture::new();
        index.create().await;
        let source = HttpNotion::start().await;
        let service = runner(&index, &source);
        let provider = Arc::new(CountingProvider::new(index.embed.clone()));
        let plan = service.dry_run().await.unwrap();
        record_plan(&index, "owned-binding", &plan);
        *provider.boundary_pause.lock().unwrap() = Some(source.pause.clone());
        let owned = service.clone();
        let embedding = provider.clone();
        let lease = index.lease.clone();
        let task = tokio::spawn(async move {
            owned
                .apply("owned-binding", lease, clock(), embedding.as_ref())
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            source.pause.started.notified(),
        )
        .await
        .unwrap();
        let sql = rusqlite::Connection::open(&index.db).unwrap();
        sql.execute_batch(change).unwrap();
        source.pause.proceed.notify_one();
        assert!(task.await.unwrap().is_err());
        assert!(index.guarded().page_chunks(PAGE).await.unwrap().is_empty());
        sql.execute(
            "UPDATE index_commit_binding SET epoch=1,scope=?1",
            [index.journal_scope.identity()],
        )
        .unwrap();
        let progress = service
            .apply(
                "owned-binding",
                index.lease.clone(),
                clock(),
                provider.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!((progress.changed, progress.failed_pages()), (3, 0));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_mixed_inventory_counts_missing_stale_restored_orphan_and_unchanged_once() {
    use notion_knowledge_retrieval::planner::Difference;
    const RESTORED: &str = "66666666-6666-6666-6666-666666666666";
    const ORPHAN: &str = "77777777-7777-7777-7777-777777777777";
    const MISSING: &str = "88888888-8888-8888-8888-888888888888";
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    source.change(|m| {
        for (id, title) in [(RESTORED, "Restored"), (ORPHAN, "Orphan")] {
            m.extra.insert(
                id.into(),
                ExtraPage {
                    title: title.into(),
                    visible: true,
                    archived: false,
                },
            );
        }
    });
    let service = runner(&index, &source);
    let provider = CountingProvider::new(index.embed.clone());
    let seed = service
        .apply("mixed-seed", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(seed.changed, 5);
    source.change(|m| {
        let page = m.extra.get_mut(RESTORED).unwrap();
        page.archived = true;
        page.visible = false;
    });
    index
        .handler(source.client())
        .apply_lifecycle(
            &PageId(RESTORED.into()),
            "mixed-tombstone",
            &provider,
            index.lease.clone(),
            clock(),
        )
        .await
        .unwrap();
    source.change(|m| {
        let page = m.extra.get_mut(RESTORED).unwrap();
        page.archived = false;
        page.visible = true;
        let page = m.extra.get_mut(ORPHAN).unwrap();
        page.archived = true;
        page.visible = false;
        m.extra.insert(
            MISSING.into(),
            ExtraPage {
                title: "Missing".into(),
                visible: true,
                archived: false,
            },
        );
        m.title = "Stale exact metadata".into();
    });
    let dry = service.dry_run().await.unwrap();
    assert_eq!(
        (dry.scanned, dry.planned_refresh(), dry.planned_delete()),
        (6, 3, 1)
    );
    for (id, difference) in [
        (PAGE, Difference::Stale),
        (MISSING, Difference::Missing),
        (RESTORED, Difference::Restored),
        (ORPHAN, Difference::Orphan),
        (ROOT, Difference::Unchanged),
        (OTHER, Difference::Unchanged),
    ] {
        assert_eq!(
            dry.pages
                .iter()
                .find(|p| p.page_id == id)
                .unwrap()
                .difference,
            difference
        );
    }
    let progress = service
        .apply("mixed", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        (
            progress.scanned,
            progress.planned_refresh,
            progress.planned_delete,
            progress.changed,
            progress.deleted,
            progress.unchanged,
            progress.failed_pages(),
            progress.run_failures.len()
        ),
        (6, 3, 1, 3, 1, 2, 0, 0)
    );
    assert_eq!(progress.successful.len(), 6);
    assert!(progress.pending.is_empty());
    assert!(
        index
            .guarded()
            .page_chunks(ORPHAN)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        index
            .coordinator
            .state()
            .page_state(ORPHAN)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
    for id in [MISSING, RESTORED, PAGE] {
        assert!(!index.guarded().page_chunks(id).await.unwrap().is_empty());
        assert!(
            !index
                .coordinator
                .state()
                .page_state(id)
                .unwrap()
                .unwrap()
                .is_tombstone()
        );
    }
    let reads = source.body_reads.load(Ordering::SeqCst);
    let embeddings = provider.embeddings();
    assert_eq!(
        service
            .apply("mixed", index.lease.clone(), clock(), &provider)
            .await
            .unwrap(),
        progress
    );
    assert_eq!(source.body_reads.load(Ordering::SeqCst), reads);
    assert_eq!(provider.embeddings(), embeddings);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_reconciliation_is_durable_and_repeated_completion_preserves_original_time() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        None
    );
    service
        .apply("success", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        Some(10)
    );
    service.complete("success", &index.lease, 20).unwrap();
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        Some(10)
    );
    let reopened = IndexCommitCoordinator::open_read_only(
        &index.index,
        &index.db,
        index.coordinator.binding().clone(),
    )
    .unwrap();
    assert_eq!(reopened.last_successful_reconciliation().unwrap(), Some(10));
    assert!(
        IndexCommitCoordinator::open_read_only(
            &index.index,
            &index.db,
            CommitBinding::new("chunks", "foreign-workspace", &index.journal_scope, "1", 1)
                .unwrap()
        )
        .is_err()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_or_errorful_completed_reconciliation_never_claims_success() {
    for mode in ["incomplete", "planning-error", "run-error", "failed-page"] {
        let index = Fixture::new();
        index.create().await;
        let source = HttpNotion::start().await;
        let provider = CountingProvider::new(index.embed.clone());
        let service = runner(&index, &source);
        let mut plan = service.dry_run().await.unwrap();
        match mode {
            "incomplete" => plan.traversal_complete = false,
            "planning-error" => {
                plan.errors.insert(
                    "inaccessible-sentinel".into(),
                    notion_knowledge_retrieval::planner::PlanError::Permission,
                );
            }
            "run-error" => plan
                .run_errors
                .push(notion_knowledge_retrieval::planner::PlanError::Source),
            _ => {}
        }
        record_plan(&index, mode, &plan);
        if mode == "failed-page" {
            source.change(|m| m.inaccessible_page = true);
        }
        service
            .apply(mode, index.lease.clone(), clock(), &provider)
            .await
            .unwrap();
        service.complete(mode, &index.lease, 20).unwrap();
        assert_eq!(
            index.coordinator.last_successful_reconciliation().unwrap(),
            None,
            "{mode}"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicitly_recovered_page_failure_can_complete_successfully_with_historical_failure_retained()
 {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let plan = service.dry_run().await.unwrap();
    record_plan(&index, "recovered", &plan);
    source.change(|m| m.inaccessible_page = true);
    service
        .apply("recovered", index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        None
    );
    source.change(|m| m.inaccessible_page = false);
    service
        .retry_page("recovered", PAGE, index.lease.clone(), clock(), &provider)
        .await
        .unwrap();
    service.complete("recovered", &index.lease, 20).unwrap();
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        Some(10)
    );
    let raw = rusqlite::Connection::open(&index.db).unwrap();
    assert!(
        raw.query_row(
            "SELECT COUNT(*) FROM reconciliation_failure_history",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap()
            > 0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn success_receipt_failure_cannot_acknowledge_terminal_reconciliation() {
    let index = Fixture::new();
    index.create().await;
    let source = HttpNotion::start().await;
    let provider = CountingProvider::new(index.embed.clone());
    let service = runner(&index, &source);
    let raw = rusqlite::Connection::open(&index.db).unwrap();
    raw.execute_batch("CREATE TRIGGER reject_success BEFORE INSERT ON reconciliation_success BEGIN SELECT RAISE(ABORT,'receipt unavailable'); END;").unwrap();
    assert!(
        service
            .apply("atomic-success", index.lease.clone(), clock(), &provider)
            .await
            .is_err()
    );
    assert_ne!(
        index
            .coordinator
            .state()
            .run("atomic-success", &index.journal_scope)
            .unwrap()
            .phase,
        notion_knowledge_core::reconciliation::RunPhase::Completed
    );
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        None
    );
    raw.execute_batch("DROP TRIGGER reject_success").unwrap();
    service
        .complete("atomic-success", &index.lease, 20)
        .unwrap();
    assert_eq!(
        index.coordinator.last_successful_reconciliation().unwrap(),
        Some(20)
    );
}
