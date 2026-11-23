#![cfg(all(unix, feature = "webhook-worker"))]
//! Credential-free, real Notion HTTP -> real SQLite/LanceDB integration.
use crate::listener_shutdown;
use notion_knowledge_core::{
    backend::PageId,
    chunking::ChunkConfig,
    discovery::ExclusionRules,
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    lifecycle::LifecycleScope,
    reconciliation::{ReconciliationJournal, ReconciliationScope},
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{
    chunks::{GuardedChunkTable, LanceChunkTable},
    commit::{CommitBinding, IndexCommitCoordinator},
    refresh::AuthoritativePageRefresh,
};
use notion_knowledge_server::index_runtime::{IndexRuntime, RuntimeParts};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const OTHER: &str = "55555555-5555-5555-5555-555555555555";
const THIRD: &str = "77777777-7777-7777-7777-777777777777";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const FOREIGN: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
static NEXT: AtomicU64 = AtomicU64::new(0);

#[path = "support/shutdown_process.rs"]
mod shutdown_process;

#[derive(Clone)]
struct Model {
    markdown: String,
    title: String,
    revision: String,
    parent: String,
    archived: bool,
    root_revision: String,
    repair_bad: bool,
    /// PAGE answers 404: deleted or access revoked.
    gone: bool,
    unsupported: bool,
    joined: bool,
    linked: bool,
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
            repair_bad: false,
            gone: false,
            unsupported: false,
            joined: false,
            linked: true,
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
    pause_next: Arc<AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
    writes: Arc<AtomicUsize>,
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
        let pause_next = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        let pause = pause_next.clone();
        let paused = entered.clone();
        let resume = proceed.clone();
        let writes = Arc::new(AtomicUsize::new(0));
        let mutations = writes.clone();
        let server = tokio::spawn(async move {
            'connections: loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut part = [0u8; 2048];
                    let size = stream.read(&mut part).await.unwrap();
                    if size == 0 {
                        // A cancelled or idle client connection must not stop
                        // this authoritative fixture's entire HTTP listener.
                        continue 'connections;
                    }
                    bytes.extend_from_slice(&part[..size]);
                    if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                        break;
                    }
                }
                let end = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
                let length = String::from_utf8_lossy(&bytes[..end])
                    .lines()
                    .find_map(|line| {
                        line.to_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while bytes.len() < end + length {
                    let mut part = [0u8; 4096];
                    let n = stream.read(&mut part).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&part[..n]);
                }
                let payload = if length > 0 {
                    serde_json::from_slice::<Value>(&bytes[end..end + length]).unwrap()
                } else {
                    Value::Null
                };
                let req = String::from_utf8(bytes[..end].to_vec()).unwrap();
                assert!(req.contains("authorization: Bearer fixture-credential"));
                assert!(req.contains("notion-version: 2026-03-11"));
                assert!(
                    req.starts_with("GET /v1/")
                        || req.starts_with("POST /v1/search ")
                        || req.starts_with(&format!("PATCH /v1/pages/{PAGE}/markdown"))
                );
                counts.fetch_add(1, Ordering::SeqCst);
                let route = req
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                if req.starts_with("PATCH ") {
                    assert_eq!(payload["type"], "insert_content");
                    assert_eq!(payload["insert_content"]["position"]["type"], "end");
                    let mut model = values.lock().unwrap();
                    model
                        .markdown
                        .push_str(payload["insert_content"]["content"].as_str().unwrap());
                    model.revision = "2026-10-10T12:00:01Z".into();
                    mutations.fetch_add(1, Ordering::SeqCst);
                }
                let snapshot = values.lock().unwrap().clone();
                let mut response = if snapshot.gone
                    && route.starts_with(&format!("/v1/pages/{PAGE}"))
                {
                    json!({"object":"error", "code":"object_not_found"})
                } else if route == format!("/v1/pages/{ROOT}/markdown") {
                    // The walk reaches the children through their <page> tags.
                    let children: &[&str] = if snapshot.joined {
                        &[PAGE, OTHER, THIRD]
                    } else {
                        &[PAGE, OTHER]
                    };
                    let markdown = children
                        .iter()
                        .map(|id| {
                            format!(
                                "<page url=\"https://www.notion.so/{}\">Child</page>\n",
                                id.replace('-', "")
                            )
                        })
                        .collect::<String>();
                    json!({"object":"page_markdown","id":ROOT,"markdown":markdown,"truncated":false,"unknown_block_ids":[]})
                } else if route == format!("/v1/pages/{PAGE}/markdown") {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    json!({
                        "object": "page_markdown",
                        "id": PAGE,
                        "markdown": snapshot.markdown,
                        "truncated": snapshot.unsupported,
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
                        "markdown": if snapshot.joined { "Saffron fyr GRANNE. CSV electricity exports. Använd CSV för hushållets eldata." } else { "# Separate\n\nUntouched cinnamon.\n" }, "truncated":false,
                        "unknown_block_ids":[]})
                } else if route == format!("/v1/pages/{OTHER}") {
                    page_metadata(OTHER, ROOT, "Separate", "2026-10-10T11:00:00Z", false)
                } else if snapshot.joined && route == format!("/v1/pages/{THIRD}") {
                    page_metadata(THIRD, ROOT, "Control", "2026-10-10T11:00:00Z", false)
                } else if snapshot.joined && route == format!("/v1/pages/{THIRD}/markdown") {
                    json!({"object":"page_markdown", "id":THIRD,
                        "markdown":"Saffron fyr KONTROLL. Unrelated gardening decisions. Orelaterad odlingsplan.",
                        "truncated":false,"unknown_block_ids":[]})
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
                } else if snapshot.repair_bad
                    && route == "/v1/pages/66666666-6666-6666-6666-666666666666"
                {
                    page_metadata(
                        "66666666-6666-6666-6666-666666666666",
                        ROOT,
                        "Recovered",
                        &snapshot.revision,
                        false,
                    )
                } else if snapshot.repair_bad
                    && route == "/v1/pages/66666666-6666-6666-6666-666666666666/markdown"
                {
                    json!({"object":"page_markdown", "id":"66666666-6666-6666-6666-666666666666", "markdown":"Recovered marjoram.","truncated":false,"unknown_block_ids":[]})
                } else {
                    json!({"object":"error", "code":"object_not_found"})
                };
                if snapshot.joined && response["object"] == "page" {
                    if response["id"] == PAGE || response["id"] == OTHER {
                        response["properties"]["Alternative"] = json!({"id":"alias", "type":"rich_text",
                            "rich_text":[{"plain_text":"Fyren / Lighthouse"}]});
                    }
                    if response["id"] == PAGE {
                        response["properties"]["Related"] = json!({"id":"related", "type":"relation",
                            "relation":if snapshot.linked { json!([{"id":OTHER}]) } else {json!([])}, "has_more":false});
                    }
                }
                let bad = fail.swap(false, Ordering::SeqCst);
                let status = if bad {
                    503
                } else if response.get("code").is_some() && route.starts_with("/v1/pages/66666666")
                {
                    // The isolated bad page fails until repaired.
                    503
                } else if response.get("code").is_some() {
                    404
                } else {
                    200
                };
                let body = if bad { json!({}) } else { response }.to_string();
                if pause.swap(false, Ordering::SeqCst) {
                    paused.notify_one();
                    resume.notified().await;
                }
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
            pause_next,
            entered,
            proceed,
            writes,
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

use notion_knowledge_core::sync_state::SyncStateStore;
use notion_knowledge_core::webhook::{
    DebounceWindow, EventFailure, EventState, InboxScope, ProcessingOutcome, RetryPolicy,
    WebhookDebounce, WebhookEvent, WebhookInbox, WebhookRecovery,
};
use notion_knowledge_server::webhook_worker::runtime::{Worker, now_ms};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
const SUB: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const INTEGRATION: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
struct Provider {
    metadata: EmbeddingMetadata,
    count: Arc<AtomicUsize>,
}
impl EmbeddingProvider for Provider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.count.fetch_add(texts.len(), Ordering::SeqCst);
        Box::pin(async move {
            Ok(texts
                .iter()
                .map(|t| {
                    if t.contains("ANKARE") || t == "Saffron fyr" || t == "CSV electricity" {
                        vec![1., 0., 0.]
                    } else if t.contains("GRANNE") {
                        vec![0.96, 0.28, 0.]
                    } else if t.contains("KONTROLL") {
                        vec![0.98, 0.2, 0.]
                    } else {
                        vec![t.len() as f32, 1., 0.]
                    }
                })
                .collect())
        })
    }
}
struct TestRuntime {
    _dir: tempfile::TempDir,
    worker: Arc<Worker>,
    source: HttpNotion,
    index: std::path::PathBuf,
    db: std::path::PathBuf,
    embeddings: Arc<AtomicUsize>,
    scope: LifecycleScope,
}
impl TestRuntime {
    async fn new() -> Self {
        Self::with_scope_policy("webhook-v1").await
    }
    async fn with_scope_policy(policy: &str) -> Self {
        Self::with_scope_exclusions(policy, ExclusionRules::default()).await
    }
    async fn with_scope_exclusions(policy: &str, exclusions: ExclusionRules) -> Self {
        Self::with_joined_sources(policy, exclusions, false).await
    }
    async fn with_joined_sources(policy: &str, exclusions: ExclusionRules, joined: bool) -> Self {
        Self::with_provider(policy, exclusions, joined, None).await
    }

    async fn with_provider(
        policy: &str,
        exclusions: ExclusionRules,
        joined: bool,
        provider: Option<Arc<dyn EmbeddingProvider>>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = dir.path().join("index");
        std::fs::create_dir(&index).unwrap();
        let db = dir.path().join("state.sqlite");
        let scope = LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions,
        };
        let journal = if policy == "webhook-v1" {
            notion_knowledge_retrieval::planner::production_scope(&scope).unwrap()
        } else {
            ReconciliationScope::new(vec![ROOT.into()], vec![], policy, "1").unwrap()
        };
        let binding = CommitBinding::new("chunks", WORKSPACE, &journal, "1", 1).unwrap();
        let coordinator = IndexCommitCoordinator::initialize(&index, &db, binding).unwrap();
        if policy == "webhook-v1" {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let state = coordinator.state().clone();
        let embeddings = Arc::new(AtomicUsize::new(0));
        let provider = provider.unwrap_or_else(|| {
            Arc::new(Provider {
                metadata: EmbeddingMetadata::new("fixture".into(), "fixture".into(), "1".into(), 3)
                    .unwrap(),
                count: embeddings.clone(),
            })
        });
        let metadata = provider.metadata().clone();
        let guard =
            GuardedChunkTable::bind(&index, "chunks", metadata.clone(), coordinator.clone())
                .unwrap();
        let lease = state.acquire_lease(now_ms() / 1000, 60).unwrap();
        guard
            .create_empty(lease.clone(), Arc::new(|| now_ms() / 1000))
            .await
            .unwrap();
        state.release_lease(&lease, now_ms() / 1000).unwrap();
        let source = HttpNotion::start().await;
        if joined {
            source.change(|m| m.joined = true);
        }
        let runtime_index = index_runtime(
            source.client(),
            guard,
            coordinator.clone(),
            scope.clone(),
            journal,
            provider,
            joined.then(|| {
                notion_knowledge_core::aliases::AliasSources::new(true, vec!["alias".into()])
                    .unwrap()
            }),
        );
        seed_walk(&source, &state, &scope).await;
        let worker = Arc::new(Worker {
            webhook_enabled: true,
            scheduled: None,
            state,
            coordinator,
            index: runtime_index,
            scope: InboxScope {
                workspace_id: WORKSPACE.into(),
                subscription_id: SUB.into(),
            },
            integration: INTEGRATION.into(),
            poll: Duration::from_millis(20),
            timeout: Duration::from_secs(10),
            log_enabled: false,
        });
        Self {
            _dir: dir,
            worker,
            source,
            index,
            db,
            embeddings,
            scope,
        }
    }
    /// Run the next reconciliation walk: MCP reads authorize from the
    /// inventory it records, so a move takes effect for reads here.
    async fn rewalk(&self) {
        let walk = self
            .source
            .client()
            .walk_tree(&self.scope, None)
            .await
            .unwrap();
        self.worker
            .state
            .record_walk(&walk.run_id, &walk.entries, walk.authoritative())
            .unwrap();
    }
    async fn searchable(&self, text: &str) -> usize {
        use notion_knowledge_core::search::{LexicalQuery, LexicalSearch, SearchUnavailable};
        self.worker
            .index
            .chunks()
            .search_with_snapshot(false, |table| async move {
                LexicalSearch::search(
                    &table,
                    LexicalQuery {
                        query: text.into(),
                        limit: 10,
                        page_ids: None,
                        root_page_ids: None,
                        metadata: Default::default(),
                    },
                )
                .await
            })
            .await
            .map(|hits| hits.len())
            .unwrap_or_else(|_: SearchUnavailable| 0)
    }
}

/// The runtime the worker syncs each claimed page through.
fn index_runtime(
    source: Arc<NotionClient>,
    table: GuardedChunkTable,
    coordinator: Arc<IndexCommitCoordinator>,
    scope: LifecycleScope,
    journal: ReconciliationScope,
    provider: Arc<dyn EmbeddingProvider>,
    alias_sources: Option<notion_knowledge_core::aliases::AliasSources>,
) -> Arc<IndexRuntime> {
    Arc::new(
        IndexRuntime::from_parts(RuntimeParts {
            source,
            table,
            coordinator,
            scope,
            journal,
            provider,
            chunk_config: ChunkConfig {
                target_chars: 1000,
                overlap_chars: 0,
            },
            alias_sources,
            projection_mode: Default::default(),
            lexical: None,
        })
        .unwrap(),
    )
}

/// Record a walk of the fixture tree as the inventory, without indexing, so
/// claimed pages are placed by its parent map as in production. Request
/// counters start after it.
async fn seed_walk(
    source: &HttpNotion,
    state: &notion_knowledge_retrieval::sync_state::SqliteSyncStateStore,
    scope: &LifecycleScope,
) {
    let walk = source.client().walk_tree(scope, None).await.unwrap();
    state
        .record_walk(&walk.run_id, &walk.entries, walk.authoritative())
        .unwrap();
    source.attempts.store(0, Ordering::SeqCst);
    source.body_reads.store(0, Ordering::SeqCst);
}

fn owned_operation(runtime: &TestRuntime) -> String {
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .query_row("SELECT operation_id FROM index_commit_receipts", [], |r| {
            r.get(0)
        })
        .unwrap()
}

fn set_provider(worker: &mut Arc<Worker>, provider: Arc<dyn EmbeddingProvider>) {
    Arc::get_mut(&mut Arc::get_mut(worker).unwrap().index)
        .unwrap()
        .provider = provider;
}

struct UnavailableEmbedding(EmbeddingMetadata);
impl EmbeddingProvider for UnavailableEmbedding {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, _: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async { Err(notion_knowledge_core::embedding::EmbeddingError::Unavailable) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_refresh_preserves_verified_write_on_index_failure_and_retries_only_hash_diff() {
    use notion_knowledge_core::{
        append::{AppendRequest, append_once_and_refresh},
        write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey},
    };
    let mut runtime = TestRuntime::new().await;
    let state = runtime.worker.state.clone();
    let seed = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "0".repeat(64),
        page_id: PAGE.into(),
    };
    state.admit(&seed).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(state.status(&seed).unwrap(), Some(IndexStatus::Completed));
    let old_hash = state
        .page_state(PAGE)
        .unwrap()
        .unwrap()
        .content_hash()
        .unwrap()
        .to_owned();
    let client = runtime.source.client();
    let policy = LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    };
    let ledger = notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(
        runtime._dir.path().join("write-ledger.sqlite"),
    )
    .unwrap();
    let request = AppendRequest {
        page_id: PageId(PAGE.into()),
        markdown: "\n## Addition\n\nMarjoram and fennel.\n".into(),
        expected_last_edited_time: Some("2026-10-10T12:00:00Z".into()),
        expected_markdown_sha256: None,
        idempotency_key: "mutation-refresh-http-fixture-1234".into(),
    };
    let result = append_once_and_refresh(
        client.as_ref(),
        client.as_ref(),
        &policy,
        &ledger,
        state.as_ref(),
        request.clone(),
    )
    .await
    .unwrap();
    assert_eq!(result.index, IndexStatus::Queued);
    assert!(!result.replayed);
    assert_eq!(result.receipt.last_edited_time, "2026-10-10T12:00:01Z");
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 1);
    let provider = runtime.worker.index.provider.clone();
    set_provider(
        &mut runtime.worker,
        Arc::new(UnavailableEmbedding(provider.metadata().clone())),
    );
    tick(&runtime.worker, false).await;
    assert_eq!(
        state.status(&result.refresh).unwrap(),
        Some(IndexStatus::Failed)
    );
    assert!(
        runtime
            .source
            .state
            .lock()
            .unwrap()
            .markdown
            .contains("Marjoram")
    );
    let replay = append_once_and_refresh(
        client.as_ref(),
        client.as_ref(),
        &policy,
        &ledger,
        state.as_ref(),
        request,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.index, IndexStatus::Failed);
    assert_eq!(replay.receipt, result.receipt);
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 1);
    set_provider(&mut runtime.worker, provider);
    state.retry_mutation_refresh(&result.refresh).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        state.status(&result.refresh).unwrap(),
        Some(IndexStatus::Completed)
    );
    assert_ne!(
        state
            .page_state(PAGE)
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap(),
        old_hash
    );
    assert!(runtime.searchable("Marjoram").await > 0);
    let embedded = runtime.embeddings.load(Ordering::SeqCst);
    state
        .admit(&RefreshKey {
            mutation_digest: "f".repeat(64),
            ..seed
        })
        .unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        runtime.embeddings.load(Ordering::SeqCst),
        embedded,
        "unchanged authoritative content follows normal hash/diff reuse"
    );
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 1);
    let db = rusqlite::Connection::open(&runtime.db).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM webhook_inbox", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let reopened =
        notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open_existing(&runtime.db)
            .unwrap();
    assert_eq!(
        reopened.status(&result.refresh).unwrap(),
        Some(IndexStatus::Completed)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_refresh_recovers_committed_index_after_lost_queue_ack_without_embedding_again() {
    use notion_knowledge_core::write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey};
    let runtime = TestRuntime::new().await;
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "a".repeat(64),
        page_id: PAGE.into(),
    };
    runtime.worker.state.admit(&key).unwrap();
    let db = rusqlite::Connection::open(&runtime.db).unwrap();
    db.execute_batch("CREATE TRIGGER fail_mutation_ack BEFORE UPDATE OF state ON mutation_refresh WHEN NEW.state=2 BEGIN SELECT RAISE(ABORT,'fixture');END;").unwrap();
    assert!(
        runtime
            .worker
            .tick(CancellationToken::new(), false)
            .await
            .is_err()
    );
    assert_eq!(
        runtime.worker.state.status(&key).unwrap(),
        Some(IndexStatus::Running)
    );
    assert!(runtime.searchable("Saffron").await > 0);
    let embedded = runtime.embeddings.load(Ordering::SeqCst);
    db.execute_batch(
        "DROP TRIGGER fail_mutation_ack; UPDATE mutation_refresh SET lease_until=0 WHERE state=1;",
    )
    .unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        runtime.worker.state.status(&key).unwrap(),
        Some(IndexStatus::Completed)
    );
    assert_eq!(runtime.embeddings.load(Ordering::SeqCst), embedded);
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_refresh_cancellation_fences_the_old_attempt_and_recovers_after_expiry() {
    use notion_knowledge_core::write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey};
    let runtime = TestRuntime::new().await;
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "c".repeat(64),
        page_id: PAGE.into(),
    };
    runtime.worker.state.admit(&key).unwrap();
    runtime.source.pause_next.store(true, Ordering::SeqCst);
    let worker = runtime.worker.clone();
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    let task = tokio::spawn(async move { worker.tick(task_stop, false).await });
    tokio::time::timeout(Duration::from_secs(5), runtime.source.entered.notified())
        .await
        .unwrap();
    stop.cancel();
    assert!(task.await.unwrap().unwrap());
    runtime.source.proceed.notify_one();
    assert_eq!(
        runtime.worker.state.status(&key).unwrap(),
        Some(IndexStatus::Running)
    );
    assert!(runtime.worker.state.page_state(PAGE).unwrap().is_none());
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute_batch("UPDATE mutation_refresh SET lease_until=0 WHERE state=1;")
        .unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        runtime.worker.state.status(&key).unwrap(),
        Some(IndexStatus::Completed)
    );
    assert!(runtime.searchable("Saffron").await > 0);
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 0);
}
fn event(page: &str, kind: &str) -> WebhookEvent {
    WebhookEvent {
        id: format!(
            "cccccccc-cccc-cccc-cccc-{:012x}",
            NEXT.fetch_add(1, Ordering::SeqCst)
        ),
        timestamp: "2026-10-10T12:00:00Z".into(),
        workspace_id: WORKSPACE.into(),
        subscription_id: SUB.into(),
        integration_id: INTEGRATION.into(),
        event_type: kind.into(),
        entity_id: page.into(),
        entity_type: "page".into(),
        attempt_number: 1,
    }
}
async fn tick(worker: &Worker, page_first: bool) {
    worker
        .tick(CancellationToken::new(), page_first)
        .await
        .unwrap();
    // Fixtures that inspect completed vector results advance the separate
    // model loop after the source callback.
    let _ = worker.semantic_step().await;
}
fn inspect(worker: &Worker, event: &WebhookEvent) -> notion_knowledge_core::webhook::InboxEvent {
    worker.state.event(&event.key()).unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_bad_page_exhausts_while_healthy_page_becomes_searchable_and_operator_recovers() {
    let runtime = TestRuntime::new().await;
    let bad = event("66666666-6666-6666-6666-666666666666", "page.created");
    let good = event(PAGE, "page.created");
    runtime.worker.state.receive(&bad).unwrap();
    // Operator policy uses the same persisted recovery adapter as production.
    let (claim, _) = runtime
        .worker
        .state
        .claim(&runtime.worker.scope, now_ms() / 1000, 10)
        .unwrap()
        .unwrap();
    WebhookRecovery::complete(
        runtime.worker.state.as_ref(),
        &claim,
        now_ms() / 1000,
        ProcessingOutcome::Permanent(EventFailure::Source),
    )
    .unwrap();
    let failed = inspect(&runtime.worker, &bad);
    runtime
        .worker
        .state
        .requeue(
            &bad.key(),
            failed.generation,
            now_ms() / 1000,
            RetryPolicy {
                max_attempts: 2,
                base_seconds: 1,
                max_seconds: 1,
            },
        )
        .unwrap();
    runtime.worker.state.receive(&good).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(inspect(&runtime.worker, &bad).state, EventState::Pending);
    assert_eq!(inspect(&runtime.worker, &bad).cycle_attempts, 1);
    // Retry deadlines use whole seconds. The older bad event may already
    // be due again after its first source request, so FIFO may exhaust it
    // before selecting the healthy event. Either order must preserve progress.
    for _ in 0..2 {
        if inspect(&runtime.worker, &good).state == EventState::Succeeded {
            break;
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            runtime.worker.tick(CancellationToken::new(), false),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(inspect(&runtime.worker, &good).state, EventState::Succeeded);
    assert_eq!(inspect(&runtime.worker, &good).cycle_attempts, 1);
    assert_eq!(inspect(&runtime.worker, &good).lifetime_attempts, 1);
    assert!(runtime.searchable("Saffron").await > 0);
    if inspect(&runtime.worker, &bad).state != EventState::Failed {
        tokio::time::sleep(Duration::from_secs(1)).await;
        tick(&runtime.worker, false).await;
    }
    let failure = inspect(&runtime.worker, &bad);
    assert_eq!(failure.state, EventState::Failed);
    assert_eq!(failure.cycle_attempts, 2);
    assert_eq!(
        runtime
            .worker
            .state
            .failed(&runtime.worker.scope, 100)
            .unwrap()
            .len(),
        1
    );
    // Production local operator CLI retains counters and sanitizes output.
    let args = vec![
        "--webhook-inspect".into(),
        runtime.db.display().to_string(),
        WORKSPACE.into(),
        SUB.into(),
        bad.id.clone(),
    ];
    let report = notion_knowledge_server::webhook_recovery::run(&args)
        .unwrap()
        .to_string();
    assert!(report.contains("lifetime_attempts"));
    assert!(!report.contains("fixture-credential"));
    let args = vec![
        "--webhook-retry".into(),
        runtime.db.display().to_string(),
        WORKSPACE.into(),
        SUB.into(),
        bad.id.clone(),
        failure.generation.to_string(),
        "1".into(),
        "1".into(),
        "1".into(),
    ];
    notion_knowledge_server::webhook_recovery::run(&args).unwrap();
    assert_eq!(inspect(&runtime.worker, &bad).cycle_attempts, 0);
    assert_eq!(inspect(&runtime.worker, &bad).lifetime_attempts, 3);
    runtime.source.change(|m| m.repair_bad = true);
    tick(&runtime.worker, false).await;
    assert_eq!(inspect(&runtime.worker, &bad).state, EventState::Succeeded);
    assert!(runtime.searchable("marjoram").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failure_retries_and_unknown_hint_is_permanent() {
    let runtime = TestRuntime::new().await;
    let transient = event(PAGE, "page.created");
    let unknown = event(OTHER, "page.future_event");
    runtime.worker.state.receive(&transient).unwrap();
    runtime.worker.state.receive(&unknown).unwrap();
    runtime.source.fail_once.store(true, Ordering::SeqCst);
    tick(&runtime.worker, false).await;
    let pending = inspect(&runtime.worker, &transient);
    assert_eq!(pending.state, EventState::Pending);
    // The page retrieve failed: the source was unavailable, so it retries.
    assert_eq!(pending.last_failure, Some(EventFailure::Unavailable));
    tick(&runtime.worker, false).await;
    assert_eq!(inspect(&runtime.worker, &unknown).state, EventState::Failed);
    assert_eq!(inspect(&runtime.worker, &unknown).cycle_attempts, 1);
    // Delayed retries consume no claims while idle.
    tick(&runtime.worker, false).await;
    assert_eq!(inspect(&runtime.worker, &transient).cycle_attempts, 1);
    tokio::time::sleep(Duration::from_secs(5)).await;
    tick(&runtime.worker, false).await;
    assert_eq!(
        inspect(&runtime.worker, &transient).state,
        EventState::Succeeded
    );
    assert_eq!(inspect(&runtime.worker, &transient).cycle_attempts, 2);
    assert!(runtime.searchable("Saffron").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debounce_and_lifecycle_follow_authority_and_recover_effects_before_inbox_ack() {
    let runtime = TestRuntime::new().await;
    let a = event(PAGE, "page.content_updated");
    let b = event(PAGE, "page.properties_updated");
    for e in [&a, &b] {
        runtime
            .worker
            .state
            .receive_debounced(
                e,
                now_ms() - 100,
                DebounceWindow {
                    quiet_ms: 1,
                    max_delay_ms: 1,
                },
            )
            .unwrap();
    }
    let sql = rusqlite::Connection::open(&runtime.db).unwrap();
    // Crash boundary: index effects/checkpoint commit, inbox acknowledgement fails.
    sql.execute_batch("CREATE TRIGGER fail_ack BEFORE UPDATE OF state ON webhook_inbox WHEN NEW.state=2 BEGIN SELECT RAISE(ABORT,'fixture');END;").unwrap();
    assert!(
        runtime
            .worker
            .tick(CancellationToken::new(), true)
            .await
            .is_err()
    );
    assert!(runtime.searchable("Saffron").await > 0);
    assert_eq!(inspect(&runtime.worker, &a).state, EventState::Running);
    sql.execute_batch("DROP TRIGGER fail_ack; UPDATE webhook_inbox SET lease_until=0 WHERE state=1; UPDATE webhook_page_work SET lease_until_ms=0 WHERE lease_until_ms IS NOT NULL;").unwrap();
    let embedded_before_replay = runtime.embeddings.load(Ordering::SeqCst);
    // Reopen SQLite and index authority just as a restarted serving process does.
    let reopened = IndexCommitCoordinator::open(
        &runtime.index,
        &runtime.db,
        runtime.worker.coordinator.binding().clone(),
    )
    .unwrap();
    assert_eq!(
        reopened.state().event(&a.key()).unwrap().unwrap().state,
        EventState::Running
    );
    let guard = GuardedChunkTable::bind(
        &runtime.index,
        "chunks",
        runtime.worker.index.provider.metadata().clone(),
        reopened.clone(),
    )
    .unwrap();
    let scope = LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    };
    let journal = notion_knowledge_retrieval::planner::production_scope(&scope).unwrap();
    let restarted = Worker {
        webhook_enabled: true,
        scheduled: None,
        state: reopened.state().clone(),
        coordinator: reopened.clone(),
        index: index_runtime(
            runtime.source.client(),
            guard,
            reopened,
            scope,
            journal,
            runtime.worker.index.provider.clone(),
            None,
        ),
        scope: runtime.worker.scope.clone(),
        integration: INTEGRATION.into(),
        poll: Duration::from_millis(20),
        timeout: Duration::from_secs(10),
        log_enabled: false,
    };
    tick(&restarted, true).await;
    assert_eq!(inspect(&runtime.worker, &a).state, EventState::Succeeded);
    assert_eq!(inspect(&runtime.worker, &b).state, EventState::Succeeded);
    assert_eq!(
        runtime.embeddings.load(Ordering::SeqCst),
        embedded_before_replay
    );
    assert_eq!(inspect(&runtime.worker, &a).cycle_attempts, 2);
    runtime.source.change(|m| m.archived = true);
    let deleted = event(PAGE, "page.deleted");
    runtime.worker.state.receive(&deleted).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(runtime.searchable("Saffron").await, 0);
    runtime.source.change(|m| m.archived = false);
    let restored = event(PAGE, "page.undeleted");
    runtime.worker.state.receive(&restored).unwrap();
    tick(&runtime.worker, false).await;
    assert!(runtime.searchable("Saffron").await > 0);
    // Moved under a page the inventory does not hold: unplaced, so it stays
    // until the next walk settles it, and the event still succeeds.
    runtime.source.change(|m| m.parent = FOREIGN.into());
    let moved = event(PAGE, "page.moved");
    runtime.worker.state.receive(&moved).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        inspect(&runtime.worker, &moved).state,
        EventState::Succeeded
    );
    assert_eq!(unplaced(&runtime), 1);
    assert!(runtime.searchable("Saffron").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_answering_not_found_to_its_callback_is_removed_without_a_walk() {
    let runtime = TestRuntime::new().await;
    let created = event(PAGE, "page.created");
    runtime.worker.state.receive(&created).unwrap();
    tick(&runtime.worker, false).await;
    assert!(runtime.searchable("Saffron").await > 0);
    runtime.source.change(|m| m.gone = true);
    let deleted = event(PAGE, "page.deleted");
    runtime.worker.state.receive(&deleted).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(
        inspect(&runtime.worker, &deleted).state,
        EventState::Succeeded
    );
    assert_eq!(runtime.searchable("Saffron").await, 0);
}

fn unplaced(runtime: &TestRuntime) -> u64 {
    runtime
        .worker
        .state
        .last_sync()
        .unwrap()
        .unwrap()
        .drift
        .unplaced
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serving_lifecycle_processes_signed_http_admission_and_stops_idle_and_delayed_work() {
    use hmac::{Hmac, Mac};

    use sha2::Sha256;
    let runtime = TestRuntime::new().await;
    let values = std::collections::BTreeMap::from([
        ("NK_WEBHOOK_MODE", "verified"),
        ("NK_WEBHOOK_VERIFICATION_TOKEN", "fixture-signing"),
        ("NK_WEBHOOK_WORKSPACE_ID", WORKSPACE),
        ("NK_WEBHOOK_INTEGRATION_ID", INTEGRATION),
        ("NK_WEBHOOK_SUBSCRIPTION_ID", SUB),
        ("NK_WEBHOOK_STATE_FILE", "/tmp/fixture.sqlite"),
    ]);
    let mut config = notion_knowledge_server::config::Config::from_lookup(|name| {
        values.get(name).map(std::ffi::OsString::from)
    })
    .unwrap();
    config.webhook_state_file = Some(runtime.db.clone());
    config.webhook_debounce = DebounceWindow {
        quiet_ms: 1,
        max_delay_ms: 1,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    config.http_bind = address;
    let stop = CancellationToken::new();
    let token = stop.clone();
    let worker = runtime.worker.clone();
    let serving = tokio::spawn(async move {
        notion_knowledge_server::http::serve_composed(
            config,
            notion_knowledge_mcp::KnowledgeServer::default(),
            Some(worker),
            token,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if reqwest::get(format!("http://{address}/livez"))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let admitted = event(PAGE, "page.content_updated");
    let body=json!({"id":admitted.id,"timestamp":admitted.timestamp,"workspace_id":WORKSPACE,"subscription_id":SUB,"integration_id":INTEGRATION,"type":admitted.event_type,"entity":{"id":PAGE,"type":"page"},"attempt_number":1}).to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(b"fixture-signing").unwrap();
    mac.update(body.as_bytes());
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/webhooks/notion"))
        .header("X-Notion-Signature", format!("sha256={signature}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if inspect(&runtime.worker, &admitted).state == EventState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(runtime.searchable("Saffron").await > 0);
    assert!(runtime.source.attempts.load(Ordering::SeqCst) > 0);
    assert!(runtime.source.body_reads.load(Ordering::SeqCst) > 0);
    // Hold the shared writer fence while creating delayed work so the running
    // serving worker cannot race the fixture's explicit claim/completion.
    let fixture_lease = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(lease) = runtime.worker.state.acquire_lease(now_ms() / 1000, 60) {
                break lease;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // A future retry remains durable across graceful stop; no idle spin.
    let delayed = event(OTHER, "page.created");
    runtime.worker.state.receive(&delayed).unwrap();
    let (claim, _) = runtime
        .worker
        .state
        .claim(&runtime.worker.scope, now_ms() / 1000, 10)
        .unwrap()
        .unwrap();
    WebhookRecovery::complete(
        runtime.worker.state.as_ref(),
        &claim,
        now_ms() / 1000,
        ProcessingOutcome::Retryable(EventFailure::Source),
    )
    .unwrap();
    runtime
        .worker
        .state
        .release_lease(&fixture_lease, now_ms() / 1000)
        .unwrap();
    let pending = inspect(&runtime.worker, &delayed);
    assert!(pending.retry_at.unwrap() > now_ms() / 1000);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        inspect(&runtime.worker, &delayed).cycle_attempts,
        pending.cycle_attempts
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        runtime
            .worker
            .state
            .event(&delayed.key())
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_refresh_expired_ownership_during_source_read_cannot_commit_under_live_writer_lease()
 {
    use notion_knowledge_core::write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey};
    let runtime = TestRuntime::new().await;
    let key = RefreshKey {
        workspace_id: WORKSPACE.into(),
        mutation_digest: "d".repeat(64),
        page_id: PAGE.into(),
    };
    runtime.worker.state.admit(&key).unwrap();
    runtime.source.pause_next.store(true, Ordering::SeqCst);
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(5), runtime.source.entered.notified())
        .await
        .unwrap();
    // Keep the shared writer lease live while expiring ONLY mutation ownership.
    // The final guarded commit must consult the mutation claim's deadline.
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute_batch("UPDATE mutation_refresh SET lease_until=0 WHERE state=1;")
        .unwrap();
    runtime.source.proceed.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(runtime.searchable("Saffron").await, 0);
    assert!(runtime.worker.state.page_state(PAGE).unwrap().is_none());
    tick(&runtime.worker, false).await;
    assert_eq!(
        runtime.worker.state.status(&key).unwrap(),
        Some(IndexStatus::Completed)
    );
    assert!(runtime.searchable("Saffron").await > 0);
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_index_fence_and_expired_inbox_ownership_reject_stale_commit() {
    let runtime = TestRuntime::new().await;
    let e = event(PAGE, "page.created");
    runtime.worker.state.receive(&e).unwrap();
    let competing = runtime
        .worker
        .state
        .acquire_lease(now_ms() / 1000, 60)
        .unwrap();
    assert!(
        !runtime
            .worker
            .tick(CancellationToken::new(), false)
            .await
            .unwrap()
    );
    assert_eq!(inspect(&runtime.worker, &e).cycle_attempts, 0);
    runtime
        .worker
        .state
        .release_lease(&competing, now_ms() / 1000)
        .unwrap();
    runtime.source.pause_next.store(true, Ordering::SeqCst);
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(5), runtime.source.entered.notified())
        .await
        .unwrap();
    // Ownership expires during preparation; final commit must consult the
    // actual inbox lease, rather than the separately acquired writer fence.
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute_batch("UPDATE webhook_inbox SET lease_until=0 WHERE state=1;")
        .unwrap();
    runtime.source.proceed.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(runtime.searchable("Saffron").await, 0);
    let receipt = runtime.worker.coordinator.state().page_state(PAGE).unwrap();
    assert!(receipt.is_none());
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(!runtime.worker.tick(stop, false).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_lease_takeover_rejects_stale_effects_without_fatal_cleanup_or_releasing_new_owner()
{
    let runtime = TestRuntime::new().await;
    let stale = event(PAGE, "page.created");
    runtime.worker.state.receive(&stale).unwrap();
    runtime.source.pause_next.store(true, Ordering::SeqCst);
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(5), runtime.source.entered.notified())
        .await
        .unwrap();
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute_batch("UPDATE reconciliation_lease SET expires_at=0;")
        .unwrap();
    let new_owner = runtime
        .worker
        .state
        .acquire_lease(now_ms() / 1000, 60)
        .unwrap();
    runtime.source.proceed.notify_one();
    assert!(task.await.unwrap().unwrap());
    assert_eq!(runtime.searchable("Saffron").await, 0);
    assert_eq!(inspect(&runtime.worker, &stale).state, EventState::Pending);
    let renewed = runtime
        .worker
        .state
        .renew_lease(&new_owner, now_ms() / 1000, 60)
        .unwrap();
    runtime
        .worker
        .state
        .release_lease(&renewed, now_ms() / 1000)
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_worker_storage_failure_stops_the_actual_http_serving_lifecycle() {
    let runtime = TestRuntime::new().await;
    let mut config = notion_knowledge_server::config::Config::from_lookup(|_| None).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    config.http_bind = address;
    let stop = CancellationToken::new();
    let worker = runtime.worker.clone();
    let serving = tokio::spawn(async move {
        notion_knowledge_server::http::serve_composed(
            config,
            notion_knowledge_mcp::KnowledgeServer::default(),
            Some(worker),
            stop,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if reqwest::get(format!("http://{address}/livez"))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    rusqlite::Connection::open(&runtime.db).unwrap().execute_batch("CREATE TRIGGER fail_writer BEFORE UPDATE ON reconciliation_lease BEGIN SELECT RAISE(ABORT,'private fixture exception');END;").unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(!result.unwrap_err().to_string().contains("private"));
    assert!(
        reqwest::get(format!("http://{address}/livez"))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_content_fails_permanently_and_out_of_scope_page_is_unplaced() {
    let runtime = TestRuntime::new().await;
    runtime.source.change(|m| m.unsupported = true);
    let unsupported = event(PAGE, "page.created");
    let outside = event(FOREIGN, "page.created");
    for e in [&unsupported, &outside] {
        runtime.worker.state.receive(e).unwrap();
    }
    tick(&runtime.worker, false).await;
    tick(&runtime.worker, false).await;
    // Both settle in one attempt: no retry can read the unsupported page,
    // and the page outside the root cannot be placed by the inventory.
    let observed = inspect(&runtime.worker, &unsupported);
    assert_eq!(observed.state, EventState::Failed);
    assert_eq!(observed.cycle_attempts, 1);
    assert_eq!(observed.failure, Some(EventFailure::Source));
    let observed = inspect(&runtime.worker, &outside);
    assert_eq!(observed.state, EventState::Succeeded);
    assert_eq!(observed.cycle_attempts, 1);
    assert_eq!(unplaced(&runtime), 1);
    assert_eq!(runtime.searchable("Saffron").await, 0);
}

fn retrieval_config(runtime: &TestRuntime) -> notion_knowledge_server::config::Config {
    retrieval_config_with(runtime, |_| None)
}
fn retrieval_config_with(
    runtime: &TestRuntime,
    mut extra: impl FnMut(&str) -> Option<std::ffi::OsString>,
) -> notion_knowledge_server::config::Config {
    let values = std::collections::BTreeMap::from([
        ("NK_RETRIEVAL", "true"),
        ("NK_NOTION_AUTH", "integration"),
        ("NOTION_TOKEN", "fixture-only"),
        ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
        ("NK_NOTION_SCOPE_ROOTS", ROOT),
        ("NK_WEBHOOK_STATE_FILE", runtime.db.to_str().unwrap()),
        ("NK_WEBHOOK_INDEX_DIR", runtime.index.to_str().unwrap()),
        ("NK_WEBHOOK_QWEN_ASSETS", "/tmp/unused-typed-provider"),
        ("NK_HTTP_AUTH", "bearer"),
        (
            "NK_HTTP_BEARER_TOKEN",
            "fixture-private-authentication-token",
        ),
    ]);
    notion_knowledge_server::config::Config::from_lookup(|name| {
        extra(name).or_else(|| values.get(name).map(std::ffi::OsString::from))
    })
    .unwrap()
}
fn graph_config(runtime: &TestRuntime) -> notion_knowledge_server::config::Config {
    let mut config = retrieval_config(runtime);
    config.retrieval = None; // graph reads do not require loading an embedding model
    config.graph_index = Some(runtime.index.clone());
    config
}
fn related_request(id: u64, arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"knowledge_related","arguments":arguments}})
}
async fn commit_graph_fixture(runtime: &TestRuntime) {
    runtime.source.state.lock().unwrap().markdown = format!(
        "Saffron ravioli.\n\n[other](https://www.notion.so/{OTHER}) [duplicate](https://www.notion.so/{OTHER}) [denied](https://www.notion.so/{FOREIGN})"
    );
    commit_search_fixture(runtime).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_graph_reopens_producer_commits_and_tracks_edits_tombstones_and_scope() {
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_graph_fixture(&runtime).await;
    let config = graph_config(&runtime);
    let before = std::fs::read(&runtime.db).unwrap();
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    assert_eq!(before, std::fs::read(&runtime.db).unwrap());
    let output = wire_stdio(
        handler.clone(),
        vec![
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            related_request(3, json!({"page_id":PAGE})),
            related_request(4, json!({"page_id":OTHER,"depth":2})),
            related_request(5, json!({"page_id":PAGE,"depth":3})),
            related_request(6, json!({"page_id":PAGE,"limit":0})),
            related_request(7, json!({"page_id":PAGE,"workspace_id":WORKSPACE})),
        ],
    )
    .await;
    assert!(
        output[1]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "knowledge_related")
    );
    let edges = output[2]["result"]["structuredContent"]["relations"]
        .as_array()
        .unwrap();
    assert_eq!(edges.len(), 1, "{:?}", output);
    assert_eq!(edges[0]["source_page_id"], PAGE);
    assert_eq!(edges[0]["target_page_id"], OTHER);
    assert_eq!(edges[0]["direction"], "outgoing");
    assert!(
        edges[0]["relation_type"]
            .as_str()
            .unwrap()
            .starts_with("link:")
    );
    assert!(edges[0]["provenance"].as_str().is_some());
    assert_eq!(
        output[3]["result"]["structuredContent"]["relations"][0]["direction"],
        "incoming"
    );
    for item in &output[4..] {
        assert_eq!(item["error"]["code"], -32602, "{item}");
    }
    let mut excluded = graph_config(&runtime);
    excluded
        .notion_scope
        .as_mut()
        .unwrap()
        .exclusions
        .page_ids
        .insert(OTHER.into());
    assert!(
        installed_retrieval(
            &runtime,
            &excluded,
            notion_knowledge_server::root_scope::Transport::Stdio
        )
        .await
        .is_err()
    );
    {
        let mut model = runtime.source.state.lock().unwrap();
        model.markdown = "No links after edit.".into();
        model.revision = "2026-10-10T13:00:00Z".into();
    }
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.content_updated"))
        .unwrap();
    tick(&runtime.worker, false).await;
    let output = wire_stdio(
        handler.clone(),
        vec![related_request(2, json!({"page_id":OTHER}))],
    )
    .await;
    assert_eq!(
        output[1]["result"]["structuredContent"]["relations"],
        json!([])
    );
    runtime.source.state.lock().unwrap().archived = true;
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.deleted"))
        .unwrap();
    tick(&runtime.worker, false).await;
    let output = wire_stdio(handler, vec![related_request(2, json!({"page_id":PAGE}))]).await;
    assert_eq!(output[1]["result"]["isError"], true);
}
async fn installed_retrieval(
    runtime: &TestRuntime,
    config: &notion_knowledge_server::config::Config,
    transport: notion_knowledge_server::root_scope::Transport,
) -> Result<notion_knowledge_mcp::KnowledgeServer, &'static str> {
    notion_knowledge_server::root_scope::compose_client(
        notion_knowledge_mcp::KnowledgeServer::default(),
        config,
        transport,
        runtime.source.client(),
        runtime.worker.index.provider.clone(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_lexical_expansion_preserves_scoped_content_and_rebuilds_after_cache_loss() {
    use notion_knowledge_retrieval::lexical_dictionary::test_fixtures::dictionary;
    use notion_knowledge_server::root_scope::Transport;
    let runtime = TestRuntime::new().await;
    {
        let mut source = runtime.source.state.lock().unwrap();
        source.title = "Saffran".into();
        source.markdown = "Saffran ravioli. Råd om kryddor.".into();
    }
    let dictionary = Arc::new(dictionary(&[("saffron", "saffran")]));
    let digest = dictionary.manifest_digest().to_owned();
    let mut config = retrieval_config_with(&runtime, |name| match name {
        "NK_LEXICAL_EXPANSION" => Some("true".into()),
        "NK_LEXICAL_ASSET_BUNDLE" => Some("/tmp/unused-typed-dictionary".into()),
        "NK_LEXICAL_ASSET_SHA256" => Some(digest.clone().into()),
        _ => None,
    });
    let settings = config.retrieval.as_mut().unwrap();
    settings.lexical = Some(
        settings
            .lexical
            .take()
            .unwrap()
            .with_verified_dictionary(dictionary)
            .unwrap(),
    );
    let handler = installed_retrieval(&runtime, &config, Transport::Stdio)
        .await
        .unwrap();
    let mut requests = vec![
        search_request(2, "lexical", json!({"page_ids":[PAGE]})),
        search_request(3, "hybrid", json!({"page_ids":[PAGE]})),
    ];
    for (id, query) in [
        (4, "saffran"),
        (5, PAGE),
        (6, "\"Saffran ravioli\""),
        (7, "unmatchedxyz"),
    ] {
        let mut request = search_request(id, "lexical", json!({"page_ids":[PAGE]}));
        request["params"]["arguments"]["query"] = json!(query);
        requests.push(request);
    }
    requests.push(search_request(
        8,
        "lexical",
        json!({"metadata":{"workspace_ids":[FOREIGN]}}),
    ));
    requests.push(search_request(
        9,
        "lexical",
        json!({"root_page_ids":[FOREIGN]}),
    ));
    let query_embeddings = runtime.embeddings.load(Ordering::SeqCst);
    let responses = wire_stdio(handler, requests).await;
    assert_eq!(
        runtime.embeddings.load(Ordering::SeqCst),
        query_embeddings + 1,
        "only the requested hybrid semantic branch may embed a query"
    );
    for response in &responses[1..=5] {
        let hits = response["result"]["structuredContent"]["results"]
            .as_array()
            .unwrap_or_else(|| panic!("{response}"));
        assert!(!hits.is_empty());
        assert!(hits.iter().all(|hit| hit["source"]["page_id"] == PAGE));
        assert!(hits[0]["text"].as_str().unwrap().contains("Saffran"));
    }
    for response in &responses[1..=2] {
        let records = response["result"]["structuredContent"]["results"][0]["lexical_matches"]
            .as_array()
            .unwrap();
        assert!(
            records
                .iter()
                .any(|record| record["kind"] == "lexical_dictionary_v1"
                    && record["manifest_digest"] == digest)
        );
    }
    for response in &responses[4..=5] {
        assert!(
            !response["result"]["structuredContent"]
                .to_string()
                .contains("lexical_dictionary_v1")
        );
    }
    for response in &responses[6..] {
        assert_eq!(
            response["result"]["structuredContent"]["results"],
            json!([])
        );
    }
    {
        let mut source = runtime.source.state.lock().unwrap();
        source.markdown.push_str(" Extra mejram.");
        source.revision = "2026-10-10T12:00:01Z".into();
    }
    let command = notion_knowledge_server::reconciliation_command::Command::parse(
        &["--reconcile", "apply"].map(str::to_owned),
    )
    .unwrap();
    let updated = notion_knowledge_server::reconciliation_command::runtime::execute(
        command,
        config.retrieval.as_ref().unwrap(),
        &runtime.db,
        config.notion_scope.clone().unwrap(),
        runtime.source.client(),
        runtime.worker.index.provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(updated.exit, 0, "{}", updated.json);
    let reads = runtime.source.attempts.load(Ordering::SeqCst);
    let embeddings = runtime.embeddings.load(Ordering::SeqCst);
    let healthy = installed_retrieval(&runtime, &config, Transport::Stdio)
        .await
        .unwrap();
    assert_eq!(runtime.source.attempts.load(Ordering::SeqCst), reads);
    assert_eq!(runtime.embeddings.load(Ordering::SeqCst), embeddings);
    let refreshed = wire_stdio(
        healthy,
        vec![search_request(2, "lexical", json!({"page_ids":[PAGE]}))],
    )
    .await;
    assert!(
        refreshed[1]["result"]["structuredContent"]
            .to_string()
            .contains("mejram")
    );
    let corrupt = runtime._dir.path().join("corrupt-lexical.bundle");
    std::fs::write(&corrupt, b"invalid bundle").unwrap();
    let bad = retrieval_config_with(&runtime, |name| match name {
        "NK_LEXICAL_EXPANSION" => Some("true".into()),
        "NK_LEXICAL_ASSET_BUNDLE" => Some(corrupt.as_os_str().to_owned()),
        "NK_LEXICAL_ASSET_SHA256" => Some(digest.clone().into()),
        _ => None,
    });
    let state_before = std::fs::read(&runtime.db).unwrap();
    let cache_before = storage_files(&runtime.index);
    let reads = runtime.source.attempts.load(Ordering::SeqCst);
    assert!(
        installed_retrieval(&runtime, &bad, Transport::Stdio)
            .await
            .is_err()
    );
    assert_eq!(runtime.source.attempts.load(Ordering::SeqCst), reads);
    assert_eq!(std::fs::read(&runtime.db).unwrap(), state_before);
    assert_eq!(storage_files(&runtime.index), cache_before);
    std::fs::remove_dir_all(&runtime.index).unwrap();
    let rebuilt = installed_retrieval(&runtime, &config, Transport::Stdio)
        .await
        .unwrap();
    let response = wire_stdio(
        rebuilt.clone(),
        vec![search_request(2, "lexical", json!({"page_ids":[PAGE]}))],
    )
    .await;
    assert!(
        response[1]["result"]["structuredContent"]
            .to_string()
            .contains("lexical_dictionary_v1")
    );
    runtime.source.state.lock().unwrap().parent = FOREIGN.into();
    runtime.rewalk().await;
    let denied = wire_stdio(
        rebuilt,
        vec![search_request(2, "lexical", json!({"page_ids":[PAGE]}))],
    )
    .await;
    assert_eq!(denied[1]["result"]["isError"], true);
    assert!(denied[1]["result"]["structuredContent"].is_null());
    assert_eq!(runtime.source.writes.load(Ordering::SeqCst), 0);
}
async fn commit_search_fixture(runtime: &TestRuntime) {
    for page in [PAGE, OTHER] {
        runtime
            .worker
            .state
            .receive(&event(page, "page.created"))
            .unwrap();
        tick(&runtime.worker, false).await;
    }
    assert!(runtime.searchable("Saffron").await > 0);
    let guard = GuardedChunkTable::bind(
        &runtime.index,
        "chunks",
        runtime.worker.index.provider.metadata().clone(),
        runtime.worker.coordinator.clone(),
    )
    .unwrap();
    assert!(
        guard.search_snapshot().await.is_ok(),
        "the production worker prepares serving indexes"
    );
}
async fn wire_stdio(
    handler: notion_knowledge_mcp::KnowledgeServer,
    requests: Vec<Value>,
) -> Vec<Value> {
    use rmcp::ServiceExt;
    use tokio::io::{AsyncBufReadExt, BufReader};
    let (server, client) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        handler
            .serve(server)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap()
    });
    let (read, mut write) = tokio::io::split(client);
    let mut reader = BufReader::new(read);
    let mut responses = vec![];
    for (i, request) in std::iter::once(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"joined-retrieval","version":"1"}}})).chain(requests).enumerate() {
        write.write_all(format!("{request}\n").as_bytes()).await.unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line)).await.unwrap().unwrap();
        responses.push(serde_json::from_str(&line).unwrap());
        if i == 0 { write.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n").await.unwrap(); }
    }
    drop(write);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    responses
}
fn search_request(id: u64, mode: &str, filters: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"knowledge_search","arguments":{"query":"Saffron","limit":5,"mode":mode,"filters":filters}}})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopened_committed_index_serves_all_modes_over_stdio_and_revalidates_physical_scope() {
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_search_fixture(&runtime).await;
    let config = retrieval_config(&runtime);
    let before = std::fs::read(&runtime.db).unwrap();
    let version = GuardedChunkTable::bind(
        &runtime.index,
        "chunks",
        runtime.worker.index.provider.metadata().clone(),
        runtime.worker.coordinator.clone(),
    )
    .unwrap()
    .inventory_version()
    .await
    .unwrap();
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    assert_eq!(before, std::fs::read(&runtime.db).unwrap());
    assert_eq!(
        version,
        GuardedChunkTable::bind(
            &runtime.index,
            "chunks",
            runtime.worker.index.provider.metadata().clone(),
            runtime.worker.coordinator.clone()
        )
        .unwrap()
        .inventory_version()
        .await
        .unwrap()
    );
    let output = wire_stdio(
        handler.clone(),
        vec![
            search_request(2, "semantic", json!({"page_ids":[PAGE]})),
            search_request(3, "lexical", json!({"page_ids":[PAGE]})),
            search_request(4, "hybrid", json!({"page_ids":[PAGE]})),
            search_request(5, "hybrid", json!({"root_page_ids":[FOREIGN]})),
            search_request(6, "lexical", json!({"page_ids":[OTHER]})),
            search_request(7, "hybrid", json!({"metadata":{"workspace_ids":[FOREIGN]}})),
        ],
    )
    .await;
    for (index, path) in [(1, "semantic"), (2, "lexical"), (3, "semantic")] {
        let results = output[index]["result"]["structuredContent"]["results"]
            .as_array()
            .unwrap_or_else(|| panic!("{}", output[index]));
        assert!(!results.is_empty(), "{}", output[index]);
        assert!(results.iter().all(|hit| hit["source"]["page_id"] == PAGE));
        assert!(results[0]["score"].as_f64().unwrap().is_finite());
        assert!(
            results[0]["matched_paths"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p == path)
        );
    }
    assert_eq!(
        output[4]["result"]["structuredContent"]["results"],
        json!([])
    );
    assert_eq!(
        output[5]["result"]["structuredContent"]["results"],
        json!([])
    );
    assert_eq!(
        output[6]["result"]["structuredContent"]["results"],
        json!([])
    );
    runtime.source.change(|m| {
        m.markdown = "# Intro\n\nTurmeric ravioli.\n".into();
        m.revision = "2026-10-10T13:00:00Z".into();
    });
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.content_updated"))
        .unwrap();
    tick(&runtime.worker, false).await;
    let mut updated = search_request(3, "hybrid", json!({"page_ids":[PAGE]}));
    updated["params"]["arguments"]["query"] = json!("Turmeric");
    let output = wire_stdio(
        handler.clone(),
        vec![
            search_request(2, "lexical", json!({"page_ids":[PAGE]})),
            updated,
        ],
    )
    .await;
    assert_eq!(
        output[1]["result"]["structuredContent"]["results"],
        json!([])
    );
    assert!(
        output[2].to_string().contains("Turmeric ravioli"),
        "{}",
        output[2]
    );
    runtime.source.change(|m| m.parent = FOREIGN.into());
    runtime.rewalk().await;
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    let output = wire_stdio(handler, vec![search_request(2, "hybrid", json!({}))]).await;
    assert!(
        !output[1].to_string().contains("Turmeric ravioli"),
        "{}",
        output[1]
    );
    runtime.source.change(|m| m.parent = ROOT.into());
    runtime.rewalk().await;
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    let connection = rusqlite::Connection::open(&runtime.db).unwrap();
    connection
        .execute(
            "UPDATE index_commit_receipts SET status=0 WHERE status=1",
            [],
        )
        .unwrap();
    let output = wire_stdio(
        handler.clone(),
        vec![search_request(2, "hybrid", json!({}))],
    )
    .await;
    assert!(output[1].to_string().contains("retrieval_unavailable"));
    assert!(!output[1].to_string().contains("Turmeric ravioli"));
    connection
        .execute(
            "UPDATE index_commit_receipts SET status=1 WHERE status=0",
            [],
        )
        .unwrap();
    connection
        .execute("UPDATE index_commit_binding SET epoch=epoch+1", [])
        .unwrap();
    let output = wire_stdio(handler, vec![search_request(2, "hybrid", json!({}))]).await;
    assert!(output[1].to_string().contains("retrieval_unavailable"));
    assert!(!output[1].to_string().contains("Turmeric ravioli"));
}

async fn http_json(response: reqwest::Response) -> Value {
    let text = response.text().await.unwrap();
    if let Ok(value) = serde_json::from_str(&text) {
        return value;
    }
    serde_json::from_str(
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or_else(|| panic!("unexpected HTTP response: {text}")),
    )
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_http_serves_committed_hybrid_and_graph_and_denies_untrusted_session_requests()
 {
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_graph_fixture(&runtime).await;
    let mut config = retrieval_config(&runtime);
    config.graph_index = Some(runtime.index.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.http_bind = listener.local_addr().unwrap();
    let address = config.http_bind;
    drop(listener);
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Http,
    )
    .await
    .unwrap();
    let stop = CancellationToken::new();
    let cancelled = stop.clone();
    let state_before_http = std::fs::read(&runtime.db).unwrap();
    let index_before_http = storage_files(&runtime.index);
    let server = tokio::spawn(notion_knowledge_server::http::serve_installed(
        config, handler, None, cancelled,
    ));
    let client = reqwest::Client::new();
    let url = format!("http://{address}/mcp");
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"joined-http","version":"1"}}});
    let before_embeddings = runtime.embeddings.load(Ordering::SeqCst);
    let before_source = runtime.source.attempts.load(Ordering::SeqCst);
    for token in [None, Some("invalid-token")] {
        let mut request = client
            .post(&url)
            .header("accept", "application/json, text/event-stream")
            .json(&initialize);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert!(!response.text().await.unwrap().contains("Saffron"));
    }
    let response = client
        .post(&url)
        .bearer_auth("fixture-private-authentication-token")
        .header("accept", "application/json, text/event-stream")
        .json(&initialize)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let session = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(http_json(response).await["id"], 1);
    let response = client
        .post(&url)
        .bearer_auth("fixture-private-authentication-token")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    for token in [None, Some("invalid-token")] {
        let mut request = client
            .post(&url)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .json(&search_request(2, "hybrid", json!({})));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        let mut request = client
            .get(&url)
            .header("accept", "text/event-stream")
            .header("mcp-session-id", &session);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(runtime.embeddings.load(Ordering::SeqCst), before_embeddings);
    assert_eq!(
        runtime.source.attempts.load(Ordering::SeqCst),
        before_source
    );
    let response = client
        .post(&url)
        .bearer_auth("fixture-private-authentication-token")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .json(&search_request(2, "hybrid", json!({"page_ids":[PAGE]})))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let output = http_json(response).await;
    let results = output["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert!(!results.is_empty(), "{output}");
    assert_eq!(results[0]["source"]["page_id"], PAGE);
    assert_eq!(results[0]["matched_paths"], json!(["semantic", "lexical"]));
    for request in [
        json!({"jsonrpc":"2.0","id":8,"method":"tools/list"}),
        related_request(9, json!({"page_id":PAGE})),
        related_request(10, json!({"page_id":OTHER})),
    ] {
        let response = client
            .post(&url)
            .bearer_auth("fixture-private-authentication-token")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let output = http_json(response).await;
        if request["id"] == 8 {
            assert!(
                output["result"]["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|t| t["name"] == "knowledge_related")
            );
        } else {
            assert_eq!(
                output["result"]["structuredContent"]["relations"]
                    .as_array()
                    .unwrap()
                    .len(),
                1,
                "{output}"
            );
        }
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state_before_http, std::fs::read(&runtime.db).unwrap());
    assert_eq!(index_before_http, storage_files(&runtime.index));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_activation_preserves_protected_storage_and_untrusted_http_cannot_rebuild() {
    use std::os::unix::fs::PermissionsExt;
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_search_fixture(&runtime).await;
    let config = retrieval_config(&runtime);
    let before_state = std::fs::read(&runtime.db).unwrap();
    let before_index = storage_files(&runtime.index);
    // A missing/invalid real model is rejected before recovery changes storage.
    assert!(
        notion_knowledge_server::retrieval_runtime::configured(
            notion_knowledge_mcp::KnowledgeServer::default(),
            &config,
            notion_knowledge_server::root_scope::Transport::Stdio,
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(&runtime.db).unwrap(), before_state);
    assert_eq!(storage_files(&runtime.index), before_index);

    // Corrupt operational state is protected; it is never treated as a fresh
    // empty ledger merely because the derived cache is rebuildable.
    let mut corrupt = retrieval_config(&runtime);
    let bad_state = runtime._dir.path().join("corrupt.sqlite");
    std::fs::write(&bad_state, b"corrupt").unwrap();
    std::fs::set_permissions(&bad_state, std::fs::Permissions::from_mode(0o600)).unwrap();
    corrupt.webhook_state_file = Some(bad_state.clone());
    assert!(
        installed_retrieval(
            &runtime,
            &corrupt,
            notion_knowledge_server::root_scope::Transport::Stdio
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(&bad_state).unwrap(), b"corrupt");
    assert_eq!(std::fs::read(&runtime.db).unwrap(), before_state);
    assert_eq!(storage_files(&runtime.index), before_index);

    let mut no_bearer = retrieval_config(&runtime);
    no_bearer.bearer_fallback = None;
    let mut public = retrieval_config(&runtime);
    public.http_bind = "192.0.2.1:3000".parse().unwrap();
    let source_reads = runtime.source.attempts.load(Ordering::SeqCst);
    let embeddings = runtime.embeddings.load(Ordering::SeqCst);
    for mut untrusted in [no_bearer, public] {
        let index = runtime._dir.path().join("untrusted-missing-index");
        let state = runtime._dir.path().join("untrusted-missing.sqlite");
        untrusted.retrieval.as_mut().unwrap().index = index.clone();
        untrusted.retrieval.as_mut().unwrap().assets = runtime._dir.path().join("absent-model");
        untrusted.webhook_state_file = Some(state.clone());
        let handler = installed_retrieval(
            &runtime,
            &untrusted,
            notion_knowledge_server::root_scope::Transport::Http,
        )
        .await
        .unwrap();
        let output = wire_stdio(handler, vec![search_request(2, "hybrid", json!({}))]).await;
        assert!(output[1].to_string().contains("retrieval_unavailable"));
        assert!(
            notion_knowledge_server::retrieval_runtime::configured(
                notion_knowledge_mcp::KnowledgeServer::default(),
                &untrusted,
                notion_knowledge_server::root_scope::Transport::Http,
            )
            .await
            .is_ok()
        );
        assert!(!index.exists());
        assert!(!state.exists());
    }
    assert_eq!(runtime.source.attempts.load(Ordering::SeqCst), source_reads);
    assert_eq!(runtime.embeddings.load(Ordering::SeqCst), embeddings);
    assert_eq!(std::fs::read(&runtime.db).unwrap(), before_state);
    assert_eq!(storage_files(&runtime.index), before_index);
}

fn storage_files(
    path: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(path, path, &mut files);
    files
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsuitable_http_startup_keeps_private_read_state_closed_and_never_creates_missing_storage()
{
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    {
        let mut config = retrieval_config(&runtime);
        config.bearer_fallback = None;
        let absent = runtime._dir.path().join("absent-bootstrap");
        config.webhook_state_file = Some(absent.join("state.sqlite"));
        config.retrieval.as_mut().unwrap().index = absent.join("index");
        config.graph_index = Some(absent.join("index"));
        config.retrieval.as_mut().unwrap().assets = absent.join("model");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.http_bind = listener.local_addr().unwrap();
        let address = config.http_bind;
        drop(listener);
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let server = tokio::spawn(notion_knowledge_server::http::serve_composed(
            config,
            notion_knowledge_mcp::KnowledgeServer::default(),
            None,
            cancelled,
        ));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let client = reqwest::Client::new();
        let url = format!("http://{address}/mcp");
        let response = client.post(&url).header("accept", "application/json, text/event-stream").json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"unavailable-read","version":"1"}}})).send().await.unwrap();
        {
            assert!(response.status().is_success());
            let session = response.headers()["mcp-session-id"]
                .to_str()
                .unwrap()
                .to_owned();
            http_json(response).await;
            client
                .post(&url)
                .header("accept", "application/json, text/event-stream")
                .header("mcp-session-id", &session)
                .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
                .send()
                .await
                .unwrap();
            for request in [
                search_request(2, "hybrid", json!({})),
                related_request(4, json!({"page_id":PAGE})),
                json!({"jsonrpc":"2.0","id":5,"method":"tools/list"}),
                json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_sync_status","arguments":{}}}),
            ] {
                let output = http_json(
                    client
                        .post(&url)
                        .header("accept", "application/json, text/event-stream")
                        .header("mcp-session-id", &session)
                        .json(&request)
                        .send()
                        .await
                        .unwrap(),
                )
                .await;
                assert!(
                    !output
                        .to_string()
                        .contains("operational_schema_version\":1")
                );
                assert!(!output.to_string().contains("Saffron"));
                if output["id"] == 5 {
                    assert!(!output.to_string().contains("knowledge_related"));
                }
                if output["id"] == 4 {
                    assert_eq!(output["result"]["isError"], true);
                }
                if output["id"] == 2 {
                    assert!(output.to_string().contains("retrieval_unavailable"));
                }
                if output["id"] == 3 {
                    assert!(
                        output["result"]["structuredContent"]["operational_schema_version"]
                            .is_null()
                    );
                }
            }
        }
        assert!(!absent.exists());
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!absent.exists());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_graph_is_default_off_and_untrusted_http_never_opens_private_state() {
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_graph_fixture(&runtime).await;
    let mut config = graph_config(&runtime);
    config.graph_index = None;
    config.webhook_state_file = Some(runtime._dir.path().join("missing.sqlite"));
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    let output = wire_stdio(
        handler,
        vec![json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})],
    )
    .await;
    assert!(!output[1].to_string().contains("knowledge_related"));
    config.graph_index = Some(runtime.index.clone());
    assert!(
        installed_retrieval(
            &runtime,
            &config,
            notion_knowledge_server::root_scope::Transport::Stdio
        )
        .await
        .is_err()
    );
    assert!(!config.webhook_state_file.as_ref().unwrap().exists());
    config.bearer_fallback = None;
    let source_before = runtime.source.attempts.load(Ordering::SeqCst);
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Http,
    )
    .await
    .unwrap();
    let output = wire_stdio(
        handler,
        vec![
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            related_request(3, json!({"page_id":PAGE})),
        ],
    )
    .await;
    assert!(!output[1].to_string().contains("knowledge_related"));
    assert_eq!(output[2]["result"]["isError"], true);
    assert_eq!(
        source_before,
        runtime.source.attempts.load(Ordering::SeqCst)
    );
    assert!(!config.webhook_state_file.as_ref().unwrap().exists());
    config = graph_config(&runtime);
    let other = TestRuntime::with_scope_policy("webhook-v1").await;
    config.graph_index = Some(other.index.clone());
    assert!(
        installed_retrieval(
            &runtime,
            &config,
            notion_knowledge_server::root_scope::Transport::Stdio
        )
        .await
        .is_err()
    );
    config = graph_config(&runtime);
    let bad = runtime._dir.path().join("corrupt.sqlite");
    std::fs::write(&bad, b"invalid SQLite").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o600)).unwrap();
    config.webhook_state_file = Some(bad.clone());
    assert!(
        installed_retrieval(
            &runtime,
            &config,
            notion_knowledge_server::root_scope::Transport::Stdio
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(bad).unwrap(), b"invalid SQLite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_graph_suppresses_explicitly_excluded_producer_targets() {
    let mut exclusions = ExclusionRules::default();
    exclusions.page_ids.insert(OTHER.into());
    let runtime = TestRuntime::with_scope_exclusions("webhook-v1", exclusions.clone()).await;
    commit_graph_fixture(&runtime).await;
    let mut config = graph_config(&runtime);
    config.notion_scope.as_mut().unwrap().exclusions = exclusions;
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    let output = wire_stdio(handler, vec![related_request(2, json!({"page_id":PAGE}))]).await;
    assert_eq!(
        output[1]["result"]["structuredContent"]["relations"],
        json!([])
    );
}

#[path = "support/joined_graph.rs"]
mod joined_graph;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_fresh_get_prefers_current_cache_and_defers_live_permissions_to_notion() {
    let runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    runtime.source.change(|model| {
        model.markdown.push_str(
            "\n![document](https://prod-files-secure.s3.us-west-2.amazonaws.com/\
         aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb/document.png?\
         X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=fixture&X-Amz-Date=20261007T000000Z&\
         X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=synthetic-capability)\n",
        )
    });
    commit_search_fixture(&runtime).await;
    let config = retrieval_config(&runtime);
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();

    let request = |id, freshness| {
        json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
        "name":"knowledge_get","arguments":{"refs":[{"kind":"page","id":PAGE}],"max_chars":2000,"freshness":freshness}}})
    };
    let body_reads = runtime.source.body_reads.load(Ordering::SeqCst);
    let output = wire_stdio(handler.clone(), vec![request(2, "fresh")]).await;
    let cached = &output[1]["result"]["structuredContent"]["sources"][0];
    assert_eq!(cached["read_from"], "cache");
    assert!(cached["text"].as_str().unwrap().contains("Saffron"));
    let canonical = cached["text"].as_str().unwrap();
    assert!(canonical.contains("/document.png"));
    assert!(!canonical.contains("X-Amz-Signature"));
    assert_eq!(runtime.source.body_reads.load(Ordering::SeqCst), body_reads);

    // The persisted acceptance signal dirties the page immediately, before
    // queue processing and even though Notion's edit timestamp is unchanged.
    runtime.source.change(|model| {
        model.markdown = "# Updated\n\nFresh cardamom from the authoritative source.\n".into();
    });
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.content_updated"))
        .unwrap();
    let before = std::fs::read(&runtime.db).unwrap();
    let output = wire_stdio(
        handler.clone(),
        vec![request(2, "indexed"), request(3, "fresh")],
    )
    .await;
    assert!(
        output[1]["result"]["structuredContent"]["sources"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Saffron")
    );
    let fresh = &output[2]["result"]["structuredContent"]["sources"][0];
    assert_eq!(fresh["read_from"], "notion");
    assert_eq!(fresh["content_scope"], "page");
    assert!(fresh["text"].as_str().unwrap().contains("Fresh cardamom"));
    assert_eq!(
        runtime.source.body_reads.load(Ordering::SeqCst),
        body_reads + 1
    );
    assert_eq!(std::fs::read(&runtime.db).unwrap(), before);

    runtime.source.change(|model| model.parent = FOREIGN.into());
    runtime.rewalk().await;
    let before = std::fs::read(&runtime.db).unwrap();
    let reads = runtime.source.body_reads.load(Ordering::SeqCst);
    let output = wire_stdio(
        handler.clone(),
        vec![request(2, "indexed"), request(3, "fresh")],
    )
    .await;
    assert_eq!(output[1]["result"]["isError"], true);
    let shared = &output[2]["result"]["structuredContent"]["sources"][0];
    assert_eq!(shared["read_from"], "notion");
    assert!(shared["text"].as_str().unwrap().contains("Fresh cardamom"));
    assert!(shared["provenance"].get("root_page_id").is_none());
    assert!(
        shared["provenance"]
            .get("indexed_last_edited_time")
            .is_none()
    );
    assert_eq!(runtime.source.body_reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(std::fs::read(&runtime.db).unwrap(), before);

    runtime.source.change(|model| model.archived = true);
    let output = wire_stdio(handler, vec![request(2, "fresh")]).await;
    assert_eq!(output[1]["result"]["isError"], true);
    assert!(output[1]["result"]["structuredContent"].is_null());
}

struct PauseSourceProvider {
    metadata: EmbeddingMetadata,
    pause: Arc<AtomicBool>,
}
impl EmbeddingProvider for PauseSourceProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.pause.store(true, Ordering::SeqCst);
        Box::pin(async move {
            Ok(texts
                .iter()
                .map(|t| {
                    if t.contains("ANKARE") || t == "Saffron fyr" || t == "CSV electricity" {
                        vec![1., 0., 0.]
                    } else if t.contains("GRANNE") {
                        vec![0.96, 0.28, 0.]
                    } else if t.contains("KONTROLL") {
                        vec![0.98, 0.2, 0.]
                    } else {
                        vec![t.len() as f32, 1., 0.]
                    }
                })
                .collect())
        })
    }
}

/// Hold the owned commit (guard and fence held, before effects) once the
/// next pause is armed, as the removed in-guard source read used to.
fn pause_owned_commits(runtime: &TestRuntime) {
    let (armed, entered, proceed) = (
        runtime.source.pause_next.clone(),
        runtime.source.entered.clone(),
        runtime.source.proceed.clone(),
    );
    notion_knowledge_retrieval::test_hooks::set_commit_pause(
        &runtime.index,
        Arc::new(move || {
            let (armed, entered, proceed) = (armed.clone(), entered.clone(), proceed.clone());
            Box::pin(async move {
                if armed.swap(false, Ordering::SeqCst) {
                    entered.notify_one();
                    proceed.notified().await;
                }
            })
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_owned_commit_source_read_releases_fence_and_restart_processes_healthy_page()
 {
    let mut runtime = TestRuntime::new().await;
    let metadata = runtime.worker.index.provider.metadata().clone();
    set_provider(
        &mut runtime.worker,
        Arc::new(PauseSourceProvider {
            metadata,
            pause: runtime.source.pause_next.clone(),
        }),
    );
    pause_owned_commits(&runtime);
    let interrupted = event(PAGE, "page.created");
    runtime.worker.state.receive(&interrupted).unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let worker = runtime.worker.clone();
    let running = tokio::spawn(async move { worker.run(token).await });
    tokio::time::timeout(Duration::from_secs(10), runtime.source.entered.notified())
        .await
        .unwrap();
    // Pause is inside the operation-owned precommit source revalidation, after
    // embedding and directory acquisition, rather than only in preparation.
    let pending = rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .query_row("SELECT count(*) FROM index_commit_receipts", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(pending, 1);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The sync names its own operation; this page's is the only receipt.
    let operation = owned_operation(&runtime);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .worker
                .coordinator
                .receipt(&operation)
                .unwrap()
                .is_some_and(|r| r.failure.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(runtime.searchable("Saffron").await, 0);
    runtime.source.proceed.notify_one();
    // A newly started serving worker can process another page without waiting
    // for the cancelled page's original inbox lease to expire.
    let healthy = event(OTHER, "page.created");
    runtime.worker.state.receive(&healthy).unwrap();
    let metadata = runtime.worker.index.provider.metadata().clone();
    let original = runtime.worker.clone();
    let provider: Arc<dyn EmbeddingProvider> = Arc::new(Provider {
        metadata,
        count: Arc::new(AtomicUsize::new(0)),
    });
    let scope = LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    };
    let journal = notion_knowledge_retrieval::planner::production_scope(&scope).unwrap();
    let restarted = Worker {
        webhook_enabled: true,
        scheduled: None,
        state: original.state.clone(),
        coordinator: original.coordinator.clone(),
        index: index_runtime(
            runtime.source.client(),
            original.index.chunks().clone(),
            original.coordinator.clone(),
            scope,
            journal,
            provider,
            None,
        ),
        scope: original.scope.clone(),
        integration: original.integration.clone(),
        poll: original.poll,
        timeout: original.timeout,
        log_enabled: false,
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        restarted.tick(CancellationToken::new(), false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(inspect(&restarted, &healthy).state, EventState::Succeeded);
    assert!(runtime.searchable("cinnamon").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_cancellation_retains_due_retry_and_allows_healthy_page_progress() {
    let mut runtime = TestRuntime::new().await;
    // Exercise the actual cancelled-connection boundary before the real Notion
    // calls, rather than treating one peer's EOF as source-service shutdown.
    drop(
        tokio::net::TcpStream::connect(("127.0.0.1", runtime.source.port))
            .await
            .unwrap(),
    );
    let metadata = runtime.worker.index.provider.metadata().clone();
    set_provider(
        &mut runtime.worker,
        Arc::new(PauseSourceProvider {
            metadata,
            pause: runtime.source.pause_next.clone(),
        }),
    );
    let worker = Arc::get_mut(&mut runtime.worker).unwrap();
    worker.timeout = Duration::from_secs(6);
    pause_owned_commits(&runtime);
    let bad = event(PAGE, "page.created");
    runtime.worker.state.receive(&bad).unwrap();
    let worker = runtime.worker.clone();
    let dispatch_started_at = now_ms() / 1000;
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(10), runtime.source.entered.notified())
        .await
        .unwrap();
    // The timeout is deliberately the real six-second worker deadline. Observe
    // its cancellation by waiting for the owned index receipt to be marked
    // failed; a one-second wall-clock allowance for both cancellation and
    // background cleanup is not reliable under parallel feature CI load.
    // The sync names its own operation; this page's is the only receipt.
    let operation = owned_operation(&runtime);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if runtime
                .worker
                .coordinator
                .receipt(&operation)
                .unwrap()
                .is_some_and(|receipt| receipt.failure.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("timed-out index operation must record cancelled receipt cleanup");
    // Joining synchronizes the durable inbox completion and writer-lease release.
    // This outer watchdog detects hangs; it does not set the worker deadline.
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("timed-out worker failed to complete cancellation cleanup")
        .unwrap()
        .unwrap();
    // A successful new acquisition alone is insufficient: the old fence
    // could have expired under load without ever being explicitly released.
    let expires_at: i64 = rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .query_row(
            "SELECT expires_at FROM reconciliation_lease WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        expires_at, 0,
        "deadline cancellation must release its writer lease"
    );
    let lease = runtime
        .worker
        .state
        .acquire_lease(now_ms() / 1000, 60)
        .expect("deadline cancellation must allow a fresh writer lease");
    runtime
        .worker
        .state
        .release_lease(&lease, now_ms() / 1000)
        .unwrap();
    let pending = inspect(&runtime.worker, &bad);
    assert_eq!(pending.state, EventState::Pending);
    assert_eq!(pending.last_failure, Some(EventFailure::Unavailable));
    assert_eq!(pending.cycle_attempts, 1);
    assert_eq!(pending.lifetime_attempts, 1);
    // Compare against the dispatch time, not the observation time: parallel
    // scheduling cannot make a correctly persisted retry appear overdue.
    assert!(
        pending.retry_at.unwrap()
            >= pending
                .policy
                .deadline(dispatch_started_at + 6, pending.cycle_attempts)
                .unwrap()
    );
    assert_eq!(runtime.searchable("Saffron").await, 0);
    runtime.source.proceed.notify_one();
    let metadata = runtime.worker.index.provider.metadata().clone();
    set_provider(
        &mut runtime.worker,
        Arc::new(Provider {
            metadata,
            count: runtime.embeddings.clone(),
        }),
    );
    let worker = Arc::get_mut(&mut runtime.worker).unwrap();
    worker.timeout = Duration::from_secs(10);
    let healthy = event(OTHER, "page.created");
    runtime.worker.state.receive(&healthy).unwrap();
    // The durable FIFO may admit the older cancelled event before the new
    // healthy event once its backoff expires. Exercise that boundary explicitly
    // rather than assuming local index inspection finishes within five seconds.
    tokio::time::timeout(Duration::from_secs(6), async {
        while now_ms() / 1000 < pending.retry_at.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let guard = std::fs::File::open(&runtime.index).unwrap();
    guard.try_lock().unwrap();
    drop(guard);
    for _ in 0..2 {
        if inspect(&runtime.worker, &healthy).state == EventState::Succeeded {
            break;
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            runtime.worker.tick(CancellationToken::new(), false),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(inspect(&runtime.worker, &bad).state, EventState::Succeeded);
    assert_eq!(inspect(&runtime.worker, &bad).cycle_attempts, 2);
    assert_eq!(inspect(&runtime.worker, &bad).lifetime_attempts, 2);
    assert_eq!(
        inspect(&runtime.worker, &healthy).state,
        EventState::Succeeded
    );
    assert_eq!(inspect(&runtime.worker, &healthy).cycle_attempts, 1);
    assert_eq!(inspect(&runtime.worker, &healthy).lifetime_attempts, 1);
    assert!(runtime.searchable("cinnamon").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_graph_denies_reads_after_interrupted_owned_producer_commit() {
    let mut runtime = TestRuntime::with_scope_policy("webhook-v1").await;
    commit_graph_fixture(&runtime).await;
    let config = graph_config(&runtime);
    let handler = installed_retrieval(
        &runtime,
        &config,
        notion_knowledge_server::root_scope::Transport::Stdio,
    )
    .await
    .unwrap();
    let metadata = runtime.worker.index.provider.metadata().clone();
    set_provider(
        &mut runtime.worker,
        Arc::new(PauseSourceProvider {
            metadata,
            pause: runtime.source.pause_next.clone(),
        }),
    );
    pause_owned_commits(&runtime);
    runtime.source.change(|m| {
        m.markdown = "Changed Saffron.".into();
        m.revision = "2026-10-10T14:00:00Z".into();
    });
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.content_updated"))
        .unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let worker = runtime.worker.clone();
    let running = tokio::spawn(async move { worker.run(token).await });
    tokio::time::timeout(Duration::from_secs(10), runtime.source.entered.notified())
        .await
        .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    runtime.source.proceed.notify_one();
    let output = wire_stdio(handler, vec![related_request(2, json!({"page_id":PAGE}))]).await;
    assert_eq!(output[1]["result"]["isError"], true, "{:?}", output);
    assert!(output[1]["result"]["structuredContent"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_missing_qwen_assets_leave_fresh_and_lexical_available() {
    use notion_knowledge_retrieval::embedder::{EmbedderConfig, deferred_configured_embedder};
    use notion_knowledge_server::root_scope::Transport;

    let empty = tempfile::tempdir().unwrap();
    let provider = deferred_configured_embedder(EmbedderConfig::LocalQwen {
        assets: empty.path().join("absent"),
    });
    let runtime = TestRuntime::with_provider(
        "webhook-v1",
        ExclusionRules::default(),
        false,
        Some(provider),
    )
    .await;
    let callback = event(PAGE, "page.content_updated");
    runtime.worker.state.receive(&callback).unwrap();
    assert!(
        runtime
            .worker
            .tick(CancellationToken::new(), false)
            .await
            .unwrap()
    );
    assert_eq!(
        inspect(&runtime.worker, &callback).state,
        EventState::Succeeded
    );
    assert_eq!(
        runtime
            .worker
            .index
            .chunks()
            .semantic_status()
            .await
            .unwrap()
            .1
            .queued,
        1
    );

    let source_reads = runtime.source.body_reads.load(Ordering::SeqCst);
    assert!(runtime.worker.semantic_step().await.unwrap());
    let (vectors, backlog) = runtime
        .worker
        .index
        .chunks()
        .semantic_status()
        .await
        .unwrap();
    assert_eq!((vectors, backlog.model_unavailable), (0, 1));
    let handler = installed_retrieval(&runtime, &retrieval_config(&runtime), Transport::Stdio)
        .await
        .unwrap();
    let scoped = json!({"page_ids":[PAGE]});
    let output = wire_stdio(handler, vec![
        search_request(2, "lexical", scoped.clone()),
        search_request(3, "hybrid", scoped.clone()),
        search_request(4, "semantic", scoped),
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{
            "name":"knowledge_get","arguments":{"refs":[{"kind":"page","id":PAGE}],"freshness":"fresh"}}}),
    ]).await;
    let lexical = &output[1]["result"]["structuredContent"];
    assert_eq!(
        lexical["results"][0]["source"]["page_id"], PAGE,
        "{output:?}"
    );
    let hybrid = &output[2]["result"]["structuredContent"];
    assert_eq!(hybrid["semantic_status"], "unavailable");
    assert_eq!(hybrid["results"], lexical["results"]);
    assert!(
        output[3].to_string().contains("semantic_unavailable"),
        "{output:?}"
    );
    assert_eq!(
        output[4]["result"]["structuredContent"]["sources"][0]["read_from"],
        "cache"
    );
    assert_eq!(
        runtime.source.body_reads.load(Ordering::SeqCst),
        source_reads
    );
}
