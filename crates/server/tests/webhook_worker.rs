#![cfg(all(unix, feature = "webhook-worker"))]
//! Credential-free, real Notion HTTP -> real SQLite/LanceDB integration.
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
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
    repair_bad: bool,
    unsupported: bool,
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
            unsupported: false,
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
                let bad = fail.swap(false, Ordering::SeqCst);
                let status = if bad {
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
use sha2::Digest;
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
        Box::pin(async move { Ok(texts.iter().map(|t| vec![t.len() as f32, 1., 0.]).collect()) })
    }
}
struct TestRuntime {
    _dir: tempfile::TempDir,
    worker: Arc<Worker>,
    source: HttpNotion,
    index: std::path::PathBuf,
    db: std::path::PathBuf,
    embeddings: Arc<AtomicUsize>,
}
impl TestRuntime {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = dir.path().join("index");
        std::fs::create_dir(&index).unwrap();
        let db = dir.path().join("state.sqlite");
        let journal = ReconciliationScope::new(vec![ROOT.into()], vec![], "fixture", "1").unwrap();
        let binding = CommitBinding::new("chunks", WORKSPACE, &journal, "1", 1).unwrap();
        let coordinator = IndexCommitCoordinator::initialize(&index, &db, binding).unwrap();
        let state = coordinator.state().clone();
        let metadata =
            EmbeddingMetadata::new("fixture".into(), "fixture".into(), "1".into(), 3).unwrap();
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
        let scope = LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        };
        let handler = AuthoritativePageRefresh::new(
            source.client(),
            guard,
            coordinator.clone(),
            scope,
            &journal,
            ChunkConfig {
                target_chars: 1000,
                overlap_chars: 0,
            },
        )
        .unwrap();
        let embeddings = Arc::new(AtomicUsize::new(0));
        let worker = Arc::new(Worker {
            state,
            coordinator,
            handler,
            provider: Arc::new(Provider {
                metadata,
                count: embeddings.clone(),
            }),
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
        }
    }
    async fn searchable(&self, text: &str) -> usize {
        LanceChunkTable::open(
            &self.index,
            "chunks",
            self.worker.provider.metadata().clone(),
        )
        .await
        .unwrap()
        .fts_query("text", text, 10)
        .await
        .unwrap()
        .len()
    }
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
    tick(&runtime.worker, false).await;
    assert_eq!(inspect(&runtime.worker, &good).state, EventState::Succeeded);
    assert!(runtime.searchable("Saffron").await > 0);
    tokio::time::sleep(Duration::from_secs(1)).await;
    tick(&runtime.worker, false).await;
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
    assert_eq!(pending.last_failure, Some(EventFailure::Source));
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
        runtime.worker.provider.metadata().clone(),
        reopened.clone(),
    )
    .unwrap();
    let scope = LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    };
    let journal = ReconciliationScope::new(vec![ROOT.into()], vec![], "fixture", "1").unwrap();
    let restarted = Worker {
        state: reopened.state().clone(),
        coordinator: reopened.clone(),
        handler: AuthoritativePageRefresh::new(
            runtime.source.client(),
            guard,
            reopened,
            scope,
            &journal,
            ChunkConfig {
                target_chars: 1000,
                overlap_chars: 0,
            },
        )
        .unwrap(),
        provider: runtime.worker.provider.clone(),
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
    runtime.source.change(|m| m.parent = FOREIGN.into());
    let moved = event(PAGE, "page.moved");
    runtime.worker.state.receive(&moved).unwrap();
    tick(&runtime.worker, false).await;
    assert_eq!(runtime.searchable("Saffron").await, 0);
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

struct PausedProvider {
    metadata: EmbeddingMetadata,
    entered: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}
impl EmbeddingProvider for PausedProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.entered.notify_one();
            self.proceed.notified().await;
            Ok(texts.iter().map(|t| vec![t.len() as f32, 1., 0.]).collect())
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_index_fence_and_expired_inbox_ownership_reject_stale_commit() {
    let mut runtime = TestRuntime::new().await;
    let paused = Arc::new(PausedProvider {
        metadata: runtime.worker.provider.metadata().clone(),
        entered: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    Arc::get_mut(&mut runtime.worker).unwrap().provider = paused.clone();
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
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    paused.entered.notified().await;
    // Ownership expires during preparation; final commit must consult the
    // actual inbox lease, rather than the separately acquired writer fence.
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute_batch("UPDATE webhook_inbox SET lease_until=0 WHERE state=1;")
        .unwrap();
    paused.proceed.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(runtime.searchable("Saffron").await, 0);
    let receipt = runtime.worker.coordinator.state().page_state(PAGE).unwrap();
    assert!(receipt.is_none());
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(!runtime.worker.tick(stop, false).await.unwrap());
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
        Box::pin(async move { Ok(texts.iter().map(|t| vec![t.len() as f32, 1., 0.]).collect()) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_owned_commit_source_read_releases_fence_and_restart_processes_healthy_page()
 {
    let mut runtime = TestRuntime::new().await;
    let metadata = runtime.worker.provider.metadata().clone();
    Arc::get_mut(&mut runtime.worker).unwrap().provider = Arc::new(PauseSourceProvider {
        metadata,
        pause: runtime.source.pause_next.clone(),
    });
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
    let operation = format!(
        "webhook-{:x}",
        sha2::Sha256::digest(format!(
            "{}:{}:{:?}",
            WORKSPACE,
            SUB,
            vec![interrupted.id.clone()]
        ))
    );
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
    let metadata = runtime.worker.provider.metadata().clone();
    let original = runtime.worker.clone();
    let provider: Arc<dyn EmbeddingProvider> = Arc::new(Provider {
        metadata,
        count: Arc::new(AtomicUsize::new(0)),
    });
    let restarted = Worker {
        state: original.state.clone(),
        coordinator: original.coordinator.clone(),
        handler: original.handler.clone(),
        provider,
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
async fn deadline_cancels_owned_source_read_and_next_healthy_page_progresses() {
    let mut runtime = TestRuntime::new().await;
    let metadata = runtime.worker.provider.metadata().clone();
    let worker = Arc::get_mut(&mut runtime.worker).unwrap();
    worker.provider = Arc::new(PauseSourceProvider {
        metadata,
        pause: runtime.source.pause_next.clone(),
    });
    worker.timeout = Duration::from_secs(6);
    let bad = event(PAGE, "page.created");
    runtime.worker.state.receive(&bad).unwrap();
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(10), runtime.source.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(7), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pending = inspect(&runtime.worker, &bad);
    assert_eq!(pending.state, EventState::Pending);
    assert_eq!(pending.last_failure, Some(EventFailure::Unavailable));
    assert_eq!(pending.cycle_attempts, 1);
    assert!(pending.retry_at.unwrap() > now_ms() / 1000);
    assert_eq!(runtime.searchable("Saffron").await, 0);
    runtime.source.proceed.notify_one();
    let metadata = runtime.worker.provider.metadata().clone();
    let worker = Arc::get_mut(&mut runtime.worker).unwrap();
    worker.provider = Arc::new(Provider {
        metadata,
        count: runtime.embeddings.clone(),
    });
    worker.timeout = Duration::from_secs(10);
    let healthy = event(OTHER, "page.created");
    runtime.worker.state.receive(&healthy).unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        runtime.worker.tick(CancellationToken::new(), false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        inspect(&runtime.worker, &healthy).state,
        EventState::Succeeded
    );
    assert!(runtime.searchable("cinnamon").await > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_lease_takeover_rejects_stale_effects_without_fatal_cleanup_or_releasing_new_owner()
{
    let mut runtime = TestRuntime::new().await;
    let paused = Arc::new(PausedProvider {
        metadata: runtime.worker.provider.metadata().clone(),
        entered: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    Arc::get_mut(&mut runtime.worker).unwrap().provider = paused.clone();
    let stale = event(PAGE, "page.created");
    runtime.worker.state.receive(&stale).unwrap();
    let worker = runtime.worker.clone();
    let task = tokio::spawn(async move { worker.tick(CancellationToken::new(), false).await });
    tokio::time::timeout(Duration::from_secs(10), paused.entered.notified())
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
    paused.proceed.notify_one();
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
async fn unsupported_authoritative_content_and_unowned_out_of_scope_page_fail_permanently() {
    let runtime = TestRuntime::new().await;
    runtime.source.change(|m| m.unsupported = true);
    let unsupported = event(PAGE, "page.created");
    let outside = event(FOREIGN, "page.created");
    for e in [&unsupported, &outside] {
        runtime.worker.state.receive(e).unwrap();
    }
    tick(&runtime.worker, false).await;
    tick(&runtime.worker, false).await;
    for e in [&unsupported, &outside] {
        let observed = inspect(&runtime.worker, e);
        assert_eq!(observed.state, EventState::Failed);
        assert_eq!(observed.cycle_attempts, 1);
        assert_eq!(observed.failure, Some(EventFailure::Source));
    }
    assert_eq!(runtime.searchable("Saffron").await, 0);
}
