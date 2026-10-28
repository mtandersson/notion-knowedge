#![cfg(all(unix, feature = "webhook-worker"))]
// Index maintenance shares these real authoritative HTTP / SQLite / Lance fixtures.
//! Credential-free, real Notion HTTP -> real SQLite/LanceDB integration.
use notion_knowledge_core::{
    backend::PageId,
    discovery::ExclusionRules,
    embedding::{EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    lifecycle::LifecycleScope,
    reconciliation::ReconciliationJournal,
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_server::{
    index_runtime::{IndexRuntime, OpenMode},
    reconciliation_command::{Command, Output, runtime::execute},
    webhook_worker::WorkerSettings,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const OTHER: &str = "55555555-5555-5555-5555-555555555555";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const FOREIGN: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

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
    relation_name: Option<String>,
    alias_names: Vec<String>,
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
            relation_name: None,
            alias_names: vec![],
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
    body_reads: Arc<AtomicUsize>,
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
                    page["properties"]["Alternate names"] = json!({"id":"names","type":"multi_select","multi_select":snapshot.alias_names.iter().map(|name|json!({"id":"choice","name":name})).collect::<Vec<_>>()});
                    page["properties"]["Unselected"] = json!({"id":"not-selected","type":"rich_text","rich_text":[{"type":"text","plain_text":"Never an alias","text":{"content":"Never an alias"}}]});
                    if let Some(name) = snapshot.relation_name {
                        page["properties"][name] = json!({"id":"shared-relation","type":"relation","relation":[{"id":OTHER}],"has_more":false});
                    }
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
            body_reads,
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
struct Provider {
    metadata: EmbeddingMetadata,
    count: AtomicUsize,
    fail: AtomicBool,
    fail_next: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    boundary_pause: Mutex<Option<Arc<RequestPause>>>,
}
impl Provider {
    fn new() -> Self {
        Self {
            metadata: EmbeddingMetadata::new("count".into(), "fixture".into(), "v1".into(), 3)
                .unwrap(),
            count: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            fail_next: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            boundary_pause: Mutex::new(None),
        }
    }
}
impl EmbeddingProvider for Provider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.count.fetch_add(texts.len(), Ordering::SeqCst);
        if let Some(pause) = self.boundary_pause.lock().unwrap().take() {
            pause.armed.store(true, Ordering::SeqCst);
        }
        let fail_once = self.fail_next.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if fail_once || self.fail.load(Ordering::SeqCst) {
                return Err(EmbeddingError::Unavailable);
            }
            Ok(texts
                .iter()
                .map(|s| vec![s.len() as f32, 1.0, 0.0])
                .collect())
        })
    }
}
struct Fixture {
    dir: tempfile::TempDir,
    state: PathBuf,
    settings: WorkerSettings,
    provider: Arc<Provider>,
}
fn authority() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}
impl Fixture {
    async fn new(http: &HttpNotion) -> Self {
        notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Off);
        let dir = tempfile::tempdir().unwrap();
        let settings = WorkerSettings {
            alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![]).unwrap(),
            index: dir.path().join("index"),
            assets: dir.path().join("assets"),
            poll_ms: 1000,
            timeout_seconds: 60,
        };
        fs::create_dir(&settings.index).unwrap();
        let state = dir.path().join("state.sqlite");
        let provider = Arc::new(Provider::new());
        let fixture = Self {
            dir,
            state,
            settings,
            provider,
        };
        fixture.open(http, OpenMode::Initialize).await;
        fixture
    }
    async fn open(&self, http: &HttpNotion, mode: OpenMode) -> IndexRuntime {
        IndexRuntime::compose(
            &self.settings,
            &self.state,
            authority(),
            http.client(),
            self.provider.clone(),
            mode,
        )
        .await
        .unwrap()
    }
    async fn command(&self, http: &HttpNotion, args: &[&str]) -> Output {
        self.command_cancel(http, args, CancellationToken::new())
            .await
    }
    async fn command_cancel(
        &self,
        http: &HttpNotion,
        args: &[&str],
        stop: CancellationToken,
    ) -> Output {
        execute(
            Command::parse(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap(),
            &self.settings,
            &self.state,
            authority(),
            http.client(),
            self.provider.clone(),
            stop,
        )
        .await
    }
    fn snapshot(&self) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        fn read(dir: &std::path::Path, files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    read(&path, files);
                } else {
                    files.insert(path.clone(), fs::read(path).unwrap());
                }
            }
        }
        let mut files = Default::default();
        read(self.dir.path(), &mut files);
        files
    }
}
fn counts(out: &Output, changed: u64, deleted: u64, unchanged: u64) {
    assert_eq!(out.exit, 0, "{:?}", out.json);
    assert_eq!(out.json["changed"], changed);
    assert_eq!(out.json["deleted"], deleted);
    assert_eq!(out.json["unchanged"], unchanged);
    let report = out.json.to_string();
    for secret in [
        "Saffron",
        "ravioli",
        "First",
        "fixture-credential",
        "notion.so",
        "Root overview",
    ] {
        assert!(!report.contains(secret), "{report}");
    }
}

#[tokio::test]
async fn actual_commands_apply_differences_only_and_keep_dry_run_and_status_read_only() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let db = rusqlite::Connection::open(&f.state).unwrap();
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        notion_knowledge_retrieval::sync_state::LATEST_SCHEMA_VERSION
    );
    drop(db);
    let before = f.snapshot();
    let dry = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(dry.exit, 0);
    assert_eq!(dry.json["planned_refresh"], 3);
    assert_eq!(dry.json["changed"], 0);
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
    assert_eq!(before, f.snapshot());
    let applied = f.command(&http, &["--reconcile", "apply", "first"]).await;
    counts(&applied, 3, 0, 0);
    assert_eq!(applied.json["scanned"], 3);
    let before = f.snapshot();
    let bodies = http.body_reads.load(Ordering::SeqCst);
    let embeds = f.provider.count.load(Ordering::SeqCst);
    counts(
        &f.command(&http, &["--reconcile", "status", "first"]).await,
        3,
        0,
        0,
    );
    assert_eq!(before, f.snapshot());
    counts(
        &f.command(&http, &["--reconcile", "apply", "second"]).await,
        0,
        0,
        3,
    );
    assert!(http.body_reads.load(Ordering::SeqCst) > bodies);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);
    http.change(|m| {
        m.title = "New metadata".into();
        m.revision = "2026-10-11T12:00:00Z".into();
        m.markdown.push_str("Changed oregano.");
    });
    counts(
        &f.command(&http, &["--reconcile", "apply", "stale"]).await,
        1,
        0,
        2,
    );
    http.change(|m| {
        m.hide_page = true;
        m.archived = true;
    });
    counts(
        &f.command(&http, &["--reconcile", "apply", "orphan"]).await,
        0,
        1,
        2,
    );
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.coordinator.state().acquire_lease(10, 30).is_err());
    assert!(
        runtime
            .coordinator
            .owned_page_ids()
            .unwrap()
            .contains(&PAGE.to_string())
    );
}

#[tokio::test]
async fn unknown_runs_missing_paths_and_incompatible_authority_fail_before_mutation() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    for args in [
        ["--reconcile", "status", "missing"],
        ["--reconcile", "resume", "missing"],
    ] {
        let before = f.snapshot();
        let out = f.command(&http, &args).await;
        assert_eq!(out.exit, 2);
        assert_eq!(before, f.snapshot());
    }
    for args in [
        &["--reconcile", "dry-run"][..],
        &["--reconcile", "status", "missing"][..],
    ] {
        let missing = f.dir.path().join("missing/state.sqlite");
        let out = execute(
            Command::parse(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap(),
            &f.settings,
            &missing,
            authority(),
            http.client(),
            f.provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(out.exit, 2);
        assert!(!missing.parent().unwrap().exists());
    }
    let before = f.snapshot();
    let mut scope = authority();
    scope.exclusions.page_ids.insert(PAGE.into());
    let out = execute(
        Command::Apply("different".into()),
        &f.settings,
        &f.state,
        scope,
        http.client(),
        f.provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(out.exit, 2);
    assert_eq!(before, f.snapshot());
    let mut settings = f.settings.clone();
    settings.index = f.dir.path().join("missing-index");
    let out = execute(
        Command::DryRun,
        &settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(out.exit, 2);
    assert!(!settings.index.exists());
    assert_eq!(before, f.snapshot());
    // A current version number with a corrupt/incomplete migration ledger must
    // not become an implicit repair/migration opportunity for read commands.
    let db = rusqlite::Connection::open(&f.state).unwrap();
    db.execute("DELETE FROM schema_migrations WHERE version=10", [])
        .unwrap();
    drop(db);
    let before = f.snapshot();
    let out = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(out.exit, 2);
    assert_eq!(before, f.snapshot());
}

#[tokio::test]
async fn failed_pages_require_explicit_retry_and_webhook_writer_busy_does_not_consume_work() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = runtime.coordinator.state().acquire_lease(now, 60).unwrap();
    let before = f.snapshot();
    let busy = f.command(&http, &["--reconcile", "apply", "busy"]).await;
    assert_eq!(busy.exit, 4);
    assert_eq!(before, f.snapshot());
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
    drop(runtime);
    f.provider.fail.store(true, Ordering::SeqCst);
    let failed = f.command(&http, &["--reconcile", "apply", "failed"]).await;
    assert_eq!(failed.exit, 3);
    assert_eq!(failed.json["errors"], 3);
    let embeds = f.provider.count.load(Ordering::SeqCst);
    f.provider.fail.store(false, Ordering::SeqCst);
    let resumed = f.command(&http, &["--reconcile", "resume", "failed"]).await;
    assert_eq!(resumed.exit, 3);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);
    for page in [PAGE, ROOT, OTHER] {
        let result = f
            .command(&http, &["--reconcile", "retry", "failed", page])
            .await;
        assert!(result.exit == 0 || result.exit == 3, "{:?}", result.json);
    }
    counts(
        &f.command(&http, &["--reconcile", "status", "failed"]).await,
        3,
        0,
        0,
    );
    let before = f.snapshot();
    assert_eq!(
        f.command(&http, &["--reconcile", "retry", "failed", PAGE])
            .await
            .exit,
        2
    );
    assert_eq!(before, f.snapshot());
}

#[tokio::test]
async fn cancellation_reopens_pending_work_and_permission_failure_never_deletes() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    f.provider.pause.store(true, Ordering::SeqCst);
    let stop = CancellationToken::new();
    let pending = f.command_cancel(&http, &["--reconcile", "apply", "cancel"], stop.clone());
    let cancel = async {
        f.provider.entered.notified().await;
        stop.cancel();
    };
    let (out, _) = tokio::join!(pending, cancel);
    assert_eq!(out.exit, 5);
    assert_eq!(out.json["phase"], "Applying");
    // Independent connections and runtime are rebuilt by each command invocation.
    counts(
        &f.command(&http, &["--reconcile", "resume", "cancel"]).await,
        3,
        0,
        0,
    );
    http.change(|m| {
        m.hide_page = true;
        m.inaccessible_page = true;
    });
    let before = f
        .open(&http, OpenMode::ReadOnly)
        .await
        .coordinator
        .owned_page_ids()
        .unwrap();
    let result = f.command(&http, &["--reconcile", "apply", "denied"]).await;
    assert_eq!(result.exit, 3);
    assert_eq!(result.json["deleted"], 0);
    assert_eq!(
        before,
        f.open(&http, OpenMode::ReadOnly)
            .await
            .coordinator
            .owned_page_ids()
            .unwrap()
    );
    let report = result.json.to_string();
    assert!(!report.contains("fixture-credential"));
    assert!(!report.contains("Saffron"));
}

#[tokio::test]
async fn timed_out_dry_run_leaves_state_unchanged_and_has_a_distinct_failure_exit() {
    let http = HttpNotion::start().await;
    let mut f = Fixture::new(&http).await;
    f.settings.timeout_seconds = 1;
    http.pause.armed.store(true, Ordering::SeqCst);
    let before = f.snapshot();
    let result = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(result.exit, 3);
    assert_eq!(result.json["status"], "timeout");
    assert_eq!(before, f.snapshot());
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 0);
    http.pause.proceed.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_command_drains_its_submitted_owned_guard_before_releasing_the_writer() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    *f.provider.boundary_pause.lock().unwrap() = Some(http.pause.clone());
    let stop = CancellationToken::new();
    let pending = f.command_cancel(
        &http,
        &["--reconcile", "apply", "owned-cancel"],
        stop.clone(),
    );
    let cancel = async {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            http.pause.started.notified(),
        )
        .await
        .unwrap();
        let guard = std::fs::File::open(&f.settings.index).unwrap();
        assert!(
            guard.try_lock().is_err(),
            "owned precommit must hold the actual directory guard"
        );
        stop.cancel();
    };
    let (out, _) = tokio::join!(pending, cancel);
    assert_eq!(out.exit, 5);
    assert_eq!(out.json["pending"], 3);
    assert_eq!(out.json["changed"], 0);
    let guard = std::fs::File::open(&f.settings.index).unwrap();
    guard.try_lock().unwrap();
    drop(guard);
    http.pause.proceed.notify_one();
    counts(
        &f.command(&http, &["--reconcile", "resume", "owned-cancel"])
            .await,
        3,
        0,
        0,
    );
}

#[tokio::test]
async fn read_only_commands_reject_external_wal_before_creating_sqlite_auxiliaries() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let db = rusqlite::Connection::open(&f.state).unwrap();
    let journal: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        journal, "delete",
        "production state uses SQLite's default rollback journal"
    );
    let journal: String = db
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    assert_eq!(journal, "wal");
    drop(db);
    let wal = std::path::PathBuf::from(format!("{}-wal", f.state.display()));
    let shm = std::path::PathBuf::from(format!("{}-shm", f.state.display()));
    assert!(!wal.exists());
    assert!(!shm.exists());
    let before = f.snapshot();
    for args in [
        &["--reconcile", "dry-run"][..],
        &["--reconcile", "status", "existing"][..],
    ] {
        let out = f.command(&http, args).await;
        assert_eq!(out.exit, 2);
        assert_eq!(out.json["status"], "index_state_configuration");
        assert_eq!(before, f.snapshot());
        assert!(!wal.exists());
        assert!(!shm.exists());
    }
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn read_only_commands_reject_previous_schema_without_migration_or_auxiliary_creation() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let db = rusqlite::Connection::open(&f.state).unwrap();
    db.execute_batch(include_str!(
        "../../retrieval/tests/fixtures/remove_schedule_schema.sql"
    ))
    .unwrap();
    drop(db);
    let before = f.snapshot();
    for args in [
        ["--reconcile", "dry-run"].as_slice(),
        ["--reconcile", "status", "unstarted"].as_slice(),
    ] {
        let out = f.command(&http, args).await;
        assert_eq!(out.exit, 2);
        assert_eq!(before, f.snapshot());
    }
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

async fn scheduled_server(
    f: &Fixture,
    source: &HttpNotion,
    timeout: std::time::Duration,
) -> (
    Arc<notion_knowledge_server::webhook_worker::runtime::Worker>,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    scheduled_server_attempts(f, source, timeout, 1).await
}
async fn scheduled_server_attempts(
    f: &Fixture,
    source: &HttpNotion,
    timeout: std::time::Duration,
    attempts: i64,
) -> (
    Arc<notion_knowledge_server::webhook_worker::runtime::Worker>,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    scheduled_server_workers(f, source, timeout, attempts, false).await
}
async fn scheduled_server_workers(
    f: &Fixture,
    source: &HttpNotion,
    timeout: std::time::Duration,
    attempts: i64,
    webhook_enabled: bool,
) -> (
    Arc<notion_knowledge_server::webhook_worker::runtime::Worker>,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    use notion_knowledge_server::{
        scheduled_reconciliation::{Settings, runtime::Worker as Scheduled},
        webhook_worker::runtime::Worker,
    };
    let index = f.open(source, OpenMode::ExistingWrite).await;
    let worker = Arc::new(Worker {
        webhook_enabled,
        state: index.coordinator.state().clone(),
        coordinator: index.coordinator.clone(),
        handler: index.handler.clone(),
        provider: index.provider.clone(),
        scope: notion_knowledge_core::webhook::InboxScope {
            workspace_id: WORKSPACE.into(),
            subscription_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        },
        integration: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
        poll: std::time::Duration::from_millis(10),
        timeout,
        log_enabled: false,
        scheduled: Some(Arc::new(Scheduled {
            index,
            settings: Settings {
                interval: 60,
                attempts,
                backoff: 1,
                maximum_backoff: 1,
            },
            poll: std::time::Duration::from_millis(10),
            timeout,
        })),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port().to_string();
    drop(listener);
    let config = notion_knowledge_server::config::Config::from_lookup(|name| {
        (name == "NK_HTTP_PORT").then(|| std::ffi::OsString::from(&port))
    })
    .unwrap();
    let stop = CancellationToken::new();
    let server_worker = worker.clone();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        notion_knowledge_server::http::serve_composed(
            config,
            notion_knowledge_mcp::KnowledgeServer::default(),
            Some(server_worker),
            shutdown,
        )
        .await
    });
    // Exercise the actual serving boundary, including real HTTP transport.
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/livez"))
                .send()
                .await
            {
                assert!(response.status().is_success());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (worker, stop, server)
}
async fn await_schedule(
    worker: &notion_knowledge_server::webhook_worker::runtime::Worker,
    predicate: impl Fn(&notion_knowledge_retrieval::schedule::Schedule) -> bool,
) -> notion_knowledge_retrieval::schedule::Schedule {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            if let Ok(schedule) = worker.coordinator.schedule("periodic")
                && predicate(&schedule)
            {
                return schedule;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "schedule wait {e:?}: {:?}",
            worker.coordinator.schedule("periodic")
        )
    })
}
#[tokio::test]
async fn actual_scheduled_http_preserves_deadline_restart_and_unchanged_next_cycle() {
    use notion_knowledge_retrieval::schedule::{
        ScheduleEventKind, SchedulePolicy, ScheduleReconfiguration,
    };
    use notion_knowledge_server::webhook_worker::runtime::now_ms;
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    let finished = await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    let events = worker
        .coordinator
        .schedule_events("periodic", 0, 100)
        .unwrap();
    let run = events
        .iter()
        .find(|e| e.kind == ScheduleEventKind::Completed)
        .unwrap()
        .run_id
        .clone()
        .unwrap();
    let progress = worker.coordinator.reconciliation_progress(&run).unwrap();
    assert_eq!(
        (
            progress.scanned,
            progress.changed,
            progress.deleted,
            progress.failed_pages()
        ),
        (3, 3, 0, 0)
    );
    assert!(finished.next_deadline >= now_ms() / 1000 + 58);
    stop.cancel();
    server.await.unwrap().unwrap();
    let before = f.snapshot();
    let dry = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(dry.json["planned_refresh"], 0);
    assert_eq!(before, f.snapshot());
    let bodies = http.body_reads.load(Ordering::SeqCst);
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(worker.coordinator.schedule("periodic").unwrap(), finished);
    assert_eq!(http.body_reads.load(Ordering::SeqCst), bodies);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    let now = now_ms() / 1000;
    let lease = worker.state.acquire_lease(now, 30).unwrap();
    worker
        .coordinator
        .reconfigure_schedule(
            &lease,
            now,
            "periodic",
            &ScheduleReconfiguration {
                previous_binding: worker.coordinator.binding().clone(),
                policy: SchedulePolicy::new(60, 1, 1, 1).unwrap(),
                next_deadline: now,
            },
        )
        .unwrap();
    worker.state.release_lease(&lease, now).unwrap();
    await_schedule(&worker, |s| s.cycle == 2 && s.active_run.is_none()).await;
    // The unchanged cycle still revalidates authoritative relation membership.
    assert!(http.body_reads.load(Ordering::SeqCst) > bodies);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    let status = f
        .command(&http, &["--reconcile", "schedule", "status"])
        .await;
    assert_eq!(status.exit, 0);
    assert_eq!(status.json["cycle"], 2);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_cancellation_resumes_same_pending_plan_without_implicit_retry() {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleStatus};
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    f.provider.pause.store(true, Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        f.provider.entered.notified(),
    )
    .await
    .unwrap();
    let pending = worker.coordinator.schedule("periodic").unwrap();
    let run = pending.active_run.clone().unwrap();
    let inventory = worker.coordinator.reconciliation_plan(&run).unwrap();
    let busy = f
        .command(&http, &["--reconcile", "apply", "competing"])
        .await;
    assert_eq!(busy.exit, 4);
    assert_eq!(worker.coordinator.schedule("periodic").unwrap().attempts, 0);
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(
        worker.coordinator.schedule("periodic").unwrap().active_run,
        Some(run.clone())
    );
    assert!(
        worker
            .coordinator
            .schedule_events("periodic", 0, 100)
            .unwrap()
            .iter()
            .any(|e| e.kind == ScheduleEventKind::Cancelled)
    );
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    let done = await_schedule(&worker, |s| s.active_run.is_none()).await;
    assert_eq!(done.cycle, 1);
    assert_eq!(done.status, ScheduleStatus::Ready);
    assert_eq!(
        worker.coordinator.reconciliation_plan(&run).unwrap(),
        inventory
    );
    assert_eq!(
        worker
            .coordinator
            .reconciliation_progress(&run)
            .unwrap()
            .changed,
        3
    );
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_holds_failed_pages_until_explicit_manual_recovery() {
    use notion_knowledge_retrieval::schedule::ScheduleStatus;
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    f.provider.fail_next.store(true, Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server_workers(&f, &http, std::time::Duration::from_secs(60), 1, true).await;
    let held = await_schedule(&worker, |s| s.status == ScheduleStatus::HeldPages).await;
    let run = held.active_run.clone().unwrap();
    let count = f.provider.count.load(Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(f.provider.count.load(Ordering::SeqCst), count);
    assert_eq!(held.attempts, 0);
    let failed = worker.coordinator.reconciliation_progress(&run).unwrap();
    assert!(failed.failed.contains_key(PAGE));
    assert_eq!(failed.failed.len(), 1);
    assert_eq!(
        failed.changed, 2,
        "the first bad page must not stop later healthy pages"
    );
    assert!(failed.pending.is_empty());
    // The operator rejection must test held-page policy, not race a live
    // webhook poll for the common writer lease. Quiesce before that assertion.
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(
        f.command(&http, &["--reconcile", "schedule", "retry", "0"])
            .await
            .exit,
        3
    );
    let (worker, stop, server) =
        scheduled_server_workers(&f, &http, std::time::Duration::from_secs(60), 1, true).await;
    assert_eq!(
        worker.coordinator.schedule("periodic").unwrap().status,
        ScheduleStatus::HeldPages
    );
    use notion_knowledge_core::webhook::{EventState, WebhookEvent, WebhookInbox};
    let event = WebhookEvent {
        id: "cccccccc-cccc-cccc-cccc-000000000002".into(),
        timestamp: "2026-10-10T12:00:00Z".into(),
        workspace_id: WORKSPACE.into(),
        subscription_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        integration_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
        event_type: "page.content_updated".into(),
        entity_id: OTHER.into(),
        entity_type: "page".into(),
        attempt_number: 1,
    };
    worker.state.receive(&event).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            if worker.state.event(&event.key()).unwrap().unwrap().state == EventState::Succeeded {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "webhook did not finish while failed pages were held: {error:?}; event={:?}",
            worker.state.event(&event.key()).unwrap()
        )
    });
    assert_eq!(
        worker.coordinator.schedule("periodic").unwrap().status,
        ScheduleStatus::HeldPages
    );
    assert_eq!(
        worker
            .coordinator
            .reconciliation_progress(&run)
            .unwrap()
            .failed,
        failed.failed
    );
    stop.cancel();
    server.await.unwrap().unwrap();
    for page in failed.failed.keys() {
        let out = f
            .command(&http, &["--reconcile", "retry", &run, page])
            .await;
        assert!(out.exit == 0 || out.exit == 3, "{:?}", out.json);
    }
    let resumed = f.command(&http, &["--reconcile", "resume", &run]).await;
    assert_eq!(resumed.exit, 0, "{:?}", resumed.json);
    assert!(
        !worker
            .coordinator
            .reconciliation_failure_evidence(&run, 0, 100)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn actual_scheduled_http_timeout_exhaustion_is_inspectable_and_explicit_abandon_preserves_history()
 {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleStatus};
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    http.pause.armed.store(true, Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_millis(150)).await;
    let exhausted = await_schedule(&worker, |s| s.status == ScheduleStatus::Exhausted).await;
    assert_eq!(exhausted.attempts, 1);
    let old_run = exhausted.active_run.clone().unwrap();
    let history = worker
        .coordinator
        .schedule_events("periodic", 0, 100)
        .unwrap();
    assert!(
        history
            .iter()
            .any(|e| e.kind == ScheduleEventKind::RunFailure)
    );
    let status = f
        .command(&http, &["--reconcile", "schedule", "status"])
        .await;
    assert_eq!(status.json["status"], "Exhausted");
    assert_eq!(status.json["attempts"], 1);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(worker.coordinator.schedule("periodic").unwrap(), exhausted);
    stop.cancel();
    server.await.unwrap().unwrap();
    http.pause.proceed.notify_one();
    assert_eq!(
        f.command(&http, &["--reconcile", "schedule", "abandon", "0"])
            .await
            .exit,
        0
    );
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 2 && s.active_run.is_none()).await;
    assert!(
        worker
            .coordinator
            .schedule_events("periodic", 0, 100)
            .unwrap()
            .starts_with(&history)
    );
    assert!(
        worker
            .state
            .start_or_resume(
                &worker
                    .state
                    .acquire_lease(
                        notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000,
                        30
                    )
                    .unwrap(),
                notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000,
                &old_run,
                &worker.scheduled.as_ref().unwrap().index.scope
            )
            .is_err()
    );
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_scheduled_http_shutdown_drains_live_owned_effect_before_release() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    *f.provider.boundary_pause.lock().unwrap() = Some(http.pause.clone());
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        http.pause.started.notified(),
    )
    .await
    .unwrap();
    let guard = std::fs::File::open(&f.settings.index).unwrap();
    assert!(guard.try_lock().is_err());
    drop(guard);
    let active = worker.coordinator.schedule("periodic").unwrap();
    stop.cancel();
    server.await.unwrap().unwrap();
    let guard = std::fs::File::open(&f.settings.index).unwrap();
    guard.try_lock().unwrap();
    drop(guard);
    assert_eq!(
        worker.coordinator.schedule("periodic").unwrap().active_run,
        active.active_run
    );
    let lease = worker
        .state
        .acquire_lease(
            notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000,
            30,
        )
        .unwrap();
    worker
        .state
        .release_lease(
            &lease,
            notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000,
        )
        .unwrap();
    http.pause.proceed.notify_one();
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_reprocesses_stale_and_safely_deletes_authoritative_orphans() {
    use notion_knowledge_retrieval::schedule::ScheduleEventKind;
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
    http.change(|m| {
        m.title = "Changed metadata".into();
        m.revision = "2026-10-11T12:00:00Z".into();
        m.markdown.push_str("More basil.");
    });
    assert_eq!(
        f.command(
            &http,
            &[
                "--reconcile",
                "schedule",
                "configure",
                "60",
                "2",
                "1",
                "1",
                "0"
            ]
        )
        .await
        .exit,
        0
    );
    let (worker, stop, server) =
        scheduled_server_attempts(&f, &http, std::time::Duration::from_secs(60), 2).await;
    await_schedule(&worker, |s| s.cycle == 2 && s.active_run.is_none()).await;
    let events = worker
        .coordinator
        .schedule_events("periodic", 0, 100)
        .unwrap();
    let run = events
        .iter()
        .rev()
        .find(|e| e.kind == ScheduleEventKind::Completed)
        .unwrap()
        .run_id
        .as_ref()
        .unwrap();
    let p = worker.coordinator.reconciliation_progress(run).unwrap();
    assert_eq!(
        (
            p.scanned,
            p.changed,
            p.deleted,
            p.unchanged,
            p.failed_pages()
        ),
        (3, 1, 0, 2, 0)
    );
    stop.cancel();
    server.await.unwrap().unwrap();
    http.change(|m| {
        m.hide_page = true;
        m.archived = true;
    });
    assert_eq!(
        f.command(&http, &["--reconcile", "schedule", "interval", "60", "0"])
            .await
            .exit,
        0
    );
    let (worker, stop, server) =
        scheduled_server_attempts(&f, &http, std::time::Duration::from_secs(60), 2).await;
    await_schedule(&worker, |s| s.cycle == 3 && s.active_run.is_none()).await;
    let events = worker
        .coordinator
        .schedule_events("periodic", 0, 100)
        .unwrap();
    let run = events
        .iter()
        .rev()
        .find(|e| e.kind == ScheduleEventKind::Completed)
        .unwrap()
        .run_id
        .as_ref()
        .unwrap();
    let p = worker.coordinator.reconciliation_progress(run).unwrap();
    assert_eq!(
        (p.changed, p.deleted, p.unchanged, p.failed_pages()),
        (0, 1, 2, 0)
    );
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_transient_source_attempts_back_off_then_exhaust_without_spinning() {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleStatus};
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server_attempts(&f, &http, std::time::Duration::from_millis(50), 2).await;
    let waiting = await_schedule(&worker, |s| s.status == ScheduleStatus::RetryWait).await;
    assert_eq!(waiting.attempts, 1);
    assert_eq!(
        worker
            .coordinator
            .schedule_events("periodic", 0, 100)
            .unwrap()
            .iter()
            .filter(|e| e.kind == ScheduleEventKind::RunFailure)
            .count(),
        1
    );
    let exhausted = await_schedule(&worker, |s| s.status == ScheduleStatus::Exhausted).await;
    assert_eq!(exhausted.attempts, 2);
    assert_eq!(exhausted.active_run, waiting.active_run);
    assert_eq!(exhausted.cycle, 1);
    let failures: Vec<_> = worker
        .coordinator
        .schedule_events("periodic", 0, 100)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == ScheduleEventKind::RunFailure)
        .collect();
    assert_eq!(failures.len(), 2);
    assert!(failures[1].at_seconds >= failures[0].deadline);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(worker.coordinator.schedule("periodic").unwrap(), exhausted);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_manual_periodic_and_webhook_contention_preserves_retry_budgets() {
    use notion_knowledge_core::webhook::{EventState, WebhookEvent, WebhookInbox};
    let http = HttpNotion::start().await;
    http.change(|m| {
        m.relation_name = Some("Scheduled relations".into());
        m.title = "Äpple".into();
    });
    let f = Fixture::new(&http).await;
    f.provider.pause.store(true, Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server_workers(&f, &http, std::time::Duration::from_secs(60), 1, true).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        f.provider.entered.notified(),
    )
    .await
    .unwrap();
    let schedule = worker.coordinator.schedule("periodic").unwrap();
    let event = WebhookEvent {
        id: "cccccccc-cccc-cccc-cccc-000000000001".into(),
        timestamp: "2026-10-10T12:00:00Z".into(),
        workspace_id: WORKSPACE.into(),
        subscription_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        integration_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
        event_type: "page.content_updated".into(),
        entity_id: PAGE.into(),
        entity_type: "page".into(),
        attempt_number: 1,
    };
    worker.state.receive(&event).unwrap();
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "manual-contender"])
            .await
            .exit,
        4
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let inbox = worker.state.event(&event.key()).unwrap().unwrap();
    assert_eq!(inbox.state, EventState::Pending);
    assert_eq!(inbox.cycle_attempts, 0);
    assert_eq!(worker.coordinator.schedule("periodic").unwrap(), schedule);
    stop.cancel();
    server.await.unwrap().unwrap();
    // The durable page remains available to the same real webhook caller once
    // the scheduled owner has drained and released its fence.
    assert!(!worker.tick(CancellationToken::new(), true).await.unwrap());
    assert_eq!(
        worker
            .state
            .event(&event.key())
            .unwrap()
            .unwrap()
            .cycle_attempts,
        0
    );
    let (scheduled, resumed_stop, resumed_server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&scheduled, |s| s.cycle == 1 && s.active_run.is_none()).await;
    assert!(
        scheduled
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    use notion_knowledge_core::graph::GraphEdgeStore;
    assert_eq!(
        scheduled.coordinator.state().edges_from(PAGE).unwrap()[0].display_name(),
        Some("Scheduled relations")
    );
    assert_eq!(
        f.open(&http, OpenMode::ReadOnly)
            .await
            .lookup_alias("äpple")
            .unwrap(),
        vec![PAGE]
    );
    resumed_stop.cancel();
    resumed_server.await.unwrap().unwrap();
    http.change(|m| {
        m.relation_name = Some("Webhook relations".into());
        m.title = "Apple".into();
    });
    assert!(worker.tick(CancellationToken::new(), true).await.unwrap());
    assert!(
        worker
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    assert_eq!(
        worker.coordinator.state().edges_from(PAGE).unwrap()[0].display_name(),
        Some("Webhook relations")
    );
    assert_eq!(
        worker.state.event(&event.key()).unwrap().unwrap().state,
        EventState::Succeeded
    );
    assert_eq!(worker.coordinator.schedule("periodic").unwrap().attempts, 0);
    let aliases = f.open(&http, OpenMode::ReadOnly).await;
    assert_eq!(aliases.lookup_alias("Apple").unwrap(), vec![PAGE]);
    assert!(aliases.lookup_alias("Äpple").unwrap().is_empty());
    drop(aliases);
    http.change(|m| {
        m.relation_name = Some("Manual relations".into());
        m.title = "Pomme".into();
    });
    // A label-only edit requires a relation projection refresh, despite unchanged content.
    counts(
        &f.command(&http, &["--reconcile", "apply", "manual-relations"])
            .await,
        1,
        0,
        2,
    );
    assert!(
        worker
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    assert_eq!(
        worker.coordinator.state().edges_from(PAGE).unwrap()[0].display_name(),
        Some("Manual relations")
    );
    let aliases = f.open(&http, OpenMode::ReadOnly).await;
    assert_eq!(aliases.lookup_alias("Pomme").unwrap(), vec![PAGE]);
    assert!(aliases.lookup_alias("Apple").unwrap().is_empty());
}

#[tokio::test]
async fn fatal_scheduled_storage_failure_terminates_actual_http_and_both_serving_workers() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server_workers(&f, &http, std::time::Duration::from_secs(60), 1, true).await;
    await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    // Trusted fixture fault at the actual durable boundary, after serving is live.
    rusqlite::Connection::open(&f.state)
        .unwrap()
        .execute(
            "ALTER TABLE reconciliation_schedules RENAME TO unavailable_schedules",
            [],
        )
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(stop.is_cancelled());
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = worker.state.acquire_lease(now, 30).unwrap();
    worker.state.release_lease(&lease, now).unwrap();
}

#[tokio::test]
async fn explicit_relation_rebuild_uses_shared_runtime_and_preserves_operational_history() {
    use notion_knowledge_core::{
        graph::{GraphEdge, GraphEdgeStore, GraphTarget},
        sync_state::SyncStateStore,
    };
    let http = HttpNotion::start().await;
    http.change(|m| {
        m.markdown = format!("[root](https://www.notion.so/{ROOT})");
        m.relation_name = Some("Ingredients".into());
    });
    let f = Fixture::new(&http).await;
    counts(
        &f.command(&http, &["--reconcile", "apply", "seed"]).await,
        3,
        0,
        0,
    );
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let operation = runtime
        .coordinator
        .reconciliation_plan("seed")
        .unwrap()
        .pages[PAGE]
        .operation_id
        .clone();
    let receipt = runtime.coordinator.receipt(&operation).unwrap().unwrap();
    let checkpoint = runtime.coordinator.state().page_state(PAGE).unwrap();
    let link = GraphEdge::new(
        PAGE.into(),
        GraphTarget::Page {
            page_id: ROOT.into(),
        },
        "link:page".into(),
        "markdown:0".into(),
    )
    .unwrap();
    runtime
        .coordinator
        .state()
        .replace_page_link_edges(PAGE, std::slice::from_ref(&link))
        .unwrap();
    let before = runtime.coordinator.state().edges_from(PAGE).unwrap();
    drop(runtime);
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    counts(
        &f.command(
            &http,
            &["--reconcile", "rebuild-relations", "operator-rebuild"],
        )
        .await,
        3,
        0,
        0,
    );
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(
        runtime
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    assert!(
        runtime
            .coordinator
            .current_relation_projection_complete(OTHER)
            .unwrap()
    );
    assert_eq!(
        runtime.coordinator.state().edges_from(PAGE).unwrap(),
        before
    );
    assert_eq!(
        runtime.coordinator.receipt(&operation).unwrap().unwrap(),
        receipt
    );
    assert_eq!(
        runtime.coordinator.state().page_state(PAGE).unwrap(),
        checkpoint
    );
    drop(runtime);
    let before = f.snapshot();
    assert_eq!(
        f.command(
            &http,
            &["--reconcile", "rebuild-relations", "operator-rebuild"]
        )
        .await
        .exit,
        2
    );
    assert_eq!(f.snapshot(), before);
    counts(
        &f.command(&http, &["--reconcile", "resume", "operator-rebuild"])
            .await,
        3,
        0,
        0,
    );
}

/// The exact installed runtime used by webhook/manual/periodic callers owns
/// SQLite aliases and current configuration across real index commits/reopens.
#[tokio::test]
async fn installed_aliases_preserve_ambiguity_explicit_sources_rebuild_and_restart() {
    use notion_knowledge_core::aliases::{AliasSources, PageAlias, PageAliasStore};
    let http = HttpNotion::start().await;
    http.change(|m| {
        m.title = "Öresund".into();
        m.alias_names = vec!["Oresund".into(), "Öresund".into(), "Oresund".into()];
        m.extra.insert(
            "66666666-6666-6666-6666-666666666666".into(),
            ExtraPage {
                title: "Öresund".into(),
                visible: true,
                archived: false,
            },
        );
    });
    let mut f = Fixture::new(&http).await;
    f.settings.alias_sources = AliasSources::new(true, vec!["names".into()]).unwrap();
    let output = f
        .command(&http, &["--reconcile", "apply", "alias-initial"])
        .await;
    assert_eq!(output.exit, 0, "{}", output.json);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    assert_eq!(
        runtime.lookup_alias("  ÖRESUND  ").unwrap(),
        vec![PAGE, "66666666-6666-6666-6666-666666666666"]
    );
    assert_eq!(runtime.lookup_alias("Oresund").unwrap(), vec![PAGE]);
    assert!(runtime.lookup_alias("Never an alias").unwrap().is_empty());
    assert!(runtime.lookup_alias("\n").is_err());
    // An unrelated namespace is preserved by the owned installed effect.
    runtime
        .coordinator
        .state()
        .replace_page_aliases(
            "outside",
            ROOT,
            PAGE,
            &[PageAlias::new("Keep", "title").unwrap()],
        )
        .unwrap();
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    drop(runtime);
    // No content or edit-time change: explicit source fields still replace rows.
    http.change(|m| m.alias_names = vec!["Sundet".into()]);
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-edit"])
            .await
            .exit,
        0
    );
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.lookup_alias("Oresund").unwrap().is_empty());
    assert_eq!(runtime.lookup_alias("Sundet").unwrap(), vec![PAGE]);
    assert_eq!(
        runtime
            .coordinator
            .state()
            .lookup_alias("outside", ROOT, "Keep")
            .unwrap(),
        vec![PAGE]
    );
    drop(runtime);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    // A configuration-only change with identical extracted names is incomplete.
    f.settings.alias_sources =
        AliasSources::new(true, vec!["names".into(), "absent".into()]).unwrap();
    let before = f.snapshot();
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.lookup_alias("Sundet").is_err());
    drop(runtime);
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.command(&http, &["--reconcile", "dry-run"]).await.exit, 0);
    assert_eq!(f.snapshot(), before);
    assert_eq!(
        f.command(&http, &["--reconcile", "rebuild-aliases", "alias-rebuild"])
            .await
            .exit,
        0
    );
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    assert_eq!(
        f.open(&http, OpenMode::ReadOnly)
            .await
            .lookup_alias("Sundet")
            .unwrap(),
        vec![PAGE]
    );
    // Property removal and title opt-out remove stale names atomically.
    f.settings.alias_sources = AliasSources::new(false, vec!["names".into()]).unwrap();
    http.change(|m| m.alias_names.clear());
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-empty"])
            .await
            .exit,
        0
    );
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.lookup_alias("Sundet").unwrap().is_empty());
    assert!(runtime.lookup_alias("Öresund").unwrap().is_empty());
    drop(runtime);
    http.change(|m| {
        m.alias_names = vec!["Äpple".into(), "Apple".into()];
    });
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-live"])
            .await
            .exit,
        0
    );
    assert_eq!(
        f.open(&http, OpenMode::ReadOnly)
            .await
            .lookup_alias("Apple")
            .unwrap(),
        vec![PAGE]
    );
    // Archive, restore, then physical removal exercise distinct lifecycle exits.
    http.change(|m| {
        m.archived = true;
        m.hide_page = true;
    });
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-archive"])
            .await
            .exit,
        0
    );
    assert!(
        f.open(&http, OpenMode::ReadOnly)
            .await
            .lookup_alias("Apple")
            .unwrap()
            .is_empty()
    );
    http.change(|m| {
        m.archived = false;
        m.hide_page = false;
    });
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-restore"])
            .await
            .exit,
        0
    );
    assert_eq!(
        f.open(&http, OpenMode::ReadOnly)
            .await
            .lookup_alias("Apple")
            .unwrap(),
        vec![PAGE]
    );
    // Physical removal and tombstone use the same owned refresh/receipt lifecycle.
    http.change(|m| {
        m.parent = FOREIGN.into();
        m.hide_page = true;
    });
    assert_eq!(
        f.command(&http, &["--reconcile", "apply", "alias-remove"])
            .await
            .exit,
        0
    );
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.lookup_alias("Apple").unwrap().is_empty());
    assert_eq!(
        runtime
            .coordinator
            .state()
            .lookup_alias("outside", ROOT, "Keep")
            .unwrap(),
        vec![PAGE]
    );
}

#[tokio::test]
async fn installed_alias_persistence_failure_replays_durable_repair_without_mutating_history() {
    use notion_knowledge_core::aliases::AliasSources;
    use notion_knowledge_retrieval::commit::ReceiptStatus;
    let http = HttpNotion::start().await;
    http.change(|m| m.alias_names = vec!["Äpple".into(), "Apple".into()]);
    let mut f = Fixture::new(&http).await;
    f.settings.alias_sources = AliasSources::new(false, vec!["names".into()]).unwrap();
    let apply = |runtime: IndexRuntime, id: &'static str| async move {
        let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
        let lease = runtime.coordinator.state().acquire_lease(now, 120).unwrap();
        let result = runtime
            .handler
            .apply_lifecycle(
                &PageId(PAGE.into()),
                id,
                runtime.provider.as_ref(),
                lease.clone(),
                Arc::new(|| notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000),
            )
            .await;
        runtime
            .coordinator
            .state()
            .release_lease(
                &lease,
                notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000,
            )
            .unwrap();
        result
    };
    apply(
        f.open(&http, OpenMode::ExistingWrite).await,
        "alias-history",
    )
    .await
    .unwrap();
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    let history = runtime
        .coordinator
        .receipt("alias-history")
        .unwrap()
        .unwrap();
    assert_eq!(runtime.lookup_alias("Apple").unwrap(), vec![PAGE]);
    drop(runtime);
    let db = rusqlite::Connection::open(&f.state).unwrap();
    db.execute_batch("CREATE TRIGGER alias_fault BEFORE INSERT ON graph_aliases BEGIN SELECT RAISE(ABORT,'fixture alias storage unavailable'); END;").unwrap();
    http.change(|m| m.alias_names = vec!["Päron".into()]);
    assert!(
        apply(
            f.open(&http, OpenMode::ExistingWrite).await,
            "alias-history"
        )
        .await
        .is_err()
    );
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert_eq!(
        runtime
            .coordinator
            .receipt("alias-history")
            .unwrap()
            .unwrap(),
        history
    );
    let repair = runtime
        .coordinator
        .unfinished_alias_repair_id("alias-history", PAGE)
        .unwrap()
        .unwrap();
    assert!(matches!(
        runtime
            .coordinator
            .receipt(&repair)
            .unwrap()
            .unwrap()
            .status,
        ReceiptStatus::Pending | ReceiptStatus::Failed
    ));
    assert!(runtime.lookup_alias("Apple").is_err());
    drop(runtime);
    // A source reversion cannot conceal partially committed vector work.
    http.change(|m| m.alias_names = vec!["Äpple".into(), "Apple".into()]);
    db.execute_batch("DROP TRIGGER alias_fault").unwrap();
    drop(db);
    apply(
        f.open(&http, OpenMode::ExistingWrite).await,
        "alias-history",
    )
    .await
    .unwrap();
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert_eq!(
        runtime
            .coordinator
            .receipt(&repair)
            .unwrap()
            .unwrap()
            .status,
        ReceiptStatus::Applied
    );
    assert_eq!(
        runtime
            .coordinator
            .receipt("alias-history")
            .unwrap()
            .unwrap(),
        history
    );
    assert_eq!(runtime.lookup_alias("Apple").unwrap(), vec![PAGE]);
    assert!(runtime.lookup_alias("Päron").unwrap().is_empty());
    drop(runtime);
    apply(
        f.open(&http, OpenMode::ExistingWrite).await,
        "alias-history",
    )
    .await
    .unwrap();
    // Applied alias repair must not suppress current configuration-only repair.
    f.settings.alias_sources =
        AliasSources::new(false, vec!["names".into(), "absent".into()]).unwrap();
    apply(
        f.open(&http, OpenMode::ExistingWrite).await,
        "alias-history",
    )
    .await
    .unwrap();
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert_eq!(runtime.lookup_alias("äpple").unwrap(), vec![PAGE]);
    assert_eq!(
        runtime
            .coordinator
            .receipt("alias-history")
            .unwrap()
            .unwrap(),
        history
    );
}

#[tokio::test]
async fn index_commands_provision_reuse_and_rebuild_search_without_changing_authoritative_content()
{
    use notion_knowledge_retrieval::chunks::GuardedChunkTable;
    use notion_knowledge_server::index_command::{
        Command as IndexCommand, runtime::execute as index,
    };
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    counts(
        &f.command(&http, &["--reconcile", "apply", "seed-index"])
            .await,
        3,
        0,
        0,
    );
    let body_reads = http.body_reads.load(Ordering::SeqCst);
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    let table = GuardedChunkTable::bind(
        &f.settings.index,
        "chunks",
        f.provider.metadata().clone(),
        runtime.coordinator.clone(),
    )
    .unwrap();
    assert!(
        table.search_snapshot().await.is_err(),
        "population alone must not imply trained ANN"
    );
    let owned = runtime.coordinator.owned_page_ids().unwrap();
    drop(table);
    drop(runtime);
    for action in ["ensure", "ensure", "rebuild"] {
        let command = IndexCommand::parse(&["--index".into(), action.into()]).unwrap();
        let report = index(
            command,
            &f.settings,
            &f.state,
            authority(),
            f.provider.metadata().clone(),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(report.exit, 0, "{report:?}");
        assert_eq!(
            report.json,
            json!({"operation":"index","status":"complete"})
        );
        let runtime = f.open(&http, OpenMode::ReadOnly).await;
        assert_eq!(runtime.coordinator.owned_page_ids().unwrap(), owned);
        let table = GuardedChunkTable::bind(
            &f.settings.index,
            "chunks",
            f.provider.metadata().clone(),
            runtime.coordinator.clone(),
        )
        .unwrap();
        let snapshot = table.search_snapshot().await.unwrap();
        assert_eq!(snapshot.count_rows().await.unwrap(), 4);
        let hits = snapshot.vector_query(&[1.0, 0.0, 0.0], 3, 1).await.unwrap();
        assert!(!hits.is_empty());
        assert!(hits.iter().all(|hit| owned.contains(&hit.page_id)));
    }
    assert_eq!(http.body_reads.load(Ordering::SeqCst), body_reads);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    counts(
        &f.command(&http, &["--reconcile", "status", "seed-index"])
            .await,
        3,
        0,
        0,
    );
}

#[tokio::test]
async fn index_commands_refuse_missing_foreign_cancelled_and_busy_state() {
    use notion_knowledge_server::index_command::{
        Command as IndexCommand, runtime::execute as index,
    };
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    let before = f.snapshot();
    let foreign =
        EmbeddingMetadata::new("foreign".into(), "model".into(), "revision".into(), 3).unwrap();
    for (state, scope, metadata, stop, exit) in [
        (
            f.dir.path().join("missing.sqlite"),
            authority(),
            f.provider.metadata().clone(),
            CancellationToken::new(),
            2,
        ),
        (
            f.state.clone(),
            authority(),
            foreign,
            CancellationToken::new(),
            2,
        ),
        (
            f.state.clone(),
            LifecycleScope {
                generation: 2,
                ..authority()
            },
            f.provider.metadata().clone(),
            CancellationToken::new(),
            2,
        ),
        (
            f.state.clone(),
            authority(),
            f.provider.metadata().clone(),
            {
                let s = CancellationToken::new();
                s.cancel();
                s
            },
            5,
        ),
    ] {
        let report = index(
            IndexCommand::Ensure,
            &f.settings,
            &state,
            scope,
            metadata,
            stop,
        )
        .await;
        assert_eq!(report.exit, exit, "{report:?}");
        assert_eq!(f.snapshot(), before);
    }
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = runtime.coordinator.state().acquire_lease(now, 120).unwrap();
    let before = f.snapshot();
    let report = index(
        IndexCommand::Rebuild,
        &f.settings,
        &f.state,
        authority(),
        f.provider.metadata().clone(),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.exit, 4);
    assert_eq!(report.json["status"], "busy");
    assert_eq!(f.snapshot(), before);
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
}

#[tokio::test]
async fn index_rebuild_refuses_foreign_vector_distance_without_replacing_it() {
    use notion_knowledge_retrieval::chunks::{
        GuardedChunkTable, VectorDistance, VectorIndexConfig,
    };
    use notion_knowledge_server::index_command::{
        Command as IndexCommand, runtime::execute as index,
    };
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    counts(
        &f.command(&http, &["--reconcile", "apply", "seed-distance"])
            .await,
        3,
        0,
        0,
    );
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let table = GuardedChunkTable::bind(
        &f.settings.index,
        "chunks",
        f.provider.metadata().clone(),
        runtime.coordinator.clone(),
    )
    .unwrap();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = runtime.coordinator.state().acquire_lease(now, 120).unwrap();
    let config = VectorIndexConfig {
        distance: VectorDistance::L2,
        num_partitions: Some(1),
        ..Default::default()
    };
    table
        .ensure_vector_index(config.clone(), lease.clone(), Arc::new(move || now))
        .await
        .unwrap();
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
    for command in [IndexCommand::Ensure, IndexCommand::Rebuild] {
        let report = index(
            command,
            &f.settings,
            &f.state,
            authority(),
            f.provider.metadata().clone(),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(report.exit, 3, "{report:?}");
    }
    let lease = runtime.coordinator.state().acquire_lease(now, 120).unwrap();
    // An exact L2 ensure can still open and optimize: rejected Cosine maintenance
    // did not replace the existing persisted distance configuration.
    table
        .ensure_vector_index(config, lease.clone(), Arc::new(move || now))
        .await
        .unwrap();
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
    assert_eq!(
        table
            .search_snapshot()
            .await
            .unwrap()
            .count_rows()
            .await
            .unwrap(),
        4
    );
    counts(
        &f.command(&http, &["--reconcile", "status", "seed-distance"])
            .await,
        3,
        0,
        0,
    );
}

#[tokio::test]
async fn lost_or_incompatible_cache_resyncs_without_replaying_external_effects() {
    use notion_knowledge_retrieval::chunks::GuardedChunkTable;
    for damage in ["deleted", "incompatible", "corrupt"] {
        let http = HttpNotion::start().await;
        let f = Fixture::new(&http).await;
        counts(
            &f.command(&http, &["--reconcile", "apply", "seed-cache"])
                .await,
            3,
            0,
            0,
        );
        {
            let db = rusqlite::Connection::open(&f.state).unwrap();
            db.execute_batch(
                "CREATE TABLE external_write_ledger(id TEXT PRIMARY KEY, result TEXT);
                INSERT INTO external_write_ledger VALUES('already-written','done');
                INSERT INTO webhook_events(event_id) VALUES('already-admitted');",
            )
            .unwrap();
            db.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id,state) VALUES(?1,'done-write',?2,2)", [WORKSPACE, PAGE]).unwrap();
        }
        let provider = if damage == "incompatible" {
            Arc::new(Provider {
                metadata: EmbeddingMetadata::new("count".into(), "fixture".into(), "v2".into(), 3)
                    .unwrap(),
                ..Provider::new()
            })
        } else {
            f.provider.clone()
        };
        if damage == "deleted" {
            fs::remove_dir_all(&f.settings.index).unwrap();
        }
        if damage == "corrupt" {
            fs::remove_dir_all(f.settings.index.join("chunks.lance")).unwrap();
            fs::write(f.settings.index.join("chunks.lance"), b"unusable cache").unwrap();
        }
        let reads = http.body_reads.load(Ordering::SeqCst);
        let recovered = if damage == "corrupt" {
            IndexRuntime::reset_and_resync(
                &f.settings,
                &f.state,
                authority(),
                http.client(),
                provider.clone(),
            )
            .await
        } else {
            IndexRuntime::recover(
                &f.settings,
                &f.state,
                authority(),
                http.client(),
                provider.clone(),
            )
            .await
        }
        .unwrap();
        assert!(http.body_reads.load(Ordering::SeqCst) > reads);
        assert_eq!(recovered.coordinator.owned_page_ids().unwrap().len(), 3);
        assert!(
            !recovered
                .coordinator
                .state()
                .cache_rebuild_pending()
                .unwrap()
        );
        let table = GuardedChunkTable::bind(
            &f.settings.index,
            "chunks",
            provider.metadata().clone(),
            recovered.coordinator.clone(),
        )
        .unwrap();
        assert!(!table.page_chunks(PAGE).await.unwrap().is_empty());
        let db = rusqlite::Connection::open(&f.state).unwrap();
        for (query, expected) in [
            (
                "SELECT result FROM external_write_ledger WHERE id='already-written'",
                "done",
            ),
            ("SELECT event_id FROM webhook_events", "already-admitted"),
            ("SELECT mutation_digest FROM mutation_refresh", "done-write"),
        ] {
            assert_eq!(
                db.query_row(query, [], |r| r.get::<_, String>(0)).unwrap(),
                expected
            );
        }
        drop(db);
        drop(table);
        drop(recovered);
        let reads = http.body_reads.load(Ordering::SeqCst);
        let _reopened =
            IndexRuntime::recover(&f.settings, &f.state, authority(), http.client(), provider)
                .await
                .unwrap();
        assert_eq!(
            http.body_reads.load(Ordering::SeqCst),
            reads,
            "healthy restart must reuse the cache"
        );
        // HttpNotion rejects every non-GET request, including writes/uploads.
    }
}

#[tokio::test]
async fn failed_cache_resync_stays_pending_and_next_start_retries_from_notion() {
    let http = HttpNotion::start().await;
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    f.provider.fail.store(true, Ordering::SeqCst);
    assert!(
        IndexRuntime::recover(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone()
        )
        .await
        .is_err()
    );
    let state =
        notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open_existing(&f.state)
            .unwrap();
    assert!(state.cache_rebuild_pending().unwrap());
    f.provider.fail.store(false, Ordering::SeqCst);
    let recovered = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(
        !recovered
            .coordinator
            .state()
            .cache_rebuild_pending()
            .unwrap()
    );
    assert_eq!(recovered.coordinator.owned_page_ids().unwrap().len(), 3);
    drop(state);
    drop(recovered);
    http.state
        .lock()
        .unwrap()
        .markdown
        .push_str("\nA subsequent incremental update.");
    http.state.lock().unwrap().revision = "2026-10-28T12:00:00Z".into();
    counts(
        &f.command(&http, &["--reconcile", "apply", "after-cache-resync"])
            .await,
        1,
        0,
        2,
    );
}
