#![cfg(all(unix, feature = "webhook-worker"))]
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
        Box::pin(async move {
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if self.fail.load(Ordering::SeqCst) {
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
        let dir = tempfile::tempdir().unwrap();
        let settings = WorkerSettings {
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
    assert_eq!(http.body_reads.load(Ordering::SeqCst), bodies);
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
