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
use notion_knowledge_server::{
    index_runtime::{IndexRuntime, OpenMode, PageDeadline, SyncMode, SyncTotals},
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
use tokio_util::sync::CancellationToken;

use notion_fake::{
    ExtraPage, FOREIGN, FakeNotion as HttpNotion, OTHER, PAGE, ROOT, RequestPause, WORKSPACE,
};
/// This suite's page carries alias properties and a relation to OTHER.
async fn start_notion() -> HttpNotion {
    let notion = HttpNotion::start().await;
    notion.change(|m| {
        m.alias_properties = true;
        m.relation_property_id = "shared-relation".into();
        m.relation_targets = vec![OTHER.into()];
    });
    notion
}
struct Provider {
    metadata: EmbeddingMetadata,
    count: AtomicUsize,
    fail: AtomicBool,
    fail_next: AtomicBool,
    input_limit_bytes: AtomicUsize,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    boundary_pause: Mutex<Option<Arc<RequestPause>>>,
    retain_when_cancelled: AtomicBool,
    busy: AtomicBool,
    idle_entered: tokio::sync::Notify,
    idle_release: tokio::sync::Semaphore,
    active: AtomicUsize,
    max_active: AtomicUsize,
}
impl Provider {
    fn new() -> Self {
        Self {
            metadata: EmbeddingMetadata::new("count".into(), "fixture".into(), "v1".into(), 3)
                .unwrap(),
            count: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            fail_next: AtomicBool::new(false),
            input_limit_bytes: AtomicUsize::new(0),
            pause: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            boundary_pause: Mutex::new(None),
            retain_when_cancelled: AtomicBool::new(false),
            busy: AtomicBool::new(false),
            idle_entered: tokio::sync::Notify::new(),
            idle_release: tokio::sync::Semaphore::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        }
    }
}
impl EmbeddingProvider for Provider {
    fn wait_idle(&self) -> notion_knowledge_core::embedding::EmbeddingIdleFuture<'_> {
        Box::pin(async move {
            if self.busy.load(Ordering::SeqCst) {
                self.idle_entered.notify_one();
                self.idle_release.acquire().await.unwrap().forget();
                self.busy.store(false, Ordering::SeqCst);
            }
            Ok(())
        })
    }
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn partition_input<'a>(
        &'a self,
        text: &'a str,
        boundaries: Vec<usize>,
    ) -> notion_knowledge_core::embedding::EmbeddingPartitionFuture<'a> {
        let input_limit = self.input_limit_bytes.load(Ordering::SeqCst);
        Box::pin(async move {
            notion_knowledge_core::embedding::partition_at_boundaries(text, &boundaries, |part| {
                if input_limit > 0 && part.len() > input_limit {
                    Err(EmbeddingError::InputLimit)
                } else {
                    Ok(())
                }
            })
        })
    }
    fn embed_batch<'a>(&'a self, texts: &'a [String]) -> EmbeddingFuture<'a> {
        self.count.fetch_add(texts.len(), Ordering::SeqCst);
        if let Some(pause) = self.boundary_pause.lock().unwrap().take() {
            pause.armed.store(true, Ordering::SeqCst);
        }
        let fail_once = self.fail_next.swap(false, Ordering::SeqCst);
        let input_limit = self.input_limit_bytes.load(Ordering::SeqCst);
        Box::pin(async move {
            struct Active<'a>(&'a AtomicUsize);
            impl Drop for Active<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(now, Ordering::SeqCst);
            let _active = Active(&self.active);
            if self.busy.load(Ordering::SeqCst) {
                return Err(EmbeddingError::ResourceExhausted);
            }
            if texts.len() > 32
                || (input_limit > 0 && texts.iter().any(|text| text.len() > input_limit))
            {
                return Err(EmbeddingError::InputLimit);
            }
            if self.pause.swap(false, Ordering::SeqCst) {
                if self.retain_when_cancelled.load(Ordering::SeqCst) {
                    self.busy.store(true, Ordering::SeqCst);
                }
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
    scope: LifecycleScope,
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
        Self::with_scope(http, authority()).await
    }
    async fn with_scope(http: &HttpNotion, scope: LifecycleScope) -> Self {
        notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Off);
        let dir = tempfile::tempdir().unwrap();
        let settings = WorkerSettings {
            projection_mode: Default::default(),
            lexical: None,
            alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![]).unwrap(),
            index: dir.path().join("index"),
            assets: dir.path().join("assets"),
            poll_ms: 1000,
            timeout_seconds: 60,
            inventory_timeout_seconds: 900,
            prefetch_workers: 2,
        };
        fs::create_dir(&settings.index).unwrap();
        let state = dir.path().join("state.sqlite");
        let provider = Arc::new(Provider::new());
        let fixture = Self {
            dir,
            state,
            settings,
            provider,
            scope,
        };
        fixture.open(http, OpenMode::Initialize).await;
        fixture
    }
    async fn open(&self, http: &HttpNotion, mode: OpenMode) -> IndexRuntime {
        IndexRuntime::compose(
            &self.settings,
            &self.state,
            self.scope.clone(),
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
            self.scope.clone(),
            http.client(),
            self.provider.clone(),
            stop,
        )
        .await
    }
    /// Record a prior walk of the root tree. Relation and link targets
    /// resolve only from the inventory, so a target a walk has not placed
    /// stays unresolved until the next walk.
    async fn seed_inventory(&self, http: &HttpNotion) {
        let runtime = self.open(http, OpenMode::ExistingWrite).await;
        let walk = http.client().walk_tree(&self.scope, None).await.unwrap();
        runtime
            .coordinator
            .state()
            .record_walk(&walk.run_id, &walk.entries, walk.authoritative())
            .unwrap();
    }
    /// Populate through the core refresh seam without production ANN completion,
    /// for fixtures that deliberately construct missing or foreign indexes.
    async fn populate(&self, http: &HttpNotion, _run: &str) {
        let runtime = self.open(http, OpenMode::ExistingWrite).await;
        let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
        let mut lease = runtime.coordinator.state().acquire_lease(now, 330).unwrap();
        runtime
            .sync_from_walk(
                &notion_knowledge_server::index_runtime::PageDeadline {
                    timeout_seconds: 60,
                },
                &mut lease,
                notion_knowledge_server::index_runtime::SyncMode::Full,
            )
            .await
            .unwrap();
        // Seed the v1 physical fixture, without promoting an ANN. This
        // intentionally predates separate text and embedding state.
        let raw = notion_knowledge_retrieval::chunks::LanceChunkTable::open(
            &self.settings.index,
            "chunks",
            self.provider.metadata().clone(),
        )
        .await
        .unwrap();
        for page in runtime.coordinator.owned_page_ids().unwrap() {
            let text = runtime.chunks().page_chunks(&page).await.unwrap();
            raw.apply_page_diff(self.provider.as_ref(), &page, &text)
                .await
                .unwrap();
        }
        remove_text_projection(&self.settings.index);
        runtime
            .coordinator
            .state()
            .release_lease(&lease, now)
            .unwrap();
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

#[tokio::test]
async fn lexical_cache_rebuild_blocks_reads_and_preserves_external_work() {
    use notion_knowledge_core::search::{LexicalQuery, LexicalSearch};
    use notion_knowledge_retrieval::{
        chunks::{GuardedChunkTable, lexical_binding::LexicalCachePlan},
        lexical_dictionary::test_fixtures::dictionary,
    };
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let existing = f.open(&http, OpenMode::ReadOnly).await;
    let binding = existing.coordinator.binding().clone();
    drop(existing);
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
    let plan = LexicalCachePlan::verified(Arc::new(dictionary(&[("saffron", "saffran")]))).unwrap();
    // Without a search listing the walk reads PAGE's metadata, where it pauses.
    http.change(|m| m.search_unavailable = true);
    http.pause.armed.store(true, Ordering::SeqCst);
    let source = http.client();
    let recovery = IndexRuntime::recover_lexical_cache(
        &f.settings,
        &f.state,
        authority(),
        source,
        f.provider.clone(),
        plan.clone(),
        OpenMode::Initialize,
    );
    tokio::pin!(recovery);
    tokio::select! {
        result = &mut recovery => panic!("recovery must wait for its authoritative read: {}", result.is_ok()),
        _ = http.pause.started.notified() => {}
    }
    let coordinator = notion_knowledge_retrieval::commit::IndexCommitCoordinator::open_read_only(
        &f.settings.index,
        &f.state,
        binding,
    )
    .unwrap();
    assert!(coordinator.state().cache_rebuild_pending().unwrap());
    let table = GuardedChunkTable::bind_with_lexical_plan(
        &f.settings.index,
        "chunks",
        f.provider.metadata().clone(),
        coordinator,
        plan.identity(),
    )
    .unwrap();
    assert!(
        table.search_snapshot().await.is_err(),
        "partially rebuilt content must not be served"
    );
    http.pause.proceed.notify_one();
    let runtime = recovery.await.unwrap();
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    assert!(
        runtime
            .lexical_plan()
            .unwrap()
            .identity()
            .matches(plan.dictionary())
    );
    drain_semantic(&runtime).await;
    let snapshot = runtime.chunks().search_snapshot().await.unwrap();
    for text in ["ravioli", PAGE] {
        let hits = snapshot
            .search(LexicalQuery {
                query: text.into(),
                limit: 3,
                page_ids: Some(vec![PAGE.into()]),
                root_page_ids: Some(vec![ROOT.into()]),
                metadata: Default::default(),
            })
            .await
            .unwrap();
        assert!(hits.iter().any(|hit| hit.source.page_id == PAGE));
    }
    assert!(
        !snapshot
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
    drop(snapshot);
    drop(runtime);
    let reads = http.body_reads.load(Ordering::SeqCst);
    let embeds = f.provider.count.load(Ordering::SeqCst);
    let before = f.snapshot();
    let healthy = IndexRuntime::recover_lexical_cache(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
        plan.clone(),
        OpenMode::ReadOnly,
    )
    .await
    .unwrap();
    assert_eq!(http.body_reads.load(Ordering::SeqCst), reads);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);
    assert_eq!(
        f.snapshot(),
        before,
        "healthy read-only recovery must not write"
    );
    drop(healthy);

    // Metadata damage and cache loss both use the same authoritative resync.
    for damage in ["missing-binding", "mismatched-plan", "deleted-cache"] {
        let path = f.settings.index.join(".nk-lexical-binding-v1.json");
        match damage {
            "missing-binding" => fs::remove_file(path).unwrap(),
            "mismatched-plan" => {
                use notion_knowledge_retrieval::chunks::lexical_binding::LexicalIndexBinding;
                let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                value["plan"]["asset_sha256"] = json!("a".repeat(64));
                let changed: LexicalIndexBinding = serde_json::from_value(value).unwrap();
                fs::write(path, changed.canonical_bytes().unwrap()).unwrap();
            }
            _ => fs::remove_dir_all(&f.settings.index).unwrap(),
        }
        let reads = http.body_reads.load(Ordering::SeqCst);
        let recovered = IndexRuntime::recover_lexical_cache(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone(),
            plan.clone(),
            OpenMode::ReadOnly,
        )
        .await
        .unwrap();
        assert!(http.body_reads.load(Ordering::SeqCst) > reads, "{damage}");
        assert!(
            !recovered
                .chunks()
                .page_chunks(PAGE)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(recovered.chunks().search_snapshot().await.is_ok());
    }
    let db = rusqlite::Connection::open(&f.state).unwrap();
    for (query, expected) in [
        ("SELECT result FROM external_write_ledger", "done"),
        ("SELECT event_id FROM webhook_events", "already-admitted"),
        ("SELECT mutation_digest FROM mutation_refresh", "done-write"),
    ] {
        assert_eq!(
            db.query_row(query, [], |r| r.get::<_, String>(0)).unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn lexical_cache_failed_resync_remains_unavailable_and_retries_current_scope() {
    use notion_knowledge_retrieval::{
        chunks::lexical_binding::LexicalCachePlan, lexical_dictionary::test_fixtures::dictionary,
    };
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let plan = LexicalCachePlan::verified(Arc::new(dictionary(&[("saffron", "saffran")]))).unwrap();
    http.change(|source| source.unauthenticated = true);
    assert!(
        IndexRuntime::recover_lexical_cache(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone(),
            plan.clone(),
            OpenMode::Initialize,
        )
        .await
        .is_err()
    );
    let state =
        notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open_read_only(&f.state)
            .unwrap();
    assert!(state.cache_rebuild_pending().unwrap());
    drop(state);
    http.change(|source| source.unauthenticated = false);
    let recovered = IndexRuntime::recover_lexical_cache(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
        plan,
        OpenMode::Initialize,
    )
    .await
    .unwrap();
    assert!(
        !recovered
            .chunks()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .is_empty()
    );
    drop(recovered);
    let mut scope = authority();
    scope.exclusions.page_ids.insert(PAGE.into());
    let changed =
        LexicalCachePlan::verified(Arc::new(dictionary(&[("cinnamon", "kanel")]))).unwrap();
    let scoped = IndexRuntime::recover_lexical_cache(
        &f.settings,
        &f.state,
        scope,
        http.client(),
        f.provider.clone(),
        changed,
        OpenMode::ReadOnly,
    )
    .await
    .unwrap();
    assert!(scoped.chunks().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(!scoped.coordinator.state().cache_rebuild_pending().unwrap());
    assert!(scoped.chunks().search_snapshot().await.is_ok());
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
    let http = start_notion().await;
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
    assert_eq!(dry.json["drift"]["added"], 3);
    assert_eq!(dry.json["complete"], true);
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 3); // The walk reads each of the 3 pages once.
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
    assert_eq!(before, f.snapshot());
    let applied = f.command(&http, &["--reconcile", "apply"]).await;
    counts(&applied, 3, 0, 0);
    assert_eq!(applied.json["scanned"], 3);
    let bodies = http.body_reads.load(Ordering::SeqCst);
    let embeds = f.provider.count.load(Ordering::SeqCst);
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 0, 0, 3);
    assert!(http.body_reads.load(Ordering::SeqCst) > bodies);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);
    http.change(|m| {
        m.title = "New metadata".into();
        m.revision = "2026-10-11T12:00:00Z".into();
        m.markdown.push_str("Changed oregano.");
    });
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 0, 2);
    http.change(|m| {
        m.hide_page = true;
        m.archived = true;
    });
    // Removing PAGE's child tag edits the root; OTHER keeps its identity.
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 1, 1);
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
async fn missing_paths_and_incompatible_authority_fail_before_mutation() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    for args in [&["--reconcile", "dry-run"][..]] {
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
        Command::Apply,
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
async fn index_failures_stop_the_sync_and_a_busy_writer_does_not_consume_work() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = runtime.coordinator.state().acquire_lease(now, 60).unwrap();
    let before = f.snapshot();
    let busy = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(busy.exit, 4);
    assert_eq!(before, f.snapshot());
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
    drop(runtime);
    let db = rusqlite::Connection::open(&f.state).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_text_ack BEFORE UPDATE OF status ON index_commit_receipts
        WHEN NEW.status = 1 BEGIN SELECT RAISE(ABORT,'fixture'); END;",
    )
    .unwrap();
    let failed = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(failed.exit, 3);
    // An index failure stops the sync; nothing partial is acknowledged.
    assert_eq!(failed.json["status"], "work_failure");
    db.execute_batch("DROP TRIGGER fail_text_ack;").unwrap();
    drop(db);
    for page in [PAGE, ROOT, OTHER] {
        let result = f.command(&http, &["--reconcile", "retry", page]).await;
        assert!(result.exit == 0 || result.exit == 3, "{:?}", result.json);
    }
    // A page the inventory cannot place is not synced: that is not success.
    let unknown = f
        .command(
            &http,
            &[
                "--reconcile",
                "retry",
                "99999999-9999-9999-9999-999999999999",
            ],
        )
        .await;
    assert_eq!(unknown.exit, 3, "{}", unknown.json);
    // A normal full sync recovers every page the failed one left behind.
    let done = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(done.exit, 0, "{}", done.json);
    assert_eq!(
        done.json["changed"].as_u64().unwrap() + done.json["unchanged"].as_u64().unwrap(),
        3
    );
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    assert!(out_of_sync(&runtime).is_empty());
}

#[tokio::test]
async fn cancellation_leaves_no_partial_state_and_a_vanished_page_is_removed_by_the_next_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let commit = Arc::new(RequestPause::default());
    commit.armed.store(true, Ordering::SeqCst);
    pause_owned_commits(&f.settings.index, commit.clone());
    let stop = CancellationToken::new();
    let pending = f.command_cancel(&http, &["--reconcile", "apply"], stop.clone());
    let cancel = async {
        commit.started.notified().await;
        stop.cancel();
    };
    let (out, _) = tokio::join!(pending, cancel);
    assert_eq!(out.exit, 5);
    commit.proceed.notify_one();
    // Independent connections and runtime are rebuilt by each command invocation.
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
    http.change(|m| {
        m.hide_page = true;
        m.inaccessible_page = true;
    });
    // The complete walk is the evidence: a page it no longer reaches is removed.
    let result = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(result.exit, 0, "{}", result.json);
    assert_eq!(result.json["deleted"], 1, "{}", result.json);
    let report = result.json.to_string();
    assert!(!report.contains("fixture-credential"));
    assert!(!report.contains("Saffron"));
}

#[tokio::test]
async fn timed_out_dry_run_leaves_state_unchanged_and_has_a_distinct_failure_exit() {
    let http = start_notion().await;
    let mut f = Fixture::new(&http).await;
    f.settings.inventory_timeout_seconds = 1;
    http.discovery_pause.armed.store(true, Ordering::SeqCst);
    let before = f.snapshot();
    let result = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(result.exit, 3);
    assert_eq!(result.json["status"], "timeout");
    assert_eq!(before, f.snapshot());
    // The walk may read Markdown before the paused metadata read; nothing is written.
    assert!(http.body_reads.load(Ordering::SeqCst) <= 3);
    http.discovery_pause.proceed.notify_one();
}

#[tokio::test]
async fn read_only_commands_reject_external_wal_before_creating_sqlite_auxiliaries() {
    let http = start_notion().await;
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
    for args in [&["--reconcile", "dry-run"][..]] {
        let out = f.command(&http, args).await;
        assert_eq!(out.exit, 2);
        assert_eq!(out.json["status"], "index_state_configuration");
        assert_eq!(before, f.snapshot());
        assert!(!wal.exists());
        assert!(!shm.exists());
    }
    assert!(
        IndexRuntime::recover_for_reads(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(before, f.snapshot());
    assert!(!wal.exists());
    assert!(!shm.exists());
    assert_eq!(http.body_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

/// After a crash mid-transaction the state file holds uncommitted pages and a
/// hot rollback journal. Read-only startup must roll it back, not fail forever.
#[tokio::test]
async fn read_only_startup_rolls_back_a_hot_journal_left_by_a_crash() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
    let journal = simulate_hot_rollback_journal(&f.state);
    let runtime = IndexRuntime::recover_for_reads(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .expect("read-only startup recovers a hot journal");
    assert_eq!(runtime.coordinator.owned_page_ids().unwrap().len(), 3);
    assert!(!journal.exists());
    let db = rusqlite::Connection::open(&f.state).unwrap();
    let leftover: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='uncommitted_crash'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(leftover, 0, "the crashed transaction is rolled back");
    let check: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}

fn simulate_hot_rollback_journal(path: &std::path::Path) -> std::path::PathBuf {
    let journal = std::path::PathBuf::from(format!("{}-journal", path.display()));
    let crashed = path.with_extension("crashed");
    let crashed_journal = path.with_extension("crashed-journal");
    {
        let db = rusqlite::Connection::open(&path).unwrap();
        // A tiny cache spills uncommitted pages into the database file.
        db.execute_batch(
            "PRAGMA cache_size=1; BEGIN;
             CREATE TABLE uncommitted_crash(x);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<200)
             INSERT INTO uncommitted_crash SELECT randomblob(4000) FROM n;",
        )
        .unwrap();
        assert!(fs::metadata(&journal).unwrap().len() > 0);
        // Capture the files as a crash would leave them.
        fs::copy(&path, &crashed).unwrap();
        fs::copy(&journal, &crashed_journal).unwrap();
    }
    fs::rename(&crashed, &path).unwrap();
    fs::rename(&crashed_journal, &journal).unwrap();
    journal
}

#[tokio::test]
async fn both_writers_recover_an_interrupted_semantic_queue_without_refetching_pages() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
    let queue = f.settings.index.join("semantic-projection.sqlite");
    let reads = http.body_reads.load(Ordering::SeqCst);
    let embeds = f.provider.count.load(Ordering::SeqCst);

    let journal = simulate_hot_rollback_journal(&queue);
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(!journal.exists());
    assert!(runtime.chunks().semantic_status().await.unwrap().0 > 0);
    assert_eq!(http.body_reads.load(Ordering::SeqCst), reads);
    drop(runtime);

    let journal = simulate_hot_rollback_journal(&queue);
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 0, 0, 3);
    assert!(!journal.exists());
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);
}

#[tokio::test]
async fn read_only_commands_reject_previous_schema_without_migration_or_auxiliary_creation() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let db = rusqlite::Connection::open(&f.state).unwrap();
    db.execute_batch(include_str!(
        "../../../retrieval/tests/it/fixtures/remove_source_cache_schema.sql"
    ))
    .unwrap();
    db.execute_batch(include_str!(
        "../../../retrieval/tests/it/fixtures/remove_schedule_schema.sql"
    ))
    .unwrap();
    drop(db);
    let before = f.snapshot();
    for args in [["--reconcile", "dry-run"].as_slice()] {
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
    scheduled_server_budgets(f, source, timeout, timeout, attempts, webhook_enabled).await
}

async fn scheduled_server_budgets(
    f: &Fixture,
    source: &HttpNotion,
    timeout: std::time::Duration,
    inventory_timeout: std::time::Duration,
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
    let index = Arc::new(f.open(source, OpenMode::ExistingWrite).await);
    let worker = Arc::new(Worker {
        webhook_enabled,
        state: index.coordinator.state().clone(),
        coordinator: index.coordinator.clone(),
        index: index.clone(),
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
            inventory_timeout,
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
    use notion_knowledge_server::webhook_worker::runtime::now_ms;
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    let finished = await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    let counts = worker.state.inventory_counts().unwrap();
    assert_eq!((counts.total, counts.indexed), (3, 3));
    let last = worker.state.last_sync().unwrap().unwrap();
    assert!(last.complete && !last.incremental);
    assert_eq!(last.drift.added, 3);
    assert!(finished.next_deadline >= now_ms() / 1000 + 58);
    assert!(
        worker
            .coordinator
            .last_successful_reconciliation()
            .unwrap()
            .is_some()
    );
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if let Ok((vectors, backlog)) = worker.index.chunks().semantic_status().await
                && vectors > 0
                && backlog.queued == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    server.await.unwrap().unwrap();
    let bodies = http.body_reads.load(Ordering::SeqCst);
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    let requests = http.requests().len();
    // A restart inside the interval only catches up through search.
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !worker.state.last_sync().unwrap().unwrap().incremental {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.coordinator.schedule("periodic").unwrap(), finished);
    assert!(
        http.requests()[requests..]
            .iter()
            .all(|r| r == "/v1/search")
    );
    assert_eq!(http.body_reads.load(Ordering::SeqCst), bodies);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    stop.cancel();
    server.await.unwrap().unwrap();
    make_schedule_due(&worker);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 2 && s.active_run.is_none()).await;
    // The unchanged cycle walks the tree again but embeds nothing.
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

/// Make the next scheduled cycle due now. Requires quiescent workers.
fn make_schedule_due(worker: &notion_knowledge_server::webhook_worker::runtime::Worker) {
    use notion_knowledge_retrieval::schedule::{SchedulePolicy, ScheduleReconfiguration};
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
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
}

#[tokio::test]
async fn drift_walk_detects_move_trash_revoke_and_add_in_one_pass() {
    let http = start_notion().await;
    let extra = |title: &str| ExtraPage {
        title: title.into(),
        visible: true,
        archived: false,
    };
    const A: &str = "aaaaaaaa-0000-0000-0000-00000000000a";
    const B: &str = "bbbbbbbb-0000-0000-0000-00000000000b";
    http.change(|m| {
        m.extra.insert(A.into(), extra("Trashed later"));
    });
    let f = Fixture::new(&http).await;
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
    http.change(|m| {
        m.extra.insert(B.into(), extra("Added later"));
        m.extra.get_mut(A).unwrap().archived = true;
        // Moved out of the root: no longer tagged, parented elsewhere.
        m.hide_page = true;
        m.parent = FOREIGN.into();
        m.inaccessible_other = true;
    });
    make_schedule_due(&worker);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 2 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
    let last = worker.state.last_sync().unwrap().unwrap();
    assert!(last.complete);
    assert_eq!(
        (last.drift.added, last.drift.trashed, last.drift.removed),
        (1, 1, 1)
    );
    // OTHER answers 404 for the first time: its removal waits one walk.
    assert_eq!(last.held, 1);
    let indexed = indexed_pages(&worker.coordinator);
    assert_eq!(
        indexed,
        [ROOT, OTHER, B].map(String::from).into_iter().collect()
    );
    assert!(worker.state.inventory_entry(PAGE).unwrap().is_none());
    let status = local_status(&f).await;
    let inventory = status["inventory"].clone();
    assert_eq!(inventory["walk_complete"], true);
    assert_eq!(inventory["held"], 1);
    assert_eq!(
        inventory["total"],
        ["indexed", "skipped", "failed", "pending"]
            .iter()
            .map(|k| inventory[k].as_u64().unwrap())
            .sum::<u64>()
    );
    assert!(
        inventory["out_of_sync"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["page_id"] == OTHER && p["outcome"] == "skipped:inaccessible")
    );
    let body = status.to_string();
    for title in ["Root", "First", "Separate", "Trashed later", "Added later"] {
        assert!(!body.contains(title), "{title}");
    }
    // The next walk confirms the revocation and removes the held page.
    make_schedule_due(&worker);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 3 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(worker.state.last_sync().unwrap().unwrap().held, 0);
    let indexed = indexed_pages(&worker.coordinator);
    assert_eq!(indexed, [ROOT, B].map(String::from).into_iter().collect());
}

/// The `knowledge_sync_status` payload over this fixture's state.
async fn local_status(f: &Fixture) -> Value {
    use notion_knowledge_core::sync_status::SyncStatusSource;
    let state = f.state.clone();
    let config = notion_knowledge_server::config::Config::from_lookup(|name| match name {
        "NK_NOTION_AUTH" => Some("integration".into()),
        "NOTION_TOKEN" => Some("fixture-credential".into()),
        "NK_WEBHOOK_MODE" => Some("verified".into()),
        "NK_WEBHOOK_WORKSPACE_ID" => Some(WORKSPACE.into()),
        "NK_WEBHOOK_SUBSCRIPTION_ID" => Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into()),
        "NK_WEBHOOK_INTEGRATION_ID" => Some("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into()),
        "NK_WEBHOOK_VERIFICATION_TOKEN" => Some("fixture-signing".into()),
        "NK_WEBHOOK_STATE_FILE" => Some(state.clone().into_os_string()),
        _ => None,
    })
    .unwrap();
    serde_json::to_value(
        notion_knowledge_server::sync_status::LocalStatus::from_config(&config)
            .status()
            .await,
    )
    .unwrap()
}

/// One sync through the shared runtime: totals and the indexed page ids.
async fn sync(
    f: &Fixture,
    http: &HttpNotion,
    mode: SyncMode,
) -> (SyncTotals, std::collections::BTreeSet<String>) {
    let runtime = f.open(http, OpenMode::ExistingWrite).await;
    let state = runtime.coordinator.state().clone();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let mut lease = state.acquire_lease(now, 90).unwrap();
    let totals = runtime
        .sync_from_walk(
            &PageDeadline {
                timeout_seconds: 60,
            },
            &mut lease,
            mode,
        )
        .await
        .unwrap();
    state.release_lease(&lease, now).unwrap();
    (totals, indexed_pages(&runtime.coordinator))
}

fn remove_text_projection(index: &std::path::Path) {
    fs::remove_dir_all(index.join("chunks_text.lance")).unwrap();
    fs::remove_file(index.join("semantic-projection.sqlite")).unwrap();
    fs::remove_file(index.join(".nk-text-projection-v1")).unwrap();
}

async fn drain_semantic(runtime: &IndexRuntime) {
    use notion_knowledge_server::webhook_worker::runtime::now_ms;
    let state = runtime.coordinator.state();
    let now = now_ms() / 1000;
    let lease = state.acquire_lease(now, 90).unwrap();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
    let table = runtime.chunks();
    while let Some(page) = table.next_semantic_retry(clock()).unwrap() {
        table
            .retry_semantic(
                &page,
                runtime.provider.as_ref(),
                lease.clone(),
                clock.clone(),
            )
            .await
            .unwrap();
    }
    state.release_lease(&lease, clock()).unwrap();
}

/// Pages with live (not removed) index content.
fn indexed_pages(
    coordinator: &notion_knowledge_retrieval::commit::IndexCommitCoordinator,
) -> std::collections::BTreeSet<String> {
    use notion_knowledge_core::sync_state::SyncStateStore;
    coordinator
        .owned_page_ids()
        .unwrap()
        .into_iter()
        .filter(|page| {
            coordinator
                .state()
                .page_state(page)
                .unwrap()
                .is_some_and(|state| !state.is_tombstone())
        })
        .collect()
}

fn new_requests(http: &HttpNotion, before: usize) -> Vec<String> {
    http.requests()[before..].to_vec()
}

#[tokio::test]
async fn unchanged_rerun_embeds_nothing() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let (first, _) = sync(&f, &http, SyncMode::Full).await;
    assert_eq!(first.indexed, 3);
    let embedded = f.provider.count.load(Ordering::SeqCst);
    let (second, _) = sync(&f, &http, SyncMode::Full).await;
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embedded);
    assert_eq!((second.indexed, second.unchanged), (0, second.inventory));
}

/// Relation targets resolve through the walk inventory, which exists only when
/// the walk ends: the first sync must resolve them itself, and a second sync of
/// the same content must neither embed nor re-read a page for relations.
#[tokio::test]
async fn first_sync_resolves_a_relation_between_pages_it_walks() {
    use notion_knowledge_core::graph::{GraphEdgeStore, GraphTarget};
    let http = start_notion().await;
    http.change(|m| m.relation_name = Some("Related".into()));
    let f = Fixture::new(&http).await;
    let (first, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(indexed.contains(PAGE) && indexed.contains(OTHER));
    assert_eq!(first.failed, 0);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let relations: Vec<_> = runtime
        .coordinator
        .state()
        .edges_from(PAGE)
        .unwrap()
        .into_iter()
        .filter(|edge| edge.relation_type().starts_with("relation:"))
        .collect();
    assert_eq!(relations.len(), 1);
    assert!(matches!(
        relations[0].target(),
        GraphTarget::Page { page_id } if page_id == OTHER
    ));
    assert!(
        runtime
            .coordinator
            .current_relation_projection_complete(PAGE)
            .unwrap()
    );
    drain_semantic(&runtime).await;
    // Every chunk was embedded once: the resolving pass embedded nothing.
    let embedded = f.provider.count.load(Ordering::SeqCst);
    let mut chunks = 0;
    for page in &indexed {
        chunks += runtime.chunks().page_chunks(page).await.unwrap().len();
    }
    assert_eq!(embedded, chunks);
    let before = http.requests().len();
    let (second, _) = sync(&f, &http, SyncMode::Full).await;
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embedded);
    assert_eq!((second.indexed, second.unchanged), (0, second.inventory));
    // The walk's own read only: no pass, since nothing is left unresolved.
    assert_eq!(
        new_requests(&http, before)
            .iter()
            .filter(|r| r.ends_with(&format!("{PAGE}/markdown")))
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_moved_out_between_read_and_commit_is_removed_on_next_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let commits = Arc::new(RequestPause::default());
    pause_owned_commits(&f.settings.index, commits.clone());
    commits.armed.store(true, Ordering::SeqCst);
    let moved = async {
        commits.started.notified().await;
        http.change(|m| {
            m.hide_page = true;
            m.parent = FOREIGN.into();
        });
        commits.proceed.notify_one();
    };
    let ((_, indexed), ()) = tokio::join!(sync(&f, &http, SyncMode::Full), moved);
    // Nothing rechecks the page before its commit (accepted staleness).
    assert!(indexed.contains(PAGE));
    let (totals, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(!indexed.contains(PAGE));
    assert_eq!((totals.removed, totals.drift.removed), (1, 1));
    let state = f.open(&http, OpenMode::ExistingWrite).await;
    assert!(
        state
            .coordinator
            .state()
            .inventory_entry(PAGE)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn trashed_page_is_removed_after_one_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.archived = true);
    let (totals, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(!indexed.contains(PAGE));
    assert_eq!((totals.removed, totals.drift.trashed), (1, 1));
    let status = local_status(&f).await;
    assert!(
        status["inventory"]["out_of_sync"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["page_id"] == PAGE && p["outcome"] == "skipped:archived")
    );
}

#[tokio::test]
async fn unparseable_metadata_keeps_an_indexed_page_in_incremental_and_full_syncs() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| {
        m.revision = "2026-10-11T12:00:00Z".into();
        m.unknown_property_type = true;
    });
    // Doubt is not removal, in the hit path or the walk.
    let (_, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(indexed.contains(PAGE));
    let (totals, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(indexed.contains(PAGE));
    assert!(indexed.contains(OTHER));
    assert_eq!(totals.removed, 0);
    assert!(!totals.complete);
    let status = local_status(&f).await;
    assert!(
        status["inventory"]["out_of_sync"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["page_id"] == PAGE && p["outcome"] == "skipped:unreadable_metadata")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walk_removal_stalled_past_its_deadline_releases_the_index_guard() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.archived = true);
    // An incremental sync commits nothing for settled pages, so the only
    // owned commit is PAGE's removal.
    let commits = Arc::new(RequestPause::default());
    pause_owned_commits(&f.settings.index, commits.clone());
    commits.armed.store(true, Ordering::SeqCst);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let state = runtime.coordinator.state().clone();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let mut lease = state.acquire_lease(now, 90).unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        runtime.sync_from_walk(
            &PageDeadline { timeout_seconds: 1 },
            &mut lease,
            SyncMode::Incremental,
        ),
    )
    .await
    .expect("a stalled removal must give up at its deadline");
    assert_eq!(result.unwrap_err().reason, "cache sync removal timeout");
    // The pause was never released: the deadline poll freed the guard.
    let guard = std::fs::File::open(&f.settings.index).unwrap();
    guard.try_lock().unwrap();
    drop(guard);
    state.release_lease(&lease, now).unwrap();
    notion_knowledge_retrieval::test_hooks::clear_commit_pause(&f.settings.index);
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(!indexed.contains(PAGE));
    assert_eq!(totals.removed, 1);
}

#[tokio::test]
async fn revoked_subtree_is_held_for_one_walk_then_removed() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.inaccessible_page = true);
    let (totals, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(totals.complete && indexed.contains(PAGE));
    assert_eq!((totals.held, totals.removed), (1, 0));
    let (totals, indexed) = sync(&f, &http, SyncMode::Full).await;
    assert!(!indexed.contains(PAGE));
    assert_eq!((totals.held, totals.removed), (0, 1));
}

#[tokio::test]
async fn incremental_with_no_changes_only_calls_search() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    let (before, embedded) = (
        http.requests().len(),
        f.provider.count.load(Ordering::SeqCst),
    );
    let (totals, _) = sync(&f, &http, SyncMode::Incremental).await;
    assert_eq!(new_requests(&http, before), ["/v1/search"]);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embedded);
    assert_eq!(
        (totals.indexed, totals.removed, totals.inventory),
        (0, 0, 0)
    );
}

#[tokio::test]
async fn cut_short_search_listing_recovers_missed_changes_with_a_full_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    // An edit the listing never reaches: it reports more results but its
    // cursor cannot be followed.
    http.change(|m| {
        m.repeat_search_cursor = true;
        m.revision = "2026-10-11T12:00:00Z".into();
        m.markdown.push_str("Missed basil.\n");
    });
    let before = http.requests().len();
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    let requests = new_requests(&http, before);
    assert!(
        requests.contains(&format!("/v1/pages/{PAGE}/markdown")),
        "{requests:?}"
    );
    assert!(totals.complete && totals.indexed >= 1);
    assert!(indexed.contains(PAGE));
    let state = f.open(&http, OpenMode::ExistingWrite).await;
    let last = state.coordinator.state().last_sync().unwrap().unwrap();
    assert!(!last.incremental && last.complete);
    assert_eq!(local_status(&f).await["inventory"]["walk_complete"], true);
    // The recovered edit is settled: a later complete search finds no work.
    http.change(|m| m.repeat_search_cursor = false);
    let (totals, _) = sync(&f, &http, SyncMode::Incremental).await;
    assert_eq!(totals.indexed, 0);
}

#[tokio::test]
async fn incremental_refreshes_only_changed_page() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| {
        m.revision = "2026-10-11T12:00:00Z".into();
        m.markdown.push_str("More basil.\n");
    });
    let before = http.requests().len();
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    let reads: Vec<_> = new_requests(&http, before)
        .into_iter()
        .filter(|r| r.starts_with("/v1/pages/"))
        .collect();
    assert_eq!(reads, [format!("/v1/pages/{PAGE}/markdown")]);
    assert_eq!((totals.indexed, totals.drift.changed), (1, 1));
    assert!(indexed.contains(PAGE));
}

#[tokio::test]
async fn incremental_indexes_new_page_under_an_inventoried_parent() {
    const NEW: &str = "cccccccc-0000-0000-0000-00000000000c";
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| {
        m.extra.insert(
            NEW.into(),
            ExtraPage {
                title: "New".into(),
                visible: true,
                archived: false,
            },
        );
    });
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(indexed.contains(NEW));
    assert_eq!(totals.drift.added, 1);
}

#[tokio::test]
async fn incremental_places_a_new_child_under_a_new_parent_in_one_search() {
    const NEW: &str = "cccccccc-0000-0000-0000-00000000000c";
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    // FOREIGN becomes a new child of a new root page; search lists it after
    // (newer than) NEW, so placement must see NEW from this same run.
    http.change(|m| {
        m.extra.insert(
            NEW.into(),
            ExtraPage {
                title: "New".into(),
                visible: true,
                archived: false,
            },
        );
        m.foreign_parent = NEW.into();
        m.foreign_revision = "2026-10-11T12:00:00Z".into();
    });
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(indexed.contains(NEW), "{totals:?}");
    // The fake serves no Markdown for FOREIGN, so it is placed and skipped.
    assert_eq!((totals.drift.added, totals.drift.unplaced), (2, 0));
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let entry = runtime
        .coordinator
        .state()
        .inventory_entry(FOREIGN)
        .unwrap()
        .unwrap();
    assert_eq!(
        entry.parent,
        notion_knowledge_core::inventory::InventoryParent::Page(NEW.into())
    );
}

/// NEW and its child FOREIGN are both edited inside one catch-up search;
/// `parent` makes NEW unindexable. FOREIGN must never be placed under it.
async fn child_of_unindexable_new_parent_is_not_placed(
    parent: impl FnOnce(&mut notion_fake::Model, &mut LifecycleScope),
) {
    let http = start_notion().await;
    let mut scope = authority();
    let mut model = notion_fake::Model::default();
    parent(&mut model, &mut scope);
    let f = Fixture::with_scope(&http, scope).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| {
        m.extra.insert(
            NEW_PARENT.into(),
            ExtraPage {
                title: "New".into(),
                visible: true,
                archived: false,
            },
        );
        m.foreign_parent = NEW_PARENT.into();
        m.foreign_revision = "2026-10-11T12:00:00Z".into();
        if let Some(page) = model.extra.get(NEW_PARENT) {
            m.extra.get_mut(NEW_PARENT).unwrap().archived = page.archived;
        }
    });
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let state = runtime.coordinator.state().clone();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let mut lease = state.acquire_lease(now, 90).unwrap();
    let totals = runtime
        .sync_from_walk(
            &PageDeadline {
                timeout_seconds: 60,
            },
            &mut lease,
            SyncMode::Incremental,
        )
        .await
        .unwrap();
    state.release_lease(&lease, now).unwrap();
    assert_eq!(totals.drift.unplaced, 1, "{totals:?}");
    assert!(state.inventory_entry(FOREIGN).unwrap().is_none());
    assert!(!indexed_pages(&runtime.coordinator).contains(FOREIGN));
}
const NEW_PARENT: &str = "cccccccc-0000-0000-0000-00000000000c";

#[tokio::test]
async fn incremental_never_places_children_of_a_descendants_excluded_new_page() {
    child_of_unindexable_new_parent_is_not_placed(|_, scope| {
        scope.exclusions.descendants_of.insert(NEW_PARENT.into());
    })
    .await;
}

#[tokio::test]
async fn incremental_never_places_children_of_an_excluded_new_page() {
    child_of_unindexable_new_parent_is_not_placed(|_, scope| {
        scope.exclusions.page_ids.insert(NEW_PARENT.into());
    })
    .await;
}

#[tokio::test]
async fn incremental_never_places_children_of_a_trashed_new_page() {
    child_of_unindexable_new_parent_is_not_placed(|m, _| {
        m.extra.insert(
            NEW_PARENT.into(),
            ExtraPage {
                title: "New".into(),
                visible: true,
                archived: true,
            },
        );
    })
    .await;
}

#[tokio::test]
async fn search_hit_with_unknown_parent_is_counted_not_indexed() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| {
        m.foreign_parent = "99999999-9999-9999-9999-999999999999".into();
        m.foreign_revision = "2026-10-11T12:00:00Z".into();
    });
    let before = http.requests().len();
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert_eq!(totals.drift.unplaced, 1);
    assert!(!indexed.contains(FOREIGN));
    assert!(
        !new_requests(&http, before)
            .iter()
            .any(|r| r.contains(FOREIGN))
    );
}

#[tokio::test]
async fn trashed_in_search_is_confirmed_before_removal() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.stale_search_trash = true);
    let before = http.requests().len();
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(new_requests(&http, before).contains(&format!("/v1/pages/{PAGE}")));
    assert!(indexed.contains(PAGE));
    assert_eq!(totals.removed, 0);
    // Search agrees with the page once it is really trashed.
    http.change(|m| m.archived = true);
    let (totals, indexed) = sync(&f, &http, SyncMode::Incremental).await;
    assert!(!indexed.contains(PAGE));
    assert_eq!((totals.removed, totals.drift.removed), (1, 1));
}

#[tokio::test]
async fn incremental_rereads_pages_edited_in_the_last_synced_minute() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // Notion timestamps are minute-granular: a second edit in this minute
    // keeps the same `last_edited_time`.
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let minute = chrono::DateTime::from_timestamp(now, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:00.000Z")
        .to_string();
    http.change(|m| m.revision = minute);
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.markdown.push_str("Same minute.\n"));
    let (totals, _) = sync(&f, &http, SyncMode::Incremental).await;
    assert_eq!(totals.indexed, 1);
}

#[tokio::test]
async fn single_page_refresh_reads_one_page_through_the_parent_map() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    sync(&f, &http, SyncMode::Full).await;
    http.change(|m| m.markdown.push_str("Callback edit.\n"));
    let before = http.requests().len();
    let (totals, _) = sync(&f, &http, SyncMode::Page(PageId(PAGE.into()))).await;
    assert_eq!(
        new_requests(&http, before),
        [
            format!("/v1/pages/{PAGE}"),
            format!("/v1/pages/{PAGE}/markdown")
        ]
    );
    assert_eq!(totals.indexed, 1);
    // An unplaced page is counted and left to the next walk.
    http.change(|m| m.foreign_parent = "99999999-9999-9999-9999-999999999999".into());
    let (totals, indexed) = sync(&f, &http, SyncMode::Page(PageId(FOREIGN.into()))).await;
    assert_eq!(totals.drift.unplaced, 1);
    assert!(!indexed.contains(FOREIGN));
}

#[tokio::test]
async fn actual_scheduled_http_timeout_exhaustion_is_inspectable_and_explicit_abandon_preserves_history()
 {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleStatus};
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // Stall the source so the run deadline, not run speed, ends the attempt:
    // without search, the walk must read PAGE's metadata itself.
    http.change(|m| m.search_unavailable = true);
    http.pause.armed.store(true, Ordering::SeqCst);
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(5)).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        http.pause.started.notified(),
    )
    .await
    .unwrap();
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
    http.change(|m| m.search_unavailable = false);
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
    // Schedule completion can precede bounded search maintenance. Drain the
    // serving worker before taking an inspection lease ourselves.
    stop.cancel();
    server.await.unwrap().unwrap();
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
}

#[tokio::test]
async fn actual_scheduled_http_rejected_credential_holds_the_cycle_without_consuming_attempts() {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleHold, ScheduleStatus};
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.change(|m| m.unauthenticated = true);
    // One attempt: any consumed attempt would exhaust the schedule.
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    let events = || {
        worker
            .coordinator
            .schedule_events("periodic", 0, 100)
            .unwrap()
    };
    let held = await_schedule(&worker, |s| {
        s.active_run.is_some()
            && events()
                .iter()
                .filter(|e| e.kind == ScheduleEventKind::Cancelled)
                .count()
                >= 2
    })
    .await;
    // Held past more than one backoff ceiling: still no attempt consumed.
    assert_eq!((held.status, held.attempts), (ScheduleStatus::Ready, 0));
    // The operator sees why the cycle is held, with a fixed reason.
    assert_eq!(held.hold, Some(ScheduleHold::Unauthenticated));
    let status = f
        .command(&http, &["--reconcile", "schedule", "status"])
        .await;
    assert_eq!(status.exit, 0);
    assert_eq!(status.json["hold"], "held_unauthenticated");
    assert_eq!(
        status.json["hold_reason"],
        "source rejected the credential; the scheduled sync retries once per maximum backoff until the credential is fixed"
    );
    assert!(
        !events()
            .iter()
            .any(|e| e.kind == ScheduleEventKind::RunFailure)
    );
    assert!(worker.state.inventory_counts().unwrap().indexed == 0);
    // Once the credential is accepted again, the same cycle completes.
    http.change(|m| m.unauthenticated = false);
    let done = await_schedule(&worker, |s| s.active_run.is_none()).await;
    assert_eq!((done.cycle, done.attempts, done.hold), (1, 0, None));
    assert_eq!(worker.state.inventory_counts().unwrap().indexed, 3);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn actual_scheduled_http_transient_source_attempts_back_off_then_exhaust_without_spinning() {
    use notion_knowledge_retrieval::schedule::{ScheduleEventKind, ScheduleStatus};
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // Stall the source during the walk so its deadline determines the retry
    // path: without search, the walk reads PAGE's metadata itself.
    http.change(|m| m.search_unavailable = true);
    http.pause.armed.store(true, Ordering::SeqCst);
    let (worker, stop, server) = scheduled_server_budgets(
        &f,
        &http,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(1),
        2,
        false,
    )
    .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        http.pause.started.notified(),
    )
    .await
    .unwrap();
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
    let http = start_notion().await;
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 4);
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
    // Drain owned workers before asserting stable graph and alias projections.
    resumed_stop.cancel();
    resumed_server.await.unwrap().unwrap();
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
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 0, 2);
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
    let http = start_notion().await;
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
async fn explicit_relation_rebuild_recommits_projections_and_preserves_checkpoints() {
    use notion_knowledge_core::{
        graph::{GraphEdge, GraphEdgeStore, GraphTarget},
        sync_state::SyncStateStore,
    };
    let http = start_notion().await;
    http.change(|m| {
        m.markdown = format!("[root](https://www.notion.so/{ROOT})");
        m.relation_name = Some("Ingredients".into());
    });
    let f = Fixture::new(&http).await;
    f.seed_inventory(&http).await;
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
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
        &f.command(&http, &["--reconcile", "rebuild-relations"])
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
        runtime.coordinator.state().page_state(PAGE).unwrap(),
        checkpoint
    );
}

/// The exact installed runtime used by webhook/manual/periodic callers owns
/// SQLite aliases and current configuration across real index commits/reopens.
#[tokio::test]
async fn installed_aliases_preserve_ambiguity_explicit_sources_rebuild_and_restart() {
    use notion_knowledge_core::aliases::{AliasSources, PageAlias, PageAliasStore};
    let http = start_notion().await;
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
    let output = f.command(&http, &["--reconcile", "apply"]).await;
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
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
        f.command(&http, &["--reconcile", "rebuild-aliases"])
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
    let runtime = f.open(&http, OpenMode::ReadOnly).await;
    assert!(runtime.lookup_alias("Sundet").unwrap().is_empty());
    assert!(runtime.lookup_alias("Öresund").unwrap().is_empty());
    drop(runtime);
    http.change(|m| {
        m.alias_names = vec!["Äpple".into(), "Apple".into()];
    });
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
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
    assert_eq!(f.command(&http, &["--reconcile", "apply"]).await.exit, 0);
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
    let http = start_notion().await;
    http.change(|m| m.alias_names = vec!["Äpple".into(), "Apple".into()]);
    let mut f = Fixture::new(&http).await;
    f.settings.alias_sources = AliasSources::new(false, vec!["names".into()]).unwrap();
    let apply = |runtime: IndexRuntime, id: &'static str| async move {
        let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
        let lease = runtime.coordinator.state().acquire_lease(now, 120).unwrap();
        let result = runtime
            .handler
            .refresh_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
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
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
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
        table.search_snapshot().await.is_ok(),
        "authoritative reconciliation must prepare serving search indexes"
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
}

#[tokio::test]
async fn index_commands_refuse_missing_foreign_cancelled_and_busy_state() {
    use notion_knowledge_server::index_command::{
        Command as IndexCommand, runtime::execute as index,
    };
    let http = start_notion().await;
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
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    f.populate(&http, "seed-distance").await;
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
}

#[tokio::test]
async fn ordinary_reconciliation_repairs_missing_rows_without_a_source_revision_change() {
    use notion_knowledge_core::search::{
        LexicalQuery, LexicalSearch, SemanticQuery, SemanticSearch,
    };
    use notion_knowledge_core::sync_state::SyncStateStore;
    use notion_knowledge_retrieval::chunks::{
        GuardedChunkTable, LanceChunkTable, LanceSemanticSearch,
    };

    for remove_all in [false, true] {
        let http = start_notion().await;
        let f = Fixture::new(&http).await;
        counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
        let runtime = f.open(&http, OpenMode::ReadOnly).await;
        let checkpoint = runtime.coordinator.state().page_state(PAGE).unwrap();
        drop(runtime);
        let db = rusqlite::Connection::open(&f.state).unwrap();
        db.execute_batch(
            "CREATE TABLE external_write_ledger(id TEXT PRIMARY KEY, result TEXT);
             INSERT INTO external_write_ledger VALUES('already-written','done');
             INSERT INTO webhook_events(event_id) VALUES('already-admitted');",
        )
        .unwrap();
        db.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id,state) VALUES(?1,'done-write',?2,2)", [WORKSPACE, PAGE]).unwrap();
        drop(db);

        // Lose only derived rows; retain the operational checkpoint and source
        // revision. No writer is active during this deliberate fixture damage.
        let table =
            LanceChunkTable::open(&f.settings.index, "chunks", f.provider.metadata().clone())
                .await
                .unwrap();
        let mut original = table.page_chunks(PAGE).await.unwrap();
        original.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
        let retained = original
            .iter()
            .filter(|chunk| !remove_all && !chunk.text.contains("ravioli"))
            .cloned()
            .collect::<Vec<_>>();
        assert!(retained.len() < original.len());
        assert!(remove_all || !retained.is_empty());
        table
            .apply_page_diff(f.provider.as_ref(), PAGE, &retained)
            .await
            .unwrap();
        drop(table);
        let embeds = f.provider.count.load(Ordering::SeqCst);
        let dry = f.command(&http, &["--reconcile", "dry-run"]).await;
        assert_eq!(dry.exit, 0, "{dry:?}");
        assert_eq!(dry.json["scanned"], 3);
        assert_eq!(dry.json["drift"]["changed"], 0);
        assert_eq!(dry.json["drift"]["removed"], 0);
        counts(&f.command(&http, &["--reconcile", "apply"]).await, 0, 0, 3);
        assert_eq!(
            f.provider.count.load(Ordering::SeqCst) - embeds,
            original.len() - retained.len(),
            "only missing rows need embeddings"
        );
        let runtime = f.open(&http, OpenMode::ReadOnly).await;
        assert_eq!(
            runtime.coordinator.state().page_state(PAGE).unwrap(),
            checkpoint
        );
        let table = GuardedChunkTable::bind(
            &f.settings.index,
            "chunks",
            f.provider.metadata().clone(),
            runtime.coordinator.clone(),
        )
        .unwrap();
        let mut repaired = table.page_chunks(PAGE).await.unwrap();
        repaired.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
        assert_eq!(repaired, original);
        let snapshot = Arc::new(table.search_snapshot().await.unwrap());
        let lexical = LexicalQuery {
            query: "ravioli".into(),
            limit: 10,
            page_ids: Some(vec![PAGE.into()]),
            root_page_ids: Some(vec![ROOT.into()]),
            metadata: Default::default(),
        };
        let semantic_query = SemanticQuery {
            query: "ravioli".into(),
            limit: 10,
            page_ids: lexical.page_ids.clone(),
            root_page_ids: lexical.root_page_ids.clone(),
            metadata: Default::default(),
        };
        let semantic = LanceSemanticSearch::new(snapshot.clone(), f.provider.clone(), 1).unwrap();
        let lexical_hits = snapshot.search(lexical.clone()).await.unwrap();
        let semantic_hits = semantic.search(semantic_query.clone()).await.unwrap();
        assert!(!lexical_hits.is_empty());
        assert_eq!(semantic_hits.len(), original.len());
        for hit in lexical_hits.iter().chain(&semantic_hits) {
            let chunk = original
                .iter()
                .find(|c| c.chunk_id == hit.source.chunk_id)
                .unwrap();
            assert_eq!(hit.source.page_id, PAGE);
            assert_eq!(hit.source.title, "First");
            assert_eq!(hit.source.url, format!("https://www.notion.so/{PAGE}"));
            assert_eq!(hit.source.last_edited_time, "2026-10-10T12:00:00Z");
            assert_eq!(hit.source.heading_path, chunk.metadata.heading_path);
            assert_eq!(hit.text, chunk.text);
        }
        drop(semantic);
        drop(snapshot);
        drop(table);
        drop(runtime);
        let embeds = f.provider.count.load(Ordering::SeqCst);
        counts(&f.command(&http, &["--reconcile", "apply"]).await, 0, 0, 3);
        assert_eq!(f.provider.count.load(Ordering::SeqCst), embeds);

        let runtime = if remove_all {
            let mut scope = authority();
            scope.exclusions.page_ids.insert(PAGE.into());
            IndexRuntime::recover_for_reads(
                &f.settings,
                &f.state,
                scope,
                http.client(),
                f.provider.clone(),
            )
            .await
            .unwrap()
        } else {
            http.change(|m| {
                m.hide_page = true;
                m.archived = true;
            });
            // Removing PAGE's child tag edits the root; OTHER keeps its identity.
            counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 1, 1);
            f.open(&http, OpenMode::ReadOnly).await
        };
        let table = GuardedChunkTable::bind(
            &f.settings.index,
            "chunks",
            f.provider.metadata().clone(),
            runtime.coordinator,
        )
        .unwrap();
        assert!(table.page_chunks(PAGE).await.unwrap().is_empty());
        let snapshot = Arc::new(table.search_snapshot().await.unwrap());
        assert!(snapshot.search(lexical).await.unwrap().is_empty());
        let semantic = LanceSemanticSearch::new(snapshot, f.provider.clone(), 1).unwrap();
        assert!(semantic.search(semantic_query).await.unwrap().is_empty());
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
        // The HTTP fixture rejects every non-GET request throughout.
    }
}

#[tokio::test]
async fn lost_or_incompatible_cache_resyncs_without_replaying_external_effects() {
    use notion_knowledge_core::search::{LexicalQuery, LexicalSearch};
    use notion_knowledge_retrieval::chunks::GuardedChunkTable;
    for damage in ["deleted", "incompatible", "corrupt", "missing-index"] {
        let http = start_notion().await;
        let f = Fixture::new(&http).await;
        if damage == "missing-index" {
            f.populate(&http, "seed-cache").await;
        } else {
            counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
        }
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
            remove_text_projection(&f.settings.index);
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
        assert!(
            table.search_snapshot().await.is_ok(),
            "rebuilt cache must have usable search indexes"
        );
        drain_semantic(&recovered).await;
        let snapshot = table.search_snapshot().await.unwrap();
        assert!(
            !snapshot
                .vector_query(&[1.0, 0.0, 0.0], 3, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let query = LexicalQuery {
            query: "ravioli".into(),
            limit: 3,
            page_ids: None,
            root_page_ids: Some(vec![ROOT.into()]),
            metadata: Default::default(),
        };
        let hits = snapshot.search(query.clone()).await.unwrap();
        assert!(hits.iter().any(|hit| hit.source.page_id == PAGE));
        drop(snapshot);
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
        // Coordinator-owned index threads can outlive the sync call. Join
        // them (and require no typed failure) before reopening so the
        // restart check observes a quiescent cache.
        assert!(
            recovered
                .coordinator
                .drain_owned_outcomes()
                .await
                .unwrap()
                .is_empty()
        );
        drop(recovered);
        let reads = http.body_reads.load(Ordering::SeqCst);
        let _reopened = IndexRuntime::recover(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            provider.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            http.body_reads.load(Ordering::SeqCst),
            reads,
            "healthy restart must reuse the cache"
        );
        assert!(
            _reopened
                .coordinator
                .drain_owned_outcomes()
                .await
                .unwrap()
                .is_empty()
        );
        drop(_reopened);
        if damage == "missing-index" {
            let mut changed_scope = authority();
            changed_scope.exclusions.page_ids.insert(PAGE.into());
            let scoped = IndexRuntime::recover_for_reads(
                &f.settings,
                &f.state,
                changed_scope,
                http.client(),
                provider.clone(),
            )
            .await
            .unwrap();
            let table = GuardedChunkTable::bind(
                &f.settings.index,
                "chunks",
                provider.metadata().clone(),
                scoped.coordinator,
            )
            .unwrap();
            assert!(table.page_chunks(PAGE).await.unwrap().is_empty());
            assert!(
                table
                    .search_snapshot()
                    .await
                    .unwrap()
                    .search(query)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        // HttpNotion rejects every non-GET request, including writes/uploads.
    }
}

#[tokio::test]
async fn cache_sync_indexes_available_text_without_resolving_template_references() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.state.lock().unwrap().markdown = format!(
        "Available recipe text.\n<synced_block_reference url=\"https://notion.so/{PAGE}\">\n</synced_block_reference>"
    );
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    drain_semantic(&runtime).await;
    assert!(
        runtime
            .chunks()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .any(|c| c.text.contains("Available recipe text."))
    );
    assert!(
        !runtime
            .chunks()
            .search_snapshot()
            .await
            .unwrap()
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unreadable_page_does_not_block_embedded_search_and_next_sync_can_recover_it() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.state.lock().unwrap().unsupported_page_content = true;
    fs::remove_dir_all(&f.settings.index).unwrap();
    let embedded = f.provider.count.load(Ordering::SeqCst);
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        out_of_sync(&runtime),
        [(
            PAGE.to_owned(),
            notion_knowledge_core::inventory::InventoryOutcome::Failed(
                notion_knowledge_core::reconciliation::FailureClass::Source
            )
        )]
    );
    // Startup persists readable pages while discovering, once each.
    let inventory = runtime.coordinator.state().inventory_counts().unwrap();
    assert_eq!((inventory.indexed, inventory.pending), (2, 0));
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    drain_semantic(&runtime).await;
    assert!(runtime.chunks().page_chunks(PAGE).await.unwrap().is_empty());
    assert!(
        !runtime
            .chunks()
            .page_chunks(OTHER)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(f.provider.count.load(Ordering::SeqCst) > embedded);
    assert!(
        !runtime
            .chunks()
            .search_snapshot()
            .await
            .unwrap()
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
    drop(runtime);
    http.state.lock().unwrap().unsupported_page_content = false;
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 0, 2);
}

#[tokio::test]
async fn authentication_revocation_at_any_sync_stage_keeps_cache_unavailable() {
    // Revocation during the commit is no longer observed (no commit-time
    // source request); the next walk fails on the revoked credential.
    for after_embedding in [false] {
        let http = start_notion().await;
        let f = Fixture::new(&http).await;
        if after_embedding {
            *f.provider.boundary_pause.lock().unwrap() = Some(http.pause.clone());
        } else {
            http.state.lock().unwrap().unauthenticated_content = true;
        }
        fs::remove_dir_all(&f.settings.index).unwrap();
        let recover = IndexRuntime::recover(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone(),
        );
        let revoke = async {
            if after_embedding {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    http.pause.started.notified(),
                )
                .await
                .unwrap();
                http.state.lock().unwrap().unauthenticated_page_metadata = true;
                http.pause.proceed.notify_one();
            }
        };
        let (result, ()) = tokio::join!(recover, revoke);
        assert!(result.is_err());
        let runtime = f.open(&http, OpenMode::ReadOnly).await;
        assert!(runtime.coordinator.state().cache_rebuild_pending().unwrap());
        // A streaming attempt may fail before the final inventory exists.
        // In both orders, no unauthorized page is acknowledged and the cache
        // remains unavailable; an early failure needs no extra recovery ledger.
        assert!(runtime.chunks().page_chunks(PAGE).await.unwrap().is_empty());
        // A failed sync records no indexed outcome for the unauthorized page.
        assert!(
            runtime
                .coordinator
                .state()
                .inventory_counts()
                .unwrap()
                .indexed
                < 3
        );
    }
}

#[tokio::test]
async fn failed_cache_resync_stays_pending_and_next_start_retries_from_notion() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    http.change(|source| source.unauthenticated = true);
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
    http.change(|source| source.unauthenticated = false);
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
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 1, 0, 2);
}

#[tokio::test]
async fn empty_authorized_content_completes_resync_and_search_returns_no_hits() {
    use notion_knowledge_core::search::{LexicalQuery, LexicalSearch};
    use notion_knowledge_retrieval::chunks::GuardedChunkTable;
    let http = start_notion().await;
    http.state.lock().unwrap().empty_content = true;
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    let table = GuardedChunkTable::bind(
        &f.settings.index,
        "chunks",
        f.provider.metadata().clone(),
        runtime.coordinator,
    )
    .unwrap();
    let snapshot = table.search_snapshot().await.unwrap();
    assert_eq!(snapshot.count_rows().await.unwrap(), 0);
    assert!(
        snapshot
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        snapshot
            .search(LexicalQuery {
                query: "anything".into(),
                limit: 3,
                page_ids: None,
                root_page_ids: Some(vec![ROOT.into()]),
                metadata: Default::default()
            })
            .await
            .unwrap()
            .is_empty()
    );
    drop(snapshot);
    {
        let mut model = http.state.lock().unwrap();
        model.empty_content = false;
        model.revision = "2026-10-28T12:00:00Z".into();
        model.root_revision = "2026-10-28T12:00:00Z".into();
    }
    let update = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(update.exit, 0, "{:?}", update.json);
    // The same reader can search the first incremental content without restart.
    let snapshot = table.search_snapshot().await.unwrap();
    assert!(
        !snapshot
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let hits = snapshot
        .search(LexicalQuery {
            query: "ravioli".into(),
            limit: 3,
            page_ids: None,
            root_page_ids: Some(vec![ROOT.into()]),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    assert!(hits.iter().any(|hit| hit.source.page_id == PAGE));
}

#[tokio::test]
async fn manual_inventory_uses_its_budget_and_expires_without_source_mutations() {
    use std::time::Duration;
    let http = start_notion().await;
    let mut f = Fixture::new(&http).await;
    f.settings.timeout_seconds = 1;
    f.settings.inventory_timeout_seconds = 60;
    http.discovery_pause.armed.store(true, Ordering::SeqCst);
    let release = async {
        tokio::time::timeout(
            Duration::from_secs(10),
            http.discovery_pause.started.notified(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        http.discovery_pause.proceed.notify_one();
    };
    let (out, _) = tokio::join!(f.command(&http, &["--reconcile", "dry-run"]), release);
    assert_eq!(out.exit, 0, "{}", out.json);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);

    f.settings.inventory_timeout_seconds = 1;
    f.settings.timeout_seconds = 60;
    http.discovery_pause.armed.store(true, Ordering::SeqCst);
    let out = f.command(&http, &["--reconcile", "dry-run"]).await;
    assert_eq!(out.exit, 3);
    assert_eq!(out.json["status"], "timeout");
    http.discovery_pause.proceed.notify_one();
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn streaming_initial_sync_separates_discovery_and_page_budgets() {
    use std::time::Duration;
    let http = start_notion().await;
    let mut f = Fixture::new(&http).await;
    notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Info);
    // The page budget must exceed one real page commit even on a loaded
    // runner; only the discovery wait below is meant to outlast it.
    f.settings.timeout_seconds = 5;
    f.settings.inventory_timeout_seconds = 7200;
    http.discovery_pause.armed.store(true, Ordering::SeqCst);
    let source_and_page = async {
        tokio::time::timeout(
            Duration::from_secs(10),
            http.discovery_pause.started.notified(),
        )
        .await
        .unwrap();
        await_cached_page(&f.state, ROOT).await;
        // Discovery can outlast a page budget while earlier text is readable.
        tokio::time::sleep(Duration::from_millis(5100)).await;
        http.discovery_pause.proceed.notify_one();
    };
    let recovery = IndexRuntime::reset_and_resync(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    );
    let (result, _) = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::join!(recovery, source_and_page)
    })
    .await
    .unwrap();
    let runtime = result.unwrap();
    assert!(out_of_sync(&runtime).is_empty());
    let inventory = runtime.coordinator.state().inventory_counts().unwrap();
    assert_eq!((inventory.indexed, inventory.pending), (3, 0));
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
    drain_semantic(&runtime).await;
    assert!(f.provider.count.load(Ordering::SeqCst) > 0);
    assert_writer_available_during_inventory(&f.state);
}

#[tokio::test]
async fn periodic_sync_retries_unreadable_pages_on_the_next_normal_cycle() {
    use notion_knowledge_retrieval::schedule::{
        SchedulePolicy, ScheduleReconfiguration, ScheduleStatus,
    };
    use notion_knowledge_server::scheduled_reconciliation::{Settings, runtime::Worker};
    use notion_knowledge_server::webhook_worker::runtime::now_ms;
    use std::time::Duration;
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.state.lock().unwrap().unsupported_page_content = true;
    let worker = Worker {
        index: Arc::new(f.open(&http, OpenMode::ExistingWrite).await),
        settings: Settings {
            interval: 86400,
            attempts: 1,
            backoff: 1,
            maximum_backoff: 1,
        },
        poll: Duration::from_secs(1),
        timeout: Duration::from_secs(60),
        inventory_timeout: Duration::from_secs(60),
    };
    assert!(worker.tick(CancellationToken::new()).await.unwrap());
    drain_semantic(&worker.index).await;
    let coordinator = &worker.index.coordinator;
    let first = coordinator.schedule("periodic").unwrap();
    assert_eq!(first.status, ScheduleStatus::Ready);
    assert!(first.active_run.is_none());
    assert!(first.next_deadline >= now_ms() / 1000 + 86390);
    assert!(
        worker
            .index
            .chunks()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !worker
            .index
            .chunks()
            .search_snapshot()
            .await
            .unwrap()
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        coordinator
            .last_successful_reconciliation()
            .unwrap()
            .is_none()
    );
    // Advance the existing schedule explicitly; no day-long sleep or poll loop.
    http.state.lock().unwrap().unsupported_page_content = false;
    let now = now_ms() / 1000;
    let state = coordinator.state();
    let lease = state.acquire_lease(now, 30).unwrap();
    coordinator
        .reconfigure_schedule(
            &lease,
            now,
            "periodic",
            &ScheduleReconfiguration {
                previous_binding: coordinator.binding().clone(),
                policy: SchedulePolicy::new(86400, 1, 1, 1).unwrap(),
                next_deadline: now,
            },
        )
        .unwrap();
    state.release_lease(&lease, now).unwrap();
    assert!(worker.tick(CancellationToken::new()).await.unwrap());
    let second = coordinator.schedule("periodic").unwrap();
    assert_eq!(second.status, ScheduleStatus::Ready);
    assert!(second.active_run.is_none());
    assert!(
        !worker
            .index
            .chunks()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        coordinator
            .last_successful_reconciliation()
            .unwrap()
            .is_some()
    );
}

fn assert_writer_available_during_inventory(path: &std::path::Path) {
    let state = notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(path).unwrap();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = state
        .acquire_lease(now, 30)
        .expect("read-only inventory must not retain writer ownership");
    state.release_lease(&lease, now).unwrap();
}

async fn await_cached_page(path: &std::path::Path, page: &str) {
    use notion_knowledge_core::sync_state::SyncStateStore;
    use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if SqliteSyncStateStore::open_read_only(path)
                .ok()
                .and_then(|state| state.page_state(page).ok().flatten())
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_sync_publishes_text_before_discovery_finishes() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    http.discovery_pause.armed.store(true, Ordering::SeqCst);
    let settings = f.settings.clone();
    let state = f.state.clone();
    let source = http.client();
    let provider = f.provider.clone();
    let recovery = tokio::spawn(async move {
        IndexRuntime::recover(&settings, &state, authority(), source, provider).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        http.discovery_pause.started.notified(),
    )
    .await
    .unwrap();
    await_cached_page(&f.state, ROOT).await;
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
    assert!(
        !recovery.is_finished(),
        "serving readiness still requires the completed inventory"
    );
    http.discovery_pause.proceed.notify_one();
    let runtime = recovery.await.unwrap().unwrap();
    assert_eq!(runtime.coordinator.owned_page_ids().unwrap().len(), 3);
    assert!(!runtime.chunks().page_chunks(ROOT).await.unwrap().is_empty());
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
}

#[tokio::test]
async fn truncated_walk_read_fails_only_that_page_without_a_second_read() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Info);
    // Truncated for the walk's single read; the next walk retries it.
    http.state.lock().unwrap().unsupported_page_reads = 1;
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        out_of_sync(&runtime),
        [(
            PAGE.to_owned(),
            notion_knowledge_core::inventory::InventoryOutcome::Failed(
                notion_knowledge_core::reconciliation::FailureClass::Source
            )
        )]
    );
    assert_eq!(
        runtime
            .coordinator
            .state()
            .inventory_counts()
            .unwrap()
            .indexed,
        2
    );
    assert!(runtime.chunks().page_chunks(PAGE).await.unwrap().is_empty());
    assert_eq!(
        http.requests()
            .iter()
            .filter(|r| *r == &format!("/v1/pages/{PAGE}/markdown"))
            .count(),
        1
    );
}

#[tokio::test]
async fn a_relation_past_the_inline_limit_is_read_in_full_without_failing_the_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let targets: Vec<String> = (0..30)
        .map(|n| format!("aaaaaaaa-0000-0000-0000-{n:012}"))
        .collect();
    http.change(|m| {
        m.relation_name = Some("Related".into());
        m.relation_targets = targets.clone();
    });
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(out_of_sync(&runtime).is_empty());
    let state = runtime.coordinator.state();
    assert_eq!(state.inventory_counts().unwrap().indexed, 3);
    assert!(!state.cache_rebuild_pending().unwrap());
    let mut related: Vec<String> =
        notion_knowledge_core::graph::GraphEdgeStore::edges_from(&**state, PAGE)
            .unwrap()
            .iter()
            .filter(|edge| edge.relation_type().starts_with("relation:"))
            .map(|edge| match edge.target() {
                notion_knowledge_core::graph::GraphTarget::Page { page_id } => page_id.clone(),
                notion_knowledge_core::graph::GraphTarget::Unresolved { reference } => {
                    reference.clone()
                }
            })
            .collect();
    related.sort();
    assert_eq!(related, targets);
}

#[tokio::test]
async fn text_with_more_than_25_mentions_is_read_in_full_without_failing_the_walk() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let targets: Vec<String> = (0..30)
        .map(|n| format!("bbbbbbbb-0000-0000-0000-{n:012}"))
        .collect();
    http.change(|m| m.mention_targets = targets.clone());
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(out_of_sync(&runtime).is_empty());
    let state = runtime.coordinator.state();
    assert_eq!(state.inventory_counts().unwrap().indexed, 3);
    let page = http.client().fetch_page(PAGE).await.unwrap();
    let expected: String = targets.iter().map(|id| format!("@{id} ")).collect();
    assert_eq!(
        page.properties["mentions-stable"],
        notion_knowledge_core::indexed::PropertyValue::Text(expected)
    );
}

#[tokio::test]
async fn unreadable_text_past_the_mention_limit_fails_only_its_page() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.change(|m| {
        m.mention_targets = (0..30)
            .map(|n| format!("bbbbbbbb-0000-0000-0000-{n:012}"))
            .collect();
        m.unreadable_mention_items = true;
    });
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        out_of_sync(&runtime),
        [(
            PAGE.to_owned(),
            notion_knowledge_core::inventory::InventoryOutcome::Failed(
                notion_knowledge_core::reconciliation::FailureClass::Source
            )
        )]
    );
    assert!(
        !runtime
            .chunks()
            .page_chunks(OTHER)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(runtime.chunks().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_unreadable_long_relation_fails_only_its_page() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.change(|m| {
        m.relation_name = Some("Related".into());
        m.relation_targets = (0..30)
            .map(|n| format!("aaaaaaaa-0000-0000-0000-{n:012}"))
            .collect();
        m.unreadable_relation_items = true;
    });
    fs::remove_dir_all(&f.settings.index).unwrap();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        out_of_sync(&runtime),
        [(
            PAGE.to_owned(),
            notion_knowledge_core::inventory::InventoryOutcome::Failed(
                notion_knowledge_core::reconciliation::FailureClass::Source
            )
        )]
    );
    assert_eq!(
        runtime
            .coordinator
            .state()
            .inventory_counts()
            .unwrap()
            .indexed,
        2
    );
    assert!(
        !runtime
            .chunks()
            .page_chunks(OTHER)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(runtime.chunks().page_chunks(PAGE).await.unwrap().is_empty());
}

#[tokio::test]
async fn streaming_progress_reports_actual_commits_and_failed_pages() {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "reconciliation_command::truncated_walk_read_fails_only_that_page_without_a_second_read",
                "--nocapture",
            ])
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("fixture-credential"));
    let events: Vec<serde_json::Value> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let ready = events
        .iter()
        .find(|event| {
            event["operation"] == "cache_sync"
                && event["phase"] == "preparing_search"
                && event["status"] == "completed"
        })
        .unwrap();
    // One line per consumed page, each with its own timings (#662).
    let pages: Vec<_> = events
        .iter()
        .filter(|event| {
            event["phase"] == "indexing"
                && event["status"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("page_"))
        })
        .collect();
    let status = |name: &str| pages.iter().filter(|e| e["status"] == name).count();
    assert_eq!(
        (
            status("page_committed"),
            status("page_skipped"),
            pages.len()
        ),
        (2, 1, 3)
    );
    pages
        .iter()
        .for_each(|page| assert_page_line_is_content_free(page));
    assert!(
        pages
            .iter()
            .all(|page| page["walk_api_ops"].as_u64().unwrap() >= 1)
    );
    let finished = events
        .iter()
        .find(|e| e["phase"] == "indexing" && e["status"] == "completed")
        .unwrap();
    let totals = &finished["page_totals"];
    assert_eq!(totals["pages"], 3);
    let sum = |name: &str| pages.iter().map(|p| p[name].as_u64().unwrap()).sum::<u64>();
    assert_eq!(totals["wall_ms"].as_u64().unwrap(), sum("wall_ms"));
    assert_eq!(
        totals["walk_api_ops"].as_u64().unwrap(),
        sum("walk_api_ops")
    );
    let counts = &ready["sync_progress"];
    assert_eq!(counts["discovered_pages"], 3);
    assert_eq!(counts["total_pages"], 3);
    assert_eq!(counts["committed_pages"], 2);
    assert_eq!(counts["skipped_pages"], 1);
    assert_eq!(counts["embedded_chunks"], 0);
    assert!(totals["text_chunks"].as_u64().unwrap() > 0);
    for event in events
        .iter()
        .filter(|event| event["operation"] == "notion_api" || event["operation"] == "index_commit")
    {
        assert!(event["sync_progress"].is_object(), "{event}");
    }
}

#[tokio::test]
async fn readable_html_siblings_and_many_chunk_pages_use_bounded_embedding_requests() {
    for markdown in [
        (0..60).map(|n| format!("<page url=\"https://example.invalid/{n}\">Independent readable source {n}</page>\n")).collect::<String>(),
        (0..40).map(|n| format!("# Section {n}\n\nIndependent readable section {n}.\n\n")).collect(),
    ] {
        let http = start_notion().await;
        let f = Fixture::new(&http).await;
        http.state.lock().unwrap().markdown = markdown;
        f.provider.input_limit_bytes.store(2048, Ordering::SeqCst);
        fs::remove_dir_all(&f.settings.index).unwrap();
        let runtime = IndexRuntime::recover(&f.settings, &f.state, authority(), http.client(), f.provider.clone()).await.unwrap();
        let chunks = runtime.chunks().page_chunks(PAGE).await.unwrap();
        assert!(chunks.len() >= 4);
        assert!(chunks.iter().all(|chunk| chunk.metadata.page_id == PAGE && chunk.metadata.source.root_page_id == ROOT));
        assert!(out_of_sync(&runtime).is_empty());
        assert_eq!(runtime.coordinator.state().inventory_counts().unwrap().indexed, 3);
        assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    }
}

#[tokio::test]
async fn oversized_table_sections_commit_losslessly_and_retry_as_unchanged() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let rows: Vec<_> = (0..4)
        .map(|n| {
            format!(
                "<tr><td>Row {n}</td><td>{}</td></tr>",
                "test values ".repeat(55)
            )
        })
        .collect();
    let bullets: Vec<_> = (0..5)
        .map(|n| {
            format!(
                "- [Related entry {n}](https://example.invalid/entry/{n}) {}",
                "context ".repeat(24)
            )
        })
        .collect();
    let markdown = format!(
        "# Investigation\n\n<table>\n{}\n</table>\n{}\n",
        rows.join("\n"),
        bullets.join("\n")
    );
    http.change(|m| m.markdown = markdown.clone());
    f.provider.input_limit_bytes.store(1024, Ordering::SeqCst);
    let result = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(result.exit, 0, "{:?}", result.json);
    assert_eq!(result.json["semantic_projection"], "deferred");
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let canonical = runtime.chunks().page_chunks(PAGE).await.unwrap();
    let original = runtime
        .chunks()
        .search_snapshot()
        .await
        .unwrap()
        .page_chunks(PAGE)
        .await
        .unwrap();
    let split = original
        .iter()
        .find(|part| {
            !canonical
                .iter()
                .any(|chunk| chunk.chunk_id == part.chunk_id)
        })
        .expect("semantic table split has a distinct source anchor");
    use notion_knowledge_core::source::{SourceExpandQuery, SourceExpansion, StableSourceRef};
    let sources = runtime
        .chunks()
        .expand(SourceExpandQuery {
            refs: vec![
                StableSourceRef::Page(PAGE.into()),
                StableSourceRef::Chunk(split.chunk_id.clone()),
            ],
            max_chars: 6000,
            root_page_ids: vec![ROOT.into()],
        })
        .await
        .unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[1].provenance.page_id, PAGE);
    assert!(sources[1].text.contains("Investigation"));
    assert!(original.len() > 4);
    let mut source_order: Vec<_> = original
        .iter()
        .map(|chunk| (markdown.find(&chunk.text).unwrap(), chunk.text.as_str()))
        .collect();
    source_order.sort_unstable();
    assert_eq!(
        source_order
            .into_iter()
            .map(|(_, text)| text)
            .collect::<String>(),
        markdown
    );
    assert!(original.iter().all(|chunk| chunk.text.len() <= 1024
        && chunk.metadata.page_id == PAGE
        && chunk.metadata.source.root_page_id == ROOT
        && chunk.metadata.heading_path == ["Investigation"]));
    for row in rows {
        assert!(original.iter().any(|chunk| chunk.text.contains(&row)));
    }
    for n in 0..bullets.len() {
        let link = format!("[Related entry {n}](https://example.invalid/entry/{n})");
        assert!(original.iter().any(|chunk| chunk.text.contains(&link)));
    }
    assert!(out_of_sync(&runtime).is_empty());
    drop(runtime);

    // A compatible v1 installation has this semantic table but no separate
    // text projection or marker. Preserve the exact historical table split
    // and its existing vectors without rewriting checkpoints or calling Notion.
    let text_path = f.settings.index.join("chunks_text.lance");
    fs::remove_dir_all(&text_path).unwrap();
    fs::remove_file(f.settings.index.join("semantic-projection.sqlite")).unwrap();
    fs::remove_file(f.settings.index.join(".nk-text-projection-v1")).unwrap();
    let embeddings = f.provider.count.load(Ordering::SeqCst);
    let requests = http.requests().len();
    let runtime = IndexRuntime::recover(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert_eq!(http.requests().len(), requests);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    assert_eq!(runtime.chunks().page_chunks(PAGE).await.unwrap(), original);
    assert_eq!(
        runtime
            .chunks()
            .search_snapshot()
            .await
            .unwrap()
            .page_chunks(PAGE)
            .await
            .unwrap(),
        original,
    );
    drop(runtime);
    let retry = f.command(&http, &["--reconcile", "retry", PAGE]).await;
    assert_eq!(retry.exit, 0, "{:?}", retry.json);
    assert_eq!(retry.json["unchanged"], 1);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), embeddings);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    assert_eq!(runtime.chunks().page_chunks(PAGE).await.unwrap(), original);
    drop(runtime);

    // An installed text projection is more complete than its vector
    // projection. Its loss cannot be treated as a v1 migration.
    let saved = f.settings.index.join("saved-text");
    fs::rename(&text_path, &saved).unwrap();
    let requests = http.requests().len();
    assert!(
        IndexRuntime::recover(
            &f.settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone(),
        )
        .await
        .is_err()
    );
    assert!(!text_path.exists());
    assert_eq!(http.requests().len(), requests);
    fs::rename(saved, text_path).unwrap();
}

#[tokio::test]
async fn oversized_atomic_input_remains_fresh_and_searchable_with_a_typed_semantic_failure() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Info);
    http.state.lock().unwrap().markdown = format!(
        "<details>\n{}\n</details>\n",
        "private atomic source ".repeat(100)
    );
    f.provider.input_limit_bytes.store(1024, Ordering::SeqCst);
    let result = f.command(&http, &["--reconcile", "apply"]).await;
    assert_eq!(result.exit, 0, "{:?}", result.json);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    assert!(out_of_sync(&runtime).is_empty());
    let inventory = runtime.coordinator.state().inventory_counts().unwrap();
    assert_eq!((inventory.indexed, inventory.pending), (3, 0));
    let text = runtime.chunks().page_chunks(PAGE).await.unwrap();
    assert!(
        text.iter()
            .any(|chunk| chunk.text.contains("private atomic source"))
    );
    let (vectors, backlog) = runtime.chunks().semantic_status().await.unwrap();
    assert_eq!(backlog.input_limit, 1);
    assert!(vectors > 0);
    assert!(
        runtime
            .chunks()
            .search_snapshot()
            .await
            .unwrap()
            .page_chunks(PAGE)
            .await
            .unwrap()
            .is_empty()
    );
    use notion_knowledge_core::source::FreshPageCache;
    let cache =
        notion_knowledge_retrieval::page_documents::PageDocuments::new(runtime.coordinator.clone());
    let cached = cache
        .lookup(&PageId(PAGE.into()))
        .await
        .expect("source remains current despite the embedding limit");
    assert!(cached.content.markdown.contains("private atomic source"));
    assert!(cache.is_current(&cached.stamp));
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
}

#[tokio::test]
async fn embedding_input_limit_logs_contain_only_the_typed_reason_and_counts() {
    let output = tokio::time::timeout(std::time::Duration::from_secs(30),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "reconciliation_command::oversized_atomic_input_remains_fresh_and_searchable_with_a_typed_semantic_failure", "--nocapture"])
            .stdin(std::process::Stdio::null()).kill_on_drop(true).output()).await.unwrap().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    for private in ["fixture-credential", "private atomic source", PAGE, ROOT] {
        assert!(!stderr.contains(private), "source or credential leaked");
    }
    let events: Vec<serde_json::Value> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let event = events
        .iter()
        .find(|e| e["outcome"] == "embedding_deferred" && e["reason"] == "InputLimit")
        .unwrap();
    assert_eq!(event["operation"], "semantic_projection");
    assert_eq!(event.as_object().unwrap().len(), 3);
}

/// A per-page line holds fixed words, the sync counts and numbers only; its
/// numeric parts add up and none of the fixture's identifiers or text appear.
fn assert_page_line_is_content_free(event: &serde_json::Value) {
    const FIXED: [&str; 5] = ["operation", "phase", "reason", "status", "sync_progress"];
    const NUMBERS: [&str; 18] = [
        "wall_ms",
        "consumer_wait_ms",
        "embed_ms",
        "lance_merge_ms",
        "fts_optimize_ms",
        "vector_optimize_ms",
        "receipt_ms",
        "other_ms",
        "queue_wait_ms",
        "walk_api_ops",
        "walk_api_ms",
        "prepare_api_ops",
        "prepare_api_ms",
        "commit_api_ops",
        "commit_api_ms",
        "limiter_wait_ms",
        "text_chunks",
        "skipped_chunks",
    ];
    let line = event.to_string();
    for private in [
        PAGE,
        OTHER,
        ROOT,
        "First",
        "Saffron",
        "evergreen",
        "fixture-credential",
    ] {
        assert!(!line.contains(private), "{private} leaked into {line}");
    }
    let fields = event.as_object().unwrap();
    for (name, value) in fields {
        if FIXED.contains(&name.as_str()) {
            continue;
        }
        assert!(NUMBERS.contains(&name.as_str()), "unexpected field {name}");
        assert!(value.is_u64(), "{name} is not a count or duration: {value}");
    }
    assert!(
        NUMBERS.iter().all(|name| fields.contains_key(*name)),
        "{line}"
    );
    let ms = |name: &str| event[name].as_u64().unwrap();
    let parts: u64 = [
        "consumer_wait_ms",
        "embed_ms",
        "lance_merge_ms",
        "fts_optimize_ms",
        "vector_optimize_ms",
        "receipt_ms",
        "other_ms",
    ]
    .iter()
    .map(|name| ms(name))
    .sum();
    assert_eq!(parts, ms("wall_ms"), "{line}");
    // The commit path never asks Notion for anything.
    assert_eq!(ms("commit_api_ops"), 0, "{line}");
}

#[tokio::test]
async fn an_existing_coalesced_layout_is_recommitted_by_sync_and_by_cache_rebuild() {
    use notion_knowledge_core::chunking::ChunkConfig;
    use notion_knowledge_retrieval::refresh::AuthoritativePageRefresh;
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.state.lock().unwrap().markdown = (0..30)
        .map(|n| {
            format!(
                "<page url=\"https://example.invalid/{n}\">Independent readable source {n}</page>\n"
            )
        })
        .collect();
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    // Represent an already indexed wider chunk layout without changing the
    // source revision, content, model space or any authorization policy.
    let previous_layout = AuthoritativePageRefresh::new(
        http.client(),
        runtime.chunks().clone(),
        runtime.coordinator.clone(),
        authority(),
        &runtime.scope,
        ChunkConfig {
            target_chars: 100_000,
            overlap_chars: 0,
        },
    )
    .unwrap()
    .with_alias_sources(f.settings.alias_sources.clone());
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let lease = runtime.coordinator.state().acquire_lease(now, 330).unwrap();
    previous_layout
        .refresh_page(
            &PageId(PAGE.into()),
            &PageId(ROOT.into()),
            "previous-layout",
            f.provider.as_ref(),
            lease.clone(),
            Arc::new(|| notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000),
        )
        .await
        .unwrap();
    runtime
        .coordinator
        .state()
        .release_lease(&lease, now)
        .unwrap();
    assert_eq!(runtime.chunks().page_chunks(PAGE).await.unwrap().len(), 1);
    drop(previous_layout);
    drop(runtime);
    let result = f.command(&http, &["--reconcile", "apply"]).await;
    // A walk sync recommits the page under the configured layout.
    assert_eq!(result.exit, 0, "{}", result.json);
    let rebuilt = IndexRuntime::reset_and_resync(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    assert!(rebuilt.chunks().page_chunks(PAGE).await.unwrap().len() > 1);
    assert!(!rebuilt.coordinator.state().cache_rebuild_pending().unwrap());
}

#[tokio::test]
async fn target_projection_mode_changes_reconcile_without_reembedding_canonical_content() {
    use notion_knowledge_core::graph::{GraphEdgeStore, GraphTarget};
    use notion_knowledge_notion::relation_proposal::ProjectionMode;
    let http = start_notion().await;
    let mut f = Fixture::new(&http).await;
    {
        let mut source = http.state.lock().unwrap();
        source.relation_name = Some("Related".into());
        source.markdown = format!("[related](https://www.notion.so/{OTHER})\n");
    }
    f.seed_inventory(&http).await;
    let mut first_vectors = 0;
    for (n, mode) in [
        ProjectionMode::UnresolvedTargets,
        ProjectionMode::ResolveTargets,
        ProjectionMode::UnresolvedTargets,
        ProjectionMode::ResolveTargets,
    ]
    .into_iter()
    .enumerate()
    {
        f.settings.projection_mode = mode;
        f.populate(&http, &format!("projection-mode-{n}")).await;
        let runtime = f.open(&http, OpenMode::ExistingWrite).await;
        let edges = runtime.coordinator.state().edges_from(PAGE).unwrap();
        assert_eq!(edges.len(), 2);
        // Reads authorize from the walk's inventory, which this runner path does not record.
        let walk = http.client().walk_tree(&authority(), None).await.unwrap();
        runtime
            .coordinator
            .state()
            .record_walk(&walk.run_id, &walk.entries, walk.authoritative())
            .unwrap();
        let committed =
            notion_knowledge_retrieval::graph::CommittedGraph::new(runtime.coordinator.clone());
        let gate = notion_knowledge_core::root_scope::ReadAuthority::Inventory(
            notion_knowledge_core::root_scope::ReadScopeGate::new(
                runtime.coordinator.state().clone(),
                authority(),
            )
            .unwrap(),
        );
        let related = notion_knowledge_mcp::related::navigate(
            &notion_knowledge_mcp::related::RelatedRequest {
                page_id: PAGE.into(),
                depth: 1,
                limit: 10,
            },
            &committed,
            &gate,
            &[ROOT.into()],
        )
        .await
        .unwrap();
        assert_eq!(
            related.relations.is_empty(),
            mode == ProjectionMode::UnresolvedTargets
        );

        assert!(edges.iter().all(|edge| match (mode, edge.target()) {
            (ProjectionMode::ResolveTargets, GraphTarget::Page { page_id }) => page_id == OTHER,
            (ProjectionMode::UnresolvedTargets, GraphTarget::Unresolved { reference }) =>
                reference == OTHER,
            _ => false,
        }));
        assert_eq!(
            runtime.chunks().page_chunks(PAGE).await.unwrap()[0].text,
            http.state.lock().unwrap().markdown
        );
        if n == 0 {
            first_vectors = f.provider.count.load(Ordering::SeqCst);
        } else {
            assert_eq!(f.provider.count.load(Ordering::SeqCst), first_vectors);
        }
    }
}

#[tokio::test]
async fn page_deadline_logs_expose_only_fixed_reason_and_aggregate_counts() {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(180),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "reconciliation_command::a_page_deadline_is_counted_as_a_timeout_not_only_a_failure",
                "--exact",
                "--nocapture",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let skips: Vec<_> = events
        .iter()
        .filter(|event| event["reason"] == "page_timeout")
        .collect();
    assert!(!skips.is_empty());
    for event in skips {
        assert_page_line_is_content_free(event);
        assert_eq!(event["status"], "page_skipped");
        assert!(event["sync_progress"].is_object());
    }
}

/// Wait until at least `count` Markdown reads (walk reads included) happened.
async fn await_body_reads(http: &HttpNotion, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while http.body_reads.load(Ordering::SeqCst) < count {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_sync_fetches_next_page_while_a_text_commit_is_paused() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    http.reset_markdown_pages();
    let commit = Arc::new(RequestPause::default());
    commit.armed.store(true, Ordering::SeqCst);
    pause_owned_commits(&f.settings.index, commit.clone());
    let settings = f.settings.clone();
    let state = f.state.clone();
    let source = http.client();
    let provider = f.provider.clone();
    let recovery = tokio::spawn(async move {
        IndexRuntime::recover(&settings, &state, authority(), source, provider).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        commit.started.notified(),
    )
    .await
    .unwrap();
    // The source commit waits while discovery reads other pages.
    await_body_reads(&http, 3).await;
    assert!(!recovery.is_finished());
    commit.proceed.notify_one();
    let runtime = recovery.await.unwrap().unwrap();
    assert_eq!(runtime.coordinator.owned_page_ids().unwrap().len(), 3);
    for page in [ROOT, PAGE, OTHER] {
        assert!(!runtime.chunks().page_chunks(page).await.unwrap().is_empty());
    }
    assert!(!runtime.coordinator.state().cache_rebuild_pending().unwrap());
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_sync_read_ahead_is_bounded_while_a_text_commit_is_paused() {
    let http = start_notion().await;
    http.change(|m| {
        for n in 0..40 {
            m.extra.insert(
                format!("66666666-6666-6666-6666-{n:012}"),
                ExtraPage {
                    title: format!("Extra {n}"),
                    visible: true,
                    archived: false,
                },
            );
        }
    });
    let f = Fixture::new(&http).await;
    fs::remove_dir_all(&f.settings.index).unwrap();
    http.reset_markdown_pages();
    let commit = Arc::new(RequestPause::default());
    commit.armed.store(true, Ordering::SeqCst);
    pause_owned_commits(&f.settings.index, commit.clone());
    let settings = f.settings.clone();
    let state = f.state.clone();
    let source = http.client();
    let provider = f.provider.clone();
    let recovery = tokio::spawn(async move {
        IndexRuntime::recover(&settings, &state, authority(), source, provider).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        commit.started.notified(),
    )
    .await
    .unwrap();
    await_body_reads(&http, 3).await;
    // Give unbounded read-ahead ample time to read all 43 pages.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let fetched = http.body_reads.load(Ordering::SeqCst);
    assert!(
        fetched <= 24,
        "root, held page, one queued, two preparing, 8 in the walk channel and 8 in its outbox or in flight, with slack; got {fetched}"
    );
    commit.proceed.notify_one();
    let runtime = recovery.await.unwrap().unwrap();
    assert_eq!(runtime.coordinator.owned_page_ids().unwrap().len(), 43);
    assert_eq!(f.provider.count.load(Ordering::SeqCst), 0);
}

/// Add `count` visible pages under the root, so planned work exceeds read-ahead.
fn add_extra_pages(http: &HttpNotion, count: usize) {
    http.change(|m| {
        for n in 0..count {
            m.extra.insert(
                format!("66666666-6666-6666-6666-{n:012}"),
                ExtraPage {
                    title: format!("Extra {n}"),
                    visible: true,
                    archived: false,
                },
            );
        }
    });
}

#[tokio::test]
async fn initial_sync_records_inventory_totals_equal_outcomes() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    http.change(|m| {
        m.inaccessible_other = true;
        for (id, title) in [
            ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "Extra one"),
            ("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "Extra two"),
        ] {
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
    let runtime = IndexRuntime::reset_and_resync(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    let counts = runtime.coordinator.state().inventory_counts().unwrap();
    assert_eq!(counts.total, 5);
    assert_eq!((counts.indexed, counts.skipped, counts.failed), (4, 1, 0));
    assert_eq!(counts.pending, 0);
}

/// Skipped and failed pages recorded by the last sync's inventory.
fn out_of_sync(
    runtime: &IndexRuntime,
) -> Vec<(String, notion_knowledge_core::inventory::InventoryOutcome)> {
    runtime.coordinator.state().out_of_sync_pages(100).unwrap()
}

/// `NK_PREFETCH_WORKERS=0` prepares each page inline on demand; the sync
/// still settles and commits every walked page.
#[tokio::test]
async fn full_sync_without_prefetch_workers_commits_every_page() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    add_extra_pages(&http, 4);
    let mut settings = f.settings.clone();
    settings.prefetch_workers = 0;
    let runtime = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        IndexRuntime::reset_and_resync(
            &settings,
            &f.state,
            authority(),
            http.client(),
            f.provider.clone(),
        ),
    )
    .await
    .expect("inline preparation must not stall the sync")
    .unwrap();
    let counts = runtime.coordinator.state().inventory_counts().unwrap();
    assert!(counts.total > 4, "{counts:?}");
    assert_eq!(counts.indexed, counts.total, "{counts:?}");
}

#[tokio::test]
async fn full_sync_makes_two_calls_per_plain_page_and_one_per_row() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // 10 plain pages under the root (PAGE, OTHER and 8 extras) and a
    // database with 20 rows.
    add_extra_pages(&http, 8);
    http.change(|m| {
        m.rows = (0..20)
            .map(|n| format!("99999999-9999-9999-9999-{n:012}"))
            .collect();
    });
    let before = http.requests().len();
    let runtime = IndexRuntime::reset_and_resync(
        &f.settings,
        &f.state,
        authority(),
        http.client(),
        f.provider.clone(),
    )
    .await
    .unwrap();
    let requests = http.requests()[before..].to_vec();
    // One search listing, the root's two reads, one Markdown read for each
    // listed plain page and row, and one database read and query.
    assert!(
        requests.len() <= 2 * 11 + 20 + 2,
        "{} {requests:?}",
        requests.len()
    );
    assert!(
        requests
            .iter()
            .filter(|r| r.starts_with("/v1/pages/") && !r.ends_with("/markdown"))
            .all(|r| r == &format!("/v1/pages/{ROOT}")),
        "{requests:?}"
    );
    let counts = runtime.coordinator.state().inventory_counts().unwrap();
    assert_eq!(counts.total, 31);
    assert_eq!(counts.indexed, 31);
    assert_eq!(
        counts.indexed + counts.skipped + counts.failed + counts.pending,
        counts.total
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_command_drains_its_submitted_owned_guard_before_releasing_the_writer() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let commits = Arc::new(RequestPause::default());
    commits.armed.store(true, Ordering::SeqCst);
    pause_owned_commits(&f.settings.index, commits.clone());
    let stop = CancellationToken::new();
    let pending = f.command_cancel(&http, &["--reconcile", "apply"], stop.clone());
    let cancel = async {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            commits.started.notified(),
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
    let guard = std::fs::File::open(&f.settings.index).unwrap();
    guard.try_lock().unwrap();
    drop(guard);
    commits.proceed.notify_one();
    counts(&f.command(&http, &["--reconcile", "apply"]).await, 3, 0, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_scheduled_http_shutdown_drains_live_owned_effect_before_release() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let commits = Arc::new(RequestPause::default());
    commits.armed.store(true, Ordering::SeqCst);
    pause_owned_commits(&f.settings.index, commits.clone());
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        commits.started.notified(),
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
    commits.proceed.notify_one();
    let (worker, stop, server) =
        scheduled_server(&f, &http, std::time::Duration::from_secs(60)).await;
    await_schedule(&worker, |s| s.cycle == 1 && s.active_run.is_none()).await;
    stop.cancel();
    server.await.unwrap().unwrap();
}

/// Hold the owned commit (guard and fence held, before effects) whenever
/// `pause` is armed. Give it its own pause so no source read can fire it.
fn pause_owned_commits(index: &std::path::Path, pause: Arc<RequestPause>) {
    notion_knowledge_retrieval::test_hooks::set_commit_pause(
        index,
        Arc::new(move || {
            let pause = pause.clone();
            Box::pin(async move { pause.hold().await })
        }),
    );
}

#[tokio::test]
async fn expired_claim_cannot_write_inventory_or_drift_metadata() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // Not indexed, so no commit or removal runs that a fence would reject
    // first: only the inventory and drift writes are left to refuse.
    http.change(|m| m.inaccessible_page = true);
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let state = runtime.coordinator.state().clone();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let mut lease = state.acquire_lease(now, 90).unwrap();
    let expired: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| i64::MAX);
    let error = runtime
        .sync_page(
            &PageDeadline {
                timeout_seconds: 60,
            },
            &mut lease,
            PageId(PAGE.into()),
            expired,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.failure,
        notion_knowledge_server::index_runtime::SyncFailure::Authority
    );
    assert!(state.last_sync().unwrap().is_none());
    // The same sync under a live claim records its drift.
    runtime
        .sync_page(
            &PageDeadline {
                timeout_seconds: 60,
            },
            &mut lease,
            PageId(PAGE.into()),
            Arc::new(|| notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000),
        )
        .await
        .unwrap();
    assert!(state.last_sync().unwrap().is_some());
}

#[tokio::test]
async fn a_page_deadline_is_counted_as_a_timeout_not_only_a_failure() {
    use std::time::Duration;
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    let runtime = f.open(&http, OpenMode::ExistingWrite).await;
    let state = runtime.coordinator.state().clone();
    let now = notion_knowledge_server::webhook_worker::runtime::now_ms() / 1000;
    let mut lease = state.acquire_lease(now, 90).unwrap();
    notion_knowledge_core::logging::set_level(notion_knowledge_core::logging::Level::Info);
    let pause = Arc::new(RequestPause::default());
    pause.armed.store(true, Ordering::SeqCst);
    let observed = pause.clone();
    notion_knowledge_retrieval::test_hooks::set_preparation_pause(
        &f.settings.index,
        Arc::new(move || {
            let pause = pause.clone();
            Box::pin(async move { pause.hold().await })
        }),
    );
    let release = async {
        tokio::time::timeout(Duration::from_secs(10), observed.started.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        observed.proceed.notify_one();
    };
    let (totals, ()) = tokio::join!(
        async {
            notion_knowledge_core::logging::with_sync_progress(runtime.sync_from_walk(
                &PageDeadline { timeout_seconds: 1 },
                &mut lease,
                SyncMode::Full,
            ))
            .await
            .unwrap()
        },
        release
    );
    assert_eq!((totals.failed, totals.timed_out), (1, 1), "{totals:?}");
    assert_eq!(totals.permanent, 0);
}

#[tokio::test]
async fn http_answers_liveness_and_reports_startup_while_the_cache_rebuild_runs() {
    let http = start_notion().await;
    let f = Fixture::new(&http).await;
    // A failed rebuild leaves the durable rebuild flag pending for the next start.
    fs::remove_dir_all(&f.settings.index).unwrap();
    http.change(|source| source.unauthenticated = true);
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
    http.change(|source| source.unauthenticated = false);
    // Without a search listing the walk reads PAGE's metadata, where it pauses.
    http.change(|m| m.search_unavailable = true);
    http.pause.armed.store(true, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut config = notion_knowledge_server::config::Config::from_lookup(|_| None).unwrap();
    config.http_bind = address;
    config.webhook_state_file = Some(f.state.clone());
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let (settings, state, provider, source) = (
        f.settings.clone(),
        f.state.clone(),
        f.provider.clone(),
        http.client(),
    );
    let server = tokio::spawn(async move {
        notion_knowledge_server::http::serve_starting(
            config,
            async move {
                IndexRuntime::recover(&settings, &state, authority(), source, provider).await?;
                Ok(notion_knowledge_server::http::Installed {
                    handler: notion_knowledge_mcp::KnowledgeServer::default(),
                    worker: None,
                })
            },
            shutdown,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        http.pause.started.notified(),
    )
    .await
    .expect("the rebuild reaches its authoritative read");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let live = client
        .get(format!("http://{address}/livez"))
        .send()
        .await
        .expect("the listener answers during the rebuild");
    assert_eq!(live.status(), reqwest::StatusCode::OK);
    let ready = client
        .get(format!("http://{address}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["status"], "starting");
    assert_eq!(body["startup"], "cache_rebuild");
    let mcp = client
        .post(format!("http://{address}/mcp"))
        .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
        .send()
        .await
        .unwrap();
    assert_eq!(mcp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = mcp.json().await.unwrap();
    assert_eq!(body["startup"], "cache_rebuild");
    http.pause.proceed.notify_one();
    // After the rebuild the full router replaces the startup phase.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let body: serde_json::Value = client
                .get(format!("http://{address}/readyz"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if body["status"] != "starting" {
                assert!(body.get("startup").is_none());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("startup completes after the rebuild");
    let state =
        notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open_existing(&f.state)
            .unwrap();
    assert!(!state.cache_rebuild_pending().unwrap());
    stop.cancel();
    server.await.unwrap().unwrap();
}
