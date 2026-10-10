//! Actual local LanceDB and SQLite tests; no model downloads or credentials.
use crate::{
    chunks::{GuardedChunkTable, LanceChunkTable, VectorIndexConfig},
    commit::{
        CommitBinding, CommitError, CommitOutcome, IndexCommitCoordinator, PageAction,
        PageOperation, ReceiptStatus,
    },
};
use notion_knowledge_core::{
    embedding::{EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    reconciliation::{FailureClass, Lease, ReconciliationJournal, ReconciliationScope},
    sync_state::{PageSyncState, SyncStateStore},
};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    index: PathBuf,
    coordinator: Arc<IndexCommitCoordinator>,
    embedding: EmbeddingMetadata,
    lease: Lease,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "nk-guarded-lance-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = root.join("index");
        let state = root.join("state.sqlite");
        fs::create_dir_all(&index).unwrap();
        let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
        let binding = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
        let coordinator = IndexCommitCoordinator::initialize(&index, &state, binding).unwrap();
        let lease = coordinator.state().acquire_lease(10, 100).unwrap();
        let embedding =
            EmbeddingMetadata::new("count".into(), "test-model".into(), "v1".into(), 3).unwrap();
        Self {
            root,
            index,
            coordinator,
            embedding,
            lease,
        }
    }
    fn guarded(&self) -> GuardedChunkTable {
        GuardedChunkTable::bind(
            &self.index,
            "chunks",
            self.embedding.clone(),
            self.coordinator.clone(),
        )
        .unwrap()
    }
    async fn create(&self) {
        self.guarded()
            .create_empty(self.lease.clone(), clock())
            .await
            .unwrap();
    }
    async fn readable(&self) -> LanceChunkTable {
        LanceChunkTable::open(&self.index, "chunks", self.embedding.clone())
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn clock() -> Arc<dyn Fn() -> i64 + Send + Sync> {
    Arc::new(|| 10)
}
fn operation(id: &str, source_hash: &str, delete: bool) -> PageOperation {
    let checkpoint = if delete {
        PageSyncState::tombstone("page".into(), Some("revision-v1".into())).unwrap()
    } else {
        PageSyncState::present(
            "page".into(),
            source_hash.into(),
            Some("revision-v1".into()),
        )
        .unwrap()
    };
    PageOperation::new(
        id,
        "revision-v1",
        if delete {
            PageAction::Delete
        } else {
            PageAction::Refresh
        },
        checkpoint,
    )
    .unwrap()
}
fn chunk(id: &str, text: &str, title: &str) -> IndexedChunk {
    IndexedChunk {
        schema_version: SchemaVersion::V1,
        chunk_id: id.into(),
        metadata: IndexedMetadata {
            page_id: "page".into(),
            block_id: Some("block".into()),
            url: "https://example.invalid/page".into(),
            title: title.into(),
            heading_path: vec!["Section".into()],
            last_edited_time: "revision-v1".into(),
            source: SourceMetadata {
                workspace_id: "workspace".into(),
                root_page_id: "root".into(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: text.into(),
        content_hash: format!("hash:{text}"),
        links: vec![],
    }
}
struct CountingProvider {
    metadata: EmbeddingMetadata,
    total: AtomicUsize,
    fail: bool,
}
impl CountingProvider {
    fn new(metadata: EmbeddingMetadata) -> Self {
        Self {
            metadata,
            total: AtomicUsize::new(0),
            fail: false,
        }
    }
    fn total(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }
}
impl EmbeddingProvider for CountingProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        self.total.fetch_add(inputs.len(), Ordering::SeqCst);
        Box::pin(async move {
            if self.fail {
                Err(EmbeddingError::Unavailable)
            } else {
                Ok(inputs
                    .iter()
                    .map(|s| vec![s.len() as f32, 1.0, 0.0])
                    .collect())
            }
        })
    }
}

// Only unit-test child processes select this wrapper. Production builds have no
// environment switch, injected callbacks, or alternative storage admission.
fn child_root(directory: &std::path::Path) -> Option<PathBuf> {
    let root = PathBuf::from(std::env::var_os("NK_REAL_IO_ROOT")?);
    (root.join("index") == directory).then_some(root)
}
pub(super) fn test_uri(directory: &std::path::Path, uri: String) -> String {
    if child_root(directory).is_some() {
        format!("file-object-store://{}", directory.display())
    } else {
        uri
    }
}
pub(super) fn instrument(
    directory: &std::path::Path,
    builder: lancedb::connection::OpenTableBuilder,
) -> lancedb::connection::OpenTableBuilder {
    let Some(root) = child_root(directory) else {
        return builder;
    };
    if std::env::var("NK_REAL_IO_NAME").as_deref() != Ok("one") {
        return builder;
    }
    builder.lance_read_params(lance::dataset::ReadParams {
        store_options: Some(lance::io::ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(Gate { root })),
            ..Default::default()
        }),
        ..Default::default()
    })
}
#[derive(Debug, Clone)]
struct Gate {
    root: PathBuf,
}
impl Gate {
    async fn wait(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        // Real data and index multipart completion or put calls, never the
        // source callback. Mark only the first write per family.
        let name = location.as_ref();
        let stage = if name.contains("/_indices/") {
            "index"
        } else if name.contains("/data/") {
            "data"
        } else {
            return Ok(());
        };
        let marker = self.root.join(format!("{stage}-entered"));
        let claimed = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(marker)
            .is_ok();
        if !claimed {
            return Ok(());
        }
        if stage == "index" && self.root.join("fail-index").exists() {
            return Err(object_store::Error::Generic {
                store: "fixture",
                source: "private fixture error".into(),
            });
        }
        let start = std::time::Instant::now();
        while !self.root.join(format!("{stage}-release")).exists() {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(40),
                "real IO fixture timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }
}
impl lance::io::WrappingObjectStore for Gate {
    fn wrap(
        &self,
        _: &str,
        inner: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(PausedStore {
            inner,
            gate: self.clone(),
        })
    }
    fn wrap_paginated(
        &self,
        _: &str,
        _: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        None
    }
}
#[derive(Debug)]
struct PausedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gate: Gate,
}
impl std::fmt::Display for PausedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausedStore")
    }
}
#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl object_store::ObjectStore for PausedStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.gate.wait(location).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(PausedUpload {
            inner,
            gate: self.gate.clone(),
            location: location.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &object_store::path::Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct PausedUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    gate: Gate,
    location: object_store::path::Path,
}
#[async_trait::async_trait]
impl object_store::MultipartUpload for PausedUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        self.gate.wait(&self.location).await?;
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

fn wait_for(mut predicate: impl FnMut() -> bool) {
    let begin = std::time::Instant::now();
    while !predicate() {
        assert!(
            begin.elapsed() < std::time::Duration::from_secs(40),
            "writer timed out"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn launch(f: &Fixture, name: &str) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "chunks::guarded::io_tests::real_io_child",
            "--ignored",
            "--nocapture",
        ])
        .env("NK_REAL_IO_ROOT", &f.root)
        .env("NK_REAL_IO_NAME", name)
        .env("NK_REAL_IO_FENCE", f.lease.fence.to_string())
        .spawn()
        .unwrap()
}

#[test]
fn independent_writer_waits_through_real_data_and_index_io_after_cancellation_and_expiry() {
    let rt = runtime();
    let f = Fixture::new();
    rt.block_on(async {
        f.create().await;
        let table = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let seed = table
            .prepare_page(
                &provider,
                operation("seed", "seed", false),
                &[chunk("one", "initialcinnamon", "Seed")],
            )
            .await
            .unwrap();
        assert_eq!(
            table
                .commit_page(seed, f.lease.clone(), clock(), || async { Ok(()) })
                .await,
            Ok(CommitOutcome::Applied)
        );
        table
            .ensure_vector_index(
                VectorIndexConfig {
                    num_partitions: Some(1),
                    sample_rate: 8,
                    max_iterations: 20,
                    ..Default::default()
                },
                f.lease.clone(),
                clock(),
            )
            .await
            .unwrap();
    });
    fs::write(f.root.join("time"), "10").unwrap();
    let mut one = launch(&f, "one");
    let mut two = launch(&f, "two");
    wait_for(|| f.root.join("one-prepared").exists() && f.root.join("two-prepared").exists());
    fs::write(f.root.join("one-begin"), "").unwrap();
    wait_for(|| f.root.join("data-entered").exists());
    wait_for(|| f.root.join("observer-dropped").exists());
    fs::write(f.root.join("time"), "110").unwrap();
    let fresh = f.coordinator.state().acquire_lease(110, 100).unwrap();
    fs::write(f.root.join("fresh-fence"), fresh.fence.to_string()).unwrap();
    fs::write(f.root.join("two-begin"), "").unwrap();
    wait_for(|| f.root.join("two-submitted").exists());
    for stage in ["data", "index"] {
        wait_for(|| f.root.join(format!("{stage}-entered")).exists());
        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(!f.root.join("two-finished").exists());
        assert!(one.try_wait().unwrap().is_none());
        assert!(two.try_wait().unwrap().is_none());
        assert_eq!(
            f.coordinator.receipt("one").unwrap().unwrap().status,
            ReceiptStatus::Pending
        );
        assert_eq!(
            f.coordinator
                .state()
                .page_state("page")
                .unwrap()
                .unwrap()
                .content_hash(),
            Some("seed")
        );
        fs::write(f.root.join(format!("{stage}-release")), "").unwrap();
    }
    wait_for(|| f.root.join("one-finished").exists() && f.root.join("two-finished").exists());
    assert!(one.wait().unwrap().success());
    assert!(two.wait().unwrap().success());
    // The owned IO completed, but an expired fence must not acknowledge it.
    assert_eq!(
        f.coordinator.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    assert_eq!(
        f.coordinator.receipt("two").unwrap().unwrap().failure,
        Some(FailureClass::Conflict)
    );
    rt.block_on(async {
        let readable = f.readable().await;
        assert_eq!(
            readable
                .fts_query("text", "leaderblueberry", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            readable
                .fts_query("text", "contenderpapaya", 10)
                .await
                .unwrap()
                .len(),
            0
        );
        let table = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let replay = table
            .prepare_page(
                &provider,
                operation("one", "one", false),
                &[chunk("one", "leaderblueberry", "one")],
            )
            .await
            .unwrap();
        assert_eq!(provider.total(), 0, "replay reuses actual applied vectors");
        assert_eq!(
            table
                .commit_page(replay, fresh.clone(), Arc::new(|| 110), || async { Ok(()) })
                .await,
            Ok(CommitOutcome::Applied)
        );
        table
            .ensure_vector_index(
                VectorIndexConfig {
                    num_partitions: Some(1),
                    sample_rate: 8,
                    max_iterations: 20,
                    ..Default::default()
                },
                fresh,
                Arc::new(|| 110),
            )
            .await
            .unwrap();
        let readable = f.readable().await;
        assert_eq!(readable.count_rows().await.unwrap(), 1);
        assert_eq!(
            readable
                .vector_query(&[15.0, 1.0, 0.0], 10, 1)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            readable
                .fts_query("text", "leaderblueberry", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    });
    assert_eq!(
        f.coordinator.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert_eq!(
        f.coordinator
            .state()
            .page_state("page")
            .unwrap()
            .unwrap()
            .content_hash(),
        Some("one")
    );
}

#[tokio::test]
#[ignore]
async fn real_io_child() {
    let Ok(root) = std::env::var("NK_REAL_IO_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let name = std::env::var("NK_REAL_IO_NAME").unwrap();
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let binding = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
    let coordinator =
        IndexCommitCoordinator::open(root.join("index"), root.join("state.sqlite"), binding)
            .unwrap();
    let embedding =
        EmbeddingMetadata::new("count".into(), "test-model".into(), "v1".into(), 3).unwrap();
    let table = GuardedChunkTable::bind(
        root.join("index"),
        "chunks",
        embedding.clone(),
        coordinator.clone(),
    )
    .unwrap();
    let provider = CountingProvider::new(embedding);
    let text = if name == "one" {
        "leaderblueberry"
    } else {
        "contenderpapaya"
    };
    let prepared = table
        .prepare_page(
            &provider,
            operation(&name, &name, false),
            &[chunk("one", text, &name)],
        )
        .await
        .unwrap();
    fs::write(root.join(format!("{name}-prepared")), "").unwrap();
    wait_for(|| root.join(format!("{name}-begin")).exists());
    let fence = fs::read_to_string(if name == "one" {
        root.join("initial-fence")
    } else {
        root.join("fresh-fence")
    });
    let fence: i64 = fence
        .unwrap_or_else(|_| std::env::var("NK_REAL_IO_FENCE").unwrap())
        .parse()
        .unwrap();
    let lease = Lease {
        fence,
        expires_at: if name == "one" { 110 } else { 210 },
    };
    let time_path = root.join("time");
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> =
        Arc::new(move || fs::read_to_string(&time_path).unwrap().parse().unwrap());
    let observer = table.commit_page(prepared, lease, clock, || async { Ok(()) });
    if name == "one" && root.join("fail-index").exists() {
        assert_eq!(
            observer.await,
            Err(CommitError::Operation(FailureClass::Index))
        );
    } else if name == "one" {
        wait_for(|| root.join("data-entered").exists());
        drop(observer);
        fs::write(root.join("observer-dropped"), "").unwrap();
        // Eager operation ownership means observer drop is not completion.
        wait_for(|| root.join("index-release").exists());
        // Taking the same directory guard proves all IO and final ack attempt
        // have finished before the child exits.
        let guard = std::fs::File::open(root.join("index")).unwrap();
        guard.lock().unwrap();
        assert_eq!(
            coordinator.receipt("one").unwrap().unwrap().status,
            ReceiptStatus::Pending
        );
    } else {
        fs::write(root.join("two-submitted"), "").unwrap();
        assert_eq!(
            observer.await,
            Err(CommitError::Operation(FailureClass::Conflict))
        );
    }
    fs::write(root.join(format!("{name}-finished")), "").unwrap();
}

#[test]
fn failed_real_index_write_is_unacknowledged_and_new_fence_replay_repairs_search() {
    let rt = runtime();
    let f = Fixture::new();
    rt.block_on(f.create());
    fs::write(f.root.join("time"), "10").unwrap();
    fs::write(f.root.join("fail-index"), "").unwrap();
    fs::write(f.root.join("data-release"), "").unwrap();
    let mut one = launch(&f, "one");
    wait_for(|| f.root.join("one-prepared").exists());
    fs::write(f.root.join("one-begin"), "").unwrap();
    wait_for(|| f.root.join("one-finished").exists());
    assert!(one.wait().unwrap().success());
    assert!(
        f.root.join("index-entered").exists(),
        "failure must be inside real maintenance"
    );
    assert_eq!(
        f.coordinator.receipt("one").unwrap().unwrap().failure,
        Some(FailureClass::Index)
    );
    assert!(f.coordinator.state().page_state("page").unwrap().is_none());
    let fresh = f.coordinator.state().acquire_lease(110, 100).unwrap();
    rt.block_on(async {
        // Read raw persisted rows through a preparation handle, whose opening
        // cannot perform startup repairs, proving apply preceded index failure.
        let current = super::open_current(&f.index, "chunks", &f.embedding)
            .await
            .unwrap();
        assert_eq!(current.count_rows().await.unwrap(), 1);
        let table = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let replay = table
            .prepare_page(
                &provider,
                operation("one", "one", false),
                &[chunk("one", "leaderblueberry", "one")],
            )
            .await
            .unwrap();
        assert_eq!(provider.total(), 0);
        assert_eq!(
            table
                .commit_page(replay, fresh.clone(), Arc::new(|| 110), || async { Ok(()) })
                .await,
            Ok(CommitOutcome::Applied)
        );
        table
            .ensure_vector_index(
                VectorIndexConfig {
                    num_partitions: Some(1),
                    sample_rate: 8,
                    max_iterations: 20,
                    ..Default::default()
                },
                fresh,
                Arc::new(|| 110),
            )
            .await
            .unwrap();
        let readable = f.readable().await;
        assert_eq!(readable.count_rows().await.unwrap(), 1);
        assert_eq!(
            readable
                .fts_query("text", "leaderblueberry", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            readable
                .vector_query(&[15.0, 1.0, 0.0], 10, 1)
                .await
                .unwrap()
                .len(),
            1
        );
    });
    assert_eq!(
        f.coordinator.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert_eq!(
        f.coordinator
            .state()
            .page_state("page")
            .unwrap()
            .unwrap()
            .content_hash(),
        Some("one")
    );
}

#[tokio::test]
async fn changed_scope_generation_rejects_old_proposal_without_changing_real_search() {
    let f = Fixture::new();
    f.create().await;
    let table = f.guarded();
    let provider = CountingProvider::new(f.embedding.clone());
    let initial = table
        .prepare_page(
            &provider,
            operation("seed", "seed", false),
            &[chunk("one", "approvedsaffron", "Seed")],
        )
        .await
        .unwrap();
    assert_eq!(
        table
            .commit_page(initial, f.lease.clone(), clock(), || async { Ok(()) })
            .await,
        Ok(CommitOutcome::Applied)
    );
    let old = table
        .prepare_page(
            &provider,
            operation("old-scope", "old", false),
            &[chunk("one", "forbiddenpapaya", "Old")],
        )
        .await
        .unwrap();
    let scope =
        ReconciliationScope::new(vec!["root".into()], vec![], "updated-policy", "v1").unwrap();
    let next = CommitBinding::new("chunks", "workspace", &scope, "generation-two", 2).unwrap();
    f.coordinator.change_scope(next, &f.lease, 10).unwrap();
    assert_eq!(
        table
            .commit_page(old, f.lease.clone(), clock(), || async {
                panic!("obsolete authority must never reach source check")
            })
            .await,
        Err(CommitError::BindingMismatch)
    );
    assert!(f.coordinator.receipt("old-scope").unwrap().is_none());
    assert_eq!(
        f.coordinator
            .state()
            .page_state("page")
            .unwrap()
            .unwrap()
            .content_hash(),
        Some("seed")
    );
    let readable = f.readable().await;
    assert_eq!(readable.count_rows().await.unwrap(), 1);
    assert_eq!(
        readable
            .fts_query("text", "approvedsaffron", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        readable
            .fts_query("text", "forbiddenpapaya", 10)
            .await
            .unwrap()
            .len(),
        0
    );
}
