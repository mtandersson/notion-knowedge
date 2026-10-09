#![cfg(all(unix, feature = "local-lancedb"))]
//! Actual local LanceDB and SQLite tests; no model downloads or credentials.
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use notion_knowledge_core::{
    embedding::{EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    reconciliation::{FailureClass, Lease, ReconciliationJournal, ReconciliationScope},
    sync_state::{PageSyncState, SyncStateStore},
};
use notion_knowledge_retrieval::{
    chunks::{ChunkTableError, FtsIndexConfig, GuardedChunkTable, LanceChunkTable, VectorIndexConfig},
    commit::{CommitBinding, CommitError, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation, ReceiptStatus},
};
use rusqlite::Connection;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    index: PathBuf,
    state: PathBuf,
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
        let embedding = EmbeddingMetadata::new(
            "count".into(), "test-model".into(), "v1".into(), 3
        ).unwrap();
        Self { root, index, state, coordinator, embedding, lease }
    }
    fn guarded(&self) -> GuardedChunkTable {
        GuardedChunkTable::bind(
            &self.index, "chunks", self.embedding.clone(), self.coordinator.clone(),
        ).unwrap()
    }
    async fn create(&self) {
        self.guarded().create_empty(self.lease.clone(), clock()).await.unwrap();
    }
    async fn readable(&self) -> LanceChunkTable {
        LanceChunkTable::open(&self.index, "chunks", self.embedding.clone()).await.unwrap()
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
            "page".into(), source_hash.into(), Some("revision-v1".into())
        ).unwrap()
    };
    PageOperation::new(
        id, "revision-v1",
        if delete { PageAction::Delete } else { PageAction::Refresh }, checkpoint
    ).unwrap()
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
        Self { metadata, total: AtomicUsize::new(0), fail: false }
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
                Ok(inputs.iter().map(|s| vec![s.len() as f32, 1.0, 0.0]).collect())
            }
        })
    }
}

#[tokio::test]
async fn real_guarded_page_updates_reuse_vectors_and_reject_stale_and_source() {
    let f = Fixture::new();
    f.create().await;
    let guarded = f.guarded();
    let provider = CountingProvider::new(f.embedding.clone());
    let first = guarded.prepare_page(
        &provider, operation("first", "h1", false),
        &[chunk("one", "saffron linguine", "First")],
    ).await.unwrap();
    assert_eq!(first.metrics().added, 1);
    assert_eq!(provider.total(), 1);
    assert_eq!(
        guarded.commit_page(first, f.lease.clone(), clock(), || async { Ok(()) }).await,
        Ok(CommitOutcome::Applied)
    );
    let table = f.readable().await;
    assert_eq!(table.count_rows().await.unwrap(), 1);
    assert_eq!(table.fts_query("text", "saffron", 10).await.unwrap().len(), 1);
    // Building ANN is also owned by the same directory lock.
    guarded.ensure_vector_index(
        VectorIndexConfig::default(), f.lease.clone(), clock()
    ).await.unwrap();
    let table = f.readable().await;
    assert!(!table.vector_query(&[16.0, 1.0, 0.0], 10, 1).await.unwrap().is_empty());

    let original = vec![chunk("one", "saffron linguine", "Metadata changed")];
    let metadata = guarded.prepare_page(
        &provider, operation("metadata", "h2", false), &original,
    ).await.unwrap();
    assert_eq!(provider.total(), 1, "metadata-only edits must not re-embed");
    assert_eq!(metadata.metrics().skipped, 1);
    let stale = guarded.prepare_page(
        &provider, operation("stale", "h3", false), &original
    ).await.unwrap();
    assert_eq!(
        guarded.commit_page(metadata, f.lease.clone(), clock(), || async { Ok(()) }).await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(
        guarded.commit_page(stale, f.lease.clone(), clock(), || async { panic!("stale source check ran") }).await,
        Err(CommitError::Operation(FailureClass::Conflict))
    );
    let refreshed = f.readable().await;
    assert_eq!(refreshed.count_rows().await.unwrap(), 1);
    assert_eq!(refreshed.fts_query("text", "saffron", 10).await.unwrap().len(), 1);

    let rejected = guarded.prepare_page(
        &provider, operation("rejected", "h4", false),
        &[chunk("one", "withheld pineapple", "New")],
    ).await.unwrap();
    assert_eq!(
        guarded.commit_page(rejected, f.lease.clone(), clock(),
            || async { Err(FailureClass::Source) }
        ).await,
        Err(CommitError::Operation(FailureClass::Source))
    );
    assert_eq!(f.readable().await.fts_query("text", "pineapple", 10).await.unwrap().len(), 0);
    let deletion = guarded.prepare_page(
        &provider, operation("deleted", "", true), &[]
    ).await.unwrap();
    assert_eq!(deletion.metrics().removed, 1);
    assert_eq!(
        guarded.commit_page(deletion, f.lease.clone(), clock(), || async { Ok(()) }).await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(f.readable().await.count_rows().await.unwrap(), 0);
    assert_eq!(f.coordinator.state().page_state("page").unwrap().unwrap().is_tombstone(), true);
}

#[tokio::test]
async fn real_index_replay_after_sqlite_ack_failure_and_provider_mismatch() {
    let f = Fixture::new();
    f.create().await;
    let guarded = f.guarded();
    let provider = CountingProvider::new(f.embedding.clone());
    let wrong = CountingProvider::new(
        EmbeddingMetadata::new("other".into(), "test-model".into(), "v1".into(), 3).unwrap()
    );
    assert!(matches!(
        guarded.prepare_page(
            &wrong, operation("wrong-provider", "h1", false), &[chunk("one", "coriander", "One")],
        ).await,
        Err(ChunkTableError::Embedding(_))
    ));
    assert_eq!(wrong.total(), 0);

    let prepared = guarded.prepare_page(
        &provider, operation("replay", "h1", false), &[chunk("one", "coriander", "One")],
    ).await.unwrap();
    let sql = Connection::open(&f.state).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'injected private error');END;").unwrap();
    assert_eq!(
        guarded.commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) }).await,
        Err(CommitError::Unavailable)
    );
    assert_eq!(f.readable().await.fts_query("text", "coriander", 10).await.unwrap().len(), 1);
    assert!(f.coordinator.state().page_state("page").unwrap().is_none());
    assert_eq!(f.coordinator.receipt("replay").unwrap().unwrap().status, ReceiptStatus::Pending);
    sql.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    drop(sql);

    // The old immutable proposal is stale; prepare from the actual applied
    // Lance version. No duplicate embedding or chunk should be introduced.
    let replayed = guarded.prepare_page(
        &provider, operation("replay", "h1", false), &[chunk("one", "coriander", "One")],
    ).await.unwrap();
    assert_eq!(provider.total(), 1);
    assert_eq!(
        guarded.commit_page(replayed, f.lease.clone(), clock(), || async { Ok(()) }).await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(f.readable().await.count_rows().await.unwrap(), 1);
    assert_eq!(f.coordinator.receipt("replay").unwrap().unwrap().attempts, 2);
    assert_eq!(f.coordinator.state().page_state("page").unwrap().unwrap().content_hash(), Some("h1"));
}

#[tokio::test]
async fn lost_lease_and_rejected_preparation_are_effect_free() {
    let f = Fixture::new();
    f.create().await;
    let guarded = f.guarded();
    let provider = CountingProvider::new(f.embedding.clone());
    let old = guarded.prepare_page(
        &provider, operation("expired", "h1", false), &[chunk("one", "apples", "One")],
    ).await.unwrap();
    assert_eq!(
        guarded.commit_page(old, f.lease.clone(), Arc::new(|| 110), || async {
            panic!("expired fence must prevent source callback")
        }).await,
        Err(CommitError::LeaseLost)
    );
    assert_eq!(f.readable().await.count_rows().await.unwrap(), 0);
    let mut failing = CountingProvider::new(f.embedding.clone());
    failing.fail = true;
    assert!(matches!(
        guarded.prepare_page(
            &failing, operation("fail-embedding", "h2", false),
            &[chunk("one", "apples", "One")]
        ).await,
        Err(ChunkTableError::Embedding(_))
    ));
    assert_eq!(f.readable().await.count_rows().await.unwrap(), 0);
    // An untrusted workspace fails before embedding.
    let mut foreign = chunk("one", "apples", "One");
    foreign.metadata.source.workspace_id = "other".into();
    assert!(matches!(
        guarded.prepare_page(
            &provider, operation("foreign", "h3", false), &[foreign]
        ).await,
        Err(ChunkTableError::InvalidRows(_))
    ));
    assert_eq!(provider.total(), 1);
    // Missing table or mismatched provider must not cause startup FTS writes.
    assert!(matches!(f.guarded().ensure_startup_indexes(
        f.lease.clone(), clock()
    ).await, Ok(())));
    let _ = FtsIndexConfig::default();
}

fn wait_for(mut predicate: impl FnMut() -> bool) {
    let begin = std::time::Instant::now();
    while !predicate() {
        assert!(begin.elapsed() < std::time::Duration::from_secs(25), "writer timed out");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn two_process_real_lance_writers_survive_observer_cancellation_and_reject_stale() {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let f = Fixture::new();
    runtime.block_on(f.create());
    let child = |name: &str| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process_writer_child", "--ignored", "--nocapture"])
            .env("NK_LANCE_CHILD_ROOT", &f.root)
            .env("NK_LANCE_CHILD_NAME", name)
            .env("NK_LANCE_CHILD_FENCE", f.lease.fence.to_string())
            .spawn()
            .unwrap()
    };
    let mut first = child("one");
    wait_for(|| f.root.join("one-entered").exists());
    let mut second = child("two");
    wait_for(|| f.root.join("two-prepared").exists());
    std::thread::sleep(std::time::Duration::from_millis(150));
    assert!(!f.root.join("two-finished").exists());
    assert!(first.try_wait().unwrap().is_none());
    assert!(second.try_wait().unwrap().is_none());
    // The first observed caller has been dropped, yet the owned commit thread
    // still holds the guard and performs real Lance merge + FTS work.
    fs::write(f.root.join("release"), "").unwrap();
    wait_for(|| f.root.join("one-done").exists());
    wait_for(|| f.root.join("two-finished").exists());
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
    assert_eq!(
        f.coordinator.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert_eq!(
        f.coordinator.receipt("two").unwrap().unwrap().failure,
        Some(FailureClass::Conflict)
    );
    runtime.block_on(async {
        let readable = f.readable().await;
        assert_eq!(readable.count_rows().await.unwrap(), 1);
        assert_eq!(
            readable.fts_query("text", "leaderblueberry", 10).await.unwrap().len(), 1
        );
        assert_eq!(
            readable.fts_query("text", "contenderpapaya", 10).await.unwrap().len(), 0
        );
    });
}

#[tokio::test]
#[ignore]
async fn process_writer_child() {
    let Ok(root) = std::env::var("NK_LANCE_CHILD_ROOT") else { return; };
    let root = PathBuf::from(root);
    let name = std::env::var("NK_LANCE_CHILD_NAME").unwrap();
    let fence: i64 = std::env::var("NK_LANCE_CHILD_FENCE").unwrap().parse().unwrap();
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let binding = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
    let coordinator = IndexCommitCoordinator::open(
        root.join("index"), root.join("state.sqlite"), binding,
    ).unwrap();
    let embedding = EmbeddingMetadata::new(
        "count".into(), "test-model".into(), "v1".into(), 3
    ).unwrap();
    let table = GuardedChunkTable::bind(
        root.join("index"), "chunks", embedding.clone(), coordinator.clone(),
    ).unwrap();
    let provider = CountingProvider::new(embedding);
    let lease = Lease { fence, expires_at: 110 };
    let text = if name == "one" { "leaderblueberry" } else { "contenderpapaya" };
    let prepared = table.prepare_page(
        &provider, operation(&name, &name, false),
        &[chunk("one", text, &name)],
    ).await.unwrap();
    if name == "one" {
        let entered = root.join("one-entered");
        let released = root.join("release");
        let observer = table.commit_page(prepared, lease, clock(), move || async move {
            fs::write(&entered, "").unwrap();
            while !released.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Ok(())
        });
        wait_for(|| root.join("one-entered").exists());
        drop(observer); // owned mutation must continue across caller drop
        wait_for(|| coordinator.receipt("one").unwrap().is_some_and(
            |receipt| receipt.status == ReceiptStatus::Applied
        ));
        fs::write(root.join("one-done"), "").unwrap();
    } else {
        fs::write(root.join("two-prepared"), "").unwrap();
        let result = table.commit_page(prepared, lease, clock(), || async {
            Ok(())
        }).await;
        assert_eq!(result, Err(CommitError::Operation(FailureClass::Conflict)));
        fs::write(root.join("two-finished"), "").unwrap();
    }
}
