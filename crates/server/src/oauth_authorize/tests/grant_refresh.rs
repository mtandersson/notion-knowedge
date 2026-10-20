//! Real OAuth grants, durable write/refresh stores and guarded SQLite/Lance work.
use super::*;
use crate::grant_refresh::{AuthorizedRefreshQueue, GrantRefreshSettings, GrantRefreshWorker};
use notion_knowledge_core::{
    backend::PageId,
    chunking::ChunkConfig,
    create::{CreateRequest, create_once_and_refresh},
    embedding::{EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    lifecycle::LifecycleScope,
    root_scope::RootScopeGate,
    sync_state::SyncStateStore,
    write_refresh::{IndexStatus, MutationRefreshQueue, WriteCompletion},
};
use notion_knowledge_retrieval::{
    chunks::GuardedChunkTable,
    commit::{CommitBinding, IndexCommitCoordinator},
    idempotency::SqliteIdempotencyStore,
    mutation_refresh::GrantMutationRefreshQueue,
    planner::production_scope,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: Default::default(),
    }
}
struct Model {
    metadata: EmbeddingMetadata,
    fail: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
}
impl EmbeddingProvider for Model {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, values: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(EmbeddingError::Unavailable);
            }
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                let _permit = self.release.acquire().await.unwrap();
            }
            Ok(values.iter().map(|_| vec![1., 0., 0.]).collect())
        })
    }
}
struct Index {
    coordinator: Arc<IndexCommitCoordinator>,
    guard: GuardedChunkTable,
    model: Arc<Model>,
    seeded: content_index::Seeded,
}
impl Index {
    async fn new(h: &Harness) -> Self {
        let seeded = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let model = Arc::new(Model {
            metadata: seeded.provider.metadata().clone(),
            fail: false.into(),
            pause: false.into(),
            entered: Default::default(),
            release: tokio::sync::Semaphore::new(0),
            calls: 0.into(),
        });
        Self::open(seeded, model)
    }
    fn open(seeded: content_index::Seeded, model: Arc<Model>) -> Self {
        let policy = production_scope(&scope()).unwrap();
        let binding = CommitBinding::new("chunks", WORKSPACE, &policy, "1", 1).unwrap();
        let coordinator =
            IndexCommitCoordinator::open_existing(&seeded.index, &seeded.state, binding).unwrap();
        let guard = GuardedChunkTable::bind(
            &seeded.index,
            "chunks",
            model.metadata.clone(),
            coordinator.clone(),
        )
        .unwrap();
        Self {
            coordinator,
            guard,
            model,
            seeded,
        }
    }
    fn worker(&self, h: &Harness) -> GrantRefreshWorker {
        GrantRefreshWorker::new(
            h.login.callback.as_ref().unwrap().credentials.clone(),
            GrantRefreshSettings {
                scope: scope(),
                chunk_config: ChunkConfig::default(),
                alias_sources: None,
                timeout: Duration::from_secs(20),
            },
            self.guard.clone(),
            self.coordinator.clone(),
            self.model.clone(),
        )
        .unwrap()
    }
}
async fn harness() -> Harness {
    Harness::with_authority(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        None,
        Duration::ZERO,
        Some(scope()),
        true,
    )
    .await
}
async fn authorize(h: &Harness) -> (String, notion_knowledge_mcp::HttpAuthorization) {
    let code = h
        .approved_code_with_scope("knowledge:read knowledge:write")
        .await;
    let token = resource::redeem(h, &code).await;
    let auth = h.login.local_resource_authorization(&token).unwrap();
    (token, auth)
}
fn request() -> CreateRequest {
    CreateRequest {
        parent_page_id: PageId(ROOT.into()),
        title: "Saffron private evidence".into(),
        markdown: "Saffron private evidence fresh content".into(),
        idempotency_key: "durable-oauth-create-123".into(),
    }
}
async fn create(
    h: &Harness,
    index: &Index,
    auth: &notion_knowledge_mcp::HttpAuthorization,
) -> (WriteCompletion, AuthorizedRefreshQueue) {
    let runtime = &h.login.callback.as_ref().unwrap().credentials;
    let queue =
        AuthorizedRefreshQueue::bind(index.coordinator.state().clone(), runtime, auth, &scope())
            .await
            .unwrap();
    let source = h
        .login
        .content_factory(scope())
        .unwrap()
        .bind(auth)
        .unwrap()
        .source;
    let gate = RootScopeGate::new(source.clone(), scope()).unwrap();
    let path = h._dir.path().join("source-write-ledger.sqlite");
    let ledger = if path.exists() {
        SqliteIdempotencyStore::open_existing(path).unwrap()
    } else {
        SqliteIdempotencyStore::initialize_new(path).unwrap()
    };
    let result = create_once_and_refresh(
        source.as_ref(),
        source.mutation_ports().unwrap(),
        &gate,
        &ledger,
        &queue,
        request(),
    )
    .await
    .unwrap();
    (result, queue)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_write_refresh_survives_restart_and_session_logout_with_no_repeat_effect() {
    let h = harness().await;
    let index = Index::new(&h).await;
    let (token, auth) = authorize(&h).await;
    let (written, request_queue) = create(&h, &index, &auth).await;
    assert_eq!(written.index, IndexStatus::Queued);
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    let authority = request_queue.authority().clone();
    let before = index.coordinator.state().page_state(PAGE).unwrap().unwrap();
    let Index {
        coordinator,
        guard,
        seeded,
        model,
    } = index;
    drop(guard);
    drop(coordinator);
    let index = Index::open(seeded, model);
    h.login.flow.revoke("chatgpt", &token).unwrap(); // session-only logout, not sealed-grant removal
    assert!(request_queue.status(&written.refresh).is_err());
    let durable =
        GrantMutationRefreshQueue::new(index.coordinator.state().clone(), authority).unwrap();
    assert_eq!(
        durable.status(&written.refresh).unwrap(),
        Some(IndexStatus::Queued)
    );
    assert!(
        index
            .worker(&h)
            .tick(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(
        durable.status(&written.refresh).unwrap(),
        Some(IndexStatus::Completed)
    );
    let after = index.coordinator.state().page_state(PAGE).unwrap().unwrap();
    assert_ne!(before.content_hash(), after.content_hash());
    let chunks = index.guard.page_chunks(PAGE).await.unwrap();
    assert!(chunks.iter().any(|c| c.text.contains("fresh content")));
    index.guard.search_snapshot().await.unwrap();
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    assert!(
        !index
            .worker(&h)
            .tick(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(written.receipt.page_id, PAGE);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_index_refresh_retries_under_same_grant_without_repeating_verified_create() {
    let h = harness().await;
    let index = Index::new(&h).await;
    let (_, auth) = authorize(&h).await;
    let (written, queue) = create(&h, &index, &auth).await;
    let before = index.coordinator.state().page_state(PAGE).unwrap();
    index.model.fail.store(true, Ordering::SeqCst);
    index
        .worker(&h)
        .tick(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        queue.status(&written.refresh).unwrap(),
        Some(IndexStatus::Failed)
    );
    assert_eq!(index.coordinator.state().page_state(PAGE).unwrap(), before);
    let (replayed, _) = create(&h, &index, &auth).await;
    assert!(replayed.replayed);
    assert_eq!(replayed.receipt, written.receipt);
    assert_eq!(replayed.index, IndexStatus::Failed);
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    let durable = GrantMutationRefreshQueue::new(
        index.coordinator.state().clone(),
        queue.authority().clone(),
    )
    .unwrap();
    durable.retry(&written.refresh).unwrap();
    index.model.fail.store(false, Ordering::SeqCst);
    index
        .worker(&h)
        .tick(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        durable.status(&written.refresh).unwrap(),
        Some(IndexStatus::Completed)
    );
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_upstream_credentials_refresh_same_grant_but_failure_preserves_write_receipt() {
    for available in [false, true] {
        let h = harness().await;
        let index = Index::new(&h).await;
        let (_, auth) = authorize(&h).await;
        let (written, queue) = create(&h, &index, &auth).await;
        let authority = queue.authority().clone();
        let durable =
            GrantMutationRefreshQueue::new(index.coordinator.state().clone(), authority.clone())
                .unwrap();
        h.clock.store(4000, Ordering::SeqCst);
        h.available.store(available, Ordering::SeqCst);
        index
            .worker(&h)
            .tick(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            durable.status(&written.refresh).unwrap(),
            Some(if available {
                IndexStatus::Completed
            } else {
                IndexStatus::Failed
            })
        );
        let stored = h.store.load_for_refresh().unwrap().unwrap();
        assert_eq!(stored.grant_id(), authority.grant_id());
        assert_eq!(stored.epoch(), authority.epoch());
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        assert_eq!(written.receipt.page_id, PAGE);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_grant_and_scope_change_never_reinterpret_queued_work() {
    for replacement in [false, true] {
        let h = harness().await;
        let index = Index::new(&h).await;
        let (_, auth) = authorize(&h).await;
        let (written, queue) = create(&h, &index, &auth).await;
        let before = h.page_reads.load(Ordering::SeqCst);
        let durable = GrantMutationRefreshQueue::new(
            index.coordinator.state().clone(),
            queue.authority().clone(),
        )
        .unwrap();
        if replacement {
            h.approved_code().await;
        } else {
            // A different policy is persisted by trusted admission but cannot
            // be consumed under this worker's current roots/exclusions.
            let mut changed = scope();
            changed.exclusions.page_ids.insert(PAGE.into());
            let foreign = AuthorizedRefreshQueue::bind(
                index.coordinator.state().clone(),
                &h.login.callback.as_ref().unwrap().credentials,
                &auth,
                &changed,
            )
            .await
            .unwrap();
            let mut other = written.refresh.clone();
            other.mutation_digest = "f".repeat(64);
            foreign.admit(&other).unwrap();
            // Complete original work; changed-policy row then fails without reads.
            index
                .worker(&h)
                .tick(CancellationToken::new())
                .await
                .unwrap();
            let reads = h.page_reads.load(Ordering::SeqCst);
            index
                .worker(&h)
                .tick(CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(foreign.status(&other).unwrap(), Some(IndexStatus::Failed));
            assert_eq!(h.page_reads.load(Ordering::SeqCst), reads);
            continue;
        }
        index
            .worker(&h)
            .tick(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            durable.status(&written.refresh).unwrap(),
            Some(IndexStatus::Failed)
        );
        assert_eq!(h.page_reads.load(Ordering::SeqCst), before);
        durable.retry(&written.refresh).unwrap();
        index
            .worker(&h)
            .tick(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            durable.status(&written.refresh).unwrap(),
            Some(IndexStatus::Failed)
        );
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_expiry_and_cancel_during_embedding_fence_native_commit() {
    for invalidation in ["replacement", "expiry", "cancel", "stop"] {
        let h = harness().await;
        let index = Index::new(&h).await;
        let (_, auth) = authorize(&h).await;
        let (written, queue) = create(&h, &index, &auth).await;
        let durable = GrantMutationRefreshQueue::new(
            index.coordinator.state().clone(),
            queue.authority().clone(),
        )
        .unwrap();
        let before = index.coordinator.state().page_state(PAGE).unwrap();
        index.model.pause.store(true, Ordering::SeqCst);
        let worker = index.worker(&h);
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let pending = tokio::spawn(async move { worker.tick(stopped).await });
        tokio::time::timeout(Duration::from_secs(5), index.model.entered.notified())
            .await
            .unwrap();
        match invalidation {
            "replacement" => {
                h.approved_code().await;
            }
            "expiry" => {
                h.clock.store(4000, Ordering::SeqCst);
            }
            "cancel" => {
                durable.cancel(&written.refresh).unwrap();
            }
            _ => stop.cancel(),
        }
        index.model.release.add_permits(1);
        pending.await.unwrap().unwrap();
        index.coordinator.drain_owned().await.unwrap();
        assert_eq!(
            index.coordinator.state().page_state(PAGE).unwrap(),
            before,
            "{invalidation}"
        );
        assert!(
            !index
                .guard
                .page_chunks(PAGE)
                .await
                .unwrap()
                .iter()
                .any(|c| c.text.contains("fresh content")),
            "{invalidation}"
        );
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        if invalidation != "stop" {
            assert_eq!(
                durable.status(&written.refresh).unwrap(),
                Some(IndexStatus::Failed)
            );
        }
    }
}
