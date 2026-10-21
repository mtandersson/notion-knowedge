//! Production request/root guards plus real OAuth, provider, SQLite and Lance.
use super::*;
use crate::{
    file_refresh::{AuthorizedFileIntent, reconcile_clocked},
    grant_refresh::{GrantRefreshSettings, GrantRefreshWorker},
};
use notion_knowledge_core::{
    backend::{AppendPageContent, NotionRead, PageId},
    chunking::ChunkConfig,
    lifecycle::LifecycleScope,
    write_refresh::{IndexStatus, MutationRefreshQueue},
};
use notion_knowledge_retrieval::{
    chunks::GuardedChunkTable,
    commit::{CommitBinding, IndexCommitCoordinator},
    file_refresh_intents::FileIntentStatus,
    mutation_refresh::GrantMutationRefreshQueue,
    planner::production_scope,
};
use std::{sync::atomic::AtomicI64, time::Duration};
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
async fn authorization(h: &Harness) -> notion_knowledge_mcp::HttpAuthorization {
    let code = h
        .approved_code_with_scope("knowledge:read knowledge:write")
        .await;
    let token = resource::redeem(h, &code).await;
    h.login.local_resource_authorization(&token).unwrap()
}
fn open(seed: &content_index::Seeded) -> (Arc<IndexCommitCoordinator>, GuardedChunkTable) {
    let policy = production_scope(&scope()).unwrap();
    let binding = CommitBinding::new("chunks", WORKSPACE, &policy, "1", 1).unwrap();
    let coordinator =
        IndexCommitCoordinator::open_existing(&seed.index, &seed.state, binding).unwrap();
    let guard = GuardedChunkTable::bind(
        &seed.index,
        "chunks",
        seed.provider.metadata().clone(),
        coordinator.clone(),
    )
    .unwrap();
    (coordinator, guard)
}
fn worker(
    h: &Harness,
    seed: &content_index::Seeded,
    coordinator: Arc<IndexCommitCoordinator>,
    guard: GuardedChunkTable,
) -> GrantRefreshWorker {
    GrantRefreshWorker::new(
        h.login.callback.as_ref().unwrap().credentials.clone(),
        GrantRefreshSettings {
            scope: scope(),
            chunk_config: ChunkConfig::default(),
            alias_sources: None,
            timeout: Duration::from_secs(20),
        },
        guard,
        coordinator,
        seed.provider.clone(),
    )
    .unwrap()
}
async fn intent(
    h: &Harness,
    auth: &notion_knowledge_mcp::HttpAuthorization,
    coordinator: &Arc<IndexCommitCoordinator>,
    clock: Arc<AtomicI64>,
) -> Arc<AuthorizedFileIntent> {
    AuthorizedFileIntent::begin_clocked(
        coordinator.state().clone(),
        &h.login.callback.as_ref().unwrap().credentials,
        auth,
        &scope(),
        &PageId(PAGE.into()),
        CancellationToken::new(),
        10,
        Arc::new(move || clock.load(Ordering::SeqCst)),
    )
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_provider_effect_crossing_expiry_stays_quarantined_until_explicit_reopen_reconciliation()
 {
    let h = harness().await;
    *h.document.lock().unwrap() = Some(("Saffron private evidence".into(), 0));
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let (coordinator, guard) = open(&seed);
    let auth = authorization(&h).await;
    let clock = Arc::new(AtomicI64::new(0));
    let owner = intent(&h, &auth, &coordinator, clock.clone()).await;
    let key = owner.key().clone();
    let authority = owner.authority().clone();
    let held = h.mutation_gate.clone().acquire_owned().await.unwrap();
    let client = owner.client().unwrap();
    let pending = tokio::spawn(async move {
        client
            .append_content(AppendPageContent {
                page_id: PageId(PAGE.into()),
                markdown: " late source evidence".into(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), h.mutation_started.notified())
        .await
        .unwrap();
    assert!(owner.settle(false).is_err());
    clock.store(10, Ordering::SeqCst);
    assert!(owner.renew(10).is_err());
    assert!(owner.client().is_err());
    assert!(owner.settle(true).is_err());
    let reads = h.page_reads.load(Ordering::SeqCst);
    assert!(
        !worker(&h, &seed, coordinator.clone(), guard.clone())
            .tick(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(h.page_reads.load(Ordering::SeqCst), reads);
    assert!(
        reconcile_clocked(
            coordinator.state(),
            &h.login.callback.as_ref().unwrap().credentials,
            &auth,
            &scope(),
            &key,
            10
        )
        .await
        .is_err(),
        "a locally-live remote effect is not quiescent"
    );
    drop(held);
    assert!(
        pending.await.unwrap().is_err(),
        "expired owner cannot acknowledge late effect"
    );
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    assert!(
        h.document
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .contains("late source evidence")
    );
    assert_eq!(
        coordinator
            .state()
            .file_refresh_intent_status(&key, &authority, 10)
            .unwrap(),
        Some(FileIntentStatus::RecoveryRequired)
    );
    drop(owner);
    drop(guard);
    drop(coordinator);
    let (coordinator, guard) = open(&seed);
    assert!(
        !worker(&h, &seed, coordinator.clone(), guard.clone())
            .tick(CancellationToken::new())
            .await
            .unwrap()
    );
    reconcile_clocked(
        coordinator.state(),
        &h.login.callback.as_ref().unwrap().credentials,
        &auth,
        &scope(),
        &key,
        10,
    )
    .await
    .unwrap();
    assert!(
        worker(&h, &seed, coordinator.clone(), guard.clone())
            .tick(CancellationToken::new())
            .await
            .unwrap()
    );
    let queue = GrantMutationRefreshQueue::new(coordinator.state().clone(), authority).unwrap();
    assert_eq!(queue.status(&key).unwrap(), Some(IndexStatus::Completed));
    assert!(
        guard
            .page_chunks(PAGE)
            .await
            .unwrap()
            .iter()
            .any(|chunk| chunk.text.contains("late source evidence"))
    );
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_guard_tracks_mutation_attempts_and_refuses_false_no_effect_proofs() {
    for mode in ["read", "success", "unknown"] {
        let h = harness().await;
        let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let (coordinator, _) = open(&seed);
        let auth = authorization(&h).await;
        let owner = intent(&h, &auth, &coordinator, Arc::new(AtomicI64::new(0))).await;
        let client = owner.client().unwrap();
        if mode == "read" {
            NotionRead::fetch_page(&client, &PageId(PAGE.into()))
                .await
                .unwrap();
            owner.settle(false).unwrap();
            owner.settle(false).unwrap();
            assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
        } else {
            if mode == "unknown" {
                h.mutation_status.store(500, Ordering::SeqCst);
            }
            let result = client
                .append_content(AppendPageContent {
                    page_id: PageId(PAGE.into()),
                    markdown: "potential effect".into(),
                })
                .await;
            assert_eq!(result.is_ok(), mode == "success");
            assert!(owner.settle(false).is_err());
            owner.settle(true).unwrap();
            owner.settle(true).unwrap();
            assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_original_grant_is_never_adopted_by_replacement_or_revoked_request() {
    for replacement in [false, true] {
        let h = harness().await;
        let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let (coordinator, guard) = open(&seed);
        let auth = authorization(&h).await;
        let owner = intent(&h, &auth, &coordinator, Arc::new(AtomicI64::new(0))).await;
        let key = owner.key().clone();
        let authority = owner.authority().clone();
        if replacement {
            h.approved_code().await;
        } else {
            h.clock.store(4000, Ordering::SeqCst);
        }
        assert!(owner.client().is_err());
        assert!(owner.renew(10).is_err());
        assert!(owner.settle(true).is_err());
        let reads = h.page_reads.load(Ordering::SeqCst);
        assert!(
            reconcile_clocked(
                coordinator.state(),
                &h.login.callback.as_ref().unwrap().credentials,
                &auth,
                &scope(),
                &key,
                10
            )
            .await
            .is_err()
        );
        assert!(
            !worker(&h, &seed, coordinator.clone(), guard)
                .tick(CancellationToken::new())
                .await
                .unwrap()
        );
        assert_eq!(h.page_reads.load(Ordering::SeqCst), reads);
        assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
        assert_eq!(
            coordinator
                .state()
                .file_refresh_intent_status(&key, &authority, 10)
                .unwrap(),
            Some(FileIntentStatus::RecoveryRequired)
        );
    }
}
