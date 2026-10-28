//! Shared production authority for webhook, manual and periodic reconciliation callers.
#![cfg(all(unix, feature = "webhook-worker"))]
use crate::webhook_worker::{WorkerSettings, runtime::now_ms};
use notion_knowledge_core::{
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    lifecycle::LifecycleScope,
    reconciliation::{ReconciliationJournal, ReconciliationScope},
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{
    chunks::GuardedChunkTable,
    commit::{CommitBinding, IndexCommitCoordinator},
    planner::{AuthoritativeReconciliationPlanner, production_scope},
    refresh::AuthoritativePageRefresh,
    runner::AuthoritativeReconciliationRunner,
};
use std::{path::Path, sync::Arc};

pub struct IndexRuntime {
    pub coordinator: Arc<IndexCommitCoordinator>,
    pub scope: ReconciliationScope,
    pub handler: AuthoritativePageRefresh,
    pub runner: AuthoritativeReconciliationRunner,
    pub provider: Arc<dyn EmbeddingProvider>,
    alias_sources: notion_knowledge_core::aliases::AliasSources,
}
impl IndexRuntime {
    /// Production startup for a disposable cache. Compatibility errors reset
    /// derived state; permission and arbitrary storage failures remain errors.
    /// Serving is not composed until a required authoritative resync completes.
    pub async fn recover(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self, &'static str> {
        Self::recover_with_reset(
            settings,
            state,
            scope,
            source,
            provider,
            false,
            OpenMode::Initialize,
        )
        .await
    }

    pub async fn recover_for_reads(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self, &'static str> {
        Self::recover_with_reset(
            settings,
            state,
            scope,
            source,
            provider,
            false,
            OpenMode::ReadOnly,
        )
        .await
    }

    pub async fn reset_and_resync(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self, &'static str> {
        Self::recover_with_reset(
            settings,
            state,
            scope,
            source,
            provider,
            true,
            OpenMode::Initialize,
        )
        .await
    }

    async fn recover_with_reset(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
        force: bool,
        healthy_mode: OpenMode,
    ) -> Result<Self, &'static str> {
        use notion_knowledge_retrieval::{commit::CommitError, sync_state::SqliteSyncStateStore};
        let exists = |p: &Path| p.try_exists().map_err(|_| "cache path unavailable");
        let state_exists = exists(state)?;
        let pending = if state_exists {
            SqliteSyncStateStore::open_read_only(state)
                .map_err(|_| "operational state unavailable")?
                .cache_rebuild_pending()
                .map_err(|_| "operational state unavailable")?
        } else {
            false
        };
        let journal_path = notion_knowledge_retrieval::commit::transition::journal_path(state)
            .map_err(|_| "invalid state path")?;
        let mut rebuild = force
            || pending
            || !state_exists
            || exists(&journal_path)?
            || !exists(&settings.index)?
            || !exists(&settings.index.join(".nk-operational-binding"))?
            || !exists(&settings.index.join("chunks.lance"))?;
        if !rebuild {
            let journal = production_scope(&scope).map_err(|_| "invalid index scope")?;
            let binding = CommitBinding::new(
                "chunks",
                &scope.workspace_id,
                &journal,
                &scope.generation.to_string(),
                scope.generation,
            )
            .map_err(|_| "invalid index binding")?;
            match IndexCommitCoordinator::open_read_only(&settings.index, state, binding) {
                Err(CommitError::BindingMismatch) => rebuild = true,
                Err(_) => return Err("index authority unavailable"),
                Ok(coordinator) => match GuardedChunkTable::bind(
                    &settings.index,
                    "chunks",
                    provider.metadata().clone(),
                    coordinator,
                ) {
                    Err(error) if error.is_incompatible_index() => rebuild = true,
                    Err(_) => return Err("index unavailable"),
                    Ok(table) => match table.search_snapshot().await {
                        Err(error) if error.is_incompatible_index() => rebuild = true,
                        Err(_) => return Err("index unavailable"),
                        Ok(_) => {}
                    },
                },
            }
        }
        if rebuild {
            let index = settings.index.clone();
            let state = state.to_path_buf();
            tokio::task::spawn_blocking(move || {
                IndexCommitCoordinator::reset_cache(&index, &state, now_ms() / 1000)
            })
            .await
            .map_err(|_| "cache reset unavailable")?
            .map_err(|_| "cache reset unavailable")?;
        }
        let runtime = Self::compose(
            settings,
            state,
            scope,
            source,
            provider,
            if rebuild {
                OpenMode::Initialize
            } else {
                healthy_mode
            },
        )
        .await?;
        if rebuild {
            runtime.resync_cache(settings.timeout_seconds).await?;
        }
        Ok(runtime)
    }

    async fn resync_cache(&self, timeout_seconds: u64) -> Result<(), &'static str> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        struct Attempt(Arc<AtomicBool>);
        impl Drop for Attempt {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let state = self.coordinator.state();
        let mut lease = state
            .acquire_lease(now_ms() / 1000, timeout_seconds as i64 + 30)
            .map_err(|_| "cache sync writer busy")?;
        let result = loop {
            let expired = Arc::new(AtomicBool::new(false));
            let _attempt = Attempt(expired.clone());
            let flag = expired.clone();
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                if flag.load(Ordering::SeqCst) {
                    i64::MAX
                } else {
                    now_ms() / 1000
                }
            });
            let attempt = tokio::time::timeout(
                Duration::from_secs(timeout_seconds),
                self.runner
                    .apply_next("cache-reset", lease.clone(), clock, self.provider.as_ref()),
            )
            .await;
            let progress = match attempt {
                Ok(Ok(progress)) => progress,
                _ => {
                    expired.store(true, Ordering::SeqCst);
                    let _ = self.coordinator.drain_owned().await;
                    break Err("cache resync failed; retry startup");
                }
            };
            if !progress.failed.is_empty()
                || !progress.planning_failures.is_empty()
                || !progress.run_failures.is_empty()
            {
                break Err("cache resync incomplete; retry startup");
            }
            if progress.pending.is_empty() {
                match state.renew_lease(&lease, now_ms() / 1000, timeout_seconds as i64 + 30) {
                    Ok(next) => lease = next,
                    Err(_) => break Err("cache sync lease unavailable"),
                }
                let flag = expired.clone();
                let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                    if flag.load(Ordering::SeqCst) {
                        i64::MAX
                    } else {
                        now_ms() / 1000
                    }
                });
                let ready = tokio::time::timeout(
                    Duration::from_secs(timeout_seconds),
                    prepare_search_if_missing(
                        &self.coordinator,
                        self.provider.metadata(),
                        lease.clone(),
                        clock,
                    ),
                )
                .await;
                if !matches!(ready, Ok(Ok(()))) {
                    expired.store(true, Ordering::SeqCst);
                    let _ = self.coordinator.drain_owned().await;
                    break Err("rebuilt search index unavailable");
                }
                break state
                    .finish_cache_rebuild(&lease, now_ms() / 1000)
                    .map_err(|_| "cache sync state unavailable");
            }
            match state.renew_lease(&lease, now_ms() / 1000, timeout_seconds as i64 + 30) {
                Ok(next) => lease = next,
                Err(_) => break Err("cache sync lease unavailable"),
            }
        };
        let _ = state.release_lease(&lease, now_ms() / 1000);
        result
    }

    /// Local operator composition only; no caller may choose a workspace/root.
    /// Candidates remain subject to authoritative content authorization.
    pub fn lookup_alias(
        &self,
        alias: &str,
    ) -> Result<Vec<String>, notion_knowledge_retrieval::commit::CommitError> {
        self.coordinator
            .lookup_canonical_alias(&self.alias_sources, alias)
    }

    /// Trusted dependency seam: production supplies an authenticated Notion
    /// client and validated offline Qwen; fixtures inject these typed ports.
    /// There is no environment-selectable test transport or provider.
    pub async fn compose(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
        mode: OpenMode,
    ) -> Result<Self, &'static str> {
        let journal = production_scope(&scope).map_err(|_| "invalid index scope")?;
        let binding = CommitBinding::new(
            "chunks",
            &scope.workspace_id,
            &journal,
            &scope.generation.to_string(),
            scope.generation,
        )
        .map_err(|_| "invalid index binding")?;
        let index = settings.index.clone();
        let state = state.to_path_buf();
        // Existing-state commands validate authority AND vector metadata before
        // opening a writable SQLite connection or acquiring a writer lease.
        let coordinator = if mode == OpenMode::Initialize {
            tokio::task::spawn_blocking(move || {
                IndexCommitCoordinator::initialize(index, state, binding)
            })
            .await
            .map_err(|_| "index authority unavailable")?
            .map_err(|_| "index authority unavailable or incompatible")?
        } else {
            let checked = IndexCommitCoordinator::open_read_only(&index, &state, binding.clone())
                .map_err(|_| "existing index authority unavailable or incompatible")?;
            let guard = GuardedChunkTable::bind(
                &index,
                "chunks",
                provider.metadata().clone(),
                checked.clone(),
            )
            .map_err(|_| "index binding incompatible")?;
            guard
                .inventory_version()
                .await
                .map_err(|_| "existing index unavailable or incompatible")?;
            if mode == OpenMode::ReadOnly {
                checked
            } else {
                tokio::task::spawn_blocking(move || {
                    IndexCommitCoordinator::open_existing(index, state, binding)
                })
                .await
                .map_err(|_| "index authority unavailable")?
                .map_err(|_| "index authority unavailable or incompatible")?
            }
        };
        let guard = GuardedChunkTable::bind(
            &settings.index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
        )
        .map_err(|_| "index binding incompatible")?;
        if mode == OpenMode::Initialize {
            let lease = coordinator
                .state()
                .acquire_lease(now_ms() / 1000, 330)
                .map_err(|_| "index writer busy")?;
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
            let result = if settings.index.join("chunks.lance").exists() {
                guard.ensure_startup_indexes(lease.clone(), clock).await
            } else {
                guard.create_empty(lease.clone(), clock).await
            };
            let _ = coordinator.state().release_lease(&lease, now_ms() / 1000);
            result.map_err(|_| "index unavailable or incompatible")?;
        }
        let handler = AuthoritativePageRefresh::new(
            source.clone(),
            guard.clone(),
            coordinator.clone(),
            scope.clone(),
            &journal,
            ChunkConfig {
                target_chars: 1000,
                overlap_chars: 100,
            },
        )
        .map_err(|_| "authoritative handler incompatible")?
        .with_alias_sources(settings.alias_sources.clone());
        let planner =
            AuthoritativeReconciliationPlanner::new(source, guard, coordinator.clone(), scope)
                .map_err(|_| "authoritative planner incompatible")?
                .with_alias_sources(settings.alias_sources.clone());
        let runner = AuthoritativeReconciliationRunner::new(
            planner,
            handler.clone(),
            coordinator.clone(),
            journal.clone(),
        )
        .map_err(|_| "authoritative runner incompatible")?;
        Ok(Self {
            coordinator,
            scope: journal,
            handler,
            runner,
            provider,
            alias_sources: settings.alias_sources.clone(),
        })
    }
}
/// Finish derived search maintenance after authoritative sync, including the
/// first populated page after an empty cache. A healthy snapshot is a no-op.
pub(crate) async fn prepare_search_if_missing(
    coordinator: &Arc<IndexCommitCoordinator>,
    embedding: &notion_knowledge_core::embedding::EmbeddingMetadata,
    lease: notion_knowledge_core::reconciliation::Lease,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Result<(), &'static str> {
    use notion_knowledge_retrieval::chunks::{VectorDistance, VectorIndexConfig};
    let table = GuardedChunkTable::bind(
        coordinator.index_directory(),
        "chunks",
        embedding.clone(),
        coordinator.clone(),
    )
    .map_err(|_| "search maintenance binding unavailable")?;
    match table.search_snapshot().await {
        Ok(_) => return Ok(()),
        Err(error) if error.is_incompatible_index() => {}
        Err(_) => return Err("search storage unavailable"),
    }
    table
        .ensure_startup_indexes(lease.clone(), clock.clone())
        .await
        .map_err(|_| "search FTS preparation failed")?;
    table
        .ensure_vector_index(
            VectorIndexConfig {
                distance: VectorDistance::Cosine,
                ..Default::default()
            },
            lease,
            clock,
        )
        .await
        .map_err(|_| "search ANN preparation failed")?;
    table
        .search_snapshot()
        .await
        .map_err(|_| "search unavailable after preparation")?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Initialize,
    ExistingWrite,
    ReadOnly,
}
