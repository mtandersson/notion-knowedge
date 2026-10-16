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
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Initialize,
    ExistingWrite,
    ReadOnly,
}
