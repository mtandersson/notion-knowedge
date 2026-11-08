//! Shared production authority for webhook, manual and periodic reconciliation callers.
#![cfg(all(unix, feature = "webhook-worker"))]
use crate::webhook_worker::{WorkerSettings, runtime::now_ms};
use notion_knowledge_core::{
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    lifecycle::LifecycleScope,
    reconciliation::{JournalError, ReconciliationJournal, ReconciliationScope, RunPhase},
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{
    chunks::{GuardedChunkTable, lexical_binding::LexicalCachePlan},
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
    table: GuardedChunkTable,
    lexical: Option<LexicalCachePlan>,
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
            RecoveryOptions {
                reset: false,
                mode: OpenMode::Initialize,
                lexical: configured_plan(settings)?,
            },
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
            RecoveryOptions {
                reset: false,
                mode: OpenMode::ReadOnly,
                lexical: configured_plan(settings)?,
            },
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
            RecoveryOptions {
                reset: true,
                mode: OpenMode::Initialize,
                lexical: configured_plan(settings)?,
            },
        )
        .await
    }

    /// Typed preparation seam for a configured lexical cache. It is not an
    /// environment switch or an installed search-expansion runtime.
    pub async fn recover_lexical_cache(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
        plan: LexicalCachePlan,
        mode: OpenMode,
    ) -> Result<Self, &'static str> {
        Self::recover_with_reset(
            settings,
            state,
            scope,
            source,
            provider,
            RecoveryOptions {
                reset: false,
                mode,
                lexical: Some(plan),
            },
        )
        .await
    }
    pub fn chunks(&self) -> &GuardedChunkTable {
        &self.table
    }
    pub fn lexical_plan(&self) -> Option<&LexicalCachePlan> {
        self.lexical.as_ref()
    }
    pub async fn prepare_search(
        &self,
        lease: notion_knowledge_core::reconciliation::Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), &'static str> {
        prepare_table_search(&self.table, &self.coordinator, lease, clock).await
    }

    async fn recover_with_reset(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
        options: RecoveryOptions,
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
        let legacy_journal = IndexCommitCoordinator::has_legacy_cache_journal(state)
            .map_err(|_| "cache path unavailable")?;
        let mut rebuild = options.reset
            || pending
            || !state_exists
            || legacy_journal
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
                Ok(coordinator) => match bind_cache(
                    &settings.index,
                    provider.metadata().clone(),
                    coordinator,
                    options.lexical.as_ref(),
                ) {
                    Err(error) if error.is_incompatible_index() => rebuild = true,
                    Err(_) => return Err("index unavailable"),
                    Ok(table) => {
                        let ready = if options.lexical.is_some() {
                            Box::pin(table.probe_lexical_cache()).await
                        } else {
                            table.verify_search_indexes().await
                        };
                        match ready {
                            Err(error) if error.is_incompatible_index() => rebuild = true,
                            Err(_) => return Err("index unavailable"),
                            Ok(()) => {}
                        }
                    }
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
        let runtime = Box::pin(Self::compose_cache(
            settings,
            state,
            scope,
            source,
            provider,
            if rebuild {
                OpenMode::Initialize
            } else {
                options.mode
            },
            options.lexical,
        ))
        .await?;
        if rebuild {
            runtime.resync_cache(settings).await?;
        }
        Ok(runtime)
    }

    async fn resync_cache(&self, settings: &WorkerSettings) -> Result<(), &'static str> {
        let timeout_seconds = settings.timeout_seconds;
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
            let inventory = match self.inventory_pending("cache-reset") {
                Ok(inventory) => inventory,
                Err(_) => break Err("cache sync state unavailable"),
            };
            let prepared = if inventory {
                if state
                    .start_or_resume(&lease, now_ms() / 1000, "cache-reset", &self.scope)
                    .is_err()
                {
                    break Err("cache sync state unavailable");
                }
                if state.release_lease(&lease, now_ms() / 1000).is_err() {
                    break Err("cache sync lease unavailable");
                }
                let plan = match tokio::time::timeout(
                    Duration::from_secs(settings.inventory_timeout_seconds),
                    self.runner.dry_run(),
                )
                .await
                {
                    Ok(Ok(plan)) => plan,
                    _ => break Err("cache inventory failed; retry startup"),
                };
                lease = match state.acquire_lease(now_ms() / 1000, timeout_seconds as i64 + 30) {
                    Ok(lease) => lease,
                    Err(_) => break Err("cache sync writer busy"),
                };
                Some(plan)
            } else {
                None
            };
            lease = match state.renew_lease(&lease, now_ms() / 1000, timeout_seconds as i64 + 30) {
                Ok(lease) => lease,
                Err(_) => break Err("cache sync lease unavailable"),
            };

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
            let attempt = tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
                if let Some(plan) = prepared {
                    self.runner
                        .record_inventory_plan("cache-reset", &plan, lease.clone(), clock)
                        .await
                } else {
                    self.runner
                        .apply_next("cache-reset", lease.clone(), clock, self.provider.as_ref())
                        .await
                }
            })
            .await;
            let progress = match attempt {
                Ok(Ok(progress)) => progress,
                _ => {
                    expired.store(true, Ordering::SeqCst);
                    let _ = self.coordinator.drain_owned().await;
                    break Err("cache resync failed; retry startup");
                }
            };
            // An unreadable source page does not invalidate other cached pages.
            // Keep its existing failure record for status/retry, while storage,
            // ownership and credential failures still prevent serving this cache.
            if progress
                .failed
                .values()
                .chain(progress.planning_failures.values())
                .chain(progress.run_failures.values())
                .any(|failure| {
                    use notion_knowledge_retrieval::durable_plan::DurableFailure;
                    matches!(
                        failure,
                        DurableFailure::Index
                            | DurableFailure::Ownership
                            | DurableFailure::Unauthenticated
                            | DurableFailure::TraversalUnauthenticated
                    )
                })
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
                let ready = tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
                    self.prepare_search(lease.clone(), clock).await?;
                    if self.lexical.is_some() {
                        Box::pin(self.table.probe_lexical_cache())
                            .await
                            .map_err(|_| "lexical search probe failed")?;
                    }
                    Ok::<(), &'static str>(())
                })
                .await;
                if !matches!(ready, Ok(Ok(()))) {
                    expired.store(true, Ordering::SeqCst);
                    let _ = self.coordinator.drain_owned().await;
                    break Err("rebuilt search index unavailable");
                }
                if self
                    .runner
                    .complete("cache-reset", &lease, now_ms() / 1000)
                    .is_err()
                {
                    break Err("cache sync completion unavailable");
                }
                if progress.failed_pages() > 0 || !progress.run_failures.is_empty() {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "operation": "cache_sync",
                            "status": "ready_with_skips",
                            "skipped_pages": progress.failed_pages(),
                            "discovery_errors": progress.run_failures.len(),
                        })
                    );
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

    /// An absent or inventory-phase run still needs read-only planning before
    /// writer admission. Other phases retain their existing page attempts.
    pub(crate) fn inventory_pending(&self, run: &str) -> Result<bool, JournalError> {
        match self.coordinator.state().run(run, &self.scope) {
            Ok(run) => Ok(run.phase == RunPhase::Inventory),
            Err(JournalError::InvalidTransition) => Ok(true),
            Err(error) => Err(error),
        }
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
        // Keep the native composition future out of every caller's stack frame.
        // Both legacy composition and configured recovery share this boundary.
        Box::pin(Self::compose_cache(
            settings,
            state,
            scope,
            source,
            provider,
            mode,
            configured_plan(settings)?,
        ))
        .await
    }

    async fn compose_cache(
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        source: Arc<NotionClient>,
        provider: Arc<dyn EmbeddingProvider>,
        mode: OpenMode,
        lexical: Option<LexicalCachePlan>,
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
            let guard = bind_cache(
                &index,
                provider.metadata().clone(),
                checked.clone(),
                lexical.as_ref(),
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
        let guard = if mode == OpenMode::Initialize {
            let lease = coordinator
                .state()
                .acquire_lease(now_ms() / 1000, 330)
                .map_err(|_| "index writer busy")?;
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
            let result: Result<GuardedChunkTable, &'static str> = async {
                if let Some(plan) = &lexical
                    && !settings.index.join("chunks.lance").exists()
                {
                    return Box::pin(GuardedChunkTable::create_fresh_lexical(
                        &settings.index,
                        "chunks",
                        provider.metadata().clone(),
                        coordinator.clone(),
                        plan.identity(),
                        lease.clone(),
                        clock,
                    ))
                    .await
                    .map_err(|_| "fresh lexical cache unavailable");
                }
                let guard = bind_cache(
                    &settings.index,
                    provider.metadata().clone(),
                    coordinator.clone(),
                    lexical.as_ref(),
                )
                .map_err(|_| "index binding incompatible")?;
                let result = if settings.index.join("chunks.lance").exists() {
                    guard.ensure_startup_indexes(lease.clone(), clock).await
                } else {
                    guard.create_empty(lease.clone(), clock).await
                };
                result.map_err(|_| "index unavailable or incompatible")?;
                Ok(guard)
            }
            .await;
            let _ = coordinator.state().release_lease(&lease, now_ms() / 1000);
            result?
        } else {
            bind_cache(
                &settings.index,
                provider.metadata().clone(),
                coordinator.clone(),
                lexical.as_ref(),
            )
            .map_err(|_| "index binding incompatible")?
        };
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
        let planner = AuthoritativeReconciliationPlanner::new(
            source,
            guard.clone(),
            coordinator.clone(),
            scope,
        )
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
            table: guard,
            lexical,
        })
    }
}
/// Finish derived search maintenance after authoritative sync, including the
/// first populated page after an empty cache. A healthy snapshot is a no-op.
pub(crate) async fn prepare_search_if_missing(
    coordinator: &Arc<IndexCommitCoordinator>,
    handler: &AuthoritativePageRefresh,
    lease: notion_knowledge_core::reconciliation::Lease,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Result<(), &'static str> {
    prepare_table_search(handler.chunks(), coordinator, lease, clock).await
}

pub(crate) fn configured_plan(
    settings: &WorkerSettings,
) -> Result<Option<LexicalCachePlan>, &'static str> {
    settings
        .lexical
        .as_ref()
        .map(crate::lexical_expansion::Settings::load_plan)
        .transpose()
}

async fn prepare_table_search(
    table: &GuardedChunkTable,
    coordinator: &Arc<IndexCommitCoordinator>,
    lease: notion_knowledge_core::reconciliation::Lease,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Result<(), &'static str> {
    use notion_knowledge_retrieval::chunks::{VectorDistance, VectorIndexConfig};
    if coordinator
        .has_unfinished_commits()
        .map_err(|_| "search coordination unavailable")?
    {
        return Ok(());
    }
    match table.verify_search_indexes().await {
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
        .verify_search_indexes()
        .await
        .map_err(|_| "search unavailable after preparation")?;
    Ok(())
}

struct RecoveryOptions {
    reset: bool,
    mode: OpenMode,
    lexical: Option<LexicalCachePlan>,
}

pub(crate) fn bind_cache(
    directory: &Path,
    embedding: notion_knowledge_core::embedding::EmbeddingMetadata,
    coordinator: Arc<IndexCommitCoordinator>,
    lexical: Option<&LexicalCachePlan>,
) -> Result<GuardedChunkTable, notion_knowledge_retrieval::chunks::ChunkTableError> {
    match lexical {
        Some(plan) => GuardedChunkTable::bind_with_lexical_plan(
            directory,
            "chunks",
            embedding,
            coordinator,
            plan.identity(),
        ),
        None => GuardedChunkTable::bind(directory, "chunks", embedding, coordinator),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Initialize,
    ExistingWrite,
    ReadOnly,
}
