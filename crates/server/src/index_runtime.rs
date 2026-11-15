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

pub(crate) fn has_fatal_sync_failure(
    progress: &notion_knowledge_retrieval::durable_plan::DurableProgress,
) -> bool {
    progress
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
}

/// Concurrent read-only page preparations ahead of the embedding consumer.
const PREFETCH_WORKERS: usize = 2;
/// Completed preparations queued for the consumer beyond those in flight.
const PREFETCH_QUEUE: usize = 1;
/// Shared retained-byte budget (KiB) for in-flight, queued and committing pages.
const PREFETCH_BUDGET_KIB: usize = 64 * 1024;
/// Worst-case reservation (KiB) per preparation: the 8 MiB streamed Markdown
/// cap retained as source, normalized text, new and previous chunk copies.
const PREFETCH_RESERVE_KIB: usize = 32 * 1024;

/// One retained-byte budget for read-ahead. A preparation reserves the
/// worst case before its first request, then keeps only its measured share
/// while queued and committing; dropping the permit returns the bytes.
struct PrefetchBudget {
    kib: Arc<tokio::sync::Semaphore>,
    reserve_kib: usize,
}
impl PrefetchBudget {
    fn new(total_kib: usize, reserve_kib: usize) -> Self {
        assert!(reserve_kib > 0 && reserve_kib <= total_kib);
        Self {
            kib: Arc::new(tokio::sync::Semaphore::new(total_kib)),
            reserve_kib,
        }
    }
    async fn reserve(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.kib
            .clone()
            .acquire_many_owned(u32::try_from(self.reserve_kib).ok()?)
            .await
            .ok()
    }
    /// Shrink a reservation to a measured size, or `None` if it exceeds it.
    fn retain(
        &self,
        mut reservation: tokio::sync::OwnedSemaphorePermit,
        bytes: usize,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let kib = bytes.div_ceil(1024).max(1);
        if kib > self.reserve_kib {
            return None;
        }
        drop(reservation.split(self.reserve_kib - kib));
        Some(reservation)
    }
}

enum Prefetched {
    Skipped,
    Fatal(&'static str),
    Ready {
        prefetched: Box<notion_knowledge_retrieval::refresh::PrefetchedPage>,
        bytes: tokio::sync::OwnedSemaphorePermit,
    },
    /// Prepared again inline by the consumer from current state: the page was
    /// too large to queue, or its source changed while it was being prefetched.
    Inline {
        page: notion_knowledge_core::backend::PageId,
        root: notion_knowledge_core::backend::PageId,
        operation: String,
    },
    Failed {
        page: notion_knowledge_core::backend::PageId,
        operation: String,
        error: notion_knowledge_retrieval::refresh::RefreshError,
    },
    TimedOut {
        page: notion_knowledge_core::backend::PageId,
        operation: String,
    },
}
enum PageWork {
    Commit(
        Box<notion_knowledge_retrieval::refresh::PrefetchedPage>,
        tokio::sync::OwnedSemaphorePermit,
    ),
    Inline(notion_knowledge_core::backend::PageId),
    Failed(notion_knowledge_retrieval::refresh::RefreshError),
    TimedOut,
}

struct SyncEmbedding<'a>(&'a dyn EmbeddingProvider);
impl EmbeddingProvider for SyncEmbedding<'_> {
    fn wait_idle(&self) -> notion_knowledge_core::embedding::EmbeddingIdleFuture<'_> {
        self.0.wait_idle()
    }
    fn metadata(&self) -> &notion_knowledge_core::embedding::EmbeddingMetadata {
        self.0.metadata()
    }
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
    ) -> notion_knowledge_core::embedding::EmbeddingFuture<'a> {
        Box::pin(async move {
            let vectors = self.0.embed_batch(texts).await?;
            notion_knowledge_core::logging::sync_count(
                notion_knowledge_core::logging::SyncCount::EmbeddedChunk,
                vectors.len(),
            );
            Ok(vectors)
        })
    }
}

pub struct IndexRuntime {
    pub coordinator: Arc<IndexCommitCoordinator>,
    pub scope: ReconciliationScope,
    pub handler: AuthoritativePageRefresh,
    pub runner: AuthoritativeReconciliationRunner,
    pub provider: Arc<dyn EmbeddingProvider>,
    alias_sources: notion_knowledge_core::aliases::AliasSources,
    table: GuardedChunkTable,
    lexical: Option<LexicalCachePlan>,
    source: Arc<NotionClient>,
    source_scope: LifecycleScope,
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

    /// Called only after dropping the timed-out observer and expiring its clock.
    /// Never renew a lease while an old owned commit may still hold its guard.
    async fn settle_page_timeout(
        &self,
        settings: &WorkerSettings,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        operation: &str,
    ) -> Result<bool, &'static str> {
        use notion_knowledge_retrieval::commit::ReceiptStatus;
        use std::time::Duration;
        let failures = tokio::time::timeout(
            Duration::from_secs(30),
            self.coordinator.drain_owned_outcomes(),
        )
        .await
        .map_err(|_| "cache sync drain timeout")?
        .map_err(|_| "cache sync drain failed")?;
        if failures
            .iter()
            .any(|error| *error != notion_knowledge_retrieval::commit::CommitError::LeaseLost)
        {
            return Err("cache sync timed-out owned operation failed");
        }
        let applied = match self
            .coordinator
            .receipt(operation)
            .map_err(|_| "cache sync receipt unavailable")?
        {
            None => false,
            Some(receipt)
                if receipt.status == ReceiptStatus::Applied && receipt.failure.is_none() =>
            {
                true
            }
            // Effects may have started. Never label an unresolved or failed
            // commit a harmless page timeout or hide index/authority failures.
            Some(_) => return Err("cache sync timed-out effects require recovery"),
        };
        if self
            .coordinator
            .has_unfinished_commits()
            .map_err(|_| "cache sync receipt unavailable")?
        {
            return Err("cache sync effects require recovery");
        }
        let state = self.coordinator.state();
        let ttl = settings.timeout_seconds as i64 + 30;
        *lease = state
            .renew_lease(lease, now_ms() / 1000, ttl)
            .map_err(|_| "cache sync lease unavailable")?;
        let idle = tokio::time::timeout(
            Duration::from_secs(settings.timeout_seconds),
            self.provider.wait_idle(),
        );
        tokio::pin!(idle);
        loop {
            tokio::select! {
                result = &mut idle => {
                    result.map_err(|_| "cache embedding drain timeout")?
                        .map_err(|_| "cache embedding drain unavailable")?;
                    break;
                },
                _ = tokio::time::sleep(Duration::from_secs((ttl as u64/3).max(1))) => {
                    *lease = state.renew_lease(lease, now_ms()/1000, ttl)
                        .map_err(|_| "cache sync lease unavailable")?;
                }
            }
        }
        *lease = state
            .renew_lease(lease, now_ms() / 1000, ttl)
            .map_err(|_| "cache sync lease unavailable")?;
        tokio::time::timeout(
            Duration::from_secs(settings.timeout_seconds),
            self.table.inventory_version(),
        )
        .await
        .map_err(|_| "cache sync snapshot timeout")?
        .map_err(|_| "cache sync snapshot unavailable")?;
        Ok(applied)
    }

    /// Stream only initial/reset-cache population. Final discovery still owns
    /// inventory/deletion decisions; ordinary reconciliation is unchanged.
    async fn stream_initial_pages(
        &self,
        settings: &WorkerSettings,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
    ) -> Result<
        (
            notion_knowledge_core::discovery::DiscoveryReport,
            std::collections::BTreeSet<String>,
        ),
        &'static str,
    > {
        use futures::StreamExt;
        use notion_knowledge_core::{
            logging::{SyncPhase, SyncProgressLog, sync_task},
            root_scope::RootScopeGate,
        };
        use notion_knowledge_retrieval::refresh::RefreshError;
        let (send, receive) = tokio::sync::mpsc::channel(8);
        let crawl = self.source.crawl_with_pages(
            &self.source_scope.roots,
            &self.source_scope.exclusions,
            send,
        );
        // Bounded read-ahead: source preparation for later pages overlaps the
        // one embedding/commit consumer. Each preparation reserves worst-case
        // bytes before any request and keeps its measured share until the
        // consumer finishes it, so in-flight and queued pages share one budget.
        let budget = PrefetchBudget::new(PREFETCH_BUDGET_KIB, PREFETCH_RESERVE_KIB);
        let (ready, mut prepared) = tokio::sync::mpsc::channel::<Prefetched>(PREFETCH_QUEUE);
        let produce = async {
            let ready = ready;
            let gate = RootScopeGate::new(self.source.clone(), self.source_scope.clone())
                .map_err(|_| "cache scope unavailable")?;
            let pages = futures::stream::unfold(receive, |mut receive| async move {
                receive.recv().await.map(|page| (page, receive))
            });
            let mut fetched = std::pin::pin!(
                pages
                    .enumerate()
                    .map(|(index, page)| {
                        Box::pin(self.prefetch_initial_page(
                            settings,
                            &gate,
                            &budget,
                            index + 1,
                            page,
                        ))
                    })
                    .buffered(PREFETCH_WORKERS)
            );
            while let Some(item) = fetched.next().await {
                if ready.send(item).await.is_err() {
                    break;
                }
            }
            Ok::<_, &'static str>(())
        };
        let consume = async {
            let mut progress = SyncProgressLog::new(SyncPhase::Indexing);
            let mut processed = 0;
            let mut persisted = 0;
            let mut skipped = 0;
            let mut timed_out = std::collections::BTreeSet::new();
            let state = self.coordinator.state();
            let ttl = settings.timeout_seconds as i64 + 30;
            loop {
                let item = tokio::select! {
                    item = prepared.recv() => match item { Some(item) => item, None => break },
                    _ = tokio::time::sleep(std::time::Duration::from_secs((ttl as u64 / 3).max(1))) => {
                        *lease = state.renew_lease(lease, now_ms()/1000, ttl).map_err(|_| "cache sync lease unavailable")?;
                        continue;
                    }
                };
                *lease = state
                    .renew_lease(lease, now_ms() / 1000, ttl)
                    .map_err(|_| "cache sync lease unavailable")?;
                processed += 1;
                let (page, operation, work) = match item {
                    Prefetched::Fatal(error) => return Err(error),
                    Prefetched::Skipped => {
                        skipped += 1;
                        notion_knowledge_core::logging::sync_count(
                            notion_knowledge_core::logging::SyncCount::SkippedPage,
                            1,
                        );
                        progress.report(processed, None, persisted, skipped, false);
                        continue;
                    }
                    Prefetched::Ready { prefetched, bytes } => (
                        prefetched.page().clone(),
                        prefetched.operation().to_owned(),
                        PageWork::Commit(prefetched, bytes),
                    ),
                    Prefetched::Inline {
                        page,
                        root,
                        operation,
                    } => (page, operation, PageWork::Inline(root)),
                    Prefetched::Failed {
                        page,
                        operation,
                        error,
                    } => (page, operation, PageWork::Failed(error)),
                    Prefetched::TimedOut { page, operation } => {
                        (page, operation, PageWork::TimedOut)
                    }
                };
                struct Attempt(Arc<std::sync::atomic::AtomicBool>);
                impl Drop for Attempt {
                    fn drop(&mut self) {
                        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let _attempt = Attempt(expired.clone());
                let flag = expired.clone();
                let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                    if flag.load(std::sync::atomic::Ordering::SeqCst) {
                        i64::MAX
                    } else {
                        now_ms() / 1000
                    }
                });
                let deadline = std::time::Duration::from_secs(settings.timeout_seconds);
                let embedding = SyncEmbedding(self.provider.as_ref());
                let result: Result<Result<_, RefreshError>, ()> = match work {
                    PageWork::Commit(prefetched, bytes) => {
                        let result = tokio::time::timeout(
                            deadline,
                            sync_task(
                                SyncPhase::Indexing,
                                processed,
                                Box::pin(self.handler.refresh_prefetched(
                                    *prefetched,
                                    &embedding,
                                    lease.clone(),
                                    clock,
                                )),
                            ),
                        )
                        .await
                        .map_err(|_| ());
                        drop(bytes);
                        result
                    }
                    PageWork::Inline(root) => tokio::time::timeout(
                        deadline,
                        sync_task(
                            SyncPhase::Indexing,
                            processed,
                            Box::pin(self.handler.refresh_page(
                                &page,
                                &root,
                                &operation,
                                &embedding,
                                lease.clone(),
                                clock,
                            )),
                        ),
                    )
                    .await
                    .map_err(|_| ()),
                    PageWork::Failed(error) => Ok(Err(error)),
                    PageWork::TimedOut => Err(()),
                };
                expired.store(true, std::sync::atomic::Ordering::SeqCst);
                if result.is_ok() {
                    self.coordinator
                        .drain_owned()
                        .await
                        .map_err(|_| "cache sync drain failed")?;
                }
                match result {
                    Ok(Ok(_)) => {
                        persisted += 1;
                        notion_knowledge_core::logging::sync_count(
                            notion_knowledge_core::logging::SyncCount::CommittedPage,
                            1,
                        );
                    }
                    Ok(Err(
                        error @ (RefreshError::Source
                        | RefreshError::PermanentSource
                        | RefreshError::EmbeddingInputLimit
                        | RefreshError::OutOfScope),
                    )) => {
                        skipped += 1;
                        notion_knowledge_core::logging::sync_count(
                            notion_knowledge_core::logging::SyncCount::SkippedPage,
                            1,
                        );
                        if error == RefreshError::EmbeddingInputLimit {
                            progress.embedding_input_limit();
                        }
                    }
                    Ok(Err(RefreshError::Unauthenticated)) => {
                        return Err("cache streaming source authentication failed");
                    }
                    Ok(Err(RefreshError::Index)) => return Err("cache streaming index failed"),
                    Ok(Err(RefreshError::Conflict | RefreshError::InvalidConfiguration)) => {
                        return Err("cache streaming authority failed");
                    }
                    Ok(Err(RefreshError::Unavailable)) => {
                        return Err("cache streaming dependency unavailable");
                    }
                    Err(()) => {
                        if self
                            .settle_page_timeout(settings, lease, &operation)
                            .await?
                        {
                            persisted += 1;
                            notion_knowledge_core::logging::sync_count(
                                notion_knowledge_core::logging::SyncCount::CommittedPage,
                                1,
                            );
                        } else {
                            timed_out.insert(page.0.clone());
                            skipped += 1;
                            notion_knowledge_core::logging::sync_count(
                                notion_knowledge_core::logging::SyncCount::SkippedPage,
                                1,
                            );
                            progress.page_timeout();
                        }
                    }
                }
                progress.report(processed, None, persisted, skipped, false);
            }
            progress.report(processed, Some(processed), persisted, skipped, true);
            Ok::<_, &'static str>(timed_out)
        };
        let (report, (), timed_out) = tokio::try_join!(
            async { Box::pin(crawl).await.map_err(|_| "cache inventory failed") },
            Box::pin(produce),
            Box::pin(consume)
        )?;
        Ok((report, timed_out))
    }

    /// Read-only source preparation for one discovered page. Authorization and
    /// content reads share the client's Notion rate limiter; nothing here
    /// embeds, takes the index version, or needs the writer lease.
    async fn prefetch_initial_page(
        &self,
        settings: &WorkerSettings,
        gate: &notion_knowledge_core::root_scope::RootScopeGate,
        budget: &PrefetchBudget,
        index: usize,
        page: notion_knowledge_core::backend::PageId,
    ) -> Prefetched {
        use notion_knowledge_core::logging::{SyncCount, SyncPhase, sync_count, sync_task};
        let Ok(permit) = gate.authorize(&page).await else {
            return Prefetched::Skipped;
        };
        let Some(root) = self
            .source_scope
            .roots
            .iter()
            .find(|root| permit.belongs_to_any(std::slice::from_ref(&root.0)))
            .cloned()
        else {
            return Prefetched::Fatal("cache source authority unavailable");
        };
        let operation = format!(
            "initial-{}",
            notion_knowledge_core::logging::CorrelationId::new().as_str()
        );
        let Some(reservation) = budget.reserve().await else {
            return Prefetched::Fatal("cache prefetch budget unavailable");
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(settings.timeout_seconds),
            sync_task(
                SyncPhase::Indexing,
                index,
                Box::pin(self.handler.prefetch_page(&page, &root, &operation)),
            ),
        )
        .await;
        match result {
            Err(_) => Prefetched::TimedOut { page, operation },
            // Source changed during preparation: nothing was captured, so the
            // consumer prepares it again from current state.
            Ok(Err(notion_knowledge_retrieval::refresh::RefreshError::Conflict)) => {
                Prefetched::Inline {
                    page,
                    root,
                    operation,
                }
            }
            Ok(Err(error)) => Prefetched::Failed {
                page,
                operation,
                error,
            },
            Ok(Ok(prefetched)) => {
                let Some(bytes) = budget.retain(reservation, prefetched.retained_bytes()) else {
                    // Never queue more than was reserved. The consumer prepares
                    // this rare page inline, as the serial path always did.
                    drop(prefetched);
                    return Prefetched::Inline {
                        page,
                        root,
                        operation,
                    };
                };
                sync_count(SyncCount::PrefetchedPage, 1);
                Prefetched::Ready {
                    prefetched: Box::new(prefetched),
                    bytes,
                }
            }
        }
    }

    async fn resync_cache(&self, settings: &WorkerSettings) -> Result<(), &'static str> {
        // Erase the large adapter future at the composition boundary; this
        // keeps both runtime stack frames and callers' layout queries bounded.
        let work: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), &'static str>> + Send + '_>,
        > = Box::pin(self.resync_cache_inner(settings));
        notion_knowledge_core::logging::with_sync_progress(work).await
    }

    async fn resync_cache_inner(&self, settings: &WorkerSettings) -> Result<(), &'static str> {
        use notion_knowledge_core::logging::{SyncPhase, SyncProgressLog};
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
        let mut progress_log = None;
        let mut reported_commits = 0;
        let mut reported_failures = 0;
        let mut streaming_timeouts = std::collections::BTreeSet::new();
        let mut timed_out_recovery = std::collections::BTreeSet::new();
        let result = 'resync: loop {
            let inventory = match self.inventory_pending("cache-reset") {
                Ok(inventory) => inventory,
                Err(_) => break Err("cache sync state unavailable"),
            };
            let prepared = if inventory {
                let plan = match tokio::time::timeout(
                    Duration::from_secs(settings.inventory_timeout_seconds),
                    async {
                        let (report, timeouts) =
                            Box::pin(self.stream_initial_pages(settings, &mut lease)).await?;
                        streaming_timeouts = timeouts;
                        state
                            .release_lease(&lease, now_ms() / 1000)
                            .map_err(|_| "cache sync lease unavailable")?;
                        Box::pin(self.runner.dry_run_discovered(report))
                            .await
                            .map_err(|_| "cache inventory planning failed")
                    },
                )
                .await
                {
                    Ok(Ok(plan)) => plan,
                    Ok(Err(error)) => {
                        let _ = self.coordinator.drain_owned().await;
                        break Err(error);
                    }
                    Err(_) => {
                        let _ = self.coordinator.drain_owned().await;
                        break Err("cache inventory timeout; retry startup");
                    }
                };
                lease = match state.acquire_lease(now_ms() / 1000, timeout_seconds as i64 + 30) {
                    Ok(lease) => lease,
                    Err(_) => break Err("cache sync writer busy"),
                };
                if state
                    .start_or_resume(&lease, now_ms() / 1000, "cache-reset", &self.scope)
                    .is_err()
                {
                    break Err("cache sync state unavailable");
                }
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
            let pending_page = if prepared.is_none() {
                match self.coordinator.reconciliation_progress("cache-reset") {
                    Ok(progress) => progress.pending.first().cloned(),
                    Err(_) => break Err("cache sync progress unavailable"),
                }
            } else {
                None
            };
            let operation = pending_page
                .as_ref()
                .map(|page| {
                    self.coordinator
                        .reconciliation_page_proposal("cache-reset", page)
                        .map(|(proposal, _)| proposal.operation_id)
                })
                .transpose();
            let operation = match operation {
                Ok(operation) => operation,
                Err(_) => break Err("cache sync proposal unavailable"),
            };
            let attempt = tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
                if let Some(plan) = prepared {
                    self.runner
                        .record_inventory_plan("cache-reset", &plan, lease.clone(), clock)
                        .await
                } else {
                    self.runner
                        .apply_next(
                            "cache-reset",
                            lease.clone(),
                            clock,
                            &SyncEmbedding(self.provider.as_ref()),
                        )
                        .await
                }
            })
            .await;
            if matches!(&attempt, Ok(Ok(_))) && self.coordinator.drain_owned().await.is_err() {
                break Err("cache sync drain failed");
            }
            let progress = match attempt {
                Ok(Ok(progress)) => progress,
                Err(_) if operation.is_some() => {
                    expired.store(true, Ordering::SeqCst);
                    let page = pending_page.as_ref().expect("operation has page");
                    match self
                        .settle_page_timeout(settings, &mut lease, operation.as_ref().unwrap())
                        .await
                    {
                        Ok(true) => {
                            // Existing recovery/ack path must verify an applied receipt;
                            // allow one bounded recovery, never an endless timeout loop.
                            if !timed_out_recovery.insert(page.clone()) {
                                break Err("cache sync applied receipt recovery timeout");
                            }
                            continue;
                        }
                        Ok(false) => {
                            let current =
                                match self.coordinator.reconciliation_progress("cache-reset") {
                                    Ok(progress) => progress,
                                    Err(_) => break Err("cache sync progress unavailable"),
                                };
                            if current.failed.contains_key(page) {
                                // Keep the genuine failure recorded before observer cancellation.
                                current
                            } else {
                                if self.coordinator.fail_reconciliation_page(&lease, now_ms()/1000, "cache-reset", page,
                                    notion_knowledge_retrieval::durable_plan::DurableFailure::PageTimeout).is_err() {
                                    break Err("cache sync timeout status unavailable");
                                }
                                SyncProgressLog::new(SyncPhase::Indexing).page_timeout();
                                match self.coordinator.reconciliation_progress("cache-reset") {
                                    Ok(progress) => progress,
                                    Err(_) => break Err("cache sync progress unavailable"),
                                }
                            }
                        }
                        Err(error) => break Err(error),
                    }
                }
                _ => {
                    expired.store(true, Ordering::SeqCst);
                    let _ = self.coordinator.drain_owned().await;
                    break Err("cache resync failed; retry startup");
                }
            };
            let progress = if !streaming_timeouts.is_empty() {
                for page in std::mem::take(&mut streaming_timeouts) {
                    if progress.pending.contains(&page) && self.coordinator.fail_reconciliation_page(
                        &lease, now_ms()/1000, "cache-reset", &page,
                        notion_knowledge_retrieval::durable_plan::DurableFailure::PageTimeout,
                    ).is_err() {
                        break 'resync Err("cache sync timeout status unavailable");
                    }
                }
                match self.coordinator.reconciliation_progress("cache-reset") {
                    Ok(progress) => progress,
                    Err(_) => break Err("cache sync progress unavailable"),
                }
            } else {
                progress
            };
            // Report actual application outcomes after the streaming handoff.
            // Unchanged verification must not count the early page commit again.
            let commits = progress.changed.saturating_add(progress.deleted) as usize;
            notion_knowledge_core::logging::sync_count(
                notion_knowledge_core::logging::SyncCount::CommittedPage,
                commits.saturating_sub(reported_commits),
            );
            let failures = progress.failed_pages();
            notion_knowledge_core::logging::sync_count(
                notion_knowledge_core::logging::SyncCount::SkippedPage,
                failures.saturating_sub(reported_failures),
            );
            reported_commits = commits;
            reported_failures = failures;
            // An unreadable source page does not invalidate other cached pages.
            // Keep its existing failure record for status/retry, while storage,
            // ownership and credential failures still prevent serving this cache.
            if has_fatal_sync_failure(&progress) {
                break Err("cache resync incomplete; retry startup");
            }
            let total = progress.successful.len() + progress.failed.len() + progress.pending.len();
            progress_log
                .get_or_insert_with(|| SyncProgressLog::new(SyncPhase::Indexing))
                .report(
                    total - progress.pending.len(),
                    Some(total),
                    progress.changed as usize,
                    progress.failed_pages(),
                    progress.pending.is_empty(),
                );
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
                let mut preparing = SyncProgressLog::new(SyncPhase::PreparingSearch);
                preparing.report(
                    0,
                    None,
                    progress.changed as usize,
                    progress.failed_pages(),
                    false,
                );
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
                preparing.report(
                    1,
                    Some(1),
                    progress.changed as usize,
                    progress.failed_pages(),
                    true,
                );
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
                            "sync_progress": notion_knowledge_core::logging::current_sync_progress(),
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
        .with_alias_sources(settings.alias_sources.clone())
        .with_projection_mode(settings.projection_mode);
        let planner = AuthoritativeReconciliationPlanner::new(
            source.clone(),
            guard.clone(),
            coordinator.clone(),
            scope.clone(),
        )
        .map_err(|_| "authoritative planner incompatible")?
        .with_alias_sources(settings.alias_sources.clone())
        .with_projection_mode(settings.projection_mode);
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
            source,
            source_scope: scope,
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

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use notion_knowledge_core::{
        aliases::AliasSources,
        backend::PageId,
        embedding::{EmbeddingFuture, EmbeddingMetadata},
        reconciliation::{FailureClass, Lease},
        sync_state::PageSyncState,
    };
    use notion_knowledge_retrieval::commit::{PageAction, PageOperation, ReceiptStatus};
    struct Model(EmbeddingMetadata);
    impl EmbeddingProvider for Model {
        fn metadata(&self) -> &EmbeddingMetadata {
            &self.0
        }
        fn embed_batch<'a>(&'a self, _: &'a [String]) -> EmbeddingFuture<'a> {
            panic!("timeout settlement must never start embeddings")
        }
    }
    async fn fixture() -> (tempfile::TempDir, WorkerSettings, IndexRuntime, Lease) {
        let dir = tempfile::tempdir().unwrap();
        let settings = WorkerSettings {
            projection_mode: Default::default(),
            lexical: None,
            alias_sources: AliasSources::new(true, vec![]).unwrap(),
            index: dir.path().join("index"),
            assets: dir.path().join("assets"),
            poll_ms: 10,
            timeout_seconds: 1,
            inventory_timeout_seconds: 60,
        };
        std::fs::create_dir(&settings.index).unwrap();
        let scope = LifecycleScope {
            workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
            roots: vec![PageId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into())],
            generation: 1,
            exclusions: Default::default(),
        };
        let runtime = IndexRuntime::compose(
            &settings,
            &dir.path().join("state.sqlite"),
            scope,
            Arc::new(NotionClient::integration("fixture-credential").unwrap()),
            Arc::new(Model(
                EmbeddingMetadata::new("fixture".into(), "fixture".into(), "v1".into(), 3).unwrap(),
            )),
            OpenMode::Initialize,
        )
        .await
        .unwrap();
        runtime.coordinator.drain_owned().await.unwrap();
        let lease = runtime
            .coordinator
            .state()
            .acquire_lease(now_ms() / 1000, 31)
            .unwrap();
        (dir, settings, runtime, lease)
    }
    fn clock() -> Arc<dyn Fn() -> i64 + Send + Sync> {
        Arc::new(|| now_ms() / 1000)
    }

    #[tokio::test]
    async fn timeout_settlement_preserves_receipt_failures_and_applied_work() {
        for status in [
            ReceiptStatus::Pending,
            ReceiptStatus::Failed,
            ReceiptStatus::Applied,
        ] {
            let (dir, settings, runtime, mut lease) = fixture().await;
            if status == ReceiptStatus::Pending {
                rusqlite::Connection::open(dir.path().join("state.sqlite")).unwrap()
                    .execute_batch("CREATE TRIGGER retain_pending BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=2 BEGIN SELECT RAISE(ABORT,'injected persistence failure'); END;").unwrap();
            }
            let checkpoint =
                PageSyncState::tombstone("cccccccc-cccc-cccc-cccc-cccccccccccc".into(), None)
                    .unwrap();
            let operation =
                PageOperation::new("timed-out", "revision", PageAction::Delete, checkpoint)
                    .unwrap();
            let observer = runtime.coordinator.submit(
                operation,
                lease.clone(),
                clock(),
                move |_| async move {
                    if status == ReceiptStatus::Applied {
                        Ok(())
                    } else {
                        Err(FailureClass::Index)
                    }
                },
            );
            // Drain with the receiver still alive: even a successfully queued
            // result can remain unconsumed by a timed-out observer.
            runtime.coordinator.drain_owned().await.unwrap();
            drop(observer);
            assert_eq!(
                runtime
                    .coordinator
                    .receipt("timed-out")
                    .unwrap()
                    .unwrap()
                    .status,
                status
            );
            let result = runtime
                .settle_page_timeout(&settings, &mut lease, "timed-out")
                .await;
            if status == ReceiptStatus::Applied {
                assert_eq!(result, Ok(true)); // Caller must use existing recovery/ack.
            } else {
                assert!(result.is_err()); // Never relabel unresolved effects PageTimeout.
            }
        }
    }

    #[tokio::test]
    async fn timeout_settlement_never_hides_receipt_free_owned_failures_or_real_lease_loss() {
        for failure in [
            FailureClass::Index,
            FailureClass::Unavailable,
            FailureClass::Conflict,
        ] {
            let (_dir, settings, runtime, mut lease) = fixture().await;
            let observer =
                runtime
                    .coordinator
                    .maintain(lease.clone(), clock(), move |_| async move { Err(failure) });
            // Keep the result queued, then cancel its observer. Typed drain must
            // distinguish this from a harmless pre-commit page deadline.
            drop(observer);
            assert!(
                runtime
                    .settle_page_timeout(&settings, &mut lease, "no-receipt")
                    .await
                    .is_err()
            );
            assert!(runtime.coordinator.receipt("no-receipt").unwrap().is_none());
        }
        for lose_actual_lease in [false, true] {
            let (_dir, settings, runtime, mut lease) = fixture().await;
            let observer =
                runtime
                    .coordinator
                    .maintain(lease.clone(), Arc::new(|| i64::MAX), |_| async { Ok(()) });
            drop(observer);
            if lose_actual_lease {
                runtime.coordinator.drain_owned_outcomes().await.unwrap();
                runtime
                    .coordinator
                    .state()
                    .release_lease(&lease, now_ms() / 1000)
                    .unwrap();
                runtime
                    .coordinator
                    .state()
                    .acquire_lease(now_ms() / 1000, 31)
                    .unwrap();
            }
            let result = runtime
                .settle_page_timeout(&settings, &mut lease, "no-receipt")
                .await;
            if lose_actual_lease {
                assert!(result.is_err());
            } else {
                assert_eq!(result, Ok(false));
            }
        }
    }

    #[tokio::test]
    async fn prefetch_budget_applies_byte_backpressure_until_a_page_is_released() {
        use std::time::Duration;
        let budget = PrefetchBudget::new(8, 3);
        let first = budget.reserve().await.unwrap();
        let second = budget.reserve().await.unwrap();
        // Two worst-case reservations exhaust the budget; a third preparation
        // must not start any request until retained bytes are returned.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), budget.reserve())
                .await
                .is_err()
        );
        // A measured page keeps only its share while queued or committing.
        let queued = budget.retain(first, 1000).unwrap();
        let third = tokio::time::timeout(Duration::from_secs(1), budget.reserve())
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), budget.reserve())
                .await
                .is_err()
        );
        drop(queued);
        drop(third);
        let _released = tokio::time::timeout(Duration::from_secs(1), budget.reserve())
            .await
            .unwrap()
            .unwrap();
        // A page larger than its reservation is never retained in the queue.
        assert!(budget.retain(second, 3 * 1024 + 1).is_none());
    }
}
