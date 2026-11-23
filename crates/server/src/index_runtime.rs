//! Shared production authority for webhook, manual and periodic reconciliation callers.
#![cfg(all(unix, feature = "webhook-worker"))]
use crate::webhook_worker::{WorkerSettings, runtime::now_ms};
use notion_knowledge_core::inventory::InventoryOutcome;
use notion_knowledge_core::logging::page_timings::{self, ApiPhase, PageTimings};
use notion_knowledge_core::{
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    lifecycle::LifecycleScope,
    reconciliation::{ReconciliationJournal, ReconciliationScope},
};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{
    chunks::{ChunkDiffMetrics, GuardedChunkTable, lexical_binding::LexicalCachePlan},
    commit::{CommitBinding, IndexCommitCoordinator},
    planner::production_scope,
    refresh::AuthoritativePageRefresh,
};
use std::{path::Path, sync::Arc};

/// Default concurrent read-only page preparations ahead of the embedding
/// consumer (`NK_PREFETCH_WORKERS`). Zero prepares each page inline, only
/// once the consumer asks for it.
pub(crate) const PREFETCH_WORKERS: usize = 2;
/// Completed preparations queued for the consumer beyond those in flight.
const PREFETCH_QUEUE: usize = 1;
/// Most pages whose relation targets one walk resolves after recording its
/// inventory. Others resolve at the next walk.
const RELATION_PASS_LIMIT: usize = 500;
/// Longest one walk spends on that pass before leaving the rest to the next.
const RELATION_PASS_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);
/// Shared retained-byte budget (KiB) for in-flight, queued and committing pages.
pub(crate) const PREFETCH_BUDGET_KIB: usize = 64 * 1024;
/// Worst-case reservation (KiB) per preparation: the 8 MiB streamed Markdown
/// cap retained as source, normalized text, new and previous chunk copies.
pub(crate) const PREFETCH_RESERVE_KIB: usize = 32 * 1024;

use notion_knowledge_retrieval::prefetch::PrefetchBudget;

enum Prefetched {
    Fatal(&'static str),
    Ready {
        prefetched: Box<notion_knowledge_retrieval::refresh::PrefetchedPage>,
        bytes: tokio::sync::OwnedSemaphorePermit,
    },
    /// Too large to queue within the budget: the consumer prepares it again
    /// from the same walked read when it reaches it.
    Deferred {
        walked: Box<notion_knowledge_notion::walk::WalkedPage>,
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
        Option<tokio::sync::OwnedSemaphorePermit>,
    ),
    Deferred(Box<notion_knowledge_notion::walk::WalkedPage>),
    Failed(notion_knowledge_retrieval::refresh::RefreshError),
    TimedOut,
}

/// A commit clock: seconds now, or `i64::MAX` once its authority expired.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// The per-page deadline every sync page and removal runs under.
#[derive(Debug, Clone, Copy)]
pub struct PageDeadline {
    pub timeout_seconds: u64,
}

/// What one sync reads. Every mode feeds the same consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMode {
    /// Walk the tree: the only mode that replaces the inventory and removes
    /// pages it no longer reached.
    Full,
    /// Search for pages edited since the inventory's newest edit and place
    /// them through its parent map. Falls back to `Full` with no inventory,
    /// and is followed by `Full` when the search listing is cut short.
    Incremental,
    /// One page retrieve placed through the parent map (callbacks).
    Page(notion_knowledge_core::backend::PageId),
}

/// Pages a search or single retrieve found, settled against the inventory.
#[derive(Default)]
struct HitSync {
    entries: Vec<notion_knowledge_core::inventory::InventoryEntry>,
    skipped: Vec<notion_knowledge_core::discovery::SkippedContent>,
    /// In the inventory and confirmed trashed, archived or excluded.
    removals: Vec<String>,
    unplaced: u64,
    /// The search listing was cut short (see [`SearchChanges::complete`]).
    ///
    /// [`SearchChanges::complete`]: notion_knowledge_notion::search::SearchChanges::complete
    truncated: bool,
}
enum Discovered {
    Walk(notion_knowledge_notion::walk::TreeWalk),
    Hits(HitSync),
}

/// What stopped a sync, so a scheduler decides by type whether to hold,
/// retry or fail the cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncFailure {
    /// The source rejected the credential (401). Retrying cannot help until
    /// an operator fixes it.
    Unauthenticated,
    /// The local index failed.
    Index,
    /// Local authority (lease, fence, receipt or configuration) conflicted.
    Authority,
    /// A dependency or the source was unavailable; a retry may succeed.
    Unavailable,
}

/// A failed [`IndexRuntime::sync_from_walk`]: its class and a stable reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncError {
    pub failure: SyncFailure,
    pub reason: &'static str,
}
impl SyncError {
    pub const fn new(failure: SyncFailure, reason: &'static str) -> Self {
        Self { failure, reason }
    }
    /// A source read failure: authentication is told apart from the rest.
    fn source(error: &notion_knowledge_core::backend::BackendError) -> Self {
        if error.kind == notion_knowledge_core::backend::BackendErrorKind::Unauthenticated {
            Self::new(
                SyncFailure::Unauthenticated,
                "cache inventory source authentication failed",
            )
        } else {
            Self::new(SyncFailure::Unavailable, "cache inventory failed")
        }
    }
}
/// Untyped local failures are unavailability: retried within the budget.
impl From<&'static str> for SyncError {
    fn from(reason: &'static str) -> Self {
        Self::new(SyncFailure::Unavailable, reason)
    }
}
impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason)
    }
}

/// What a full sync would change, from a read-only walk ([`IndexRuntime::dry_run`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DryRun {
    /// Every page the walk reached, including skipped ones.
    pub inventory: u64,
    /// Settled by the walk itself (excluded, archived, unreadable).
    pub skipped: u64,
    /// The walk was authoritative, so its `removed` count is meaningful.
    pub complete: bool,
    pub drift: notion_knowledge_core::inventory::DriftCounts,
}

/// Page counts of one [`IndexRuntime::sync_from_walk`] run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncTotals {
    /// Every page the walk reached, including skipped ones.
    pub inventory: u64,
    pub indexed: u64,
    /// Committed already: content, metadata and projections matched.
    pub unchanged: u64,
    /// Settled by the walk itself (excluded, archived, unreadable).
    pub skipped: u64,
    pub failed: u64,
    /// Of `failed`, pages with a permanent source failure.
    pub permanent: u64,
    /// Of `failed`, pages that hit the per-page deadline: a slow dependency,
    /// not a source failure.
    pub timed_out: u64,
    pub removed: u64,
    /// Unreached pages under a page that became inaccessible in this walk,
    /// kept until the next walk confirms it.
    pub held: u64,
    /// The walk was authoritative (no unproven subtree). Never set by
    /// single-page syncs, or by incremental syncs except through the full
    /// walk that follows a cut-short search listing.
    pub complete: bool,
    pub drift: notion_knowledge_core::inventory::DriftCounts,
}

/// Expires a page attempt's clock when dropped.
struct Attempt(Arc<std::sync::atomic::AtomicBool>);
impl Drop for Attempt {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub struct IndexRuntime {
    pub coordinator: Arc<IndexCommitCoordinator>,
    pub scope: ReconciliationScope,
    pub handler: AuthoritativePageRefresh,
    pub provider: Arc<dyn EmbeddingProvider>,
    alias_sources: Option<notion_knowledge_core::aliases::AliasSources>,
    table: GuardedChunkTable,
    lexical: Option<LexicalCachePlan>,
    source: Arc<NotionClient>,
    source_scope: LifecycleScope,
    /// One retained-byte budget for initial streaming and reconciliation read-ahead.
    prefetch_budget: PrefetchBudget,
    /// Concurrent page preparations; zero prepares inline on demand.
    prefetch_workers: usize,
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
            recover_hot_journal(state)?;
            SqliteSyncStateStore::open_read_only(state)
                .map_err(|error| {
                    let cause = SqliteSyncStateStore::read_only_open_error(state).map_or_else(
                        || format!("{error:?}"),
                        |sqlite| format!("{error:?}: {sqlite}"),
                    );
                    log_state_error("open_read_only", &cause);
                    "operational state unavailable"
                })?
                .cache_rebuild_pending()
                .map_err(|error| {
                    log_state_error("cache_rebuild_pending", &format!("{error:?}"));
                    "operational state unavailable"
                })?
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
                        let installed = table
                            .text_projection_installed()
                            .map_err(|_| "text projection state unavailable")?;
                        let ready = table.verify_bound_text_layout().await;
                        match ready {
                            Err(error) if !installed && error.is_incompatible_index() => {
                                rebuild = true
                            }
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
                let waiting = std::sync::atomic::AtomicBool::new(false);
                IndexCommitCoordinator::reset_cache_waiting(
                    &index,
                    &state,
                    &|| now_ms() / 1000,
                    &|| {
                        if !waiting.swap(true, std::sync::atomic::Ordering::Relaxed) {
                            eprintln!(
                                "cache reset waiting up to {}s for a held index lease or lock",
                                notion_knowledge_retrieval::commit::STARTUP_RESET_WAIT_SECONDS
                            );
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1))
                    },
                )
            })
            .await
            .map_err(|_| "cache reset unavailable")?
            .map_err(reset_failure)?;
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
        settings: &PageDeadline,
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
        tokio::time::timeout(
            Duration::from_secs(settings.timeout_seconds),
            self.table.inventory_version(),
        )
        .await
        .map_err(|_| "cache sync snapshot timeout")?
        .map_err(|_| "cache sync snapshot unavailable")?;
        Ok(applied)
    }

    /// The one sync path: discover pages (a walk, a search, or one page),
    /// prepare each from that read, and commit it. The walk is the inventory:
    /// it is persisted with each page's outcome, and only a complete walk
    /// replaces it and removes indexed pages it no longer reached. Nothing is
    /// read again to recheck a page before its commit (staleness is bounded
    /// by the next walk). Search metadata the walk reuses can be one walk
    /// stale for the same reason.
    pub async fn sync_from_walk(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        mode: SyncMode,
    ) -> Result<SyncTotals, SyncError> {
        let clock: Clock = Arc::new(|| now_ms() / 1000);
        self.sync(settings, lease, mode, clock).await
    }

    /// Walk the tree and compare it with the stored inventory. Nothing is
    /// written: no inventory, outcome, receipt, embedding or index change.
    pub async fn dry_run(&self) -> Result<DryRun, SyncError> {
        let walk = self
            .source
            .walk_tree_with_search(&self.source_scope, None)
            .await
            .map_err(|e| SyncError::source(&e))?;
        let diff = self
            .coordinator
            .state()
            .preview_walk(&walk.entries, walk.authoritative())
            .map_err(|_| SyncError::from("cache inventory state unavailable"))?;
        Ok(DryRun {
            inventory: walk.entries.len() as u64,
            skipped: walk.skipped.len() as u64,
            complete: walk.authoritative(),
            drift: diff.drift(),
        })
    }

    /// Sync one page ([`SyncMode::Page`]) for a claimed callback: a webhook
    /// event or a post-write refresh. `claim` is the claim's clock: every
    /// commit and removal of this sync is fenced by it, so an expired,
    /// taken-over or cancelled claim cannot authorize a write.
    pub async fn sync_page(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        page: notion_knowledge_core::backend::PageId,
        claim: Clock,
    ) -> Result<SyncTotals, SyncError> {
        self.sync(settings, lease, SyncMode::Page(page), claim)
            .await
    }

    /// Erases the large sync future once, so callers' layout and auto-trait
    /// queries stay bounded (see [`Self::resync_cache`]).
    fn sync<'a>(
        &'a self,
        settings: &'a PageDeadline,
        lease: &'a mut notion_knowledge_core::reconciliation::Lease,
        mode: SyncMode,
        base: Clock,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<SyncTotals, SyncError>> + Send + 'a>,
    > {
        Box::pin(self.sync_inner(settings, lease, mode, base))
    }

    async fn sync_inner(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        mode: SyncMode,
        base: Clock,
    ) -> Result<SyncTotals, SyncError> {
        use futures::StreamExt;
        use notion_knowledge_core::{
            discovery::SkipReason,
            inventory::InventoryOutcome::{Failed, Skipped},
            logging::{PageEnd, SyncCount, SyncPhase, SyncProgressLog, sync_count, sync_task},
            reconciliation::FailureClass,
        };
        use notion_knowledge_retrieval::{commit::CommitOutcome, refresh::RefreshError};
        let indexed = || InventoryOutcome::Indexed {
            revision: String::new(),
        };
        let state = self.coordinator.state();
        // The source signal fence is taken before discovery and before any
        // prefetched Markdown reads. A later event cannot be cleared by this run.
        let watermark = state
            .source_signal_watermark()
            .map_err(|_| "source cache freshness unavailable")?;
        let since = match &mode {
            SyncMode::Incremental => state
                .inventory_high_water()
                .map_err(|_| "cache inventory state unavailable")?,
            _ => None,
        };
        let full = mode == SyncMode::Full || (mode == SyncMode::Incremental && since.is_none());
        let (send, receive) = tokio::sync::mpsc::channel(8);
        let walk = async move {
            let send = send;
            if full {
                return self
                    .source
                    .walk_tree_with_search(&self.source_scope, Some(&send))
                    .await
                    .map(Discovered::Walk)
                    .map_err(|e| SyncError::source(&e));
            }
            let (hits, complete) = match mode {
                SyncMode::Page(page) => {
                    match self.source.page_hit(&page).await {
                        Ok(hit) => {
                            if hit.is_none() {
                                self.invalidate_source(&page.0, true)?;
                            }
                            (hit.into_iter().collect(), true)
                        }
                        // An authenticated page lookup confirms it is gone or
                        // revoked. Invalidate any dependent cached descendants.
                        Err(e) if notion_knowledge_core::discovery::inaccessible(e.kind) => {
                            self.invalidate_source(&page.0, true)?;
                            return Ok(Discovered::Hits(HitSync {
                                removals: vec![page.0.clone()],
                                ..HitSync::default()
                            }));
                        }
                        Err(e) => {
                            self.invalidate_source(&page.0, false)?;
                            return Err(SyncError::source(&e));
                        }
                    }
                }
                _ => {
                    let changes = self
                        .source
                        .search_changed_since(since.as_deref().unwrap_or_default())
                        .await
                        .map_err(|e| SyncError::source(&e))?;
                    (changes.hits, changes.complete)
                }
            };
            let single = hits.len() == 1 && since.is_none();
            let mut hits = Box::pin(self.discover_hits(hits, single, &send)).await?;
            hits.truncated = !complete;
            Ok(Discovered::Hits(hits))
        };
        // Bounded read-ahead: preparation of later pages overlaps the one
        // embedding/commit consumer. Each preparation reserves worst-case bytes
        // and keeps its measured share until the consumer finishes it, so
        // in-flight and queued pages share one budget.
        let budget = &self.prefetch_budget;
        // Each ready page carries its timings and the instant it was handed over,
        // so the consumer can report how long it sat queued (#662).
        let (ready, mut prepared) =
            tokio::sync::mpsc::channel::<(std::time::Instant, PageTimings, Prefetched)>(
                PREFETCH_QUEUE,
            );
        // Inline mode (zero workers): each preparation waits for a credit the
        // consumer grants when it asks for its next page, so no page is
        // prepared ahead of, or concurrently with, the consumer's work.
        let inline = (self.prefetch_workers == 0).then(|| tokio::sync::Semaphore::new(0));
        let inline = inline.as_ref();
        let produce = async {
            let ready = ready;
            // Skipped pages are already settled by the walk itself.
            let pages = futures::stream::unfold(receive, |mut receive| async move {
                receive.recv().await.map(|page| (page, receive))
            })
            .filter(|page: &notion_knowledge_notion::walk::WalkedPage| {
                std::future::ready(page.skip.is_none())
            });
            let mut fetched = std::pin::pin!(
                pages
                    .enumerate()
                    .map(|(index, page)| {
                        let timings = page.timings.clone();
                        Box::pin(async move {
                            if let Some(credits) = inline {
                                // The semaphore is never closed while the producer runs.
                                if let Ok(credit) = credits.acquire().await {
                                    credit.forget();
                                }
                            }
                            let item = self
                                .prepare_walked_page(settings, budget, index + 1, page)
                                .await;
                            (timings, item)
                        })
                    })
                    .buffered(self.prefetch_workers.max(1))
            );
            while let Some((timings, item)) = fetched.next().await {
                if ready
                    .send((std::time::Instant::now(), timings, item))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok::<_, SyncError>(())
        };
        let consume = async {
            let mut progress = SyncProgressLog::new(SyncPhase::Indexing);
            let mut totals = SyncTotals::default();
            let mut processed = 0;
            let mut outcomes = Vec::new();
            // Committed pages whose targets the inventory could not place yet.
            let mut unresolved: Vec<(String, Vec<String>)> = Vec::new();
            let state = self.coordinator.state();
            let ttl = settings.timeout_seconds as i64 + 30;
            // The page's wall time starts when the consumer asks for it, so
            // its waiting for a prepared page is part of that time.
            let mut asked = std::time::Instant::now();
            if let Some(credits) = inline {
                credits.add_permits(1);
            }
            loop {
                let (queued, timings, item) = tokio::select! {
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
                let consumer_wait = asked.elapsed();
                let queue_wait = queued.elapsed();
                let mut diff = ChunkDiffMetrics::default();
                let (page, operation, work) = match item {
                    Prefetched::Fatal(error) => return Err(error.into()),
                    Prefetched::Ready { prefetched, bytes } => (
                        prefetched.page().clone(),
                        prefetched.operation().to_owned(),
                        PageWork::Commit(prefetched, Some(bytes)),
                    ),
                    Prefetched::Deferred { walked, operation } => (
                        notion_knowledge_core::backend::PageId(walked.entry.page_id.clone()),
                        operation,
                        PageWork::Deferred(walked),
                    ),
                    Prefetched::Failed {
                        page,
                        operation,
                        error,
                    } => (page, operation, PageWork::Failed(error)),
                    Prefetched::TimedOut { page, operation } => {
                        (page, operation, PageWork::TimedOut)
                    }
                };
                let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let _attempt = Attempt(expired.clone());
                let flag = expired.clone();
                let claim = base.clone();
                let clock: Clock = Arc::new(move || {
                    if flag.load(std::sync::atomic::Ordering::SeqCst) {
                        i64::MAX
                    } else {
                        claim()
                    }
                });
                let deadline = std::time::Duration::from_secs(settings.timeout_seconds);
                let pending_targets = std::sync::Mutex::new(Vec::new());
                let source_published = std::sync::atomic::AtomicBool::new(false);
                let end;
                let result = match work {
                    PageWork::Failed(error) => Ok(Err(error)),
                    PageWork::TimedOut => Err(()),
                    work => {
                        let work = async {
                            let (prefetched, bytes) = match work {
                                PageWork::Commit(prefetched, bytes) => (*prefetched, bytes),
                                // Too large to queue: prepared here from the same read.
                                PageWork::Deferred(walked) => (
                                    timings
                                        .scope(
                                            ApiPhase::Prepare,
                                            self.handler.prepare_walked(&walked, &operation),
                                        )
                                        .await?,
                                    None,
                                ),
                                PageWork::Failed(_) | PageWork::TimedOut => unreachable!(),
                            };
                            *pending_targets.lock().expect("targets") =
                                prefetched.unresolved_targets();
                            self.handler
                                .publish_prefetched_document(
                                    &prefetched,
                                    watermark,
                                    lease.clone(),
                                    clock.clone(),
                                )
                                .await?;
                            source_published.store(true, std::sync::atomic::Ordering::SeqCst);
                            let result = timings
                                .scope(
                                    ApiPhase::Commit,
                                    Box::pin(self.handler.publish_prefetched(
                                        prefetched,
                                        lease.clone(),
                                        clock,
                                    )),
                                )
                                .await;
                            drop(bytes);
                            result
                        };
                        tokio::time::timeout(
                            deadline,
                            sync_task(SyncPhase::Indexing, processed, work),
                        )
                        .await
                        .map_err(|_| ())
                    }
                };
                expired.store(true, std::sync::atomic::Ordering::SeqCst);
                if result.is_ok() {
                    self.coordinator
                        .drain_owned()
                        .await
                        .map_err(|_| "cache sync drain failed")?;
                }
                if !source_published.load(std::sync::atomic::Ordering::SeqCst) {
                    self.invalidate_source(&page.0, false)?;
                }
                match result {
                    Ok(Ok(result)) => {
                        diff = result.diff;
                        end = if result.outcome == CommitOutcome::AlreadyApplied {
                            PageEnd::Unchanged
                        } else {
                            PageEnd::Committed
                        };
                        outcomes.push((page.0.clone(), indexed()));
                        let targets =
                            std::mem::take(&mut *pending_targets.lock().expect("targets"));
                        if !targets.is_empty() {
                            unresolved.push((page.0.clone(), targets));
                        }
                        if result.outcome == CommitOutcome::AlreadyApplied {
                            totals.unchanged += 1;
                        } else {
                            totals.indexed += 1;
                            sync_count(SyncCount::CommittedPage, 1);
                        }
                    }
                    Ok(Err(
                        error @ (RefreshError::Source
                        | RefreshError::PermanentSource
                        | RefreshError::OutOfScope),
                    )) => {
                        outcomes.push((
                            page.0.clone(),
                            if error == RefreshError::OutOfScope {
                                Skipped(SkipReason::OutsideScope)
                            } else {
                                Failed(FailureClass::Source)
                            },
                        ));
                        if error == RefreshError::OutOfScope {
                            totals.skipped += 1;
                        } else {
                            totals.failed += 1;
                            if error != RefreshError::Source {
                                totals.permanent += 1;
                            }
                        }
                        sync_count(SyncCount::SkippedPage, 1);
                        end = PageEnd::Skipped(match error {
                            RefreshError::OutOfScope => "out_of_scope",
                            RefreshError::PermanentSource => "permanent_source",
                            RefreshError::Source => "source",
                            // Excluded by the enclosing arm's pattern.
                            _ => unreachable!("only page-local source failures are skipped"),
                        });
                    }
                    Ok(Err(RefreshError::Unauthenticated)) => {
                        return Err(SyncError::new(
                            SyncFailure::Unauthenticated,
                            "cache streaming source authentication failed",
                        ));
                    }
                    Ok(Err(RefreshError::Index)) => {
                        return Err(SyncError::new(
                            SyncFailure::Index,
                            "cache streaming index failed",
                        ));
                    }
                    // Only local authority (lease, fence, receipt) can conflict:
                    // the source is not checked again before commit.
                    Ok(Err(RefreshError::Conflict | RefreshError::InvalidConfiguration)) => {
                        return Err(SyncError::new(
                            SyncFailure::Authority,
                            "cache streaming authority failed",
                        ));
                    }
                    Ok(Err(RefreshError::Unavailable)) => {
                        return Err("cache streaming dependency unavailable".into());
                    }
                    Err(()) => {
                        if self
                            .settle_page_timeout(settings, lease, &operation)
                            .await?
                        {
                            outcomes.push((page.0.clone(), indexed()));
                            totals.indexed += 1;
                            sync_count(SyncCount::CommittedPage, 1);
                            end = PageEnd::Committed;
                        } else {
                            outcomes.push((page.0.clone(), Failed(FailureClass::Unavailable)));
                            totals.failed += 1;
                            totals.timed_out += 1;
                            sync_count(SyncCount::SkippedPage, 1);
                            end = PageEnd::Skipped("page_timeout");
                        }
                    }
                }
                let fields = page_timings::page_fields(
                    &timings.snapshot(),
                    &page_timings::PageWait {
                        wall: asked.elapsed(),
                        consumer_wait,
                        queue_wait,
                        text_chunks: diff.added + diff.changed,
                        skipped_chunks: diff.skipped,
                    },
                );
                progress.page(end, &fields);
                progress.report(
                    processed,
                    None,
                    (totals.indexed + totals.unchanged) as usize,
                    totals.failed as usize,
                    false,
                );
                asked = std::time::Instant::now();
                if let Some(credits) = inline {
                    credits.add_permits(1);
                }
            }
            progress.report(
                processed,
                Some(processed),
                (totals.indexed + totals.unchanged) as usize,
                totals.failed as usize,
                true,
            );
            Ok::<_, SyncError>((totals, outcomes, unresolved))
        };
        let (discovered, (), (mut totals, outcomes, unresolved)) =
            tokio::try_join!(Box::pin(walk), Box::pin(produce), Box::pin(consume))?;
        let mut truncated = false;
        let run_id = match discovered {
            Discovered::Walk(walk) => {
                // Read the previous inventory before this walk replaces it.
                let held = if walk.authoritative() {
                    self.held_removals(&walk).await?
                } else {
                    Default::default()
                };
                Self::fence(lease, &base)?;
                let diff = self.record_inventory(&walk, outcomes)?;
                totals.drift = diff.drift();
                totals.held = held.len() as u64;
                totals.inventory = walk.entries.len() as u64;
                totals.skipped += walk.skipped.len() as u64;
                totals.complete = walk.authoritative();
                if walk.authoritative() {
                    state
                        .record_source_inventory_watermark(
                            self.coordinator.binding().workspace(),
                            self.coordinator.binding().scope(),
                            watermark,
                        )
                        .map_err(|_| "source cache freshness unavailable")?;
                    totals.removed =
                        Box::pin(self.remove_unreached(settings, lease, &walk, &held, &base))
                            .await?;
                }
                Box::pin(self.resolve_relations(settings, lease, &walk, unresolved, &base)).await?;
                walk.run_id
            }
            Discovered::Hits(hits) => {
                truncated = hits.truncated;
                let run = notion_knowledge_core::logging::CorrelationId::new()
                    .as_str()
                    .to_owned();
                let unavailable = |_| "cache inventory state unavailable";
                Self::fence(lease, &base)?;
                for entry in &hits.entries {
                    let diff = state
                        .upsert_inventory_entry(&run, entry)
                        .map_err(unavailable)?
                        .drift();
                    totals.drift.added += diff.added;
                    totals.drift.changed += diff.changed;
                    totals.drift.moved += diff.moved;
                    totals.drift.trashed += diff.trashed;
                }
                self.record_outcomes(&hits.entries, &hits.skipped, outcomes)?;
                totals.inventory = hits.entries.len() as u64;
                totals.skipped += hits.skipped.len() as u64;
                totals.drift.unplaced = hits.unplaced;
                totals.removed =
                    Box::pin(self.remove_pages(settings, lease, &run, &hits.removals, &base))
                        .await?;
                run
            }
        };
        let mut walk = (totals.complete, totals.held);
        if !full {
            totals.drift.removed = totals.removed;
            // Completeness describes the last walk; a complete search does not
            // change it. A cut-short listing may have missed older changes, so
            // the inventory is incomplete until the next full walk.
            walk = state
                .last_sync()
                .map_err(|_| "cache inventory state unavailable")?
                .map_or((false, 0), |last| (last.complete && !truncated, last.held));
        }
        Self::fence(lease, &base)?;
        state
            .record_sync(&notion_knowledge_core::inventory::SyncRecord {
                run_id,
                incremental: !full,
                complete: walk.0,
                held: walk.1,
                drift: totals.drift,
                finished_at: now_ms() / 1000,
            })
            .map_err(|_| "cache inventory state unavailable")?;
        // Fold the tail of pages committed since the last cadence pass into
        // the search indexes. Committed rows stay searchable if this fails; the
        // next commit or sync repeats it.
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
        let chunks = self.handler.chunks();
        if chunks.has_pending_optimization()
            && let Err(error) = Box::pin(chunks.optimize_indexes(lease.clone(), clock)).await
        {
            eprintln!(
                "{}",
                serde_json::json!({
                    "operation": "index_optimize",
                    "outcome": "deferred",
                    "error": format!("{error:?}"),
                })
            );
        }
        if truncated {
            // A cut-short listing may have missed older changes that no later
            // search returns (the high-water mark has moved past them). Recover
            // them now with a full walk under the same lease and budget instead
            // of waiting for the next periodic walk.
            // The walk's figures (inventory, drift, completeness) describe the
            // result; the incremental pass's own drift is already recorded.
            // Pages it committed come back unchanged, so work is not double
            // counted. A failed walk fails the call; the inventory stays
            // marked incomplete for the next walk.
            let recovered = Box::pin(self.sync_from_walk(settings, lease, SyncMode::Full)).await?;
            return Ok(SyncTotals {
                indexed: totals.indexed + recovered.indexed,
                failed: totals.failed + recovered.failed,
                timed_out: totals.timed_out + recovered.timed_out,
                removed: totals.removed + recovered.removed,
                ..recovered
            });
        }
        Ok(totals)
    }

    /// Inventory and drift writes need the same authority as index commits:
    /// the claim clock (expired, taken over or cancelled reads as the end of
    /// time) and the writer lease must both still be live.
    fn fence(
        lease: &notion_knowledge_core::reconciliation::Lease,
        base: &Clock,
    ) -> Result<(), SyncError> {
        if base() >= lease.expires_at {
            return Err(SyncError::new(
                SyncFailure::Authority,
                "cache sync claim lost before inventory write",
            ));
        }
        Ok(())
    }

    /// Settle search hits (or one retrieved page) against the inventory and
    /// send each placed, changed page to the consumer. A hit whose parent
    /// chain the inventory cannot resolve is counted and never indexed; the
    /// next full walk settles it. A trashed hit is confirmed by one page
    /// retrieve before removal, since search lags (`single` hits already are
    /// a retrieve).
    async fn discover_hits(
        &self,
        hits: Vec<notion_knowledge_notion::search::SearchHit>,
        single: bool,
        send: &tokio::sync::mpsc::Sender<notion_knowledge_notion::walk::WalkedPage>,
    ) -> Result<HitSync, SyncError> {
        use notion_knowledge_core::discovery::{SkipReason, SkippedContent, inaccessible};
        let state = self.coordinator.state();
        let unavailable = |_| "cache inventory state unavailable";
        let mut out = HitSync::default();
        // Pages of this run sent to the consumer: the only placeable parents
        // besides the inventory (never skipped, trashed or excluded ones).
        let mut sent = Vec::new();
        // Oldest first, so a new parent is placed before its new children.
        for hit in hits.into_iter().rev() {
            let previous = state.inventory_entry(&hit.page_id).map_err(unavailable)?;
            if hit.in_trash {
                let Some(mut entry) = previous else { continue };
                let confirmed = single
                    || match self
                        .source
                        .page_hit(&notion_knowledge_core::backend::PageId(hit.page_id.clone()))
                        .await
                    {
                        Ok(confirmed) => confirmed.is_some_and(|c| c.in_trash),
                        Err(e) if inaccessible(e.kind) => false,
                        Err(e) => return Err(SyncError::source(&e)),
                    };
                if confirmed {
                    self.invalidate_source(&hit.page_id, true)?;
                    entry.in_trash = true;
                    entry.last_edited_time = hit.last_edited_time.clone();
                    out.skipped.push(SkippedContent {
                        id: hit.page_id.clone(),
                        reason: SkipReason::Archived,
                    });
                    out.removals.push(hit.page_id.clone());
                    out.entries.push(entry);
                }
                continue;
            }
            // Read after its edit minute closed: a later edit would show.
            if !single
                && state
                    .inventory_settled_at(&hit.page_id, &hit.last_edited_time)
                    .map_err(unavailable)?
                    .zip(
                        chrono::DateTime::parse_from_rfc3339(&hit.last_edited_time)
                            .ok()
                            .map(|t| t.timestamp()),
                    )
                    .is_some_and(|(fetched, edited)| fetched >= edited + 120)
            {
                continue;
            }
            let Some((parent, root)) = self.place(&hit, previous, &sent)? else {
                self.invalidate_source(&hit.page_id, true)?;
                out.unplaced += 1;
                continue;
            };
            let page = match self
                .source
                .read_placed(&self.source_scope, &hit, parent, root)
                .await
            {
                Ok(Some(page)) => page,
                Ok(None) => {
                    self.invalidate_source(&hit.page_id, true)?;
                    out.unplaced += 1;
                    continue;
                }
                Err(e) if inaccessible(e.kind) => {
                    self.invalidate_source(&hit.page_id, true)?;
                    out.unplaced += 1;
                    continue;
                }
                Err(e) => {
                    self.invalidate_source(&hit.page_id, false)?;
                    return Err(SyncError::source(&e));
                }
            };
            out.entries.push(page.entry.clone());
            if let Some(reason) = page.skip {
                out.skipped.push(SkippedContent {
                    id: hit.page_id.clone(),
                    reason,
                });
                // An unreadable body or unparseable metadata is doubt, not
                // removal: the walk decides.
                if !matches!(
                    reason,
                    SkipReason::Inaccessible | SkipReason::UnreadableMetadata
                ) {
                    out.removals.push(hit.page_id.clone());
                }
                continue;
            }
            sent.push(page.entry.clone());
            send.send(page)
                .await
                .map_err(|_| "cache inventory failed")?;
        }
        Ok(out)
    }

    /// Where the inventory places a hit: under a configured root, under an
    /// inventoried page with a chain to the root, beside rows of a known data
    /// source, or, for a known page whose parent search cannot name (a
    /// block), where the last walk found it. `None` is unplaced.
    fn place(
        &self,
        hit: &notion_knowledge_notion::search::SearchHit,
        previous: Option<notion_knowledge_core::inventory::InventoryEntry>,
        placed: &[notion_knowledge_core::inventory::InventoryEntry],
    ) -> Result<Option<(notion_knowledge_core::inventory::InventoryParent, String)>, &'static str>
    {
        use notion_knowledge_core::inventory::{InventoryOutcome, InventoryParent};
        use notion_knowledge_notion::search::HitParent;
        let state = self.coordinator.state();
        let unavailable = |_| "cache inventory state unavailable";
        if self.source_scope.roots.iter().any(|r| r.0 == hit.page_id) {
            return Ok(Some((InventoryParent::Root, hit.page_id.clone())));
        }
        Ok(match &hit.parent {
            // A configured root places its children with no inventory yet.
            HitParent::Page(parent) if self.source_scope.roots.iter().any(|r| &r.0 == parent) => {
                (!self.source_scope.exclusions.descendants_of.contains(parent))
                    .then(|| (InventoryParent::Page(parent.clone()), parent.clone()))
            }
            HitParent::Page(parent) if placed.iter().any(|e| &e.page_id == parent) => {
                let entry = placed.iter().find(|e| &e.page_id == parent).unwrap();
                // Pruned the same way as an inventoried parent.
                (!self.source_scope.exclusions.descendants_of.contains(parent))
                    .then(|| (InventoryParent::Page(parent.clone()), entry.root_id.clone()))
            }
            HitParent::Page(parent) => {
                let Some(entry) = state.inventory_entry(parent).map_err(unavailable)? else {
                    return Ok(None);
                };
                let pruned = self.source_scope.exclusions.descendants_of.contains(parent)
                    || matches!(
                        state.inventory_outcome(parent).map_err(unavailable)?,
                        Some(InventoryOutcome::Skipped(_))
                    );
                (!pruned
                    && state
                        .inventory_parent_chain(parent)
                        .map_err(unavailable)?
                        .is_some())
                .then(|| (InventoryParent::Page(parent.clone()), entry.root_id))
            }
            HitParent::DataSource(source) => state
                .inventory_data_source(source)
                .map_err(unavailable)?
                .map(|row| (row.parent, row.root_id)),
            HitParent::Other => previous.map(|entry| (entry.parent, entry.root_id)),
        })
    }

    fn invalidate_source(&self, page: &str, affects_subtree: bool) -> Result<(), SyncError> {
        self.coordinator
            .state()
            .invalidate_source_page(
                self.coordinator.binding().workspace(),
                page,
                affects_subtree,
            )
            .map_err(|_| "source cache freshness unavailable".into())
    }

    async fn owned_source_and_search_pages(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, &'static str> {
        let mut candidates: std::collections::BTreeSet<_> = self
            .coordinator
            .owned_page_ids()
            .map_err(|_| "cache sync state unavailable")?
            .into_iter()
            .collect();
        candidates.extend(
            notion_knowledge_retrieval::page_documents::PageDocuments::new(
                self.coordinator.clone(),
            )
            .page_ids()
            .await
            .map_err(|_| "source cache unavailable")?,
        );
        Ok(candidates)
    }

    /// Indexed pages a complete walk did not reach that sit under a page
    /// newly answering 404/403. A spurious 404 then costs one walk of
    /// staleness instead of the subtree; the next walk confirms and removes.
    async fn held_removals(
        &self,
        walk: &notion_knowledge_notion::walk::TreeWalk,
    ) -> Result<std::collections::BTreeSet<String>, &'static str> {
        use notion_knowledge_core::discovery::SkipReason;
        let readable = walk.readable();
        let candidates: Vec<_> = self
            .owned_source_and_search_pages()
            .await?
            .into_iter()
            .filter(|page| !readable.contains(page))
            .collect();
        let inaccessible: Vec<_> = walk
            .skipped
            .iter()
            .filter(|s| s.reason == SkipReason::Inaccessible)
            .map(|s| s.id.clone())
            .collect();
        self.coordinator
            .state()
            .held_removals(&inaccessible, &candidates)
            .map_err(|_| "cache inventory state unavailable")
    }

    /// After a complete walk, remove every indexed page the walk did not
    /// reach as indexable: moved out, trashed, revoked, excluded or deleted.
    /// `held` pages wait for the next walk (see [`Self::held_removals`]).
    async fn remove_unreached(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        walk: &notion_knowledge_notion::walk::TreeWalk,
        held: &std::collections::BTreeSet<String>,
        base: &Clock,
    ) -> Result<u64, &'static str> {
        let readable = walk.readable();
        let unreached: Vec<_> = self
            .owned_source_and_search_pages()
            .await?
            .into_iter()
            .filter(|page| !readable.contains(page) && !held.contains(page))
            .collect();
        self.remove_pages(settings, lease, &walk.run_id, &unreached, base)
            .await
    }

    /// After the walk recorded its inventory, resolve the relation and link
    /// targets that inventory now places in scope. A page committed during the
    /// walk could not see targets the walk reached later, so on a first walk
    /// they stayed unresolved until the next one. Each affected page is read
    /// once and committed again: unchanged chunks are not embedded. The pass
    /// is bounded by page count and time; a page left over, or one that fails,
    /// resolves at the next walk. A page failure never fails the walk.
    async fn resolve_relations(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        walk: &notion_knowledge_notion::walk::TreeWalk,
        unresolved: Vec<(String, Vec<String>)>,
        base: &Clock,
    ) -> Result<(), SyncError> {
        let state = self.coordinator.state();
        let ttl = settings.timeout_seconds as i64 + 30;
        let entries: std::collections::BTreeMap<_, _> = walk
            .entries
            .iter()
            .map(|entry| (entry.page_id.as_str(), entry))
            .collect();
        let mut resolved = 0;
        let started = std::time::Instant::now();
        for (page, targets) in unresolved {
            let Some(entry) = entries.get(page.as_str()).filter(|entry| !entry.in_trash) else {
                continue;
            };
            if !self
                .handler
                .any_resolvable(&targets)
                .await
                .is_ok_and(|resolvable| resolvable)
            {
                continue;
            }
            if resolved == RELATION_PASS_LIMIT {
                break;
            }
            resolved += 1;
            // A lost claim ends the pass; the walk's own record then fails its fence.
            if Self::fence(lease, base).is_err() || started.elapsed() > RELATION_PASS_BUDGET {
                break;
            }
            let Ok(renewed) = state.renew_lease(lease, now_ms() / 1000, ttl) else {
                break;
            };
            *lease = renewed;
            let operation = format!(
                "relations-{}",
                notion_knowledge_core::logging::CorrelationId::new().as_str()
            );
            let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let _attempt = Attempt(expired.clone());
            let flag = expired.clone();
            let claim = base.clone();
            let clock: Clock = Arc::new(move || {
                if flag.load(std::sync::atomic::Ordering::SeqCst) {
                    i64::MAX
                } else {
                    claim()
                }
            });
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(settings.timeout_seconds),
                Box::pin(self.handler.publish_relation_refresh(
                    &notion_knowledge_core::backend::PageId(page.clone()),
                    entry.parent.clone(),
                    &notion_knowledge_core::backend::PageId(entry.root_id.clone()),
                    &operation,
                    lease.clone(),
                    clock,
                )),
            )
            .await;
            expired.store(true, std::sync::atomic::Ordering::SeqCst);
            self.coordinator
                .drain_owned()
                .await
                .map_err(|_| "cache sync drain failed")?;
            if !matches!(result, Ok(Ok(_))) {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "operation": "resolve_relations",
                        "outcome": "deferred",
                        "page": page,
                    })
                );
            }
        }
        Ok(())
    }

    /// Remove each indexed page of `pages`; pages not indexed are ignored.
    async fn remove_pages(
        &self,
        settings: &PageDeadline,
        lease: &mut notion_knowledge_core::reconciliation::Lease,
        run_id: &str,
        pages: &[String],
        base: &Clock,
    ) -> Result<u64, &'static str> {
        use notion_knowledge_core::sync_state::SyncStateStore;
        let mut removed = 0;
        let state = self.coordinator.state();
        let ttl = settings.timeout_seconds as i64 + 30;
        for page in pages {
            let indexed = state
                .page_state(page)
                .map_err(|_| "cache sync state unavailable")?
                .is_some_and(|state| !state.is_tombstone());
            *lease = state
                .renew_lease(lease, now_ms() / 1000, ttl)
                .map_err(|_| "cache sync lease unavailable")?;
            let operation = format!("walk-removal-{run_id}-{page}");
            // Past the deadline the clock expires, so the owned commit's
            // deadline poll gives up its index guard instead of holding it.
            let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            // Also expire it if this future is dropped (stop or sync budget).
            let _attempt = Attempt(expired.clone());
            let flag = expired.clone();
            let claim = base.clone();
            let clock: Clock = Arc::new(move || {
                if flag.load(std::sync::atomic::Ordering::SeqCst) {
                    i64::MAX
                } else {
                    claim()
                }
            });
            let documents = notion_knowledge_retrieval::page_documents::PageDocuments::new(
                self.coordinator.clone(),
            );
            let page = notion_knowledge_core::backend::PageId(page.clone());
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(settings.timeout_seconds),
                async {
                    documents
                        .remove(page.clone(), lease.clone(), clock.clone())
                        .await
                        .map_err(|_| "source cache removal failed")?;
                    if indexed {
                        Box::pin(self.handler.remove_unreached(
                            &page,
                            &operation,
                            self.provider.as_ref(),
                            lease.clone(),
                            clock,
                        ))
                        .await
                        .map_err(|_| "cache sync removal failed")?;
                    }
                    Ok::<(), &'static str>(())
                },
            )
            .await;
            expired.store(true, std::sync::atomic::Ordering::SeqCst);
            self.coordinator
                .drain_owned()
                .await
                .map_err(|_| "cache sync drain failed")?;
            result
                .map_err(|_| "cache sync removal timeout")?
                .map_err(|_| "cache sync removal failed")?;
            removed += u64::from(indexed);
        }
        Ok(removed)
    }

    /// Persist the walk and settle every reached page: indexed, or skipped
    /// with a typed reason. Pages still pending are left without an outcome.
    fn record_inventory(
        &self,
        walk: &notion_knowledge_notion::walk::TreeWalk,
        outcomes: Vec<(String, InventoryOutcome)>,
    ) -> Result<notion_knowledge_core::inventory::InventoryDiff, &'static str> {
        let state = self.coordinator.state();
        let unavailable = |_| "cache inventory state unavailable";
        let diff = state
            .record_walk(&walk.run_id, &walk.entries, walk.authoritative())
            .map_err(unavailable)?;
        self.record_outcomes(&walk.entries, &walk.skipped, outcomes)?;
        Ok(diff)
    }

    fn record_outcomes(
        &self,
        entries: &[notion_knowledge_core::inventory::InventoryEntry],
        skipped: &[notion_knowledge_core::discovery::SkippedContent],
        outcomes: Vec<(String, InventoryOutcome)>,
    ) -> Result<(), &'static str> {
        let state = self.coordinator.state();
        let unavailable = |_| "cache inventory state unavailable";
        let now = now_ms() / 1000;
        for skipped in skipped {
            state
                .record_inventory_outcome(
                    &skipped.id,
                    &InventoryOutcome::Skipped(skipped.reason),
                    now,
                )
                .map_err(unavailable)?;
        }
        let revisions: std::collections::BTreeMap<_, _> = entries
            .iter()
            .map(|e| (e.page_id.as_str(), e.last_edited_time.as_str()))
            .collect();
        for (page, mut outcome) in outcomes {
            if let InventoryOutcome::Indexed { revision } = &mut outcome {
                // The walk's revision: the page was prepared from that read.
                *revision = revisions
                    .get(page.as_str())
                    .copied()
                    .unwrap_or_default()
                    .to_owned();
            }
            state
                .record_inventory_outcome(&page, &outcome, now)
                .map_err(unavailable)?;
        }
        Ok(())
    }

    /// Local preparation for one walked page. No request is made for the
    /// page itself; relation targets share the client's rate limiter.
    async fn prepare_walked_page(
        &self,
        settings: &PageDeadline,
        budget: &PrefetchBudget,
        index: usize,
        walked: notion_knowledge_notion::walk::WalkedPage,
    ) -> Prefetched {
        use notion_knowledge_core::logging::{SyncCount, SyncPhase, sync_count, sync_task};
        let page = notion_knowledge_core::backend::PageId(walked.entry.page_id.clone());
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
                walked.timings.scope(
                    ApiPhase::Prepare,
                    Box::pin(self.handler.prepare_walked(&walked, &operation)),
                ),
            ),
        )
        .await;
        match result {
            Err(_) => Prefetched::TimedOut { page, operation },
            Ok(Err(error)) => Prefetched::Failed {
                page,
                operation,
                error,
            },
            Ok(Ok(prefetched)) => {
                let Some(bytes) = budget.retain(reservation, prefetched.retained_bytes()) else {
                    // Never queue more than was reserved. The consumer prepares
                    // this rare page again from the same read when it is next.
                    drop(prefetched);
                    return Prefetched::Deferred {
                        walked: Box::new(walked),
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
        use std::time::Duration;
        let timeout_seconds = settings.timeout_seconds;
        let state = self.coordinator.state();
        let mut lease = state
            .acquire_lease(now_ms() / 1000, timeout_seconds as i64 + 30)
            .map_err(|_| "cache sync writer busy")?;
        let result = async {
            let totals = tokio::time::timeout(
                Duration::from_secs(settings.inventory_timeout_seconds),
                Box::pin(self.sync_from_walk(
                    &PageDeadline { timeout_seconds },
                    &mut lease,
                    SyncMode::Full,
                )),
            )
            .await
            .map_err(|_| "cache inventory timeout; retry startup")?
            .map_err(|e| e.reason)?;
            lease = state
                .renew_lease(&lease, now_ms() / 1000, timeout_seconds as i64 + 30)
                .map_err(|_| "cache sync lease unavailable")?;
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
            let changed = (totals.indexed + totals.removed) as usize;
            let mut preparing = SyncProgressLog::new(SyncPhase::PreparingSearch);
            preparing.report(0, None, changed, totals.failed as usize, false);
            tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
                self.prepare_search(lease.clone(), clock).await?;
                if self.lexical.is_some() {
                    Box::pin(self.table.probe_lexical_cache())
                        .await
                        .map_err(|_| "lexical search probe failed")?;
                }
                Ok::<(), &'static str>(())
            })
            .await
            .map_err(|_| "rebuilt search index unavailable")?
            .map_err(|_| "rebuilt search index unavailable")?;
            preparing.report(1, Some(1), changed, totals.failed as usize, true);
            if totals.failed > 0 || totals.held > 0 || !totals.complete {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "operation": "cache_sync",
                        "status": "ready_with_skips",
                        "sync_progress": notion_knowledge_core::logging::current_sync_progress(),
                        "skipped_pages": totals.failed,
                        "walk_complete": totals.complete,
                        "held_removals": totals.held,
                    })
                );
            }
            state
                .finish_cache_rebuild(&lease, now_ms() / 1000)
                .map_err(|_| "cache sync state unavailable")
        }
        .await;
        if result.is_err() {
            let _ = self.coordinator.drain_owned().await;
        }
        let _ = state.release_lease(&lease, now_ms() / 1000);
        result
    }

    /// Local operator composition only; no caller may choose a workspace/root.
    /// Candidates remain subject to authoritative content authorization.
    pub fn lookup_alias(
        &self,
        alias: &str,
    ) -> Result<Vec<String>, notion_knowledge_retrieval::commit::CommitError> {
        match &self.alias_sources {
            Some(sources) => self.coordinator.lookup_canonical_alias(sources, alias),
            None => Ok(Vec::new()),
        }
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
                let guard = if let Some(plan) = &lexical
                    && !settings.index.join("chunks.lance").exists()
                {
                    Box::pin(GuardedChunkTable::create_fresh_lexical(
                        &settings.index,
                        "chunks",
                        provider.metadata().clone(),
                        coordinator.clone(),
                        plan.identity(),
                        lease.clone(),
                        clock.clone(),
                    ))
                    .await
                    .map_err(|_| "fresh lexical cache unavailable")?
                } else {
                    let guard = bind_cache(
                        &settings.index,
                        provider.metadata().clone(),
                        coordinator.clone(),
                        lexical.as_ref(),
                    )
                    .map_err(|_| "index binding incompatible")?;
                    let result = if settings.index.join("chunks.lance").exists() {
                        guard
                            .ensure_startup_indexes(lease.clone(), clock.clone())
                            .await
                    } else {
                        guard.create_empty(lease.clone(), clock.clone()).await
                    };
                    result.map_err(|_| "index unavailable or incompatible")?;
                    guard
                };
                guard
                    .ensure_text_projection(lease.clone(), clock)
                    .await
                    .map_err(|_| "text projection unavailable or incompatible")?;
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
        Self::from_parts(RuntimeParts {
            source,
            table: guard,
            coordinator,
            scope,
            journal,
            provider,
            chunk_config: ChunkConfig {
                target_chars: 1000,
                overlap_chars: 100,
            },
            alias_sources: Some(settings.alias_sources.clone()),
            projection_mode: settings.projection_mode,
            lexical,
        })
        .map(|runtime| runtime.with_prefetch_workers(settings.prefetch_workers))
    }

    /// Concurrent page preparations ahead of the consumer; zero prepares inline.
    pub fn with_prefetch_workers(mut self, workers: usize) -> Self {
        self.prefetch_workers = workers;
        self
    }

    /// Compose a runtime over an opened index and one source client. Startup
    /// recovery uses it after binding the cache; callers holding their own
    /// client (a fixture) use it to sync through that client.
    pub fn from_parts(parts: RuntimeParts) -> Result<Self, &'static str> {
        let RuntimeParts {
            source,
            table,
            coordinator,
            scope,
            journal,
            provider,
            chunk_config,
            alias_sources,
            projection_mode,
            lexical,
        } = parts;
        let handler = AuthoritativePageRefresh::new(
            source.clone(),
            table.clone(),
            coordinator.clone(),
            scope.clone(),
            &journal,
            chunk_config,
        )
        .map_err(|_| "authoritative handler incompatible")?
        .with_projection_mode(projection_mode);
        let handler = match &alias_sources {
            Some(sources) => handler.with_alias_sources(sources.clone()),
            None => handler,
        };
        let prefetch_budget = PrefetchBudget::new(PREFETCH_BUDGET_KIB, PREFETCH_RESERVE_KIB);
        Ok(Self {
            coordinator,
            scope: journal,
            handler,
            provider,
            alias_sources,
            table,
            lexical,
            source,
            source_scope: scope,
            prefetch_budget,
            prefetch_workers: PREFETCH_WORKERS,
        })
    }
}

/// What [`IndexRuntime::from_parts`] binds together.
pub struct RuntimeParts {
    pub source: Arc<NotionClient>,
    pub table: GuardedChunkTable,
    pub coordinator: Arc<IndexCommitCoordinator>,
    pub scope: LifecycleScope,
    pub journal: ReconciliationScope,
    pub provider: Arc<dyn EmbeddingProvider>,
    pub chunk_config: ChunkConfig,
    /// `None` projects no aliases.
    pub alias_sources: Option<notion_knowledge_core::aliases::AliasSources>,
    pub projection_mode: notion_knowledge_notion::relation_proposal::ProjectionMode,
    pub lexical: Option<LexicalCachePlan>,
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

/// Roll back a hot rollback journal left by a crash before any read-only
/// open, logging the cause when recovery fails. A journal beside a WAL
/// file is left alone; the read-only open then refuses WAL as before.
fn recover_hot_journal(state: &Path) -> Result<(), &'static str> {
    use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
    match SqliteSyncStateStore::recover_hot_journal(state) {
        Ok(false) => Ok(()),
        Ok(true) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "operation": "operational_state_recovery",
                    "outcome": "journal_rollback_attempted",
                })
            );
            Ok(())
        }
        Err(error) => {
            log_state_error("hot_journal_recovery", &error);
            Err("operational state unavailable")
        }
    }
}

/// Keep the startup error generic but log the underlying cause.
fn log_state_error(stage: &str, error: &str) {
    eprintln!(
        "{}",
        serde_json::json!({
            "operation": "operational_state_open",
            "stage": stage,
            "outcome": "failed",
            "error": error,
        })
    );
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

/// Name the refusal class so a failed startup reset is diagnosable.
fn reset_failure(error: notion_knowledge_retrieval::commit::CommitError) -> &'static str {
    use notion_knowledge_retrieval::commit::CommitError;
    match error {
        CommitError::Conflict => "cache reset unavailable: lease or index lock still held",
        CommitError::InvalidInput => "cache reset unavailable: invalid cache or state path",
        CommitError::BindingMismatch => "cache reset unavailable: index binding mismatch",
        CommitError::Unavailable => "cache reset unavailable: operational state unavailable",
        _ => "cache reset unavailable: index operation failed",
    }
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
            prefetch_workers: 2,
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
                .settle_page_timeout(
                    &PageDeadline {
                        timeout_seconds: settings.timeout_seconds,
                    },
                    &mut lease,
                    "timed-out",
                )
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
                    .settle_page_timeout(
                        &PageDeadline {
                            timeout_seconds: settings.timeout_seconds
                        },
                        &mut lease,
                        "no-receipt"
                    )
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
                .settle_page_timeout(
                    &PageDeadline {
                        timeout_seconds: settings.timeout_seconds,
                    },
                    &mut lease,
                    "no-receipt",
                )
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
