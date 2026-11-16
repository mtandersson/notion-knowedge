//! Phase 3 one-page, authoritative Notion -> durable SQLite/LanceDB refresh.
#![cfg(all(unix, feature = "local-lancedb"))]
//!
//! The webhook is only a hint for which ID to inspect. The actual Notion
//! credential/scope and local index binding come from trusted operator config.
//! No receipts are acknowledged until the guarded local commit completes.
use std::sync::{Arc, atomic::Ordering};

use notion_knowledge_core::{
    aliases::AliasSources,
    backend::{BackendErrorKind, PageId},
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    lifecycle::{LifecycleEvidence, LifecycleScope, LifecycleStatus, PageLifecycle},
    reconciliation::{FailureClass, Lease, ReconciliationScope},
    root_scope::RootScopeGate,
    sync_state::{PageSyncState, SyncStateStore},
};
use notion_knowledge_notion::{NotionClient, relation_proposal::RelationProposalAdapter};
use sha2::{Digest, Sha256};

use crate::{
    chunks::{ChunkDiffMetrics, ChunkTableError, GuardedChunkTable},
    commit::{
        AliasProjection, CommitBinding, CommitError, CommitOutcome, IndexCommitCoordinator,
        PageAction, PageOperation, ReceiptStatus, RelationProjection,
    },
};

// Owned operations use a coarse failure port. Invalid credentials must remain
// fatal there, rather than being flattened into a skippable source/conflict error.
fn source_revalidation_failure(kind: BackendErrorKind) -> FailureClass {
    match kind {
        BackendErrorKind::Unauthenticated => FailureClass::Unavailable,
        BackendErrorKind::Conflict => FailureClass::Conflict,
        _ => FailureClass::Source,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshError {
    InvalidConfiguration,
    OutOfScope,
    Source,
    /// A sanitized source contract/authentication failure; automatic retry cannot repair it.
    PermanentSource,
    Unauthenticated,
    Index,
    Conflict,
    Unavailable,
    /// A readable atomic input exceeds the local model's configured budget.
    EmbeddingInputLimit,
}
impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "authoritative page refresh failed: {self:?}")
    }
}
impl std::error::Error for RefreshError {}

impl From<ChunkTableError> for RefreshError {
    fn from(error: ChunkTableError) -> Self {
        match error {
            ChunkTableError::Embedding(
                notion_knowledge_core::embedding::EmbeddingError::InputLimit,
            ) => Self::EmbeddingInputLimit,
            _ => Self::Index,
        }
    }
}
impl From<CommitError> for RefreshError {
    fn from(error: CommitError) -> Self {
        match error {
            CommitError::InvalidInput => Self::InvalidConfiguration,
            CommitError::ProjectionRepairRequired | CommitError::BindingMismatch => Self::Conflict,
            CommitError::LeaseLost | CommitError::Conflict => Self::Conflict,
            CommitError::Unavailable | CommitError::Operation(FailureClass::Unavailable) => {
                Self::Unavailable
            }
            CommitError::Operation(FailureClass::Conflict) => Self::Conflict,
            CommitError::Operation(FailureClass::Source) => Self::Source,
            CommitError::Operation(FailureClass::Index) => Self::Index,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshResult {
    pub outcome: CommitOutcome,
    pub diff: ChunkDiffMetrics,
}

/// A proposal checked again inside the operation-owned commit boundary.
#[derive(Clone)]
pub(crate) struct ExpectedReconciliation {
    pub run: String,
    pub page: crate::durable_plan::DurablePage,
    pub replan: bool,
}

/// Source-prepared page content awaiting the embedding/commit consumer.
/// It carries no index version, lease or receipt decision; those are taken
/// fresh by [`AuthoritativePageRefresh::refresh_prefetched`].
pub struct PrefetchedPage {
    page: PageId,
    root: PageId,
    operation: String,
    previous: Vec<notion_knowledge_core::indexed::IndexedChunk>,
    proposal: notion_knowledge_notion::relation_proposal::RelationProposal,
}
impl PrefetchedPage {
    pub fn page(&self) -> &PageId {
        &self.page
    }
    pub fn operation(&self) -> &str {
        &self.operation
    }
    /// Conservative estimate of the heap bytes this prepared page retains.
    pub fn retained_bytes(&self) -> usize {
        self.previous
            .iter()
            .map(notion_knowledge_notion::prepare::chunk_retained_bytes)
            .sum::<usize>()
            .saturating_add(self.proposal.retained_bytes())
    }
}

#[derive(Clone)]
pub struct AuthoritativePageRefresh {
    notion: Arc<NotionClient>,
    relations: RelationProposalAdapter,
    alias_sources: Option<AliasSources>,
    guard: GuardedChunkTable,
    coordinator: Arc<IndexCommitCoordinator>,
    scope: LifecycleScope,
    chunk_config: ChunkConfig,
}
impl AuthoritativePageRefresh {
    /// Retain the exact configured binding for post-refresh search maintenance.
    pub fn chunks(&self) -> &GuardedChunkTable {
        &self.guard
    }
    /// Accept only paired, trusted Notion workspace and persistent SQLite
    /// index binding. Do not infer either of them from incoming webhook events.
    pub fn new(
        notion: Arc<NotionClient>,
        guard: GuardedChunkTable,
        coordinator: Arc<IndexCommitCoordinator>,
        scope: LifecycleScope,
        journal_scope: &ReconciliationScope,
        chunk_config: ChunkConfig,
    ) -> Result<Self, RefreshError> {
        let binding: &CommitBinding = coordinator.binding();
        let expected = crate::planner::production_scope(&scope)
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        if !binding.matches_policy(&scope.workspace_id, &expected)
            || journal_scope != &expected
            || !guard.is_bound_to(&coordinator)
            || scope.roots.is_empty()
            || chunk_config.target_chars == 0
            || chunk_config.overlap_chars >= chunk_config.target_chars
        {
            return Err(RefreshError::InvalidConfiguration);
        }
        RootScopeGate::new(notion.clone(), scope.clone())
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        let relations = RelationProposalAdapter::new((*notion).clone(), scope.clone())
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        Ok(Self {
            relations,
            alias_sources: None,
            notion,
            guard,
            coordinator,
            scope,
            chunk_config,
        })
    }

    /// Apply the same trusted target projection policy in every refresh/planning role.
    pub fn with_projection_mode(
        mut self,
        mode: notion_knowledge_notion::relation_proposal::ProjectionMode,
    ) -> Self {
        self.relations = self.relations.with_projection_mode(mode);
        self
    }

    /// Trusted composition only: immutable source policy shared by runtime callers.
    pub fn with_alias_sources(mut self, sources: AliasSources) -> Self {
        self.alias_sources = Some(sources);
        self
    }

    fn aliases_complete(&self, projection: &Option<AliasProjection>) -> Result<bool, CommitError> {
        projection
            .as_ref()
            .map_or(Ok(true), |p| self.coordinator.alias_projection_complete(p))
    }
    fn current_aliases_complete(&self, page: &str) -> Result<bool, CommitError> {
        self.alias_sources.as_ref().map_or(Ok(true), |s| {
            self.coordinator.current_alias_projection_complete(page, s)
        })
    }
    fn unfinished_alias_repair(&self, operation: &str, page: &str) -> Result<bool, CommitError> {
        if self.alias_sources.is_none() {
            return Ok(false);
        }
        if !self
            .coordinator
            .receipt(operation)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied)
        {
            return Ok(false);
        }
        Ok(self
            .coordinator
            .unfinished_alias_repair_id(operation, page)?
            .is_some())
    }
    fn attach_aliases(
        &self,
        operation: PageOperation,
        aliases: Option<AliasProjection>,
    ) -> Result<PageOperation, CommitError> {
        aliases.map_or(Ok(operation.clone()), |p| {
            operation.with_alias_projection(p)
        })
    }

    /// Apply a page hint using current authoritative status, never its event
    /// type. All allowed transitions share the canonical refresh pipeline.
    /// Negative evidence is revalidated inside the same SQLite/Lance guard.
    pub async fn apply_lifecycle(
        &self,
        page: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        self.apply_lifecycle_expected(page, operation_id, provider, lease, clock, None, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply_lifecycle_expected(
        &self,
        page: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        expected: Option<ExpectedReconciliation>,
        // Read-ahead source preparation; used only if it still matches.
        prefetched: Option<PrefetchedPage>,
    ) -> Result<RefreshResult, RefreshError> {
        let evidence =
            self.notion
                .lifecycle(page, &self.scope)
                .await
                .map_err(|error| match error.kind {
                    BackendErrorKind::Conflict => RefreshError::Conflict,
                    BackendErrorKind::Unauthenticated => RefreshError::Unauthenticated,
                    BackendErrorKind::InvalidInput | BackendErrorKind::UnsupportedContent => {
                        RefreshError::PermanentSource
                    }
                    _ => RefreshError::Source,
                })?;
        if let Some(expected) = &expected {
            self.check_expected(expected, &evidence)
                .await
                .map_err(|e| match e {
                    FailureClass::Source => RefreshError::Source,
                    FailureClass::Index => RefreshError::Index,
                    FailureClass::Unavailable => RefreshError::Unavailable,
                    FailureClass::Conflict => RefreshError::Conflict,
                })?;
        }
        if let LifecycleStatus::Allowed { roots } = &evidence.status {
            let root = roots.first().ok_or(RefreshError::Source)?;
            let root = PageId(root.clone());
            // The durable plan was revalidated above from current state; a
            // read-ahead preparation replaces only the source read that follows.
            if let Some((previous, proposal)) = self
                .matching_prefetch(prefetched, page, &root, operation_id, true)
                .await?
            {
                return Box::pin(self.embed_and_commit(
                    page,
                    operation_id,
                    previous,
                    proposal,
                    provider,
                    lease,
                    clock,
                    true,
                    expected,
                    None,
                ))
                .await;
            }
            return self
                .refresh_page_inner(
                    page,
                    &root,
                    operation_id,
                    provider,
                    lease,
                    clock,
                    true,
                    expected,
                )
                .await;
        }
        let previous = self.guard.page_chunks(&page.0).await?;
        let owned_state = self
            .coordinator
            .state()
            .page_state(&page.0)
            .map_err(|_| RefreshError::Unavailable)?
            .is_some();
        let owned_work = self
            .coordinator
            .owns_page_work(&page.0)
            .map_err(RefreshError::from)?;
        if (!owned_state && !owned_work && previous.is_empty())
            || previous.iter().any(|chunk| {
                chunk.metadata.source.workspace_id != self.scope.workspace_id
                    || !self
                        .scope
                        .roots
                        .iter()
                        .any(|root| root.0 == chunk.metadata.source.root_page_id)
            })
        {
            return Err(RefreshError::OutOfScope);
        }
        let selected = evidence.ancestry.first().ok_or(RefreshError::Source)?;
        let checkpoint = PageSyncState::tombstone(page.0.clone(), Some(selected.revision.clone()))
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        // Evidence includes the physical chain and policy, not merely a source
        // timestamp. Its debug representation contains no private content.
        let revision = format!("{:x}", Sha256::digest(format!("{evidence:?}").as_bytes()));
        self.coordinator
            .validate_page_receipt_authority(operation_id, &page.0)?;
        // Replaying an old acknowledged delete cannot delete a later restored
        // snapshot. Its current projection must still be durably complete.
        if self
            .coordinator
            .receipt(operation_id)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied)
            && self
                .coordinator
                .state()
                .page_state(&page.0)
                .map_err(|_| RefreshError::Index)?
                .is_some_and(|state| !state.is_tombstone())
        {
            if self.unfinished_alias_repair(operation_id, &page.0)?
                || self
                    .coordinator
                    .unfinished_relation_repair_id(operation_id)?
                    .is_some()
            {
                return Err(RefreshError::Conflict);
            }
            let handler = self.clone();
            let page = page.clone();
            self.check_owned_snapshot(lease, clock, move |context| async move {
                let _authority = handler
                    .relations
                    .revalidate_removal(&evidence)
                    .await
                    .map_err(|_| FailureClass::Conflict)?;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                if !handler
                    .current_aliases_complete(&page.0)
                    .map_err(|_| FailureClass::Index)?
                    || !handler
                        .coordinator
                        .current_body_links_complete(&page.0)
                        .map_err(|_| FailureClass::Index)?
                {
                    return Err(FailureClass::Conflict);
                }
                Ok(())
            })
            .await?;
            return Ok(RefreshResult {
                outcome: CommitOutcome::AlreadyApplied,
                diff: Default::default(),
            });
        }
        let projection =
            RelationProjection::new(self.coordinator.binding(), &page.0, true, vec![])?
                .with_links(vec![])?;
        let aliases = self
            .alias_sources
            .as_ref()
            .map(|sources| {
                AliasProjection::tombstone(
                    self.coordinator.binding(),
                    &self.coordinator.alias_root(&page.0)?,
                    &page.0,
                    sources,
                )
                .map(AliasProjection::with_owned_root_cleanup)
            })
            .transpose()?;
        let alias_complete = self.aliases_complete(&aliases)?;
        let alias_unfinished = self.unfinished_alias_repair(operation_id, &page.0)?;
        if self
            .coordinator
            .receipt(operation_id)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied)
            && alias_complete
            && !alias_unfinished
            && self.coordinator.relation_projection_complete(&projection)?
            && self
                .coordinator
                .unfinished_relation_repair_id(operation_id)?
                .is_none()
        {
            let handler = self.clone();
            self.check_owned_snapshot(lease, clock, move |context| async move {
                let _authority = handler
                    .relations
                    .revalidate_removal(&evidence)
                    .await
                    .map_err(|_| FailureClass::Conflict)?;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                if !handler
                    .aliases_complete(&aliases)
                    .map_err(|_| FailureClass::Index)?
                    || !handler
                        .coordinator
                        .relation_projection_complete(&projection)
                        .map_err(|_| FailureClass::Index)?
                {
                    return Err(FailureClass::Conflict);
                }
                Ok(())
            })
            .await?;
            return Ok(RefreshResult {
                outcome: CommitOutcome::AlreadyApplied,
                diff: Default::default(),
            });
        }
        let repair_id;
        let operation_id = if self
            .coordinator
            .receipt(operation_id)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied)
            && (!alias_complete
                || alias_unfinished
                || !self.coordinator.relation_projection_complete(&projection)?
                || self
                    .coordinator
                    .unfinished_relation_repair_id(operation_id)?
                    .is_some())
        {
            repair_id = if let Some(unfinished) = self
                .coordinator
                .unfinished_relation_repair_id(operation_id)?
            {
                unfinished
            } else if self.alias_sources.is_some() {
                self.coordinator.alias_repair_id(operation_id, &page.0)?
            } else {
                self.coordinator.relation_repair_id(operation_id)?
            };
            &repair_id
        } else {
            operation_id
        };
        let operation = PageOperation::retryable_lifecycle(
            operation_id,
            &revision,
            PageAction::Delete,
            checkpoint,
        )
        .map_err(RefreshError::from)?
        .with_relation_projection(projection)?;
        let operation = self.attach_aliases(operation, aliases)?;
        let expected_operation = operation.clone();
        let prepared = self.guard.prepare_page(provider, operation, &[]).await?;
        let diff = prepared.metrics();
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        let expected_lease = lease.clone();
        let handler = self.clone();
        let outcome = self
            .guard
            .commit_page_with_authority(prepared, lease, clock, move || async move {
                owned_revalidation(
                    async {
                        if let Some(expected) = &expected {
                            handler.check_expected(expected, &evidence).await?;
                            handler
                                .coordinator
                                .prepare_reconciliation_operation(
                                    &expected_lease,
                                    owned_clock(),
                                    &expected.run,
                                    &expected_operation,
                                    &evidence,
                                    expected.replan,
                                )
                                .map_err(|_| FailureClass::Conflict)?;
                        }
                        handler
                            .relations
                            .revalidate_removal(&evidence)
                            .await
                            .map_err(|error| source_revalidation_failure(error.kind))
                    },
                    owned_clock.clone(),
                    expires_at,
                )
                .await
            })
            .await
            .map_err(RefreshError::from)?;
        Ok(RefreshResult { outcome, diff })
    }

    /// A single refresh for a page hinted by a webhook, manual command, or
    /// reconciliation. Callers own bounded retries and durable inbox claims.
    /// A lost SQLite fence or stale Lance version is a retryable conflict; the
    /// earlier proposal MUST be discarded and recomputed.
    pub async fn refresh_page(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        self.refresh_page_inner(
            page,
            root,
            operation_id,
            provider,
            lease,
            clock,
            false,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn refresh_page_inner(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        lifecycle: bool,
        expected: Option<ExpectedReconciliation>,
    ) -> Result<RefreshResult, RefreshError> {
        let PrefetchedPage {
            previous,
            proposal: relation_proposal,
            ..
        } = Box::pin(self.prefetch_page(page, root, operation_id)).await?;
        Box::pin(self.embed_and_commit(
            page,
            operation_id,
            previous,
            relation_proposal,
            provider,
            lease,
            clock,
            lifecycle,
            expected,
            None,
        ))
        .await
    }

    /// Read-only source preparation for one page: local receipt authority,
    /// current Notion authorization, content and chunk drafts. No embedding,
    /// index version, lease or durable effect is captured here, so a caller may
    /// run several of these concurrently ahead of the single embedding/commit
    /// consumer. Commit still revalidates the source inside its owned guard.
    pub async fn prefetch_page(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
    ) -> Result<PrefetchedPage, RefreshError> {
        Box::pin(self.prepare_source(page, root, operation_id, None)).await
    }

    /// Read-ahead source preparation for a planned reconciliation page. The
    /// root is the single configured root, or else the page's indexed
    /// provenance; the consumer discards it unless its own lifecycle read
    /// selects the same root. No durable plan state is checked or captured
    /// here: that stays with the consumer. Any failure yields `None`, and the
    /// consumer then prepares the page itself exactly as without read-ahead.
    pub(crate) async fn prefetch_reconciliation(
        &self,
        page: &PageId,
        operation_id: &str,
    ) -> Option<PrefetchedPage> {
        let root = if let [root] = self.scope.roots.as_slice() {
            root.clone()
        } else {
            let previous = self.guard.page_chunks(&page.0).await.ok()?;
            PageId(previous.first()?.metadata.source.root_page_id.clone())
        };
        Box::pin(self.prefetch_page(page, &root, operation_id))
            .await
            .ok()
    }

    /// Take a read-ahead preparation only if it was made for this page,
    /// operation and root and this page's indexed chunks did not change since,
    /// so its chunk identities are still current. `authority` re-checks the
    /// receipt authority the inline preparation would have checked.
    async fn matching_prefetch(
        &self,
        prefetched: Option<PrefetchedPage>,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        authority: bool,
    ) -> Result<
        Option<(
            Vec<notion_knowledge_core::indexed::IndexedChunk>,
            notion_knowledge_notion::relation_proposal::RelationProposal,
        )>,
        RefreshError,
    > {
        let Some(prefetched) = prefetched else {
            return Ok(None);
        };
        if prefetched.page != *page
            || prefetched.root != *root
            || prefetched.operation != operation_id
            || self.guard.page_chunks(&page.0).await? != prefetched.previous
        {
            return Ok(None);
        }
        if authority {
            self.coordinator
                .validate_page_receipt_authority(operation_id, &page.0)?;
        }
        Ok(Some((prefetched.previous, prefetched.proposal)))
    }

    /// [`Self::prefetch_page`], additionally marking `source_changed` when the
    /// conflict means only that the source changed during preparation
    /// ([`notion_knowledge_notion::prepare::SOURCE_CHANGED`]). Receipt
    /// authority, scope policy, bounds, lock, lease, storage and ownership
    /// conflicts are never marked and stay fatal.
    async fn prepare_source(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        source_changed: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<PrefetchedPage, RefreshError> {
        if !self.scope.roots.contains(root) || operation_id.is_empty() {
            return Err(RefreshError::InvalidConfiguration);
        }

        self.coordinator
            .validate_page_receipt_authority(operation_id, &page.0)?;

        // This is trusted local indexed state, NOT a physical scope decision.
        // The notion adapter validates its previous-page provenance/IDs/hash
        // after live Notion ancestry authorization.
        let previous = self.guard.page_chunks(&page.0).await?;
        let proposal = self
            .relations
            .prepare(page, root, &previous, self.chunk_config)
            .await
            .map_err(|e| match e.kind {
                BackendErrorKind::PermissionDenied => RefreshError::OutOfScope,
                BackendErrorKind::Conflict => {
                    if e.operation == notion_knowledge_notion::prepare::SOURCE_CHANGED
                        && let Some(flag) = source_changed
                    {
                        flag.store(true, Ordering::SeqCst);
                    }
                    RefreshError::Conflict
                }
                BackendErrorKind::Unauthenticated => RefreshError::Unauthenticated,
                BackendErrorKind::InvalidInput | BackendErrorKind::UnsupportedContent => {
                    RefreshError::PermanentSource
                }
                _ => RefreshError::Source,
            })?;
        Ok(PrefetchedPage {
            page: page.clone(),
            root: root.clone(),
            operation: operation_id.to_owned(),
            previous,
            proposal,
        })
    }

    /// Embed and durably commit a page prepared by [`Self::prefetch_page`].
    /// The guarded mutation plan (pinned index version, receipt and projection
    /// state) is built here from current local state, never from prefetch time.
    /// When this page's own indexed chunks changed while it waited, its
    /// chunk-identity preparation is stale and the source is prepared again.
    pub async fn refresh_prefetched(
        &self,
        prefetched: PrefetchedPage,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let PrefetchedPage {
            page,
            root,
            operation,
            previous,
            proposal,
        } = prefetched;
        let (previous, proposal) = if self.guard.page_chunks(&page.0).await? == previous {
            self.coordinator
                .validate_page_receipt_authority(&operation, &page.0)?;
            (previous, proposal)
        } else {
            drop((previous, proposal));
            let fresh = Box::pin(self.prefetch_page(&page, &root, &operation)).await?;
            (fresh.previous, fresh.proposal)
        };
        Box::pin(self.commit_with_one_retry(
            &page, &root, &operation, previous, proposal, provider, lease, clock, true,
        ))
        .await
    }

    /// Prepare and commit a page the streaming consumer could not take from
    /// the prefetch queue (too large to retain, or its source changed while it
    /// was prefetched). It gets the same single bounded re-preparation as a
    /// queued prefetched page: a source change during preparation or commit
    /// re-prepares once from current state, and a page that changes again is a
    /// page-local transient [`RefreshError::Source`] failure. Any other conflict
    /// (receipt authority, lease, ownership, storage) is returned unchanged.
    /// The bound is per call: a page the producer already saw change during
    /// prefetch is handed here and may be prepared at most twice more.
    pub async fn refresh_inline(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let changed = std::sync::atomic::AtomicBool::new(false);
        let (prepared, retry) =
            match Box::pin(self.prepare_source(page, root, operation_id, Some(&changed))).await {
                Err(RefreshError::Conflict) if changed.load(Ordering::SeqCst) => {
                    // The single retry is spent on re-preparation.
                    (
                        Box::pin(self.prepare_once_more(page, root, operation_id)).await?,
                        false,
                    )
                }
                prepared => (prepared?, true),
            };
        Box::pin(self.commit_with_one_retry(
            page,
            root,
            operation_id,
            prepared.previous,
            prepared.proposal,
            provider,
            lease,
            clock,
            retry,
        ))
        .await
    }

    /// The single re-preparation after a source change. A source that changes
    /// again is a page-local transient failure; other errors are unchanged.
    async fn prepare_once_more(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
    ) -> Result<PrefetchedPage, RefreshError> {
        let changed = std::sync::atomic::AtomicBool::new(false);
        match Box::pin(self.prepare_source(page, root, operation_id, Some(&changed))).await {
            Err(RefreshError::Conflict) if changed.load(Ordering::SeqCst) => {
                Err(RefreshError::Source)
            }
            prepared => prepared,
        }
    }

    /// Embed and commit a prepared page. When guarded source revalidation
    /// reports the page changed and nothing was committed, it is prepared once
    /// more (if `retry`) and committed through the same guards; a page that
    /// keeps changing is a page-local transient source failure.
    #[allow(clippy::too_many_arguments)]
    async fn commit_with_one_retry(
        &self,
        page: &PageId,
        root: &PageId,
        operation: &str,
        previous: Vec<notion_knowledge_core::indexed::IndexedChunk>,
        proposal: notion_knowledge_notion::relation_proposal::RelationProposal,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        retry: bool,
    ) -> Result<RefreshResult, RefreshError> {
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first = Box::pin(self.embed_and_commit(
            page,
            operation,
            previous,
            proposal,
            provider,
            lease.clone(),
            clock.clone(),
            false,
            None,
            Some(changed.clone()),
        ))
        .await;
        if !matches!(first, Err(RefreshError::Conflict)) || !changed.load(Ordering::SeqCst) {
            return first;
        }
        if !retry {
            return Err(RefreshError::Source);
        }
        // The source changed after preparation and nothing was committed.
        // Prepare this page once more from current source and local state;
        // every guard still applies. A page that keeps changing is a
        // page-local transient source failure, so its siblings continue.
        changed.store(false, Ordering::SeqCst);
        let fresh = Box::pin(self.prepare_once_more(page, root, operation)).await?;
        let retry = Box::pin(self.embed_and_commit(
            page,
            operation,
            fresh.previous,
            fresh.proposal,
            provider,
            lease,
            clock,
            false,
            None,
            Some(changed.clone()),
        ))
        .await;
        if matches!(retry, Err(RefreshError::Conflict)) && changed.load(Ordering::SeqCst) {
            return Err(RefreshError::Source);
        }
        retry
    }

    #[allow(clippy::too_many_arguments)]
    async fn embed_and_commit(
        &self,
        page: &PageId,
        operation_id: &str,
        previous: Vec<notion_knowledge_core::indexed::IndexedChunk>,
        relation_proposal: notion_knowledge_notion::relation_proposal::RelationProposal,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        lifecycle: bool,
        expected: Option<ExpectedReconciliation>,
        // Set when guarded source revalidation reports the page changed.
        source_changed: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<RefreshResult, RefreshError> {
        let mark_changed = move |kind: BackendErrorKind| {
            if kind == BackendErrorKind::Conflict
                && let Some(flag) = &source_changed
            {
                flag.store(true, Ordering::SeqCst);
            }
        };
        let proposal = relation_proposal.snapshot().clone();
        let projection = RelationProjection::new(
            self.coordinator.binding(),
            &page.0,
            false,
            relation_proposal.edges().to_vec(),
        )?
        .with_links(relation_proposal.links().to_vec())?;
        let aliases = self
            .alias_sources
            .as_ref()
            .map(|sources| {
                AliasProjection::from_indexed(
                    self.coordinator.binding(),
                    &proposal.document.metadata,
                    sources,
                )
                .map(AliasProjection::with_owned_root_cleanup)
            })
            .transpose()?;
        let alias_complete = self.aliases_complete(&aliases)?;
        let alias_unfinished = self.unfinished_alias_repair(operation_id, &page.0)?;
        let complete =
            alias_complete && self.coordinator.relation_projection_complete(&projection)?;
        let document = proposal.document;
        let chunks = proposal.chunks;
        let metadata_page = notion_knowledge_core::backend::Page {
            relation_names: Default::default(),
            id: page.clone(),
            url: document.metadata.url.clone(),
            title: document.metadata.title.clone(),
            last_edited_time: document.metadata.last_edited_time.clone(),
            archived: false,
            properties: document.metadata.properties.clone(),
        };
        let metadata_identity =
            crate::planner::metadata_identity(&metadata_page, &proposal.evidence);
        let checkpoint = PageSyncState::present(
            page.0.clone(),
            document.content_hash.clone(),
            Some(document.metadata.last_edited_time.clone()),
        )
        .map_err(|_| RefreshError::InvalidConfiguration)?;
        // Every existing receipt must pass guarded identity/generation checks.
        // Pending/failed work completes even if another event indexed the
        // snapshot; terminal receipts cannot bypass scope checks via a no-op.
        let has_receipt = self
            .coordinator
            .receipt(operation_id)
            .map_err(RefreshError::from)?
            .is_some();
        // A genuinely unchanged page is a no-op, including durable state.
        // Metadata/links are compared too, so citations are still refreshed.
        if complete
            && expected.is_none()
            && !has_receipt
            && self
                .coordinator
                .metadata_identity(&page.0)
                .map_err(RefreshError::from)?
                .as_ref()
                == Some(&metadata_identity)
            && self
                .coordinator
                .state()
                .page_state(&page.0)
                .map_err(|_| RefreshError::Unavailable)?
                .as_ref()
                == Some(&checkpoint)
        {
            let mut old = previous;
            let mut new = chunks.clone();
            old.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
            new.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
            if old == new {
                let handler = self.clone();
                let mark_changed = mark_changed.clone();
                let expected_projection = projection.clone();
                let expected_aliases = aliases.clone();
                let proposal = relation_proposal;
                let expected_checkpoint = checkpoint.clone();
                let expected_metadata = metadata_identity.clone();
                let expected_chunks = new.clone();
                self.check_owned_snapshot(lease, clock, move |context| async move {
                    let _authority =
                        handler
                            .relations
                            .revalidate(&proposal)
                            .await
                            .map_err(|error| {
                                mark_changed(error.kind);
                                if error.kind == BackendErrorKind::Unauthenticated {
                                    FailureClass::Unavailable
                                } else {
                                    FailureClass::Conflict
                                }
                            })?;
                    context.revalidate().map_err(|_| FailureClass::Conflict)?;
                    let mut current = handler
                        .guard
                        .page_chunks(expected_projection.page_id())
                        .await
                        .map_err(|_| FailureClass::Index)?;
                    current.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
                    if current != expected_chunks
                        || handler
                            .coordinator
                            .state()
                            .page_state(expected_projection.page_id())
                            .map_err(|_| FailureClass::Index)?
                            .as_ref()
                            != Some(&expected_checkpoint)
                        || handler
                            .coordinator
                            .metadata_identity(expected_projection.page_id())
                            .map_err(|_| FailureClass::Index)?
                            .as_ref()
                            != Some(&expected_metadata)
                    {
                        return Err(FailureClass::Conflict);
                    }
                    if !handler
                        .aliases_complete(&expected_aliases)
                        .map_err(|_| FailureClass::Index)?
                        || !handler
                            .coordinator
                            .relation_projection_complete(&expected_projection)
                            .map_err(|_| FailureClass::Index)?
                    {
                        return Err(FailureClass::Conflict);
                    }
                    Ok(())
                })
                .await?;
                return Ok(RefreshResult {
                    outcome: CommitOutcome::AlreadyApplied,
                    diff: ChunkDiffMetrics {
                        skipped: new.len(),
                        ..ChunkDiffMetrics::default()
                    },
                });
            }
        }

        if complete
            && !alias_unfinished
            && self
                .coordinator
                .unfinished_relation_repair_id(operation_id)?
                .is_none()
            && self
                .coordinator
                .receipt(operation_id)?
                .is_some_and(|r| r.status == ReceiptStatus::Applied)
        {
            let handler = self.clone();
            let mark_changed = mark_changed.clone();
            self.check_owned_snapshot(lease, clock, move |context| async move {
                let _authority = handler
                    .relations
                    .revalidate(&relation_proposal)
                    .await
                    .map_err(|error| {
                        mark_changed(error.kind);
                        FailureClass::Conflict
                    })?;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                if !handler
                    .aliases_complete(&aliases)
                    .map_err(|_| FailureClass::Index)?
                    || !handler
                        .coordinator
                        .relation_projection_complete(&projection)
                        .map_err(|_| FailureClass::Index)?
                {
                    return Err(FailureClass::Conflict);
                }
                Ok(())
            })
            .await?;
            return Ok(RefreshResult {
                outcome: CommitOutcome::AlreadyApplied,
                diff: ChunkDiffMetrics {
                    skipped: chunks.len(),
                    ..Default::default()
                },
            });
        }

        // Revision includes canonical metadata and content, not just body text.
        // Unacknowledged durable refresh work can retry a changed proposal.
        // Applied receipts remain immutable in the SQLite store.
        let mut hasher = Sha256::new();
        let serialized =
            serde_json::to_vec(&document).map_err(|_| RefreshError::InvalidConfiguration)?;
        hasher.update(serialized);
        hasher.update(&metadata_identity);
        let revision = format!("{:x}", hasher.finalize());
        let repair_id;
        let operation_id = if self
            .coordinator
            .receipt(operation_id)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied)
            && (!complete
                || alias_unfinished
                || self
                    .coordinator
                    .unfinished_relation_repair_id(operation_id)?
                    .is_some())
        {
            repair_id = if let Some(unfinished) = self
                .coordinator
                .unfinished_relation_repair_id(operation_id)?
            {
                unfinished
            } else if self.alias_sources.is_some() {
                self.coordinator.alias_repair_id(operation_id, &page.0)?
            } else {
                self.coordinator.relation_repair_id(operation_id)?
            };
            &repair_id
        } else {
            operation_id
        };
        let operation = if lifecycle {
            PageOperation::retryable_lifecycle(
                operation_id,
                &revision,
                PageAction::Refresh,
                checkpoint,
            )
        } else {
            PageOperation::retryable_refresh(operation_id, &revision, checkpoint)
        }
        .map_err(RefreshError::from)?
        .with_metadata_identity(metadata_identity)
        .map_err(RefreshError::from)?
        .with_relation_projection(projection)
        .map_err(RefreshError::from)?;
        let operation = self.attach_aliases(operation, aliases)?;
        let expected_operation = operation.clone();
        let prepared = self
            .guard
            .prepare_page(provider, operation, &chunks)
            .await?;
        let diff = prepared.metrics();

        // The commit's callback is invoked by the operation-owned runtime
        // after its cross-process Lance directory guard and SQLite fencing.
        // Reject every changed source field or physical ancestry; no event
        // name, cached root, index URL, or revision hint grants authority.
        let guarded_evidence = proposal.evidence;
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        let expected_lease = lease.clone();
        let handler = self.clone();
        let result = self
            .guard
            .commit_page_with_authority(prepared, lease, clock, move || async move {
                owned_revalidation(
                    async {
                        if let Some(expected) = &expected {
                            handler.check_expected(expected, &guarded_evidence).await?;
                            handler
                                .coordinator
                                .prepare_reconciliation_operation(
                                    &expected_lease,
                                    owned_clock(),
                                    &expected.run,
                                    &expected_operation,
                                    &guarded_evidence,
                                    expected.replan,
                                )
                                .map_err(|_| FailureClass::Conflict)?;
                        }
                        handler
                            .relations
                            .revalidate(&relation_proposal)
                            .await
                            .map_err(|error| {
                                mark_changed(error.kind);
                                source_revalidation_failure(error.kind)
                            })
                    },
                    owned_clock.clone(),
                    expires_at,
                )
                .await
            })
            .await
            .map_err(RefreshError::from)?;
        Ok(RefreshResult {
            outcome: result,
            diff,
        })
    }
    /// A no-op still owns its validation after observer cancellation. Poll its
    /// lease while source I/O is pending so losing ownership releases the guard.
    fn check_owned_snapshot<F, Fut>(
        &self,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        check: F,
    ) -> impl std::future::Future<Output = Result<(), CommitError>> + Send + 'static
    where
        F: FnOnce(crate::commit::CommitContext) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), FailureClass>> + Send + 'static,
    {
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                owned_revalidation(check(context), owned_clock, expires_at).await
            })
    }

    pub(crate) fn projection_mode(
        &self,
    ) -> notion_knowledge_notion::relation_proposal::ProjectionMode {
        self.relations.projection_mode()
    }

    pub(crate) fn alias_configuration_identity(&self) -> Option<String> {
        self.alias_sources.as_ref().map(AliasSources::identity)
    }

    pub(crate) fn is_bound_to(&self, coordinator: &Arc<IndexCommitCoordinator>) -> bool {
        Arc::ptr_eq(&self.coordinator, coordinator)
            && self.guard.is_bound_to(coordinator)
            && crate::planner::production_scope(&self.scope).is_ok_and(|s| {
                self.coordinator
                    .binding()
                    .matches_policy(&self.scope.workspace_id, &s)
            })
    }

    /// Complete local snapshot and exact metadata, independently of extraction.
    async fn check_expected(
        &self,
        expected: &ExpectedReconciliation,
        evidence: &LifecycleEvidence,
    ) -> Result<(), FailureClass> {
        use crate::durable_plan::{DurablePlan, ProposedDifference};
        if DurablePlan::lifecycle_digest(evidence) != expected.page.lifecycle_digest
            || (expected.page.difference == ProposedDifference::Orphan)
                == matches!(evidence.status, LifecycleStatus::Allowed { .. })
        {
            return Err(FailureClass::Conflict);
        }
        let planner = crate::planner::AuthoritativeReconciliationPlanner::new(
            self.notion.clone(),
            self.guard.clone(),
            self.coordinator.clone(),
            self.scope.clone(),
        )
        .map_err(|_| FailureClass::Conflict)?
        .with_projection_mode(self.relations.projection_mode());
        let planner = if let Some(sources) = &self.alias_sources {
            planner.with_alias_sources(sources.clone())
        } else {
            planner
        };
        let local = planner.local_inventory().await.map_err(|e| match e {
            crate::planner::PlanError::Index => FailureClass::Index,
            _ => FailureClass::Conflict,
        })?;
        if DurablePlan::local_digest(local.get(&expected.page.page_id).filter(|p| {
            !p.chunks.is_empty() || p.checkpoint.is_some() || p.metadata_identity.is_some()
        })) != expected.page.local_digest
        {
            return Err(FailureClass::Conflict);
        }
        if let Some(identity) = &expected.page.metadata_identity {
            let page = self
                .notion
                .fetch_page(&expected.page.page_id)
                .await
                .map_err(|error| source_revalidation_failure(error.kind))?;
            if crate::planner::metadata_identity(&page, evidence) != *identity {
                return Err(FailureClass::Conflict);
            }
        }
        self.notion
            .revalidate_lifecycle(evidence, &self.scope)
            .await
            .map_err(|error| source_revalidation_failure(error.kind))
    }

    /// An unchanged canonical page still verifies the full source/relation
    /// snapshot. Missing derived rows are repaired through the normal refresh
    /// pipeline; intact pages commit only their projection and Unchanged receipt.
    pub(crate) async fn apply_unchanged(
        &self,
        expected: ExpectedReconciliation,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        prefetched: Option<PrefetchedPage>,
    ) -> Result<(), RefreshError> {
        let evidence = self
            .notion
            .lifecycle(&PageId(expected.page.page_id.clone()), &self.scope)
            .await
            .map_err(|error| {
                if error.kind == BackendErrorKind::Unauthenticated {
                    RefreshError::Unauthenticated
                } else {
                    RefreshError::Source
                }
            })?;
        let checkpoint = self
            .coordinator
            .state()
            .page_state(&expected.page.page_id)
            .map_err(|_| RefreshError::Index)?
            .ok_or(RefreshError::Conflict)?;
        let root = match &evidence.status {
            LifecycleStatus::Allowed { roots } => {
                PageId(roots.first().ok_or(RefreshError::Conflict)?.clone())
            }
            _ => return Err(RefreshError::Conflict),
        };
        let page_id = PageId(expected.page.page_id.clone());
        let (previous, proposal) = match self
            .matching_prefetch(
                prefetched,
                &page_id,
                &root,
                &expected.page.operation_id,
                false,
            )
            .await?
        {
            Some(prepared) => prepared,
            None => {
                let previous = self.guard.page_chunks(&expected.page.page_id).await?;
                let proposal = self
                    .relations
                    .prepare(&page_id, &root, &previous, self.chunk_config)
                    .await
                    .map_err(|error| {
                        if error.kind == BackendErrorKind::Unauthenticated {
                            RefreshError::Unauthenticated
                        } else {
                            RefreshError::Source
                        }
                    })?;
                (previous, proposal)
            }
        };
        let mut prepared_chunks = proposal.snapshot().chunks.clone();
        let mut previous_chunks = previous;
        prepared_chunks.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
        previous_chunks.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
        if checkpoint.content_hash() != Some(proposal.snapshot().document.content_hash.as_str()) {
            return Err(RefreshError::Conflict);
        }
        if prepared_chunks != previous_chunks {
            // The source revision and canonical body still agree with the
            // checkpoint. An incomplete local subset is disposable cache loss,
            // not a source conflict. Reclassify this operation as Refresh so the
            // existing diff path embeds only missing rows. The exact planned
            // local/source identities remain checked inside the owned commit.
            if !previous_chunks.iter().all(|chunk| {
                prepared_chunks
                    .binary_search_by(|candidate| candidate.chunk_id.cmp(&chunk.chunk_id))
                    .is_ok_and(|index| prepared_chunks[index] == *chunk)
            }) {
                return Err(RefreshError::Conflict);
            }
            let mut repair = expected;
            repair.replan = true;
            let page = PageId(repair.page.page_id.clone());
            let operation = repair.page.operation_id.clone();
            // Keep the full refresh future out of the unchanged fast path's
            // stack frame, including callers that never need this fallback.
            return Box::pin(self.apply_lifecycle_expected(
                &page,
                &operation,
                provider,
                lease,
                clock,
                Some(repair),
                None,
            ))
            .await
            .map(|_| ());
        }
        let projection = RelationProjection::new(
            self.coordinator.binding(),
            &expected.page.page_id,
            false,
            proposal.edges().to_vec(),
        )?
        .with_links(proposal.links().to_vec())?;
        let operation = PageOperation::retryable_lifecycle(
            &expected.page.operation_id,
            &expected.page.lifecycle_digest,
            PageAction::Unchanged,
            checkpoint,
        )
        .map_err(RefreshError::from)?
        .with_metadata_identity(
            expected
                .page
                .metadata_identity
                .clone()
                .ok_or(RefreshError::Conflict)?,
        )
        .map_err(RefreshError::from)?
        .with_relation_projection(projection)?;
        let aliases = self
            .alias_sources
            .as_ref()
            .map(|sources| {
                AliasProjection::from_indexed(
                    self.coordinator.binding(),
                    &proposal.snapshot().document.metadata,
                    sources,
                )
                .map(AliasProjection::with_owned_root_cleanup)
            })
            .transpose()?;
        let operation = self.attach_aliases(operation, aliases)?;
        let handler = self.clone();
        let prepared = operation.clone();
        let owned_lease = lease.clone();
        let owned_clock = clock.clone();
        self.coordinator
            .submit_with_authority(operation, lease, clock, move |context| async move {
                owned_revalidation(
                    async {
                        handler.check_expected(&expected, &evidence).await?;
                        context.revalidate().map_err(|_| FailureClass::Conflict)?;
                        handler
                            .coordinator
                            .prepare_reconciliation_operation(
                                &owned_lease,
                                owned_clock(),
                                &expected.run,
                                &prepared,
                                &evidence,
                                expected.replan,
                            )
                            .map_err(|_| FailureClass::Conflict)?;
                        handler
                            .relations
                            .revalidate(&proposal)
                            .await
                            .map_err(|_| FailureClass::Conflict)
                    },
                    owned_clock.clone(),
                    owned_lease.expires_at,
                )
                .await
            })
            .await
            .map_err(RefreshError::from)?;
        Ok(())
    }

    /// Recover historical Applied work without reinterpreting it using new source.
    /// Ownership, lease and source accessibility are still checked under the guard.
    pub(crate) async fn recover_reconciliation(
        &self,
        provider: &dyn EmbeddingProvider,
        run: String,
        page: String,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), RefreshError> {
        let operation = self
            .coordinator
            .reconciliation_plan(&run)
            .map_err(|_| RefreshError::Conflict)?
            .pages
            .get(&page)
            .ok_or(RefreshError::Conflict)?
            .operation_id
            .clone();
        self.apply_lifecycle(
            &PageId(page.clone()),
            &operation,
            provider,
            lease.clone(),
            clock.clone(),
        )
        .await?;
        let evidence = self
            .notion
            .lifecycle(&PageId(page.clone()), &self.scope)
            .await
            .map_err(|error| {
                if error.kind == BackendErrorKind::Unauthenticated {
                    RefreshError::Unauthenticated
                } else {
                    RefreshError::Source
                }
            })?;
        let proposal = if let LifecycleStatus::Allowed { roots } = &evidence.status {
            let previous = self.guard.page_chunks(&page).await?;
            Some(
                self.relations
                    .prepare(
                        &PageId(page.clone()),
                        &PageId(roots.first().ok_or(RefreshError::Conflict)?.clone()),
                        &previous,
                        self.chunk_config,
                    )
                    .await
                    .map_err(|error| {
                        if error.kind == BackendErrorKind::Unauthenticated {
                            RefreshError::Unauthenticated
                        } else {
                            RefreshError::Source
                        }
                    })?,
            )
        } else {
            None
        };
        let handler = self.clone();
        let owned_lease = lease.clone();
        let owned_clock = clock.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                owned_revalidation(
                    async {
                        let _authority = if let Some(proposal) = &proposal {
                            let authority = handler
                                .relations
                                .revalidate(proposal)
                                .await
                                .map_err(|_| FailureClass::Conflict)?;
                            let aliases = handler
                                .alias_sources
                                .as_ref()
                                .map(|sources| {
                                    AliasProjection::from_indexed(
                                        handler.coordinator.binding(),
                                        &proposal.snapshot().document.metadata,
                                        sources,
                                    )
                                    .map(AliasProjection::with_owned_root_cleanup)
                                })
                                .transpose()
                                .map_err(|_| FailureClass::Index)?;
                            if !handler
                                .aliases_complete(&aliases)
                                .map_err(|_| FailureClass::Index)?
                            {
                                return Err(FailureClass::Conflict);
                            }
                            let exact = RelationProjection::new(
                                handler.coordinator.binding(),
                                &page,
                                false,
                                proposal.edges().to_vec(),
                            )
                            .and_then(|projection| projection.with_links(proposal.links().to_vec()))
                            .map_err(|_| FailureClass::Index)?;
                            if !handler
                                .coordinator
                                .relation_projection_complete(&exact)
                                .map_err(|_| FailureClass::Index)?
                            {
                                return Err(FailureClass::Conflict);
                            }
                            authority
                        } else {
                            handler
                                .relations
                                .revalidate_removal(&evidence)
                                .await
                                .map_err(|_| FailureClass::Conflict)?
                        };
                        context.revalidate().map_err(|_| FailureClass::Conflict)?;
                        if !handler
                            .current_aliases_complete(&page)
                            .map_err(|_| FailureClass::Index)?
                            || !handler
                                .coordinator
                                .current_body_links_complete(&page)
                                .map_err(|_| FailureClass::Index)?
                        {
                            return Err(FailureClass::Index);
                        }
                        handler
                            .coordinator
                            .acknowledge_reconciliation_receipt(
                                &owned_lease,
                                owned_clock(),
                                &run,
                                &page,
                            )
                            .map_err(|_| FailureClass::Conflict)?;
                        Ok(())
                    },
                    owned_clock.clone(),
                    owned_lease.expires_at,
                )
                .await
            })
            .await
            .map_err(RefreshError::from)
    }
}

/// Cancel source revalidation inside the operation-owned commit runtime when
/// the caller loses ownership. Dropping its observer alone cannot release that
/// runtime's index directory guard. All messages remain sanitized enum classes.
async fn owned_revalidation<Authority>(
    source: impl std::future::Future<Output = Result<Authority, FailureClass>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    expires_at: i64,
) -> Result<Authority, FailureClass> {
    tokio::pin!(source);
    loop {
        if clock() >= expires_at {
            return Err(FailureClass::Conflict);
        }
        tokio::select! {
            result = &mut source => return result,
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {},
        }
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;
    use notion_knowledge_core::embedding::EmbeddingError;

    #[test]
    fn only_deterministic_embedding_input_limits_are_page_failures() {
        assert_eq!(
            RefreshError::from(ChunkTableError::Embedding(EmbeddingError::InputLimit)),
            RefreshError::EmbeddingInputLimit
        );
        for error in [
            EmbeddingError::InvalidInput,
            EmbeddingError::InvalidOutput,
            EmbeddingError::Unavailable,
            EmbeddingError::ResourceExhausted,
            EmbeddingError::UnsupportedModel,
            EmbeddingError::InvalidMetadata,
            EmbeddingError::IncompatibleIndex,
        ] {
            assert_eq!(
                RefreshError::from(ChunkTableError::Embedding(error)),
                RefreshError::Index
            );
        }
    }
}
