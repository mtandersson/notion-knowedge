//! Phase 3 one-page, authoritative Notion -> durable SQLite/LanceDB refresh.
#![cfg(all(unix, feature = "local-lancedb"))]
//!
//! The webhook is only a hint for which ID to inspect. The actual Notion
//! credential/scope and local index binding come from trusted operator config.
//! No receipts are acknowledged until the guarded local commit completes.
use std::sync::Arc;

use notion_knowledge_core::{
    aliases::AliasSources,
    backend::{BackendErrorKind, PageId},
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    inventory::InventoryParent,
    lifecycle::LifecycleScope,
    reconciliation::{FailureClass, Lease, ReconciliationScope},
    sync_state::{PageSyncState, SyncStateStore},
};
use notion_knowledge_notion::{
    NotionClient, relation_proposal::RelationProposalAdapter, walk::WalkedPage,
};
use sha2::{Digest, Sha256};

use crate::{
    chunks::{ChunkDiffMetrics, ChunkTableError, GuardedChunkTable},
    commit::{
        AliasProjection, CommitBinding, CommitError, CommitOutcome, IndexCommitCoordinator,
        PageAction, PageOperation, ReceiptStatus, RelationProjection,
    },
};

fn source_failure(kind: BackendErrorKind) -> RefreshError {
    match kind {
        BackendErrorKind::PermissionDenied => RefreshError::OutOfScope,
        BackendErrorKind::Conflict => RefreshError::Conflict,
        BackendErrorKind::Unauthenticated => RefreshError::Unauthenticated,
        BackendErrorKind::InvalidInput | BackendErrorKind::UnsupportedContent => {
            RefreshError::PermanentSource
        }
        _ => RefreshError::Source,
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
}
impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "authoritative page refresh failed: {self:?}")
    }
}
impl std::error::Error for RefreshError {}

impl From<ChunkTableError> for RefreshError {
    fn from(_: ChunkTableError) -> Self {
        Self::Index
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
    /// Relation and link targets the inventory could not place when this page
    /// was prepared. A later walk step may place them.
    pub fn unresolved_targets(&self) -> Vec<String> {
        self.proposal
            .targets()
            .iter()
            .filter(|(_, outcome)| {
                matches!(
                    outcome,
                    Err(notion_knowledge_core::root_scope::ScopeError::Unverifiable)
                )
            })
            .map(|(target, _)| target.clone())
            .collect()
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
    #[cfg(any(test, feature = "test-fixtures"))]
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
        let relations = RelationProposalAdapter::new((*notion).clone(), scope.clone())
            .map_err(|_| RefreshError::InvalidConfiguration)?
            .with_inventory(coordinator.state().clone());
        Ok(Self {
            relations,
            alias_sources: None,
            #[cfg(any(test, feature = "test-fixtures"))]
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

    /// Fixture seam: walk the source and sync one page it reached, as a
    /// single-page sync does. Never part of production composition.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn refresh_page(
        &self,
        page: &PageId,
        root: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let (send, mut receive) = tokio::sync::mpsc::channel(64);
        let walk = async move {
            self.notion
                .walk_tree_with_search(&self.scope, Some(&send))
                .await
        };
        let collect = async {
            let mut found = None;
            while let Some(walked) = receive.recv().await {
                if walked.entry.page_id == page.0 {
                    found = Some(walked);
                }
            }
            found
        };
        let (walk, found) = tokio::join!(walk, collect);
        walk.map_err(|e| source_failure(e.kind))?;
        let walked = found.ok_or(RefreshError::OutOfScope)?;
        if walked.skip.is_some() || walked.entry.root_id != root.0 {
            return Err(RefreshError::OutOfScope);
        }
        let prepared = self.prepare_walked(&walked, operation_id).await?;
        self.refresh_prefetched(prepared, provider, lease, clock)
            .await
    }

    /// Prepare a page the walk reached from its single read. No request is
    /// made for the page itself; only relation targets are authorized. The
    /// receipt authority and this page's indexed chunks are local state.
    pub async fn prepare_walked(
        &self,
        walked: &WalkedPage,
        operation_id: &str,
    ) -> Result<PrefetchedPage, RefreshError> {
        crate::test_hooks::preparation_pause(self.coordinator.index_directory()).await;
        let page = PageId(walked.entry.page_id.clone());
        let root = PageId(walked.entry.root_id.clone());
        if !self.scope.roots.contains(&root) || operation_id.is_empty() {
            return Err(RefreshError::InvalidConfiguration);
        }
        self.coordinator
            .validate_page_receipt_authority(operation_id, &page.0)?;
        let previous = self.guard.page_chunks(&page.0).await?;
        let proposal = self
            .relations
            .prepare_walked(walked, &previous, self.chunk_config)
            .await
            .map_err(|e| source_failure(e.kind))?;
        Ok(PrefetchedPage {
            page,
            root,
            operation: operation_id.to_owned(),
            previous,
            proposal,
        })
    }

    /// One read of a page whose parent the caller holds (metadata and Markdown),
    /// then local preparation.
    async fn prepare_source_with(
        &self,
        page: &PageId,
        parent: InventoryParent,
        root: &PageId,
        operation_id: &str,
    ) -> Result<PrefetchedPage, RefreshError> {
        if !self.scope.roots.contains(root) || operation_id.is_empty() {
            return Err(RefreshError::InvalidConfiguration);
        }
        self.coordinator
            .validate_page_receipt_authority(operation_id, &page.0)?;
        // Trusted local indexed state, NOT a physical scope decision.
        let previous = self.guard.page_chunks(&page.0).await?;
        let proposal = self
            .relations
            .prepare_read(page, parent, root, &previous, self.chunk_config)
            .await
            .map_err(|e| source_failure(e.kind))?;
        Ok(PrefetchedPage {
            page: page.clone(),
            root: root.clone(),
            operation: operation_id.to_owned(),
            previous,
            proposal,
        })
    }

    /// Whether the inventory now places any of these targets in scope.
    pub async fn any_resolvable(&self, targets: &[String]) -> Result<bool, RefreshError> {
        self.relations
            .any_resolvable(targets)
            .await
            .map_err(|e| source_failure(e.kind))
    }

    /// Resolve a page's relation and link targets after the inventory that
    /// places them was recorded. One read of the page, then the normal commit:
    /// unchanged chunks are neither embedded nor rewritten, only the page's
    /// relation projection changes.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_relation_refresh(
        &self,
        page: &PageId,
        parent: InventoryParent,
        root: &PageId,
        operation_id: &str,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let PrefetchedPage {
            previous, proposal, ..
        } = Box::pin(self.prepare_source_with(page, parent, root, operation_id)).await?;
        Box::pin(self.commit_text(page, operation_id, previous, proposal, lease, clock)).await
    }

    /// Explicit two-phase refresh for local callers that need both outputs.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_relations(
        &self,
        page: &PageId,
        parent: InventoryParent,
        root: &PageId,
        operation_id: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let result = self
            .publish_relation_refresh(
                page,
                parent,
                root,
                operation_id,
                lease.clone(),
                clock.clone(),
            )
            .await?;
        self.guard
            .retry_semantic(&page.0, provider, lease, clock)
            .await?;
        Ok(result)
    }

    /// The verified canonical source can be published before embedding. The
    /// source watermark was captured before discovery started.
    pub async fn publish_prefetched_document(
        &self,
        prefetched: &PrefetchedPage,
        watermark: i64,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), RefreshError> {
        let snapshot = prefetched.proposal.snapshot();
        crate::page_documents::PageDocuments::new(self.coordinator.clone())
            .publish(
                snapshot.document.clone(),
                &snapshot.parent,
                watermark,
                lease,
                clock,
            )
            .await
            .map_err(Into::into)
    }

    /// Publish source text prepared by [`Self::prepare_walked`] without inference.
    /// The guarded mutation plan (pinned index version, receipt and projection
    /// state) is built here from current local state, never from prefetch time.
    /// When this page's own indexed chunks changed while it waited, its chunk
    /// identities are prepared again from the same read, without a request.
    pub async fn publish_prefetched(
        &self,
        prefetched: PrefetchedPage,
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
        self.coordinator
            .validate_page_receipt_authority(&operation, &page.0)?;
        let current = self.guard.page_chunks(&page.0).await?;
        let proposal = if current == previous {
            proposal
        } else {
            let snapshot = proposal.snapshot();
            self.relations
                .prepare_content(
                    snapshot.source().clone(),
                    snapshot.parent.clone(),
                    &root,
                    &current,
                    self.chunk_config,
                )
                .await
                .map_err(|e| source_failure(e.kind))?
        };
        Box::pin(self.commit_text(&page, &operation, current, proposal, lease, clock)).await
    }

    /// Explicit two-phase refresh for local callers that need both outputs.
    pub async fn refresh_prefetched(
        &self,
        prefetched: PrefetchedPage,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        let page = prefetched.page.clone();
        let result = self
            .publish_prefetched(prefetched, lease.clone(), clock.clone())
            .await?;
        self.guard
            .retry_semantic(&page.0, provider, lease, clock)
            .await?;
        Ok(result)
    }

    /// Remove a page a complete walk no longer reached (moved out, trashed,
    /// revoked or deleted). The walk is the evidence: nothing is read again.
    pub async fn remove_unreached(
        &self,
        page: &PageId,
        operation_id: &str,
        _provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
        self.coordinator
            .validate_page_receipt_authority(operation_id, &page.0)?;
        let checkpoint = PageSyncState::tombstone(page.0.clone(), None)
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        let revision = format!(
            "{:x}",
            Sha256::digest(format!("walk-removal:{operation_id}:{}", page.0).as_bytes())
        );
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
        let operation = PageOperation::retryable_lifecycle(
            operation_id,
            &revision,
            PageAction::Delete,
            checkpoint,
        )
        .map_err(RefreshError::from)?
        .with_relation_projection(projection)?;
        let operation = self.attach_aliases(operation, aliases)?;
        let previous = self.guard.page_chunks(&page.0).await?;
        let handler = self.clone();
        let (owned_clock, expires_at) = (clock.clone(), lease.expires_at);
        let (outcome, diff) = self
            .guard
            .commit_text_with_authority(
                operation,
                previous,
                Vec::new(),
                lease,
                clock,
                move || async move {
                    owned_revalidation(
                        handler.guard.index_directory().to_path_buf(),
                        async {
                            handler
                                .relations
                                .removal_authority(&handler.scope)
                                .await
                                .map_err(|_| FailureClass::Conflict)
                        },
                        owned_clock,
                        expires_at,
                    )
                    .await
                },
            )
            .await
            .map_err(RefreshError::from)?;
        Ok(RefreshResult { outcome, diff })
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_text(
        &self,
        page: &PageId,
        operation_id: &str,
        previous: Vec<notion_knowledge_core::indexed::IndexedChunk>,
        relation_proposal: notion_knowledge_notion::relation_proposal::RelationProposal,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<RefreshResult, RefreshError> {
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
            self.check_owned_snapshot(lease, clock, move |context| async move {
                let _authority = handler
                    .relations
                    .revalidate(&relation_proposal)
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
                diff: ChunkDiffMetrics {
                    skipped: proposal.chunks.len(),
                    ..Default::default()
                },
            });
        }

        let document = proposal.document;
        let mut chunks = proposal.chunks;
        let metadata_page = notion_knowledge_core::backend::Page {
            relation_names: Default::default(),
            id: page.clone(),
            url: document.metadata.url.clone(),
            title: document.metadata.title.clone(),
            last_edited_time: document.metadata.last_edited_time.clone(),
            archived: false,
            properties: document.metadata.properties.clone(),
        };
        let metadata_identity = crate::planner::metadata_identity(&metadata_page, &proposal.parent);
        let checkpoint = PageSyncState::present(
            page.0.clone(),
            document.content_hash.clone(),
            Some(document.metadata.last_edited_time.clone()),
        )
        .map_err(|_| RefreshError::InvalidConfiguration)?;
        // Revision includes canonical metadata and content, not just body text.
        // Unacknowledged durable refresh work can retry a changed proposal.
        // Applied receipts remain immutable in the SQLite store.
        let mut hasher = Sha256::new();
        let serialized =
            serde_json::to_vec(&document).map_err(|_| RefreshError::InvalidConfiguration)?;
        hasher.update(serialized);
        hasher.update(&metadata_identity);
        let revision = format!("{:x}", hasher.finalize());
        let same_revision = self
            .coordinator
            .state()
            .page_state(&page.0)
            .map_err(|_| RefreshError::Unavailable)?
            .as_ref()
            == Some(&checkpoint)
            && self.coordinator.metadata_identity(&page.0)?.as_ref() == Some(&metadata_identity)
            && (!previous.is_empty() || chunks.is_empty())
            && self
                .guard
                .text_matches_revision(&page.0, &revision, &previous)?;
        // The compatible v1 copy may contain previously partitioned HTML
        // tables. Preserve parents, split boundaries and citations while
        // the canonical source revision remains identical.
        if same_revision {
            chunks.clone_from(&previous);
        }
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
        if complete && !has_receipt && same_revision {
            let handler = self.clone();
            let expected_projection = projection.clone();
            let expected_aliases = aliases.clone();
            let proposal = relation_proposal;
            let expected_checkpoint = checkpoint.clone();
            let expected_metadata = metadata_identity.clone();
            let unchanged_chunks = previous.len();
            let expected_chunks = previous;
            let owned_clock = clock.clone();
            self.check_owned_snapshot(lease, clock, move |context| async move {
                let _authority = handler
                    .relations
                    .revalidate(&proposal)
                    .await
                    .map_err(|_| FailureClass::Conflict)?;
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
                handler
                    .guard
                    .repair_semantic_if_missing(
                        &context,
                        expected_projection.page_id(),
                        &revision,
                        &expected_chunks,
                        owned_clock(),
                    )
                    .await?;
                Ok(())
            })
            .await?;
            return Ok(RefreshResult {
                outcome: CommitOutcome::AlreadyApplied,
                diff: ChunkDiffMetrics {
                    skipped: unchanged_chunks,
                    ..ChunkDiffMetrics::default()
                },
            });
        }

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
        let operation = PageOperation::retryable_refresh(operation_id, &revision, checkpoint)
            .map_err(RefreshError::from)?
            .with_metadata_identity(metadata_identity)
            .map_err(RefreshError::from)?
            .with_relation_projection(projection)
            .map_err(RefreshError::from)?;
        let operation = self.attach_aliases(operation, aliases)?;

        // The commit's callback runs inside the operation-owned runtime, after
        // its cross-process Lance directory guard and SQLite fencing. It makes
        // no source request: it holds the current policy through acknowledgment.
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        let handler = self.clone();
        let (outcome, diff) = self
            .guard
            .commit_text_with_authority(
                operation,
                previous,
                chunks,
                lease,
                clock,
                move || async move {
                    owned_revalidation(
                        handler.guard.index_directory().to_path_buf(),
                        async {
                            handler
                                .relations
                                .revalidate(&relation_proposal)
                                .await
                                .map_err(|_| FailureClass::Conflict)
                        },
                        owned_clock.clone(),
                        expires_at,
                    )
                    .await
                },
            )
            .await
            .map_err(RefreshError::from)?;
        Ok(RefreshResult { outcome, diff })
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
        let index = self.guard.index_directory().to_path_buf();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                owned_revalidation(index, check(context), owned_clock, expires_at).await
            })
    }
}

/// Cancel source revalidation inside the operation-owned commit runtime when
/// the caller loses ownership. Dropping its observer alone cannot release that
/// runtime's index directory guard. All messages remain sanitized enum classes.
async fn owned_revalidation<Authority>(
    index: std::path::PathBuf,
    source: impl std::future::Future<Output = Result<Authority, FailureClass>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    expires_at: i64,
) -> Result<Authority, FailureClass> {
    #[cfg(any(test, feature = "test-fixtures"))]
    let source = async move {
        crate::test_hooks::commit_pause(&index).await;
        source.await
    };
    #[cfg(not(any(test, feature = "test-fixtures")))]
    drop(index);
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
