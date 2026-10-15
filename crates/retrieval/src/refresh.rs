//! Phase 3 one-page, authoritative Notion -> durable SQLite/LanceDB refresh.
#![cfg(all(unix, feature = "local-lancedb"))]
//!
//! The webhook is only a hint for which ID to inspect. The actual Notion
//! credential/scope and local index binding come from trusted operator config.
//! No receipts are acknowledged until the guarded local commit completes.
use std::sync::Arc;

use notion_knowledge_core::{
    backend::{BackendErrorKind, PageId},
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    indexed::IndexedDocument,
    lifecycle::{LifecycleEvidence, LifecycleScope, LifecycleStatus, PageLifecycle},
    reconciliation::{FailureClass, Lease, ReconciliationScope},
    root_scope::RootScopeGate,
    sync_state::{PageSyncState, SyncStateStore},
};
use notion_knowledge_notion::NotionClient;
use sha2::{Digest, Sha256};

use crate::{
    chunks::{ChunkDiffMetrics, ChunkTableError, GuardedChunkTable},
    commit::{
        CommitBinding, CommitError, CommitOutcome, IndexCommitCoordinator, PageAction,
        PageOperation,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshError {
    InvalidConfiguration,
    OutOfScope,
    Source,
    /// A sanitized source contract/authentication failure; automatic retry cannot repair it.
    PermanentSource,
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
            CommitError::BindingMismatch => Self::Conflict,
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

#[derive(Clone)]
pub struct AuthoritativePageRefresh {
    notion: Arc<NotionClient>,
    guard: GuardedChunkTable,
    coordinator: Arc<IndexCommitCoordinator>,
    scope: LifecycleScope,
    chunk_config: ChunkConfig,
}
impl AuthoritativePageRefresh {
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
        if binding.workspace() != scope.workspace_id
            || binding.generation() != scope.generation.to_string()
            || binding.scope() != journal_scope.identity()
            || scope.roots.is_empty()
            || chunk_config.target_chars == 0
            || chunk_config.overlap_chars >= chunk_config.target_chars
        {
            return Err(RefreshError::InvalidConfiguration);
        }
        RootScopeGate::new(notion.clone(), scope.clone())
            .map_err(|_| RefreshError::InvalidConfiguration)?;
        Ok(Self {
            notion,
            guard,
            coordinator,
            scope,
            chunk_config,
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
        self.apply_lifecycle_expected(page, operation_id, provider, lease, clock, None)
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
    ) -> Result<RefreshResult, RefreshError> {
        let evidence =
            self.notion
                .lifecycle(page, &self.scope)
                .await
                .map_err(|error| match error.kind {
                    BackendErrorKind::Conflict => RefreshError::Conflict,
                    BackendErrorKind::Unauthenticated
                    | BackendErrorKind::InvalidInput
                    | BackendErrorKind::UnsupportedContent => RefreshError::PermanentSource,
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
            return self
                .refresh_page_inner(
                    page,
                    &PageId(root.clone()),
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
        let operation = PageOperation::retryable_lifecycle(
            operation_id,
            &revision,
            PageAction::Delete,
            checkpoint,
        )
        .map_err(RefreshError::from)?;
        let expected_operation = operation.clone();
        let prepared = self.guard.prepare_page(provider, operation, &[]).await?;
        let diff = prepared.metrics();
        let notion = self.notion.clone();
        let scope = self.scope.clone();
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        let expected_lease = lease.clone();
        let handler = self.clone();
        let outcome = self
            .guard
            .commit_page(prepared, lease, clock, move || async move {
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
                        notion
                            .revalidate_lifecycle(&evidence, &scope)
                            .await
                            .map_err(|error| {
                                if error.kind == BackendErrorKind::Conflict {
                                    FailureClass::Conflict
                                } else {
                                    FailureClass::Source
                                }
                            })
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
        if !self.scope.roots.contains(root) || operation_id.is_empty() {
            return Err(RefreshError::InvalidConfiguration);
        }

        // This is trusted local indexed state, NOT a physical scope decision.
        // The notion adapter validates its previous-page provenance/IDs/hash
        // after live Notion ancestry authorization.
        let previous = self.guard.page_chunks(&page.0).await?;
        let proposal = self
            .notion
            .prepare_scoped_page(page, root, &self.scope, &previous, self.chunk_config)
            .await
            .map_err(|e| match e.kind {
                BackendErrorKind::PermissionDenied => RefreshError::OutOfScope,
                BackendErrorKind::Conflict => RefreshError::Conflict,
                BackendErrorKind::InvalidInput
                | BackendErrorKind::UnsupportedContent
                | BackendErrorKind::Unauthenticated => RefreshError::PermanentSource,
                _ => RefreshError::Source,
            })?;

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
        if expected.is_none()
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
                return Ok(RefreshResult {
                    outcome: CommitOutcome::AlreadyApplied,
                    diff: ChunkDiffMetrics {
                        skipped: new.len(),
                        ..ChunkDiffMetrics::default()
                    },
                });
            }
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
        .map_err(RefreshError::from)?;
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
        let notion = self.notion.clone();
        let scope = self.scope.clone();
        let page = page.clone();
        let root = root.clone();
        let guarded_document = document.clone();
        let guarded_evidence = proposal.evidence;
        let owned_clock = clock.clone();
        let expires_at = lease.expires_at;
        let expected_lease = lease.clone();
        let handler = self.clone();
        let result = self
            .guard
            .commit_page(prepared, lease, clock, move || async move {
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
                        verify_source(
                            &notion,
                            &page,
                            &root,
                            &scope,
                            &guarded_document,
                            &guarded_evidence,
                        )
                        .await
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
    pub(crate) fn is_bound_to(&self, coordinator: &Arc<IndexCommitCoordinator>) -> bool {
        Arc::ptr_eq(&self.coordinator, coordinator)
            && self.guard.is_bound_to(coordinator)
            && self.coordinator.binding().epoch() == self.scope.generation
            && crate::planner::production_scope(&self.scope)
                .is_ok_and(|s| s.identity() == self.coordinator.binding().scope())
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
        .map_err(|_| FailureClass::Conflict)?;
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
                .map_err(|_| FailureClass::Source)?;
            if crate::planner::metadata_identity(&page, evidence) != *identity {
                return Err(FailureClass::Conflict);
            }
        }
        self.notion
            .revalidate_lifecycle(evidence, &self.scope)
            .await
            .map_err(|error| {
                if error.kind == BackendErrorKind::Conflict {
                    FailureClass::Conflict
                } else {
                    FailureClass::Source
                }
            })
    }

    /// An unchanged plan validates metadata/lifecycle/local state without reading
    /// Markdown or running embeddings. The actual no-op receipt is durable.
    pub(crate) async fn apply_unchanged(
        &self,
        expected: ExpectedReconciliation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), RefreshError> {
        let evidence = self
            .notion
            .lifecycle(&PageId(expected.page.page_id.clone()), &self.scope)
            .await
            .map_err(|_| RefreshError::Source)?;
        let checkpoint = self
            .coordinator
            .state()
            .page_state(&expected.page.page_id)
            .map_err(|_| RefreshError::Index)?
            .ok_or(RefreshError::Conflict)?;
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
        .map_err(RefreshError::from)?;
        let handler = self.clone();
        let prepared = operation.clone();
        let owned_lease = lease.clone();
        let owned_clock = clock.clone();
        self.coordinator
            .submit(operation, lease, clock, move |context| async move {
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
        run: String,
        page: String,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), RefreshError> {
        let handler = self.clone();
        let owned_lease = lease.clone();
        let owned_clock = clock.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                owned_revalidation(
                    async {
                        handler
                            .notion
                            .lifecycle(&PageId(page.clone()), &handler.scope)
                            .await
                            .map_err(|_| FailureClass::Source)?;
                        context.revalidate().map_err(|_| FailureClass::Conflict)?;
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

/// Recheck the *entire* source document inside the real SQLite/Lance commit
/// guard. Timestamp equality alone is insufficient for property/URL changes.
async fn verify_source(
    notion: &NotionClient,
    page: &PageId,
    root: &PageId,
    scope: &LifecycleScope,
    expected: &IndexedDocument,
    evidence: &LifecycleEvidence,
) -> Result<(), FailureClass> {
    if !matches!(&evidence.status, LifecycleStatus::Allowed { roots } if roots.contains(&root.0)) {
        return Err(FailureClass::Source);
    }
    notion
        .revalidate_lifecycle(evidence, scope)
        .await
        .map_err(|error| {
            if error.kind == BackendErrorKind::Conflict {
                FailureClass::Conflict
            } else {
                FailureClass::Source
            }
        })?;
    let fresh = notion
        .read_content(&page.0)
        .await
        .map_err(|_| FailureClass::Source)?;
    if fresh.page.archived
        || fresh.page.id != *page
        || fresh.page.url != expected.metadata.url
        || fresh.page.title != expected.metadata.title
        || fresh.page.last_edited_time != expected.metadata.last_edited_time
        || fresh.page.properties != expected.metadata.properties
        || fresh.markdown != expected.text
    {
        return Err(FailureClass::Conflict);
    }
    notion
        .revalidate_lifecycle(evidence, scope)
        .await
        .map_err(|error| {
            if error.kind == BackendErrorKind::Conflict {
                FailureClass::Conflict
            } else {
                FailureClass::Source
            }
        })
}

/// Cancel source revalidation inside the operation-owned commit runtime when
/// the caller loses ownership. Dropping its observer alone cannot release that
/// runtime's index directory guard. All messages remain sanitized enum classes.
async fn owned_revalidation(
    source: impl std::future::Future<Output = Result<(), FailureClass>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    expires_at: i64,
) -> Result<(), FailureClass> {
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
