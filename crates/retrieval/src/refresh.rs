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
    lifecycle::LifecycleScope,
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
            CommitError::InvalidInput | CommitError::BindingMismatch => Self::InvalidConfiguration,
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
                BackendErrorKind::InvalidInput => RefreshError::InvalidConfiguration,
                _ => RefreshError::Source,
            })?;

        let document = proposal.document;
        let chunks = proposal.chunks;
        let checkpoint = PageSyncState::present(
            page.0.clone(),
            document.content_hash.clone(),
            Some(document.metadata.last_edited_time.clone()),
        )
        .map_err(|_| RefreshError::InvalidConfiguration)?;
        // A genuinely unchanged page is a no-op, including durable state.
        // Metadata/links are compared too, so citations are still refreshed.
        if self
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
        // A reused event ID for a changed proposal conflicts in the SQLite
        // receipt store instead of overwriting an earlier acknowledged effect.
        let mut hasher = Sha256::new();
        let serialized =
            serde_json::to_vec(&document).map_err(|_| RefreshError::InvalidConfiguration)?;
        hasher.update(serialized);
        let revision = format!("{:x}", hasher.finalize());
        let operation =
            PageOperation::new(operation_id, &revision, PageAction::Refresh, checkpoint)
                .map_err(RefreshError::from)?;
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
        let result = self
            .guard
            .commit_page(prepared, lease, clock, move || async move {
                verify_source(&notion, &page, &root, &scope, &guarded_document).await
            })
            .await
            .map_err(RefreshError::from)?;
        Ok(RefreshResult {
            outcome: result,
            diff,
        })
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
) -> Result<(), FailureClass> {
    let gate = RootScopeGate::new(Arc::new(notion.clone()), scope.clone())
        .map_err(|_| FailureClass::Conflict)?;
    let permit = gate
        .authorize(page)
        .await
        .map_err(|_| FailureClass::Source)?;
    if !permit.belongs_to_any(std::slice::from_ref(&root.0)) {
        return Err(FailureClass::Source);
    }
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
    gate.revalidate(&permit, scope)
        .await
        .map_err(|_| FailureClass::Conflict)
}
