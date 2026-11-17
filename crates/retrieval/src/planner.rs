//! Scope identity and the typed reconciliation plan the durable journal stores.
use crate::commit::CommitBinding;
use notion_knowledge_core::{
    backend::{BackendErrorKind, Page},
    indexed::IndexedChunk,
    inventory::InventoryParent,
    lifecycle::{LifecycleEvidence, LifecycleScope},
    reconciliation::ReconciliationScope,
    sync_state::PageSyncState,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Shared authority identity for webhook and reconciliation consumers.
pub fn production_scope(scope: &LifecycleScope) -> Result<ReconciliationScope, PlanError> {
    let exclusions = scope
        .exclusions
        .page_ids
        .iter()
        .map(|id| format!("page:{id}"))
        .chain(
            scope
                .exclusions
                .descendants_of
                .iter()
                .map(|id| format!("descendant:{id}")),
        )
        .chain(
            scope
                .exclusions
                .source_types
                .iter()
                .map(|kind| format!("type:{kind:?}")),
        )
        .collect();
    ReconciliationScope::new(
        scope.roots.iter().map(|r| r.0.clone()).collect(),
        exclusions,
        "webhook-v1",
        &scope.generation.to_string(),
    )
    .map_err(|_| PlanError::Ownership)
}
/// v2 includes exact citation/property metadata, `last_edited_time` and the
/// walk's parent. Unknown legacy identities conservatively require refresh.
pub(crate) fn metadata_identity(page: &Page, parent: &InventoryParent) -> String {
    let properties = serde_json::to_string(&page.properties).expect("typed properties serialize");
    format!(
        "{:x}",
        Sha256::digest(
            format!(
                "metadata-v2:{:?}:{:?}:{:?}:{:?}:{}:{properties}:{parent:?}",
                page.id, page.url, page.title, page.last_edited_time, page.archived
            )
            .as_bytes()
        )
    )
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    Ownership,
    Index,
    Source,
    Permission,
    MissingSource,
    Unauthenticated,
    UnsupportedSource,
    Conflict,
    IncompleteTraversal,
    /// One run failure with its sanitized source classification.
    Traversal(BackendErrorKind),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Difference {
    Missing,
    Stale,
    Restored,
    Unchanged,
    Orphan,
}
#[derive(Debug, Clone, PartialEq)]
pub struct LocalPage {
    pub chunks: Vec<IndexedChunk>,
    pub checkpoint: Option<PageSyncState>,
    pub metadata_identity: Option<String>,
}
#[derive(Debug, Clone)]
pub struct PlannedPage {
    pub page_id: String,
    pub difference: Difference,
    pub evidence: LifecycleEvidence,
    pub metadata_identity: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ReconciliationPlan {
    pub binding: CommitBinding,
    pub index_version: u64,
    pub local: BTreeMap<String, LocalPage>,
    pub pages: Vec<PlannedPage>,
    pub errors: BTreeMap<String, PlanError>,
    pub run_errors: Vec<PlanError>,
    pub scanned: usize,
    /// Distinct successful inspections, including pages requiring no work.
    pub inspected_ids: BTreeSet<String>,
    pub traversal_complete: bool,
}
impl ReconciliationPlan {
    pub fn planned_refresh(&self) -> usize {
        self.pages
            .iter()
            .filter(|p| {
                matches!(
                    p.difference,
                    Difference::Missing | Difference::Stale | Difference::Restored
                )
            })
            .count()
    }
    pub fn planned_delete(&self) -> usize {
        self.pages
            .iter()
            .filter(|p| p.difference == Difference::Orphan)
            .count()
    }
    /// Planning never reports successful application or writes acknowledgments.
    pub fn changed(&self) -> usize {
        0
    }
    pub fn deleted(&self) -> usize {
        0
    }
}
