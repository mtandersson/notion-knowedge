//! Read-only reconciliation planning against the production worker's ownership.
use crate::{
    chunks::GuardedChunkTable,
    commit::{CommitBinding, IndexCommitCoordinator},
};
use notion_knowledge_core::{
    backend::{BackendErrorKind, Page, PageId},
    discovery::SkipReason,
    indexed::IndexedChunk,
    lifecycle::{LifecycleEvidence, LifecycleScope, LifecycleStatus, PageLifecycle},
    reconciliation::ReconciliationScope,
    sync_state::{PageSyncState, SyncStateStore},
};
use notion_knowledge_notion::NotionClient;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

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
/// v1 includes exact citation/property metadata and complete physical ancestry.
/// Unknown legacy identities conservatively require refresh, even for empty pages.
pub(crate) fn metadata_identity(page: &Page, evidence: &LifecycleEvidence) -> String {
    let properties = serde_json::to_string(&page.properties).expect("typed properties serialize");
    format!(
        "{:x}",
        Sha256::digest(
            format!(
                "metadata-v1:{:?}:{:?}:{:?}:{:?}:{}:{properties}:{evidence:?}",
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
}
fn source_error(kind: BackendErrorKind) -> PlanError {
    match kind {
        BackendErrorKind::PermissionDenied => PlanError::Permission,
        BackendErrorKind::NotFound => PlanError::MissingSource,
        BackendErrorKind::Unauthenticated => PlanError::Unauthenticated,
        BackendErrorKind::UnsupportedContent | BackendErrorKind::InvalidInput => {
            PlanError::UnsupportedSource
        }
        BackendErrorKind::Conflict => PlanError::Conflict,
        _ => PlanError::Source,
    }
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
#[derive(Clone)]
pub struct AuthoritativeReconciliationPlanner {
    notion: Arc<NotionClient>,
    guard: GuardedChunkTable,
    coordinator: Arc<IndexCommitCoordinator>,
    scope: LifecycleScope,
}
impl AuthoritativeReconciliationPlanner {
    pub fn new(
        notion: Arc<NotionClient>,
        guard: GuardedChunkTable,
        coordinator: Arc<IndexCommitCoordinator>,
        scope: LifecycleScope,
    ) -> Result<Self, PlanError> {
        let binding = coordinator.binding();
        let expected = production_scope(&scope)?;
        if !guard.is_bound_to(&coordinator)
            || guard.binding() != binding
            || binding.workspace() != scope.workspace_id
            || binding.scope() != expected.identity()
            || binding.generation() != scope.generation.to_string()
            || binding.epoch() != scope.generation
        {
            return Err(PlanError::Ownership);
        }
        Ok(Self {
            notion,
            guard,
            coordinator,
            scope,
        })
    }
    /// Union of index rows and current-generation receipt-owned SQLite pages.
    /// Bare SQLite page IDs do not establish authority.
    pub async fn local_inventory(&self) -> Result<BTreeMap<String, LocalPage>, PlanError> {
        let mut ids: BTreeSet<_> = self
            .coordinator
            .owned_page_ids()
            .map_err(|_| PlanError::Ownership)?
            .into_iter()
            .collect();
        ids.extend(
            self.guard
                .indexed_page_ids()
                .await
                .map_err(|_| PlanError::Index)?,
        );
        let mut result = BTreeMap::new();
        for id in ids {
            let chunks = self
                .guard
                .page_chunks(&id)
                .await
                .map_err(|_| PlanError::Index)?;
            if chunks.iter().any(|c| {
                c.metadata.source.workspace_id != self.scope.workspace_id
                    || !self
                        .scope
                        .roots
                        .iter()
                        .any(|r| r.0 == c.metadata.source.root_page_id)
            }) {
                return Err(PlanError::Ownership);
            }
            // Index-only pages lack generation/policy provenance: fail closed.
            if !self
                .coordinator
                .owns_page_work(&id)
                .map_err(|_| PlanError::Ownership)?
            {
                return Err(PlanError::Ownership);
            }
            let checkpoint = self
                .coordinator
                .state()
                .page_state(&id)
                .map_err(|_| PlanError::Index)?;
            let metadata_identity = self
                .coordinator
                .metadata_identity(&id)
                .map_err(|_| PlanError::Ownership)?;
            result.insert(
                id,
                LocalPage {
                    chunks,
                    checkpoint,
                    metadata_identity,
                },
            );
        }
        Ok(result)
    }
    /// The dry-run and apply sibling share this typed plan. No operational
    /// metadata, content extraction, embedding, or acknowledgments are written.
    pub async fn plan(&self) -> Result<ReconciliationPlan, PlanError> {
        let index_version = self
            .guard
            .inventory_version()
            .await
            .map_err(|_| PlanError::Index)?;
        let local = self.local_inventory().await?;
        let mut plan = ReconciliationPlan {
            binding: self.coordinator.binding().clone(),
            index_version,
            local,
            pages: vec![],
            errors: BTreeMap::new(),
            run_errors: vec![],
            scanned: 0,
            traversal_complete: false,
        };
        let mut discovered = BTreeSet::new();
        match self
            .notion
            .crawl_with_exclusions(&self.scope.roots, &self.scope.exclusions)
            .await
        {
            Ok(report) => {
                plan.traversal_complete = !report.skipped.iter().any(|s| {
                    matches!(
                        s.reason,
                        SkipReason::Inaccessible | SkipReason::SyncedReference
                    )
                });
                for skipped in &report.skipped {
                    if skipped.reason == SkipReason::Inaccessible {
                        plan.errors
                            .insert(skipped.id.clone(), PlanError::Permission);
                    }
                }
                discovered.extend(report.pages.into_iter().map(|p| p.id));
                if !plan.traversal_complete {
                    plan.run_errors.push(PlanError::IncompleteTraversal);
                }
            }
            Err(error) => {
                plan.run_errors.push(PlanError::IncompleteTraversal);
                plan.run_errors.push(source_error(error.kind));
            }
        }
        let ids: BTreeSet<_> = discovered
            .iter()
            .cloned()
            .chain(plan.local.keys().cloned())
            .collect();
        for id in ids {
            match self
                .inspect(
                    &id,
                    discovered.contains(&id),
                    plan.local.get(&id),
                    plan.traversal_complete,
                )
                .await
            {
                Ok(Some(page)) => {
                    plan.scanned += 1;
                    plan.pages.push(page);
                }
                Ok(None) => {
                    plan.scanned += 1;
                }
                Err(error) => {
                    plan.errors.insert(id, error);
                }
            }
        }
        if !plan.errors.is_empty() {
            plan.traversal_complete = false;
        }
        if self.local_inventory().await? != plan.local
            || self
                .guard
                .inventory_version()
                .await
                .map_err(|_| PlanError::Index)?
                != index_version
        {
            return Err(PlanError::Conflict);
        }
        if !plan.traversal_complete {
            for page in &plan.pages {
                if page.difference == Difference::Orphan {
                    plan.errors
                        .insert(page.page_id.clone(), PlanError::IncompleteTraversal);
                }
            }
            plan.pages.retain(|p| p.difference != Difference::Orphan);
        }
        Ok(plan)
    }
    async fn inspect(
        &self,
        id: &str,
        discovered: bool,
        local: Option<&LocalPage>,
        complete: bool,
    ) -> Result<Option<PlannedPage>, PlanError> {
        let page_id = PageId(id.into());
        let evidence = self
            .notion
            .lifecycle(&page_id, &self.scope)
            .await
            .map_err(|error| source_error(error.kind))?;
        let (difference, identity) = if matches!(evidence.status, LifecycleStatus::Allowed { .. }) {
            let page = self
                .notion
                .fetch_page(id)
                .await
                .map_err(|error| source_error(error.kind))?;
            let identity = metadata_identity(&page, &evidence);
            let difference = match local {
                None => Difference::Missing,
                Some(local) if local.checkpoint.as_ref().is_some_and(|c| c.is_tombstone()) => {
                    Difference::Restored
                }
                Some(local)
                    if local.checkpoint.as_ref().is_some_and(|c| {
                        !c.is_tombstone()
                            && c.last_edited_time() == Some(page.last_edited_time.as_str())
                    }) && local.metadata_identity.as_ref() == Some(&identity) =>
                {
                    Difference::Unchanged
                }
                Some(_) => Difference::Stale,
            };
            // Exact page metadata gets its own second read: lifecycle only
            // compares ancestry/timestamps, which cannot detect property races.
            let fresh = self
                .notion
                .fetch_page(id)
                .await
                .map_err(|error| source_error(error.kind))?;
            if fresh != page {
                return Err(PlanError::Conflict);
            }
            (difference, Some(identity))
        } else {
            if discovered {
                return Err(PlanError::Conflict);
            }
            if !complete {
                return Err(PlanError::IncompleteTraversal);
            }
            if local.is_some_and(|l| {
                l.checkpoint.as_ref().is_some_and(|c| c.is_tombstone()) && l.chunks.is_empty()
            }) {
                return Ok(None);
            }
            (Difference::Orphan, None)
        };
        self.notion
            .revalidate_lifecycle(&evidence, &self.scope)
            .await
            .map_err(|_| PlanError::Conflict)?;
        Ok(Some(PlannedPage {
            page_id: id.into(),
            difference,
            evidence,
            metadata_identity: identity,
        }))
    }
}
