//! Scope identity and page metadata identity shared by refresh consumers.
use notion_knowledge_core::{
    backend::Page, inventory::InventoryParent, lifecycle::LifecycleScope,
    reconciliation::ReconciliationScope,
};
use sha2::{Digest, Sha256};

/// The configured scope cannot be expressed as a reconciliation scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeError;

/// Shared authority identity for webhook and reconciliation consumers.
pub fn production_scope(scope: &LifecycleScope) -> Result<ReconciliationScope, ScopeError> {
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
    .map_err(|_| ScopeError)
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
