//! Authoritative lifecycle evidence. No source content or index mutation capability.
use crate::{
    backend::{BackendFuture, PageId},
    discovery::{ExclusionRules, SkipReason},
};

/// Trusted composition configuration, never copied from a webhook or page response.
/// Workspace identity binds the credential externally: Notion's workspace parent
/// carries only `true`, and cannot prove a workspace UUID itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleScope {
    pub workspace_id: String,
    pub generation: u64,
    pub roots: Vec<PageId>,
    pub exclusions: ExclusionRules,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LifecycleKind {
    Page,
    Block,
    Database,
    DataSource,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalParent {
    Workspace,
    Object { kind: LifecycleKind, id: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleNode {
    pub kind: LifecycleKind,
    pub id: String,
    pub revision: String,
    pub inactive: bool,
    pub parent: PhysicalParent,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleStatus {
    Allowed { roots: Vec<String> },
    Inactive,
    OutsideScope,
    Excluded { id: String, reason: SkipReason },
}
/// Active-page evidence contains the complete selected-page-to-workspace chain.
/// Inactive evidence contains only the affirmatively inactive selected page.
/// This is a sequential observation, not an atomic Notion snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvidence {
    pub scope: LifecycleScope,
    pub status: LifecycleStatus,
    pub ancestry: Vec<LifecycleNode>,
}
/// Revalidation rejects changed scope/generation or observed source evidence.
/// Consumers must additionally serialize/fence their actual external index commit;
/// a successful revalidation cannot prevent a later Notion edit or move.
pub trait PageLifecycle: Send + Sync {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence>;
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        current_scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()>;
}
