//! Read-only discovery. Reports are disposable snapshots, never indexing writes.
use crate::backend::{BackendErrorKind, BackendFuture, PageId};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveredPage {
    pub id: String,
    pub url: String,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedContent {
    pub id: String,
    pub reason: SkipReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    Inaccessible,
    OutsideScope,
    Archived,
    SyncedReference,
}

/// Returned only after successful traversal. A failed/interrupted traversal has
/// no checkpoint or side effects: restart with the same roots and deduplicate
/// downstream writes by page ID. Not an atomic Notion snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveryReport {
    pub roots: Vec<String>,
    pub pages: Vec<DiscoveredPage>,
    pub skipped: Vec<SkippedContent>,
}

pub trait ScopedDiscovery: Send + Sync {
    /// Configured roots are explicit authority; links/relations do not expand it.
    fn discover<'a>(&'a self, roots: &'a [PageId]) -> BackendFuture<'a, DiscoveryReport>;
}

pub fn inaccessible(kind: BackendErrorKind) -> bool {
    matches!(
        kind,
        BackendErrorKind::NotFound | BackendErrorKind::PermissionDenied
    )
}
