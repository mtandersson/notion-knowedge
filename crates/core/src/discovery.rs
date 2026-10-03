//! Read-only discovery. Reports are disposable snapshots, never indexing writes.
use crate::backend::{BackendErrorKind, BackendFuture, PageId};
use serde::Serialize;
use std::collections::BTreeSet;

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
    ExcludedPage,
    ExcludedDescendants,
    ExcludedSourceType,
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
    /// Index consumers must supply the current exclusion policy before any
    /// content extraction or embedding; a failed traversal authorizes nothing.
    fn discover_with_exclusions<'a>(
        &'a self,
        roots: &'a [PageId],
        rules: &'a ExclusionRules,
    ) -> BackendFuture<'a, DiscoveryReport>;
}

pub fn inaccessible(kind: BackendErrorKind) -> bool {
    matches!(
        kind,
        BackendErrorKind::NotFound | BackendErrorKind::PermissionDenied
    )
}

/// Physical source categories. Excluding a container prunes its complete subtree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceType {
    Page,
    Database,
    DataSource,
}

/// IDs must be normalized by the authoritative adapter before traversal.
/// Page exclusions prune the page and all descendants; descendants-only rules
/// retain the named page but prohibit exploring anything below it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExclusionRules {
    pub page_ids: BTreeSet<String>,
    pub descendants_of: BTreeSet<String>,
    pub source_types: BTreeSet<SourceType>,
}

impl ExclusionRules {
    pub fn exclusion(&self, id: &str, source: SourceType) -> Option<SkipReason> {
        if source == SourceType::Page && self.page_ids.contains(id) {
            Some(SkipReason::ExcludedPage)
        } else if self.source_types.contains(&source) {
            Some(SkipReason::ExcludedSourceType)
        } else {
            None
        }
    }
}
