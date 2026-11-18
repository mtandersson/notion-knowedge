//! Discovery vocabulary: typed skips and exclusion rules applied by the walk.
use crate::backend::BackendErrorKind;
use serde::Serialize;
use std::collections::BTreeSet;

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
    /// The page was read but its metadata could not be understood (for
    /// example an unknown property type), so it cannot be indexed.
    UnreadableMetadata,
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
