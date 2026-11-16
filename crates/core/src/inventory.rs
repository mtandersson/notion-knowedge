//! Disposable page inventory: every page one walk reached from the root, with
//! title-free metadata and its local sync outcome. Ids and timestamps only.
use crate::{discovery::SkipReason, reconciliation::FailureClass};
use serde::Serialize;

/// Where the walk reached a page from. The first parent reached wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InventoryParent {
    Root,
    Page(String),
    /// A database row. `owner_page_id` tagged the database in its Markdown.
    DataSource {
        data_source_id: String,
        database_id: String,
        owner_page_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InventoryEntry {
    pub page_id: String,
    pub parent: InventoryParent,
    pub root_id: String,
    /// Empty when the page could not be read (for example a 404 child).
    pub last_edited_time: String,
    pub in_trash: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryOutcome {
    Indexed { revision: String },
    Skipped(SkipReason),
    Failed(FailureClass),
}

/// Ids are sorted. `removed` lists entries the replacing walk did not reach.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InventoryDiff {
    pub added: Vec<String>,
    pub changed: Vec<String>,
    pub moved: Vec<String>,
    pub removed: Vec<String>,
    pub trashed: Vec<String>,
    pub unchanged: usize,
}

/// `indexed + skipped + failed + pending == total` always holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InventoryCounts {
    pub total: u64,
    pub indexed: u64,
    pub skipped: u64,
    pub failed: u64,
    pub pending: u64,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inaccessible => "inaccessible",
            Self::OutsideScope => "outside_scope",
            Self::Archived => "archived",
            Self::SyncedReference => "synced_reference",
            Self::ExcludedPage => "excluded_page",
            Self::ExcludedDescendants => "excluded_descendants",
            Self::ExcludedSourceType => "excluded_source_type",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::Inaccessible,
            Self::OutsideScope,
            Self::Archived,
            Self::SyncedReference,
            Self::ExcludedPage,
            Self::ExcludedDescendants,
            Self::ExcludedSourceType,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == value)
    }
}

impl FailureClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Index => "index",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        [Self::Source, Self::Index, Self::Conflict, Self::Unavailable]
            .into_iter()
            .find(|class| class.as_str() == value)
    }
}
