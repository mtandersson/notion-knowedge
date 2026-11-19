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

/// Read-only placement of a page in the last walk's inventory, used to decide
/// whether a relation or link target is inside the configured scope without a
/// Notion request.
pub trait InventoryPlacement: Send + Sync {
    /// The page's placement under a root. `None` when the page or any
    /// ancestor is not a readable, live, successfully walked inventory entry
    /// or its chain does not reach a root. An unreadable inventory is an
    /// error, never an absent page.
    fn placed_chain(
        &self,
        page_id: &str,
    ) -> Result<Option<PlacedChain>, crate::sync_state::SyncStateError>;
}

/// Where the inventory places a page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacedChain {
    /// Ancestor pages, nearest first and ending at the root (empty for a root).
    pub ancestors: Vec<String>,
    /// `(data_source_id, database_id)` of every database the page or an
    /// ancestor is a row of.
    pub data_sources: Vec<(String, String)>,
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

/// What one sync found changed against the previous inventory. `unplaced`
/// counts search hits whose parent chain the inventory could not resolve;
/// they are left for the next full walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DriftCounts {
    pub added: u64,
    pub changed: u64,
    pub moved: u64,
    pub removed: u64,
    pub trashed: u64,
    pub unplaced: u64,
}
impl InventoryDiff {
    pub fn drift(&self) -> DriftCounts {
        DriftCounts {
            added: self.added.len() as u64,
            changed: self.changed.len() as u64,
            moved: self.moved.len() as u64,
            removed: self.removed.len() as u64,
            trashed: self.trashed.len() as u64,
            unplaced: 0,
        }
    }
}

/// The last finished sync: whether its walk was complete (only a complete
/// walk replaces the inventory and removes pages), and what it changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SyncRecord {
    pub run_id: String,
    pub incremental: bool,
    pub complete: bool,
    /// Removals deferred one walk under a newly inaccessible page.
    pub held: u64,
    pub drift: DriftCounts,
    pub finished_at: i64,
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
            Self::UnreadableMetadata => "unreadable_metadata",
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
            Self::UnreadableMetadata,
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
