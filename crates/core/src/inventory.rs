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

impl InventoryParent {
    /// The walk's parent for a page whose physical chain a lifecycle read
    /// verified: the nearest page or data source above it, or `Root` for a
    /// configured root. Single-page callers use it so their metadata matches
    /// what the walk records for the same page.
    pub fn from_evidence(evidence: &crate::lifecycle::LifecycleEvidence) -> Option<Self> {
        use crate::lifecycle::LifecycleKind;
        let page = evidence.ancestry.first()?;
        if evidence.scope.roots.iter().any(|root| root.0 == page.id) {
            return Some(Self::Root);
        }
        let (mut source, mut database) = (None, None);
        for node in &evidence.ancestry[1..] {
            match node.kind {
                // The nearest container wins, as in the walk.
                LifecycleKind::DataSource => {
                    source.get_or_insert_with(|| node.id.clone());
                }
                LifecycleKind::Database => {
                    database.get_or_insert_with(|| node.id.clone());
                }
                LifecycleKind::Block => {}
                LifecycleKind::Page => {
                    return Some(match (source, database) {
                        (Some(data_source_id), Some(database_id)) => Self::DataSource {
                            data_source_id,
                            database_id,
                            owner_page_id: node.id.clone(),
                        },
                        (None, None) => Self::Page(node.id.clone()),
                        _ => return None,
                    });
                }
            }
        }
        None
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::PageId,
        lifecycle::{
            LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
            PhysicalParent,
        },
    };

    fn evidence(chain: &[(LifecycleKind, &str)]) -> LifecycleEvidence {
        LifecycleEvidence {
            scope: LifecycleScope {
                workspace_id: "w".into(),
                generation: 1,
                roots: vec![PageId("root".into())],
                exclusions: Default::default(),
            },
            status: LifecycleStatus::Allowed {
                roots: vec!["root".into()],
            },
            ancestry: chain
                .iter()
                .map(|(kind, id)| LifecycleNode {
                    kind: *kind,
                    id: (*id).into(),
                    revision: String::new(),
                    inactive: false,
                    parent: PhysicalParent::Workspace,
                })
                .collect(),
        }
    }

    #[test]
    fn evidence_maps_to_the_parent_the_walk_records() {
        use LifecycleKind::*;
        assert_eq!(
            InventoryParent::from_evidence(&evidence(&[(Page, "root")])),
            Some(InventoryParent::Root)
        );
        assert_eq!(
            InventoryParent::from_evidence(&evidence(&[(Page, "p"), (Block, "b"), (Page, "root")])),
            Some(InventoryParent::Page("root".into()))
        );
        assert_eq!(
            InventoryParent::from_evidence(&evidence(&[
                (Page, "row"),
                (DataSource, "ds"),
                (Database, "db"),
                (Page, "root"),
            ])),
            Some(InventoryParent::DataSource {
                data_source_id: "ds".into(),
                database_id: "db".into(),
                owner_page_id: "root".into(),
            })
        );
        assert_eq!(
            InventoryParent::from_evidence(&evidence(&[(Page, "p")])),
            None
        );
    }
}
