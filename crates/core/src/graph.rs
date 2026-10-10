//! Provider-independent edges for the derived local knowledge graph.
//!
//! A node is identified by its stable Notion page ID, not by a title or a
//! LanceDB chunk ID. Unresolved references are never promoted to node IDs.

use crate::sync_state::{SyncStateError, validate_identifier};

/// A known Notion page or a reference that cannot yet be resolved to a page.
/// An unresolved reference must not be used as an authoritative page identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTarget {
    Page { page_id: String },
    Unresolved { reference: String },
}

/// One directional, provenance-carrying page-to-page relationship.
///
/// `relation_type` describes the semantic relationship (such as a Notion
/// relation property or internal link). `provenance` identifies the particular
/// source property/block that produced it, without storing page content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdge {
    source_page_id: String,
    target: GraphTarget,
    relation_type: String,
    provenance: String,
}

impl GraphEdge {
    pub fn new(
        source_page_id: String,
        target: GraphTarget,
        relation_type: String,
        provenance: String,
    ) -> Result<Self, SyncStateError> {
        validate_identifier(&source_page_id)?;
        match &target {
            GraphTarget::Page { page_id } => validate_identifier(page_id)?,
            GraphTarget::Unresolved { reference } => validate_identifier(reference)?,
        }
        validate_identifier(&relation_type)?;
        validate_identifier(&provenance)?;
        Ok(Self {
            source_page_id,
            target,
            relation_type,
            provenance,
        })
    }

    pub fn source_page_id(&self) -> &str {
        &self.source_page_id
    }

    pub fn target(&self) -> &GraphTarget {
        &self.target
    }

    pub fn relation_type(&self) -> &str {
        &self.relation_type
    }

    pub fn provenance(&self) -> &str {
        &self.provenance
    }
}

/// Local derived graph storage. Callers must establish trusted read/write
/// scope before using this port; it intentionally exposes no MCP tool.
///
/// Replacement is atomic, including replacing with an empty collection.
/// Failed validation or persistence must leave previous edges untouched.
pub trait GraphEdgeStore: Send + Sync {
    fn replace_page_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError>;

    /// Atomically replace only relation-property edges for this page. Other
    /// edge families (such as future Markdown links) must be left intact.
    /// Empty input removes stale relation edges, e.g. deleted properties.
    fn replace_page_relation_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError>;

    fn edges_from(&self, source_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError>;

    /// Only resolved targets can be queried as authoritative graph nodes.
    fn edges_to(&self, target_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError>;
}
