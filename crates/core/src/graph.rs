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
    display_name: Option<String>,
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
            display_name: None,
        })
    }

    /// Attach the authoritative property label without changing edge identity.
    pub fn with_display_name(mut self, name: String) -> Result<Self, SyncStateError> {
        if !self.relation_type.starts_with("relation:")
            || self.provenance != format!("property:{}", &self.relation_type[9..])
            || name.chars().count() > 4096
        {
            return Err(SyncStateError::InvalidInput);
        }
        self.display_name = Some(name);
        Ok(self)
    }

    /// None means that no authoritative label was present in the snapshot.
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
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
    /// Optional owned read view held through one bounded navigation. Acquired
    /// only after seed authorization; persistent adapters can fence writers and
    /// pending effects across all incoming/outgoing reads and awaited scope checks.
    fn read_snapshot(&self) -> Result<Option<Box<dyn GraphEdgeStore>>, SyncStateError> {
        Ok(None)
    }

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

    /// Atomically replace only Markdown/Notion-link edges for a single page.
    /// Must preserve relation-property and other independently indexed edges.
    /// Empty replacement removes stale links after edits or page deletion.
    fn replace_page_link_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError>;

    fn edges_from(&self, source_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError>;

    /// Only resolved targets can be queried as authoritative graph nodes.
    fn edges_to(&self, target_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError>;
    /// Read at most `limit` edges, rejecting overflow without partial evidence.
    /// Implementations must bound storage consumption to limit plus one sentinel
    /// before allocating edges or validating their source projections. Adapters
    /// without bounded reads fail closed rather than falling back to full reads.
    fn edges_from_bounded(&self, _: &str, _: usize) -> Result<Vec<GraphEdge>, SyncStateError> {
        Err(SyncStateError::Unavailable)
    }

    /// Incoming equivalent of `edges_from_bounded`; zero permits only no edges.
    fn edges_to_bounded(&self, _: &str, _: usize) -> Result<Vec<GraphEdge>, SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
}
