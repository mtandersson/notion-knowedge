//! Exact derived relation effects. These proposals convey no source authority.
use super::*;
use notion_knowledge_core::graph::{GraphEdge, GraphTarget};

const VERSION: i64 = 1;

/// A complete, canonical relation-family replacement for one bound page.
/// Labels, target resolution and a tombstone are all part of the effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationProjection {
    binding: CommitBinding,
    page_id: String,
    tombstone: bool,
    edges: Vec<GraphEdge>,
    pub(super) effect: String,
}
impl RelationProjection {
    pub fn new(
        binding: &CommitBinding,
        page_id: &str,
        tombstone: bool,
        mut edges: Vec<GraphEdge>,
    ) -> Result<Self, CommitError> {
        validate(page_id).map_err(|_| CommitError::InvalidInput)?;
        if (tombstone && !edges.is_empty())
            || edges.iter().any(|edge| {
                edge.source_page_id() != page_id
                    || !edge.relation_type().starts_with("relation:")
                    || edge.provenance() != format!("property:{}", &edge.relation_type()[9..])
            })
        {
            return Err(CommitError::InvalidInput);
        }
        edges.sort_by_key(edge_identity);
        edges.dedup();
        // Two different labels for one edge cannot both be persisted.
        if edges.windows(2).any(|pair| {
            edge_identity_without_label(&pair[0]) == edge_identity_without_label(&pair[1])
        }) {
            return Err(CommitError::InvalidInput);
        }
        let effect = effect_identity(tombstone, &edges);
        Ok(Self {
            binding: binding.clone(),
            page_id: page_id.into(),
            tombstone,
            edges,
            effect,
        })
    }
    pub fn page_id(&self) -> &str {
        &self.page_id
    }
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }
    pub(super) fn matches_operation(
        &self,
        binding: &CommitBinding,
        operation: &PageOperation,
    ) -> bool {
        self.binding == *binding
            && self.page_id == operation.checkpoint.page_id()
            && self.tombstone == operation.checkpoint.is_tombstone()
    }
    pub(super) fn complete(&self, db: &Connection) -> Result<bool, CommitError> {
        let stored: Option<(String, i64, String)> = db
            .query_row(
                "SELECT generation,version,effect FROM page_relation_projections WHERE page_id=?1",
                [&self.page_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        if stored
            != Some((
                self.binding.generation.clone(),
                VERSION,
                self.effect.clone(),
            ))
        {
            return Ok(false);
        }
        let mut actual = crate::graph::read_edges(db, "SELECT source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'", &self.page_id).map_err(|_| CommitError::Unavailable)?;
        actual.sort_by_key(edge_identity);
        Ok(actual == self.edges)
    }
    pub(super) fn install(&self, db: &Connection) -> Result<(), CommitError> {
        db.execute("DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'", [&self.page_id]).map_err(sql)?;
        for edge in &self.edges {
            let (target, unresolved): (Option<&str>, Option<&str>) = match edge.target() {
                GraphTarget::Page { page_id } => (Some(page_id), None),
                GraphTarget::Unresolved { reference } => (None, Some(reference)),
            };
            db.execute("INSERT INTO graph_edges(source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name) VALUES(?1,?2,?3,?4,?5,?6)", params![self.page_id,target,unresolved,edge.relation_type(),edge.provenance(),edge.display_name()]).map_err(sql)?;
        }
        db.execute("INSERT INTO page_relation_projections VALUES(?1,?2,?3,?4) ON CONFLICT(page_id) DO UPDATE SET generation=excluded.generation,version=excluded.version,effect=excluded.effect",params![self.page_id,self.binding.generation,VERSION,self.effect]).map_err(sql)?;
        Ok(())
    }
}
fn field(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}
fn edge_identity_without_label(edge: &GraphEdge) -> String {
    let mut result = String::new();
    field(&mut result, edge.relation_type());
    field(&mut result, edge.provenance());
    match edge.target() {
        GraphTarget::Page { page_id } => {
            result.push('p');
            field(&mut result, page_id);
        }
        GraphTarget::Unresolved { reference } => {
            result.push('u');
            field(&mut result, reference);
        }
    }
    result
}
fn edge_identity(edge: &GraphEdge) -> String {
    let mut result = edge_identity_without_label(edge);
    match edge.display_name() {
        Some(name) => {
            result.push('s');
            field(&mut result, name);
        }
        None => result.push('n'),
    }
    result
}
fn effect_identity(tombstone: bool, edges: &[GraphEdge]) -> String {
    let mut result = format!("relation-v1:{}:", u8::from(tombstone));
    for edge in edges {
        field(&mut result, &edge_identity(edge));
    }
    result
}
impl IndexCommitCoordinator {
    /// Only reads compatible existing state. Does not acquire a writer lease,
    /// migrate, repair, or infer completeness from a nonzero edge count.
    pub fn relation_projection_complete(
        &self,
        projection: &RelationProjection,
    ) -> Result<bool, CommitError> {
        self.identities()?;
        if projection.binding != self.binding {
            return Err(CommitError::BindingMismatch);
        }
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db.transaction().map_err(sql)?;
        self.validate_binding(&tx)?;
        let complete = projection.complete(&tx)?;
        tx.commit().map_err(sql)?;
        Ok(complete)
    }
}
