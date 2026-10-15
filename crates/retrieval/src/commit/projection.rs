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
    /// Receipt IDs cannot cross page or generation authority, even on a no-op.
    pub fn validate_page_receipt_authority(&self, id: &str, page: &str) -> Result<(), CommitError> {
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let valid:Option<bool>=db.query_row("SELECT generation=?2 AND page_id=?3 FROM index_commit_receipts WHERE operation_id=?1",params![id,self.binding.generation,page],|r|r.get(0)).optional().map_err(sql)?;
        if valid == Some(false) {
            return Err(CommitError::Conflict);
        }
        Ok(())
    }

    /// Stable fresh-ID repair chain. Unacknowledged repairs keep their ID across
    /// restart; a completed repair never changes its immutable receipt.
    pub fn relation_repair_id(&self, original: &str) -> Result<String, CommitError> {
        use sha2::{Digest, Sha256};
        let prefix = format!("relation-repair:{:x}:", Sha256::digest(original.as_bytes()));
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let mut query = db.prepare("SELECT operation_id,status FROM index_commit_receipts WHERE operation_id>=?1 AND operation_id<?2").map_err(sql)?;
        let rows = query
            .query_map(params![prefix, format!("{prefix}~")], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(sql)?;
        let mut latest = None;
        for row in rows {
            let (id, status) = row.map_err(sql)?;
            let sequence = id[prefix.len()..]
                .parse::<u64>()
                .map_err(|_| CommitError::Conflict)?;
            if latest.is_none_or(|(last, _)| sequence > last) {
                latest = Some((sequence, status));
            }
        }
        let next = match latest {
            Some((sequence, 0 | 2)) => sequence,
            Some((sequence, _)) => sequence.checked_add(1).ok_or(CommitError::Conflict)?,
            None => 0,
        };
        Ok(format!("{prefix}{next}"))
    }

    /// A prior partial repair must be replayed even if the source later reverts
    /// to an older complete graph; its Lance effects may still be unacknowledged.
    pub fn unfinished_relation_repair_id(
        &self,
        original: &str,
    ) -> Result<Option<String>, CommitError> {
        let id = self.relation_repair_id(original)?;
        Ok(self
            .receipt(&id)?
            .filter(|receipt| {
                matches!(
                    receipt.status,
                    ReceiptStatus::Pending | ReceiptStatus::Failed
                )
            })
            .map(|_| id))
    }

    /// Verify the current exact persisted effect without treating edges as authority.
    pub fn current_relation_projection_complete(&self, page: &str) -> Result<bool, CommitError> {
        self.identities()?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db.transaction().map_err(sql)?;
        self.validate_binding(&tx)?;
        let complete = current_complete(&tx, &self.binding, page)?;
        tx.commit().map_err(sql)?;
        Ok(complete)
    }

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

pub(crate) fn current_complete(
    db: &Connection,
    binding: &CommitBinding,
    page: &str,
) -> Result<bool, CommitError> {
    let row: Option<(String, i64, String)> = db
        .query_row(
            "SELECT generation,version,effect FROM page_relation_projections WHERE page_id=?1",
            [page],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql)?;
    let Some((generation, version, effect)) = row else {
        return Ok(false);
    };
    if generation != binding.generation || version != VERSION {
        return Ok(false);
    }
    let tombstone = if effect.starts_with("relation-v1:0:") {
        false
    } else if effect.starts_with("relation-v1:1:") {
        true
    } else {
        return Ok(false);
    };
    let checkpoint: Option<bool> = db
        .query_row(
            "SELECT tombstoned FROM page_sync_state WHERE page_id=?1",
            [page],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    if checkpoint != Some(tombstone) {
        return Ok(false);
    }
    let edges = crate::graph::read_edges(db,"SELECT source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'",page).map_err(|_|CommitError::Unavailable)?;
    Ok(RelationProjection::new(binding, page, tombstone, edges)?.effect == effect)
}
