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
    links: Option<Vec<GraphEdge>>,
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
            links: None,
            effect,
        })
    }
    /// Bind a complete body-link family to the same owned receipt and final
    /// SQLite transaction. An empty family deliberately removes stale links.
    pub fn with_links(mut self, mut links: Vec<GraphEdge>) -> Result<Self, CommitError> {
        if (self.tombstone && !links.is_empty())
            || links.iter().any(|edge| {
                edge.source_page_id() != self.page_id
                    || !matches!(
                        edge.relation_type(),
                        "link:page" | "link:block" | "link:invalid"
                    )
                    || edge
                        .provenance()
                        .strip_prefix("markdown:")
                        .is_none_or(|offset| offset.parse::<usize>().is_err())
                    || edge.display_name().is_some()
            })
        {
            return Err(CommitError::InvalidInput);
        }
        links.sort_by_key(edge_identity);
        links.dedup();
        self.links = Some(links);
        self.effect = self.exact_effect();
        Ok(self)
    }
    fn exact_effect(&self) -> String {
        let mut effect = effect_identity(self.tombstone, &self.edges);
        if let Some(links) = &self.links {
            effect = effect.replacen("relation-v1:", "graph-v2:", 1);
            for edge in links {
                field(&mut effect, &edge_identity(edge));
            }
        }
        effect
    }
    fn actual_links(&self, db: &Connection) -> Result<Vec<GraphEdge>, CommitError> {
        let mut links = crate::graph::read_edges(db, "SELECT source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,5)='link:'", &self.page_id).map_err(|_| CommitError::Unavailable)?;
        links.sort_by_key(edge_identity);
        Ok(links)
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
        // Metadata-only planners compare their relation subset while still
        // requiring the stored combined graph to be internally complete.
        if self.links.is_none()
            && stored
                .as_ref()
                .is_some_and(|(_, _, effect)| effect.starts_with("graph-v2:"))
        {
            if !current_complete(db, &self.binding, &self.page_id)? {
                return Ok(false);
            }
            let mut actual = crate::graph::read_edges(db, "SELECT source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'", &self.page_id).map_err(|_| CommitError::Unavailable)?;
            actual.sort_by_key(edge_identity);
            return Ok(actual == self.edges);
        }
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
        if let Some(links) = &self.links {
            return Ok(actual == self.edges && self.actual_links(db)? == *links);
        }
        Ok(actual == self.edges)
    }
    pub(super) fn install(&self, db: &Connection) -> Result<(), CommitError> {
        db.execute("DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,9)='relation:'", [&self.page_id]).map_err(sql)?;
        if self.links.is_some() {
            db.execute("DELETE FROM graph_edges WHERE source_page_id=?1 AND substr(relation_type,1,5)='link:'", [&self.page_id]).map_err(sql)?;
        }
        for edge in self.edges.iter().chain(self.links.iter().flatten()) {
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
/// Validate the complete bounded persisted grammar without trusting it as an
/// authoritative graph snapshot. Adoption preserves these bytes unchanged.
#[cfg(any(feature = "local-lancedb", test))]
pub(crate) fn valid_effect_encoding(effect: &str) -> bool {
    fn field<'a>(input: &mut &'a str) -> Option<&'a str> {
        let (length, rest) = input.split_once(':')?;
        let size: usize = length.parse().ok()?;
        if size.to_string() != length {
            return None;
        }
        let value = rest.get(..size)?;
        *input = rest.get(size..)?;
        Some(value)
    }
    fn edge(input: &str) -> Option<GraphEdge> {
        let mut rest = input;
        let kind = field(&mut rest)?;
        let provenance = field(&mut rest)?;
        let tag = rest.get(..1)?;
        rest = rest.get(1..)?;
        let target = field(&mut rest)?.to_owned();
        let target = match tag {
            "p" => GraphTarget::Page { page_id: target },
            "u" => GraphTarget::Unresolved { reference: target },
            _ => return None,
        };
        let mut result = GraphEdge::new(
            "validation-page".into(),
            target,
            kind.into(),
            provenance.into(),
        )
        .ok()?;
        let tag = rest.get(..1)?;
        rest = rest.get(1..)?;
        match tag {
            "s" => {
                result = result
                    .with_display_name(field(&mut rest)?.to_owned())
                    .ok()?
            }
            "n" => {}
            _ => return None,
        }
        if !rest.is_empty() || edge_identity(&result) != input {
            return None;
        }
        Some(result)
    }
    let (combined, mut body) = if let Some(body) = effect.strip_prefix("relation-v1:") {
        (false, body)
    } else if let Some(body) = effect.strip_prefix("graph-v2:") {
        (true, body)
    } else {
        return false;
    };
    let tombstone = if let Some(rest) = body.strip_prefix("0:") {
        body = rest;
        false
    } else if let Some(rest) = body.strip_prefix("1:") {
        body = rest;
        true
    } else {
        return false;
    };
    if tombstone {
        return body.is_empty();
    }
    let mut links = false;
    let mut previous_relation = None;
    let mut previous_link = None;
    while !body.is_empty() {
        let Some(encoded) = field(&mut body) else {
            return false;
        };
        let Some(edge) = edge(encoded) else {
            return false;
        };
        if let Some(property) = edge.relation_type().strip_prefix("relation:") {
            let identity = edge_identity_without_label(&edge);
            if links
                || edge.provenance().strip_prefix("property:") != Some(property)
                || previous_relation
                    .as_ref()
                    .is_some_and(|previous| previous >= &identity)
            {
                return false;
            }
            previous_relation = Some(identity);
        } else {
            if !combined
                || !matches!(
                    edge.relation_type(),
                    "link:page" | "link:block" | "link:invalid"
                )
                || edge
                    .provenance()
                    .strip_prefix("markdown:")
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_none()
                || edge.display_name().is_some()
                || previous_link
                    .as_ref()
                    .is_some_and(|previous: &String| previous.as_str() >= encoded)
            {
                return false;
            }
            links = true;
            previous_link = Some(encoded.to_owned());
        }
    }
    true
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

    /// Explicit operator invalidation of derived completeness only. The owned
    /// maintenance guard and current writer fence serialize it with all commits.
    /// Existing edges, immutable receipts and operational coordination survive.
    #[cfg(feature = "local-lancedb")]
    pub(crate) fn begin_projection_rebuild(
        self: &Arc<Self>,
        run: String,
        aliases: bool,
        scope: ReconciliationScope,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        self.maintain(lease, clock, move |context| async move {
            context
                .invalidate_projections(&run, &scope, aliases)
                .map_err(|error| match error {
                    CommitError::LeaseLost
                    | CommitError::BindingMismatch
                    | CommitError::Conflict => FailureClass::Conflict,
                    _ => FailureClass::Unavailable,
                })
        })
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

    /// A relation-only legacy receipt cannot attest body-link completeness.
    pub fn current_body_links_complete(&self, page: &str) -> Result<bool, CommitError> {
        if !self.current_relation_projection_complete(page)? {
            return Ok(false);
        }
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        db.query_row(
            "SELECT effect LIKE 'graph-v2:%' FROM page_relation_projections WHERE page_id=?1",
            [page],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)
        .map(|value| value.unwrap_or(false))
    }

    /// Bound source projection materialization before recomputing its receipt.
    /// The graph caller holds its owned read fence across this check and the
    /// complete validation, so a writer cannot expand the projection between them.
    #[cfg(all(unix, feature = "local-lancedb"))]
    pub(crate) fn current_body_links_complete_bounded(
        &self,
        page: &str,
        limit: usize,
    ) -> Result<bool, CommitError> {
        use notion_knowledge_core::graph::GraphEdgeStore;
        self.state
            .edges_from_bounded(page, limit)
            .map_err(|_| CommitError::Unavailable)?;
        self.current_body_links_complete(page)
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
    let combined = effect.starts_with("graph-v2:");
    let tombstone = if effect.starts_with("relation-v1:0:") || effect.starts_with("graph-v2:0:") {
        false
    } else if effect.starts_with("relation-v1:1:") || effect.starts_with("graph-v2:1:") {
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
    let mut projection = RelationProjection::new(binding, page, tombstone, edges)?;
    if combined {
        projection = projection
            .clone()
            .with_links(projection.actual_links(db)?)?;
    }
    Ok(projection.effect == effect)
}

#[cfg(feature = "local-lancedb")]
impl CommitContext {
    fn invalidate_projections(
        &self,
        run: &str,
        scope: &ReconciliationScope,
        aliases: bool,
    ) -> Result<(), CommitError> {
        self.coordinator.identities()?;
        let mut db = self
            .coordinator
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        self.coordinator.validate_binding(&tx)?;
        self.coordinator
            .validate_fence(&tx, &self.lease, (self.clock)())?;
        if scope.identity() != self.coordinator.binding.scope {
            return Err(CommitError::BindingMismatch);
        }
        let current =
            crate::reconciliation::start_run(&tx, &self.lease, (self.clock)(), run, scope)
                .map_err(|_| CommitError::Conflict)?;
        if current.phase != notion_knowledge_core::reconciliation::RunPhase::Inventory {
            return Err(CommitError::Conflict);
        }
        tx.execute(
            if aliases {
                "DELETE FROM page_alias_projections WHERE generation=?1"
            } else {
                "DELETE FROM page_relation_projections WHERE generation=?1"
            },
            [&self.coordinator.binding.generation],
        )
        .map_err(sql)?;
        self.coordinator
            .validate_fence(&tx, &self.lease, (self.clock)())?;
        tx.commit().map_err(sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod effect_encoding_tests {
    use super::*;
    #[test]
    fn persisted_effect_grammar_accepts_complete_families_and_rejects_malformed_fields() {
        let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "1").unwrap();
        let binding = CommitBinding::new("chunks", "workspace", &scope, "1", 1).unwrap();
        let relation = GraphEdge::new(
            "page".into(),
            GraphTarget::Page {
                page_id: "target".into(),
            },
            "relation:names".into(),
            "property:names".into(),
        )
        .unwrap()
        .with_display_name("Öresund".into())
        .unwrap();
        let link = GraphEdge::new(
            "page".into(),
            GraphTarget::Unresolved {
                reference: "reference".into(),
            },
            "link:page".into(),
            "markdown:12".into(),
        )
        .unwrap();
        let projection =
            RelationProjection::new(&binding, "page", false, vec![relation.clone()]).unwrap();
        assert!(valid_effect_encoding(&projection.effect));
        assert!(valid_effect_encoding(
            &projection.with_links(vec![link]).unwrap().effect
        ));
        assert!(valid_effect_encoding("relation-v1:1:"));
        assert!(valid_effect_encoding("graph-v2:0:"));
        for malformed in [
            "relation-v1:garbage",
            "graph-v2:garbage",
            "graph-v2:1:0:",
            "relation-v1:0:1:x",
            "relation-v1:0:999999999999999999999999999999:x",
        ] {
            assert!(!valid_effect_encoding(malformed));
        }
        assert!(!valid_effect_encoding(&effect_identity(
            false,
            &[relation.clone(), relation]
        )));
    }
}
