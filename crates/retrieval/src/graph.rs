//! SQLite graph-edge adapter. This stores derived relationship metadata
//! independently of LanceDB and does not perform Notion reads or authorization.

use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    sync_state::{SyncStateError, validate_identifier},
};
use rusqlite::{Connection, TransactionBehavior, params};

use crate::sync_state::SqliteSyncStateStore;

impl GraphEdgeStore for SqliteSyncStateStore {
    fn replace_page_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError> {
        validate_identifier(source_page_id)?;
        // Validate the complete replacement before deleting anything. In
        // particular a caller cannot accidentally erase a different page.
        if edges
            .iter()
            .any(|edge| edge.source_page_id() != source_page_id)
        {
            return Err(SyncStateError::InvalidInput);
        }

        let mut connection = self.lock_connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_failure)?;
        transaction
            .execute(
                "DELETE FROM graph_edges WHERE source_page_id = ?1",
                params![source_page_id],
            )
            .map_err(sqlite_failure)?;

        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO graph_edges (
                        source_page_id, target_page_id, unresolved_reference,
                        relation_type, provenance, display_name
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT DO NOTHING",
                )
                .map_err(sqlite_failure)?;
            for edge in edges {
                let (page_id, unresolved): (Option<&str>, Option<&str>) = match edge.target() {
                    GraphTarget::Page { page_id } => (Some(page_id), None),
                    GraphTarget::Unresolved { reference } => (None, Some(reference)),
                };
                statement
                    .execute(params![
                        source_page_id,
                        page_id,
                        unresolved,
                        edge.relation_type(),
                        edge.provenance(),
                        edge.display_name(),
                    ])
                    .map_err(sqlite_failure)?;
            }
        }

        transaction.commit().map_err(sqlite_failure)
    }

    fn replace_page_relation_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError> {
        validate_identifier(source_page_id)?;
        // Check the full caller-provided batch before any destructive DELETE.
        // Relation-property edges cannot overwrite independently indexed link edges.
        if edges.iter().any(|edge| {
            edge.source_page_id() != source_page_id
                || !edge.relation_type().starts_with("relation:")
                || edge.provenance()
                    != format!("property:{}", &edge.relation_type()["relation:".len()..])
        }) {
            return Err(SyncStateError::InvalidInput);
        }

        let mut connection = self.lock_connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_failure)?;
        transaction
            .execute(
                "DELETE FROM graph_edges
                 WHERE source_page_id = ?1 AND substr(relation_type, 1, 9) = 'relation:'",
                params![source_page_id],
            )
            .map_err(sqlite_failure)?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO graph_edges (
                        source_page_id, target_page_id, unresolved_reference,
                        relation_type, provenance, display_name
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT DO NOTHING",
                )
                .map_err(sqlite_failure)?;
            for edge in edges {
                let (target, unresolved): (Option<&str>, Option<&str>) = match edge.target() {
                    GraphTarget::Page { page_id } => (Some(page_id), None),
                    GraphTarget::Unresolved { reference } => (None, Some(reference)),
                };
                statement
                    .execute(params![
                        source_page_id,
                        target,
                        unresolved,
                        edge.relation_type(),
                        edge.provenance(),
                        edge.display_name(),
                    ])
                    .map_err(sqlite_failure)?;
            }
        }
        transaction.commit().map_err(sqlite_failure)
    }

    fn replace_page_link_edges(
        &self,
        source_page_id: &str,
        edges: &[GraphEdge],
    ) -> Result<(), SyncStateError> {
        validate_identifier(source_page_id)?;
        // Fail before DELETE on malformed or cross-family input. A page link
        // update can NEVER delete relation-property edges from #61.
        if edges.iter().any(|edge| {
            edge.source_page_id() != source_page_id
                || !matches!(
                    edge.relation_type(),
                    "link:page" | "link:block" | "link:invalid"
                )
                || !edge
                    .provenance()
                    .strip_prefix("markdown:")
                    .is_some_and(|offset| offset.parse::<usize>().is_ok())
        }) {
            return Err(SyncStateError::InvalidInput);
        }

        let mut connection = self.lock_connection()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_failure)?;
        tx.execute(
            "DELETE FROM graph_edges
             WHERE source_page_id = ?1 AND substr(relation_type, 1, 5) = 'link:'",
            params![source_page_id],
        )
        .map_err(sqlite_failure)?;
        {
            let mut insert = tx
                .prepare(
                    "INSERT INTO graph_edges (
                        source_page_id, target_page_id, unresolved_reference,
                        relation_type, provenance, display_name
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT DO NOTHING",
                )
                .map_err(sqlite_failure)?;
            for edge in edges {
                let (page_id, unresolved): (Option<&str>, Option<&str>) = match edge.target() {
                    GraphTarget::Page { page_id } => (Some(page_id), None),
                    GraphTarget::Unresolved { reference } => (None, Some(reference)),
                };
                insert
                    .execute(params![
                        source_page_id,
                        page_id,
                        unresolved,
                        edge.relation_type(),
                        edge.provenance(),
                        edge.display_name()
                    ])
                    .map_err(sqlite_failure)?;
            }
        }
        tx.commit().map_err(sqlite_failure)
    }

    fn edges_from(&self, source_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        validate_identifier(source_page_id)?;
        let connection = self.lock_connection()?;
        read_edges(
            &connection,
            "SELECT source_page_id, target_page_id, unresolved_reference,
                    relation_type, provenance, display_name
             FROM graph_edges WHERE source_page_id = ?1
             ORDER BY relation_type, provenance, target_page_id, unresolved_reference",
            source_page_id,
        )
    }

    fn edges_to(&self, target_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        validate_identifier(target_page_id)?;
        let connection = self.lock_connection()?;
        read_edges(
            &connection,
            "SELECT source_page_id, target_page_id, unresolved_reference,
                    relation_type, provenance, display_name
             FROM graph_edges WHERE target_page_id = ?1
             ORDER BY source_page_id, relation_type, provenance",
            target_page_id,
        )
    }
    fn edges_from_bounded(
        &self,
        page: &str,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read_bounded(page, false, limit)
    }

    fn edges_to_bounded(&self, page: &str, limit: usize) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read_bounded(page, true, limit)
    }
}

impl SqliteSyncStateStore {
    fn read_bounded(
        &self,
        page: &str,
        incoming: bool,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, SyncStateError> {
        validate_identifier(page)?;
        let sentinel = limit
            .checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or(SyncStateError::InvalidInput)?;
        let connection = self.lock_connection()?;
        // No ORDER BY: the matching index can stop after the overflow sentinel.
        // Navigation sorts only the accepted, bounded adjacency.
        let column = if incoming {
            "target_page_id"
        } else {
            "source_page_id"
        };
        let sql = format!(
            "SELECT source_page_id,target_page_id,unresolved_reference,relation_type,provenance,display_name FROM graph_edges WHERE {column}=?1 LIMIT {sentinel}"
        );
        read_edges_limited(&connection, &sql, page, Some(limit))
    }
}

pub(crate) fn read_edges(
    connection: &Connection,
    sql: &str,
    page_id: &str,
) -> Result<Vec<GraphEdge>, SyncStateError> {
    read_edges_limited(connection, sql, page_id, None)
}

fn read_edges_limited(
    connection: &Connection,
    sql: &str,
    page_id: &str,
    limit: Option<usize>,
) -> Result<Vec<GraphEdge>, SyncStateError> {
    let mut statement = connection.prepare(sql).map_err(sqlite_failure)?;
    let mut rows = statement
        .query_map(params![page_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .map_err(sqlite_failure)?;
    let stored = consume_rows(&mut rows, limit)?;

    stored
        .into_iter()
        .map(
            |(source, page, unresolved, relation_type, provenance, display_name)| {
                let target = match (page, unresolved) {
                    (Some(page_id), None) => GraphTarget::Page { page_id },
                    (None, Some(reference)) => GraphTarget::Unresolved { reference },
                    _ => return Err(SyncStateError::CorruptState),
                };
                let edge = GraphEdge::new(source, target, relation_type, provenance)
                    .map_err(|_| SyncStateError::CorruptState)?;
                match display_name {
                    Some(name) => edge
                        .with_display_name(name)
                        .map_err(|_| SyncStateError::CorruptState),
                    None => Ok(edge),
                }
            },
        )
        .collect()
}

// Stop on the first overflow row, before materializing a full adjacency.
fn consume_rows<T>(
    rows: impl Iterator<Item = rusqlite::Result<T>>,
    limit: Option<usize>,
) -> Result<Vec<T>, SyncStateError> {
    let mut stored = Vec::new();
    for row in rows {
        if limit.is_some_and(|limit| stored.len() == limit) {
            return Err(SyncStateError::Unavailable);
        }
        stored.push(row.map_err(sqlite_failure)?);
    }
    Ok(stored)
}

fn sqlite_failure(_: rusqlite::Error) -> SyncStateError {
    // Do not leak local paths, page IDs, source content, or raw SQL errors.
    SyncStateError::Unavailable
}

/// Read-only graph view fenced by the same committed authority as retrieval.
/// Pending effects, replaced storage and incomplete current projections fail closed.
#[cfg(all(unix, feature = "local-lancedb"))]
pub struct CommittedGraph {
    coordinator: std::sync::Arc<crate::commit::IndexCommitCoordinator>,
    guard: Option<std::fs::File>,
}
#[cfg(all(unix, feature = "local-lancedb"))]
impl CommittedGraph {
    pub fn new(coordinator: std::sync::Arc<crate::commit::IndexCommitCoordinator>) -> Self {
        Self {
            coordinator,
            guard: None,
        }
    }
    const MAX_SOURCE_PROJECTION_EDGES: usize = 256;

    fn read(
        &self,
        page: &str,
        incoming: bool,
        limit: Option<usize>,
    ) -> Result<Vec<GraphEdge>, SyncStateError> {
        let _guard = if self.guard.is_none() {
            Some(
                self.coordinator
                    .search_guard()
                    .map_err(|_| SyncStateError::Unavailable)?,
            )
        } else {
            None
        };
        // Reject degree overflow before any per-source commit validation.
        let state = self.coordinator.state();
        let edges = match (incoming, limit) {
            (true, Some(limit)) => state.edges_to_bounded(page, limit)?,
            (false, Some(limit)) => state.edges_from_bounded(page, limit)?,
            (true, None) => state.edges_to(page)?,
            (false, None) => state.edges_from(page)?,
        };
        if self
            .coordinator
            .owns_page_work(page)
            .map_err(|_| SyncStateError::Unavailable)?
            && !self
                .coordinator
                .current_body_links_complete_bounded(page, Self::MAX_SOURCE_PROJECTION_EDGES)
                .map_err(|_| SyncStateError::Unavailable)?
        {
            return Err(SyncStateError::Unavailable);
        }
        for source in edges
            .iter()
            .map(GraphEdge::source_page_id)
            .collect::<std::collections::BTreeSet<_>>()
        {
            if !self
                .coordinator
                .current_body_links_complete_bounded(source, Self::MAX_SOURCE_PROJECTION_EDGES)
                .map_err(|_| SyncStateError::Unavailable)?
            {
                return Err(SyncStateError::Unavailable);
            }
        }
        Ok(edges)
    }
}
#[cfg(all(unix, feature = "local-lancedb"))]
impl GraphEdgeStore for CommittedGraph {
    fn read_snapshot(&self) -> Result<Option<Box<dyn GraphEdgeStore>>, SyncStateError> {
        let guard = self
            .coordinator
            .search_guard()
            .map_err(|_| SyncStateError::Unavailable)?;
        Ok(Some(Box::new(Self {
            coordinator: self.coordinator.clone(),
            guard: Some(guard),
        })))
    }
    fn replace_page_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::InvalidInput)
    }
    fn replace_page_relation_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::InvalidInput)
    }
    fn replace_page_link_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::InvalidInput)
    }
    fn edges_from(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read(page, false, None)
    }
    fn edges_to(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read(page, true, None)
    }
    fn edges_from_bounded(
        &self,
        page: &str,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read(page, false, Some(limit))
    }
    fn edges_to_bounded(&self, page: &str, limit: usize) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.read(page, true, Some(limit))
    }
}

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn row_consumption_stops_at_one_overflow_sentinel() {
        for limit in [0, 1, 255, 256] {
            let consumed = Cell::new(0);
            let rows = (0..10000).map(|value| {
                consumed.set(consumed.get() + 1);
                Ok(value)
            });
            assert_eq!(
                consume_rows(rows, Some(limit)),
                Err(SyncStateError::Unavailable)
            );
            assert_eq!(consumed.get(), limit + 1);
            assert_eq!(
                consume_rows((0..limit).map(Ok), Some(limit)).unwrap().len(),
                limit
            );
        }
    }
}
