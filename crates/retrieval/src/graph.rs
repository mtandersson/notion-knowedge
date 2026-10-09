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
                        relation_type, provenance
                     ) VALUES (?1, ?2, ?3, ?4, ?5)
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
                    ])
                    .map_err(sqlite_failure)?;
            }
        }

        transaction.commit().map_err(sqlite_failure)
    }

    fn edges_from(&self, source_page_id: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        validate_identifier(source_page_id)?;
        let connection = self.lock_connection()?;
        read_edges(
            &connection,
            "SELECT source_page_id, target_page_id, unresolved_reference,
                    relation_type, provenance
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
                    relation_type, provenance
             FROM graph_edges WHERE target_page_id = ?1
             ORDER BY source_page_id, relation_type, provenance",
            target_page_id,
        )
    }
}

fn read_edges(
    connection: &Connection,
    sql: &str,
    page_id: &str,
) -> Result<Vec<GraphEdge>, SyncStateError> {
    let mut statement = connection.prepare(sql).map_err(sqlite_failure)?;
    let rows = statement
        .query_map(params![page_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(sqlite_failure)?;
    let stored = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sqlite_failure)?;

    stored
        .into_iter()
        .map(|(source, page, unresolved, relation_type, provenance)| {
            let target = match (page, unresolved) {
                (Some(page_id), None) => GraphTarget::Page { page_id },
                (None, Some(reference)) => GraphTarget::Unresolved { reference },
                _ => return Err(SyncStateError::CorruptState),
            };
            GraphEdge::new(source, target, relation_type, provenance)
                .map_err(|_| SyncStateError::CorruptState)
        })
        .collect()
}

fn sqlite_failure(_: rusqlite::Error) -> SyncStateError {
    // Do not leak local paths, page IDs, source content, or raw SQL errors.
    SyncStateError::Unavailable
}
