//! Scope-aware derivation of Notion relation-property graph edges (#61).
//! A link or relation cannot authorize its own destination page.

use std::collections::BTreeSet;

use crate::{
    graph::{GraphEdge, GraphTarget},
    indexed::{IndexedMetadata, PropertyValue},
    sync_state::{SyncStateError, validate_identifier},
};

/// Derive one edge per distinct (stable relation property ID, target page ID).
///
/// `discovered_page_ids` must originate in a *completed, authoritative* crawl
/// under the already authorized root and exclusion policy. Never pass IDs
/// extracted from links/relations as the authority set. Out-of-scope references
/// are retained as unresolved metadata only; no target content is fetched.
pub fn relation_edges(
    metadata: &IndexedMetadata,
    discovered_page_ids: &BTreeSet<String>,
) -> Result<Vec<GraphEdge>, SyncStateError> {
    for id in [
        metadata.page_id.as_str(),
        metadata.source.workspace_id.as_str(),
        metadata.source.root_page_id.as_str(),
    ] {
        validate_identifier(id)?;
    }
    if !discovered_page_ids.contains(&metadata.page_id) {
        return Err(SyncStateError::InvalidInput);
    }

    let mut distinct = BTreeSet::new();
    for (property_id, value) in &metadata.properties {
        let PropertyValue::PageIds(targets) = value else {
            continue;
        };
        validate_identifier(property_id)?;
        if property_id.len() > 128 {
            return Err(SyncStateError::InvalidInput);
        }
        for target in targets {
            validate_identifier(target)?;
            if target.len() > 128 {
                return Err(SyncStateError::InvalidInput);
            }
            distinct.insert((property_id.clone(), target.clone()));
            if distinct.len() > 10_000 {
                return Err(SyncStateError::InvalidInput);
            }
        }
    }
    distinct
        .into_iter()
        .map(|(property_id, target_id)| {
            let target = if discovered_page_ids.contains(&target_id) {
                GraphTarget::Page { page_id: target_id }
            } else {
                GraphTarget::Unresolved {
                    reference: target_id,
                }
            };
            GraphEdge::new(
                metadata.page_id.clone(),
                target,
                format!("relation:{property_id}"),
                format!("property:{property_id}"),
            )
        })
        .collect()
}
