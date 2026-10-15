//! Scope-aware derivation of Notion relation-property graph edges (#61).
//! A link or relation cannot authorize its own destination page.

use std::collections::{BTreeMap, BTreeSet};

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
    relation_edges_with_names(metadata, &BTreeMap::new(), discovered_page_ids)
}

/// Project fresh authoritative names alongside canonical membership metadata.
/// The sidecar is keyed by stable property ID and does not change indexed
/// document/chunk schemas. Callers must obtain both inputs from the same
/// verified authoritative page; names never authorize destination pages.
pub fn relation_edges_with_names(
    metadata: &IndexedMetadata,
    relation_names: &BTreeMap<String, String>,
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
        let name = relation_names.get(property_id);
        if name.is_some_and(|name| name.chars().count() > 4096) {
            return Err(SyncStateError::InvalidInput);
        }
        validate_identifier(property_id)?;
        if property_id.len() > 128 {
            return Err(SyncStateError::InvalidInput);
        }
        for target in targets {
            validate_identifier(target)?;
            if target.len() > 128 {
                return Err(SyncStateError::InvalidInput);
            }
            distinct.insert((property_id.clone(), target.clone(), name.cloned()));
            if distinct.len() > 10_000 {
                return Err(SyncStateError::InvalidInput);
            }
        }
    }
    distinct
        .into_iter()
        .map(|(property_id, target_id, name)| {
            let target = if discovered_page_ids.contains(&target_id) {
                GraphTarget::Page { page_id: target_id }
            } else {
                GraphTarget::Unresolved {
                    reference: target_id,
                }
            };
            let edge = GraphEdge::new(
                metadata.page_id.clone(),
                target,
                format!("relation:{property_id}"),
                format!("property:{property_id}"),
            )?;
            match name {
                Some(name) => edge.with_display_name(name),
                None => Ok(edge),
            }
        })
        .collect()
}
