//! Scoped Markdown-page and enhanced Notion mention graph projection (#62).
//!
//! References are parsed by the existing lossless Notion extractor. Only a
//! completed, explicitly authorized discovery inventory may resolve a target.
use std::collections::BTreeSet;

use notion_knowledge_core::{
    backend::PageContent,
    graph::{GraphEdge, GraphTarget},
    indexed::LinkTarget,
    sync_state::{SyncStateError, validate_identifier},
};
use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::links::{ReferenceSource, extract_relationships};

const MAX_EDGES_PER_PAGE: usize = 10_000;

/// Recognize ONLY Notion-owned hosts. Never treat deceptive suffixes, external
/// links or plain strings as internal page references.
fn notion_host(raw: &str) -> bool {
    let Ok(url) = Url::parse(raw) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    host == "notion.so"
        || host.ends_with(".notion.so")
        || host == "notion.site"
        || host.ends_with(".notion.site")
        || host == "app.notion.com"
}

/// Return canonical page edges and opaque diagnostics without fetching any
/// target. Duplicate links to the same kind/page/block are collapsed to the
/// earliest source byte offset. External links are not graph nodes.
///
/// `discovered_page_ids` MUST come from a successful scoped crawl, never
/// from the content being parsed. Unlisted/excluded targets stay unresolved.
pub fn page_link_edges(
    content: &PageContent,
    discovered_page_ids: &BTreeSet<String>,
) -> Result<Vec<GraphEdge>, SyncStateError> {
    let source = &content.page.id.0;
    validate_identifier(source)?;
    if !discovered_page_ids.contains(source) {
        return Err(SyncStateError::InvalidInput);
    }

    let mut seen = BTreeSet::new();
    let mut edges = Vec::new();
    for reference in extract_relationships(content).references {
        let ReferenceSource::Markdown { bytes, kind } = reference.source else {
            // Relation property edges belong exclusively to #61.
            continue;
        };
        let mention = kind == "page" || kind == "mention-page";
        let (relation_type, target, identity) = match reference.target {
            Some(LinkTarget::Page { page_id }) => {
                let target = if discovered_page_ids.contains(&page_id) {
                    GraphTarget::Page {
                        page_id: page_id.clone(),
                    }
                } else {
                    GraphTarget::Unresolved {
                        reference: page_id.clone(),
                    }
                };
                ("link:page", target, format!("page:{page_id}"))
            }
            Some(LinkTarget::Block { page_id, block_id }) => {
                let target = if discovered_page_ids.contains(&page_id) {
                    GraphTarget::Page {
                        page_id: page_id.clone(),
                    }
                } else {
                    GraphTarget::Unresolved {
                        reference: page_id.clone(),
                    }
                };
                ("link:block", target, format!("block:{page_id}:{block_id}"))
            }
            Some(LinkTarget::External { .. }) => continue,
            None if mention || (kind == "link" && notion_host(&reference.raw_target)) => {
                // Keep malformed internal targets diagnosable, but never store
                // raw URL query parameters, tokens, or malformed tag contents.
                let hash = Sha256::digest(reference.raw_target.as_bytes());
                let identifier = format!("invalid-page-link:{hash:x}");
                (
                    "link:invalid",
                    GraphTarget::Unresolved {
                        reference: identifier.clone(),
                    },
                    identifier,
                )
            }
            None => continue,
        };
        // The semantic key is stable across reordered duplicates. The first
        // occurrence retains a byte-offset provenance for local diagnosis.
        if !seen.insert((relation_type, identity)) {
            continue;
        }
        if edges.len() >= MAX_EDGES_PER_PAGE {
            return Err(SyncStateError::InvalidInput);
        }
        edges.push(GraphEdge::new(
            source.clone(),
            target,
            relation_type.into(),
            format!("markdown:{}", bytes.start),
        )?);
    }
    Ok(edges)
}
