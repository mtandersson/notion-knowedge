//! Bounded, read-only graph navigation from an explicitly authorized page.
//! Links are metadata, not authority: every discovered page is checked against
//! its current physical Notion ancestry before it can appear or be traversed.

use notion_knowledge_core::{
    backend::PageId,
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    root_scope::{RootScopeGate, ScopeError},
};
use rmcp::model::{Tool, ToolAnnotations};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeSet, VecDeque};

const MAX_FANOUT: usize = 256;
const MAX_DEPTH: u8 = 2;
const MAX_RELATIONS: usize = 100;

fn default_depth() -> u8 {
    1
}
fn default_limit() -> usize {
    50
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelatedRequest {
    pub page_id: String,
    #[serde(default = "default_depth")]
    pub depth: u8,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

impl RelatedRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        let id = self.page_id.as_bytes();
        if id.len() != 36
            || id.iter().enumerate().any(|(i, byte)| match i {
                8 | 13 | 18 | 23 => *byte != b'-',
                _ => !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase(),
            })
        {
            return Err("page_id must be a canonical lowercase page UUID");
        }
        if !(1..=MAX_DEPTH).contains(&self.depth) {
            return Err("depth must be between 1 and 2");
        }
        if !(1..=MAX_RELATIONS).contains(&self.limit) {
            return Err("limit must be between 1 and 100");
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct RelatedEdge {
    pub source_page_id: String,
    pub target_page_id: String,
    pub relation_type: String,
    pub provenance: String,
    /// Authoritative property label; absent for legacy snapshots and links.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Distance from the requested page to this edge via authorized nodes.
    pub depth: u8,
    /// Direction relative to the node at which this edge was discovered.
    pub direction: &'static str,
}

#[derive(Debug, Serialize)]
pub struct RelatedResponse {
    pub page_id: String,
    pub relations: Vec<RelatedEdge>,
    pub limit_reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatedError {
    Inaccessible,
    Unavailable,
}

fn target_key(edge: &GraphEdge) -> &str {
    match edge.target() {
        GraphTarget::Page { page_id } => page_id,
        GraphTarget::Unresolved { reference } => reference,
    }
}

async fn authorized(
    gate: &RootScopeGate,
    id: &str,
    roots: &[String],
) -> Result<bool, RelatedError> {
    let permit = match gate.authorize(&PageId(id.to_owned())).await {
        Ok(permit) => permit,
        Err(ScopeError::Denied) => return Ok(false),
        Err(_) => return Err(RelatedError::Unavailable),
    };
    if !permit.belongs_to_any(roots) {
        return Ok(false);
    }
    gate.revalidate(&permit, gate.configured_scope())
        .await
        .map_err(|_| RelatedError::Unavailable)?;
    Ok(true)
}

/// Scope is supplied by the trusted MCP composition root, never tool JSON.
/// Unresolved edges and out-of-scope targets are neither emitted nor traversed.
/// Failures reading graph storage fail the entire request without partial data.
pub async fn navigate(
    input: &RelatedRequest,
    graph: &dyn GraphEdgeStore,
    gate: &RootScopeGate,
    roots: &[String],
) -> Result<RelatedResponse, RelatedError> {
    input.validate().map_err(|_| RelatedError::Inaccessible)?;
    if roots.is_empty() || !authorized(gate, &input.page_id, roots).await? {
        return Err(RelatedError::Inaccessible);
    }

    let mut visited = BTreeSet::from([input.page_id.clone()]);
    let mut queue = VecDeque::from([(input.page_id.clone(), 0u8)]);
    let mut distinct = BTreeSet::new();
    let mut relations = Vec::new();
    while let Some((current, hops)) = queue.pop_front() {
        if hops >= input.depth {
            continue;
        }
        let outgoing = graph
            .edges_from(&current)
            .map_err(|_| RelatedError::Unavailable)?;
        let incoming = graph
            .edges_to(&current)
            .map_err(|_| RelatedError::Unavailable)?;
        if outgoing.len().saturating_add(incoming.len()) > MAX_FANOUT {
            return Err(RelatedError::Unavailable);
        }
        let mut edges: Vec<_> = outgoing
            .into_iter()
            .map(|edge| (true, edge))
            .chain(incoming.into_iter().map(|edge| (false, edge)))
            .collect();
        edges.sort_by(|(dir_a, a), (dir_b, b)| {
            (
                a.source_page_id(),
                target_key(a),
                a.relation_type(),
                a.provenance(),
                dir_a,
            )
                .cmp(&(
                    b.source_page_id(),
                    target_key(b),
                    b.relation_type(),
                    b.provenance(),
                    dir_b,
                ))
        });

        for (outgoing, edge) in edges {
            let (neighbor, direction) = if outgoing {
                if edge.source_page_id() != current {
                    return Err(RelatedError::Unavailable);
                }
                match edge.target() {
                    GraphTarget::Page { page_id } => (page_id.as_str(), "outgoing"),
                    GraphTarget::Unresolved { .. } => continue,
                }
            } else {
                if !matches!(edge.target(), GraphTarget::Page { page_id } if page_id == &current) {
                    return Err(RelatedError::Unavailable);
                }
                (edge.source_page_id(), "incoming")
            };
            // Never use graph links or caller supplied IDs as permission proofs.
            if !authorized(gate, neighbor, roots).await? {
                continue;
            }
            let key = (
                edge.source_page_id().to_owned(),
                match edge.target() {
                    GraphTarget::Page { page_id } => page_id.clone(),
                    GraphTarget::Unresolved { .. } => return Err(RelatedError::Unavailable),
                },
                edge.relation_type().to_owned(),
                edge.provenance().to_owned(),
            );
            if !distinct.insert(key.clone()) {
                continue;
            }
            relations.push(RelatedEdge {
                source_page_id: key.0,
                target_page_id: key.1,
                relation_type: key.2,
                provenance: key.3,
                display_name: edge.display_name().map(str::to_owned),
                depth: hops + 1,
                direction,
            });
            if relations.len() == input.limit {
                return Ok(RelatedResponse {
                    page_id: input.page_id.clone(),
                    relations,
                    limit_reached: true,
                });
            }
            if hops + 1 < input.depth && visited.insert(neighbor.to_owned()) {
                queue.push_back((neighbor.to_owned(), hops + 1));
            }
        }
    }
    Ok(RelatedResponse {
        page_id: input.page_id.clone(),
        relations,
        limit_reached: false,
    })
}

pub fn tool() -> Tool {
    let input = json!({
        "type":"object",
        "additionalProperties":false,
        "required":["page_id"],
        "properties":{
            "page_id":{"type":"string","description":"Canonical Notion page UUID from an authorized search/get result.","minLength":36,"maxLength":36},
            "depth":{"type":"integer","minimum":1,"maximum":2,"default":1},
            "limit":{"type":"integer","minimum":1,"maximum":100,"default":50}
        }
    });
    let output = json!({
        "type":"object",
        "additionalProperties":false,
        "required":["page_id","relations","limit_reached"],
        "properties":{
            "page_id":{"type":"string"},
            "limit_reached":{"type":"boolean"},
            "relations":{"type":"array","maxItems":100,"items":{
                "type":"object","additionalProperties":false,
                "required":["source_page_id","target_page_id","relation_type","provenance","depth","direction"],
                "properties":{
                    "source_page_id":{"type":"string"},
                    "target_page_id":{"type":"string"},
                    "relation_type":{"type":"string"},
                    "provenance":{"type":"string"},
                    "depth":{"type":"integer","minimum":1,"maximum":2},
                    "direction":{"type":"string","enum":["incoming","outgoing"]}
                }
            }}
        }
    });
    Tool::new(
        "knowledge_related",
        "Navigate local, indexed Notion relations around an authorized canonical page ID. Depth is 1 by default and limited to 2; results include relation type, direction and stable provenance. Every resolved page is freshly checked against server-controlled physical root scope. Unresolved and inaccessible targets are never returned. No Notion content is fetched or changed.",
        input.as_object().unwrap().clone(),
    )
    .with_raw_output_schema(std::sync::Arc::new(output.as_object().unwrap().clone()))
    .with_annotations(
        ToolAnnotations::new()
            .read_only(true)
            .destructive(false)
            .open_world(false),
    )
}
