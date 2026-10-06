//! Provider-independent source expansion port for stable indexed references.

use serde::Serialize;
use std::{future::Future, pin::Pin};

/// Stable reference emitted by retrieval results and accepted by source expansion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum StableSourceRef {
    Page(String),
    Chunk(String),
}

/// Provenance for expanded source text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceProvenance {
    pub page_id: String,
    pub root_page_id: String,
    pub url: String,
    pub title: String,
    pub heading_path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    pub chunk_ids: Vec<String>,
}

/// Expanded source content returned in the same order as requested references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExpandedSource {
    pub reference: StableSourceRef,
    pub text: String,
    pub truncated: bool,
    pub provenance: SourceProvenance,
}

/// Expansion request. Root scope is supplied by the trusted server composition,
/// never by MCP callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceExpandQuery {
    pub refs: Vec<StableSourceRef>,
    pub max_chars: usize,
    pub root_page_ids: Vec<String>,
}

/// Missing and out-of-scope are intentionally distinct inside the application so
/// adapters can be tested, while the MCP boundary may collapse them to one safe
/// public error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceExpansionError {
    Missing,
    OutOfScope,
    Unavailable,
}

pub type SourceExpansionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Vec<ExpandedSource>, SourceExpansionError>>
            + Send
            + 'a,
    >,
>;

/// Adapter boundary for expanding stable page/chunk references without rerunning
/// broad retrieval. Implementations must honor `root_page_ids` before returning
/// content.
pub trait SourceExpansion: Send + Sync {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_>;
}
