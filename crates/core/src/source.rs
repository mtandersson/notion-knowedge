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
    /// Notion edit timestamp persisted with the resolved indexed source.
    pub indexed_last_edited_time: String,
    /// Present only after a successful authoritative read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refreshed_last_edited_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_stale: Option<bool>,
}

/// Expanded source content returned in the same order as requested references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExpandedSource {
    pub reference: StableSourceRef,
    pub text: String,
    pub truncated: bool,
    pub provenance: SourceProvenance,
    /// Fresh reads always return whole-page content; indexed reads expand the requested scope.
    pub content_scope: SourceContentScope,
}

/// Scope of returned text, independently of the indexed anchor reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceContentScope {
    Indexed,
    Page,
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

pub type SourceExpansionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ExpandedSource>, SourceExpansionError>> + Send + 'a>>;

/// Adapter boundary for expanding stable page/chunk references without rerunning
/// broad retrieval. Implementations must honor `root_page_ids` before returning
/// content.
pub trait SourceExpansion: Send + Sync {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_>;
}

/// Sanitized authoritative read failures; MCP maps these to public tool errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshSourceError {
    Inaccessible,
    Unavailable,
    Conflict,
}

/// Read one whole Notion page and verify that its metadata stayed consistent.
pub async fn read_verified_page(
    id: &crate::backend::PageId,
    backend: &dyn crate::backend::NotionRead,
) -> Result<crate::backend::PageContent, FreshSourceError> {
    use crate::backend::BackendErrorKind;

    let failure = |error: crate::backend::BackendError| match error.kind {
        BackendErrorKind::NotFound | BackendErrorKind::PermissionDenied => {
            FreshSourceError::Inaccessible
        }
        BackendErrorKind::Conflict => FreshSourceError::Conflict,
        _ => FreshSourceError::Unavailable,
    };
    let content = backend.read_content(id).await.map_err(failure)?;
    if &content.page.id != id
        || content.page.url.is_empty()
        || content.page.last_edited_time.is_empty()
    {
        return Err(FreshSourceError::Unavailable);
    }
    if content.page.archived {
        return Err(FreshSourceError::Inaccessible);
    }
    // The content adapter obtains metadata before Markdown. Check it again
    // to detect edits or revocation during the read.
    if backend.fetch_page(id).await.map_err(failure)? != content.page {
        return Err(FreshSourceError::Conflict);
    }
    Ok(content)
}

/// A local page's published source identity and the change signals it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshPageStamp {
    pub page_id: crate::backend::PageId,
    pub root_page_id: String,
    pub revision: String,
    pub parent: String,
    pub watermark: i64,
    pub publication: String,
}

pub struct CachedPage {
    pub content: crate::backend::PageContent,
    pub stamp: FreshPageStamp,
}

pub type CachedPageFuture<'a> = Pin<Box<dyn Future<Output = Option<CachedPage>> + Send + 'a>>;

/// Local, read-only whole-page source cache. A missing, stale or unverifiable
/// row is a miss. Eligibility does not depend on the embedding projection.
pub trait FreshPageCache: Send + Sync {
    fn lookup<'a>(&'a self, page: &'a crate::backend::PageId) -> CachedPageFuture<'a>;
    fn is_current(&self, stamp: &FreshPageStamp) -> bool;
}
