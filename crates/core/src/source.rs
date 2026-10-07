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

/// Only called after the indexed adapter output has passed identity/root checks.
/// Reads cannot mutate the disposable index: this capability exposes no writes.
pub async fn refresh_sources(
    sources: &mut [ExpandedSource],
    backend: &dyn crate::backend::NotionRead,
    max_chars: usize,
) -> Result<(), FreshSourceError> {
    use crate::backend::{BackendErrorKind, PageContent, PageId};
    use std::collections::HashMap;

    let failure = |error: crate::backend::BackendError| match error.kind {
        BackendErrorKind::NotFound | BackendErrorKind::PermissionDenied => {
            FreshSourceError::Inaccessible
        }
        BackendErrorKind::Conflict => FreshSourceError::Conflict,
        _ => FreshSourceError::Unavailable,
    };
    let mut pages = HashMap::<String, PageContent>::new();
    // First verify all pages. A failure never releases partial/stale content.
    for source in sources.iter() {
        let id = &source.provenance.page_id;
        if pages.contains_key(id) {
            continue;
        }
        let page_id = PageId(id.clone());
        let content = backend.read_content(&page_id).await.map_err(failure)?;
        if content.page.id != page_id
            || content.page.url.is_empty()
            || content.page.last_edited_time.is_empty()
        {
            return Err(FreshSourceError::Unavailable);
        }
        if content.page.archived {
            return Err(FreshSourceError::Inaccessible);
        }
        // The Notion content adapter obtains metadata before Markdown. Re-read
        // after it to catch edits/archive changes during those requests.
        let after = backend.fetch_page(&page_id).await.map_err(failure)?;
        if after != content.page {
            return Err(FreshSourceError::Conflict);
        }
        pages.insert(id.clone(), content);
    }
    let mut remaining = max_chars;
    for source in sources {
        let content = &pages[&source.provenance.page_id];
        let text: String = content.markdown.chars().take(remaining).collect();
        source.truncated = content.markdown.chars().count() > remaining;
        remaining = remaining.saturating_sub(text.chars().count());
        source.text = text;
        source.content_scope = SourceContentScope::Page;
        let provenance = &mut source.provenance;
        provenance.refreshed_last_edited_time = Some(content.page.last_edited_time.clone());
        provenance.index_stale =
            Some(provenance.indexed_last_edited_time != content.page.last_edited_time);
        provenance.url = content.page.url.clone();
        provenance.title = content.page.title.clone();
        // Chunk IDs retain the indexed anchor lineage; no claim that those
        // sections/block IDs still correspond to the refreshed page is made.
        provenance.heading_path.clear();
        provenance.block_id = None;
    }
    Ok(())
}
