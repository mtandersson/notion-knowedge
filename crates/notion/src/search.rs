//! Search metadata for a walk: one paged listing of every page the
//! integration can see, used only to skip per-page metadata reads. Search never
//! decides scope: a listed page the walk does not reach is ignored, and the
//! walk falls back to reading a page when its listing is missing or disagrees.
use crate::{
    NotionClient,
    pages::{metadata, page_id},
};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind, Page, PageId};
use reqwest::Method;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const MAX_SEARCH_PAGES: usize = 10_000;
/// Listed pages kept per walk. The walk itself reaches at most 100,000
/// nodes; past this many listings the remaining pages are read directly, so
/// a workspace much larger than the configured roots costs memory only up
/// to this bound.
pub const MAX_SEARCH_ENTRIES: usize = 20_000;

/// One complete listing: parsed metadata and its direct page parent, if any.
#[derive(Debug, Clone, PartialEq)]
pub struct ListedPage {
    pub page: Page,
    /// `None` unless the physical parent is a page (block, data source and
    /// workspace parents always fall back to a direct read).
    pub parent_page_id: Option<String>,
}

/// Page id to its complete search listing. Partial listings (missing fields,
/// truncated properties) are left out, so those pages are read directly.
pub type SearchMetadata = BTreeMap<String, ListedPage>;

fn listed(value: &Value) -> Option<(String, ListedPage)> {
    let id = page_id(value["id"].as_str()?).ok()?;
    let page = metadata(value, &id).ok()?;
    let parent_page_id = (value["parent"]["type"] == "page_id")
        .then(|| page_id(value["parent"]["page_id"].as_str()?).ok())
        .flatten()
        .map(|PageId(id)| id);
    Some((
        id.0,
        ListedPage {
            page,
            parent_page_id,
        },
    ))
}

impl NotionClient {
    /// Every page search lists, oldest edit first, 100 per request. A
    /// repeated or missing cursor ends the listing early; the walk then reads
    /// the unlisted pages itself, so a short listing costs calls, not scope.
    pub async fn search_metadata(&self) -> Result<SearchMetadata, BackendError> {
        self.search_pages(&mut 0).await
    }

    pub(crate) async fn search_pages(
        &self,
        calls: &mut u64,
    ) -> Result<SearchMetadata, BackendError> {
        let mut pages = SearchMetadata::new();
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        for _ in 0..MAX_SEARCH_PAGES {
            let mut body = json!({
                "filter": {"property": "object", "value": "page"},
                "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
                "page_size": 100,
            });
            if let Some(cursor) = &cursor {
                body["start_cursor"] = json!(cursor);
            }
            *calls += 1;
            let value = self
                .walk_request(Method::POST, "search", Some(&body))
                .await?;
            let results = value["results"]
                .as_array()
                .filter(|_| value["object"] == "list")
                .ok_or(BackendError {
                    kind: BackendErrorKind::Internal,
                    operation: "notion.search",
                    retry_after: None,
                    committed_page_id: None,
                })?;
            for (id, page) in results
                .iter()
                .filter(|page| page["object"] == "page")
                .filter_map(listed)
            {
                if pages.len() >= MAX_SEARCH_ENTRIES {
                    return Ok(pages);
                }
                pages.insert(id, page);
            }
            match value["next_cursor"].as_str() {
                Some(next)
                    if value["has_more"] == true
                        && !next.is_empty()
                        && cursors.insert(next.to_owned()) =>
                {
                    cursor = Some(next.to_owned());
                }
                _ => return Ok(pages),
            }
        }
        Ok(pages)
    }
}
