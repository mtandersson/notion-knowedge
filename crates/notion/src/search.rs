//! Search metadata for a walk: one paged listing of every page the
//! integration can see, used only to skip per-page metadata reads. Search never
//! decides scope: a listed page the walk does not reach is ignored, and the
//! walk falls back to reading a page when its listing is missing or disagrees.
use crate::{NotionClient, pages::page_id};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use reqwest::Method;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const MAX_SEARCH_PAGES: usize = 10_000;

/// Page id to the page object search returned for it.
pub type SearchMetadata = BTreeMap<String, Value>;

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
            for page in results.iter().filter(|page| page["object"] == "page") {
                if let Some(id) = page["id"].as_str().and_then(|id| page_id(id).ok()) {
                    pages.insert(id.0, page.clone());
                }
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
