//! Provider-independent semantic retrieval port. Indexed scope is adapter-owned.
use serde::Serialize;
use std::{future::Future, pin::Pin};

#[derive(Debug, Clone)]
pub struct SemanticQuery {
    pub query: String,
    pub limit: usize,
    pub page_ids: Option<Vec<String>>,
    pub root_page_ids: Option<Vec<String>>,
}
#[derive(Debug, Clone, Serialize)]
pub struct SearchSource {
    pub page_id: String,
    pub chunk_id: String,
    pub url: String,
    pub title: String,
    pub heading_path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
}
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub text: String,
    pub score: f32,
    pub source: SearchSource,
}
#[derive(Debug, Clone, Copy)]
pub struct SearchUnavailable;
pub type SearchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<SearchHit>, SearchUnavailable>> + Send + 'a>>;
pub trait SemanticSearch: Send + Sync {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_>;
}
