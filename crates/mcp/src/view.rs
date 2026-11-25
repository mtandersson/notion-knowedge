//! The installed read-only Notion saved-view contract.
use std::{future::Future, pin::Pin};

use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use rmcp::model::{Tool, ToolAnnotations};
use serde::Deserialize;
use serde_json::{Value, json};

pub const TOOL: &str = "knowledge_query_view";
pub const MAX_PAGE_SIZE: usize = 25;
pub const MAX_CURSOR_BYTES: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub view_id: String,
    pub page_size: Option<usize>,
    pub continuation: Option<Continuation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    pub query_id: String,
    pub cursor: String,
    pub view_revision: String,
}

impl Request {
    pub(crate) fn valid(&self) -> bool {
        !self.view_id.is_empty()
            && self.view_id.len() <= 43
            && self
                .page_size
                .is_none_or(|size| (1..=MAX_PAGE_SIZE).contains(&size))
            && self.continuation.as_ref().is_none_or(|next| {
                !next.query_id.is_empty()
                    && next.query_id.len() <= 36
                    && !next.cursor.is_empty()
                    && next.cursor.len() <= MAX_CURSOR_BYTES
                    && !next.cursor.chars().any(char::is_control)
                    && (1..=64).contains(&next.view_revision.len())
            })
    }
}

pub trait SavedViewQuery: Send + Sync {
    fn query(
        &self,
        input: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Value, BackendError>> + Send + '_>>;
}

pub(crate) fn tool() -> Tool {
    Tool::new(
        TOOL,
        "Read ordered rows from a saved Notion view using its current filters and sorts. Pass the returned continuation unchanged with the same view ID for subsequent pages.",
        json!({"type":"object","additionalProperties":false,"required":["view_id"],"properties":{
            "view_id":{"type":"string","minLength":32,"maxLength":43,"description":"View UUID or view:// followed by the UUID."},
            "page_size":{"type":"integer","minimum":1,"maximum":MAX_PAGE_SIZE,"default":10},
            "continuation":{"type":"object","additionalProperties":false,"required":["query_id","cursor","view_revision"],"properties":{
                "query_id":{"type":"string","minLength":32,"maxLength":36},
                "cursor":{"type":"string","minLength":1,"maxLength":MAX_CURSOR_BYTES},
                "view_revision":{"type":"string","minLength":1,"maxLength":64}
            }}
        }}).as_object().unwrap().clone(),
    )
    .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(true))
}

pub(crate) fn failure(kind: BackendErrorKind) -> &'static str {
    match kind {
        BackendErrorKind::InvalidInput => {
            "invalid_input: expected a Notion view UUID and a valid continuation"
        }
        BackendErrorKind::Unauthenticated
        | BackendErrorKind::PermissionDenied
        | BackendErrorKind::NotFound => {
            "source_not_accessible: view or source is missing or outside configured scope"
        }
        BackendErrorKind::UnsupportedContent => {
            "unsupported_view: Notion cannot query this saved view"
        }
        BackendErrorKind::Conflict => {
            "view_query_expired_or_changed: start again without a continuation"
        }
        BackendErrorKind::RateLimited => "notion_rate_limited: retry the view query later",
        BackendErrorKind::Unavailable | BackendErrorKind::Internal => {
            "notion_unavailable: no authoritative view page was returned"
        }
    }
}
