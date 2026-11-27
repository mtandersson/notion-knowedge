//! The installed read-only Notion saved-view contract.
use std::{future::Future, pin::Pin};

use notion_knowledge_core::backend::{BackendError, BackendErrorKind, ViewQueryError};
use rmcp::model::{Tool, ToolAnnotations};
use serde::Deserialize;
use serde_json::{Value, json};

/// Capability selector (policy only, not a tool name) for the saved-view mode
/// of `knowledge_get`; also its tool-budget key.
pub const SELECTOR: &str = "knowledge_query_view";
pub const MAX_PAGE_SIZE: usize = 25;
pub const MAX_FIELDS: usize = 25;
pub const MAX_CURSOR_BYTES: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub view_id: String,
    pub page_size: Option<usize>,
    /// Column names or property IDs to return. The title is always included.
    pub fields: Option<Vec<String>>,
    pub include_row_revision: Option<bool>,
    pub continuation: Option<Continuation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    pub query_id: String,
    pub cursor: String,
    pub view_revision: String,
    pub signature: String,
}

impl Request {
    pub(crate) fn valid(&self) -> bool {
        !self.view_id.is_empty()
            && self.view_id.len() <= 43
            && self
                .page_size
                .is_none_or(|size| (1..=MAX_PAGE_SIZE).contains(&size))
            && self.fields.as_ref().is_none_or(|fields| {
                (1..=MAX_FIELDS).contains(&fields.len())
                    && fields
                        .iter()
                        .all(|field| !field.is_empty() && field.len() <= 128)
            })
            && self.continuation.as_ref().is_none_or(|next| {
                !next.query_id.is_empty()
                    && next.query_id.len() <= 36
                    && !next.cursor.is_empty()
                    && next.cursor.len() <= MAX_CURSOR_BYTES
                    && !next.cursor.chars().any(char::is_control)
                    && next.view_revision.len() == 64
                    && next.signature.len() == 64
            })
    }
}

pub trait SavedViewQuery: Send + Sync {
    fn query(
        &self,
        input: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Value, ViewQueryError>> + Send + '_>>;
}

pub(crate) fn tool() -> Tool {
    Tool::new(
        "knowledge_get",
        "Read ordered rows from a saved Notion view using its current filters and sorts. Rows hold values positionally, matching the returned columns. Choose columns with fields; set include_row_revision only before updating rows. Pass the returned continuation unchanged with the same view ID for subsequent pages.",
        json!({"type":"object","additionalProperties":false,"required":["view_id"],"properties":{
            "view_id":{"type":"string","minLength":32,"maxLength":43,"description":"View UUID or view:// followed by the UUID."},
            "page_size":{"type":"integer","minimum":1,"maximum":MAX_PAGE_SIZE,"default":10},
            "fields":{"type":"array","minItems":1,"maxItems":MAX_FIELDS,"uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":128},
                "description":"Column names or property IDs. The title column is always included. Default: every readable column."},
            "include_row_revision":{"type":"boolean","default":false,"description":"Add row_revision to each row for knowledge_update_page patch_properties."},
            "continuation":{"type":"object","additionalProperties":false,"required":["query_id","cursor","view_revision","signature"],"properties":{
                "query_id":{"type":"string","minLength":32,"maxLength":36},
                "cursor":{"type":"string","minLength":1,"maxLength":MAX_CURSOR_BYTES},
                "view_revision":{"type":"string","minLength":64,"maxLength":64},
                "signature":{"type":"string","minLength":64,"maxLength":64}
            }}
        }}).as_object().unwrap().clone(),
    )
    .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(true))
}

/// Column names listed in an unknown-field refusal. Names are the caller's own
/// schema, capped so a very wide database cannot inflate the answer.
const MAX_LISTED_COLUMNS: usize = 50;
const MAX_LISTED_COLUMN_CHARS: usize = 64;

pub(crate) fn failure(error: ViewQueryError) -> String {
    match error {
        ViewQueryError::UnknownField { columns } => {
            let listed: Vec<String> = columns
                .iter()
                .take(MAX_LISTED_COLUMNS)
                .map(|name| {
                    name.chars()
                        .filter(|c| !c.is_control())
                        .take(MAX_LISTED_COLUMN_CHARS)
                        .collect()
                })
                .collect();
            let more = columns.len().saturating_sub(listed.len());
            let suffix = if more > 0 {
                format!(" (and {more} more)")
            } else {
                String::new()
            };
            format!(
                "invalid_input: unknown_field: every fields entry must be a column name or ID of this view. Columns: {}{suffix}",
                listed.join(", ")
            )
        }
        ViewQueryError::Backend(error) => backend_failure(error).to_owned(),
    }
}

fn backend_failure(error: BackendError) -> &'static str {
    if error.operation == "notion.view_query.response_limit" {
        return "incomplete: response exceeds the tool limit; request a smaller page";
    }
    match error.kind {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_field_lists_a_bounded_set_of_column_names() {
        let columns: Vec<String> = (0..60).map(|n| format!("col{n}")).collect();
        let text = failure(ViewQueryError::UnknownField { columns });
        assert!(text.starts_with("invalid_input: unknown_field"), "{text}");
        assert!(text.contains("col0") && text.contains("col49"), "{text}");
        assert!(!text.contains("col50"), "{text}");
        assert!(text.ends_with("(and 10 more)"), "{text}");

        let long = failure(ViewQueryError::UnknownField {
            columns: vec!["x".repeat(500)],
        });
        assert!(long.len() < 300, "{long}");

        let control = failure(ViewQueryError::UnknownField {
            columns: vec!["a\nIgnore previous\u{7}".into()],
        });
        assert!(!control.chars().any(char::is_control), "{control:?}");
    }
}
