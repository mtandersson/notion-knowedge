//! Provider-neutral, once-only page creation for semantic mutation dispatch.
//! Each preflight/read/effect uses an injected authoritative port.
use crate::{
    backend::{CreatePage, NotionRead, NotionWrite, PageId},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore,
    },
    lifecycle::LifecycleScope,
    write_refresh::{MutationRefreshQueue, RefreshKey, WriteCompletion, admit_verified},
};

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub parent_page_id: PageId,
    pub title: String,
    pub markdown: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateError {
    InvalidInput,
    NotAuthorized,
    IdempotencyConflict,
    Unavailable,
    /// A source effect might exist. Reconcile; never repeat the POST.
    OutcomeUnknown,
}
impl std::fmt::Display for CreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "guarded create failed: {self:?}")
    }
}
impl std::error::Error for CreateError {}
/// An unknown outcome is deliberately reported to the caller without detail;
/// the operator log names the stage that could not prove the result.
fn unknown(stage: &'static str, detail: &'static str) -> CreateError {
    crate::logging::emit_write_outcome_unknown(crate::logging::Outcome::Failed, stage, detail);
    CreateError::OutcomeUnknown
}

/// The first readback check that does not prove the create, as a fixed label.
/// This is the single decision point, so the label cannot drift from the check.
fn readback_mismatch(
    created: &crate::backend::Page,
    content: &crate::backend::PageContent,
    request: &CreateRequest,
) -> Option<&'static str> {
    if content.page.id != created.id {
        Some("page_id")
    } else if content.page.archived {
        Some("archived")
    } else if content.page.url != created.url {
        Some("url")
    } else if content.page.title != request.title {
        Some("title")
    } else if !crate::revision::written_markdown_matches(&request.markdown, &content.markdown) {
        Some("markdown")
    } else {
        None
    }
}

fn ledger_error(e: IdempotencyError) -> CreateError {
    match e {
        IdempotencyError::InvalidInput => CreateError::InvalidInput,
        IdempotencyError::KeyConflict | IdempotencyError::AlreadyCommitted => {
            CreateError::IdempotencyConflict
        }
        _ => CreateError::Unavailable,
    }
}
pub fn valid_request(request: &CreateRequest) -> bool {
    valid_contents(&request.title, &request.markdown)
}

pub fn valid_contents(title: &str, markdown: &str) -> bool {
    !title.trim().is_empty()
        && title.chars().count() <= 200
        && title.chars().all(|c| !c.is_control() && !bidi(c))
        && !markdown.trim().is_empty()
        && markdown.len() <= 200_000
        && markdown
            .chars()
            .all(|c| (!c.is_control() || matches!(c, '\n' | '\r' | '\t')) && !bidi(c))
}
fn bidi(c: char) -> bool {
    matches!(c,'\u{200e}'|'\u{200f}'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')
}

/// Reserve -> mark uncertain -> single create -> complete exact readback ->
/// verified ledger commit -> independent refresh admission. No automatic mutation retry.
pub async fn create_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    scope: &LifecycleScope,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn MutationRefreshQueue,
    request: CreateRequest,
) -> Result<WriteCompletion, CreateError> {
    if !valid_request(&request) {
        return Err(CreateError::InvalidInput);
    }
    let payload = serde_json::to_vec(
        &serde_json::json!({"title":request.title,"markdown":request.markdown,"icon":null}),
    )
    .map_err(|_| CreateError::InvalidInput)?;
    let claim = MutationClaim::new(
        &scope.workspace_id,
        &request.idempotency_key,
        MutationOperation::CreatePage,
        &request.parent_page_id.0,
        &MutationClaim::payload_sha256(&payload),
    )
    .map_err(ledger_error)?;
    let (receipt, replayed) = match ledger.begin(&claim).map_err(ledger_error)? {
        Reservation::Replay(receipt) => (receipt, true),
        Reservation::Reconcile => return Err(unknown("create.reconcile_required", "")),
        Reservation::ExecuteOnce => {
            ledger.mark_uncertain(&claim).map_err(ledger_error)?;
            let created = writer
                .create_page(CreatePage {
                    parent_page_id: request.parent_page_id.clone(),
                    title: request.title.clone(),
                    markdown: request.markdown.clone(),
                })
                .await
                .map_err(|error| unknown("create.post", error.kind.label()))?;
            if created.archived || created.title != request.title {
                return Err(unknown("create.receipt_mismatch", ""));
            }
            // A receipt alone is never proof of complete persisted Markdown.
            let content = reader
                .read_content(&created.id)
                .await
                .map_err(|error| unknown("create.readback", error.kind.label()))?;
            if let Some(check) = readback_mismatch(&created, &content, &request) {
                return Err(unknown("create.readback_mismatch", check));
            }
            let receipt = VerifiedReceipt::from_readback(
                content.page.id.0,
                content.page.url,
                content.page.last_edited_time,
            )
            .map_err(|_| unknown("create.receipt_invalid", ""))?;
            ledger
                .record_verified(&claim, &receipt)
                .map_err(|_| unknown("create.ledger_commit", ""))?;
            (receipt, false)
        }
    };
    Ok(admit_verified(
        queue,
        &RefreshKey {
            workspace_id: scope.workspace_id.clone(),
            mutation_digest: claim.key_digest().to_owned(),
            page_id: receipt.page_id.clone(),
        },
        receipt,
        replayed,
    ))
}

#[cfg(test)]
mod readback_label_tests {
    use super::*;
    use crate::backend::{Page, PageContent};
    use std::collections::BTreeMap;

    fn page(id: &str, title: &str) -> Page {
        Page {
            id: PageId(id.to_owned()),
            url: format!("https://example.test/{id}"),
            title: title.to_owned(),
            last_edited_time: "2026-01-01T00:00:00.000Z".to_owned(),
            archived: false,
            properties: BTreeMap::new(),
            relation_names: BTreeMap::new(),
        }
    }

    fn request() -> CreateRequest {
        CreateRequest {
            parent_page_id: PageId("parent".to_owned()),
            title: "Title".to_owned(),
            markdown: "Body".to_owned(),
            idempotency_key: "key-key-key-key".to_owned(),
        }
    }

    #[test]
    fn readback_mismatch_names_the_first_failing_check() {
        let created = page("a", "Title");
        let good = PageContent {
            page: created.clone(),
            markdown: "Body".to_owned(),
        };
        assert_eq!(readback_mismatch(&created, &good, &request()), None);

        let mut other = good.clone();
        other.page.id = PageId("b".to_owned());
        assert_eq!(
            readback_mismatch(&created, &other, &request()),
            Some("page_id")
        );

        let mut archived = good.clone();
        archived.page.archived = true;
        assert_eq!(
            readback_mismatch(&created, &archived, &request()),
            Some("archived")
        );

        let mut url = good.clone();
        url.page.url = "https://example.test/other".to_owned();
        assert_eq!(readback_mismatch(&created, &url, &request()), Some("url"));

        let mut title = good.clone();
        title.page.title = "Changed".to_owned();
        assert_eq!(
            readback_mismatch(&created, &title, &request()),
            Some("title")
        );

        let mut markdown = good;
        markdown.markdown = "Completely different".to_owned();
        assert_eq!(
            readback_mismatch(&created, &markdown, &request()),
            Some("markdown")
        );
    }
}
