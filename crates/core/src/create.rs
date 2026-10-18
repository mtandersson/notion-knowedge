//! Provider-neutral, once-only page creation for semantic mutation dispatch.
//! Each preflight/read/effect uses an injected authoritative port, permitting
//! request-bound adapters to recheck authority at every upstream boundary.
use crate::{
    backend::{CreatePage, NotionRead, NotionWrite, PageId},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore,
    },
    lifecycle::{LifecycleKind, PhysicalParent},
    root_scope::{AuthorizedPage, RootScopeGate},
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
fn ledger_error(e: IdempotencyError) -> CreateError {
    match e {
        IdempotencyError::InvalidInput => CreateError::InvalidInput,
        IdempotencyError::KeyConflict | IdempotencyError::AlreadyCommitted => {
            CreateError::IdempotencyConflict
        }
        _ => CreateError::Unavailable,
    }
}
fn correct_parent(permit: &AuthorizedPage, parent: &PageId) -> bool {
    permit
        .lifecycle_evidence()
        .ancestry
        .first()
        .is_some_and(|node| {
            matches!(&node.parent,
        PhysicalParent::Object {kind:LifecycleKind::Page,id} if id==&parent.0)
        })
}
pub fn valid_request(request: &CreateRequest) -> bool {
    !request.title.trim().is_empty()
        && request.title.chars().count() <= 200
        && request.title.chars().all(|c| !c.is_control() && !bidi(c))
        && !request.markdown.trim().is_empty()
        && request.markdown.len() <= 200_000
        && request
            .markdown
            .chars()
            .all(|c| (!c.is_control() || matches!(c, '\n' | '\r' | '\t')) && !bidi(c))
}
fn bidi(c: char) -> bool {
    matches!(c,'\u{200e}'|'\u{200f}'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')
}

/// Reserve -> fresh parent revalidation -> mark uncertain -> single create ->
/// complete exact readback -> fresh physical child authorization -> verified
/// ledger commit -> independent refresh admission. No automatic mutation retry.
pub async fn create_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn MutationRefreshQueue,
    request: CreateRequest,
) -> Result<WriteCompletion, CreateError> {
    if !valid_request(&request) {
        return Err(CreateError::InvalidInput);
    }
    let parent = gate
        .authorize(&request.parent_page_id)
        .await
        .map_err(|_| CreateError::NotAuthorized)?;
    let payload = serde_json::to_vec(
        &serde_json::json!({"title":request.title,"markdown":request.markdown,"icon":null}),
    )
    .map_err(|_| CreateError::InvalidInput)?;
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::CreatePage,
        &request.parent_page_id.0,
        &MutationClaim::payload_sha256(&payload),
    )
    .map_err(ledger_error)?;
    let (receipt, replayed) = match ledger.begin(&claim).map_err(ledger_error)? {
        Reservation::Replay(receipt) => {
            let child = gate
                .authorize(&PageId(receipt.page_id.clone()))
                .await
                .map_err(|_| CreateError::NotAuthorized)?;
            if !correct_parent(&child, &request.parent_page_id) {
                return Err(CreateError::NotAuthorized);
            }
            gate.revalidate(&child, gate.configured_scope())
                .await
                .map_err(|_| CreateError::NotAuthorized)?;
            (receipt, true)
        }
        Reservation::Reconcile => return Err(CreateError::OutcomeUnknown),
        Reservation::ExecuteOnce => {
            gate.revalidate(&parent, gate.configured_scope())
                .await
                .map_err(|_| CreateError::NotAuthorized)?;
            ledger.mark_uncertain(&claim).map_err(ledger_error)?;
            let created = writer
                .create_page(CreatePage {
                    parent_page_id: request.parent_page_id.clone(),
                    title: request.title.clone(),
                    markdown: request.markdown.clone(),
                })
                .await
                .map_err(|_| CreateError::OutcomeUnknown)?;
            if created.archived || created.title != request.title {
                return Err(CreateError::OutcomeUnknown);
            }
            // A receipt alone is never proof of complete persisted Markdown.
            let content = reader
                .read_content(&created.id)
                .await
                .map_err(|_| CreateError::OutcomeUnknown)?;
            if content.page.id != created.id
                || content.page.archived
                || content.page.url != created.url
                || content.page.title != request.title
                || content.markdown != request.markdown
            {
                return Err(CreateError::OutcomeUnknown);
            }
            let child = gate
                .authorize(&created.id)
                .await
                .map_err(|_| CreateError::OutcomeUnknown)?;
            if !correct_parent(&child, &request.parent_page_id) {
                return Err(CreateError::OutcomeUnknown);
            }
            gate.revalidate(&child, gate.configured_scope())
                .await
                .map_err(|_| CreateError::OutcomeUnknown)?;
            let receipt = VerifiedReceipt::from_readback(
                content.page.id.0,
                content.page.url,
                content.page.last_edited_time,
            )
            .map_err(|_| CreateError::OutcomeUnknown)?;
            ledger
                .record_verified(&claim, &receipt)
                .map_err(|_| CreateError::OutcomeUnknown)?;
            (receipt, false)
        }
    };
    Ok(admit_verified(
        queue,
        &RefreshKey {
            workspace_id: gate.configured_scope().workspace_id.clone(),
            mutation_digest: claim.key_digest().to_owned(),
            page_id: receipt.page_id.clone(),
        },
        receipt,
        replayed,
    ))
}
