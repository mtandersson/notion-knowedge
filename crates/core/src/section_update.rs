//! Guarded, once-only bounded Markdown section edit. Not exposed as an MCP tool.
//! All inputs are authoritative, not derived from indexed or normalized content.
//! There is no Notion atomic compare-and-swap: optimistic checks and exact
//! readback detect conflicts but cannot make concurrent edits transactional.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::{
    backend::{NotionRead, NotionSectionWrite, PageId, UpdateSectionContent},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore,
    },
    revision::{ExpectedRevision, RevisionError, check_revision, markdown_sha256},
    root_scope::RootScopeGate,
    sections::{SectionAnchor, SectionError, plan_section},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionUpdateRequest {
    pub page_id: PageId,
    pub anchor: SectionAnchor,
    pub replacement: String,
    /// Caller-provided revision from a prior, authoritative Notion read.
    pub expected_last_edited_time: String,
    /// Required hash of the complete, original UTF-8 Markdown.
    pub expected_markdown_sha256: String,
    /// Globally unique per intent and at least 16 URL-safe characters.
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionUpdateResult {
    pub receipt: VerifiedReceipt,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionUpdateError {
    InvalidInput,
    NotAuthorized,
    RevisionConflict,
    SelectionRejected,
    IdempotencyConflict,
    Unavailable,
    /// The ledger must remain reserved; reconcile, NEVER resend the PATCH.
    OutcomeUnknown,
}

impl std::fmt::Display for SectionUpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "guarded section update failed: {self:?}")
    }
}
impl std::error::Error for SectionUpdateError {}

fn ledger_error(error: IdempotencyError) -> SectionUpdateError {
    match error {
        IdempotencyError::InvalidInput => SectionUpdateError::InvalidInput,
        IdempotencyError::KeyConflict | IdempotencyError::AlreadyCommitted => {
            SectionUpdateError::IdempotencyConflict
        }
        _ => SectionUpdateError::Unavailable,
    }
}

fn fingerprint(request: &SectionUpdateRequest) -> String {
    let mut hash = Sha256::new();
    for part in [
        "section-v1",
        &request.anchor.level.to_string(),
        request.anchor.heading.as_str(),
        request.replacement.as_str(),
        request.expected_last_edited_time.as_str(),
        request.expected_markdown_sha256.as_str(),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn matches_revision(
    page_id: &PageId,
    request: &SectionUpdateRequest,
    content: &crate::backend::PageContent,
) -> bool {
    content.page.id == *page_id
        && !content.page.archived
        && DateTime::parse_from_rfc3339(&content.page.last_edited_time).ok()
            == DateTime::parse_from_rfc3339(&request.expected_last_edited_time).ok()
        && markdown_sha256(&content.markdown)
            .eq_ignore_ascii_case(&request.expected_markdown_sha256)
}

/// Authorize, reserve durably, verify full fresh revision and scope, then issue
/// exactly one provider-side targeted search-and-replace request. Empty section
/// bodies and repeated source bodies are unsupported: upstream requires a
/// nonempty, unambiguous old_str, and no guessed position is ever supplied.
///
/// Ledger order is reserve -> preflight -> mark_uncertain -> one PATCH ->
/// authoritative full readback -> record_verified. Failures after reservation
/// never release the key automatically. The ledger MUST be persistent,
/// transactional and shared between workers, as required by its interface.
pub async fn update_section_once(
    reader: &dyn NotionRead,
    writer: &dyn NotionSectionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    request: SectionUpdateRequest,
) -> Result<SectionUpdateResult, SectionUpdateError> {
    if DateTime::parse_from_rfc3339(&request.expected_last_edited_time).is_err()
        || request.expected_markdown_sha256.len() != 64
        || !request.expected_markdown_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || request.replacement.len() > 200_000
        || request.replacement.contains('\0')
    {
        return Err(SectionUpdateError::InvalidInput);
    }
    let permit = gate.authorize(&request.page_id).await
        .map_err(|_| SectionUpdateError::NotAuthorized)?;
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::UpdateSection,
        &request.page_id.0,
        &fingerprint(&request),
    ).map_err(ledger_error)?;
    match ledger.begin(&claim).map_err(ledger_error)? {
        Reservation::Replay(receipt) => {
            if receipt.page_id != request.page_id.0 {
                return Err(SectionUpdateError::IdempotencyConflict);
            }
            return Ok(SectionUpdateResult { receipt, replayed: true });
        }
        Reservation::Reconcile => return Err(SectionUpdateError::OutcomeUnknown),
        Reservation::ExecuteOnce => {}
    }

    let expected = ExpectedRevision {
        last_edited_time: Some(request.expected_last_edited_time.clone()),
        markdown_sha256: Some(request.expected_markdown_sha256.clone()),
    };
    check_revision(reader, &request.page_id, &expected)
        .await
        .map_err(|err| match err {
            RevisionError::Conflict(_) | RevisionError::Inactive(_) => {
                SectionUpdateError::RevisionConflict
            }
            RevisionError::InvalidPrecondition => SectionUpdateError::InvalidInput,
            RevisionError::Read(_) => SectionUpdateError::Unavailable,
        })?;

    let original = reader.read_content(&request.page_id).await
        .map_err(|_| SectionUpdateError::Unavailable)?;
    if !matches_revision(&request.page_id, &request, &original) {
        return Err(SectionUpdateError::RevisionConflict);
    }

    let plan = plan_section(&original.markdown, request.anchor.clone(), &request.replacement)
        .map_err(|err| match err {
            SectionError::InvalidInput | SectionError::NoChange => SectionUpdateError::InvalidInput,
            SectionError::NotFound | SectionError::Ambiguous | SectionError::Unsupported => {
                SectionUpdateError::SelectionRejected
            }
        })?;
    let old = plan.original_body();
    // Upstream update_content matches textual occurrences, not our byte offsets.
    // It cannot select an empty range or distinguish identical sibling content.
    if old.is_empty() || original.markdown.match_indices(old).count() != 1 {
        return Err(SectionUpdateError::SelectionRejected);
    }

    gate.revalidate(&permit, gate.configured_scope()).await
        .map_err(|_| SectionUpdateError::NotAuthorized)?;
    // Best-effort final immediate preflight. Not an atomic CAS.
    let fresh = reader.read_content(&request.page_id).await
        .map_err(|_| SectionUpdateError::Unavailable)?;
    if !matches_revision(&request.page_id, &request, &fresh)
        || fresh.markdown != original.markdown
    {
        return Err(SectionUpdateError::RevisionConflict);
    }

    // MUST be persisted before any network mutation, including failed or
    // timed-out requests. No automatic replay on any outcome.
    ledger.mark_uncertain(&claim).map_err(ledger_error)?;
    writer.update_section(UpdateSectionContent {
        page_id: request.page_id.clone(),
        old_str: old.to_owned(),
        new_str: plan.replacement_body().to_owned(),
    }).await.map_err(|_| SectionUpdateError::OutcomeUnknown)?;

    let after = reader.read_content(&request.page_id).await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    if after.page.id != request.page_id || after.page.archived
        || !plan.verify_readback(&after.markdown)
    {
        return Err(SectionUpdateError::OutcomeUnknown);
    }
    gate.revalidate(&permit, gate.configured_scope()).await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;

    let receipt = VerifiedReceipt::from_readback(
        after.page.id.0,
        after.page.url,
        after.page.last_edited_time,
    ).map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    ledger.record_verified(&claim, &receipt)
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    Ok(SectionUpdateResult { receipt, replayed: false })
}
