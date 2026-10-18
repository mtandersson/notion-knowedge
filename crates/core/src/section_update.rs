//! Guarded, once-only bounded Markdown section edit. Not exposed as an MCP tool.
//! All inputs are authoritative, not derived from indexed or normalized content.
//! There is no Notion atomic compare-and-swap: optimistic checks and exact
//! readback detect conflicts but cannot make concurrent edits transactional.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::{
    backend::{NotionRead, NotionSectionWrite, PageId, UpdateSectionContent},
    destructive::{ConfirmationProvider, ConfirmationRequest, DestructivePolicy, WriteOperation},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore,
    },
    revision::{
        ExpectedRevision, RevisionError, RevisionMetadata, check_revision, compare_content_snapshot,
    },
    root_scope::RootScopeGate,
    sections::{SectionAnchor, SectionError, plan_section},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionUpdateRequest {
    pub page_id: PageId,
    pub anchor: SectionAnchor,
    pub replacement: String,
    /// Caller-provided revision from a prior, authoritative Notion read.
    pub expected_last_edited_time: Option<String>,
    /// Optional hash of the complete, original UTF-8 Markdown. At least one revision guard is required.
    pub expected_markdown_sha256: Option<String>,
    /// Globally unique per intent and at least 16 URL-safe characters.
    pub idempotency_key: String,
}

/// Once-only verified section edit with independent durable index status.
pub async fn update_section_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionSectionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn crate::write_refresh::MutationRefreshQueue,
    request: SectionUpdateRequest,
) -> Result<crate::write_refresh::WriteCompletion, SectionUpdateError> {
    update_section_and_refresh(reader, writer, gate, ledger, queue, request, None).await
}

/// An actual new effect requires trusted confirmation; a verified ledger replay
/// returns its old receipt without consuming a second one-use approval.
pub async fn update_section_confirmed_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionSectionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn crate::write_refresh::MutationRefreshQueue,
    request: SectionUpdateRequest,
    confirmation: &dyn ConfirmationProvider,
) -> Result<crate::write_refresh::WriteCompletion, SectionUpdateError> {
    update_section_and_refresh(
        reader,
        writer,
        gate,
        ledger,
        queue,
        request,
        Some(confirmation),
    )
    .await
}

async fn update_section_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionSectionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn crate::write_refresh::MutationRefreshQueue,
    request: SectionUpdateRequest,
    confirmation: Option<&dyn ConfirmationProvider>,
) -> Result<crate::write_refresh::WriteCompletion, SectionUpdateError> {
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::UpdateSection,
        &request.page_id.0,
        &fingerprint(&request),
    )
    .map_err(ledger_error)?;
    let result = update_section_inner(reader, writer, gate, ledger, request, confirmation).await?;
    Ok(crate::write_refresh::admit_verified(
        queue,
        &crate::write_refresh::RefreshKey {
            workspace_id: gate.configured_scope().workspace_id.clone(),
            mutation_digest: claim.key_digest().to_owned(),
            page_id: result.receipt.page_id.clone(),
        },
        result.receipt,
        result.replayed,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionUpdateResult {
    pub receipt: VerifiedReceipt,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionUpdateError {
    InvalidInput,
    NotAuthorized,
    RevisionConflict(RevisionMetadata),
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

fn map_revision_error(err: RevisionError) -> SectionUpdateError {
    match err {
        RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
            SectionUpdateError::RevisionConflict(meta)
        }
        RevisionError::InvalidPrecondition => SectionUpdateError::InvalidInput,
        RevisionError::Read(_) => SectionUpdateError::Unavailable,
    }
}

fn fingerprint(request: &SectionUpdateRequest) -> String {
    let mut hash = Sha256::new();
    for part in [
        "section-v1",
        &request.anchor.level.to_string(),
        request.anchor.heading.as_str(),
        request.replacement.as_str(),
        request.expected_last_edited_time.as_deref().unwrap_or(""),
        request.expected_markdown_sha256.as_deref().unwrap_or(""),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
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
    update_section_inner(reader, writer, gate, ledger, request, None).await
}

async fn update_section_inner(
    reader: &dyn NotionRead,
    writer: &dyn NotionSectionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    request: SectionUpdateRequest,
    confirmation: Option<&dyn ConfirmationProvider>,
) -> Result<SectionUpdateResult, SectionUpdateError> {
    if (request.expected_last_edited_time.is_none() && request.expected_markdown_sha256.is_none())
        || request
            .expected_last_edited_time
            .as_deref()
            .is_some_and(|time| DateTime::parse_from_rfc3339(time).is_err())
        || request
            .expected_markdown_sha256
            .as_deref()
            .is_some_and(|hash| {
                hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || request.replacement.len() > 200_000
        || request.replacement.contains('\0')
    {
        return Err(SectionUpdateError::InvalidInput);
    }
    // Enhanced Notion structural blocks are not safe to synthesize through
    // a Markdown search-and-replace, even if the old range is ordinary text.
    if ["<unknown", "<page ", "<database ", "<data-source "]
        .iter()
        .any(|marker| request.replacement.contains(marker))
    {
        return Err(SectionUpdateError::SelectionRejected);
    }
    let permit = gate
        .authorize(&request.page_id)
        .await
        .map_err(|_| SectionUpdateError::NotAuthorized)?;
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::UpdateSection,
        &request.page_id.0,
        &fingerprint(&request),
    )
    .map_err(ledger_error)?;
    match ledger.begin(&claim).map_err(ledger_error)? {
        Reservation::Replay(receipt) => {
            if receipt.page_id != request.page_id.0 {
                return Err(SectionUpdateError::IdempotencyConflict);
            }
            return Ok(SectionUpdateResult {
                receipt,
                replayed: true,
            });
        }
        Reservation::Reconcile => return Err(SectionUpdateError::OutcomeUnknown),
        Reservation::ExecuteOnce => {}
    }

    let expected = ExpectedRevision {
        last_edited_time: request.expected_last_edited_time.clone(),
        markdown_sha256: request.expected_markdown_sha256.clone(),
    };
    check_revision(reader, &request.page_id, &expected)
        .await
        .map_err(|err| match err {
            RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                SectionUpdateError::RevisionConflict(meta)
            }
            RevisionError::InvalidPrecondition => SectionUpdateError::InvalidInput,
            RevisionError::Read(_) => SectionUpdateError::Unavailable,
        })?;

    let original = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| SectionUpdateError::Unavailable)?;
    compare_content_snapshot(&request.page_id, &expected, &original).map_err(map_revision_error)?;

    let plan = plan_section(
        &original.markdown,
        request.anchor.clone(),
        &request.replacement,
    )
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

    if let Some(provider) = confirmation {
        let scope = gate.configured_scope();
        let scope_digest=MutationClaim::payload_sha256(&serde_json::to_vec(&serde_json::json!({"workspace":scope.workspace_id,"generation":scope.generation,"roots":scope.roots.iter().map(|r|&r.0).collect::<Vec<_>>()})).map_err(|_|SectionUpdateError::InvalidInput)?);
        let intended = ConfirmationRequest::new(
            WriteOperation::UpdateSection,
            &format!("scope-{scope_digest}"),
            &request.page_id.0,
            &fingerprint(&request),
            &request.idempotency_key,
        )
        .map_err(|_| SectionUpdateError::InvalidInput)?;
        DestructivePolicy::new(true)
            .require_confirmation(&intended, Some(provider))
            .await
            .map_err(|_| SectionUpdateError::NotAuthorized)?
            .consume(&intended)
            .map_err(|_| SectionUpdateError::NotAuthorized)?;
    }
    // Approval can await human input; recheck source and scope AFTER it.
    gate.revalidate(&permit, gate.configured_scope())
        .await
        .map_err(|_| SectionUpdateError::NotAuthorized)?;
    // Best-effort final immediate preflight. Not an atomic CAS.
    let fresh = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| SectionUpdateError::Unavailable)?;
    let fresh_meta = compare_content_snapshot(&request.page_id, &expected, &fresh)
        .map_err(map_revision_error)?;
    if fresh.markdown != original.markdown {
        return Err(SectionUpdateError::RevisionConflict(fresh_meta));
    }

    // MUST be persisted before any network mutation, including failed or
    // timed-out requests. No automatic replay on any outcome.
    ledger.mark_uncertain(&claim).map_err(ledger_error)?;
    writer
        .update_section(UpdateSectionContent {
            page_id: request.page_id.clone(),
            old_str: old.to_owned(),
            new_str: plan.replacement_body().to_owned(),
        })
        .await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;

    let after = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    if after.page.id != request.page_id
        || after.page.archived
        || !plan.verify_readback(&after.markdown)
    {
        return Err(SectionUpdateError::OutcomeUnknown);
    }
    // The write itself changes the selected page's last-edited revision.
    // Never revalidate the pre-write lifecycle evidence: Notion's lifecycle
    // adapter compares the entire chain, including the old revision.
    // Acquire fresh physical authorization and revalidate its new evidence.
    let post_permit = gate
        .authorize(&request.page_id)
        .await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    gate.revalidate(&post_permit, gate.configured_scope())
        .await
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;

    let receipt = VerifiedReceipt::from_readback(
        after.page.id.0,
        after.page.url,
        after.page.last_edited_time,
    )
    .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    ledger
        .record_verified(&claim, &receipt)
        .map_err(|_| SectionUpdateError::OutcomeUnknown)?;
    Ok(SectionUpdateResult {
        receipt,
        replayed: false,
    })
}
