//! Safe, opt-in targeted append orchestration. Not an MCP mutation tool.
//!
//! A page ID, root claim, or Notion write receipt alone is never permission or
//! proof of completion. The durable ledger is required, even for the first call.
//! Unknown outcomes are never automatically retried.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::{
    backend::{AppendPageContent, NotionRead, NotionWrite, PageId},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore,
    },
    revision::{
        ExpectedRevision, RevisionError, RevisionMetadata, check_revision, compare_content_snapshot,
    },
    root_scope::RootScopeGate,
};

/// Exact end-of-page insertion, never whole-page replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub page_id: PageId,
    pub markdown: String,
    pub expected_last_edited_time: Option<String>,
    pub expected_markdown_sha256: Option<String>,
    /// Required, at least 16 URL-safe ASCII characters.
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub receipt: VerifiedReceipt,
    /// True means the stored verified receipt was replayed; no Notion PATCH.
    pub replayed: bool,
}

/// Public errors intentionally contain no page data, revision values, or
/// Notion response bodies. Unknown means *do not retry* this mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendError {
    InvalidInput,
    NotAuthorized,
    RevisionConflict(RevisionMetadata),
    IdempotencyConflict,
    Unavailable,
    OutcomeUnknown,
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "guarded append failed: {self:?}")
    }
}
impl std::error::Error for AppendError {}

fn ledger_error(error: IdempotencyError) -> AppendError {
    match error {
        IdempotencyError::InvalidInput => AppendError::InvalidInput,
        IdempotencyError::KeyConflict | IdempotencyError::AlreadyCommitted => {
            AppendError::IdempotencyConflict
        }
        _ => AppendError::Unavailable,
    }
}

fn fingerprint(request: &AppendRequest) -> String {
    let mut hash = Sha256::new();
    for part in [
        request.markdown.as_str(),
        request.expected_last_edited_time.as_deref().unwrap_or(""),
        request.expected_markdown_sha256.as_deref().unwrap_or(""),
        "end",
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

/// Attempt one authorized append, or safely replay a previously verified
/// receipt. A timed-out operation or crash leaves the ledger in a reconciliation
/// state and MUST NOT cause a second network PATCH.
///
/// Ordering: authorize -> durable reservation -> revision and exact preimage
/// read -> revalidate scope -> durably mark uncertain -> single PATCH -> fresh
/// full readback -> record verified receipt. This preflight is not atomic CAS:
/// Notion may accept another edit between the final check and the PATCH.
///
/// Strict readback requires the complete original Markdown byte prefix
/// *unchanged* followed by the exact new Markdown. Provider-side formatting or
/// ambiguous readback is classified unknown rather than silently accepted.
pub async fn append_once(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    request: AppendRequest,
) -> Result<AppendResult, AppendError> {
    if request.markdown.trim().is_empty()
        || request.markdown.len() > 200_000
        || request.markdown.contains('\0')
        || (request.expected_last_edited_time.is_none()
            && request.expected_markdown_sha256.is_none())
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
    {
        return Err(AppendError::InvalidInput);
    }

    let permit = gate
        .authorize(&request.page_id)
        .await
        .map_err(|_| AppendError::NotAuthorized)?;

    // Bind a caller's key to the server-verified workspace, the exact target,
    // payload, source precondition, and the explicit END position.
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::Append,
        &request.page_id.0,
        &fingerprint(&request),
    )
    .map_err(ledger_error)?;

    match ledger.begin(&claim).map_err(ledger_error)? {
        Reservation::Replay(receipt) => {
            if receipt.page_id != request.page_id.0 {
                return Err(AppendError::IdempotencyConflict);
            }
            return Ok(AppendResult {
                receipt,
                replayed: true,
            });
        }
        Reservation::Reconcile => return Err(AppendError::OutcomeUnknown),
        Reservation::ExecuteOnce => {}
    }

    // Revision preflight never uses indexed Markdown. Once a reservation
    // exists, any failure leaves it non-retryable until explicit reconciliation.
    let expected = ExpectedRevision {
        last_edited_time: request.expected_last_edited_time.clone(),
        markdown_sha256: request.expected_markdown_sha256.clone(),
    };
    let revision = check_revision(reader, &request.page_id, &expected)
        .await
        .map_err(|failure| match failure {
            RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                AppendError::RevisionConflict(meta)
            }
            RevisionError::InvalidPrecondition => AppendError::InvalidInput,
            RevisionError::Read(_) => AppendError::Unavailable,
        })?;

    let before = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| AppendError::Unavailable)?;
    let before_meta =
        compare_content_snapshot(&request.page_id, &expected, &before).map_err(|failure| {
            match failure {
                RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                    AppendError::RevisionConflict(meta)
                }
                RevisionError::InvalidPrecondition => AppendError::InvalidInput,
                RevisionError::Read(_) => AppendError::Unavailable,
            }
        })?;
    // Detect a change between the first fresh preflight and this full read.
    if DateTime::parse_from_rfc3339(&before_meta.last_edited_time).ok()
        != DateTime::parse_from_rfc3339(&revision.last_edited_time).ok()
    {
        return Err(AppendError::RevisionConflict(before_meta));
    }

    gate.revalidate(&permit, gate.configured_scope())
        .await
        .map_err(|_| AppendError::NotAuthorized)?;

    // Best-effort immediate fresh preflight. This cannot be atomic CAS.
    let fresh = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| AppendError::Unavailable)?;
    let final_meta =
        compare_content_snapshot(&request.page_id, &expected, &fresh).map_err(|failure| {
            match failure {
                RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                    AppendError::RevisionConflict(meta)
                }
                RevisionError::InvalidPrecondition => AppendError::InvalidInput,
                RevisionError::Read(_) => AppendError::Unavailable,
            }
        })?;
    if fresh.markdown != before.markdown {
        return Err(AppendError::RevisionConflict(final_meta));
    }

    // Persist the uncertain state BEFORE issuing the network mutation: after
    // any crash or timeout, repeat requests are reconciliation-only.
    ledger.mark_uncertain(&claim).map_err(ledger_error)?;

    let written = writer
        .append_content(AppendPageContent {
            page_id: request.page_id.clone(),
            markdown: request.markdown.clone(),
        })
        .await
        .map_err(|_| AppendError::OutcomeUnknown)?;
    if written.id != request.page_id || written.archived {
        return Err(AppendError::OutcomeUnknown);
    }

    let after = reader
        .read_content(&request.page_id)
        .await
        .map_err(|_| AppendError::OutcomeUnknown)?;
    if after.page.id != request.page_id
        || after.page.archived
        || !after.markdown.starts_with(&before.markdown)
        || after.markdown[before.markdown.len()..] != request.markdown
    {
        return Err(AppendError::OutcomeUnknown);
    }
    let post_permit = gate
        .authorize(&request.page_id)
        .await
        .map_err(|_| AppendError::OutcomeUnknown)?;
    gate.revalidate(&post_permit, gate.configured_scope())
        .await
        .map_err(|_| AppendError::OutcomeUnknown)?;

    let receipt = VerifiedReceipt::from_readback(
        after.page.id.0,
        after.page.url,
        after.page.last_edited_time,
    )
    .map_err(|_| AppendError::OutcomeUnknown)?;
    ledger
        .record_verified(&claim, &receipt)
        .map_err(|_| AppendError::OutcomeUnknown)?;
    Ok(AppendResult {
        receipt,
        replayed: false,
    })
}

/// Verified append followed by durable local refresh admission. Queue failure
/// never turns a committed Notion mutation into a retryable write failure.
pub async fn append_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    gate: &RootScopeGate,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn crate::write_refresh::MutationRefreshQueue,
    request: AppendRequest,
) -> Result<crate::write_refresh::WriteCompletion, AppendError> {
    let result = append_once(reader, writer, gate, ledger, request.clone()).await?;
    let claim = MutationClaim::new(
        &gate.configured_scope().workspace_id,
        &request.idempotency_key,
        MutationOperation::Append,
        &request.page_id.0,
        &fingerprint(&request),
    )
    .map_err(ledger_error)?;
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
