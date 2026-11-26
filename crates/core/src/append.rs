//! Safe, opt-in targeted append orchestration. Not an MCP mutation tool.
//!
//! A page ID, root claim, or Notion write receipt alone is never permission or
//! proof of completion. The durable ledger is required, even for the first call.
//! Unknown outcomes are never automatically retried.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::{
    backend::{AppendPageContent, NotionRead, NotionWrite, PageId, WriteRejection},
    idempotency::{
        IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
        WriteIdempotencyStore, release_rejected, release_unstarted,
    },
    lifecycle::LifecycleScope,
    revision::{
        ExpectedRevision, RevisionError, RevisionMetadata, check_revision, compare_content_snapshot,
    },
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
    /// Notion definitely refused the request before any effect; the claim
    /// was released, so the same key may be retried.
    Rejected(WriteRejection),
    OutcomeUnknown,
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "guarded append failed: {self:?}")
    }
}
impl std::error::Error for AppendError {}

/// An unknown outcome is deliberately reported to the caller without detail;
/// the operator log names the stage that could not prove the result.
fn unknown(stage: &'static str, detail: &'static str) -> AppendError {
    crate::logging::emit_write_outcome_unknown(crate::logging::Outcome::Failed, stage, detail);
    AppendError::OutcomeUnknown
}

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
/// Ordering: durable reservation -> revision and exact preimage
/// read -> durably mark uncertain -> single PATCH -> fresh
/// full readback -> record verified receipt. This preflight is not atomic CAS:
/// Notion may accept another edit between the final check and the PATCH.
///
/// Strict readback requires the complete original Markdown byte prefix
/// *unchanged* followed by the new Markdown. When the original ends mid-line,
/// Notion can add one separating LF. One final LF can also vary.
/// Other formatting changes or ambiguous readback remain unknown.
pub async fn append_once(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    scope: &LifecycleScope,
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

    // Bind a caller's key to the server-verified workspace, the exact target,
    // payload, source precondition, and the explicit END position.
    let claim = MutationClaim::new(
        &scope.workspace_id,
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
        Reservation::Reconcile => return Err(unknown("append.reconcile_required", "")),
        Reservation::ExecuteOnce => {}
    }

    // Nothing has been sent yet: any preflight failure releases the claim so
    // the same key can run again.
    let before = match append_preflight(reader, &request).await {
        Ok(before) => before,
        Err(error) => {
            let rejection = match &error {
                AppendError::Rejected(rejection) => Some(*rejection),
                _ => None,
            };
            release_unstarted(ledger, &claim, "append.preflight", rejection);
            return Err(error);
        }
    };
    let before_comparison = reader.comparison_markdown(&before.markdown);

    // Persist the uncertain state BEFORE issuing the network mutation: after
    // any crash or timeout, repeat requests are reconciliation-only.
    ledger.mark_uncertain(&claim).map_err(ledger_error)?;

    let written = match writer
        .append_content(AppendPageContent {
            page_id: request.page_id.clone(),
            markdown: request.markdown.clone(),
        })
        .await
    {
        Ok(written) => written,
        Err(error) => {
            let Some(rejection) = WriteRejection::of(&error) else {
                return Err(unknown("append.patch", error.kind.label()));
            };
            release_rejected(ledger, &claim, "append.patch", rejection.label());
            return Err(AppendError::Rejected(rejection));
        }
    };
    if written.id != request.page_id || written.archived {
        return Err(unknown("append.receipt_mismatch", ""));
    }

    let after = reader
        .read_content(&request.page_id)
        .await
        .map_err(|error| unknown("append.readback", error.kind.label()))?;
    if after.page.id != request.page_id || after.page.archived {
        return Err(unknown("append.readback_identity", ""));
    }
    let after_comparison = reader.comparison_markdown(&after.markdown);
    let inserted = reader.comparison_markdown(&request.markdown);
    let may_insert_separator = !before_comparison.is_empty()
        && !before_comparison.ends_with('\n')
        && !inserted.starts_with('\n');
    let matches_append = after_comparison
        .strip_prefix(before_comparison.as_ref())
        .is_some_and(|suffix| {
            crate::revision::written_markdown_matches(&inserted, suffix)
                || (may_insert_separator
                    && suffix.strip_prefix('\n').is_some_and(|after_separator| {
                        crate::revision::written_markdown_matches(&inserted, after_separator)
                    }))
        });
    if !matches_append {
        return Err(unknown("append.readback_mismatch", "markdown"));
    }

    let receipt = VerifiedReceipt::from_readback(
        after.page.id.0,
        after.page.url,
        after.page.last_edited_time,
    )
    .map_err(|_| unknown("append.receipt_invalid", ""))?;
    ledger
        .record_verified(&claim, &receipt)
        .map_err(|_| unknown("append.ledger_commit", ""))?;
    Ok(AppendResult {
        receipt,
        replayed: false,
    })
}

/// Revision preflight before any effect. It never uses indexed Markdown and
/// returns the exact preimage the readback must preserve.
async fn append_preflight(
    reader: &dyn NotionRead,
    request: &AppendRequest,
) -> Result<crate::backend::PageContent, AppendError> {
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
            RevisionError::Read(error) => read_failure(&error),
        })?;

    let before = reader
        .read_content(&request.page_id)
        .await
        .map_err(|error| read_failure(&error))?;
    let before_meta =
        compare_content_snapshot(&request.page_id, &expected, &before).map_err(|failure| {
            match failure {
                RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                    AppendError::RevisionConflict(meta)
                }
                RevisionError::InvalidPrecondition => AppendError::InvalidInput,
                RevisionError::Read(error) => read_failure(&error),
            }
        })?;
    // Detect a change between the first fresh preflight and this full read.
    if DateTime::parse_from_rfc3339(&before_meta.last_edited_time).ok()
        != DateTime::parse_from_rfc3339(&revision.last_edited_time).ok()
    {
        return Err(AppendError::RevisionConflict(before_meta));
    }

    // Best-effort immediate fresh preflight. This cannot be atomic CAS.
    let fresh = reader
        .read_content(&request.page_id)
        .await
        .map_err(|error| read_failure(&error))?;
    let final_meta =
        compare_content_snapshot(&request.page_id, &expected, &fresh).map_err(|failure| {
            match failure {
                RevisionError::Conflict(meta) | RevisionError::Inactive(meta) => {
                    AppendError::RevisionConflict(meta)
                }
                RevisionError::InvalidPrecondition => AppendError::InvalidInput,
                RevisionError::Read(error) => read_failure(&error),
            }
        })?;
    let before_comparison = reader.comparison_markdown(&before.markdown);
    if reader.comparison_markdown(&fresh.markdown) != before_comparison {
        return Err(AppendError::RevisionConflict(final_meta));
    }
    Ok(before)
}

/// A read refusal during preflight is definite; anything else is transient.
fn read_failure(error: &crate::backend::BackendError) -> AppendError {
    match WriteRejection::of(error) {
        Some(rejection @ (WriteRejection::NotFound | WriteRejection::PermissionDenied)) => {
            AppendError::Rejected(rejection)
        }
        _ => AppendError::Unavailable,
    }
}

/// Verified append followed by durable local refresh admission. Queue failure
/// never turns a committed Notion mutation into a retryable write failure.
pub async fn append_once_and_refresh(
    reader: &dyn NotionRead,
    writer: &dyn NotionWrite,
    scope: &LifecycleScope,
    ledger: &dyn WriteIdempotencyStore,
    queue: &dyn crate::write_refresh::MutationRefreshQueue,
    request: AppendRequest,
) -> Result<crate::write_refresh::WriteCompletion, AppendError> {
    let claim = MutationClaim::new(
        &scope.workspace_id,
        &request.idempotency_key,
        MutationOperation::Append,
        &request.page_id.0,
        &fingerprint(&request),
    )
    .map_err(ledger_error)?;
    let result = append_once(reader, writer, scope, ledger, request).await?;
    Ok(crate::write_refresh::admit_verified(
        queue,
        &crate::write_refresh::RefreshKey {
            workspace_id: scope.workspace_id.clone(),
            mutation_digest: claim.key_digest().to_owned(),
            page_id: result.receipt.page_id.clone(),
        },
        result.receipt,
        result.replayed,
    ))
}
