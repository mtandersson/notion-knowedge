//! Fresh, fail-closed preflight for future guarded Notion writes.
//!
//! This module deliberately does not mutate anything. The caller must authorize
//! the target before invoking it and must not treat a successful check as an
//! atomic Notion compare-and-swap: the source can change before a later PATCH.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::backend::{BackendError, BackendErrorKind, NotionRead, PageId};

/// A caller's revision, captured from an earlier authoritative read.
/// SHA-256 is over the **exact UTF-8 Markdown bytes**, not indexed/search text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpectedRevision {
    pub last_edited_time: Option<String>,
    pub markdown_sha256: Option<String>,
}

/// Intentionally content-free metadata suitable for a conflict response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionMetadata {
    pub page_id: PageId,
    pub last_edited_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionError {
    /// Missing or malformed caller precondition; do not try a mutation.
    InvalidPrecondition,
    /// Source identity or upstream metadata was not trustworthy.
    Read(BackendError),
    /// A changed source revision requires an explicit fresh read and retry.
    Conflict(RevisionMetadata),
    /// An archived page must never be mutated through this guard.
    Inactive(RevisionMetadata),
}

fn internal_error() -> RevisionError {
    RevisionError::Read(BackendError {
        operation: "knowledge.revision_preflight",
        kind: BackendErrorKind::Internal,
        retry_after: None,
        committed_page_id: None,
    })
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Compute the guard's exact Markdown hash for a previously fetched source.
/// Do not substitute the indexed normalized-content fingerprint.
pub fn markdown_sha256(markdown: &str) -> String {
    format!("{:x}", Sha256::digest(markdown.as_bytes()))
}

/// Compare caller preconditions against one fresh authoritative page read.
///
/// All supplied conditions must match. No local index is consulted, no
/// mutation is attempted, and no automatic retry is performed. After a
/// conflict, the caller must fetch current content and explicitly retry.
pub async fn check_revision(
    reader: &dyn NotionRead,
    page_id: &PageId,
    expected: &ExpectedRevision,
) -> Result<RevisionMetadata, RevisionError> {
    if expected.last_edited_time.is_none() && expected.markdown_sha256.is_none() {
        return Err(RevisionError::InvalidPrecondition);
    }
    let expected_time = expected
        .last_edited_time
        .as_deref()
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| RevisionError::InvalidPrecondition)?;
    if expected
        .markdown_sha256
        .as_deref()
        .is_some_and(|hash| !valid_hash(hash))
    {
        return Err(RevisionError::InvalidPrecondition);
    }

    // A hash requires full fresh Markdown. Otherwise fetch only metadata.
    let (page, current_hash) = if expected.markdown_sha256.is_some() {
        let content = reader
            .read_content(page_id)
            .await
            .map_err(RevisionError::Read)?;
        let hash = markdown_sha256(&content.markdown);
        (content.page, Some(hash))
    } else {
        (
            reader
                .fetch_page(page_id)
                .await
                .map_err(RevisionError::Read)?,
            None,
        )
    };
    if &page.id != page_id {
        return Err(internal_error());
    }
    let current_time =
        DateTime::parse_from_rfc3339(&page.last_edited_time).map_err(|_| internal_error())?;
    let metadata = RevisionMetadata {
        page_id: page.id,
        last_edited_time: page.last_edited_time,
    };
    if page.archived {
        return Err(RevisionError::Inactive(metadata));
    }
    let hash_changed = match (expected.markdown_sha256.as_deref(), current_hash.as_deref()) {
        (Some(expected_hash), Some(actual_hash)) => {
            !expected_hash.eq_ignore_ascii_case(actual_hash)
        }
        (None, _) => false,
        _ => return Err(internal_error()),
    };
    if expected_time.is_some_and(|time| time != current_time) || hash_changed {
        return Err(RevisionError::Conflict(metadata));
    }
    Ok(metadata)
}
