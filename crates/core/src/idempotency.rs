//! Durable once-only mutation claims for safe write workflows.
//! A network timeout MUST remain indeterminate until authoritative reconciliation.
//! This port does not invoke Notion or authorize a target.

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOperation {
    CreatePage,
    Append,
    UpdateSection,
    ArchivePage,
    UploadFile,
}

impl MutationOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreatePage => "create_page",
            Self::Append => "append",
            Self::UpdateSection => "update_section",
            Self::ArchivePage => "archive_page",
            Self::UploadFile => "upload_file",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyError {
    InvalidInput,
    KeyConflict,
    NotReserved,
    AlreadyCommitted,
    CorruptState,
    Unavailable,
}

impl std::fmt::Display for IdempotencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "idempotency failed: {self:?}")
    }
}
impl std::error::Error for IdempotencyError {}

/// A validated caller claim. Scope MUST be a server-verified tenant identity,
/// not an untrusted root_page_id. Never put credentials into the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationClaim {
    key_digest: String,
    request_digest: String,
}

fn digest(domain: &[u8], values: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for value in values {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

impl MutationClaim {
    pub fn new(
        trusted_scope: &str,
        key: &str,
        operation: MutationOperation,
        target: &str,
        payload_sha256: &str,
    ) -> Result<Self, IdempotencyError> {
        let safe = |value: &str, maximum: usize| {
            !value.is_empty()
                && value.len() <= maximum
                && value.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
        };
        if !safe(trusted_scope, 128)
            || key.len() < 16
            || !safe(key, 128)
            || !safe(target, 128)
            || payload_sha256.len() != 64
            || !payload_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(IdempotencyError::InvalidInput);
        }
        Ok(Self {
            key_digest: digest(b"nk-idempotency-key-v1", &[trusted_scope, key]),
            request_digest: digest(
                b"nk-idempotency-request-v1",
                &[operation.as_str(), target, &payload_sha256.to_ascii_lowercase()],
            ),
        })
    }

    pub fn payload_sha256(payload: &[u8]) -> String {
        format!("{:x}", Sha256::digest(payload))
    }

    pub fn key_digest(&self) -> &str {
        &self.key_digest
    }

    pub fn request_digest(&self) -> &str {
        &self.request_digest
    }
}

/// Only a read-back-verified write may be recorded as committed by the trusted
/// workflow. This model excludes Markdown, credentials, and arbitrary bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReceipt {
    pub page_id: String,
    pub url: String,
    pub last_edited_time: String,
}

impl VerifiedReceipt {
    pub fn from_readback(
        page_id: String,
        url: String,
        last_edited_time: String,
    ) -> Result<Self, IdempotencyError> {
        if page_id.is_empty()
            || page_id.len() > 128
            || page_id.chars().any(char::is_control)
            || url.len() > 2048
            || !url.starts_with("https://")
            || chrono::DateTime::parse_from_rfc3339(&last_edited_time).is_err()
        {
            return Err(IdempotencyError::InvalidInput);
        }
        Ok(Self {
            page_id,
            url,
            last_edited_time,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// The only result that permits initiating a network mutation.
    ExecuteOnce,
    /// A previous attempt could have committed; reconcile, never resend.
    Reconcile,
    /// Return the original verified receipt; do not perform the write again.
    Replay(VerifiedReceipt),
}

/// All transitions must be durable, atomic and cross-process safe.
/// Losing this ledger is NOT equivalent to having no committed operations.
pub trait WriteIdempotencyStore: Send + Sync {
    fn begin(&self, claim: &MutationClaim) -> Result<Reservation, IdempotencyError>;
    fn mark_uncertain(&self, claim: &MutationClaim) -> Result<(), IdempotencyError>;
    fn record_verified(
        &self,
        claim: &MutationClaim,
        receipt: &VerifiedReceipt,
    ) -> Result<(), IdempotencyError>;
}
