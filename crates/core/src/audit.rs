//! Privacy-aware operational audit records for agent mutations.
//!
//! This is an allowlisted record schema, not a logger for raw requests. Page
//! bodies, credentials, file names, signed URLs and error strings have no field.
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAction {
    PageCreate,
    PageAppend,
    PageReplace,
    PageArchive,
    FileAttach,
}

impl AuditAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PageCreate => "page.create",
            Self::PageAppend => "page.append",
            Self::PageReplace => "page.replace",
            Self::PageArchive => "page.archive",
            Self::FileAttach => "file.attach",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOutcome {
    Applied,
    Rejected,
    Failed,
    Ambiguous,
}

impl AuditOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Ambiguous => "ambiguous",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeAuditId(String);

impl SafeAuditId {
    pub fn parse(value: &str) -> Result<Self, AuditError> {
        // Strict opaque IDs. This rejects URLs, paths, query parameters,
        // free-form user text, whitespace and authorization header values.
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b':' | b'.'))
        {
            return Err(AuditError::InvalidMetadata);
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileAuditMetadata {
    pub bytes: u64,
    /// MIME only; never filename, local path, remote download URL or content.
    pub mime_type: String,
}

impl FileAuditMetadata {
    pub fn new(bytes: u64, mime_type: &str) -> Result<Self, AuditError> {
        if mime_type.len() > 100
            || !mime_type.contains('/')
            || mime_type.starts_with('/')
            || mime_type.ends_with('/')
            || !mime_type
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'/' | b'-' | b'+' | b'.'))
        {
            return Err(AuditError::InvalidMetadata);
        }
        Ok(Self {
            bytes,
            mime_type: mime_type.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub at_unix_seconds: i64,
    pub actor: SafeAuditId,
    pub action: AuditAction,
    pub target: SafeAuditId,
    pub outcome: AuditOutcome,
    pub correlation_id: SafeAuditId,
    pub file: Option<FileAuditMetadata>,
}

impl AuditEvent {
    pub fn new(
        at_unix_seconds: i64,
        actor: &str,
        action: AuditAction,
        target: &str,
        outcome: AuditOutcome,
        correlation_id: &str,
        file: Option<FileAuditMetadata>,
    ) -> Result<Self, AuditError> {
        if at_unix_seconds < 0 || (file.is_some() != (action == AuditAction::FileAttach)) {
            return Err(AuditError::InvalidMetadata);
        }
        Ok(Self {
            at_unix_seconds,
            actor: SafeAuditId::parse(actor)?,
            action,
            target: SafeAuditId::parse(target)?,
            outcome,
            correlation_id: SafeAuditId::parse(correlation_id)?,
            file,
        })
    }
}

/// No external I/O failures or rejected user input is ever embedded in errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditError {
    InvalidMetadata,
    Unavailable,
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMetadata => f.write_str("invalid audit metadata"),
            Self::Unavailable => f.write_str("audit store unavailable"),
        }
    }
}

impl Error for AuditError {}

/// An agent mutation adapter must explicitly opt into this sink. In fail-closed
/// mode a failed append must block further effects; a post-mutation append
/// failure cannot roll back a remote Notion change and must be reconciled.
pub trait AuditSink: Send + Sync {
    fn append(&self, event: &AuditEvent, now_unix_seconds: i64) -> Result<(), AuditError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_audit_fields_reject_private_payloads() {
        for value in [
            "",
            "https://files.example/a?token=secret",
            "Bearer secret",
            "token=supersecret",
            "file/name.png",
            "private page body\nhello",
            "x".repeat(129).as_str(),
        ] {
            assert!(SafeAuditId::parse(value).is_err());
        }
        let file = FileAuditMetadata::new(123, "image/png").unwrap();
        assert!(AuditEvent::new(
            17, "agent-1", AuditAction::FileAttach, "page-1",
            AuditOutcome::Applied, "corr-1", Some(file),
        ).is_ok());
        assert!(AuditEvent::new(
            17, "agent-1", AuditAction::PageAppend, "page-1",
            AuditOutcome::Applied, "corr-1",
            Some(FileAuditMetadata::new(1, "image/png").unwrap()),
        ).is_err());
        for mime in ["file:///secret", "image/png?sig=123", "text/plain; charset=utf-8"] {
            assert!(FileAuditMetadata::new(12, mime).is_err());
        }
    }

    #[test]
    fn audit_debug_contains_only_allowlisted_metadata() {
        let event = AuditEvent::new(
            42, "agent-1", AuditAction::PageCreate, "page-1",
            AuditOutcome::Ambiguous, "request-7", None,
        ).unwrap();
        let output = format!("{event:?}");
        assert!(output.contains("page-1"));
        assert!(!output.contains("markdown"));
        assert!(!output.contains("Authorization"));
    }
}
