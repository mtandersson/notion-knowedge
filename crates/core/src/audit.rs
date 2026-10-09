//! Payload-free audit contract for future authenticated agent mutations.
//! No page body, query, filename, URL, token, or arbitrary error string can be
//! represented by an AuditEvent. Record from trusted server-owned metadata only.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditActor {
    ApprovedUser,
    Automation,
    Server,
}

impl AuditActor {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApprovedUser => "approved_user",
            Self::Automation => "automation",
            Self::Server => "server",
        }
    }
}

/// A finite semantic operation set. Do not use a free-form tool or method name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditTool {
    PageCreate,
    PageAppend,
    PageReplace,
    PageDelete,
    PageMove,
    FileAttach,
}

impl AuditTool {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PageCreate => "page_create",
            Self::PageAppend => "page_append",
            Self::PageReplace => "page_replace",
            Self::PageDelete => "page_delete",
            Self::PageMove => "page_move",
            Self::FileAttach => "file_attach",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    Succeeded,
    Denied,
    Failed,
    /// Transport failure or timeout after an effect may have occurred.
    Indeterminate,
}

impl AuditOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Denied => "denied",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Image,
    Other,
}

impl FileKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Other => "other",
        }
    }
}

/// Whitelisted file facts only: no name, MIME parameters, temporary/signed URL,
/// bytes, hash of content, or upstream error response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SafeFileFacts {
    kind: FileKind,
    size_bytes: u64,
}

impl SafeFileFacts {
    pub fn new(kind: FileKind, size_bytes: u64) -> Self {
        Self { kind, size_bytes }
    }

    pub fn kind(&self) -> FileKind {
        self.kind
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditError {
    InvalidInput,
    Unavailable,
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "invalid audit metadata",
            Self::Unavailable => "audit store unavailable",
        })
    }
}

impl std::error::Error for AuditError {}

/// Only a Notion UUID is allowed in the target slot; free-form titles, links,
/// paths and email addresses are not an admissible substitute.
fn valid_notion_id(id: &str) -> bool {
    if id.len() == 32 {
        id.bytes().all(|b| b.is_ascii_hexdigit())
    } else if id.len() == 36 {
        id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    } else {
        false
    }
}

/// Owned, schema-limited audit record. Fields are private by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEvent {
    occurred_at_unix: i64,
    actor: AuditActor,
    tool: AuditTool,
    target_page_id: Option<String>,
    outcome: AuditOutcome,
    correlation_id: String,
    file: Option<SafeFileFacts>,
}

impl AuditEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        occurred_at_unix: i64,
        actor: AuditActor,
        tool: AuditTool,
        target_page_id: Option<String>,
        outcome: AuditOutcome,
        correlation_id: String,
        file: Option<SafeFileFacts>,
    ) -> Result<Self, AuditError> {
        // Correlation IDs must be minted by the trusted server (not derived from
        // client content). No whitespace, control bytes or URL punctuation.
        if occurred_at_unix < 0
            || !(16..=64).contains(&correlation_id.len())
            || !correlation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || target_page_id
                .as_deref()
                .is_some_and(|id| !valid_notion_id(id))
            || (file.is_some() && tool != AuditTool::FileAttach)
        {
            return Err(AuditError::InvalidInput);
        }
        Ok(Self {
            occurred_at_unix,
            actor,
            tool,
            target_page_id,
            outcome,
            correlation_id,
            file,
        })
    }

    pub fn occurred_at_unix(&self) -> i64 {
        self.occurred_at_unix
    }
    pub fn actor(&self) -> AuditActor {
        self.actor
    }
    pub fn tool(&self) -> AuditTool {
        self.tool
    }
    pub fn target_page_id(&self) -> Option<&str> {
        self.target_page_id.as_deref()
    }
    pub fn outcome(&self) -> AuditOutcome {
        self.outcome
    }
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
    pub fn file(&self) -> Option<SafeFileFacts> {
        self.file
    }
}

/// Configured bounded retention; an event is deleted once occurred_at <= cutoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditRetention {
    days: i64,
}

impl AuditRetention {
    pub fn days(days: i64) -> Result<Self, AuditError> {
        if !(1..=3650).contains(&days) {
            return Err(AuditError::InvalidInput);
        }
        Ok(Self { days })
    }

    pub fn cutoff(self, now_unix: i64) -> Result<i64, AuditError> {
        if now_unix < 0 {
            return Err(AuditError::InvalidInput);
        }
        now_unix
            .checked_sub(self.days * 86_400)
            .ok_or(AuditError::InvalidInput)
    }

    pub fn configured_days(self) -> i64 {
        self.days
    }
}

impl Default for AuditRetention {
    fn default() -> Self {
        Self { days: 30 }
    }
}

/// Append must be durable before success and prune expired rows atomically.
/// If this returns an error, the caller must not claim the operation is audited.
/// Before a mutation, fail closed. After an ambiguous effect, reconcile rather
/// than automatically replaying a non-idempotent Notion write.
pub trait AuditStore: Send + Sync {
    fn record(&self, event: &AuditEvent, trusted_now_unix: i64) -> Result<(), AuditError>;
    fn prune(&self, trusted_now_unix: i64) -> Result<u64, AuditError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tool: AuditTool, target: Option<String>, file: Option<SafeFileFacts>) -> Result<AuditEvent, AuditError> {
        AuditEvent::new(
            1_800_000_000,
            AuditActor::ApprovedUser,
            tool,
            target,
            AuditOutcome::Indeterminate,
            "request_0123456789abcdef".to_owned(),
            file,
        )
    }

    #[test]
    fn only_canonical_identifiers_and_file_facts_can_enter_audit() {
        assert!(event(
            AuditTool::FileAttach,
            Some("01234567-89ab-cdef-0123-456789abcdef".to_owned()),
            Some(SafeFileFacts::new(FileKind::Image, 2048)),
        ).is_ok());
        for target in ["secret body", "https://example.com/?token=secret", "admin@example.com", "01234567-89ab-cdef-0123-456789abcdeg"] {
            assert_eq!(event(AuditTool::PageReplace, Some(target.to_owned()), None), Err(AuditError::InvalidInput));
        }
        assert_eq!(event(AuditTool::PageCreate, None, Some(SafeFileFacts::new(FileKind::Other, 100))), Err(AuditError::InvalidInput));
        assert_eq!(AuditEvent::new(1, AuditActor::Server, AuditTool::PageDelete, None,
            AuditOutcome::Denied, "private text / token".into(), None), Err(AuditError::InvalidInput));
    }

    #[test]
    fn retention_is_bounded_and_defaults_to_thirty_days() {
        let policy = AuditRetention::default();
        assert_eq!(policy.configured_days(), 30);
        assert_eq!(policy.cutoff(2_592_001), Ok(1));
        assert_eq!(AuditRetention::days(0), Err(AuditError::InvalidInput));
        assert_eq!(AuditRetention::days(3651), Err(AuditError::InvalidInput));
        assert_eq!(policy.cutoff(-1), Err(AuditError::InvalidInput));
    }

    #[test]
    fn errors_never_echo_sensitive_inputs() {
        assert_eq!(AuditError::Unavailable.to_string(), "audit store unavailable");
        assert_eq!(AuditError::InvalidInput.to_string(), "invalid audit metadata");
    }
}
