//! Fail-closed, server-side confirmation gate for destructive Notion writes (#82).
//!
//! This policy cannot authorize a Notion target or grant MCP access. Callers
//! must separately enforce physical root scope, revision and idempotency.
//! Confirmation decisions MUST originate in a trusted server-owned provider,
//! never a caller-supplied boolean, confirmation ID or tool argument.

use std::{fmt, future::Future, pin::Pin};

pub fn confirmation_scope(
    scope: &crate::lifecycle::LifecycleScope,
) -> Result<String, ConfirmationError> {
    use crate::discovery::SourceType;
    let source_types: Vec<_> = scope
        .exclusions
        .source_types
        .iter()
        .map(|kind| match kind {
            SourceType::Page => "page",
            SourceType::Database => "database",
            SourceType::DataSource => "data_source",
        })
        .collect();
    let bytes = serde_json::to_vec(&serde_json::json!({
        "workspace": scope.workspace_id, "generation": scope.generation,
        "roots": scope.roots.iter().map(|root| &root.0).collect::<Vec<_>>(),
        "excluded_pages": scope.exclusions.page_ids,
        "excluded_descendants": scope.exclusions.descendants_of,
        "excluded_types": source_types,
    }))
    .map_err(|_| ConfirmationError::InvalidRequest)?;
    Ok(format!(
        "scope-{}",
        crate::idempotency::MutationClaim::payload_sha256(&bytes)
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOperation {
    CreatePage,
    Append,
    UpdateSection,
    ReplaceContent,
    ArchivePage,
    DeletePage,
}

impl WriteOperation {
    pub fn is_destructive(self) -> bool {
        matches!(
            self,
            Self::UpdateSection | Self::ReplaceContent | Self::ArchivePage | Self::DeletePage
        )
    }

    pub fn from_tool_name(name: &str) -> Option<Self> {
        match name {
            "knowledge_create_page" => Some(Self::CreatePage),
            "knowledge_append" => Some(Self::Append),
            "knowledge_update_section" => Some(Self::UpdateSection),
            "knowledge_archive_page" => Some(Self::ArchivePage),
            _ => None,
        }
    }
}

/// Bound to an independently authorized scope and the complete intended
/// mutation (represented by an application-computed SHA-256 fingerprint).
/// Never construct this object directly from untrusted MCP JSON.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfirmationRequest {
    operation: WriteOperation,
    trusted_scope: String,
    target_id: String,
    request_sha256: String,
    idempotency_key: String,
    section_preview: Option<SectionConfirmationPreview>,
}

/// Server-generated readable effect, displayed alongside the exact fingerprint.
/// Never populated from a client-supplied approval receipt.
#[derive(Clone, PartialEq, Eq)]
pub struct SectionConfirmationPreview {
    pub section_anchor: String,
    pub original_body: String,
    pub replacement_body: String,
}

impl fmt::Debug for ConfirmationRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfirmationRequest([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmationError {
    InvalidRequest,
    Disabled,
    Unavailable,
    Declined,
    Mismatch,
}

impl fmt::Display for ConfirmationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "destructive operation not authorized: {self:?}")
    }
}

impl std::error::Error for ConfirmationError {}

impl ConfirmationRequest {
    pub fn new(
        operation: WriteOperation,
        trusted_scope: &str,
        target_id: &str,
        request_sha256: &str,
        idempotency_key: &str,
    ) -> Result<Self, ConfirmationError> {
        let identifier = |s: &str, min: usize, max: usize| {
            (min..=max).contains(&s.len())
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
        };
        if !operation.is_destructive()
            || !identifier(trusted_scope, 1, 128)
            || !identifier(target_id, 1, 128)
            || !identifier(idempotency_key, 16, 128)
            || request_sha256.len() != 64
            || !request_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(ConfirmationError::InvalidRequest);
        }
        Ok(Self {
            operation,
            trusted_scope: trusted_scope.to_owned(),
            target_id: target_id.to_owned(),
            request_sha256: request_sha256.to_ascii_lowercase(),
            idempotency_key: idempotency_key.to_owned(),
            section_preview: None,
        })
    }

    pub fn with_section_preview(
        mut self,
        preview: SectionConfirmationPreview,
    ) -> Result<Self, ConfirmationError> {
        if self.operation != WriteOperation::UpdateSection
            || preview.section_anchor.is_empty()
            || preview.section_anchor.len() > 1024
            || preview.original_body.len() > 1024 * 1024
            || preview.replacement_body.len() > 200_000
        {
            return Err(ConfirmationError::InvalidRequest);
        }
        self.section_preview = Some(preview);
        Ok(self)
    }

    pub fn section_preview(&self) -> Option<&SectionConfirmationPreview> {
        self.section_preview.as_ref()
    }

    pub fn operation(&self) -> WriteOperation {
        self.operation
    }

    pub fn trusted_scope(&self) -> &str {
        &self.trusted_scope
    }

    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmationDecision {
    Approved,
    Declined,
    Unavailable,
}

pub type ConfirmationFuture<'a> = Pin<Box<dyn Future<Output = ConfirmationDecision> + Send + 'a>>;

/// Implement only with a trusted operator/interactive confirmation backend.
/// A client-submitted confirmation ID must be validated by that backend,
/// including scope, operation, target, revision and one-time consumption.
pub trait ConfirmationProvider: Send + Sync {
    fn decide<'a>(&'a self, request: &'a ConfirmationRequest) -> ConfirmationFuture<'a>;
}

/// Not serializable or cloneable; grants one *local* use for the exact request.
/// The durable write ledger and actual target authorization remain mandatory.
pub struct ConfirmationPermit(ConfirmationRequest);

impl fmt::Debug for ConfirmationPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfirmationPermit([REDACTED])")
    }
}

impl ConfirmationPermit {
    /// Consumes the approved permit. It cannot be replayed or retargeted.
    pub fn consume(self, request: &ConfirmationRequest) -> Result<(), ConfirmationError> {
        if &self.0 != request {
            return Err(ConfirmationError::Mismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DestructivePolicy {
    enabled: bool,
}

impl DestructivePolicy {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub fn enabled(self) -> bool {
        self.enabled
    }

    /// Exactly one confirmation attempt; no implicit reprompt or retry on a
    /// refusal, transport failure, timeout or missing UI confirmation support.
    pub async fn require_confirmation(
        &self,
        request: &ConfirmationRequest,
        provider: Option<&dyn ConfirmationProvider>,
    ) -> Result<ConfirmationPermit, ConfirmationError> {
        if !self.enabled {
            return Err(ConfirmationError::Disabled);
        }
        let provider = provider.ok_or(ConfirmationError::Unavailable)?;
        match provider.decide(request).await {
            ConfirmationDecision::Approved => Ok(ConfirmationPermit(request.clone())),
            ConfirmationDecision::Declined => Err(ConfirmationError::Declined),
            ConfirmationDecision::Unavailable => Err(ConfirmationError::Unavailable),
        }
    }
}
