//! Request-scoped audited native-file application service, independent of HTTP.
use crate::{
    audit::{AuditActor, AuditEvent, AuditOutcome, AuditStore, AuditTool, FileKind, SafeFileFacts},
    backend::{BackendErrorKind, PageId},
    root_scope::RootScopeGate,
};
use std::future::Future;

/// Incoming metadata confers no authority. Never persist or debug this request.
pub struct FileIngestRequest {
    pub target: PageId,
    pub temporary_url: String,
    pub filename: Option<String>,
    pub declared_mime: Option<String>,
    pub caption: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestFailure {
    Denied,
    InvalidInput,
    SourceExpired,
    DownloadUnavailable,
    Upstream(BackendErrorKind),
    AuditUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReferences {
    pub target: Option<PageId>,
    pub upload_id: Option<String>,
    pub block_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestStatus {
    Failed,
    Denied,
    Indeterminate,
    Verified,
    VerifiedAuditFailed,
}

/// Verified external effects remain verified even if their terminal audit fails.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileIngestResult {
    pub references: FileReferences,
    pub status: IngestStatus,
    pub failure: Option<IngestFailure>,
    pub terminal_audited: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEffectFailure {
    pub operation: &'static str,
    pub kind: BackendErrorKind,
    pub upload_id: Option<String>,
    pub block_id: Option<String>,
    pub effect_possible: bool,
}

/// Adapter-owned local files must unlink on drop, including dropped futures.
/// Implementations validate observed MIME, declaration, bytes, name and size.
pub trait FileIngestAdapter: Send + Sync {
    fn configured_scope(&self) -> &crate::lifecycle::LifecycleScope;
    type LocalFile: Send + Sync;
    type Upload: Send + Sync;
    fn download(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<Self::LocalFile, IngestFailure>> + Send;
    fn validate(
        &self,
        file: &Self::LocalFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> impl Future<Output = Result<SafeFileFacts, IngestFailure>> + Send;
    fn upload(
        &self,
        file: &Self::LocalFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> impl Future<Output = Result<Self::Upload, FileEffectFailure>> + Send;
    fn upload_id<'a>(&self, upload: &'a Self::Upload) -> &'a str;
    fn attach(
        &self,
        target: &PageId,
        upload: &Self::Upload,
        caption: Option<&str>,
        kind: FileKind,
    ) -> impl Future<Output = Result<String, FileEffectFailure>> + Send;
}

/// Construct from trusted transport authentication and operator policy only.
/// No caller-provided identity or operation permission belongs in this context.
pub struct FileIngestContext<'a> {
    pub gate: &'a RootScopeGate,
    pub audit: &'a dyn AuditStore,
    pub actor: AuditActor,
    pub correlation_id: &'a str,
    pub permitted: bool,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct AuditAttempt<'a> {
    context: &'a FileIngestContext<'a>,
    target: &'a PageId,
    facts: Option<SafeFileFacts>,
    effect: bool,
    finished: bool,
}
impl AuditAttempt<'_> {
    fn record(&self, outcome: AuditOutcome) -> bool {
        let time = now();
        AuditEvent::new(
            time,
            self.context.actor,
            AuditTool::FileAttach,
            Some(self.target.0.clone()),
            outcome,
            self.context.correlation_id.to_owned(),
            self.facts,
        )
        .is_ok_and(|event| self.context.audit.record(&event, time).is_ok())
    }
    fn finish(&mut self, result: &mut FileIngestResult) {
        let outcome = match result.status {
            IngestStatus::Verified | IngestStatus::VerifiedAuditFailed => AuditOutcome::Succeeded,
            IngestStatus::Denied => AuditOutcome::Denied,
            IngestStatus::Failed => AuditOutcome::Failed,
            IngestStatus::Indeterminate => AuditOutcome::Indeterminate,
        };
        result.terminal_audited = self.record(outcome);
        self.finished = true;
        if !result.terminal_audited && result.status == IngestStatus::Verified {
            result.status = IngestStatus::VerifiedAuditFailed;
        }
        if !result.terminal_audited && result.failure.is_none() {
            result.failure = Some(IngestFailure::AuditUnavailable);
        }
    }
}
impl Drop for AuditAttempt<'_> {
    fn drop(&mut self) {
        // Cancellation cannot claim rollback. An intent without a terminal row
        // after a process crash likewise requires explicit reconciliation.
        if !self.finished {
            self.record(if self.effect {
                AuditOutcome::Indeterminate
            } else {
                AuditOutcome::Failed
            });
        }
    }
}

/// Execute once. There is no background queue, source URL replay or write retry.
pub async fn ingest_file<A: FileIngestAdapter>(
    adapter: &A,
    context: &FileIngestContext<'_>,
    request: FileIngestRequest,
) -> FileIngestResult {
    let canonical_target = request.target.0.len() == 36
        && request.target.0.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit() && !b.is_ascii_uppercase(),
        });
    let mut result = FileIngestResult {
        references: FileReferences {
            target: canonical_target.then(|| request.target.clone()),
            upload_id: None,
            block_id: None,
        },
        status: IngestStatus::Failed,
        failure: None,
        terminal_audited: false,
    };
    if !canonical_target {
        result.failure = Some(IngestFailure::InvalidInput);
        return result;
    }
    let mut attempt = AuditAttempt {
        context,
        target: &request.target,
        facts: None,
        effect: false,
        finished: true,
    };
    // Validate metadata needed for durable admission before any adapter effect.
    if !attempt.record(AuditOutcome::Attempted) {
        result.failure = Some(IngestFailure::AuditUnavailable);
        return result;
    }
    attempt.finished = false;
    let execution = async {
        if !context.permitted || adapter.configured_scope() != context.gate.configured_scope() {
            return Err(IngestFailure::Denied);
        }
        let permit = context
            .gate
            .authorize(&request.target)
            .await
            .map_err(|_| IngestFailure::Denied)?;
        if request.caption.as_deref().is_some_and(|value| value.chars().count() > 2000 || value.chars().any(|ch| ch.is_control() || matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))) { return Err(IngestFailure::InvalidInput); }
        let file = adapter.download(&request.temporary_url).await?;
        let facts = adapter
            .validate(
                &file,
                request.filename.as_deref(),
                request.declared_mime.as_deref(),
            )
            .await?;
        attempt.facts = Some(facts);
        // Audit availability is checked again immediately before Notion upload.
        if !attempt.record(AuditOutcome::Attempted) {
            return Err(IngestFailure::AuditUnavailable);
        }
        context
            .gate
            .revalidate(&permit, context.gate.configured_scope())
            .await
            .map_err(|_| IngestFailure::Denied)?;
        attempt.effect = true;
        let uploaded = adapter
            .upload(
                &file,
                request.filename.as_deref(),
                request.declared_mime.as_deref(),
            )
            .await
            .map_err(|failure| {
                result.references.upload_id = failure.upload_id;
                result.references.block_id = failure.block_id;
                result.status = if failure.effect_possible {
                    IngestStatus::Indeterminate
                } else {
                    IngestStatus::Failed
                };
                IngestFailure::Upstream(failure.kind)
            })?;
        result.references.upload_id = Some(adapter.upload_id(&uploaded).to_owned());
        context
            .gate
            .revalidate(&permit, context.gate.configured_scope())
            .await
            .map_err(|_| IngestFailure::Denied)?;
        let block = adapter
            .attach(
                &request.target,
                &uploaded,
                request.caption.as_deref().map(str::trim),
                facts.kind(),
            )
            .await
            .map_err(|failure| {
                result.references.block_id = failure.block_id;
                result.status = if failure.effect_possible {
                    IngestStatus::Indeterminate
                } else {
                    IngestStatus::Failed
                };
                IngestFailure::Upstream(failure.kind)
            })?;
        result.references.block_id = Some(block);
        result.status = IngestStatus::Verified;
        Ok(())
    };
    let execution = execution.await;
    if let Err(failure) = execution {
        if failure == IngestFailure::Denied {
            result.status = IngestStatus::Denied;
        }
        result.failure = Some(failure);
    }
    attempt.finish(&mut result);
    result
}
