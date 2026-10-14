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
    fn upload_tracked(
        &self,
        file: &Self::LocalFile,
        name: Option<&str>,
        mime: Option<&str>,
        references: &mut FileReferences,
    ) -> impl Future<Output = Result<Self::Upload, FileEffectFailure>> + Send {
        async move {
            let upload = self.upload(file, name, mime).await?;
            references.upload_id = Some(self.upload_id(&upload).to_owned());
            Ok(upload)
        }
    }
    fn attach_tracked(
        &self,
        target: &PageId,
        upload: &Self::Upload,
        caption: Option<&str>,
        kind: FileKind,
        references: &mut FileReferences,
    ) -> impl Future<Output = Result<String, FileEffectFailure>> + Send {
        async move {
            let block = self.attach(target, upload, caption, kind).await?;
            references.block_id = Some(block.clone());
            Ok(block)
        }
    }
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
    let mut result = empty_result(&request.target);
    ingest_file_into(adapter, context, request, &mut result).await;
    result
}
fn empty_result(target: &PageId) -> FileIngestResult {
    FileIngestResult {
        references: FileReferences {
            target: canonical_target(target).then(|| target.clone()),
            upload_id: None,
            block_id: None,
        },
        status: IngestStatus::Failed,
        failure: None,
        terminal_audited: false,
    }
}
async fn ingest_file_into<A: FileIngestAdapter>(
    adapter: &A,
    context: &FileIngestContext<'_>,
    request: FileIngestRequest,
    result: &mut FileIngestResult,
) {
    let canonical_target = canonical_target(&request.target);
    if !canonical_target {
        result.failure = Some(IngestFailure::InvalidInput);
        return;
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
        return;
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
        result.status = IngestStatus::Indeterminate;
        let uploaded = adapter
            .upload_tracked(
                &file,
                request.filename.as_deref(),
                request.declared_mime.as_deref(),
                &mut result.references,
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
            .attach_tracked(
                &request.target,
                &uploaded,
                request.caption.as_deref().map(str::trim),
                facts.kind(),
                &mut result.references,
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
    attempt.finish(result);
}

/// Trusted server-owned request count; callers cannot construct an unchecked cap.
#[derive(Clone, Copy, Debug)]
pub struct FileBatchPolicy {
    maximum_files: usize,
}
impl FileBatchPolicy {
    pub const HARD_CAP: usize = 20;
    pub fn new(maximum_files: usize) -> Result<Self, IngestFailure> {
        if !(1..=Self::HARD_CAP).contains(&maximum_files) {
            return Err(IngestFailure::InvalidInput);
        }
        Ok(Self { maximum_files })
    }
}
impl Default for FileBatchPolicy {
    fn default() -> Self {
        Self { maximum_files: 5 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedFileResult {
    pub index: usize,
    /// None means unattempted. Results never contain temporary source URLs.
    pub outcome: Option<FileIngestResult>,
}

/// Request-owned ordered work and observable progress. Keep this object outside
/// the execution future: canceling a later item preserves earlier receipts.
/// Execution is one-shot, including after cancellation; it cannot replay URLs.
pub struct FileIngestBatch {
    requests: Option<Vec<FileIngestRequest>>,
    results: Vec<IndexedFileResult>,
}
impl FileIngestBatch {
    pub fn new(
        requests: Vec<FileIngestRequest>,
        policy: FileBatchPolicy,
    ) -> Result<Self, IngestFailure> {
        if requests.is_empty() || requests.len() > policy.maximum_files {
            return Err(IngestFailure::InvalidInput);
        }
        let target = &requests[0].target;
        if requests.iter().any(|r| {
            &r.target != target
                || !canonical_target(&r.target)
                || r.temporary_url.is_empty()
                || r.temporary_url.len() > 8192
                || r.filename.as_ref().is_some_and(|v| v.len() > 4096)
                || r.declared_mime.as_ref().is_some_and(|v| v.len() > 128)
                || !valid_caption(r.caption.as_deref())
        }) {
            return Err(IngestFailure::InvalidInput);
        }
        let results = (0..requests.len())
            .map(|index| IndexedFileResult {
                index,
                outcome: None,
            })
            .collect();
        Ok(Self {
            requests: Some(requests),
            results,
        })
    }
    pub fn results(&self) -> &[IndexedFileResult] {
        &self.results
    }
    pub fn partial_success(&self) -> bool {
        let verified = self
            .results
            .iter()
            .filter(|r| {
                r.outcome.as_ref().is_some_and(|r| {
                    matches!(
                        r.status,
                        IngestStatus::Verified | IngestStatus::VerifiedAuditFailed
                    )
                })
            })
            .count();
        verified > 0 && verified < self.results.len()
    }
    pub async fn execute<A: FileIngestAdapter>(
        &mut self,
        adapter: &A,
        context: &FileIngestContext<'_>,
    ) -> Result<(), IngestFailure> {
        let requests = self.requests.take().ok_or(IngestFailure::InvalidInput)?;
        // Common authority is checked before any item admission/download.
        if !context.permitted
            || adapter.configured_scope() != context.gate.configured_scope()
            || context.gate.authorize(&requests[0].target).await.is_err()
        {
            return Err(IngestFailure::Denied);
        }
        for (index, request) in requests.into_iter().enumerate() {
            self.results[index].outcome = Some(empty_result(&request.target));
            let outcome = self.results[index]
                .outcome
                .as_mut()
                .expect("initialized item");
            ingest_file_into(adapter, context, request, outcome).await;
            let stop = !outcome.terminal_audited
                || matches!(
                    outcome.failure,
                    Some(
                        IngestFailure::Denied
                            | IngestFailure::AuditUnavailable
                            | IngestFailure::Upstream(
                                BackendErrorKind::Unauthenticated
                                    | BackendErrorKind::PermissionDenied
                            )
                    )
                );
            if stop {
                break;
            }
        }
        Ok(())
    }
}
fn canonical_target(target: &PageId) -> bool {
    target.0.len() == 36
        && target.0.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit() && !b.is_ascii_uppercase(),
        })
}
fn valid_caption(value: Option<&str>) -> bool {
    value.is_none_or(|v| v.chars().count() <= 2000 && !v.chars().any(|ch| ch.is_control()
        || matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')))
}
