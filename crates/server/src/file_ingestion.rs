//! Validated operator-owned admission settings and installed audited ingestion.
use crate::config::{ConfigError, invalid, optional, text};
use notion_knowledge_core::audit::AuditRetention;
use notion_knowledge_notion::{file_download::DownloadPolicy, file_multipart::MultipartPolicy};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone)]
pub struct Settings {
    pub download: DownloadPolicy,
    pub maximum_bytes: usize,
    pub multipart: Option<MultipartPolicy>,
    pub audit_file: PathBuf,
    pub retention: AuditRetention,
    #[cfg(test)]
    pub source_clock: Option<std::sync::Arc<dyn Fn() -> i64 + Send + Sync>>,
    #[cfg(test)]
    pub request_completion: Option<std::sync::Arc<tokio::sync::Notify>>,
}
impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileIngestionSettings([REDACTED])")
    }
}
impl Settings {
    pub(crate) fn parse(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        read_only: bool,
        authorized: bool,
        scope: bool,
    ) -> Result<Option<Self>, ConfigError> {
        let enabled = match optional(lookup, "NK_FILE_INGESTION", "false")?.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(invalid("NK_FILE_INGESTION", "must be true or false")),
        };
        const KEYS: [&str; 8] = [
            "NK_FILE_SOURCE_HOSTS",
            "NK_FILE_TIMEOUT_SECONDS",
            "NK_FILE_PLAN",
            "NK_FILE_MAX_BYTES",
            "NK_FILE_MULTIPART_THRESHOLD_BYTES",
            "NK_FILE_PART_BYTES",
            "NK_FILE_AUDIT_FILE",
            "NK_FILE_AUDIT_RETENTION_DAYS",
        ];
        if !enabled {
            for key in KEYS {
                if lookup(key).is_some() {
                    return Err(invalid(key, "requires enabled file ingestion"));
                }
            }
            return Ok(None);
        }
        if read_only || !authorized || !scope {
            return Err(invalid(
                "NK_FILE_INGESTION",
                "requires write mode, authorized provider and authoritative physical scope",
            ));
        }
        let number = |lookup: &mut dyn FnMut(&str) -> Option<OsString>, key, default, maximum| {
            optional(lookup, key, default)?
                .parse::<usize>()
                .ok()
                .filter(|n| (1..=maximum).contains(n))
                .ok_or_else(|| invalid(key, "must be an integer within the documented range"))
        };
        let paid = match optional(lookup, "NK_FILE_PLAN", "free")?.as_str() {
            "free" => false,
            "paid" => true,
            _ => return Err(invalid("NK_FILE_PLAN", "must be free or paid")),
        };
        let maximum_bytes = number(
            lookup,
            "NK_FILE_MAX_BYTES",
            "5242880",
            if paid { 104857600 } else { 5242880 },
        )?;
        let timeout = number(lookup, "NK_FILE_TIMEOUT_SECONDS", "30", 120)?;
        let hosts = lookup("NK_FILE_SOURCE_HOSTS")
            .ok_or_else(|| invalid("NK_FILE_SOURCE_HOSTS", "required for file ingestion"))?;
        let hosts = text(hosts, "NK_FILE_SOURCE_HOSTS")?;
        let hosts: Vec<_> = hosts.split(',').collect();
        let download =
            DownloadPolicy::new(&hosts, maximum_bytes, Duration::from_secs(timeout as u64))
                .map_err(|_| {
                    invalid(
                        "NK_FILE_SOURCE_HOSTS",
                        "must contain 1 to 32 exact HTTPS DNS hostnames",
                    )
                })?;
        let threshold = lookup("NK_FILE_MULTIPART_THRESHOLD_BYTES");
        let part = lookup("NK_FILE_PART_BYTES");
        let multipart = match (threshold, part) {
            (None, None) if maximum_bytes <= 20971520 => None,
            (Some(t), Some(p)) if paid => Some(
                MultipartPolicy::new(
                    maximum_bytes,
                    text(t, "NK_FILE_MULTIPART_THRESHOLD_BYTES")?
                        .parse()
                        .map_err(|_| {
                            invalid("NK_FILE_MULTIPART_THRESHOLD_BYTES", "invalid integer")
                        })?,
                    text(p, "NK_FILE_PART_BYTES")?
                        .parse()
                        .map_err(|_| invalid("NK_FILE_PART_BYTES", "invalid integer"))?,
                )
                .map_err(|_| {
                    invalid(
                        "NK_FILE_MULTIPART_THRESHOLD_BYTES",
                        "invalid multipart byte or part count bounds",
                    )
                })?,
            ),
            _ => {
                return Err(invalid(
                    "NK_FILE_MULTIPART_THRESHOLD_BYTES",
                    "requires paired bounds, paid plan and compatible maximum",
                ));
            }
        };
        let audit_file = lookup("NK_FILE_AUDIT_FILE")
            .ok_or_else(|| invalid("NK_FILE_AUDIT_FILE", "required protected durable path"))?;
        let audit_file = PathBuf::from(text(audit_file, "NK_FILE_AUDIT_FILE")?);
        validate_audit_path(&audit_file)?;
        let retention = AuditRetention::days(number(
            lookup,
            "NK_FILE_AUDIT_RETENTION_DAYS",
            "30",
            3650,
        )? as i64)
        .map_err(|_| invalid("NK_FILE_AUDIT_RETENTION_DAYS", "invalid retention"))?;
        Ok(Some(Self {
            download,
            maximum_bytes,
            multipart,
            audit_file,
            retention,
            #[cfg(test)]
            source_clock: None,
            #[cfg(test)]
            request_completion: None,
        }))
    }
}
pub(crate) fn validate_audit_path(path: &Path) -> Result<(), ConfigError> {
    let err = || {
        invalid(
            "NK_FILE_AUDIT_FILE",
            "must be an absolute private owner-controlled durable file path",
        )
    };
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(err());
    }
    let parent = path.parent().ok_or_else(err)?;
    // The existing parent must be private, preventing symlink/file replacement.
    let metadata = std::fs::symlink_metadata(parent).map_err(|_| err())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let own = std::fs::metadata("/proc/self").map_err(|_| err())?.uid();
        if !metadata.is_dir() || metadata.uid() != own || metadata.mode() & 0o077 != 0 {
            return Err(err());
        }
        for ancestor in parent.ancestors() {
            let m = std::fs::symlink_metadata(ancestor).map_err(|_| err())?;
            // Only root or this operator can replace an ancestor. Shared sticky
            // directories (such as /tmp) protect our owned child from others.
            if !m.is_dir()
                || (m.uid() != own && m.uid() != 0)
                || (m.mode() & 0o022 != 0 && m.mode() & 0o1000 == 0)
            {
                return Err(err());
            }
        }
        match std::fs::symlink_metadata(path) {
            Ok(m) if m.is_file() && m.uid() == own && m.mode() & 0o077 == 0 => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            _ => return Err(err()),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        return Err(err());
    }
    Ok(())
}

/// The real installed application: one immutable operator policy and durable
/// audit store, with request-owned one-shot batches and private temporary files.
pub(crate) struct Runtime {
    adapter: notion_knowledge_notion::file_ingest::NotionFileIngestAdapter,
    gate: notion_knowledge_core::root_scope::RootScopeGate,
    audit: notion_knowledge_retrieval::audit::SqliteAuditStore,
    policy: notion_knowledge_core::file_ingest::FileBatchPolicy,
    actor: notion_knowledge_core::audit::AuditActor,
    #[cfg(all(unix, feature = "webhook-worker"))]
    intent: Option<std::sync::Arc<crate::file_refresh::AuthorizedFileIntent>>,
    request_cancel: Option<tokio_util::sync::CancellationToken>,
}
impl Runtime {
    pub(crate) fn new(
        config: &crate::config::Config,
        settings: &Settings,
        client: notion_knowledge_notion::NotionClient,
    ) -> Result<Self, &'static str> {
        let scope = config
            .notion_scope
            .as_ref()
            .ok_or("file ingestion scope unavailable")?;
        Self::scoped(settings, client, scope, config.max_upload_files)
    }
    fn scoped(
        settings: &Settings,
        client: notion_knowledge_notion::NotionClient,
        scope: &notion_knowledge_core::lifecycle::LifecycleScope,
        maximum_files: usize,
    ) -> Result<Self, &'static str> {
        validate_audit_path(&settings.audit_file)
            .map_err(|_| "file ingestion audit path unavailable")?;
        let validation =
            notion_knowledge_notion::file_validation::FileValidationPolicy::for_single_part(
                settings
                    .multipart
                    .map_or(settings.maximum_bytes, |p| p.threshold_bytes()),
            )
            .map_err(|_| "file validation policy unavailable")?;
        let mut adapter = notion_knowledge_notion::file_ingest::NotionFileIngestAdapter::new(
            client.clone(),
            settings.download.clone(),
            validation,
            scope.clone(),
        );
        if let Some(policy) = settings.multipart {
            adapter = adapter.with_multipart(policy);
        }
        Ok(Self {
            adapter,
            gate: notion_knowledge_core::root_scope::RootScopeGate::new(
                std::sync::Arc::new(client),
                scope.clone(),
            )
            .map_err(|_| "file ingestion scope unavailable")?,
            audit: notion_knowledge_retrieval::audit::SqliteAuditStore::open(
                &settings.audit_file,
                settings.retention,
            )
            .map_err(|_| "file ingestion audit unavailable")?,
            actor: notion_knowledge_core::audit::AuditActor::Server,
            #[cfg(all(unix, feature = "webhook-worker"))]
            intent: None,
            request_cancel: None,
            policy: notion_knowledge_core::file_ingest::FileBatchPolicy::new(maximum_files)
                .map_err(|_| "file batch policy unavailable")?,
        })
    }
}
impl notion_knowledge_mcp::upload::FileIngestion for Runtime {
    fn ingest<'a>(
        &'a self,
        input: notion_knowledge_mcp::upload::UploadFileRequest,
        cancellation: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, &'static str>> + Send + 'a>,
    > {
        Box::pin(async move {
            use notion_knowledge_core::file_ingest::{
                FileIngestBatch, FileIngestContext, FileIngestRequest, IngestFailure, IngestStatus,
            };
            // The MCP handler already applied its effective operation policy.
            // Only trusted stdio or authenticated local HTTP installs this runtime.
            let requests = input
                .ordered_files()
                .into_iter()
                .map(|file| FileIngestRequest {
                    target: notion_knowledge_core::backend::PageId(input.target.clone()),
                    temporary_url: file.download_url.clone(),
                    filename: file.file_name.clone(),
                    declared_mime: file.mime_type.clone(),
                    caption: input.caption.clone(),
                })
                .collect();
            let mut batch = FileIngestBatch::new(requests, self.policy)
                .map_err(|_| "file_upload_invalid_input")?;
            let correlation = notion_knowledge_core::logging::CorrelationId::new();
            let correlation = correlation.as_str();
            let context = FileIngestContext {
                gate: &self.gate,
                audit: &self.audit,
                actor: self.actor,
                correlation_id: correlation,
                permitted: true,
            };
            // Keep the request-owned batch OUTSIDE this cancellable execution
            // future, so receipts and earlier results survive cancellation.
            let request_cancel = self.request_cancel.clone().unwrap_or_default();
            let renew = async {
                #[cfg(all(unix, feature = "webhook-worker"))]
                if let Some(owner) = &self.intent {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                        if owner.renew(120).is_err() {
                            return;
                        }
                    }
                }
                std::future::pending::<()>().await;
            };
            let (failure, cancelled) = tokio::select! {
                biased;
                _ = request_cancel.cancelled() => (None, true),
                _ = renew => (Some(IngestFailure::Denied), true),
                _ = cancellation => (None, true),
                result = batch.execute(&self.adapter, &context) => (result.err(), false),
            };
            let failure_name = |failure: IngestFailure| match failure {
                IngestFailure::Denied => "denied",
                IngestFailure::InvalidInput => "invalid_input",
                IngestFailure::SourceExpired => "source_expired",
                IngestFailure::DownloadUnavailable => "download_unavailable",
                IngestFailure::AuditUnavailable => "audit_unavailable",
                IngestFailure::Upstream(kind) => match kind {
                    notion_knowledge_core::backend::BackendErrorKind::Unauthenticated => {
                        "upstream_unauthenticated"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::PermissionDenied => {
                        "upstream_permission_denied"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::NotFound => {
                        "upstream_not_found"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::InvalidInput => {
                        "upstream_invalid_input"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::UnsupportedContent => {
                        "upstream_unsupported_content"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::Conflict => {
                        "upstream_conflict"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::RateLimited => {
                        "upstream_rate_limited"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::Unavailable => {
                        "upstream_unavailable"
                    }
                    notion_knowledge_core::backend::BackendErrorKind::Internal => {
                        "upstream_internal"
                    }
                },
            };
            let results: Vec<_> = batch.results().iter().map(|item| match &item.outcome {
                None => serde_json::json!({"index":item.index,"status":"unattempted","terminal_audited":false}),
                Some(outcome) => serde_json::json!({"index":item.index,"status":match outcome.status {
                    IngestStatus::Failed => "failed", IngestStatus::Denied => "denied", IngestStatus::Indeterminate => "indeterminate", IngestStatus::Verified => "verified", IngestStatus::VerifiedAuditFailed => "verified_audit_failed",
                },"target":outcome.references.target.as_ref().map(|p| &p.0),"upload_id":outcome.references.upload_id,"block_id":outcome.references.block_id,"terminal_audited":outcome.terminal_audited,"failure":outcome.failure.map(failure_name)}),
            }).collect();
            #[allow(unused_mut)]
            let mut results = results;
            #[cfg(all(unix, feature = "webhook-worker"))]
            if let Some(owner) = &self.intent {
                let possible = owner.mutation_attempted().unwrap_or(true);
                let uncertain = cancelled
                    || batch.results().iter().any(|item| {
                        item.outcome
                            .as_ref()
                            .is_some_and(|outcome| outcome.status == IngestStatus::Indeterminate)
                    });
                // A canceled/ambiguous network operation does not prove remote
                // quiescence. Keep its pre-admitted job held for reconciliation.
                let settled = !uncertain && owner.settle(possible).is_ok();
                let status = owner.status().unwrap_or_else(|_| serde_json::json!({"source_intent":"recovery_required","index_status":"unavailable"}));
                for (item, output) in batch.results().iter().zip(&mut results) {
                    let affected = item.outcome.as_ref().is_some_and(|result| {
                        result.references.upload_id.is_some()
                            || matches!(
                                result.status,
                                IngestStatus::Indeterminate
                                    | IngestStatus::Verified
                                    | IngestStatus::VerifiedAuditFailed
                            )
                    });
                    output["index_status"] = if !affected && settled {
                        serde_json::json!("not_needed")
                    } else if status["source_intent"] == "held" {
                        serde_json::json!("held")
                    } else {
                        status["index_status"].clone()
                    };
                    if affected || !settled {
                        output["source_intent"] = status["source_intent"].clone();
                        output["refresh"] = serde_json::json!({"workspace_id":owner.key().workspace_id,"page_id":owner.key().page_id,"mutation_digest":owner.key().mutation_digest});
                    }
                }
            }
            Ok(
                serde_json::json!({"files":results,"partial_success":batch.partial_success(),"failure":if cancelled {Some("cancelled")}else{failure.map(failure_name)},"cancelled":cancelled}),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    fn parse(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|key| {
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, v)| OsString::from(v))
        })
    }
    #[test]
    fn enabled_ingestion_requires_physical_scope_and_protected_audit_storage() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = dir.path().join("audit.sqlite");
        let path = path.to_str().unwrap();
        let mut settings = vec![
            ("NK_FILE_INGESTION", "true"),
            ("NK_READ_ONLY", "false"),
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "secret"),
            (
                "NK_NOTION_SCOPE_ROOTS",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
            (
                "NK_NOTION_SCOPE_WORKSPACE_ID",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            ),
            ("NK_FILE_SOURCE_HOSTS", "files.example.test"),
            ("NK_FILE_AUDIT_FILE", path),
        ];
        let config = parse(&settings).unwrap();
        let ingest = config.file_ingestion.unwrap();
        assert_eq!(ingest.maximum_bytes, 5242880);
        assert_eq!(ingest.retention.configured_days(), 30);
        assert!(ingest.multipart.is_none());
        assert!(!format!("{ingest:?}").contains(path));
        for (name, value) in [
            ("NK_FILE_TIMEOUT_SECONDS", "121"),
            ("NK_FILE_PLAN", "unknown"),
            ("NK_FILE_MAX_BYTES", "5242881"),
            ("NK_FILE_PART_BYTES", "5242880"),
            ("NK_FILE_AUDIT_RETENTION_DAYS", "0"),
            ("NK_FILE_SOURCE_HOSTS", "https://secret.test/path"),
        ] {
            let mut invalid_settings = settings.clone();
            invalid_settings.retain(|(key, _)| *key != name);
            invalid_settings.push((name, value));
            assert!(parse(&invalid_settings).is_err(), "{name}");
        }
        settings.push(("NK_FILE_PLAN", "paid"));
        settings.push(("NK_FILE_MAX_BYTES", "52428800"));
        settings.push(("NK_FILE_MULTIPART_THRESHOLD_BYTES", "20971520"));
        settings.push(("NK_FILE_PART_BYTES", "5242880"));
        assert_eq!(
            parse(&settings)
                .unwrap()
                .file_ingestion
                .unwrap()
                .multipart
                .unwrap()
                .parts(52428800)
                .unwrap(),
            10
        );
        assert!(validate_audit_path(Path::new("/tmp/audit.sqlite")).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn private_audit_directory_cannot_have_replaceable_ancestors() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let outer = tempfile::tempdir().unwrap();
        let private = outer.path().join("private");
        std::fs::create_dir(&private).unwrap();
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(outer.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(validate_audit_path(&private.join("audit.sqlite")).is_err());
        std::fs::set_permissions(outer.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(validate_audit_path(&private.join("audit.sqlite")).is_ok());
        let link = outer.path().join("link");
        symlink(&private, &link).unwrap();
        assert!(validate_audit_path(&link.join("audit.sqlite")).is_err());
        let file = private.join("audit.sqlite");
        std::fs::write(&file, []).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(validate_audit_path(&file).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(validate_audit_path(&file).is_ok());
    }
}

#[cfg(all(unix, feature = "webhook-worker"))]
#[derive(Clone)]
pub(crate) struct OAuthFactory {
    pub runtime: crate::notion_oauth_runtime::NotionOAuthRuntime,
    pub coordinator: std::sync::Arc<notion_knowledge_retrieval::commit::IndexCommitCoordinator>,
    pub settings: Settings,
    pub scope: notion_knowledge_core::lifecycle::LifecycleScope,
    pub maximum_files: usize,
}

#[cfg(all(unix, feature = "webhook-worker"))]
struct OAuthService {
    factory: OAuthFactory,
    authorization: notion_knowledge_mcp::HttpAuthorization,
    scope: notion_knowledge_core::lifecycle::LifecycleScope,
    cancellation: tokio_util::sync::CancellationToken,
}
#[cfg(all(unix, feature = "webhook-worker"))]
impl notion_knowledge_mcp::upload::RequestFileIngestionFactory for OAuthFactory {
    fn bind<'a>(
        &'a self,
        adapters: notion_knowledge_mcp::RequestScopeAdapters,
        context: &'a rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        std::sync::Arc<dyn notion_knowledge_mcp::upload::FileIngestion>,
                        rmcp::ErrorData,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if adapters.scope != self.scope
                || !adapters.authorization.permissions().can_write()
                || context.ct.is_cancelled()
            {
                return Err(rmcp::ErrorData::invalid_request(
                    "file authorization unavailable",
                    None,
                ));
            }
            adapters.source.validate().await.map_err(|_| {
                rmcp::ErrorData::invalid_request("file authorization unavailable", None)
            })?;
            Ok(std::sync::Arc::new(OAuthService {
                factory: self.clone(),
                authorization: adapters.authorization,
                scope: adapters.scope,
                cancellation: context.ct.clone(),
            })
                as std::sync::Arc<
                    dyn notion_knowledge_mcp::upload::FileIngestion,
                >)
        })
    }
}
#[cfg(all(unix, feature = "webhook-worker"))]
impl notion_knowledge_mcp::upload::FileIngestion for OAuthService {
    fn refresh<'a>(
        &'a self,
        input: notion_knowledge_mcp::upload::FileRefreshRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, &'static str>> + Send + 'a>,
    > {
        Box::pin(async move {
            use notion_knowledge_core::write_refresh::RefreshKey;
            if self.cancellation.is_cancelled() {
                return Err("file_refresh_denied");
            }
            let queue = crate::grant_refresh::AuthorizedRefreshQueue::bind(
                self.factory.coordinator.state().clone(),
                &self.factory.runtime,
                &self.authorization,
                &self.scope,
            )
            .await
            .map_err(|_| "file_refresh_denied")?;
            let authority = crate::oauth_content::RequestGrantAuthority::acquire(
                &self.factory.runtime,
                &self.authorization,
            )
            .await
            .map_err(|_| "file_refresh_denied")?;
            let gate = notion_knowledge_core::root_scope::RootScopeGate::new(
                std::sync::Arc::new(authority.client().map_err(|_| "file_refresh_denied")?),
                self.scope.clone(),
            )
            .map_err(|_| "file_refresh_denied")?;
            let permit = gate
                .authorize(&notion_knowledge_core::backend::PageId(
                    input.page_id.clone(),
                ))
                .await
                .map_err(|_| "file_refresh_denied")?;
            gate.revalidate(&permit, &self.scope)
                .await
                .map_err(|_| "file_refresh_denied")?;
            let key = RefreshKey {
                workspace_id: self.scope.workspace_id.clone(),
                page_id: input.page_id,
                mutation_digest: input.mutation_digest,
            };
            crate::file_refresh::status_value(
                self.factory.coordinator.state(),
                &key,
                queue.authority(),
            )
            .map_err(|_| "file_refresh_denied")?;
            if self.cancellation.is_cancelled() {
                return Err("file_refresh_denied");
            }
            if input.retry {
                queue.retry(&key).map_err(|_| "file_refresh_retry_denied")?;
            }
            crate::file_refresh::status_value(
                self.factory.coordinator.state(),
                &key,
                queue.authority(),
            )
            .map_err(|_| "file_refresh_denied")
        })
    }
    fn ingest<'a>(
        &'a self,
        input: notion_knowledge_mcp::upload::UploadFileRequest,
        cancellation: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, &'static str>> + Send + 'a>,
    > {
        Box::pin(async move {
            #[cfg(test)]
            let _completion =
                FileRequestCompletion(self.factory.settings.request_completion.clone());
            use notion_knowledge_core::audit::{AuditActor, AuditOutcome};
            input
                .validate_with_limit(self.factory.maximum_files)
                .map_err(|_| "file_upload_invalid_input")?;
            let audit = notion_knowledge_retrieval::audit::SqliteAuditStore::open(
                &self.factory.settings.audit_file,
                self.factory.settings.retention,
            )
            .map_err(|_| "file_upload_audit_unavailable")?;
            let correlation = notion_knowledge_core::logging::CorrelationId::new();
            let record = |outcome| {
                record_oauth_file_audit(
                    &audit,
                    &input.target,
                    correlation.as_str(),
                    outcome,
                    || crate::webhook_worker::runtime::now_ms() / 1000,
                )
            };
            record(AuditOutcome::Attempted)?;
            let begin = async {
                #[cfg(test)]
                if let Some(clock) = &self.factory.settings.source_clock {
                    return crate::file_refresh::AuthorizedFileIntent::begin_clocked(
                        self.factory.coordinator.state().clone(),
                        &self.factory.runtime,
                        &self.authorization,
                        &self.scope,
                        &notion_knowledge_core::backend::PageId(input.target.clone()),
                        self.cancellation.clone(),
                        120,
                        clock.clone(),
                    )
                    .await;
                }
                crate::file_refresh::AuthorizedFileIntent::begin(
                    self.factory.coordinator.state().clone(),
                    &self.factory.runtime,
                    &self.authorization,
                    &self.scope,
                    &notion_knowledge_core::backend::PageId(input.target.clone()),
                    self.cancellation.clone(),
                    120,
                )
                .await
            };
            let owner = match begin.await {
                Ok(owner) => owner,
                Err(_) => {
                    record(AuditOutcome::Denied)?;
                    return Err("file_upload_denied");
                }
            };
            let mut service = Runtime::scoped(
                &self.factory.settings,
                owner.client().map_err(|_| "file_upload_denied")?,
                &self.scope,
                self.factory.maximum_files,
            )?;
            service.actor = AuditActor::ApprovedUser;
            service.intent = Some(owner);
            service.request_cancel = Some(self.authorization.cancellation());
            service.ingest(input, cancellation).await
        })
    }
}

/// Each record gets one fresh trusted instant, shared by event construction and
/// store validation. In particular, a terminal denial after an await must never
/// reuse the earlier attempted event's timestamp.
#[cfg(all(unix, feature = "webhook-worker"))]
fn record_oauth_file_audit(
    audit: &dyn notion_knowledge_core::audit::AuditStore,
    target: &str,
    correlation: &str,
    outcome: notion_knowledge_core::audit::AuditOutcome,
    clock: impl FnOnce() -> i64,
) -> Result<(), &'static str> {
    use notion_knowledge_core::audit::{AuditActor, AuditEvent, AuditTool};
    let at = clock();
    let event = AuditEvent::new(
        at,
        AuditActor::ApprovedUser,
        AuditTool::FileAttach,
        Some(target.into()),
        outcome,
        correlation.into(),
        None,
    )
    .map_err(|_| "file_upload_audit_unavailable")?;
    audit
        .record(&event, at)
        .map_err(|_| "file_upload_audit_unavailable")
}

#[cfg(all(test, unix, feature = "webhook-worker"))]
mod audit_clock_tests {
    use super::record_oauth_file_audit;
    use notion_knowledge_core::audit::{
        AuditActor, AuditEvent, AuditOutcome, AuditRetention, AuditStore, AuditTool,
    };
    use notion_knowledge_retrieval::audit::SqliteAuditStore;
    use std::sync::atomic::{AtomicI64, Ordering};
    const TARGET: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    const CORRELATION: &str = "server-audit-clock-boundary";

    #[test]
    fn attempted_and_later_denied_records_each_use_one_fresh_trusted_instant() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.sqlite");
        let audit = SqliteAuditStore::open(&path, AuditRetention::days(1).unwrap()).unwrap();
        // The clock advances a whole second on every sample. Separate reads for
        // event creation/store validation would deterministically disagree.
        let clock = AtomicI64::new(100);
        record_oauth_file_audit(&audit, TARGET, CORRELATION, AuditOutcome::Attempted, || {
            clock.fetch_add(1, Ordering::SeqCst)
        })
        .unwrap();
        assert_eq!(clock.load(Ordering::SeqCst), 101);
        // Represents elapsed authorization work; no wall time or sleeps needed.
        clock.store(107, Ordering::SeqCst);
        record_oauth_file_audit(&audit, TARGET, CORRELATION, AuditOutcome::Denied, || {
            clock.fetch_add(1, Ordering::SeqCst)
        })
        .unwrap();
        assert_eq!(clock.load(Ordering::SeqCst), 108);
        let db = rusqlite::Connection::open(&path).unwrap();
        let mut statement = db
            .prepare("SELECT occurred_at_unix,outcome FROM agent_audit_events ORDER BY id")
            .unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![(100, "attempted".into()), (107, "denied".into())]
        );
        // The real store's future-event rejection is deliberately unchanged.
        let future = AuditEvent::new(
            109,
            AuditActor::ApprovedUser,
            AuditTool::FileAttach,
            Some(TARGET.into()),
            AuditOutcome::Denied,
            CORRELATION.into(),
            None,
        )
        .unwrap();
        assert!(audit.record(&future, 108).is_err());
        assert_eq!(
            db.query_row("SELECT count(*) FROM agent_audit_events", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
    #[test]
    fn invalid_clock_or_unavailable_audit_storage_still_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.sqlite");
        let audit = SqliteAuditStore::open(&path, AuditRetention::days(1).unwrap()).unwrap();
        assert_eq!(
            record_oauth_file_audit(&audit, TARGET, CORRELATION, AuditOutcome::Attempted, || -1),
            Err("file_upload_audit_unavailable")
        );
        let db = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM agent_audit_events", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        db.execute_batch("DROP TABLE agent_audit_events").unwrap();
        assert_eq!(
            record_oauth_file_audit(&audit, TARGET, CORRELATION, AuditOutcome::Attempted, || 100),
            Err("file_upload_audit_unavailable")
        );
    }
}

// Test-only lifetime observation, never a production wait or cleanup mechanism.
// It fires when the actual request future completes or is dropped/cancelled.
#[cfg(all(test, unix, feature = "webhook-worker"))]
struct FileRequestCompletion(Option<std::sync::Arc<tokio::sync::Notify>>);
#[cfg(all(test, unix, feature = "webhook-worker"))]
impl Drop for FileRequestCompletion {
    fn drop(&mut self) {
        if let Some(completion) = &self.0 {
            completion.notify_one();
        }
    }
}
