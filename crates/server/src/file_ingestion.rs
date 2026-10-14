//! Validated operator-owned admission settings and installed audited ingestion.
use crate::config::{ConfigError, invalid, optional, text};
use notion_knowledge_core::audit::AuditRetention;
use notion_knowledge_notion::{file_download::DownloadPolicy, file_multipart::MultipartPolicy};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

pub struct Settings {
    pub download: DownloadPolicy,
    pub maximum_bytes: usize,
    pub multipart: Option<MultipartPolicy>,
    pub audit_file: PathBuf,
    pub retention: AuditRetention,
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
        integration: bool,
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
        if read_only || !integration || !scope {
            return Err(invalid(
                "NK_FILE_INGESTION",
                "requires write mode, integration and authoritative physical scope",
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
        }))
    }
}
fn validate_audit_path(path: &Path) -> Result<(), ConfigError> {
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

/// The real installed application: one immutable operator policy and durable
/// audit store, with request-owned one-shot batches and private temporary files.
pub(crate) struct Runtime {
    adapter: notion_knowledge_notion::file_ingest::NotionFileIngestAdapter,
    gate: notion_knowledge_core::root_scope::RootScopeGate,
    audit: notion_knowledge_retrieval::audit::SqliteAuditStore,
    policy: notion_knowledge_core::file_ingest::FileBatchPolicy,
}
impl Runtime {
    pub(crate) fn new(
        config: &crate::config::Config,
        settings: &Settings,
        client: notion_knowledge_notion::NotionClient,
    ) -> Result<Self, &'static str> {
        validate_audit_path(&settings.audit_file)
            .map_err(|_| "file ingestion audit path unavailable")?;
        let scope = config
            .notion_scope
            .as_ref()
            .ok_or("file ingestion scope unavailable")?;
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
            policy: notion_knowledge_core::file_ingest::FileBatchPolicy::new(
                config.max_upload_files,
            )
            .map_err(|_| "file batch policy unavailable")?,
        })
    }
}
impl notion_knowledge_mcp::upload::FileIngestion for Runtime {
    fn ingest(
        &self,
        input: notion_knowledge_mcp::upload::UploadFileRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, &'static str>> + Send + '_>,
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
                actor: notion_knowledge_core::audit::AuditActor::Server,
                correlation_id: correlation,
                permitted: true,
            };
            let failure = batch.execute(&self.adapter, &context).await.err();
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
            Ok(
                serde_json::json!({"files":results,"partial_success":batch.partial_success(),"failure":failure.map(failure_name)}),
            )
        })
    }
}
