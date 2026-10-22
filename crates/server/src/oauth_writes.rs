//! Explicit operator settings and request-bound semantic write composition.
use crate::config::{ConfigError, invalid};
use std::{ffi::OsString, path::PathBuf};

#[derive(Debug)]
pub struct Settings {
    pub ledger: PathBuf,
    pub audit: PathBuf,
}
impl Settings {
    pub(crate) fn parse(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        enabled: bool,
    ) -> Result<Option<Self>, ConfigError> {
        let ledger = lookup("NK_OAUTH_WRITE_LEDGER_FILE");
        let audit = lookup("NK_OAUTH_WRITE_AUDIT_FILE");
        if !enabled {
            if ledger.is_some() || audit.is_some() {
                return Err(invalid(
                    "NK_OAUTH_WRITES",
                    "write stores require explicit OAuth write activation",
                ));
            }
            return Ok(None);
        }
        if !cfg!(all(unix, feature = "webhook-worker")) {
            return Err(invalid(
                "NK_OAUTH_WRITES",
                "requires Unix and the webhook-worker feature",
            ));
        }
        let path = |value: Option<OsString>, setting| {
            let path =
                PathBuf::from(value.ok_or_else(|| invalid(setting, "required for OAuth writes"))?);
            crate::file_ingestion::validate_audit_path(&path).map_err(|_| {
                invalid(
                    setting,
                    "requires an absolute private owner-controlled durable file path",
                )
            })?;
            Ok::<_, ConfigError>(path)
        };
        let settings = Self {
            ledger: path(ledger, "NK_OAUTH_WRITE_LEDGER_FILE")?,
            audit: path(audit, "NK_OAUTH_WRITE_AUDIT_FILE")?,
        };
        if settings.ledger == settings.audit {
            return Err(invalid(
                "NK_OAUTH_WRITE_AUDIT_FILE",
                "must be separate from the mutation ledger",
            ));
        }
        Ok(Some(settings))
    }
}

#[cfg(all(unix, feature = "webhook-worker"))]
pub use installed::*;
#[cfg(all(unix, feature = "webhook-worker"))]
mod installed {
    use crate::{
        config::Config,
        grant_refresh::{AuthorizedRefreshQueue, GrantRefreshSettings, GrantRefreshWorker},
        notion_oauth_runtime::NotionOAuthRuntime,
    };
    use notion_knowledge_core::{
        audit::{AuditActor, AuditRetention},
        chunking::ChunkConfig,
        embedding::EmbeddingProvider,
    };
    use notion_knowledge_mcp::{
        KnowledgeServer,
        mutations::{MutationRuntime, RequestMutationFactory},
    };
    use notion_knowledge_retrieval::{
        audit::SqliteAuditStore, commit::IndexCommitCoordinator,
        idempotency::SqliteIdempotencyStore,
    };
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;

    struct Factory {
        runtime: NotionOAuthRuntime,
        coordinator: Arc<IndexCommitCoordinator>,
        ledger: Arc<SqliteIdempotencyStore>,
        audit: Arc<SqliteAuditStore>,
        section: bool,
    }
    impl RequestMutationFactory for Factory {
        fn supports(&self, name: &str) -> bool {
            matches!(name, "knowledge_create_page" | "knowledge_append")
                || (self.section && name == "knowledge_update_section")
        }
        fn bind<'a>(
            &'a self,
            adapters: notion_knowledge_mcp::RequestScopeAdapters,
            context: &'a rmcp::service::RequestContext<rmcp::RoleServer>,
            name: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Arc<MutationRuntime>, rmcp::ErrorData>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let denied =
                    || rmcp::ErrorData::invalid_request("write authorization unavailable", None);
                if !self.supports(name) {
                    return Err(denied());
                }
                let source = adapters
                    .source
                    .clone()
                    .mutation_source()
                    .ok_or_else(denied)?;
                let queue = AuthorizedRefreshQueue::bind(
                    self.coordinator.state().clone(),
                    &self.runtime,
                    &adapters.authorization,
                    &adapters.scope,
                )
                .await
                .map_err(|_| denied())?;
                let mut runtime = MutationRuntime::new(
                    source,
                    adapters.scope,
                    self.ledger.clone(),
                    Arc::new(queue),
                    self.audit.clone(),
                    AuditActor::ApprovedUser,
                )
                .map_err(|_| denied())?;
                if name == "knowledge_update_section" {
                    runtime = runtime.with_confirmation(Arc::new(
                        notion_knowledge_mcp::confirmation::ElicitationConfirmation::bind(
                            context,
                            Duration::from_secs(120),
                        )
                        .await
                        .map_err(|_| denied())?,
                    ));
                }
                Ok(Arc::new(runtime))
            })
        }
    }
    pub struct Runtime {
        pub handler: KnowledgeServer,
        pub coordinator: Arc<IndexCommitCoordinator>,
        worker: GrantRefreshWorker,
        poll: Duration,
    }
    impl Runtime {
        pub async fn run(&self, stop: CancellationToken) -> Result<(), &'static str> {
            loop {
                if stop.is_cancelled() {
                    return Ok(());
                }
                let processed = self.worker.tick(stop.clone()).await?;
                if !processed {
                    tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(self.poll) => {} }
                }
            }
        }
    }
    pub async fn configured(
        handler: KnowledgeServer,
        config: &Config,
        runtime: NotionOAuthRuntime,
    ) -> Result<Runtime, &'static str> {
        let settings = config
            .retrieval
            .as_ref()
            .ok_or("OAuth writes require retrieval")?;
        let provider = Arc::new(
            notion_knowledge_retrieval::qwen::QwenProvider::load(
                notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
            )
            .await
            .map_err(|_| "retrieval model unavailable or incompatible")?,
        );
        compose(handler, config, runtime, provider).await
    }
    /// Production and fixtures share the same stores, authority and mutation factory.
    pub async fn compose(
        handler: KnowledgeServer,
        config: &Config,
        runtime: NotionOAuthRuntime,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Runtime, &'static str> {
        if !config.oauth_writes
            || config.read_only
            || !matches!(config.notion_auth, crate::config::NotionAuth::None)
        {
            return Err("OAuth write configuration unavailable");
        }
        let stores = config
            .oauth_write_stores
            .as_ref()
            .ok_or("OAuth write stores unavailable")?;
        super::validate_distinct(config)?;
        let ledger = Arc::new(
            SqliteIdempotencyStore::open_existing(&stores.ledger)
                .map_err(|_| "existing OAuth mutation ledger unavailable")?,
        );
        let audit = Arc::new(
            SqliteAuditStore::open(&stores.audit, AuditRetention::default())
                .map_err(|_| "OAuth mutation audit unavailable")?,
        );
        let scope = config
            .notion_scope
            .clone()
            .ok_or("OAuth write scope unavailable")?;
        let settings = config
            .retrieval
            .as_ref()
            .ok_or("OAuth writes require retrieval")?;
        let (handler, coordinator, table) =
            crate::retrieval_runtime::compose_oauth_index(handler, config, provider.clone(), true)
                .await?;
        let file_scope = scope.clone();
        let worker = GrantRefreshWorker::new(
            runtime.clone(),
            GrantRefreshSettings {
                scope,
                chunk_config: ChunkConfig::default(),
                alias_sources: Some(settings.alias_sources.clone()),
                timeout: Duration::from_secs(settings.timeout_seconds),
            },
            table,
            coordinator.clone(),
            provider,
        )?;
        let handler = if let Some(settings) = &config.file_ingestion {
            handler.and_request_file_ingestion(Arc::new(crate::file_ingestion::OAuthFactory {
                runtime: runtime.clone(),
                coordinator: coordinator.clone(),
                settings: settings.clone(),
                scope: file_scope,
                maximum_files: config.max_upload_files,
            }))
        } else {
            handler
        };
        let handler = handler.and_request_mutations(Arc::new(Factory {
            runtime,
            coordinator: coordinator.clone(),
            ledger,
            audit,
            section: config.destructive_writes_enabled,
        }));
        Ok(Runtime {
            handler,
            coordinator,
            worker,
            poll: Duration::from_millis(settings.poll_ms),
        })
    }
}

#[cfg(unix)]
pub fn validate_distinct(config: &crate::config::Config) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    let stores = config
        .oauth_write_stores
        .as_ref()
        .ok_or("OAuth write stores unavailable")?;
    let mut paths = vec![stores.ledger.as_path(), stores.audit.as_path()];
    if let Some(files) = &config.file_ingestion {
        paths.push(files.audit_file.as_path());
    }
    if let Some(state) = config.webhook_state_file.as_deref() {
        paths.push(state);
    }
    if let Some(grant) = &config.notion_grant_store {
        for write in &paths {
            for secret in [&grant.key_file, &grant.state_file] {
                if *write == secret
                    || std::fs::metadata(write)
                        .ok()
                        .zip(std::fs::metadata(secret).ok())
                        .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino())
                {
                    return Err("OAuth write stores must be separate from credential storage");
                }
            }
        }
    }
    for path in &paths {
        crate::file_ingestion::validate_audit_path(path)
            .map_err(|_| "OAuth storage must be private and operator-owned")?;
    }
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            if a == b
                || std::fs::metadata(a)
                    .ok()
                    .zip(std::fs::metadata(b).ok())
                    .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino())
            {
                return Err("OAuth write stores must be distinct");
            }
        }
        if let Some(settings) = &config.retrieval {
            let index = std::fs::canonicalize(&settings.index)
                .map_err(|_| "existing retrieval index unavailable")?;
            let parent = std::fs::canonicalize(a.parent().ok_or("invalid write store path")?)
                .map_err(|_| "write store parent unavailable")?;
            let target = parent.join(a.file_name().ok_or("invalid write store path")?);
            if target.starts_with(index) {
                return Err("OAuth write stores must be outside disposable index");
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix, feature = "webhook-worker"))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn private() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }
    #[test]
    fn oauth_write_stores_require_explicit_activation_and_complete_private_paths() {
        let dir = private();
        let ledger = dir.path().join("ledger.sqlite");
        let audit = dir.path().join("audit.sqlite");
        let lookup = |key: &str| match key {
            "NK_OAUTH_WRITE_LEDGER_FILE" => Some(ledger.clone().into_os_string()),
            "NK_OAUTH_WRITE_AUDIT_FILE" => Some(audit.clone().into_os_string()),
            _ => None,
        };
        assert!(Settings::parse(&mut |_| None, false).unwrap().is_none());
        assert!(Settings::parse(&mut lookup.clone(), false).is_err());
        assert!(Settings::parse(&mut lookup.clone(), true).is_ok());
        for missing in ["NK_OAUTH_WRITE_LEDGER_FILE", "NK_OAUTH_WRITE_AUDIT_FILE"] {
            assert!(
                Settings::parse(
                    &mut |key| if key == missing { None } else { lookup(key) },
                    true
                )
                .is_err()
            );
        }
        assert!(Settings::parse(&mut |_| Some(ledger.clone().into_os_string()), true).is_err());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(Settings::parse(&mut lookup.clone(), true).is_err());
    }
    #[test]
    fn initialization_rejects_absent_state_aliases_and_canonical_index_containment() {
        use std::os::unix::fs::symlink;
        let dir = private();
        let index = dir.path().join("index");
        std::fs::create_dir(&index).unwrap();
        std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = dir.path().join("index-alias");
        symlink(&index, &alias).unwrap();
        let ledger = index.join("not-created.sqlite");
        let mut config = crate::config::Config::from_lookup(|_| None).unwrap();
        config.oauth_write_stores = Some(Settings {
            ledger: ledger.clone(),
            audit: dir.path().join("audit"),
        });
        config.webhook_state_file = Some(ledger.clone());
        assert!(validate_distinct(&config).is_err());
        assert!(!ledger.exists());
        config.webhook_state_file = Some(dir.path().join("state"));
        config.retrieval = Some(crate::webhook_worker::WorkerSettings {
            index: alias,
            assets: dir.path().join("assets"),
            poll_ms: 100,
            timeout_seconds: 20,
            alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![]).unwrap(),
        });
        assert_eq!(
            validate_distinct(&config),
            Err("OAuth write stores must be outside disposable index")
        );
        assert!(!ledger.exists());
        config.retrieval = None;
        config.notion_grant_store = Some(crate::config::GrantStoreSettings {
            key_file: dir.path().join("key"),
            state_file: ledger.clone(),
        });
        assert!(validate_distinct(&config).is_err());
        assert!(!ledger.exists());
    }

    #[test]
    fn durable_write_files_cannot_alias_each_other_state_or_credentials() {
        let dir = private();
        let ledger = dir.path().join("ledger.sqlite");
        let audit = dir.path().join("audit.sqlite");
        let state = dir.path().join("state.sqlite");
        let mut config = crate::config::Config::from_lookup(|_| None).unwrap();
        config.oauth_write_stores = Some(Settings {
            ledger: ledger.clone(),
            audit: audit.clone(),
        });
        config.webhook_state_file = Some(state.clone());
        assert!(validate_distinct(&config).is_ok());
        std::fs::write(&ledger, b"fixture").unwrap();
        std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::hard_link(&ledger, &audit).unwrap();
        assert!(validate_distinct(&config).is_err());
        std::fs::remove_file(&audit).unwrap();
        std::fs::hard_link(&ledger, &state).unwrap();
        assert!(validate_distinct(&config).is_err());
        std::fs::remove_file(&state).unwrap();
        config.notion_grant_store = Some(crate::config::GrantStoreSettings {
            key_file: ledger.clone(),
            state_file: dir.path().join("grant"),
        });
        assert!(validate_distinct(&config).is_err());
    }
}

#[cfg(not(unix))]
pub fn validate_distinct(_: &crate::config::Config) -> Result<(), &'static str> {
    Err("OAuth writes require Unix")
}
