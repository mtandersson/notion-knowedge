//! Opt-in production composition. Inbox hints never supply index authority.
use crate::{config::ConfigError, webhook::WebhookConfig};
use std::{ffi::OsString, path::PathBuf};

#[derive(Debug, Clone)]
pub struct WorkerSettings {
    pub alias_sources: notion_knowledge_core::aliases::AliasSources,
    pub index: PathBuf,
    pub assets: PathBuf,
    pub poll_ms: u64,
    pub timeout_seconds: u64,
}
impl WorkerSettings {
    pub(crate) fn parse(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        webhook: &WebhookConfig,
        scope: Option<&notion_knowledge_core::lifecycle::LifecycleScope>,
    ) -> Result<Option<Self>, ConfigError> {
        let error = |setting, reason| ConfigError { setting, reason };
        let enabled = match lookup("NK_WEBHOOK_WORKER").as_deref() {
            None => false,
            Some(value) if value == "false" => false,
            Some(value) if value == "true" => true,
            _ => return Err(error("NK_WEBHOOK_WORKER", "must be true or false")),
        };
        if !enabled {
            return Ok(None);
        }
        if !cfg!(all(unix, feature = "webhook-worker")) {
            return Err(error(
                "NK_WEBHOOK_WORKER",
                "requires Unix binary built with webhook-worker feature",
            ));
        }
        let WebhookConfig::Verified { workspace_id, .. } = webhook else {
            return Err(error("NK_WEBHOOK_WORKER", "requires verified webhook mode"));
        };
        if scope.is_none_or(|s| s.workspace_id != *workspace_id) {
            return Err(error(
                "NK_WEBHOOK_WORKER",
                "requires matching trusted Notion root scope",
            ));
        }
        Self::index_settings(lookup).map(Some)
    }
    pub(crate) fn index_settings(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let error = |setting, reason| ConfigError { setting, reason };
        let mut path = |name| {
            let value = lookup(name)
                .and_then(|v| v.into_string().ok())
                .ok_or_else(|| error(name, "requires an absolute local directory"))?;
            let path = PathBuf::from(&value);
            if !path.is_absolute() || value.chars().any(char::is_control) {
                return Err(error(name, "requires an absolute local directory"));
            }
            Ok(path)
        };
        let index = path("NK_WEBHOOK_INDEX_DIR")?;
        let assets = path("NK_WEBHOOK_QWEN_ASSETS")?;
        let title = match lookup("NK_ALIAS_TITLE").as_deref() {
            None => true,
            Some(value) if value == "true" => true,
            Some(value) if value == "false" => false,
            _ => return Err(error("NK_ALIAS_TITLE", "must be true or false")),
        };
        let property_ids = match lookup("NK_ALIAS_PROPERTY_IDS") {
            None => vec![],
            Some(value) => {
                serde_json::from_str::<Vec<String>>(value.to_str().ok_or_else(|| {
                    error(
                        "NK_ALIAS_PROPERTY_IDS",
                        "must be a JSON array of stable property IDs",
                    )
                })?)
                .map_err(|_| {
                    error(
                        "NK_ALIAS_PROPERTY_IDS",
                        "must be a JSON array of stable property IDs",
                    )
                })?
            }
        };
        let alias_sources = notion_knowledge_core::aliases::AliasSources::new(title, property_ids)
            .map_err(|_| error("NK_ALIAS_PROPERTY_IDS", "requires at least one source, at most 32 valid property IDs of at most 128 bytes"))?;
        let mut number = |name, default, min, max| {
            let value = lookup(name)
                .map(|v| v.into_string().ok().and_then(|s| s.parse::<u64>().ok()))
                .unwrap_or(Some(default));
            value
                .filter(|v| (min..=max).contains(v))
                .ok_or_else(|| error(name, "outside documented integer range"))
        };
        Ok(Self {
            alias_sources,
            index,
            assets,
            poll_ms: number("NK_WEBHOOK_POLL_MS", 1000, 10, 60000)?,
            timeout_seconds: number("NK_WEBHOOK_TIMEOUT_SECONDS", 60, 1, 300)?,
        })
    }
}

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod runtime {
    use notion_knowledge_core::{
        backend::PageId,
        embedding::EmbeddingProvider,
        reconciliation::ReconciliationJournal,
        webhook::{
            EventClaim, EventFailure, EventState, InboxScope, PageWorkClaim, ProcessingOutcome,
            WebhookDebounce, WebhookInbox, WebhookRecovery,
        },
    };
    use notion_knowledge_retrieval::{
        commit::IndexCommitCoordinator,
        mutation_refresh::MutationRefreshClaim,
        refresh::{AuthoritativePageRefresh, RefreshError},
        sync_state::SqliteSyncStateStore,
    };
    use sha2::{Digest, Sha256};
    use std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio_util::sync::CancellationToken;

    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .unwrap_or(i64::MAX)
    }
    #[derive(Clone)]
    enum Work {
        Page(PageWorkClaim),
        Event(EventClaim, notion_knowledge_core::webhook::InboxEvent),
        Mutation(MutationRefreshClaim),
    }
    impl Work {
        fn hint(&self) -> Option<&notion_knowledge_core::webhook::WebhookEvent> {
            match self {
                Self::Page(p) => Some(&p.newest),
                Self::Event(_, e) => Some(&e.event),
                Self::Mutation(_) => None,
            }
        }
        fn page_id(&self) -> &str {
            match self {
                Self::Page(p) => &p.key.page_id,
                Self::Event(_, e) => &e.event.entity_id,
                Self::Mutation(c) => &c.key.page_id,
            }
        }
        fn claims(&self) -> Vec<EventClaim> {
            match self {
                Self::Page(p) => p.events.clone(),
                Self::Event(c, _) => vec![c.clone()],
                Self::Mutation(_) => vec![],
            }
        }
        fn operation(&self) -> String {
            if let Self::Mutation(c) = self {
                return format!(
                    "mutation-refresh-{}-{}",
                    c.key.workspace_id, c.key.mutation_digest
                );
            }
            let mut ids: Vec<_> = self.claims().into_iter().map(|c| c.key.event_id).collect();
            ids.sort();
            let hint = self.hint().expect("webhook work has a hint");
            format!(
                "webhook-{:x}",
                Sha256::digest(format!(
                    "{}:{}:{ids:?}",
                    hint.workspace_id, hint.subscription_id
                ))
            )
        }
    }
    /// One bounded processor per server; independent processes share SQLite and
    /// the directory commit lock. Fixture tests inject the real Notion transport
    /// and a deterministic provider through these same production boundaries.
    pub struct Worker {
        pub webhook_enabled: bool,
        pub scheduled: Option<Arc<crate::scheduled_reconciliation::runtime::Worker>>,
        pub state: Arc<SqliteSyncStateStore>,
        pub scope: InboxScope,
        pub integration: String,
        pub coordinator: Arc<IndexCommitCoordinator>,
        pub handler: AuthoritativePageRefresh,
        pub provider: Arc<dyn EmbeddingProvider>,
        pub poll: Duration,
        pub timeout: Duration,
        pub log_enabled: bool,
    }
    async fn blocking<T: Send + 'static>(
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, &'static str> {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|_| "webhook worker storage unavailable")
    }
    pub async fn configured(
        config: &crate::config::Config,
    ) -> Result<Option<Worker>, &'static str> {
        let Some(settings) = config
            .webhook_worker
            .as_ref()
            .or(config.reconciliation_index.as_ref())
        else {
            return Ok(None);
        };
        let (workspace_id, subscription_id, integration_id) = match &config.webhook {
            crate::webhook::WebhookConfig::Verified {
                workspace_id,
                subscription_id,
                integration_id,
                ..
            } => (
                workspace_id.clone(),
                subscription_id.clone(),
                integration_id.clone(),
            ),
            _ if config.reconciliation_schedule.is_some() => (
                config
                    .notion_scope
                    .as_ref()
                    .ok_or("Notion scope required")?
                    .workspace_id
                    .clone(),
                "periodic".into(),
                String::new(),
            ),
            _ => return Err("verified webhook configuration required"),
        };
        let crate::config::NotionAuth::Integration(token) = &config.notion_auth else {
            return Err("integration credential required");
        };
        let scope = config.notion_scope.clone().ok_or("Notion scope required")?;
        let source = Arc::new(
            notion_knowledge_notion::NotionClient::integration(token.expose_secret())
                .map_err(|_| "Notion client unavailable")?,
        );
        let provider = Arc::new(
            notion_knowledge_retrieval::qwen::QwenProvider::load(
                notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
            )
            .await
            .map_err(|_| "Qwen assets unavailable or incompatible")?,
        );
        let state_file = config
            .webhook_state_file
            .clone()
            .ok_or("webhook state required")?;
        let runtime = crate::index_runtime::IndexRuntime::recover(
            settings,
            &state_file,
            scope,
            source,
            provider,
        )
        .await?;
        let coordinator = runtime.coordinator.clone();
        let state = coordinator.state().clone();
        let handler = runtime.handler.clone();
        let provider = runtime.provider.clone();
        let scheduled = config.reconciliation_schedule.clone().map(|policy| {
            Arc::new(crate::scheduled_reconciliation::runtime::Worker {
                index: runtime,
                settings: policy,
                poll: Duration::from_millis(settings.poll_ms),
                timeout: Duration::from_secs(settings.timeout_seconds),
            })
        });
        Ok(Some(Worker {
            webhook_enabled: config.webhook_worker.is_some(),
            scheduled,
            state,
            scope: InboxScope {
                workspace_id: workspace_id.clone(),
                subscription_id: subscription_id.clone(),
            },
            integration: integration_id.clone(),
            coordinator,
            handler,
            provider,
            poll: Duration::from_millis(settings.poll_ms),
            timeout: Duration::from_secs(settings.timeout_seconds),
            log_enabled: matches!(
                config.log_level,
                notion_knowledge_core::logging::Level::Info
                    | notion_knowledge_core::logging::Level::Debug
            ),
        }))
    }
    impl Worker {
        pub async fn run(&self, stop: CancellationToken) -> Result<(), &'static str> {
            if let Some(scheduled) = &self.scheduled {
                if !self.webhook_enabled {
                    return scheduled.run(stop).await;
                }
                let stop_on_exit = stop.clone();
                let scheduled_loop = async {
                    let _guard = stop_on_exit.drop_guard();
                    scheduled.run(stop.clone()).await
                };
                let webhook_stop = stop.clone();
                let webhook_loop = async {
                    let _guard = webhook_stop.drop_guard();
                    self.run_webhooks(stop.clone()).await
                };
                let (webhook_result, scheduled_result) = tokio::join!(webhook_loop, scheduled_loop);
                let result = webhook_result.and(scheduled_result);
                stop.cancel();
                self.coordinator
                    .drain_owned()
                    .await
                    .map_err(|_| "index drain failed")?;
                return result;
            }
            self.run_webhooks(stop).await
        }
        async fn run_webhooks(&self, stop: CancellationToken) -> Result<(), &'static str> {
            let mut page_first = true;
            loop {
                if stop.is_cancelled() {
                    return Ok(());
                }
                // Always sleep between dispatches: empty/delayed queues and a
                // repeatedly failing page cannot cause a spin or starve shutdown.
                self.tick(stop.clone(), page_first).await?;
                page_first = !page_first;
                tokio::select! { _=stop.cancelled()=>return Ok(()), _=tokio::time::sleep(self.poll)=>{} }
            }
        }
        pub async fn tick(
            &self,
            stop: CancellationToken,
            page_first: bool,
        ) -> Result<bool, &'static str> {
            if stop.is_cancelled() {
                return Ok(false);
            }
            let state = self.state.clone();
            let scope = self.scope.clone();
            let duration = self.timeout.as_secs() as i64 + 30;
            // Acquire the shared fence BEFORE claiming/effects. Busy writers do
            // not consume inbox retry budgets merely by delaying dispatch.
            let lease = match blocking(move || state.acquire_lease(now_ms() / 1000, duration))
                .await?
            {
                Ok(lease) => lease,
                Err(notion_knowledge_core::reconciliation::JournalError::Busy) => return Ok(false),
                Err(_) => return Err("index lease unavailable"),
            };
            let coordinator = self.coordinator.clone();
            let token = lease.clone();
            let admitted = blocking(move || {
                coordinator.ensure_no_active_reconciliation(&token, now_ms() / 1000)
            })
            .await?;
            if let Err(error) = admitted {
                let state = self.state.clone();
                blocking(move || state.release_lease(&lease, now_ms() / 1000))
                    .await?
                    .map_err(|_| "index lease release failed")?;
                return if error == notion_knowledge_core::reconciliation::JournalError::Busy {
                    Ok(false)
                } else {
                    Err("index authority unavailable")
                };
            }
            let state = self.state.clone();
            let admission_stop = stop.clone();
            let claim = blocking(move || {
                if admission_stop.is_cancelled() {
                    return Ok(None);
                }
                // Alternate mutation/page admission to avoid starving trusted
                // post-write refresh work under continuous webhook arrivals.
                if !page_first
                    && let Some(claim) = state
                        .claim_mutation_refresh(&scope.workspace_id, now_ms() / 1000, duration)
                        .map_err(|_| notion_knowledge_core::webhook::InboxError::Unavailable)?
                {
                    return Ok(Some(Work::Mutation(claim)));
                }
                let page = || {
                    state
                        .claim_page(&scope, now_ms(), duration)
                        .map(|p| p.map(Work::Page))
                };
                let event = || {
                    state
                        .claim(&scope, now_ms() / 1000, duration)
                        .map(|e| e.map(|(c, e)| Work::Event(c, e)))
                };
                let webhook = if page_first {
                    page()?.map_or_else(event, |p| Ok(Some(p)))
                } else {
                    event()?.map_or_else(page, |p| Ok(Some(p)))
                }?;
                if webhook.is_some() {
                    return Ok(webhook);
                }
                state
                    .claim_mutation_refresh(&scope.workspace_id, now_ms() / 1000, duration)
                    .map(|claim| claim.map(Work::Mutation))
                    .map_err(|_| notion_knowledge_core::webhook::InboxError::Unavailable)
            })
            .await?;
            let result = match claim {
                Ok(Some(work)) => self.process(work, lease.clone(), stop).await.map(|_| true),
                Ok(None) => Ok(false),
                Err(_) => Err("inbox claim unavailable"),
            };
            let state = self.state.clone();
            match blocking(move || state.release_lease(&lease, now_ms() / 1000)).await? {
                Ok(()) | Err(notion_knowledge_core::reconciliation::JournalError::LeaseLost) => {}
                Err(_) => return Err("index lease release failed"),
            }
            result
        }
        async fn process(
            &self,
            work: Work,
            lease: notion_knowledge_core::reconciliation::Lease,
            stop: CancellationToken,
        ) -> Result<(), &'static str> {
            let hint = work.hint();
            let supported_hint = |hint: &notion_knowledge_core::webhook::WebhookEvent| {
                hint.integration_id == self.integration
                    && hint.entity_type == "page"
                    && matches!(
                        hint.event_type.as_str(),
                        "page.created"
                            | "page.content_updated"
                            | "page.properties_updated"
                            | "page.deleted"
                            | "page.undeleted"
                            | "page.moved"
                    )
            };
            let supported = match &work {
                Work::Mutation(c) => c.key.workspace_id == self.scope.workspace_id,
                _ => hint.is_some_and(|hint| supported_hint(hint) && work.claims().iter().all(|claim| matches!(self.state.event(&claim.key), Ok(Some(e)) if supported_hint(&e.event) && e.event.entity_id == hint.entity_id))),
            };
            let processing = stop.child_token();
            let state = self.state.clone();
            let claims = work.claims();
            let cancelled = processing.clone();
            let page_claim = match &work {
                Work::Page(p) => Some(p.clone()),
                _ => None,
            };
            let mutation_claim = match &work {
                Work::Mutation(c) => Some(c.clone()),
                _ => None,
            };
            // The commit adapter calls this clock under its real directory
            // guard immediately before mutation and checkpoint. Expired, taken
            // over or cancelled inbox ownership cannot authorize a stale write.
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                let now = now_ms() / 1000;
                if cancelled.is_cancelled() || mutation_claim.as_ref().is_some_and(|c| !state.mutation_refresh_is_current(c,now).unwrap_or(false)) || page_claim.as_ref().is_some_and(|p| !state.page_claim_is_current(p,now_ms()).unwrap_or(false)) || claims.iter().any(|c| !matches!(state.event(&c.key), Ok(Some(e)) if e.state==EventState::Running && e.generation==c.generation && e.lease_until.is_some_and(|until|until>now))) { i64::MAX } else { now }
            });
            let outcome = if !supported {
                ProcessingOutcome::Permanent(EventFailure::Source)
            } else {
                let page = PageId(work.page_id().to_owned());
                let operation = work.operation();
                tokio::select! {
                    biased;
                    _=stop.cancelled()=>{ processing.cancel(); return Ok(()); },
                    _=tokio::time::sleep(self.timeout)=>{ processing.cancel(); ProcessingOutcome::Retryable(EventFailure::Unavailable) },
                    result=self.handler.apply_lifecycle(&page, &operation, self.provider.as_ref(), lease, clock)=> match result {
                        Ok(_)=>ProcessingOutcome::Succeeded,
                        Err(RefreshError::InvalidConfiguration | RefreshError::OutOfScope | RefreshError::PermanentSource)=>ProcessingOutcome::Permanent(EventFailure::Source),
                        Err(RefreshError::Source)=>ProcessingOutcome::Retryable(EventFailure::Source),
                        Err(RefreshError::Index)=>ProcessingOutcome::Retryable(EventFailure::Index),
                        Err(RefreshError::Conflict)=>ProcessingOutcome::Retryable(EventFailure::Conflict),
                        Err(RefreshError::Unavailable)=>ProcessingOutcome::Retryable(EventFailure::Unavailable),
                    }
                }
            };
            let state = self.state.clone();
            let observed = work.claims();
            let completion = blocking(move || match work {
                Work::Page(p) => state.complete_page(&p, now_ms(), outcome),
                Work::Event(c, _) => {
                    WebhookRecovery::complete(state.as_ref(), &c, now_ms() / 1000, outcome)
                }
                Work::Mutation(c) => state
                    .complete_mutation_refresh(
                        &c,
                        now_ms() / 1000,
                        outcome == ProcessingOutcome::Succeeded,
                    )
                    .map_err(|error| match error {
                        notion_knowledge_core::write_refresh::RefreshQueueError::ClaimLost => {
                            notion_knowledge_core::webhook::InboxError::ClaimLost
                        }
                        _ => notion_knowledge_core::webhook::InboxError::Unavailable,
                    }),
            })
            .await?;
            match completion {
                Ok(()) => {}
                Err(notion_knowledge_core::webhook::InboxError::ClaimLost) => return Ok(()),
                Err(_) => return Err("inbox completion unavailable"),
            }
            let state = self.state.clone();
            let report = blocking(move || {
                observed.iter().filter_map(|c|state.event(&c.key).ok().flatten()).map(|e|serde_json::json!({"operation":"webhook_worker", "state":format!("{:?}",e.state), "attempts":e.cycle_attempts, "lifetime_attempts":e.lifetime_attempts, "retry_at":e.retry_at, "failure":e.last_failure.map(|f|format!("{f:?}"))})).collect::<Vec<_>>()
            }).await?;
            if self.log_enabled {
                for item in report {
                    eprintln!("{item}");
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    fn parse(extra: &[(&str, &str)]) -> Result<Config, crate::config::ConfigError> {
        let mut values = std::collections::BTreeMap::from([
            ("NK_WEBHOOK_MODE", "verified"),
            ("NK_WEBHOOK_VERIFICATION_TOKEN", "fixture-signing"),
            (
                "NK_WEBHOOK_WORKSPACE_ID",
                "44444444-4444-4444-4444-444444444444",
            ),
            (
                "NK_WEBHOOK_INTEGRATION_ID",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            ),
            (
                "NK_WEBHOOK_SUBSCRIPTION_ID",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
            ("NK_WEBHOOK_STATE_FILE", "/tmp/fixture.sqlite"),
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "fixture-credential"),
            (
                "NK_NOTION_SCOPE_WORKSPACE_ID",
                "44444444-4444-4444-4444-444444444444",
            ),
            (
                "NK_NOTION_SCOPE_ROOTS",
                "22222222-2222-2222-2222-222222222222",
            ),
            ("NK_WEBHOOK_WORKER", "true"),
            ("NK_WEBHOOK_INDEX_DIR", "/tmp/index"),
            ("NK_WEBHOOK_QWEN_ASSETS", "/tmp/assets"),
        ]);
        values.extend(extra.iter().copied());
        Config::from_lookup(|name| values.get(name).map(std::ffi::OsString::from))
    }
    #[test]
    fn enabled_worker_requires_its_runtime_and_trusted_configuration() {
        if !cfg!(all(unix, feature = "webhook-worker")) {
            assert_eq!(parse(&[]).unwrap_err().setting, "NK_WEBHOOK_WORKER");
            return;
        }
        let config = parse(&[]).unwrap();
        let worker = config.webhook_worker.unwrap();
        assert_eq!(worker.poll_ms, 1000);
        assert_eq!(worker.timeout_seconds, 60);
        for (setting, value) in [
            ("NK_WEBHOOK_INDEX_DIR", "relative"),
            ("NK_WEBHOOK_QWEN_ASSETS", ""),
            ("NK_WEBHOOK_POLL_MS", "0"),
            ("NK_WEBHOOK_TIMEOUT_SECONDS", "301"),
            ("NK_WEBHOOK_WORKER", "yes"),
        ] {
            assert_eq!(parse(&[(setting, value)]).unwrap_err().setting, setting);
        }
        assert_eq!(
            parse(&[(
                "NK_WEBHOOK_WORKSPACE_ID",
                "33333333-3333-3333-3333-333333333333"
            )])
            .unwrap_err()
            .setting,
            "NK_WEBHOOK_WORKER"
        );
        let config = parse(&[(
            "NK_NOTION_SCOPE_EXCLUDED_PAGES",
            "11111111-1111-1111-1111-111111111111",
        )])
        .unwrap();
        assert!(
            config
                .notion_scope
                .unwrap()
                .exclusions
                .page_ids
                .contains("11111111-1111-1111-1111-111111111111")
        );
        assert!(parse(&[("NK_NOTION_SCOPE_EXCLUDED_DESCENDANTS", "bad")]).is_err());
    }
    #[cfg(all(unix, feature = "webhook-worker"))]
    #[tokio::test]
    async fn missing_model_assets_reject_production_startup() {
        let dir = tempfile::tempdir().unwrap();
        let assets = dir.path().join("missing");
        let config = parse(&[("NK_WEBHOOK_QWEN_ASSETS", assets.to_str().unwrap())]).unwrap();
        assert!(super::runtime::configured(&config).await.is_err());
    }
}

#[cfg(test)]
mod alias_settings_tests {
    use super::*;

    #[test]
    fn alias_sources_are_explicit_bounded_and_order_independent() {
        let parse = |values: &[(&str, &str)]| {
            WorkerSettings::index_settings(&mut |key| match key {
                "NK_WEBHOOK_INDEX_DIR" => Some("/tmp/index".into()),
                "NK_WEBHOOK_QWEN_ASSETS" => Some("/tmp/assets".into()),
                _ => values
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| OsString::from(value)),
            })
        };
        let default = parse(&[]).unwrap();
        assert_eq!(default.alias_sources.identity(), "alias-sources-v1:1:");
        let explicit = parse(&[
            ("NK_ALIAS_TITLE", "false"),
            ("NK_ALIAS_PROPERTY_IDS", "[\"b\",\"a\",\"a\"]"),
        ])
        .unwrap();
        assert_eq!(
            explicit.alias_sources.identity(),
            "alias-sources-v1:0:1:a1:b"
        );
        for values in [
            vec![("NK_ALIAS_TITLE", "false")],
            vec![("NK_ALIAS_TITLE", "")],
            vec![("NK_ALIAS_PROPERTY_IDS", "")],
            vec![("NK_ALIAS_PROPERTY_IDS", "names")],
            vec![("NK_ALIAS_PROPERTY_IDS", "[\"\"]")],
            vec![("NK_ALIAS_PROPERTY_IDS", "[\"bad\\nidentifier\"]")],
        ] {
            assert!(parse(&values).is_err());
        }
        let oversized = serde_json::to_string(&vec!["name"; 33]).unwrap();
        assert!(parse(&[("NK_ALIAS_PROPERTY_IDS", &oversized)]).is_err());
        let long_id = serde_json::to_string(&vec!["x".repeat(129)]).unwrap();
        assert!(parse(&[("NK_ALIAS_PROPERTY_IDS", &long_id)]).is_err());
    }
}
