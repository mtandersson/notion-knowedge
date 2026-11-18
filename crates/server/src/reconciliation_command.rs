//! Validated, payload-free operator commands. Scheduling is a separate caller.
use notion_knowledge_notion::pages::page_id;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// One full walk sync (`SyncMode::Full`).
    Apply,
    /// Invalidate derived relation projections, then a full walk sync.
    RebuildRelations,
    /// Invalidate derived alias projections, then a full walk sync.
    RebuildAliases,
    DryRun,
    ScheduleStatus,
    ScheduleHistory(i64),
    ScheduleRetry(i64),
    ScheduleAbandon(i64),
    ScheduleConfigure {
        settings: crate::scheduled_reconciliation::Settings,
        deadline: i64,
    },
    ScheduleInterval {
        seconds: i64,
        deadline: i64,
    },
    /// One page through the parent map (`SyncMode::Page`).
    Retry(String),
}
impl Command {
    pub fn parse(args: &[String]) -> Result<Self, &'static str> {
        match args
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["--reconcile", "dry-run"] => Ok(Self::DryRun),
            [
                "--reconcile",
                "schedule",
                "configure",
                interval,
                attempts,
                backoff,
                maximum,
                deadline,
            ] => {
                let number =
                    |value: &str| value.parse::<i64>().map_err(|_| "invalid schedule policy");
                let interval = number(interval)?;
                let attempts = number(attempts)?;
                let backoff = number(backoff)?;
                let maximum_backoff = number(maximum)?;
                let deadline = number(deadline)?;
                if !(60..=2_592_000).contains(&interval)
                    || !(1..=16).contains(&attempts)
                    || !(1..=86_400).contains(&backoff)
                    || !(backoff..=604_800).contains(&maximum_backoff)
                    || deadline < 0
                {
                    return Err("invalid schedule policy");
                }
                Ok(Self::ScheduleConfigure {
                    settings: crate::scheduled_reconciliation::Settings {
                        interval,
                        attempts,
                        backoff,
                        maximum_backoff,
                    },
                    deadline,
                })
            }
            ["--reconcile", "schedule", "status"] => Ok(Self::ScheduleStatus),
            ["--reconcile", "schedule", "status", after] => Ok(Self::ScheduleHistory(
                after
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or("invalid history cursor")?,
            )),
            ["--reconcile", "schedule", "interval", seconds, deadline] => {
                let seconds = seconds
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (60..=2_592_000).contains(n))
                    .ok_or("invalid schedule interval")?;
                let deadline = deadline
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or("invalid schedule deadline")?;
                Ok(Self::ScheduleInterval { seconds, deadline })
            }
            ["--reconcile", "schedule", action, deadline]
                if matches!(*action, "retry" | "abandon") =>
            {
                let deadline = deadline
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or("invalid schedule deadline")?;
                Ok(if *action == "retry" {
                    Self::ScheduleRetry(deadline)
                } else {
                    Self::ScheduleAbandon(deadline)
                })
            }
            ["--reconcile", "apply"] => Ok(Self::Apply),
            ["--reconcile", "rebuild-relations"] => Ok(Self::RebuildRelations),
            ["--reconcile", "rebuild-aliases"] => Ok(Self::RebuildAliases),
            ["--reconcile", "retry", page] => Ok(Self::Retry(
                page_id(page).map_err(|_| "invalid page identifier")?.0,
            )),
            _ => Err(
                "expected --reconcile apply | rebuild-relations | rebuild-aliases | dry-run | retry PAGE | schedule ...",
            ),
        }
    }
    pub fn read_only(&self) -> bool {
        matches!(
            self,
            Self::DryRun | Self::ScheduleStatus | Self::ScheduleHistory(_)
        )
    }
}
#[derive(Debug)]
pub struct Output {
    pub json: serde_json::Value,
    pub exit: u8,
}
impl Output {
    pub fn error(class: &'static str, exit: u8) -> Self {
        Self {
            json: serde_json::json!({"operation":"reconciliation", "status":class}),
            exit,
        }
    }
}

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod runtime {
    use super::*;
    use crate::{
        config::{Config, NotionAuth},
        index_runtime::{IndexRuntime, OpenMode, PageDeadline, SyncMode},
        webhook_worker::{WorkerSettings, runtime::now_ms},
    };
    use notion_knowledge_core::reconciliation::{JournalError, ReconciliationJournal};
    use std::{path::PathBuf, sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;

    pub async fn configured(command: Command, stop: CancellationToken) -> Output {
        let config = match Config::from_env() {
            Ok(c) => c,
            Err(_) => return Output::error("configuration", 2),
        };
        crate::shutdown::operator(
            "reconciliation",
            stop.clone(),
            Duration::from_secs(config.shutdown_timeout_seconds),
            configured_inner(command, stop, config),
        )
        .await
    }

    async fn configured_inner(command: Command, stop: CancellationToken, config: Config) -> Output {
        if config.validate_grant_store().is_err() {
            return Output::error("configuration", 2);
        }
        let Some(scope) = config.notion_scope else {
            return Output::error("configuration", 2);
        };
        let NotionAuth::Integration(token) = config.notion_auth else {
            return Output::error("configuration", 2);
        };
        let settings = match WorkerSettings::index_settings(&mut |name| std::env::var_os(name)) {
            Ok(s) => s,
            Err(_) => return Output::error("configuration", 2),
        };
        let state =
            match std::env::var_os("NK_WEBHOOK_STATE_FILE").and_then(|s| s.into_string().ok()) {
                Some(s) if PathBuf::from(&s).is_absolute() && !s.chars().any(char::is_control) => {
                    PathBuf::from(s)
                }
                _ => return Output::error("configuration", 2),
            };
        let source = match notion_knowledge_notion::NotionClient::integration(token.expose_secret())
        {
            Ok(s) => Arc::new(s),
            Err(_) => return Output::error("configuration", 2),
        };
        if crate::index_runtime::configured_plan(&settings).is_err() {
            return Output::error("configuration", 2);
        }
        let provider = match notion_knowledge_retrieval::embedder::load_configured_embedder(
            &notion_knowledge_retrieval::embedder::EmbedderConfig::LocalQwen {
                assets: settings.assets.clone(),
            },
            None,
        )
        .await
        {
            Ok(p) => p,
            Err(_) => return Output::error("model_configuration", 2),
        };
        execute(command, &settings, &state, scope, source, provider, stop).await
    }

    /// The actual operator boundary, also used by deterministic adapter fixtures.
    /// All CLI/run checks precede writable composition and lease acquisition.
    pub async fn execute(
        command: Command,
        settings: &WorkerSettings,
        state: &std::path::Path,
        scope: notion_knowledge_core::lifecycle::LifecycleScope,
        source: Arc<notion_knowledge_notion::NotionClient>,
        provider: Arc<dyn notion_knowledge_core::embedding::EmbeddingProvider>,
        stop: CancellationToken,
    ) -> Output {
        let inspected = match IndexRuntime::compose(
            settings,
            state,
            scope.clone(),
            source.clone(),
            provider.clone(),
            OpenMode::ReadOnly,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return Output::error("index_state_configuration", 2),
        };
        if matches!(
            command,
            Command::ScheduleStatus
                | Command::ScheduleHistory(_)
                | Command::ScheduleRetry(_)
                | Command::ScheduleAbandon(_)
                | Command::ScheduleInterval { .. }
                | Command::ScheduleConfigure { .. }
        ) {
            if command.read_only() {
                return schedule_command(&inspected, command);
            }
            drop(inspected);
            let runtime = match IndexRuntime::compose(
                settings,
                state,
                scope,
                source,
                provider,
                OpenMode::ExistingWrite,
            )
            .await
            {
                Ok(r) => r,
                Err(_) => return Output::error("index_state_configuration", 2),
            };
            return schedule_command(&runtime, command);
        }
        if command == Command::DryRun {
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Output::error("cancelled", 5),
                _ = tokio::time::sleep(Duration::from_secs(settings.inventory_timeout_seconds)) => return Output::error("timeout", 3),
                result = inspected.dry_run() => result,
            };
            return match result {
                Ok(p) => Output {
                    json: serde_json::json!({"operation":"reconciliation", "status":"dry_run",
                        "scanned":p.inventory, "skipped":p.skipped, "complete":p.complete, "drift":p.drift}),
                    exit: if p.complete { 0 } else { 3 },
                },
                Err(_) => Output::error("planning_failure", 3),
            };
        }
        drop(inspected);
        let runtime = match IndexRuntime::compose(
            settings,
            state,
            scope,
            source,
            provider,
            OpenMode::ExistingWrite,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return Output::error("index_state_configuration", 2),
        };
        let mut lease = match runtime
            .coordinator
            .state()
            .acquire_lease(now_ms() / 1000, settings.timeout_seconds as i64 + 30)
        {
            Ok(l) => l,
            Err(JournalError::Busy) => return Output::error("busy", 4),
            Err(_) => return Output::error("state_failure", 3),
        };
        let attempt = stop.child_token();
        let cancelled = attempt.clone();
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
            if cancelled.is_cancelled() {
                i64::MAX
            } else {
                now_ms() / 1000
            }
        });
        let page = PageDeadline {
            timeout_seconds: settings.timeout_seconds,
        };
        // A single-page sync never proves the whole walk complete.
        let command_is_page = matches!(command, Command::Retry(_));
        let result = {
            let runtime = &runtime;
            let lease = &mut lease;
            let work = async move {
                let rebuild = match &command {
                    Command::RebuildRelations => Some(false),
                    Command::RebuildAliases => Some(true),
                    _ => None,
                };
                if let Some(aliases) = rebuild {
                    runtime
                        .coordinator
                        .begin_projection_rebuild(
                            aliases,
                            runtime.scope.clone(),
                            lease.clone(),
                            clock.clone(),
                        )
                        .await
                        .map_err(|_| "projection rebuild failed")?;
                }
                let mode = match command {
                    Command::Retry(page) => {
                        SyncMode::Page(notion_knowledge_core::backend::PageId(page))
                    }
                    _ => SyncMode::Full,
                };
                let totals = Box::pin(runtime.sync_from_walk(&page, lease, mode))
                    .await
                    .map_err(|e| e.reason)?;
                crate::index_runtime::prepare_search_if_missing(
                    &runtime.coordinator,
                    &runtime.handler,
                    lease.clone(),
                    clock,
                )
                .await
                .map_err(|_| "search preparation failed")?;
                Ok::<_, &'static str>(totals)
            };
            tokio::select! { biased;
                _ = stop.cancelled() => { attempt.cancel(); None },
                _ = tokio::time::sleep(Duration::from_secs(settings.inventory_timeout_seconds)) => { attempt.cancel(); Some(Err("timeout")) },
                result = work => Some(result),
            }
        };
        if !matches!(result, Some(Ok(_))) {
            // A submitted owned effect still holds its guard: drain it before
            // releasing the lease or exiting.
            let _ = runtime.coordinator.drain_owned().await;
        }
        let _ = runtime
            .coordinator
            .state()
            .release_lease(&lease, now_ms() / 1000);
        match result {
            None => Output::error("cancelled", 5),
            Some(Err("timeout")) => Output::error("timeout", 3),
            Some(Err(_)) => Output::error("work_failure", 3),
            Some(Ok(t)) => {
                let errors = t.failed;
                // A single-page sync never proves the walk complete, but a page the
                // inventory could not place was not synced at all.
                let clean = errors == 0
                    && t.held == 0
                    && if command_is_page {
                        t.drift.unplaced == 0 && t.inventory > 0 || t.removed > 0
                    } else {
                        t.complete
                    };
                Output {
                    json: serde_json::json!({"operation":"reconciliation", "status":"finished",
                        "scanned":t.inventory, "changed":t.indexed, "deleted":t.removed,
                        "unchanged":t.unchanged, "skipped":t.skipped, "errors":errors,
                        "permanent":t.permanent, "held":t.held, "complete":t.complete,
                        "drift":t.drift}),
                    exit: if clean { 0 } else { 3 },
                }
            }
        }
    }
    fn schedule_command(runtime: &IndexRuntime, command: Command) -> Output {
        use crate::scheduled_reconciliation::runtime::ID;
        let coordinator = &runtime.coordinator;
        let after = if let Command::ScheduleHistory(after) = &command {
            *after
        } else {
            0
        };
        if !command.read_only() {
            let now = now_ms() / 1000;
            let lease = match coordinator.state().acquire_lease(now, 30) {
                Ok(l) => l,
                Err(JournalError::Busy) => return Output::error("busy", 4),
                Err(_) => return Output::error("state_failure", 3),
            };
            let result = match command {
                Command::ScheduleRetry(deadline) => {
                    coordinator.retry_schedule(&lease, now, ID, deadline)
                }
                Command::ScheduleAbandon(deadline) => {
                    coordinator.abandon_schedule(&lease, now, ID, deadline)
                }
                Command::ScheduleConfigure { settings, deadline } => {
                    notion_knowledge_retrieval::schedule::SchedulePolicy::new(
                        settings.interval,
                        settings.attempts,
                        settings.backoff,
                        settings.maximum_backoff,
                    )
                    .and_then(|policy| {
                        coordinator.reconfigure_schedule(
                            &lease,
                            now,
                            ID,
                            &notion_knowledge_retrieval::schedule::ScheduleReconfiguration {
                                previous_binding: coordinator.binding().clone(),
                                policy,
                                next_deadline: deadline,
                            },
                        )
                    })
                }
                Command::ScheduleInterval { seconds, deadline } => {
                    coordinator.schedule(ID).and_then(|s| {
                        let policy = notion_knowledge_retrieval::schedule::SchedulePolicy::new(
                            seconds,
                            s.policy.max_attempts(),
                            s.policy.backoff_seconds(),
                            s.policy.max_backoff_seconds(),
                        )?;
                        coordinator.reconfigure_schedule(
                            &lease,
                            now,
                            ID,
                            &notion_knowledge_retrieval::schedule::ScheduleReconfiguration {
                                previous_binding: coordinator.binding().clone(),
                                policy,
                                next_deadline: deadline,
                            },
                        )
                    })
                }
                _ => unreachable!(),
            };
            let _ = coordinator.state().release_lease(&lease, now_ms() / 1000);
            if let Err(e) = result {
                return Output::error(
                    if e == JournalError::Busy {
                        "busy"
                    } else {
                        "recovery_rejected"
                    },
                    if e == JournalError::Busy { 4 } else { 3 },
                );
            }
        }
        let schedule = match coordinator.schedule(ID) {
            Ok(s) => s,
            Err(_) => return Output::error("schedule_unavailable", 3),
        };
        let events = match coordinator.schedule_events(ID, after, 1000) {
            Ok(e) => e,
            Err(_) => return Output::error("state_failure", 3),
        };
        let mut json = serde_json::json!({"operation":"reconciliation_schedule", "history_next":events.last().map(|e|e.sequence), "status":format!("{:?}",schedule.status), "next_deadline":schedule.next_deadline,"attempts":schedule.attempts,"cycle":schedule.cycle,"active_run":schedule.active_run,
            "hold":schedule.hold.map(|h|h.as_str()),"hold_reason":schedule.hold.map(|h|h.reason()),
            "history":events.iter().map(|e|serde_json::json!({"sequence":e.sequence,"cycle":e.cycle,"at_seconds":e.at_seconds,"kind":format!("{:?}",e.kind),"failure":e.failure.as_ref().map(|f|format!("{f:?}")),"attempt":e.attempt,"deadline":e.deadline})).collect::<Vec<_>>()});
        if let Some(run) = schedule.active_run {
            json["run"] = status(runtime, &run, "inspected", None).json;
        }
        if let Some(run) = events
            .iter()
            .rev()
            .find(|e| e.kind == notion_knowledge_retrieval::schedule::ScheduleEventKind::Completed)
            .and_then(|e| e.run_id.as_ref())
        {
            json["last_completed_run"] = status(runtime, run, "inspected", None).json;
        }
        Output { json, exit: 0 }
    }
    fn status(runtime: &IndexRuntime, run: &str, class: &'static str, exit: Option<u8>) -> Output {
        let current = match runtime.coordinator.state().run(run, &runtime.scope) {
            Ok(r) => r,
            Err(_) => return Output::error(class, exit.unwrap_or(3)),
        };
        let errors = usize::from(current.failure.is_some());
        let mut json = serde_json::json!({});
        let failure_history = match runtime
            .coordinator
            .reconciliation_failure_evidence(run, 0, 1000)
        {
            Ok(events) => events,
            Err(_) => return Output::error("state_failure", 3),
        };
        json["failure_history"] = serde_json::json!(failure_history.iter().map(|e|serde_json::json!({"sequence":e.sequence,"page_id":e.page_id,"kind":e.kind,"class":e.class})).collect::<Vec<_>>());
        json["errors"] = errors.into();
        json["operation"] = "reconciliation".into();
        json["status"] = class.into();
        json["phase"] = format!("{:?}", current.phase).into();
        json["failure"] = current.failure.map(|f| format!("{f:?}")).into();
        Output {
            json,
            exit: exit.unwrap_or(if errors > 0 { 3 } else { 0 }),
        }
    }
}
