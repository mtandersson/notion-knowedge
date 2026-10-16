//! Validated, payload-free operator commands. Scheduling is a separate caller.
use notion_knowledge_notion::pages::page_id;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Apply(String),
    RebuildRelations(String),
    RebuildAliases(String),
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
    Resume(String),
    Status(String),
    Retry {
        run: String,
        page: String,
    },
}
impl Command {
    pub fn parse(args: &[String]) -> Result<Self, &'static str> {
        let run = |s: &str| -> Result<String, &'static str> {
            if s.is_empty()
                || s.len() > 128
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                Err("invalid reconciliation run identifier")
            } else {
                Ok(s.into())
            }
        };
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
            ["--reconcile", "apply", id] => Ok(Self::Apply(run(id)?)),
            ["--reconcile", "rebuild-relations", id] => Ok(Self::RebuildRelations(run(id)?)),
            ["--reconcile", "rebuild-aliases", id] => Ok(Self::RebuildAliases(run(id)?)),
            ["--reconcile", "resume", id] => Ok(Self::Resume(run(id)?)),
            ["--reconcile", "status", id] => Ok(Self::Status(run(id)?)),
            ["--reconcile", "retry", id, page] => Ok(Self::Retry {
                run: run(id)?,
                page: page_id(page)
                    .map_err(|_| "invalid failed-page identifier")?
                    .0,
            }),
            _ => Err(
                "expected --reconcile apply RUN | rebuild-relations RUN | rebuild-aliases RUN | dry-run | resume RUN | status RUN | retry RUN PAGE",
            ),
        }
    }
    pub fn read_only(&self) -> bool {
        matches!(
            self,
            Self::DryRun | Self::Status(_) | Self::ScheduleStatus | Self::ScheduleHistory(_)
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
        index_runtime::{IndexRuntime, OpenMode},
        webhook_worker::{WorkerSettings, runtime::now_ms},
    };
    use notion_knowledge_core::reconciliation::{JournalError, ReconciliationJournal, RunPhase};
    use notion_knowledge_retrieval::{durable_plan::DurableProgress, runner::RunnerError};
    use std::{path::PathBuf, sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;

    pub async fn configured(command: Command, stop: CancellationToken) -> Output {
        let config = match Config::from_env() {
            Ok(c) => c,
            Err(_) => return Output::error("configuration", 2),
        };
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
        let provider = match notion_knowledge_retrieval::qwen::QwenProvider::load(
            notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
        )
        .await
        {
            Ok(p) => Arc::new(p),
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
        if let Some(id) = match &command {
            Command::DryRun
            | Command::ScheduleStatus
            | Command::ScheduleHistory(_)
            | Command::ScheduleRetry(_)
            | Command::ScheduleAbandon(_)
            | Command::ScheduleInterval { .. }
            | Command::ScheduleConfigure { .. } => None,
            Command::Apply(s)
            | Command::RebuildRelations(s)
            | Command::RebuildAliases(s)
            | Command::Resume(s)
            | Command::Status(s) => Some(s),
            Command::Retry { run, .. } => Some(run),
        } {
            let existing = inspected.coordinator.state().run(id, &inspected.scope);
            let valid = match &command {
                Command::Apply(_) | Command::RebuildRelations(_) | Command::RebuildAliases(_) => {
                    matches!(existing, Err(JournalError::InvalidTransition))
                }
                Command::Retry { page, .. } => {
                    existing.is_ok_and(|r| r.phase == RunPhase::Applying)
                        && inspected
                            .coordinator
                            .reconciliation_progress(id)
                            .is_ok_and(|p| p.failed.contains_key(page))
                }
                _ => existing.is_ok(),
            };
            if !valid {
                return Output::error("run_configuration", 2);
            }
        }
        if let Command::Status(id) = &command {
            return status(&inspected, id, "inspected", None);
        }
        if command == Command::DryRun {
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Output::error("cancelled", 5),
                _ = tokio::time::sleep(Duration::from_secs(settings.timeout_seconds)) => return Output::error("timeout", 3),
                result = inspected.runner.dry_run() => result,
            };
            return match result {
                Ok(p) => {
                    let errors = p.errors.len() + p.run_errors.len();
                    Output {
                        json: serde_json::json!({"operation":"reconciliation", "status":"dry_run", "scanned":p.scanned,
                        "planned_refresh":p.planned_refresh(),"planned_delete":p.planned_delete(), "changed":0,"deleted":0,"unchanged":0,
                        "errors":errors,"planning_failures":p.errors.iter().map(|(id,c)|(id, format!("{c:?}"))).collect::<std::collections::BTreeMap<_,_>>(),
                        "run_failures":p.run_errors.iter().map(|c|format!("{c:?}")).collect::<Vec<_>>() }),
                        exit: if errors == 0 { 0 } else { 3 },
                    }
                }
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
        let fresh_run = matches!(
            command,
            Command::Apply(_) | Command::RebuildRelations(_) | Command::RebuildAliases(_)
        );
        let mut rebuild_aliases = matches!(command, Command::RebuildAliases(_));
        let mut rebuild_relations = matches!(command, Command::RebuildRelations(_));
        let (run, retry) = match command {
            Command::Apply(id)
            | Command::RebuildRelations(id)
            | Command::RebuildAliases(id)
            | Command::Resume(id) => (id, None),
            Command::Retry { run, page } => (run, Some(page)),
            _ => unreachable!(),
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
        if fresh_run
            && !matches!(
                runtime.coordinator.state().run(&run, &runtime.scope),
                Err(JournalError::InvalidTransition)
            )
        {
            let _ = runtime
                .coordinator
                .state()
                .release_lease(&lease, now_ms() / 1000);
            return Output::error("run_configuration", 2);
        }
        let mut retry = retry;
        let output = loop {
            let attempt = stop.child_token();
            let cancelled = attempt.clone();
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                if cancelled.is_cancelled() {
                    i64::MAX
                } else {
                    now_ms() / 1000
                }
            });
            let page = retry.take();
            let aliases = std::mem::take(&mut rebuild_aliases);
            let rebuild = std::mem::take(&mut rebuild_relations);
            let work = async {
                if aliases {
                    runtime
                        .runner
                        .begin_alias_rebuild(&run, lease.clone(), clock.clone())
                        .await?;
                }
                if rebuild {
                    runtime
                        .runner
                        .begin_relation_rebuild(&run, lease.clone(), clock.clone())
                        .await?;
                }
                if let Some(page) = &page {
                    runtime
                        .runner
                        .retry_page(&run, page, lease.clone(), clock, runtime.provider.as_ref())
                        .await
                } else {
                    runtime
                        .runner
                        .apply_next(&run, lease.clone(), clock, runtime.provider.as_ref())
                        .await
                }
            };
            let result = tokio::select! { biased;
                _ = stop.cancelled() => { attempt.cancel(); None },
                _ = tokio::time::sleep(Duration::from_secs(settings.timeout_seconds)) => { attempt.cancel(); Some(Err(RunnerError::Journal(JournalError::LeaseLost))) },
                result = work => Some(result),
            };
            if attempt.is_cancelled() {
                // The observer has dropped; an already live mutation still owns
                // its guard. Drain before releasing/renewing or exiting normally.
                let _ = runtime.coordinator.drain_owned().await;
                break status(
                    &runtime,
                    &run,
                    if stop.is_cancelled() {
                        "cancelled"
                    } else {
                        "timeout"
                    },
                    Some(if stop.is_cancelled() { 5 } else { 3 }),
                );
            }
            match result {
                Some(Ok(p)) if !p.pending.is_empty() => {
                    match runtime.coordinator.state().renew_lease(
                        &lease,
                        now_ms() / 1000,
                        settings.timeout_seconds as i64 + 30,
                    ) {
                        Ok(l) => lease = l,
                        Err(_) => break status(&runtime, &run, "lease_lost", Some(4)),
                    }
                }
                Some(Ok(_)) => break status(&runtime, &run, "finished", None),
                Some(Err(
                    RunnerError::Journal(
                        JournalError::ScopeMismatch
                        | JournalError::InvalidInput
                        | JournalError::InvalidTransition,
                    )
                    | RunnerError::InvalidConfiguration,
                )) => break Output::error("run_configuration", 2),
                Some(Err(RunnerError::Journal(JournalError::LeaseLost | JournalError::Busy))) => {
                    break status(&runtime, &run, "lease_lost", Some(4));
                }
                Some(Err(_)) => break status(&runtime, &run, "work_failure", Some(3)),
                None => break status(&runtime, &run, "cancelled", Some(5)),
            }
        };
        let _ = runtime
            .coordinator
            .state()
            .release_lease(&lease, now_ms() / 1000);
        output
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
    fn progress_json(p: &DurableProgress) -> serde_json::Value {
        serde_json::json!({"scanned":p.scanned,"planned_refresh":p.planned_refresh,"planned_delete":p.planned_delete,
            "changed":p.changed,"deleted":p.deleted,"unchanged":p.unchanged,"pending":p.pending.len(),
            "errors":p.failed_pages()+p.run_failures.len(),"application_failures":p.failed,"planning_failures":p.planning_failures,"run_failures":p.run_failures})
    }
    fn status(runtime: &IndexRuntime, run: &str, class: &'static str, exit: Option<u8>) -> Output {
        let current = match runtime.coordinator.state().run(run, &runtime.scope) {
            Ok(r) => r,
            Err(_) => return Output::error(class, exit.unwrap_or(3)),
        };
        let progress = runtime.coordinator.reconciliation_progress(run);
        let errors = progress
            .as_ref()
            .map(|p| p.failed_pages() + p.run_failures.len())
            .unwrap_or(0)
            + usize::from(current.failure.is_some());
        let mut json = match progress {
            Ok(p) => progress_json(&p),
            Err(_) if current.phase == RunPhase::Inventory => {
                serde_json::json!({"scanned":0,"changed":0,"deleted":0,"unchanged":0,"pending":0})
            }
            Err(_) => return Output::error("state_failure", 3),
        };
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
