//! Validated, payload-free operator commands. Scheduling is a separate caller.
use notion_knowledge_notion::pages::page_id;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Apply(String),
    DryRun,
    Resume(String),
    Status(String),
    Retry { run: String, page: String },
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
            ["--reconcile", "apply", id] => Ok(Self::Apply(run(id)?)),
            ["--reconcile", "resume", id] => Ok(Self::Resume(run(id)?)),
            ["--reconcile", "status", id] => Ok(Self::Status(run(id)?)),
            ["--reconcile", "retry", id, page] => Ok(Self::Retry {
                run: run(id)?,
                page: page_id(page)
                    .map_err(|_| "invalid failed-page identifier")?
                    .0,
            }),
            _ => Err(
                "expected --reconcile apply RUN | dry-run | resume RUN | status RUN | retry RUN PAGE",
            ),
        }
    }
    pub fn read_only(&self) -> bool {
        matches!(self, Self::DryRun | Self::Status(_))
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
        if let Some(id) = match &command {
            Command::DryRun => None,
            Command::Apply(s) | Command::Resume(s) | Command::Status(s) => Some(s),
            Command::Retry { run, .. } => Some(run),
        } {
            let existing = inspected.coordinator.state().run(id, &inspected.scope);
            let valid = match &command {
                Command::Apply(_) => matches!(existing, Err(JournalError::InvalidTransition)),
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
        let fresh_run = matches!(command, Command::Apply(_));
        let (run, retry) = match command {
            Command::Apply(id) | Command::Resume(id) => (id, None),
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
            let work = async {
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
