//! Bounded periodic caller of the shared authoritative index runtime.
use crate::config::ConfigError;
use std::ffi::OsString;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub interval: i64,
    pub attempts: i64,
    pub backoff: i64,
    pub maximum_backoff: i64,
}
impl Settings {
    pub(crate) fn parse(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        worker: bool,
    ) -> Result<Option<Self>, ConfigError> {
        let Some(raw) = lookup("NK_RECONCILIATION_INTERVAL_SECONDS") else {
            return Ok(None);
        };
        let error = |setting| ConfigError {
            setting,
            reason: "outside documented integer range or missing shared serving worker",
        };
        let interval = raw
            .into_string()
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| (60..=2_592_000).contains(n))
            .ok_or_else(|| error("NK_RECONCILIATION_INTERVAL_SECONDS"))?;
        if !worker {
            return Err(error("NK_RECONCILIATION_INTERVAL_SECONDS"));
        }
        let mut number = |name, default, min, max| {
            lookup(name)
                .map(|v| v.into_string().ok().and_then(|s| s.parse::<i64>().ok()))
                .unwrap_or(Some(default))
                .filter(|n| (min..=max).contains(n))
                .ok_or_else(|| error(name))
        };
        let attempts = number("NK_RECONCILIATION_MAX_ATTEMPTS", 4, 1, 16)?;
        let backoff = number("NK_RECONCILIATION_BACKOFF_SECONDS", 5, 1, 86_400)?;
        let maximum_backoff = number(
            "NK_RECONCILIATION_MAX_BACKOFF_SECONDS",
            300,
            backoff,
            604_800,
        )?;
        Ok(Some(Self {
            interval,
            attempts,
            backoff,
            maximum_backoff,
        }))
    }
}
#[cfg(all(unix, feature = "webhook-worker"))]
pub mod runtime {
    use super::Settings;
    use crate::{index_runtime::IndexRuntime, webhook_worker::runtime::now_ms};
    use notion_knowledge_core::reconciliation::{
        FailureClass, JournalError, ReconciliationJournal,
    };
    use notion_knowledge_retrieval::{runner::RunnerError, schedule::SchedulePolicy};
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;
    pub const ID: &str = "periodic";
    pub struct Worker {
        pub index: IndexRuntime,
        pub settings: Settings,
        pub poll: Duration,
        pub timeout: Duration,
    }
    impl Worker {
        pub async fn run(&self, stop: CancellationToken) -> Result<(), &'static str> {
            loop {
                if stop.is_cancelled() {
                    return Ok(());
                }
                self.tick(stop.clone()).await?;
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(self.poll) => {} }
            }
        }
        pub async fn tick(&self, stop: CancellationToken) -> Result<bool, &'static str> {
            if stop.is_cancelled() {
                return Ok(false);
            }
            let coordinator = &self.index.coordinator;
            let state = coordinator.state();
            let ttl = self.timeout.as_secs() as i64 + 30;
            let mut lease = match state.acquire_lease(now_ms() / 1000, ttl) {
                Ok(l) => l,
                Err(JournalError::Busy) => return Ok(false),
                Err(_) => return Err("scheduled lease unavailable"),
            };
            let result = async {
                let now = now_ms()/1000;
                let policy = SchedulePolicy::new(self.settings.interval, self.settings.attempts, self.settings.backoff, self.settings.maximum_backoff).map_err(|_| "scheduled policy invalid")?;
                coordinator.register_schedule(&lease, now, ID, policy, now).map_err(|_| "scheduled authority incompatible")?;
                if !coordinator.due_schedules(now, 1000).map_err(|_| "scheduled state unavailable")?.iter().any(|s| s.schedule_id == ID) { return Ok(false) }
                let proposal = format!("periodic-{}", notion_knowledge_core::logging::CorrelationId::new().as_str());
                let schedule = match coordinator.claim_schedule(&lease, now, ID, &proposal) {
                    Ok(s) => s, Err(JournalError::Busy) => return Ok(false), Err(_) => return Err("scheduled claim unavailable"),
                };
                let run = schedule.active_run.ok_or("scheduled run unavailable")?;
                loop {
                    let attempt = stop.child_token();
                    let cancelled = attempt.clone();
                    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || if cancelled.is_cancelled() { i64::MAX } else { now_ms()/1000 });
                    let result = tokio::select! { biased;
                        _ = stop.cancelled() => { attempt.cancel(); None },
                        _ = tokio::time::sleep(self.timeout) => { attempt.cancel(); Some(Err(RunnerError::Journal(JournalError::Unavailable))) },
                        result = self.index.runner.apply_next(&run, lease.clone(), clock, self.index.provider.as_ref()) => Some(result),
                    };
                    if attempt.is_cancelled() {
                        coordinator.drain_owned().await.map_err(|_| "scheduled drain failed")?;
                        // Never renew a running effect. After drain, an expired lease
                        // leaves its durable checkpoint for the next service owner.
                        if stop.is_cancelled() {
                            match coordinator.checkpoint_schedule(&lease, now_ms()/1000, ID) { Ok(_) | Err(JournalError::LeaseLost) => {}, Err(_) => return Err("scheduled checkpoint unavailable") }
                            return Ok(true)
                        }
                    }
                    match result {
                        Some(Ok(p)) => {
                            if p.pending.is_empty() && (p.failed_pages() > 0 || !p.run_failures.is_empty()) {
                                coordinator.hold_schedule(&lease, now_ms()/1000, ID).map_err(|_| "scheduled hold unavailable")?;
                                return Ok(true)
                            }
                            if p.pending.is_empty() { return Ok(true) }
                            lease = state.renew_lease(&lease, now_ms()/1000, ttl).map_err(|_| "scheduled lease lost")?;
                        }
                        Some(Err(RunnerError::Journal(JournalError::Busy | JournalError::LeaseLost))) => return Ok(false),
                        Some(Err(RunnerError::InvalidConfiguration | RunnerError::Journal(JournalError::ScopeMismatch | JournalError::InvalidInput | JournalError::InvalidTransition))) => return Err("scheduled authority invalid"),
                        Some(Err(error)) => {
                            use notion_knowledge_retrieval::planner::PlanError;
                            let class = match error {
                                RunnerError::Planning(PlanError::Index | PlanError::Ownership) => FailureClass::Index,
                                RunnerError::Planning(PlanError::Conflict) => FailureClass::Conflict,
                                RunnerError::Planning(_) => FailureClass::Source,
                                _ => FailureClass::Unavailable,
                            };
                            let event = format!("attempt-{}", notion_knowledge_core::logging::CorrelationId::new().as_str());
                            match coordinator.fail_schedule(&lease, now_ms()/1000, ID, &event, class) { Ok(_) | Err(JournalError::LeaseLost) => {}, Err(_) => return Err("scheduled failure unavailable") }
                            return Ok(true)
                        }
                        None => return Ok(true),
                    }
                }
            }.await;
            match state.release_lease(&lease, now_ms() / 1000) {
                Ok(()) | Err(JournalError::LeaseLost) => {}
                Err(_) => return Err("scheduled lease release failed"),
            }
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interval_is_opt_in_and_bounded_before_runtime_composition() {
        assert!(Settings::parse(&mut |_| None, false).unwrap().is_none());
        for value in ["", "0", "59", "2592001", "-1", "1.5"] {
            let result = Settings::parse(
                &mut |name| {
                    (name == "NK_RECONCILIATION_INTERVAL_SECONDS").then(|| OsString::from(value))
                },
                true,
            );
            assert!(result.is_err());
        }
        for value in ["60", "2592000"] {
            let mut lookup = |name: &str| {
                (name == "NK_RECONCILIATION_INTERVAL_SECONDS").then(|| OsString::from(value))
            };
            assert!(Settings::parse(&mut lookup, true).is_ok());
            assert!(Settings::parse(&mut lookup, false).is_err());
        }
    }
}

#[cfg(all(test, unix, feature = "webhook-worker"))]
mod serving_configuration_tests {
    use crate::config::Config;
    fn parse(extra: &[(&str, &str)]) -> Result<Config, crate::config::ConfigError> {
        let mut values = std::collections::BTreeMap::from([
            ("NK_RECONCILIATION_INTERVAL_SECONDS", "60"),
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
            ("NK_WEBHOOK_INDEX_DIR", "/tmp/index"),
            ("NK_WEBHOOK_QWEN_ASSETS", "/tmp/assets"),
            ("NK_WEBHOOK_STATE_FILE", "/tmp/state.sqlite"),
            ("NK_WEBHOOK_WORKER", "false"),
        ]);
        values.extend(extra.iter().copied());
        Config::from_lookup(|key| values.get(key).map(std::ffi::OsString::from))
    }
    #[test]
    fn periodic_configuration_does_not_require_webhook_activation_and_validates_all_effect_inputs()
    {
        let c = parse(&[]).unwrap();
        assert!(c.webhook_worker.is_none());
        assert!(c.reconciliation_index.is_some());
        assert!(matches!(c.webhook, crate::webhook::WebhookConfig::Disabled));
        for (key, value) in [
            ("NK_RECONCILIATION_INTERVAL_SECONDS", "59"),
            ("NK_RECONCILIATION_MAX_ATTEMPTS", "17"),
            ("NK_RECONCILIATION_BACKOFF_SECONDS", "0"),
            ("NK_RECONCILIATION_MAX_BACKOFF_SECONDS", "4"),
            ("NK_NOTION_AUTH", "none"),
            ("NK_NOTION_SCOPE_ROOTS", "bad"),
            ("NK_WEBHOOK_INDEX_DIR", "relative"),
            ("NK_WEBHOOK_QWEN_ASSETS", ""),
            ("NK_WEBHOOK_STATE_FILE", "relative"),
            ("NK_NOTION_SCOPE_EXCLUDED_PAGES", "bad"),
        ] {
            assert!(parse(&[(key, value)]).is_err(), "{key}");
        }
    }
    #[tokio::test]
    async fn missing_offline_model_rejects_periodic_startup_without_creating_state() {
        let dir = tempfile::tempdir().unwrap();
        let index = dir.path().join("index");
        let state = dir.path().join("state.sqlite");
        let assets = dir.path().join("missing");
        let c = parse(&[
            ("NK_WEBHOOK_INDEX_DIR", index.to_str().unwrap()),
            ("NK_WEBHOOK_STATE_FILE", state.to_str().unwrap()),
            ("NK_WEBHOOK_QWEN_ASSETS", assets.to_str().unwrap()),
        ])
        .unwrap();
        assert!(
            crate::webhook_worker::runtime::configured(&c)
                .await
                .is_err()
        );
        assert!(!index.exists());
        assert!(!state.exists());
    }
}
