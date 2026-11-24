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
    use crate::{
        index_runtime::{IndexRuntime, PageDeadline, SyncError, SyncFailure, SyncMode, SyncTotals},
        webhook_worker::runtime::now_ms,
    };
    use notion_knowledge_core::reconciliation::{
        FailureClass, JournalError, Lease, ReconciliationJournal,
    };
    use notion_knowledge_retrieval::schedule::SchedulePolicy;
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;
    pub const ID: &str = "periodic";
    pub struct Worker {
        pub index: Arc<IndexRuntime>,
        pub settings: Settings,
        pub poll: Duration,
        pub timeout: Duration,
        pub inventory_timeout: Duration,
    }
    impl Worker {
        pub async fn run(&self, stop: CancellationToken) -> Result<(), &'static str> {
            let mut first = true;
            loop {
                if stop.is_cancelled() {
                    return Ok(());
                }
                // A restart inside the interval catches up through search.
                // A busy writer (for example a webhook tick) retries it next poll.
                if !self.tick(stop.clone()).await? {
                    if first {
                        first = !self.catch_up(stop.clone()).await?;
                    }
                } else {
                    first = false;
                }
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(self.poll) => {} }
            }
        }

        fn lease(&self) -> Result<Option<Lease>, &'static str> {
            let ttl = self.timeout.as_secs() as i64 + 30;
            match self
                .index
                .coordinator
                .state()
                .acquire_lease(now_ms() / 1000, ttl)
            {
                Ok(lease) => Ok(Some(lease)),
                Err(JournalError::Busy) => Ok(None),
                Err(_) => Err("scheduled lease unavailable"),
            }
        }

        fn release(&self, lease: &Lease) -> Result<(), &'static str> {
            match self
                .index
                .coordinator
                .state()
                .release_lease(lease, now_ms() / 1000)
            {
                Ok(()) | Err(JournalError::LeaseLost) => Ok(()),
                Err(_) => Err("scheduled lease release failed"),
            }
        }

        /// One sync under the inventory budget, then search preparation.
        /// `None` when stopped; owned effects are drained either way.
        async fn sync(
            &self,
            stop: &CancellationToken,
            lease: &mut Lease,
            mode: SyncMode,
        ) -> Result<Option<Result<SyncTotals, SyncError>>, &'static str> {
            let page = PageDeadline {
                timeout_seconds: self.timeout.as_secs(),
            };
            let index = &self.index;
            let outcome = tokio::select! { biased;
                _ = stop.cancelled() => None,
                _ = tokio::time::sleep(self.inventory_timeout) => Some(Err(SyncError::from("scheduled sync timeout"))),
                result = async {
                    let totals = Box::pin(index.sync_from_walk(&page, lease, mode)).await?;
                    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| now_ms() / 1000);
                    crate::index_runtime::prepare_search_if_missing(&index.coordinator, &index.handler, lease.clone(), clock)
                        .await
                        .map_err(|_| SyncError::from("scheduled search preparation failed"))?;
                    Ok(totals)
                } => Some(result),
            };
            if !matches!(outcome, Some(Ok(_))) {
                // Never renew a running effect. After drain, an expired lease
                // leaves the cycle's run for the next service owner.
                index
                    .coordinator
                    .drain_owned()
                    .await
                    .map_err(|_| "scheduled drain failed")?;
            }
            Ok(outcome)
        }

        /// Run the periodic full walk when it is due. The cycle completes on
        /// the next interval; a failed walk backs off and retries.
        pub async fn tick(&self, stop: CancellationToken) -> Result<bool, &'static str> {
            if stop.is_cancelled() {
                return Ok(false);
            }
            let coordinator = &self.index.coordinator;
            let Some(mut lease) = self.lease()? else {
                return Ok(false);
            };
            let result = async {
                let now = now_ms() / 1000;
                let policy = SchedulePolicy::new(
                    self.settings.interval,
                    self.settings.attempts,
                    self.settings.backoff,
                    self.settings.maximum_backoff,
                )
                .map_err(|_| "scheduled policy invalid")?;
                coordinator
                    .register_schedule(&lease, now, ID, policy, now)
                    .map_err(|_| "scheduled authority incompatible")?;
                if !coordinator
                    .due_schedules(now, 1000)
                    .map_err(|_| "scheduled state unavailable")?
                    .iter()
                    .any(|s| s.schedule_id == ID)
                {
                    return Ok(false);
                }
                let proposal = format!(
                    "periodic-{}",
                    notion_knowledge_core::logging::CorrelationId::new().as_str()
                );
                match coordinator.claim_schedule(&lease, now, ID, &proposal) {
                    Ok(_) => {}
                    Err(JournalError::Busy) => return Ok(false),
                    Err(_) => return Err("scheduled claim unavailable"),
                }
                let outcome = self.sync(&stop, &mut lease, SyncMode::Full).await?;
                let now = now_ms() / 1000;
                let recorded = match outcome {
                    None => coordinator.checkpoint_schedule(&lease, now, ID).map(drop),
                    Some(Ok(totals)) => {
                        let success = totals.complete && totals.failed == 0 && totals.held == 0;
                        coordinator
                            .complete_schedule(&lease, now, ID, success)
                            .map(drop)
                    }
                    Some(Err(error)) => {
                        let fail = |class| {
                            let event = format!(
                                "attempt-{}",
                                notion_knowledge_core::logging::CorrelationId::new().as_str()
                            );
                            coordinator
                                .fail_schedule(&lease, now, ID, &event, class)
                                .map(drop)
                        };
                        match error.failure {
                            // A rejected credential cannot heal by retrying:
                            // hold the cycle without consuming an attempt
                            // until the backoff ceiling, then try again.
                            SyncFailure::Unauthenticated => coordinator
                                .postpone_schedule(
                                    &lease,
                                    now,
                                    ID,
                                    now.saturating_add(self.settings.maximum_backoff),
                                )
                                .map(drop),
                            SyncFailure::Index => fail(FailureClass::Index),
                            SyncFailure::Authority => fail(FailureClass::Conflict),
                            SyncFailure::Unavailable => fail(FailureClass::Unavailable),
                        }
                    }
                };
                match recorded {
                    Ok(()) | Err(JournalError::LeaseLost) => Ok(true),
                    Err(_) => Err("scheduled state unavailable"),
                }
            }
            .await;
            self.release(&lease)?;
            result
        }

        /// One incremental sync outside the schedule. Failures are left to
        /// the next full walk. `false` when the writer was busy.
        async fn catch_up(&self, stop: CancellationToken) -> Result<bool, &'static str> {
            let Some(mut lease) = self.lease()? else {
                return Ok(false);
            };
            let result = async {
                let now = now_ms() / 1000;
                let coordinator = &self.index.coordinator;
                match coordinator.schedule(ID) {
                    Ok(_) => {}
                    Err(JournalError::InvalidTransition) => return Ok(false),
                    Err(_) => return Err("scheduled state unavailable"),
                }
                // A busy lease can hide a due full walk from tick(). Give
                // that walk priority over catch-up once we acquire the lease.
                if coordinator
                    .due_schedules(now, 1000)
                    .map_err(|_| "scheduled state unavailable")?
                    .iter()
                    .any(|s| s.schedule_id == ID)
                {
                    return Ok(false);
                }
                if coordinator
                    .ensure_no_active_reconciliation(&lease, now)
                    .is_ok()
                {
                    self.sync(&stop, &mut lease, SyncMode::Incremental).await?;
                }
                Ok(true)
            }
            .await;
            self.release(&lease)?;
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
}
