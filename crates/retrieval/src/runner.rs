//! Authoritative differences-only durable application. CLI/scheduling are separate.
use crate::{
    commit::{IndexCommitCoordinator, ReceiptStatus},
    durable_plan::{DurableFailure, DurablePlan, DurableProgress, ProposedDifference},
    planner::{AuthoritativeReconciliationPlanner, PlanError, ReconciliationPlan},
    refresh::{AuthoritativePageRefresh, ExpectedReconciliation, RefreshError},
};
use notion_knowledge_core::{
    backend::PageId,
    embedding::EmbeddingProvider,
    reconciliation::{JournalError, Lease, ReconciliationJournal, ReconciliationScope, RunPhase},
};
use std::sync::Arc;

#[derive(Debug)]
pub enum RunnerError {
    Journal(JournalError),
    Planning(PlanError),
    InvalidConfiguration,
}
impl From<JournalError> for RunnerError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}

#[derive(Clone)]
pub struct AuthoritativeReconciliationRunner {
    planner: AuthoritativeReconciliationPlanner,
    refresh: AuthoritativePageRefresh,
    coordinator: Arc<IndexCommitCoordinator>,
    scope: ReconciliationScope,
}
impl AuthoritativeReconciliationRunner {
    pub fn new(
        planner: AuthoritativeReconciliationPlanner,
        refresh: AuthoritativePageRefresh,
        coordinator: Arc<IndexCommitCoordinator>,
        scope: ReconciliationScope,
    ) -> Result<Self, RunnerError> {
        if !refresh.is_bound_to(&coordinator)
            || !planner.is_bound_to(&coordinator)
            || coordinator.binding().scope() != scope.identity()
        {
            return Err(RunnerError::InvalidConfiguration);
        }
        Ok(Self {
            planner,
            refresh,
            coordinator,
            scope,
        })
    }
    /// No journal, checkpoint, content extraction or embedding writes.
    pub async fn dry_run(&self) -> Result<ReconciliationPlan, RunnerError> {
        self.planner.plan().await.map_err(RunnerError::Planning)
    }
    /// The caller supplies the shared writer lease. Each page is bounded by that
    /// lease; renew between calls, never extend an already detached commit's bound.
    /// Dropping this observer retains a pending page and any live owned commit.
    pub async fn apply(
        &self,
        run: &str,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        provider: &dyn EmbeddingProvider,
    ) -> Result<DurableProgress, RunnerError> {
        let state = self.coordinator.state();
        let current = state.start_or_resume(&lease, clock(), run, &self.scope)?;
        if current.phase == RunPhase::Inventory {
            let plan = self.dry_run().await?;
            self.coordinator
                .record_reconciliation_plan(&lease, clock(), run, &plan)?;
        }
        if current.phase == RunPhase::Completed {
            return Ok(self.coordinator.reconciliation_progress(run)?);
        }
        self.coordinator.reconciliation_plan(run)?;
        let progress = self.coordinator.reconciliation_progress(run)?;
        for id in &progress.pending {
            if clock() >= lease.expires_at {
                return Err(JournalError::LeaseLost.into());
            }
            let (page, replan) = self.coordinator.reconciliation_page_proposal(run, id)?;
            self.apply_one(
                ExpectedReconciliation {
                    run: run.into(),
                    page,
                    replan,
                },
                lease.clone(),
                clock.clone(),
                provider,
            )
            .await?;
        }
        let result = self.coordinator.reconciliation_progress(run)?;
        if result.pending.is_empty() && result.failed.is_empty() {
            self.coordinator
                .complete_reconciliation_plan(&lease, clock(), run)?;
        }
        Ok(result)
    }
    /// One inventory or page attempt. Operators may renew the global fence only
    /// after this returns; a submitted owned operation keeps its original bound.
    pub async fn apply_next(
        &self,
        run: &str,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        provider: &dyn EmbeddingProvider,
    ) -> Result<DurableProgress, RunnerError> {
        let current =
            self.coordinator
                .state()
                .start_or_resume(&lease, clock(), run, &self.scope)?;
        if current.phase == RunPhase::Inventory {
            let plan = self.dry_run().await?;
            self.coordinator
                .record_reconciliation_plan(&lease, clock(), run, &plan)?;
        } else if current.phase != RunPhase::Completed {
            self.coordinator.reconciliation_plan(run)?;
            let progress = self.coordinator.reconciliation_progress(run)?;
            if let Some(id) = progress.pending.first() {
                let (page, replan) = self.coordinator.reconciliation_page_proposal(run, id)?;
                self.apply_one(
                    ExpectedReconciliation {
                        run: run.into(),
                        page,
                        replan,
                    },
                    lease.clone(),
                    clock.clone(),
                    provider,
                )
                .await?;
            }
        }
        let progress = self.coordinator.reconciliation_progress(run)?;
        if current.phase != RunPhase::Completed
            && progress.pending.is_empty()
            && progress.failed.is_empty()
        {
            self.coordinator
                .complete_reconciliation_plan(&lease, clock(), run)?;
        }
        Ok(progress)
    }
    /// Failed pages never retry implicitly. A fresh full plan supplies a new
    /// proposal and local snapshot; races still conflict at the owned boundary.
    pub async fn retry_page(
        &self,
        run: &str,
        page: &str,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        provider: &dyn EmbeddingProvider,
    ) -> Result<DurableProgress, RunnerError> {
        self.coordinator
            .state()
            .start_or_resume(&lease, clock(), run, &self.scope)?;
        let fresh = self.dry_run().await?;
        let mut candidate = DurablePlan::from_planner(run, &fresh)?
            .pages
            .remove(page)
            .ok_or(JournalError::InvalidTransition)?;
        let old = self.coordinator.reconciliation_plan(run)?;
        candidate.operation_id = old
            .pages
            .get(page)
            .ok_or(JournalError::InvalidInput)?
            .operation_id
            .clone();
        self.coordinator
            .retry_reconciliation_proposal(&lease, clock(), run, &candidate)?;
        self.apply_one(
            ExpectedReconciliation {
                run: run.into(),
                page: candidate,
                replan: true,
            },
            lease.clone(),
            clock.clone(),
            provider,
        )
        .await?;
        let result = self.coordinator.reconciliation_progress(run)?;
        if result.pending.is_empty() && result.failed.is_empty() {
            self.coordinator
                .complete_reconciliation_plan(&lease, clock(), run)?;
        }
        Ok(result)
    }
    /// Completion is explicit so failed work remains available for explicit retry.
    pub fn complete(
        &self,
        run: &str,
        lease: &Lease,
        now: i64,
    ) -> Result<DurableProgress, RunnerError> {
        if self.coordinator.state().run(run, &self.scope)?.phase != RunPhase::Completed {
            self.coordinator
                .complete_reconciliation_plan(lease, now, run)?;
        }
        Ok(self.coordinator.reconciliation_progress(run)?)
    }
    async fn apply_one(
        &self,
        expected: ExpectedReconciliation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        provider: &dyn EmbeddingProvider,
    ) -> Result<(), RunnerError> {
        let page = expected.page.page_id.clone();
        let run = expected.run.clone();
        let applied = self
            .coordinator
            .receipt(&expected.page.operation_id)
            .map_err(|_| JournalError::Unavailable)?
            .is_some_and(|r| r.status == ReceiptStatus::Applied);
        let result = if applied {
            self.refresh
                .recover_reconciliation(run.clone(), page.clone(), lease.clone(), clock.clone())
                .await
        } else if expected.page.difference == ProposedDifference::Unchanged {
            self.refresh
                .apply_unchanged(expected, lease.clone(), clock.clone())
                .await
        } else {
            let operation = expected.page.operation_id.clone();
            self.refresh
                .apply_lifecycle_expected(
                    &PageId(page.clone()),
                    &operation,
                    provider,
                    lease.clone(),
                    clock.clone(),
                    Some(expected),
                )
                .await
                .map(|_| ())
        };
        match result {
            Ok(()) => {
                self.coordinator.acknowledge_reconciliation_receipt(
                    &lease,
                    clock(),
                    &run,
                    &page,
                )?;
            }
            Err(error) => {
                let class = match error {
                    RefreshError::Conflict | RefreshError::InvalidConfiguration => {
                        DurableFailure::Conflict
                    }
                    RefreshError::OutOfScope => DurableFailure::Permission,
                    RefreshError::PermanentSource => DurableFailure::UnsupportedSource,
                    RefreshError::Source => DurableFailure::Source,
                    RefreshError::Index | RefreshError::Unavailable => DurableFailure::Index,
                };
                self.coordinator
                    .fail_reconciliation_page(&lease, clock(), &run, &page, class)?;
            }
        }
        Ok(())
    }
}
