#![cfg(all(unix, feature = "local-lancedb"))]
use notion_knowledge_core::{
    backend::{BackendErrorKind, PageId},
    discovery::ExclusionRules,
    lifecycle::*,
    reconciliation::*,
    sync_state::*,
};
use notion_knowledge_retrieval::{
    commit::*, durable_plan::*, planner::*, sync_state::SqliteSyncStateStore,
};
use rusqlite::Connection;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    index: PathBuf,
    state: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "nk-plan-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = root.join("index");
        fs::create_dir_all(&index).unwrap();
        let state = root.join("state.sqlite");
        Self { root, index, state }
    }
    fn coordinator(&self) -> Arc<IndexCommitCoordinator> {
        IndexCommitCoordinator::initialize(&self.index, &self.state, binding()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn lifecycle() -> LifecycleScope {
    LifecycleScope {
        workspace_id: "workspace".into(),
        generation: 1,
        roots: vec![PageId("root".into())],
        exclusions: ExclusionRules::default(),
    }
}
fn scope() -> ReconciliationScope {
    production_scope(&lifecycle()).unwrap()
}
fn binding() -> CommitBinding {
    CommitBinding::new("chunks", "workspace", &scope(), "1", 1).unwrap()
}
fn evidence(status: LifecycleStatus) -> LifecycleEvidence {
    LifecycleEvidence {
        scope: lifecycle(),
        status,
        ancestry: vec![],
    }
}
fn allowed() -> LifecycleEvidence {
    evidence(LifecycleStatus::Allowed {
        roots: vec!["root".into()],
    })
}
fn plan() -> ReconciliationPlan {
    let pages = vec![
        PlannedPage {
            page_id: "gone".into(),
            difference: Difference::Orphan,
            evidence: evidence(LifecycleStatus::Inactive),
            metadata_identity: None,
        },
        PlannedPage {
            page_id: "new".into(),
            difference: Difference::Missing,
            evidence: allowed(),
            metadata_identity: Some("a".repeat(64)),
        },
        PlannedPage {
            page_id: "same".into(),
            difference: Difference::Unchanged,
            evidence: allowed(),
            metadata_identity: Some("b".repeat(64)),
        },
    ];
    ReconciliationPlan {
        binding: binding(),
        index_version: 42,
        local: BTreeMap::new(),
        pages,
        errors: BTreeMap::from([
            ("inaccessible".into(), PlanError::Permission),
            ("badmetadata".into(), PlanError::Source),
        ]),
        run_errors: vec![
            PlanError::Traversal(BackendErrorKind::Unauthenticated),
            PlanError::IncompleteTraversal,
        ],
        scanned: 4,
        inspected_ids: BTreeSet::from([
            "gone".into(),
            "new".into(),
            "same".into(),
            "no-work".into(),
        ]),
        traversal_complete: true,
    }
}
fn start(c: &IndexCommitCoordinator, plan: &ReconciliationPlan) -> Lease {
    let l = c.state().acquire_lease(10, 5).unwrap();
    c.state().start_or_resume(&l, 10, "run", &scope()).unwrap();
    c.record_reconciliation_plan(&l, 10, "run", plan).unwrap();
    l
}
fn operation(c: &IndexCommitCoordinator, page: &str, action: PageAction) -> PageOperation {
    let p = c.reconciliation_plan("run").unwrap();
    let checkpoint = if action == PageAction::Delete {
        PageSyncState::tombstone(page.into(), None).unwrap()
    } else {
        PageSyncState::present(page.into(), format!("hash-{page}"), Some("edited".into())).unwrap()
    };
    PageOperation::new(
        &p.pages[page].operation_id,
        "prepared-revision",
        action,
        checkpoint,
    )
    .unwrap()
}
#[tokio::test]
async fn committed_outcomes_survive_reopen_and_duplicate_ack_without_proposed_action_counts() {
    let f = Fixture::new();
    let c = f.coordinator();
    let source = plan();
    let l = start(&c, &source);
    c.record_reconciliation_plan(&l, 10, "run", &source)
        .unwrap();
    let persisted = c.reconciliation_plan("run").unwrap();
    assert_eq!(persisted.inspected_ids.len(), 4);
    assert_eq!(persisted.index_version, 42);
    assert_eq!(
        persisted.pages["gone"].lifecycle_digest,
        DurablePlan::lifecycle_digest(&source.pages[0].evidence)
    );
    let op = operation(&c, "gone", PageAction::Refresh);
    // Restored deletion candidates require an explicit fresh scoped decision.
    assert_eq!(
        c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), false),
        Err(JournalError::InvalidTransition)
    );
    c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), true)
        .unwrap();
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&l, 10, "run", "gone"),
        Err(JournalError::InvalidTransition)
    );
    let effect = f.index.join("committed-page");
    let write = effect.clone();
    c.submit(
        op.clone(),
        l.clone(),
        Arc::new(|| 10),
        move |context| async move {
            context.revalidate().map_err(|_| FailureClass::Conflict)?;
            fs::write(write, "effect").map_err(|_| FailureClass::Index)?;
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(effect.exists());
    assert_eq!(c.reconciliation_progress("run").unwrap().changed, 0);
    drop(c);
    let c = IndexCommitCoordinator::open(&f.index, &f.state, binding()).unwrap();
    let l = c.state().acquire_lease(15, 20).unwrap();
    let recovered = c
        .prepared_reconciliation_operation("run", "gone")
        .unwrap()
        .unwrap();
    assert_eq!(recovered.action().unwrap(), ActualOutcome::Refreshed);
    assert_eq!(recovered.content_hash(), Some("hash-gone"));

    c.state().start_or_resume(&l, 15, "run", &scope()).unwrap();
    let changed_source_operation = PageOperation::new(
        op.id(),
        "new-source-edit",
        PageAction::Refresh,
        PageSyncState::present(
            "gone".into(),
            "new-source-hash".into(),
            Some("later-edit".into()),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.prepare_reconciliation_operation(
            &l,
            15,
            "run",
            &changed_source_operation,
            &allowed(),
            true
        ),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        c.submit(op, l.clone(), Arc::new(|| 15), |_| async {
            panic!("replay cannot repeat a committed effect")
        })
        .await
        .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&l, 15, "run", "gone")
            .unwrap(),
        ActualOutcome::Refreshed
    );
    c.acknowledge_reconciliation_receipt(&l, 15, "run", "gone")
        .unwrap();
    c.fail_reconciliation_page(&l, 15, "run", "new", DurableFailure::Source)
        .unwrap();
    c.fail_reconciliation_page(&l, 15, "run", "new", DurableFailure::Source)
        .unwrap();
    c.fail_reconciliation_page(&l, 15, "run", "same", DurableFailure::Index)
        .unwrap();
    c.fail_reconciliation_run(&l, 15, "run", "local-index", DurableFailure::Index)
        .unwrap();
    c.fail_reconciliation_run(&l, 15, "run", "local-index", DurableFailure::Index)
        .unwrap();
    let s = c.reconciliation_progress("run").unwrap();
    assert_eq!(
        (
            s.scanned,
            s.planned_refresh,
            s.planned_delete,
            s.changed,
            s.deleted
        ),
        (4, 1, 1, 1, 0)
    );
    assert_eq!(s.failed_pages(), 4);
    assert_eq!(s.run_failures.len(), 3);
    assert_eq!(s.failed["new"], DurableFailure::Source);
    assert_eq!(
        c.retry_reconciliation_page(&l, 15, "run", "gone"),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&l, 15, "run", "new"),
        Err(JournalError::InvalidTransition)
    );
    c.retry_reconciliation_page(&l, 15, "run", "new").unwrap();
    let op = operation(&c, "new", PageAction::Refresh);
    c.prepare_reconciliation_operation(&l, 15, "run", &op, &allowed(), false)
        .unwrap();
    c.submit(op, l.clone(), Arc::new(|| 15), |_| async { Ok(()) })
        .await
        .unwrap();
    c.acknowledge_reconciliation_receipt(&l, 15, "run", "new")
        .unwrap();
    c.complete_reconciliation_plan(&l, 15, "run").unwrap();
    let expected = c.reconciliation_progress("run").unwrap();
    assert_eq!(
        (expected.changed, expected.deleted, expected.failed.len()),
        (2, 0, 1)
    );
    drop(c);
    let c = f.coordinator();
    assert_eq!(c.reconciliation_progress("run").unwrap(), expected);
}
#[test]
fn connections_reject_stale_fences_conflicting_plans_and_legacy_ack_bypass() {
    let f = Fixture::new();
    let c = f.coordinator();
    let source = plan();
    let old = start(&c, &source);
    let second = SqliteSyncStateStore::open(&f.state).unwrap();
    assert_eq!(second.acquire_lease(11, 10), Err(JournalError::Busy));
    let next = second.acquire_lease(15, 20).unwrap();
    assert_eq!(
        c.fail_reconciliation_page(&old, 15, "run", "new", DurableFailure::Source),
        Err(JournalError::LeaseLost)
    );
    assert_eq!(
        c.record_reconciliation_plan(&next, 15, "run", &source),
        Err(JournalError::InvalidTransition)
    );
    second.start_or_resume(&next, 15, "run", &scope()).unwrap();
    c.record_reconciliation_plan(&next, 15, "run", &source)
        .unwrap();
    let mut changed = source.clone();
    changed.index_version += 1;
    assert_eq!(
        c.record_reconciliation_plan(&next, 15, "run", &changed),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(c.reconciliation_plan("run").unwrap().index_version, 42);
    assert_eq!(
        second.acknowledge(&next, 15, "run", "new", WorkStatus::Applied),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        second.complete(&next, 15, "run", None),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        second.record_failure(&next, 15, "run", FailureClass::Source, None),
        Err(JournalError::InvalidTransition)
    );
    for result in [
        c.retry_reconciliation_page(&old, 15, "run", "new"),
        c.fail_reconciliation_run(&old, 15, "run", "error", DurableFailure::Source),
        c.complete_reconciliation_plan(&old, 15, "run"),
        c.record_reconciliation_plan(&old, 15, "run", &source),
    ] {
        assert_eq!(result, Err(JournalError::LeaseLost));
    }
    let mut wrong = lifecycle();
    wrong.generation = 2;
    let wrong_scope = production_scope(&wrong).unwrap();
    assert_eq!(
        second.start_or_resume(&next, 15, "run", &wrong_scope),
        Err(JournalError::ScopeMismatch)
    );
    let op = operation(&c, "new", PageAction::Refresh);
    let mut wrong_evidence = allowed();
    wrong_evidence.scope = wrong;
    assert_eq!(
        c.prepare_reconciliation_operation(&next, 15, "run", &op, &wrong_evidence, true),
        Err(JournalError::ScopeMismatch)
    );
    assert!(
        IndexCommitCoordinator::open(
            &f.index,
            &f.state,
            CommitBinding::new("chunks", "workspace", &wrong_scope, "2", 2).unwrap()
        )
        .is_err()
    );
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&next, 35, "run", "new"),
        Err(JournalError::LeaseLost)
    );
}
#[test]
fn plan_sealing_and_progress_rollback_leave_no_partial_contract() {
    let f = Fixture::new();
    let c = f.coordinator();
    let l = c.state().acquire_lease(10, 20).unwrap();
    c.state().start_or_resume(&l, 10, "run", &scope()).unwrap();
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch("CREATE TRIGGER deny_plan BEFORE INSERT ON reconciliation_plan_progress WHEN NEW.page_id='new' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.record_reconciliation_plan(&l, 10, "run", &plan()),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        c.state().run("run", &scope()).unwrap().phase,
        RunPhase::Inventory
    );
    assert!(c.reconciliation_plan("run").is_err());
    assert_eq!(
        raw.query_row(
            "SELECT COUNT(*) FROM reconciliation_plan_progress",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    raw.execute_batch("DROP TRIGGER deny_plan").unwrap();
    c.record_reconciliation_plan(&l, 10, "run", &plan())
        .unwrap();
    raw.execute_batch("CREATE TRIGGER deny_progress BEFORE UPDATE ON reconciliation_plan_progress BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.fail_reconciliation_page(&l, 10, "run", "new", DurableFailure::Permission),
        Err(JournalError::Unavailable)
    );
    assert_eq!(c.reconciliation_progress("run").unwrap().pending.len(), 3);
}
#[test]
fn schema_nine_journal_and_page_state_survive_atomic_upgrade() {
    let f = Fixture::new();
    let old = SqliteSyncStateStore::open(&f.state).unwrap();
    let l = old.acquire_lease(1, 20).unwrap();
    old.start_or_resume(&l, 1, "legacy", &scope()).unwrap();
    old.inventory(
        &l,
        1,
        "legacy",
        &[InventoryPage {
            page_id: "existing".into(),
            revision: "v1".into(),
            action: WorkAction::Refresh,
        }],
        None,
    )
    .unwrap();
    old.seal_inventory(&l, 1, "legacy").unwrap();
    old.acknowledge(&l, 1, "legacy", "existing", WorkStatus::Applied)
        .unwrap();
    old.complete(&l, 1, "legacy", None).unwrap();
    old.put_page_state(&PageSyncState::present("existing".into(), "hash".into(), None).unwrap())
        .unwrap();
    drop(old);
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch(include_str!("fixtures/remove_schedule_schema.sql"))
        .unwrap();
    raw.execute_batch("DROP TABLE reconciliation_retry_proposals; DROP TABLE reconciliation_plan_run_failures; DROP TABLE reconciliation_plan_progress; DROP TABLE reconciliation_plans; DELETE FROM schema_migrations WHERE version>=10;").unwrap();
    drop(raw);
    let new = SqliteSyncStateStore::open(&f.state).unwrap();
    assert_eq!(
        new.schema_version().unwrap(),
        notion_knowledge_retrieval::sync_state::LATEST_SCHEMA_VERSION
    );
    assert_eq!(new.run("legacy", &scope()).unwrap().refreshed_count, 1);
    assert_eq!(
        new.page_state("existing").unwrap().unwrap().content_hash(),
        Some("hash")
    );
    drop(new);
    assert_eq!(
        SqliteSyncStateStore::open(&f.state)
            .unwrap()
            .work("legacy")
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn actual_delete_and_unchanged_receipts_count_once_and_failed_effect_requires_explicit_retry()
{
    let f = Fixture::new();
    let c = f.coordinator();
    let source = plan();
    let l = start(&c, &source);
    for (page, action, fresh) in [
        (
            "gone",
            PageAction::Delete,
            evidence(LifecycleStatus::Inactive),
        ),
        ("same", PageAction::Unchanged, allowed()),
    ] {
        let op = operation(&c, page, action);
        c.prepare_reconciliation_operation(&l, 10, "run", &op, &fresh, false)
            .unwrap();
        c.submit(op, l.clone(), Arc::new(|| 10), |_| async { Ok(()) })
            .await
            .unwrap();
        c.acknowledge_reconciliation_receipt(&l, 10, "run", page)
            .unwrap();
        c.acknowledge_reconciliation_receipt(&l, 10, "run", page)
            .unwrap();
    }
    let op = operation(&c, "new", PageAction::Refresh);
    c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), false)
        .unwrap();
    assert_eq!(
        c.submit(op.clone(), l.clone(), Arc::new(|| 10), |_| async {
            Err(FailureClass::Index)
        })
        .await,
        Err(CommitError::Operation(FailureClass::Index))
    );
    assert_eq!(
        c.receipt(op.id()).unwrap().unwrap().status,
        ReceiptStatus::Failed
    );
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&l, 10, "run", "new"),
        Err(JournalError::InvalidTransition)
    );
    c.fail_reconciliation_page(&l, 10, "run", "new", DurableFailure::Index)
        .unwrap();
    let snapshot = c.reconciliation_progress("run").unwrap();
    drop(c);
    let c = f.coordinator();
    assert_eq!(c.reconciliation_progress("run").unwrap(), snapshot);
    assert_eq!(
        c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), false),
        Err(JournalError::InvalidTransition)
    );
    c.retry_reconciliation_page(&l, 10, "run", "new").unwrap();
    c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), false)
        .unwrap();
    c.submit(op.clone(), l.clone(), Arc::new(|| 10), |_| async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(c.receipt(op.id()).unwrap().unwrap().attempts, 2);
    c.acknowledge_reconciliation_receipt(&l, 10, "run", "new")
        .unwrap();
    let snapshot = c.reconciliation_progress("run").unwrap();
    assert_eq!(
        (snapshot.changed, snapshot.deleted, snapshot.unchanged),
        (1, 1, 1)
    );
    assert!(snapshot.failed.is_empty());
    drop(c);
    assert_eq!(
        f.coordinator().reconciliation_progress("run").unwrap(),
        snapshot
    );
}

#[tokio::test]
async fn an_applied_receipt_cannot_acknowledge_a_different_prepared_effect() {
    let f = Fixture::new();
    let c = f.coordinator();
    let l = start(&c, &plan());
    let op = operation(&c, "new", PageAction::Refresh);
    c.prepare_reconciliation_operation(&l, 10, "run", &op, &allowed(), false)
        .unwrap();
    let mismatch = PageOperation::new(
        op.id(),
        "different-revision",
        PageAction::Refresh,
        PageSyncState::present("new".into(), "different-hash".into(), None).unwrap(),
    )
    .unwrap();
    c.submit(mismatch.clone(), l.clone(), Arc::new(|| 10), |_| async {
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&l, 10, "run", "new"),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        c.prepare_reconciliation_operation(&l, 10, "run", &mismatch, &allowed(), true),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(c.reconciliation_progress("run").unwrap().changed, 0);
    c.fail_reconciliation_page(&l, 10, "run", "new", DurableFailure::Conflict)
        .unwrap();
    assert_eq!(
        c.reconciliation_progress("run").unwrap().failed["new"],
        DurableFailure::Conflict
    );
}

#[test]
fn migration_failure_rolls_back_the_whole_upgrade() {
    let f = Fixture::new();
    drop(SqliteSyncStateStore::open(&f.state).unwrap());
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch(include_str!("fixtures/remove_schedule_schema.sql"))
        .unwrap();
    raw.execute_batch("DROP TABLE reconciliation_retry_proposals; DROP TABLE reconciliation_plan_run_failures; DROP TABLE reconciliation_plan_progress; DROP TABLE reconciliation_plans; DELETE FROM schema_migrations WHERE version>=10; CREATE TABLE reconciliation_plan_progress(incompatible TEXT);").unwrap();
    assert!(SqliteSyncStateStore::open(&f.state).is_err());
    assert_eq!(
        raw.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        9
    );
    assert_eq!(
        raw.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='reconciliation_plans'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn explicit_retry_proposal_and_pending_transition_survive_reopen_and_rollback_atomically() {
    let f = Fixture::new();
    let c = f.coordinator();
    let source = plan();
    let lease = start(&c, &source);
    c.fail_reconciliation_page(&lease, 10, "run", "new", DurableFailure::Conflict)
        .unwrap();
    let mut proposal = c.reconciliation_plan("run").unwrap().pages["new"].clone();
    proposal.metadata_identity = Some("fresh-proposal".into());
    let sql = Connection::open(&f.state).unwrap();
    sql.execute_batch("CREATE TRIGGER deny_retry BEFORE INSERT ON reconciliation_retry_proposals BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.retry_reconciliation_proposal(&lease, 10, "run", &proposal),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        c.reconciliation_progress("run").unwrap().failed["new"],
        DurableFailure::Conflict
    );
    assert!(!c.reconciliation_page_proposal("run", "new").unwrap().1);
    sql.execute_batch("DROP TRIGGER deny_retry;").unwrap();
    c.retry_reconciliation_proposal(&lease, 10, "run", &proposal)
        .unwrap();
    let reopened = IndexCommitCoordinator::open(&f.index, &f.state, binding()).unwrap();
    assert_eq!(
        reopened.reconciliation_page_proposal("run", "new").unwrap(),
        (proposal, true)
    );
    assert!(
        reopened
            .reconciliation_progress("run")
            .unwrap()
            .pending
            .contains("new")
    );
}

#[test]
fn schema_ten_plan_retains_state_and_failed_retry_migration_rolls_back() {
    let f = Fixture::new();
    let c = f.coordinator();
    let source = plan();
    let lease = start(&c, &source);
    c.fail_reconciliation_page(&lease, 10, "run", "new", DurableFailure::Index)
        .unwrap();
    drop(c);
    let sql = Connection::open(&f.state).unwrap();
    sql.execute_batch(include_str!("fixtures/remove_schedule_schema.sql"))
        .unwrap();
    sql.execute_batch("DROP TABLE reconciliation_retry_proposals; DELETE FROM schema_migrations WHERE version=11; CREATE TABLE reconciliation_retry_proposals(incompatible TEXT);").unwrap();
    assert!(SqliteSyncStateStore::open(&f.state).is_err());
    assert_eq!(
        sql.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        10
    );
    assert_eq!(
        sql.query_row("SELECT COUNT(*) FROM reconciliation_plans", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    sql.execute_batch("DROP TABLE reconciliation_retry_proposals;")
        .unwrap();
    drop(sql);
    let reopened = IndexCommitCoordinator::open(&f.index, &f.state, binding()).unwrap();
    assert_eq!(
        reopened.state().schema_version().unwrap(),
        notion_knowledge_retrieval::sync_state::LATEST_SCHEMA_VERSION
    );
    assert_eq!(
        reopened.reconciliation_progress("run").unwrap().failed["new"],
        DurableFailure::Index
    );
}

#[test]
fn authoritative_completion_advances_schedule_atomically_and_holds_planning_errors() {
    use notion_knowledge_retrieval::schedule::{SchedulePolicy, ScheduleStatus};
    let f = Fixture::new();
    let c = f.coordinator();
    let l = c.state().acquire_lease(10, 100).unwrap();
    c.register_schedule(
        &l,
        10,
        "periodic",
        SchedulePolicy::new(60, 3, 5, 10).unwrap(),
        10,
    )
    .unwrap();
    c.claim_schedule(&l, 10, "periodic", "clean").unwrap();
    let mut source = plan();
    source.pages.clear();
    source.errors.clear();
    source.run_errors.clear();
    c.record_reconciliation_plan(&l, 10, "clean", &source)
        .unwrap();
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch("CREATE TRIGGER fail_deadline BEFORE UPDATE ON reconciliation_schedules BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.complete_reconciliation_plan(&l, 10, "clean"),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        c.state().run("clean", &scope()).unwrap().phase,
        RunPhase::Applying
    );
    assert_eq!(c.schedule("periodic").unwrap().next_deadline, 10);
    raw.execute_batch("DROP TRIGGER fail_deadline;").unwrap();
    c.complete_reconciliation_plan(&l, 10, "clean").unwrap();
    assert_eq!(c.schedule("periodic").unwrap().next_deadline, 70);
    assert_eq!(
        c.state().run("clean", &scope()).unwrap().next_deadline,
        Some(70)
    );
    c.claim_schedule(&l, 70, "periodic", "failed-plan").unwrap();
    source
        .errors
        .insert("inaccessible".into(), PlanError::Permission);
    c.record_reconciliation_plan(&l, 70, "failed-plan", &source)
        .unwrap();
    c.complete_reconciliation_plan(&l, 70, "failed-plan")
        .unwrap();
    assert_eq!(
        c.schedule("periodic").unwrap().status,
        ScheduleStatus::HeldPages
    );
    assert!(c.due_schedules(100, 100).unwrap().is_empty());
    assert_eq!(
        c.retry_schedule(&l, 70, "periodic", 70),
        Err(JournalError::InvalidTransition)
    );
    c.abandon_schedule(&l, 70, "periodic", 70).unwrap();
    assert_eq!(
        c.state().run("failed-plan", &scope()).unwrap().phase,
        RunPhase::Completed
    );
    assert_eq!(
        c.reconciliation_plan("failed-plan")
            .unwrap()
            .planning_failures
            .len(),
        1
    );
    let l = c.state().acquire_lease(70, 100).unwrap();
    c.claim_schedule(&l, 70, "periodic", "fresh-cycle").unwrap();
}
