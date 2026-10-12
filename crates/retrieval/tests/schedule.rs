#![cfg(unix)]
use notion_knowledge_core::{reconciliation::*, sync_state::*};
use notion_knowledge_retrieval::{commit::*, schedule::*, sync_state::*};
use rusqlite::Connection;
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    dir: PathBuf,
    index: PathBuf,
    state: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "nk-schedule-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = dir.join("index");
        fs::create_dir_all(&index).unwrap();
        Self {
            state: dir.join("state.sqlite"),
            dir,
            index,
        }
    }
    fn open(&self) -> Arc<IndexCommitCoordinator> {
        IndexCommitCoordinator::initialize(&self.index, &self.state, binding("v1", 1)).unwrap()
    }
    fn raw(&self) -> Connection {
        Connection::open(&self.state).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
fn scope(generation: &str) -> ReconciliationScope {
    ReconciliationScope::new(
        vec!["root-a".into(), "root-b".into()],
        vec!["page:excluded".into(), "descendant:hidden".into()],
        "webhook-v1",
        generation,
    )
    .unwrap()
}
fn binding(g: &str, e: u64) -> CommitBinding {
    CommitBinding::new("chunks", "workspace", &scope(g), g, e).unwrap()
}
fn policy() -> SchedulePolicy {
    SchedulePolicy::new(60, 3, 5, 8).unwrap()
}
fn register(c: &IndexCommitCoordinator, l: &Lease) {
    c.register_schedule(l, 10, "periodic", policy(), 20)
        .unwrap();
}
fn complete(c: &IndexCommitCoordinator, l: &Lease, now: i64, run: &str) {
    c.state().seal_inventory(l, now, run).unwrap();
    c.state().complete(l, now, run, None).unwrap();
}
#[test]
fn registration_replay_preserves_deadline_and_rejects_configuration_drift() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    assert_eq!(
        c.register_schedule(&l, 11, "periodic", policy(), 999)
            .unwrap()
            .next_deadline,
        20
    );
    let changed = SchedulePolicy::new(120, 3, 5, 8).unwrap();
    assert_eq!(
        c.register_schedule(&l, 11, "periodic", changed, 20),
        Err(JournalError::ScopeMismatch)
    );
    for args in [
        (0, 3, 5, 8),
        (2_592_001, 3, 5, 8),
        (60, 0, 5, 8),
        (60, 17, 5, 8),
        (60, 3, 0, 8),
        (60, 3, 5, 4),
        (60, 3, 5, 604_801),
    ] {
        assert_eq!(
            SchedulePolicy::new(args.0, args.1, args.2, args.3),
            Err(JournalError::InvalidInput)
        );
    }
    assert_eq!(
        c.register_schedule(&l, 11, "invalid", policy(), i64::MAX),
        Err(JournalError::InvalidInput)
    );
    assert_eq!(
        c.register_schedule(&l, 11, "", policy(), 20),
        Err(JournalError::InvalidInput)
    );
    assert_eq!(c.due_schedules(20, 0), Err(JournalError::InvalidInput));
    assert_eq!(c.due_schedules(19, 100).unwrap(), vec![]);
    assert_eq!(c.due_schedules(20, 100).unwrap().len(), 1);
    drop(c);
    let c = f.open();
    assert_eq!(c.schedule("periodic").unwrap().next_deadline, 20);
}
#[test]
fn due_claim_reopens_the_same_run_and_inventory_under_new_fence() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 11).unwrap();
    register(&c, &l);
    assert_eq!(
        c.claim_schedule(&l, 19, "periodic", "run-1"),
        Err(JournalError::InvalidTransition)
    );
    let s = c.claim_schedule(&l, 20, "periodic", "run-1").unwrap();
    assert_eq!(s.active_run.as_deref(), Some("run-1"));
    c.state()
        .inventory(
            &l,
            20,
            "run-1",
            &[InventoryPage {
                page_id: "page".into(),
                revision: "r1".into(),
                action: WorkAction::Refresh,
            }],
            Some("cursor"),
        )
        .unwrap();
    let before = c.schedule("periodic").unwrap();
    c.checkpoint_schedule(&l, 20, "periodic").unwrap();
    assert_eq!(before, c.schedule("periodic").unwrap());
    drop(c);
    let c = f.open();
    let new = c.state().acquire_lease(21, 20).unwrap();
    assert_eq!(
        c.claim_schedule(&l, 21, "periodic", "duplicate"),
        Err(JournalError::LeaseLost)
    );
    let s = c.claim_schedule(&new, 21, "periodic", "duplicate").unwrap();
    assert_eq!(s.active_run.as_deref(), Some("run-1"));
    assert_eq!(s.cycle, 1);
    assert_eq!(s.attempts, 0);
    assert_eq!(
        c.state()
            .run("run-1", &scope("v1"))
            .unwrap()
            .checkpoint
            .as_deref(),
        Some("cursor")
    );
    assert_eq!(c.state().work("run-1").unwrap().len(), 1);
    assert_eq!(
        f.raw()
            .query_row("SELECT COUNT(*) FROM reconciliation_runs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}
#[test]
fn manual_and_external_writer_contention_never_consumes_schedule_budgets() {
    let f = Fixture::new();
    let one = f.open();
    let two = f.open();
    let l = one.state().acquire_lease(10, 100).unwrap();
    register(&one, &l);
    assert_eq!(two.state().acquire_lease(20, 10), Err(JournalError::Busy));
    one.state()
        .start_or_resume(&l, 20, "manual", &scope("v1"))
        .unwrap();
    let before = one.schedule("periodic").unwrap();
    assert_eq!(
        two.claim_schedule(&l, 20, "periodic", "run"),
        Err(JournalError::Busy)
    );
    assert_eq!(before, one.schedule("periodic").unwrap());
    complete(&one, &l, 20, "manual");
    let guard = fs::File::open(&f.index).unwrap();
    guard.lock().unwrap();
    assert_eq!(
        two.claim_schedule(&l, 20, "periodic", "run"),
        Err(JournalError::Busy)
    );
    assert_eq!(before, one.schedule("periodic").unwrap());
    drop(guard);
    two.claim_schedule(&l, 20, "periodic", "run").unwrap();
    assert_eq!(
        one.state()
            .start_or_resume(&l, 20, "manual-next", &scope("v1")),
        Err(JournalError::Busy)
    );
}
#[test]
fn completion_and_next_deadline_commit_together_and_completed_receipts_are_immutable() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    c.state().seal_inventory(&l, 20, "run").unwrap();
    f.raw().execute_batch("CREATE TRIGGER fail_schedule BEFORE UPDATE OF next_deadline ON reconciliation_schedules BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.state().complete(&l, 20, "run", None),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        c.state().run("run", &scope("v1")).unwrap().phase,
        RunPhase::Applying
    );
    assert_eq!(
        c.schedule("periodic").unwrap().active_run.as_deref(),
        Some("run")
    );
    f.raw()
        .execute_batch("DROP TRIGGER fail_schedule;")
        .unwrap();
    c.state().complete(&l, 20, "run", None).unwrap();
    let s = c.schedule("periodic").unwrap();
    assert_eq!(s.next_deadline, 80);
    assert_eq!(s.active_run, None);
    assert_eq!(
        c.state().run("run", &scope("v1")).unwrap().next_deadline,
        Some(80)
    );
    let raw = f.raw();
    let old: (i64, i64, Option<i64>) = raw
        .query_row(
            "SELECT phase,fence,next_deadline FROM reconciliation_runs WHERE run_id='run'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    c.state().release_lease(&l, 21).unwrap();
    let next = c.state().acquire_lease(22, 100).unwrap();
    c.state()
        .start_or_resume(&next, 22, "run", &scope("v1"))
        .unwrap();
    assert_eq!(
        old,
        raw.query_row(
            "SELECT phase,fence,next_deadline FROM reconciliation_runs WHERE run_id='run'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        )
        .unwrap()
    );
    assert_eq!(
        c.claim_schedule(&next, 80, "periodic", "run"),
        Err(JournalError::InvalidTransition)
    );
    c.claim_schedule(&next, 80, "periodic", "run-2").unwrap();
    assert_eq!(c.schedule("periodic").unwrap().cycle, 2);
}
#[test]
fn bounded_backoff_exhaustion_and_explicit_retry_preserve_failure_evidence() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    let first = c
        .fail_schedule(&l, 20, "periodic", "attempt-1", FailureClass::Source)
        .unwrap();
    assert_eq!(
        (first.status, first.attempts, first.next_deadline),
        (ScheduleStatus::RetryWait, 1, 25)
    );
    assert_eq!(
        first,
        c.fail_schedule(&l, 21, "periodic", "attempt-1", FailureClass::Source)
            .unwrap()
    );
    assert_eq!(
        c.fail_schedule(&l, 21, "periodic", "attempt-1", FailureClass::Index),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        c.fail_schedule(&l, 21, "periodic", "attempt-2", FailureClass::Source),
        Err(JournalError::InvalidTransition)
    );
    assert_eq!(
        c.claim_schedule(&l, 24, "periodic", "duplicate"),
        Err(JournalError::InvalidTransition)
    );
    c.claim_schedule(&l, 25, "periodic", "duplicate").unwrap();
    let s = c
        .fail_schedule(&l, 25, "periodic", "attempt-2", FailureClass::Source)
        .unwrap();
    assert_eq!((s.attempts, s.next_deadline), (2, 33));
    drop(c);
    let c = f.open();
    c.claim_schedule(&l, 33, "periodic", "duplicate").unwrap();
    let s = c
        .fail_schedule(&l, 33, "periodic", "attempt-3", FailureClass::Source)
        .unwrap();
    assert_eq!(
        (s.status, s.attempts, s.next_deadline),
        (ScheduleStatus::Exhausted, 3, 41)
    );
    assert!(c.due_schedules(50, 100).unwrap().is_empty());
    assert_eq!(
        c.claim_schedule(&l, 50, "periodic", "duplicate"),
        Err(JournalError::InvalidTransition)
    );
    c.retry_schedule(&l, 50, "periodic", 50).unwrap();
    c.claim_schedule(&l, 50, "periodic", "duplicate").unwrap();
    complete(&c, &l, 50, "run");
    let events = c.schedule_events("periodic", 0, 100).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == ScheduleEventKind::RunFailure)
            .count(),
        3
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == ScheduleEventKind::Completed)
            .count(),
        1
    );
    assert_eq!(c.schedule("periodic").unwrap().attempts, 0);
    assert_eq!(
        c.schedule_events("periodic", events[0].sequence, 1)
            .unwrap()[0],
        events[1]
    );
}
#[test]
fn held_failed_pages_need_explicit_page_retry_or_abandonment_and_history_survives_success() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    c.state()
        .inventory(
            &l,
            20,
            "run",
            &[InventoryPage {
                page_id: "page".into(),
                revision: "r1".into(),
                action: WorkAction::Refresh,
            }],
            None,
        )
        .unwrap();
    c.state().seal_inventory(&l, 20, "run").unwrap();
    c.state()
        .acknowledge(&l, 20, "run", "page", WorkStatus::Failed)
        .unwrap();
    let held = c.hold_schedule(&l, 20, "periodic").unwrap();
    assert_eq!(held.status, ScheduleStatus::HeldPages);
    assert_eq!(held.attempts, 0);
    assert!(c.due_schedules(30, 100).unwrap().is_empty());
    assert_eq!(
        c.retry_schedule(&l, 30, "periodic", 30),
        Err(JournalError::InvalidTransition)
    );
    let evidence = c.reconciliation_failure_evidence("run", 0, 100).unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].class, "1");
    c.state().retry_failed(&l, 30, "run", "page").unwrap();
    c.retry_schedule(&l, 30, "periodic", 30).unwrap();
    c.state()
        .acknowledge(&l, 30, "run", "page", WorkStatus::Applied)
        .unwrap();
    c.state().complete(&l, 30, "run", None).unwrap();
    assert_eq!(
        evidence,
        c.reconciliation_failure_evidence("run", 0, 100).unwrap()
    );
    c.claim_schedule(&l, 90, "periodic", "second").unwrap();
    c.abandon_schedule(&l, 90, "periodic", 90).unwrap();
    assert_eq!(
        c.state().run("second", &scope("v1")).unwrap().phase,
        RunPhase::Abandoned
    );
    assert_eq!(
        c.claim_schedule(&l, 90, "periodic", "third"),
        Err(JournalError::LeaseLost)
    );
    let l = c.state().acquire_lease(90, 100).unwrap();
    assert_eq!(
        c.state().start_or_resume(&l, 90, "second", &scope("v1")),
        Err(JournalError::InvalidTransition)
    );
    c.claim_schedule(&l, 90, "periodic", "third").unwrap();
    assert_eq!(c.schedule("periodic").unwrap().cycle, 3);
    assert_eq!(
        c.state().run("run", &scope("v1")).unwrap().phase,
        RunPhase::Completed
    );
}
#[test]
fn configuration_recovery_is_explicit_and_rejects_active_or_wrong_previous_authority() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    assert_eq!(
        c.reconfigure_schedule(
            &l,
            20,
            "periodic",
            &ScheduleReconfiguration {
                previous_binding: binding("v1", 1),
                policy: policy(),
                next_deadline: 100
            }
        ),
        Err(JournalError::Busy)
    );
    c.abandon_schedule(&l, 20, "periodic", 20).unwrap();
    let l = c.state().acquire_lease(21, 100).unwrap();
    let changed = c.change_scope(binding("v2", 2), &l, 21).unwrap();
    assert_eq!(
        changed.schedule("periodic"),
        Err(JournalError::ScopeMismatch)
    );
    assert_eq!(
        changed.register_schedule(&l, 21, "periodic", policy(), 100),
        Err(JournalError::ScopeMismatch)
    );
    assert!(changed.due_schedules(100, 100).unwrap().is_empty());
    assert_eq!(
        changed.reconfigure_schedule(
            &l,
            21,
            "periodic",
            &ScheduleReconfiguration {
                previous_binding: binding("v1", 99),
                policy: policy(),
                next_deadline: 100
            }
        ),
        Err(JournalError::ScopeMismatch)
    );
    let s = changed
        .reconfigure_schedule(
            &l,
            21,
            "periodic",
            &ScheduleReconfiguration {
                previous_binding: binding("v1", 1),
                policy: policy(),
                next_deadline: 100,
            },
        )
        .unwrap();
    assert_eq!(s.next_deadline, 100);
    assert_eq!(s.cycle, 1);
    assert_eq!(
        changed.claim_schedule(&l, 99, "periodic", "new"),
        Err(JournalError::InvalidTransition)
    );
    changed.claim_schedule(&l, 100, "periodic", "new").unwrap();
    assert_eq!(c.schedule("periodic"), Err(JournalError::ScopeMismatch));
}
#[test]
fn claim_and_abandon_roll_back_every_related_row_on_injected_write_failure() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    f.raw().execute_batch("CREATE TRIGGER fail_schedule BEFORE UPDATE ON reconciliation_schedules BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.claim_schedule(&l, 20, "periodic", "run"),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        f.raw()
            .query_row("SELECT COUNT(*) FROM reconciliation_runs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    f.raw()
        .execute_batch("DROP TRIGGER fail_schedule;")
        .unwrap();
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    let events = c.schedule_events("periodic", 0, 100).unwrap();
    f.raw().execute_batch("CREATE TRIGGER fail_schedule BEFORE UPDATE ON reconciliation_schedules BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert_eq!(
        c.abandon_schedule(&l, 20, "periodic", 30),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        c.state().run("run", &scope("v1")).unwrap().phase,
        RunPhase::Inventory
    );
    assert_eq!(events, c.schedule_events("periodic", 0, 100).unwrap());
}
#[test]
fn schema_eleven_upgrade_is_atomic_and_read_only_current_schema_inspection_creates_nothing() {
    let f = Fixture::new();
    let raw = f.raw();
    let migrations = [
        (
            1,
            "initial_sync_state",
            include_str!("../migrations/0001_initial.sql"),
        ),
        (
            2,
            "reconciliation_journal",
            include_str!("../migrations/0002_reconciliation.sql"),
        ),
        (
            3,
            "webhook_inbox",
            include_str!("../migrations/0003_webhook_inbox.sql"),
        ),
        (
            4,
            "webhook_recovery",
            include_str!("../migrations/0004_webhook_recovery.sql"),
        ),
        (
            5,
            "webhook_debounce",
            include_str!("../migrations/0005_webhook_debounce.sql"),
        ),
        (
            6,
            "index_commit_coordinator",
            include_str!("../migrations/0006_index_commit.sql"),
        ),
        (
            7,
            "knowledge_graph_edges",
            include_str!("../migrations/0007_graph_edges.sql"),
        ),
        (
            8,
            "knowledge_graph_aliases",
            include_str!("../migrations/0008_graph_aliases.sql"),
        ),
        (
            9,
            "page_metadata_identity",
            include_str!("../migrations/0009_page_metadata_identity.sql"),
        ),
        (
            10,
            "authoritative_reconciliation_plan",
            include_str!("../migrations/0010_reconciliation_plan.sql"),
        ),
        (
            11,
            "reconciliation_retry",
            include_str!("../migrations/0011_reconciliation_retry.sql"),
        ),
    ];
    raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL)").unwrap();
    for (v, n, sql) in migrations {
        raw.execute_batch(sql).unwrap();
        raw.execute(
            "INSERT INTO schema_migrations VALUES(?1,?2,0)",
            rusqlite::params![v, n],
        )
        .unwrap();
    }
    raw.execute_batch("INSERT INTO reconciliation_runs VALUES('old','scope',1,1,'cursor',100,0);INSERT INTO reconciliation_inventory VALUES('old','page','rev',0,2,1);INSERT INTO page_sync_state VALUES('page','hash',NULL,0,0);").unwrap();
    assert!(SqliteSyncStateStore::open_read_only(&f.state).is_err());
    raw.execute_batch("CREATE TABLE reconciliation_schedules(injected INTEGER)")
        .unwrap();
    drop(raw);
    assert!(SqliteSyncStateStore::open(&f.state).is_err());
    let raw = f.raw();
    assert_eq!(
        raw.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        11
    );
    assert!(
        raw.prepare("SELECT abandoned FROM reconciliation_runs")
            .is_err()
    );
    raw.execute_batch("DROP TABLE reconciliation_schedules")
        .unwrap();
    drop(raw);
    let upgraded = SqliteSyncStateStore::open(&f.state).unwrap();
    assert_eq!(upgraded.schema_version().unwrap(), 12);
    assert_eq!(
        upgraded.page_state("page").unwrap().unwrap().content_hash(),
        Some("hash")
    );
    drop(upgraded);
    let raw = f.raw();
    assert_eq!(
        raw.query_row(
            "SELECT COUNT(*) FROM reconciliation_failure_history",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    drop(raw);
    let before = fs::read(&f.state).unwrap();
    let names = fs::read_dir(&f.dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect::<Vec<_>>();
    let ro = SqliteSyncStateStore::open_read_only(&f.state).unwrap();
    assert_eq!(ro.schema_version().unwrap(), 12);
    assert!(ro.acquire_lease(100, 10).is_err());
    drop(ro);
    assert_eq!(before, fs::read(&f.state).unwrap());
    assert_eq!(
        names,
        fs::read_dir(&f.dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn abandonment_preserves_real_applied_receipts_and_refuses_unresolved_submission() {
    let f = Fixture::new();
    let c = f.open();
    let l = c.state().acquire_lease(10, 100).unwrap();
    register(&c, &l);
    c.claim_schedule(&l, 20, "periodic", "run").unwrap();
    let operation = PageOperation::new(
        "successful",
        "revision",
        PageAction::Refresh,
        PageSyncState::present("page".into(), "hash".into(), Some("edited".into())).unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.submit(operation, l.clone(), Arc::new(|| 20), |ctx| async move {
            ctx.revalidate().unwrap();
            Ok(())
        })
        .await
        .unwrap(),
        CommitOutcome::Applied
    );
    let receipt = c.receipt("successful").unwrap().unwrap();
    assert_eq!(receipt.status, ReceiptStatus::Applied);
    let raw = f.raw();
    raw.execute_batch("INSERT INTO reconciliation_plans VALUES('run',1,'{}');INSERT INTO reconciliation_plan_progress(run_id,page_id,operation_id) VALUES('run','pending-page','pending-operation');INSERT INTO index_commit_receipts VALUES('pending-operation','v1','pending-page','revision',0,'hash',NULL,0,NULL,1);").unwrap();
    let before = c.schedule("periodic").unwrap();
    assert_eq!(
        c.abandon_schedule(&l, 20, "periodic", 30),
        Err(JournalError::Busy)
    );
    assert_eq!(before, c.schedule("periodic").unwrap());
    // Model the durable receipt returned by recovering the already submitted work.
    raw.execute(
        "UPDATE index_commit_receipts SET status=1 WHERE operation_id='pending-operation'",
        [],
    )
    .unwrap();
    let recovered = c.receipt("pending-operation").unwrap().unwrap();
    c.abandon_schedule(&l, 20, "periodic", 30).unwrap();
    assert_eq!(c.receipt("successful").unwrap().unwrap(), receipt);
    assert_eq!(c.receipt("pending-operation").unwrap().unwrap(), recovered);
    assert_eq!(
        c.state().renew_lease(&l, 20, 100),
        Err(JournalError::LeaseLost)
    );
    assert_eq!(
        c.state().run("run", &scope("v1")).unwrap().phase,
        RunPhase::Abandoned
    );
    let next = c.state().acquire_lease(20, 100).unwrap();
    assert!(next.fence > l.fence);
    c.claim_schedule(&next, 30, "periodic", "fresh").unwrap();
}
