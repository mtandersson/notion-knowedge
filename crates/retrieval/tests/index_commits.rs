#![cfg(unix)]
use notion_knowledge_core::{reconciliation::*, sync_state::*};
use notion_knowledge_retrieval::{commit::*, sync_state::LATEST_SCHEMA_VERSION};
use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
            "nk-commit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let index = root.join("index");
        fs::create_dir_all(&index).unwrap();
        let state = root.join("state.sqlite");
        Self { root, index, state }
    }
    fn coordinator(&self) -> Arc<IndexCommitCoordinator> {
        IndexCommitCoordinator::initialize(&self.index, &self.state, binding("v1", 1)).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn scope(generation: &str) -> ReconciliationScope {
    ReconciliationScope::new(vec!["root".into()], vec![], "policy", generation).unwrap()
}
fn binding(generation: &str, epoch: u64) -> CommitBinding {
    CommitBinding::new("chunks", "workspace", &scope(generation), generation, epoch).unwrap()
}
fn operation(id: &str) -> PageOperation {
    PageOperation::new(
        id,
        "revision-v1",
        PageAction::Refresh,
        PageSyncState::present("page".into(), "hash-v1".into(), Some("edited-v1".into())).unwrap(),
    )
    .unwrap()
}
fn clock(now: i64) -> Arc<dyn Fn() -> i64 + Send + Sync> {
    Arc::new(move || now)
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn wait_for(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "condition timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn wait_child(child: &mut Child) {
    wait_for(|| child.try_wait().unwrap().is_some());
    assert!(child.wait().unwrap().success());
}

#[tokio::test]
async fn binding_survives_migration_and_aliases_but_rejects_other_state_table_workspace_and_generation()
 {
    let f = Fixture::new();
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch(include_str!("../migrations/0001_initial.sql"))
        .unwrap();
    raw.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at_unix INTEGER NOT NULL);INSERT INTO schema_migrations VALUES(1,'initial_sync_state',0);INSERT INTO page_sync_state VALUES('old','old-hash',NULL,0,0);").unwrap();
    drop(raw);
    let c = f.coordinator();
    assert_eq!(c.state().schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(
        c.state().page_state("old").unwrap().unwrap().content_hash(),
        Some("old-hash")
    );
    let other_index = f.root.join("other-index");
    fs::create_dir(&other_index).unwrap();
    assert!(matches!(
        IndexCommitCoordinator::initialize(&other_index, &f.state, binding("v1", 1)),
        Err(CommitError::BindingMismatch)
    ));
    assert!(!other_index.join(".nk-operational-binding").exists());
    IndexCommitCoordinator::initialize(
        &other_index,
        f.root.join("independent.sqlite"),
        binding("v1", 1),
    )
    .unwrap();
    let alias = f.root.join("alias");
    std::os::unix::fs::symlink(&f.index, &alias).unwrap();
    let dbalias = f.root.join("dbalias");
    std::os::unix::fs::symlink(&f.state, &dbalias).unwrap();
    drop(c);
    let reopened =
        IndexCommitCoordinator::open(alias.join("."), dbalias, binding("v1", 1)).unwrap();
    assert!(matches!(
        IndexCommitCoordinator::initialize(&f.index, f.root.join("other.sqlite"), binding("v1", 1)),
        Err(CommitError::BindingMismatch)
    ));
    for wrong in [
        CommitBinding::new("other", "workspace", &scope("v1"), "v1", 1).unwrap(),
        CommitBinding::new("chunks", "other", &scope("v1"), "v1", 1).unwrap(),
        binding("v2", 2),
    ] {
        assert!(matches!(
            IndexCommitCoordinator::open(&f.index, &f.state, wrong),
            Err(CommitError::BindingMismatch)
        ));
    }
    let hard = f.root.join("hard.sqlite");
    fs::hard_link(&f.state, &hard).unwrap();
    assert!(matches!(
        IndexCommitCoordinator::open(&f.index, &hard, binding("v1", 1)),
        Err(CommitError::BindingMismatch)
    ));
    fs::remove_file(hard).unwrap();
    let lease = reopened.state().acquire_lease(10, 100).unwrap();
    assert_eq!(
        reopened
            .submit(operation("first"), lease, clock(10), |ctx| async move {
                ctx.revalidate().unwrap();
                Ok(())
            })
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
}

#[tokio::test]
async fn explicit_scope_changes_reject_stale_and_regressing_authority_before_effects() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(10, 100).unwrap();
    assert!(matches!(
        c.change_scope(binding("v2", 1), &lease, 10),
        Err(CommitError::BindingMismatch)
    ));
    let next = c.change_scope(binding("v2", 2), &lease, 10).unwrap();
    assert_eq!(
        c.submit(operation("stale"), lease.clone(), clock(10), |_| async {
            panic!("stale binding ran")
        })
        .await,
        Err(CommitError::BindingMismatch)
    );
    assert!(matches!(
        next.change_scope(binding("v1", 3), &lease, 10),
        Err(CommitError::Conflict)
    ));
    assert_eq!(
        next.submit(operation("current"), lease, clock(10), |_| async { Ok(()) })
            .await,
        Ok(CommitOutcome::Applied)
    );
}

#[tokio::test]
async fn current_journal_fence_scope_and_expiry_are_checked_without_holding_callback_mutex() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(10, 1).unwrap();
    assert_eq!(
        c.submit(operation("expired"), lease.clone(), clock(11), |_| async {
            panic!("expired ran")
        })
        .await,
        Err(CommitError::LeaseLost)
    );
    let next = c.state().acquire_lease(11, 100).unwrap();
    assert_eq!(
        c.submit(operation("lost"), lease, clock(11), |_| async {
            panic!("lost ran")
        })
        .await,
        Err(CommitError::LeaseLost)
    );
    c.state()
        .start_or_resume(&next, 11, "run", &scope("other"))
        .unwrap();
    assert_eq!(
        c.submit(operation("wrong-scope"), next, clock(11), |_| async {
            panic!("wrong scope ran")
        })
        .await,
        Err(CommitError::Conflict)
    );
    // A separate database verifies the matching active-run fence and callback SQL.
    let g = Fixture::new();
    let d = g.coordinator();
    let lease = d.state().acquire_lease(10, 100).unwrap();
    d.state()
        .start_or_resume(&lease, 10, "run", &scope("v1"))
        .unwrap();
    assert_eq!(
        d.submit(operation("valid"), lease, clock(10), |ctx| async move {
            ctx.revalidate().unwrap();
            assert!(ctx.state().page_state("page").unwrap().is_none());
            Ok(())
        })
        .await,
        Ok(CommitOutcome::Applied)
    );
}

#[tokio::test]
async fn successful_effect_precedes_atomic_checkpoint_and_failed_checkpoint_replays_after_reopen() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(10, 100).unwrap();
    let raw = Connection::open(&f.state).unwrap();
    raw.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'private injected payload');END;").unwrap();
    let effect = f.index.join("page-effect");
    let path = effect.clone();
    assert_eq!(
        c.submit(
            operation("replay"),
            lease.clone(),
            clock(10),
            move |ctx| async move {
                assert!(ctx.state().page_state("page").unwrap().is_none());
                fs::write(path, "revision-v1").unwrap();
                Ok(())
            }
        )
        .await,
        Err(CommitError::Unavailable)
    );
    assert_eq!(fs::read_to_string(&effect).unwrap(), "revision-v1");
    assert!(c.state().page_state("page").unwrap().is_none());
    assert_eq!(
        c.receipt("replay").unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    raw.execute_batch("DROP TRIGGER fail_checkpoint;").unwrap();
    drop(raw);
    drop(c);
    let c = IndexCommitCoordinator::open(&f.index, &f.state, binding("v1", 1)).unwrap();
    let path = effect.clone();
    assert_eq!(
        c.submit(
            operation("replay"),
            lease.clone(),
            clock(10),
            move |_| async move {
                fs::write(path, "revision-v1").unwrap();
                Ok(())
            }
        )
        .await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(
        c.receipt("replay").unwrap(),
        Some(OperationReceipt {
            status: ReceiptStatus::Applied,
            attempts: 2,
            failure: None
        })
    );
    assert_eq!(
        c.state()
            .page_state("page")
            .unwrap()
            .unwrap()
            .content_hash(),
        Some("hash-v1")
    );
    assert_eq!(
        c.submit(operation("replay"), lease.clone(), clock(10), |_| async {
            panic!("applied replay ran")
        })
        .await,
        Ok(CommitOutcome::AlreadyApplied)
    );
    let conflicting = PageOperation::new(
        "replay",
        "revision-v2",
        PageAction::Refresh,
        PageSyncState::present("page".into(), "hash-v2".into(), None).unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.submit(conflicting, lease.clone(), clock(10), |_| async {
            panic!("different revision ran")
        })
        .await,
        Err(CommitError::Conflict)
    );
    assert_eq!(
        c.submit(operation("failure"), lease.clone(), clock(10), |_| async {
            Err(FailureClass::Source)
        })
        .await,
        Err(CommitError::Operation(FailureClass::Source))
    );
    assert_eq!(
        c.receipt("failure").unwrap().unwrap().failure,
        Some(FailureClass::Source)
    );
    assert_eq!(
        c.submit(operation("failure"), lease, clock(10), |_| async { Ok(()) })
            .await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(c.receipt("failure").unwrap().unwrap().attempts, 2);
}

#[tokio::test]
async fn expiry_during_effect_never_acknowledges_but_keeps_guard_until_completion() {
    let f = Fixture::new();
    let c = f.coordinator();
    let time = Arc::new(AtomicI64::new(10));
    let read = time.clone();
    let lease = c.state().acquire_lease(10, 1).unwrap();
    let update = time.clone();
    assert_eq!(
        c.submit(
            operation("expires"),
            lease,
            Arc::new(move || read.load(Ordering::SeqCst)),
            move |ctx| async move {
                ctx.revalidate().unwrap();
                update.store(11, Ordering::SeqCst);
                Ok(())
            }
        )
        .await,
        Err(CommitError::LeaseLost)
    );
    assert_eq!(
        c.receipt("expires").unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    assert!(c.state().page_state("page").unwrap().is_none());
    assert!(matches!(
        c.change_scope(
            binding("v2", 2),
            &c.state().acquire_lease(11, 100).unwrap(),
            11
        ),
        Err(CommitError::Conflict)
    ));
}

fn child(f: &Fixture, index: &Path, state: &Path, name: &str, lease: &Lease, mode: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "commit_child", "--ignored", "--nocapture"])
        .env("NK_COMMIT_TEST_ROOT", &f.root)
        .env("NK_COMMIT_TEST_INDEX", index)
        .env("NK_COMMIT_TEST_STATE", state)
        .env("NK_COMMIT_TEST_NAME", name)
        .env("NK_COMMIT_TEST_FENCE", lease.fence.to_string())
        .env("NK_COMMIT_TEST_EXPIRY", lease.expires_at.to_string())
        .env("NK_COMMIT_TEST_MODE", mode)
        .spawn()
        .unwrap()
}
#[test]
fn independent_processes_serialize_aliases_after_caller_and_parent_runtime_drop() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(now(), 100).unwrap();
    let alias = f.root.join("alias");
    std::os::unix::fs::symlink(&f.index, &alias).unwrap();
    let mut one = child(&f, &f.index, &f.state, "one", &lease, "drop-runtime");
    wait_for(|| f.root.join("one-entered").exists());
    let mut two = child(&f, &alias.join("."), &f.state, "two", &lease, "normal");
    wait_for(|| f.root.join("two-attempting").exists());
    std::thread::sleep(Duration::from_millis(150));
    assert!(!f.root.join("two-entered").exists());
    assert!(two.try_wait().unwrap().is_none());
    fs::write(f.root.join("one-release"), "").unwrap();
    wait_for(|| f.root.join("two-entered").exists());
    assert!(f.root.join("one-completed").exists());
    fs::write(f.root.join("two-release"), "").unwrap();
    wait_child(&mut one);
    wait_child(&mut two);
    assert_eq!(
        c.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
    assert_eq!(
        c.receipt("two").unwrap().unwrap().status,
        ReceiptStatus::Applied
    );
}
#[test]
fn independent_process_cannot_bypass_active_guard_with_other_state_or_expired_lease() {
    let f = Fixture::new();
    let c = f.coordinator();
    // Child startup and scheduler load must not advance lease time. Expiry
    // is controlled only after the first owned effect is confirmed active.
    fs::write(f.root.join("controlled-clock"), "").unwrap();
    let lease = c.state().acquire_lease(10, 1).unwrap();
    let mut one = child(&f, &f.index, &f.state, "one", &lease, "drop-observer");
    wait_for(|| f.root.join("one-entered").exists());
    fs::write(f.root.join("clock-expired"), "").unwrap();
    let next = c.state().acquire_lease(11, 100).unwrap();
    let mut two = child(&f, &f.index, &f.state, "two", &next, "normal");
    let mut other = child(
        &f,
        &f.index,
        &f.root.join("other.sqlite"),
        "other",
        &next,
        "mismatch",
    );
    wait_for(|| f.root.join("two-attempting").exists() && f.root.join("other-attempting").exists());
    std::thread::sleep(Duration::from_millis(100));
    assert!(!f.root.join("two-entered").exists());
    assert!(other.try_wait().unwrap().is_none());
    fs::write(f.root.join("one-release"), "").unwrap();
    wait_for(|| f.root.join("two-entered").exists());
    assert!(f.root.join("one-completed").exists());
    fs::write(f.root.join("two-release"), "").unwrap();
    wait_child(&mut one);
    wait_child(&mut two);
    wait_child(&mut other);
    assert_eq!(
        c.receipt("one").unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    assert!(f.root.join("other-rejected").exists());
}
#[test]
#[ignore = "subprocess helper invoked by cross-process contracts"]
fn commit_child() {
    let root = PathBuf::from(std::env::var_os("NK_COMMIT_TEST_ROOT").unwrap());
    let index = PathBuf::from(std::env::var_os("NK_COMMIT_TEST_INDEX").unwrap());
    let state = PathBuf::from(std::env::var_os("NK_COMMIT_TEST_STATE").unwrap());
    let name = std::env::var("NK_COMMIT_TEST_NAME").unwrap();
    let mode = std::env::var("NK_COMMIT_TEST_MODE").unwrap();
    fs::write(root.join(format!("{name}-attempting")), "").unwrap();
    if mode == "mismatch" {
        assert!(matches!(
            IndexCommitCoordinator::initialize(index, state, binding("v1", 1)),
            Err(CommitError::BindingMismatch)
        ));
        fs::write(root.join(format!("{name}-rejected")), "").unwrap();
        return;
    }
    let c = IndexCommitCoordinator::open(index, state, binding("v1", 1)).unwrap();
    let lease = Lease {
        fence: std::env::var("NK_COMMIT_TEST_FENCE")
            .unwrap()
            .parse()
            .unwrap(),
        expires_at: std::env::var("NK_COMMIT_TEST_EXPIRY")
            .unwrap()
            .parse()
            .unwrap(),
    };
    let expires = lease.expires_at;
    let effect_root = root.clone();
    let effect_name = name.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock_root = root.clone();
    let child_clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
        if clock_root.join("controlled-clock").exists() {
            if clock_root.join("clock-expired").exists() {
                11
            } else {
                10
            }
        } else {
            now()
        }
    });
    let observer = c.submit(
        operation(&name),
        lease,
        child_clock.clone(),
        move |ctx| async move {
            ctx.revalidate().unwrap();
            assert!(ctx.state().index_version("chunks").unwrap().is_none());
            fs::write(effect_root.join(format!("{effect_name}-entered")), "").unwrap();
            while !effect_root.join(format!("{effect_name}-release")).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            fs::write(effect_root.join(format!("{effect_name}-completed")), "").unwrap();
            Ok(())
        },
    );
    if mode == "normal" {
        assert_eq!(runtime.block_on(observer), Ok(CommitOutcome::Applied));
    } else {
        if mode == "drop-runtime" {
            let observer_task = runtime.spawn(observer);
            runtime.block_on(async {
                while !root.join(format!("{name}-entered")).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            observer_task.abort();
        } else {
            drop(observer);
        }
        drop(runtime);
        wait_for(|| root.join(format!("{name}-completed")).exists());
        if child_clock() < expires {
            wait_for(|| {
                c.receipt(&name)
                    .unwrap()
                    .is_some_and(|r| r.status == ReceiptStatus::Applied)
            });
        } else {
            std::thread::sleep(Duration::from_millis(100));
            assert_eq!(
                c.receipt(&name).unwrap().unwrap().status,
                ReceiptStatus::Pending
            );
        }
    }
}

#[tokio::test]
async fn operator_supersession_requires_exact_unresolved_set_and_never_records_success() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(10, 100).unwrap();
    assert_eq!(
        c.submit(operation("failed"), lease.clone(), clock(10), |_| async {
            Err(FailureClass::Source)
        })
        .await,
        Err(CommitError::Operation(FailureClass::Source))
    );
    assert!(matches!(
        c.change_scope(binding("v2", 2), &lease, 10),
        Err(CommitError::Conflict)
    ));
    assert!(matches!(
        c.supersede_scope(binding("v2", 2), &lease, 10, &["wrong".into()]),
        Err(CommitError::Conflict)
    ));
    assert_eq!(
        c.receipt("failed").unwrap().unwrap().status,
        ReceiptStatus::Failed
    );
    let next = c
        .supersede_scope(binding("v2", 2), &lease, 10, &["failed".into()])
        .unwrap();
    assert_eq!(
        next.receipt("failed").unwrap(),
        Some(OperationReceipt {
            status: ReceiptStatus::Superseded,
            attempts: 1,
            failure: Some(FailureClass::Conflict)
        })
    );
    assert!(next.state().page_state("page").unwrap().is_none());
    assert_eq!(
        next.submit(operation("failed"), lease.clone(), clock(10), |_| async {
            panic!("old identity ran")
        })
        .await,
        Err(CommitError::Conflict)
    );
    assert_eq!(
        next.submit(operation("new-generation"), lease, clock(10), |_| async {
            Ok(())
        })
        .await,
        Ok(CommitOutcome::Applied)
    );
}
#[tokio::test]
async fn replaced_index_or_state_path_cannot_reuse_old_authority() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(10, 100).unwrap();
    fs::rename(&f.index, f.root.join("old-index")).unwrap();
    fs::create_dir(&f.index).unwrap();
    assert_eq!(
        c.submit(operation("replaced-index"), lease, clock(10), |_| async {
            panic!("replacement ran")
        })
        .await,
        Err(CommitError::BindingMismatch)
    );
    let g = Fixture::new();
    let d = g.coordinator();
    let lease = d.state().acquire_lease(10, 100).unwrap();
    fs::rename(&g.state, g.root.join("old.sqlite")).unwrap();
    fs::write(&g.state, "").unwrap();
    assert_eq!(
        d.submit(operation("replaced-state"), lease, clock(10), |_| async {
            panic!("replacement ran")
        })
        .await,
        Err(CommitError::BindingMismatch)
    );
}
#[test]
fn process_death_releases_directory_guard_and_keeps_operation_pending_for_replay() {
    let f = Fixture::new();
    let c = f.coordinator();
    let lease = c.state().acquire_lease(now(), 100).unwrap();
    let mut one = child(&f, &f.index, &f.state, "crashed", &lease, "drop-observer");
    wait_for(|| f.root.join("crashed-entered").exists());
    one.kill().unwrap();
    assert!(!one.wait().unwrap().success());
    assert_eq!(
        c.receipt("crashed").unwrap().unwrap().status,
        ReceiptStatus::Pending
    );
    assert!(c.state().page_state("page").unwrap().is_none());
    drop(c);
    let c = IndexCommitCoordinator::open(&f.index, &f.state, binding("v1", 1)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        runtime.block_on(c.submit(
            operation("crashed"),
            lease,
            Arc::new(now),
            |ctx| async move {
                ctx.revalidate().unwrap();
                Ok(())
            }
        )),
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(c.receipt("crashed").unwrap().unwrap().attempts, 2);
}
