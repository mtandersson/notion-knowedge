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

#[test]
fn derived_adoption_is_atomic_and_retains_pending_and_historical_work() {
    use notion_knowledge_retrieval::commit::transition::{adoption::*, *};
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for scenario in [
        "success",
        "row-budget",
        "plan-budget",
        "unknown-plan",
        "projection-version",
        "effect-budget",
        "late-failure",
        "unknown-effect",
        "wrong-policy",
        "missing-reservation",
        "orphan-projection",
        "two-active-runs",
        "malformed-known-effect",
        "identity-budget",
        "total-effect-budget",
        "historical-source",
        "unknown-prepared",
        "malformed-prepared",
        "wrong-database",
    ] {
        let f = Fixture::new();
        let c = f.coordinator();
        let source = plan();
        let _lease = start(&c, &source);
        let prepared_operation = operation(&c, "new", PageAction::Refresh);
        c.prepare_reconciliation_operation(
            &_lease,
            10,
            "run",
            &prepared_operation,
            &allowed(),
            false,
        )
        .unwrap();
        let original = c.reconciliation_plan("run").unwrap();
        let target = CommitBinding::new("chunks", "workspace", &scope(), "lexical-v2", 3).unwrap();
        let mut db = Connection::open(&f.state).unwrap();
        db.execute_batch("INSERT INTO index_commit_receipts VALUES('pending','1','new','revision',0,'hash',NULL,0,NULL,2); INSERT INTO index_commit_receipts VALUES('completed','1','done','revision',0,'done-hash',NULL,1,NULL,1); INSERT INTO index_commit_receipts VALUES('superseded','1','old','revision',0,'old-hash',NULL,3,2,1);").unwrap();
        db.execute(
            "INSERT INTO page_metadata_identity VALUES('new','1',?1)",
            ["a".repeat(64)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO page_relation_projections VALUES('new','1',1,'relation-v1:0:')",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO page_alias_projections VALUES('new','1',1,?1)",
            [format!("alias-v1:{}", "b".repeat(64))],
        )
        .unwrap();
        db.execute(
            "INSERT INTO alias_operation_proposals VALUES('pending',?1)",
            [format!("alias-v1:{}", "b".repeat(64))],
        )
        .unwrap();
        db.execute(
            "INSERT INTO relation_operation_proposals VALUES('pending','relation-v1:0:')",
            [],
        )
        .unwrap();
        db.execute("INSERT INTO reconciliation_schedules VALUES('schedule','chunks','workspace',?1,'1',1,60,3,1,60,100,'run',0,1,1)",[scope().identity()]).unwrap();
        db.execute(
            "INSERT INTO reconciliation_runs(run_id,scope,fence,phase) VALUES('historical',?1,1,2)",
            [scope().identity()],
        )
        .unwrap();
        let historical =
            serde_json::to_string(&DurablePlan::from_planner("historical", &source).unwrap())
                .unwrap();
        db.execute(
            "INSERT INTO reconciliation_plans VALUES('historical',1,?1)",
            [&historical],
        )
        .unwrap();
        db.execute("INSERT INTO reconciliation_success VALUES('historical','chunks','workspace',?1,'1',1,10)",[scope().identity()]).unwrap();
        let mut limits = AdoptionLimits::default();
        match scenario {
            "row-budget" => limits.rows = 1,
            "plan-budget" => limits.plan_bytes = 1,
            "effect-budget" => limits.effect_bytes = 1,
            "identity-budget" => limits.identity_bytes = 1,
            "total-effect-budget" => limits.total_effect_bytes = 1,
            "malformed-known-effect" => {
                db.execute(
                    "UPDATE relation_operation_proposals SET effect='relation-v1:garbage'",
                    [],
                )
                .unwrap();
            }
            "unknown-prepared" => {
                let value: String = db
                    .query_row(
                        "SELECT prepared FROM reconciliation_plan_progress WHERE page_id='new'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                let mut value: serde_json::Value = serde_json::from_str(&value).unwrap();
                value["unknown_contract"] = true.into();
                db.execute(
                    "UPDATE reconciliation_plan_progress SET prepared=?1 WHERE page_id='new'",
                    [serde_json::to_string(&value).unwrap()],
                )
                .unwrap();
            }
            "malformed-prepared" => {
                let value: String = db
                    .query_row(
                        "SELECT prepared FROM reconciliation_plan_progress WHERE page_id='new'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                let mut value: serde_json::Value = serde_json::from_str(&value).unwrap();
                value["action"] = 99.into();
                db.execute(
                    "UPDATE reconciliation_plan_progress SET prepared=?1 WHERE page_id='new'",
                    [serde_json::to_string(&value).unwrap()],
                )
                .unwrap();
            }
            "unknown-plan" => {
                let mut document = serde_json::to_value(&original).unwrap();
                document["unknown_contract"] = true.into();
                db.execute(
                    "UPDATE reconciliation_plans SET document=?1 WHERE run_id='run'",
                    [serde_json::to_string(&document).unwrap()],
                )
                .unwrap();
            }
            "projection-version" => {
                db.execute("UPDATE page_relation_projections SET version=2", [])
                    .unwrap();
            }
            "late-failure" => {
                db.execute_batch("CREATE TRIGGER fail_adoption BEFORE UPDATE OF document ON reconciliation_plans BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
            }
            "unknown-effect" => {
                db.execute(
                    "UPDATE alias_operation_proposals SET effect='unsupported-v2'",
                    [],
                )
                .unwrap();
            }
            "orphan-projection" => {
                db.execute(
                    "INSERT INTO page_metadata_identity VALUES('unowned','1',?1)",
                    ["c".repeat(64)],
                )
                .unwrap();
            }
            "two-active-runs" => {
                db.execute_batch("DROP INDEX reconciliation_one_active;")
                    .unwrap();
                db.execute("INSERT INTO reconciliation_runs(run_id,scope,fence,phase) VALUES('unexpected',?1,1,0)",[scope().identity()]).unwrap();
            }
            _ => {}
        }
        for path in [&f.root, &f.index] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::set_permissions(&f.state, fs::Permissions::from_mode(0o600)).unwrap();
        let candidate = f.root.join("candidate");
        fs::create_dir(&candidate).unwrap();
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700)).unwrap();
        let backup_path = f.root.join("activation.sqlite");
        fs::copy(&f.state, &backup_path).unwrap();
        fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600)).unwrap();
        let bytes = fs::read(&backup_path).unwrap();
        let source_identity = fs::metadata(&f.index).unwrap();
        let target_identity = fs::metadata(&candidate).unwrap();
        let state_identity = fs::metadata(&f.state).unwrap();
        let record = TransitionRecord {
            version: 1,
            phase: TransitionPhase::Prepared,
            state_device: state_identity.dev(),
            state_inode: state_identity.ino(),
            state_token: db
                .query_row(
                    "SELECT identity FROM index_commit_state_identity",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
            source: TransitionEndpoint {
                path: f.index.clone(),
                device: source_identity.dev(),
                inode: source_identity.ino(),
                binding: serde_json::to_string(&binding()).unwrap(),
            },
            target: TransitionEndpoint {
                path: f.index.clone(),
                device: target_identity.dev(),
                inode: target_identity.ino(),
                binding: serde_json::to_string(&target).unwrap(),
            },
            candidate_path: candidate,
            archive_path: f.root.join("archive"),
            quarantine_generation: "quarantine".into(),
            quarantine_epoch: 2,
            snapshot_sha256: "a".repeat(64),
            asset_sha256: "b".repeat(64),
            ready_sha256: "c".repeat(64),
            adoption_sha256: "d".repeat(64),
            backup_path,
            backup: OfflineBackup {
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                bytes: bytes.len() as u64,
            },
        };
        let owner = TransitionOwnership::acquire(&f.state, &record).unwrap();
        record.persist(&owner, None).unwrap();
        owner.quarantine(15).unwrap();
        owner.install_candidate(15).unwrap();
        if scenario == "wrong-policy" {
            db.execute("UPDATE index_commit_binding SET workspace='other'", [])
                .unwrap();
        }
        if scenario == "missing-reservation" {
            db.execute(
                "DELETE FROM index_commit_generations WHERE generation='lexical-v2'",
                [],
            )
            .unwrap();
        }
        let before_plan: String = db
            .query_row(
                "SELECT document FROM reconciliation_plans WHERE run_id='run'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let before_progress:Vec<(String,i64,Option<String>)> = db.prepare("SELECT operation_id,status,prepared FROM reconciliation_plan_progress ORDER BY page_id").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<Result<_,_>>().unwrap();
        let supplied_source = if scenario == "historical-source" {
            CommitBinding::new("chunks", "workspace", &scope(), "unrelated", 0).unwrap()
        } else {
            binding()
        };
        if scenario == "historical-source" {
            db.execute(
                "INSERT INTO index_commit_generations VALUES('unrelated',?1,0)",
                [scope().identity()],
            )
            .unwrap();
        }
        let mut detached = if scenario == "wrong-database" {
            let path = f.root.join("detached.sqlite");
            fs::copy(&f.state, &path).unwrap();
            Some(Connection::open(path).unwrap())
        } else {
            None
        };
        let connection = detached.as_mut().unwrap_or(&mut db);
        let mut transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)
            .unwrap();
        let result = adopt_derived_work(
            &owner,
            &mut transaction,
            &supplied_source,
            &target,
            71,
            limits,
        );
        // Commit even on error to verify the helper's own savepoint rollback.
        transaction.commit().unwrap();
        let expected_generation = if scenario == "success" {
            assert_eq!(
                result.unwrap(),
                AdoptionReport {
                    receipts: 2,
                    metadata: 1,
                    relations: 1,
                    aliases: 1,
                    schedules: 1,
                    plans: 1
                }
            );
            let actual: DurablePlan = serde_json::from_str(
                &db.query_row::<String, _, _>(
                    "SELECT document FROM reconciliation_plans WHERE run_id='run'",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                actual,
                original
                    .adopt_physical_generation(&binding(), &target, 71)
                    .unwrap()
            );
            "lexical-v2"
        } else {
            assert!(result.is_err(), "{scenario}");
            assert_eq!(
                db.query_row::<String, _, _>(
                    "SELECT document FROM reconciliation_plans WHERE run_id='run'",
                    [],
                    |r| r.get(0)
                )
                .unwrap(),
                before_plan
            );
            "1"
        };
        assert_eq!(db.query_row("SELECT generation,status,attempts FROM index_commit_receipts WHERE operation_id='pending'",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?))).unwrap(),(expected_generation.into(),0,2));
        assert_eq!(
            db.query_row(
                "SELECT generation FROM index_commit_receipts WHERE operation_id='completed'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            expected_generation
        );
        assert_eq!(
            db.query_row(
                "SELECT generation FROM index_commit_receipts WHERE operation_id='superseded'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "1"
        );
        assert_eq!(
            db.query_row("SELECT generation FROM page_metadata_identity", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            expected_generation
        );
        assert_eq!(
            db.query_row("SELECT generation FROM page_alias_projections", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            expected_generation
        );
        assert_eq!(
            db.query_row(
                "SELECT generation,next_deadline,active_run,attempts FROM reconciliation_schedules",
                [],
                |r| Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?
                ))
            )
            .unwrap(),
            (expected_generation.into(), 100, "run".into(), 1)
        );
        let after_progress:Vec<(String,i64,Option<String>)> = db.prepare("SELECT operation_id,status,prepared FROM reconciliation_plan_progress ORDER BY page_id").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(after_progress, before_progress);
        assert_eq!(
            db.query_row(
                "SELECT document FROM reconciliation_plans WHERE run_id='historical'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            historical
        );
        assert_eq!(
            db.query_row(
                "SELECT generation,completed_at_seconds FROM reconciliation_success",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            )
            .unwrap(),
            ("1".into(), 10)
        );
        assert_eq!(
            db.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            "quarantine"
        );
    }
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

#[test]
fn backup_comparison_allows_only_complete_authority_adoption_and_preserves_opaque_state() {
    use notion_knowledge_retrieval::commit::transition::{
        adoption::*, manifest::comparison::*, manifest::*, *,
    };
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for scenario in [
        "preserved",
        "queue-tamper",
        "receipt-status",
        "progress-tamper",
        "history-tamper",
        "plan-format",
        "partial-adoption",
        "wrong-target-version",
        "missing-reservation",
        "backup-corrupt",
        "sequence-tamper",
        "backup-budget",
        "reserved-source-name",
        "reserved-source-epoch",
        "wrong-manifest",
        "same-file-backup",
    ] {
        let f = Fixture::new();
        for path in [&f.root, &f.index] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let c = f.coordinator();
        let source = plan();
        let lease = start(&c, &source);
        c.prepare_reconciliation_operation(
            &lease,
            10,
            "run",
            &operation(&c, "new", PageAction::Refresh),
            &allowed(),
            false,
        )
        .unwrap();
        c.state().release_lease(&lease, 11).unwrap();
        fs::set_permissions(&f.state, fs::Permissions::from_mode(0o600)).unwrap();
        let mut db = Connection::open(&f.state).unwrap();
        db.execute_batch("INSERT INTO index_commit_receipts VALUES('pending','1','new','revision',0,'hash',NULL,0,NULL,2); INSERT INTO index_commit_receipts VALUES('completed','1','done','revision',0,'done',NULL,1,NULL,1); INSERT INTO index_commit_receipts VALUES('superseded','1','old','revision',0,'old',NULL,3,2,1); INSERT INTO page_sync_state VALUES('new','hash',NULL,0,0); INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id) VALUES('workspace','mutation','new'); CREATE TABLE opaque_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT,value BLOB); INSERT INTO opaque_ledger(value) VALUES(x'0001ff'); DELETE FROM opaque_ledger;").unwrap();
        db.execute(
            "INSERT INTO page_metadata_identity VALUES('new','1',?1)",
            ["a".repeat(64)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO page_alias_projections VALUES('new','1',1,?1)",
            [format!("alias-v1:{}", "b".repeat(64))],
        )
        .unwrap();
        db.execute(
            "INSERT INTO page_relation_projections VALUES('new','1',1,'relation-v1:0:')",
            [],
        )
        .unwrap();
        db.execute("INSERT INTO reconciliation_schedules VALUES('schedule','chunks','workspace',?1,'1',1,60,3,1,60,100,'run',0,1,1)",[scope().identity()]).unwrap();
        db.execute(
            "INSERT INTO reconciliation_runs(run_id,scope,fence,phase) VALUES('historical',?1,1,2)",
            [scope().identity()],
        )
        .unwrap();
        let historical = format!(
            "  {}\n",
            serde_json::to_string(&DurablePlan::from_planner("historical", &source).unwrap())
                .unwrap()
        );
        db.execute(
            "INSERT INTO reconciliation_plans VALUES('historical',1,?1)",
            [&historical],
        )
        .unwrap();
        db.execute("INSERT INTO reconciliation_success VALUES('historical','chunks','workspace',?1,'1',1,10)",[scope().identity()]).unwrap();
        let active: String = db
            .query_row(
                "SELECT document FROM reconciliation_plans WHERE run_id='run'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        db.execute(
            "UPDATE reconciliation_plans SET document=?1 WHERE run_id='run'",
            [format!("\n {active} \n")],
        )
        .unwrap();
        if scenario == "reserved-source-name" {
            db.execute(
                "INSERT INTO index_commit_generations VALUES('lexical-v2',?1,99)",
                [scope().identity()],
            )
            .unwrap();
        }
        if scenario == "reserved-source-epoch" {
            db.execute(
                "INSERT INTO index_commit_generations VALUES('used',?1,3)",
                [scope().identity()],
            )
            .unwrap();
        }
        let candidate = f.root.join("candidate");
        fs::create_dir(&candidate).unwrap();
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700)).unwrap();
        let backup_path = f.root.join("activation.sqlite");
        fs::copy(&f.state, &backup_path).unwrap();
        fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600)).unwrap();
        let bytes = fs::read(&backup_path).unwrap();
        let old = fs::metadata(&f.index).unwrap();
        let new = fs::metadata(&candidate).unwrap();
        let state = fs::metadata(&f.state).unwrap();
        let target = CommitBinding::new("chunks", "workspace", &scope(), "lexical-v2", 3).unwrap();
        let source_binding = binding();
        let input = ComparisonInputs {
            source: &source_binding,
            target: &target,
            target_version: 71,
            plan_bytes: 16 * 1024 * 1024,
            backup_bytes: 2 * 1024 * 1024 * 1024,
            limits: ManifestLimits::default(),
        };
        let mut record = TransitionRecord {
            version: 1,
            phase: TransitionPhase::Prepared,
            state_device: state.dev(),
            state_inode: state.ino(),
            state_token: db
                .query_row(
                    "SELECT identity FROM index_commit_state_identity",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
            source: TransitionEndpoint {
                path: f.index.clone(),
                device: old.dev(),
                inode: old.ino(),
                binding: serde_json::to_string(&source_binding).unwrap(),
            },
            target: TransitionEndpoint {
                path: f.index.clone(),
                device: new.dev(),
                inode: new.ino(),
                binding: serde_json::to_string(&target).unwrap(),
            },
            candidate_path: candidate,
            archive_path: f.root.join("archive"),
            quarantine_generation: "quarantine".into(),
            quarantine_epoch: 2,
            snapshot_sha256: "a".repeat(64),
            asset_sha256: "b".repeat(64),
            ready_sha256: "c".repeat(64),
            adoption_sha256: "0".repeat(64),
            backup_path: backup_path.clone(),
            backup: OfflineBackup {
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                bytes: bytes.len() as u64,
            },
        };
        if matches!(
            scenario,
            "reserved-source-name" | "reserved-source-epoch" | "same-file-backup"
        ) {
            if scenario == "same-file-backup" {
                record.backup_path = f.state.clone();
            }
            assert!(capture_backup_manifest(&record, &input).is_err());
            assert_eq!(
                db.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
                "1"
            );
            continue;
        }
        let sealed = capture_backup_manifest(&record, &input).unwrap();
        record.adoption_sha256 = sealed.sha256.clone();
        if scenario == "wrong-manifest" {
            record.adoption_sha256 = "f".repeat(64);
        }
        let owner = TransitionOwnership::acquire(&f.state, &record).unwrap();
        record.persist(&owner, None).unwrap();
        if scenario == "wrong-manifest" {
            assert!(
                owner
                    .with_exclusive_sql(12, |tx| verify_backup_comparison(
                        &owner,
                        tx,
                        &input,
                        ComparisonStage::Prepared
                    ))
                    .is_err()
            );
            assert_eq!(
                db.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
                "1"
            );
            continue;
        }
        assert_eq!(
            owner
                .with_exclusive_sql(12, |tx| verify_backup_comparison(
                    &owner,
                    tx,
                    &input,
                    ComparisonStage::Prepared
                ))
                .unwrap(),
            sealed
        );
        owner.quarantine(12).unwrap();
        owner.install_candidate(12).unwrap();
        assert_eq!(
            owner
                .with_exclusive_sql(12, |tx| verify_backup_comparison(
                    &owner,
                    tx,
                    &input,
                    ComparisonStage::BeforePublication
                ))
                .unwrap(),
            sealed
        );
        if scenario == "queue-tamper" {
            db.execute("UPDATE main.mutation_refresh SET state=2", [])
                .unwrap();
            assert!(
                owner
                    .with_exclusive_sql(12, |tx| verify_backup_comparison(
                        &owner,
                        tx,
                        &input,
                        ComparisonStage::BeforePublication
                    ))
                    .is_err()
            );
            continue;
        }
        if scenario == "backup-corrupt" {
            fs::write(&backup_path, b"corrupt").unwrap();
            assert!(
                owner
                    .with_exclusive_sql(12, |tx| verify_backup_comparison(
                        &owner,
                        tx,
                        &input,
                        ComparisonStage::BeforePublication
                    ))
                    .is_err()
            );
            fs::write(&backup_path, &bytes).unwrap();
            assert!(
                owner
                    .with_exclusive_sql(12, |tx| verify_backup_comparison(
                        &owner,
                        tx,
                        &input,
                        ComparisonStage::BeforePublication
                    ))
                    .is_ok()
            );
            continue;
        }
        if scenario == "backup-budget" {
            let small = ComparisonInputs {
                backup_bytes: 1,
                ..input
            };
            assert!(
                owner
                    .with_exclusive_sql(12, |tx| verify_backup_comparison(
                        &owner,
                        tx,
                        &small,
                        ComparisonStage::BeforePublication
                    ))
                    .is_err()
            );
            continue;
        }
        let mut tx = db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)
            .unwrap();
        adopt_derived_work(
            &owner,
            &mut tx,
            &source_binding,
            &target,
            71,
            AdoptionLimits::default(),
        )
        .unwrap();
        tx.execute("UPDATE main.index_commit_binding SET index_device=?1,index_inode=?2,generation='lexical-v2',epoch=3",rusqlite::params![new.dev().to_string(),new.ino().to_string()]).unwrap();
        match scenario {
            "receipt-status" => {
                tx.execute(
                    "UPDATE main.index_commit_receipts SET status=1 WHERE operation_id='pending'",
                    [],
                )
                .unwrap();
            }
            "progress-tamper" => {
                tx.execute("UPDATE main.reconciliation_plan_progress SET failure='changed',status=2 WHERE page_id='new'",[]).unwrap();
            }
            "history-tamper" => {
                tx.execute("UPDATE main.reconciliation_plans SET document=document||' ' WHERE run_id='historical'",[]).unwrap();
            }
            "plan-format" => {
                tx.execute("UPDATE main.reconciliation_plans SET document=document||' ' WHERE run_id='run'",[]).unwrap();
            }
            "partial-adoption" => {
                tx.execute("UPDATE main.page_metadata_identity SET generation='1'", [])
                    .unwrap();
            }
            "missing-reservation" => {
                tx.execute(
                    "DELETE FROM main.index_commit_generations WHERE generation='quarantine'",
                    [],
                )
                .unwrap();
            }
            "sequence-tamper" => {
                tx.execute(
                    "UPDATE main.sqlite_sequence SET seq=2 WHERE name='opaque_ledger'",
                    [],
                )
                .unwrap();
            }
            _ => {}
        }
        let wrong = ComparisonInputs {
            target_version: 72,
            ..input
        };
        let result = verify_backup_comparison(
            &owner,
            &tx,
            if scenario == "wrong-target-version" {
                &wrong
            } else {
                &input
            },
            ComparisonStage::AfterPublication,
        );
        if scenario == "preserved" {
            assert_eq!(result.unwrap(), sealed);
        } else {
            assert!(result.is_err(), "{scenario}");
        }
        tx.rollback().unwrap();
        assert_eq!(fs::metadata(&f.state).unwrap().ino(), state.ino());
        assert_eq!(fs::read(&backup_path).unwrap(), bytes);
    }
}
#[test]
fn physical_adoption_preserves_plan_work_and_rejects_policy_changes() {
    let source = binding();
    let target = CommitBinding::new("chunks", "workspace", &scope(), "lexical-v2", 3).unwrap();
    let original = DurablePlan::from_planner("run", &plan()).unwrap();
    assert_eq!(
        original.adopt_physical_generation(&source, &target, 0),
        Err(JournalError::InvalidInput)
    );
    let adopted = original
        .adopt_physical_generation(&source, &target, 71)
        .unwrap();
    assert_eq!(adopted.pages, original.pages);
    assert_eq!(adopted.inspected_ids, original.inspected_ids);
    assert_eq!(adopted.planning_failures, original.planning_failures);
    assert_eq!(adopted.run_failures, original.run_failures);
    assert_eq!(adopted.traversal_complete, original.traversal_complete);
    assert_eq!(adopted.index_version, 71);
    assert_eq!(original.index_version, 42);
    for location in ["plan", "authority", "page"] {
        let mut unknown = serde_json::to_value(&original).unwrap();
        let object = match location {
            "plan" => &mut unknown,
            "authority" => &mut unknown["authority"],
            "page" => &mut unknown["pages"]["new"],
            _ => unreachable!(),
        };
        object
            .as_object_mut()
            .unwrap()
            .insert("future_contract".into(), true.into());
        assert!(serde_json::from_value::<DurablePlan>(unknown).is_err());
    }
    assert_eq!(
        adopted.adopt_physical_generation(&source, &target, 71),
        Err(JournalError::ScopeMismatch)
    );
    let rollback = CommitBinding::new("chunks", "workspace", &scope(), "rollback-v1", 5).unwrap();
    let rolled_back = adopted
        .adopt_physical_generation(&target, &rollback, 82)
        .unwrap();
    assert_eq!(rolled_back.pages, original.pages);
    assert_eq!(rolled_back.index_version, 82);

    let changed =
        ReconciliationScope::new(vec!["other-root".into()], vec![], "webhook-v1", "1").unwrap();
    for incompatible in [
        source.clone(),
        CommitBinding::new("chunks", "workspace", &scope(), "stale", 0).unwrap(),
        CommitBinding::new("chunks", "workspace", &scope(), "1", 4).unwrap(),
        CommitBinding::new("other", "workspace", &scope(), "next", 4).unwrap(),
        CommitBinding::new("chunks", "other-workspace", &scope(), "next", 4).unwrap(),
        CommitBinding::new("chunks", "workspace", &changed, "next", 4).unwrap(),
    ] {
        assert_eq!(
            original.adopt_physical_generation(&source, &incompatible, 71),
            Err(JournalError::ScopeMismatch)
        );
    }
}
#[tokio::test]
async fn physical_generation_preserves_source_policy_and_receipt_authority_on_reopen() {
    let f = Fixture::new();
    let physical = CommitBinding::new("chunks", "workspace", &scope(), "lexical-v2", 3).unwrap();
    let c = IndexCommitCoordinator::initialize(&f.index, &f.state, physical.clone()).unwrap();
    let mut source = plan();
    source.binding = physical.clone();
    let lease = start(&c, &source);
    let op = operation(&c, "new", PageAction::Refresh);

    for changed in ["workspace", "roots", "generation", "exclusions"] {
        let mut wrong = allowed();
        match changed {
            "workspace" => wrong.scope.workspace_id = "other".into(),
            "roots" => wrong.scope.roots = vec![PageId("other-root".into())],
            "generation" => wrong.scope.generation += 1,
            "exclusions" => {
                wrong.scope.exclusions.page_ids.insert("excluded".into());
            }
            _ => unreachable!(),
        };
        assert_eq!(
            c.prepare_reconciliation_operation(&lease, 10, "run", &op, &wrong, false),
            Err(JournalError::ScopeMismatch),
            "changed source policy: {changed}"
        );
    }
    c.prepare_reconciliation_operation(&lease, 10, "run", &op, &allowed(), false)
        .unwrap();
    c.submit(
        op.clone(),
        lease.clone(),
        Arc::new(|| 10),
        |context| async move { context.revalidate().map_err(|_| FailureClass::Conflict) },
    )
    .await
    .unwrap();
    drop(c);

    assert!(IndexCommitCoordinator::open_existing(&f.index, &f.state, binding()).is_err());
    let c = IndexCommitCoordinator::open_existing(&f.index, &f.state, physical).unwrap();
    assert_eq!(
        c.submit(op, lease.clone(), Arc::new(|| 10), |_| async {
            panic!("an acknowledged source effect must not replay")
        })
        .await
        .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    assert_eq!(
        c.acknowledge_reconciliation_receipt(&lease, 10, "run", "new")
            .unwrap(),
        ActualOutcome::Refreshed
    );
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
    source.errors.clear();
    c.record_reconciliation_plan(&l, 70, "fresh-cycle", &source)
        .unwrap();
    c.fail_reconciliation_run(
        &l,
        70,
        "fresh-cycle",
        "index-unavailable",
        DurableFailure::Index,
    )
    .unwrap();
    let evidence = c
        .reconciliation_failure_evidence("fresh-cycle", 0, 100)
        .unwrap();
    assert_eq!(evidence.len(), 1);
    c.complete_reconciliation_plan(&l, 70, "fresh-cycle")
        .unwrap();
    assert_eq!(
        c.schedule("periodic").unwrap().status,
        ScheduleStatus::HeldPages
    );
    assert!(c.due_schedules(100, 100).unwrap().is_empty());
    assert_eq!(
        evidence,
        c.reconciliation_failure_evidence("fresh-cycle", 0, 100)
            .unwrap()
    );
}
