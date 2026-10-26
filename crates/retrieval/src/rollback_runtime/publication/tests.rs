use super::*;
use crate::{
    commit::IndexCommitCoordinator,
    lexical_dictionary::{prepare_archive, tests::dictionary},
    rollback_runtime::{
        candidate::{
            self, PreparationPaths, PriorInputs, RetainedCandidate,
            tests::{activate, bytes, next},
        },
        evidence,
        prepared::{self, PreparedInputs},
        probe,
        verification::{self, VerificationLimits},
    },
};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
#[test]
#[ignore = "requires matching normal NK_LEXICAL_RUNTIME"]
fn runtime_journal_legacy_handoff_quarantine_and_retention_preserve_source() {
    for newer in [false, true] {
        run_case(false, newer, Case::Publication);
    }
}
#[test]
#[ignore = "requires matching normal runtime and pinned NK_LEXICAL_TEST_ARCHIVE"]
fn runtime_journal_bound_handoff_quarantine_and_retention_preserve_source() {
    for newer in [false, true] {
        run_case(true, newer, Case::Publication);
    }
}
enum Case {
    Publication,
    Crash,
    PhysicalRefusal,
    Capture,
    ChoicePrepared,
    ChoiceCrash,
}
#[test]
#[ignore = "requires matching normal runtime"]
fn runtime_publication_rejects_physical_replacement_and_stale_preparation() {
    run_case(false, false, Case::PhysicalRefusal);
}
#[test]
#[ignore = "requires matching normal runtime"]
fn quarantined_runtime_source_capture_legacy_preserves_latest_source() {
    run_case(false, true, Case::Capture);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn quarantined_runtime_source_capture_bound_preserves_latest_source() {
    run_case(true, true, Case::Capture);
}
#[test]
#[ignore = "requires matching normal runtime"]
fn quarantined_choice_prepared_target_legacy_preserves_source_and_sql() {
    run_case(false, true, Case::ChoicePrepared);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn quarantined_choice_prepared_target_bound_preserves_source_and_sql() {
    run_case(true, true, Case::ChoicePrepared);
}
#[test]
#[ignore = "requires normal runtime; kills actual Prepared-choice publication processes"]
fn quarantined_choice_prepared_record_recovers_after_killed_processes() {
    run_case(false, true, Case::ChoiceCrash);
}
fn run_case(bound: bool, newer: bool, case: Case) {
    let crash = matches!(case, Case::Crash);
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let parent = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let root = parent.path().join("installation");
    let asset = bound.then(|| {
        let path = parent.path().join("prior.bundle");
        let info = prepare_archive(
            Path::new(&std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").unwrap()),
            &path,
        )
        .unwrap();
        PinnedAsset::pin(&path, &info.sha256).unwrap()
    });
    let mode = asset
        .as_ref()
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    runtime
        .run(
            &[
                vec!["probe-create".into(), root.as_os_str().to_owned()],
                mode,
            ]
            .concat(),
            Duration::from_secs(90),
            65536,
        )
        .unwrap();
    let (old_binding, embedding) = probe::fixture_configuration(!bound).unwrap();
    let index = root.join("index");
    let state = root.join("state.sqlite");
    let old = IndexCommitCoordinator::open_existing(&index, &state, old_binding).unwrap();
    let retained = parent.path().join("prior-evidence.json");
    old.with_owned_offline_inspection(20, |owner| {
        let proof = verification::verify(
            owner,
            &runtime,
            asset.as_ref(),
            &embedding,
            VerificationLimits::default(),
        )
        .unwrap();
        evidence::publish(&retained, owner, &proof).unwrap();
        Ok(())
    })
    .unwrap();
    let current_dictionary = dictionary(&[("cinnamon roll", "distinct current plan")]);
    let source = activate(&root, &old, &embedding, &current_dictionary);
    drop(old);
    if newer {
        probe::commit_fixture_row(&root, source.clone(), Some(&current_dictionary)).unwrap();
    }
    let current = IndexCommitCoordinator::open_existing(&index, &state, source.clone()).unwrap();
    let lineage = vec![crate::commit::transition::journal_path(&state).unwrap()];
    let prior = root.join("selected-prior-archive");
    let target = next(&source, "runtime-target");
    let candidate = root.join("rollback-candidate");
    let transfer = parent.path().join("rows");
    let inputs = || PriorInputs {
        runtime: &runtime,
        asset: asset.as_ref(),
        evidence: &retained,
        lineage: &lineage,
        archive: &prior,
    };
    let metadata = current
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            let candidate = candidate::prepare(
                owner,
                inputs(),
                &embedding,
                &target,
                PreparationPaths {
                    transfer: &transfer,
                    candidate: &candidate,
                },
                Default::default(),
                Duration::from_secs(300),
            )
            .unwrap();
            Ok(candidate.metadata().clone())
        })
        .unwrap();
    let prepared_path = parent.path().join("prepared.json");
    current
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            let proof = prepared::prepare(
                owner,
                PreparedInputs {
                    prior: inputs(),
                    retained: RetainedCandidate {
                        transfer: &transfer,
                        metadata: &metadata,
                    },
                    target: &target,
                    quarantine_generation: "runtime-quarantine",
                },
                &embedding,
                &parent.path().join("preparation.sqlite"),
                Default::default(),
            )
            .unwrap();
            proof.publish(&prepared_path).unwrap();
            Ok(())
        })
        .unwrap();
    let before = bytes(&index);
    let sql = fs::read(&state).unwrap();
    let archive = root.join("runtime-current-archive");
    let backup = parent.path().join("activation.sqlite");
    let current_config = CurrentSource {
        index: &index,
        state: &state,
        binding: &source,
        embedding: &embedding,
    };
    if !bound && !newer && !crash {
        // Refusal must preserve the original serving source and operational SQL.
        let marker = crate::commit::transition::journal_path(&state).unwrap();
        let original_marker = fs::read(&marker).unwrap();
        for artifact in [&retained, &transfer, &prepared_path] {
            let original = fs::read(artifact).unwrap();
            fs::write(artifact, b"corrupt retained input").unwrap();
            let refused = RuntimeTransitionOperator::start(
                &runtime,
                None,
                current_config,
                &prepared_path,
                ActivationPaths {
                    archive: &archive,
                    backup: &backup,
                },
                Default::default(),
                i64::MAX / 2,
            )
            .is_err();
            fs::write(artifact, original).unwrap();
            assert!(refused, "corrupt retained input was accepted");
            assert_eq!(fs::read(&state).unwrap(), sql);
            assert_eq!(bytes(&index), before);
            assert_eq!(fs::read(&marker).unwrap(), original_marker);
            assert!(!backup.exists());
            assert!(!archive.exists());
        }
    }
    if matches!(case, Case::PhysicalRefusal) {
        let marker = crate::commit::transition::journal_path(&state).unwrap();
        let original_marker = fs::read(&marker).unwrap();
        for physical in [&state, &index] {
            let retained_path = physical.with_extension("replaced-original");
            fs::rename(physical, &retained_path).unwrap();
            if physical == &state {
                fs::copy(&retained_path, physical).unwrap();
            } else {
                fs::create_dir(physical).unwrap();
            }
            let refused = RuntimeTransitionOperator::start(
                &runtime,
                None,
                current_config,
                &prepared_path,
                ActivationPaths {
                    archive: &archive,
                    backup: &backup,
                },
                Default::default(),
                i64::MAX / 2,
            )
            .is_err();
            if physical == &state {
                fs::remove_file(physical).unwrap();
            } else {
                fs::remove_dir_all(physical).unwrap();
            }
            fs::rename(retained_path, physical).unwrap();
            assert!(refused, "replacement physical authority was accepted");
            assert_eq!(fs::read(&state).unwrap(), sql);
            assert_eq!(bytes(&index), before);
            assert_eq!(fs::read(&marker).unwrap(), original_marker);
            assert!(!backup.exists());
            assert!(!archive.exists());
        }
        // Restoring the same directory does not restore its old ctime. A new
        // Prepared proof is required; the old one must remain refused.
        assert!(
            RuntimeTransitionOperator::start(
                &runtime,
                None,
                current_config,
                &prepared_path,
                ActivationPaths {
                    archive: &archive,
                    backup: &backup
                },
                Default::default(),
                i64::MAX / 2,
            )
            .is_err()
        );
        assert_eq!(fs::read(&state).unwrap(), sql);
        assert_eq!(bytes(&index), before);
        assert!(!backup.exists());
        assert!(!archive.exists());
        return;
    }
    if crash {
        let input = parent.path().join("publication-child.json");
        fs::write(
            &input,
            serde_json::to_vec(&serde_json::json!({
                "index": index, "state": state,
                "source": serde_json::to_string(&source).unwrap(),
                "embedding": embedding, "prepared": prepared_path,
                "archive": archive, "backup": backup,
                "asset": asset.as_ref().map(|_| parent.path().join("prior.bundle")),
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
        let history = killed_publication_retries(&input, &root, &binary, &sha, &before, &archive);
        let outcome = RuntimeTransitionOperator::verify_completed(
            &runtime,
            asset.as_ref(),
            current_config,
            &history,
            Default::default(),
            i64::MAX / 2,
        )
        .unwrap();
        assert_eq!(
            outcome.observation.source.snapshot_sha256,
            metadata.current.metadata.source.snapshot_sha256
        );
        assert_eq!(bytes(&archive), before);
        assert!(IndexCommitCoordinator::open_read_only(&index, &state, target).is_ok());
        return;
    }
    let operator = RuntimeTransitionOperator::start(
        &runtime,
        asset.as_ref(),
        current_config,
        &prepared_path,
        ActivationPaths {
            archive: &archive,
            backup: &backup,
        },
        Default::default(),
        i64::MAX / 2,
    )
    .unwrap();
    assert_eq!(bytes(&index), before);
    assert_eq!(fs::read(&state).unwrap(), sql);
    assert!(crate::commit::transition::TransitionRecord::read(&state).is_err());
    assert!(IndexCommitCoordinator::open_read_only(&index, &state, source.clone()).is_err());
    let record = operator.quarantine(i64::MAX / 2).unwrap();
    assert_eq!(record.phase, RuntimePhase::Quarantined);
    let record = operator.retain_current_source(i64::MAX / 2).unwrap();
    assert_eq!(record.phase, RuntimePhase::SourceRetained);
    assert!(!index.exists());
    assert_eq!(bytes(&archive), before);
    if matches!(case, Case::ChoicePrepared | Case::ChoiceCrash) {
        use crate::rollback_runtime::choice::{self, Choice, SelectionInputs};
        let original_sql = fs::read(&state).unwrap();
        let original_candidate = bytes(&candidate);
        let original_prior = bytes(&prior);
        let desired = next(&target, "explicit-prepared-choice");
        let output = parent.path().join("explicit-prepared-choice.json");
        operator
            .with_quarantined_source_inspection(i64::MAX / 2, Default::default(), |inspection| {
                let inputs = || SelectionInputs {
                    choice: Choice::PreparedTarget,
                    runtime: &runtime,
                    asset: asset.as_ref(),
                    source_evidence: None,
                    desired: &desired,
                };
                if bound {
                    assert!(
                        choice::select(
                            inspection,
                            SelectionInputs {
                                choice: Choice::PreparedTarget,
                                runtime: &runtime,
                                asset: None,
                                source_evidence: None,
                                desired: &desired,
                            },
                            Default::default()
                        )
                        .is_err(),
                        "missing selected bound asset was accepted"
                    );
                }
                let selected = choice::select(inspection, inputs(), Default::default()).unwrap();
                assert_eq!(selected.metadata().selected.path, candidate);
                let proof = selected
                    .prepare(choice::PreparationPaths {
                        transfer: &parent.path().join("explicit-choice-rows"),
                        candidate: None,
                        backup: &parent.path().join("explicit-choice-state.sqlite"),
                    })
                    .unwrap();
                assert!(proof.metadata().built.is_none());
                assert_eq!(
                    proof.metadata().candidate_report.source.snapshot_sha256,
                    metadata.current.metadata.source.snapshot_sha256
                );
                proof.publish(&output).unwrap();
                drop(proof);
                let reopened =
                    choice::reopen(inspection, inputs(), &output, Default::default()).unwrap();
                reopened.publish(&output).unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(fs::read(&state).unwrap(), original_sql);
        assert_eq!(bytes(&archive), before);
        assert_eq!(bytes(&candidate), original_candidate);
        assert_eq!(bytes(&prior), original_prior);
        assert_eq!(operator.owner.runtime_record().unwrap(), record);
        assert!(current.search_guard().is_err());
        if matches!(case, Case::ChoiceCrash) {
            let input = parent.path().join("choice-child.json");
            fs::write(
                &input,
                serde_json::to_vec(&serde_json::json!({
                    "record": record,
                    "source": serde_json::to_string(&source).unwrap(),
                    "desired": serde_json::to_string(&desired).unwrap(),
                    "embedding": embedding,
                    "index": index,
                    "state": state,
                    "prepared": output,
                }))
                .unwrap(),
            )
            .unwrap();
            drop(operator);
            for phase in ["stage-synced", "record-renamed", "record-published"] {
                let destination = parent.path().join(format!("choice-{phase}.json"));
                let retries = if phase == "record-renamed" { 4 } else { 3 };
                for attempt in 0..retries {
                    let checkpoint = if attempt == 0 {
                        phase
                    } else if phase == "record-renamed" && attempt == 1 {
                        "record-published"
                    } else {
                        ""
                    };
                    let mut child = PublicationChild(std::process::Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "rollback_runtime::publication::tests::choice_prepared_process_child", "--ignored", "--nocapture"])
                        .env_clear()
                        .env("NK_LEXICAL_RUNTIME", &binary)
                        .env("NK_CHOICE_INPUT", &input)
                        .env("NK_CHOICE_DESTINATION", &destination)
                        .env("NK_CHOICE_TEST_ROOT", &root)
                        .env("NK_CHOICE_TEST_PHASE", checkpoint)
                        .spawn().unwrap());
                    if !checkpoint.is_empty() {
                        let marker = root.join("observed-choice-prepared");
                        let started = std::time::Instant::now();
                        while !fs::read_to_string(&marker).is_ok_and(|v| v == checkpoint) {
                            assert!(
                                child.0.try_wait().unwrap().is_none(),
                                "child exited before {phase}"
                            );
                            assert!(
                                started.elapsed() < Duration::from_secs(300),
                                "checkpoint timeout: {phase}"
                            );
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        child.0.kill().unwrap();
                        assert!(!child.0.wait().unwrap().success());
                        fs::remove_file(marker).unwrap();
                    } else {
                        assert!(
                            child.0.wait().unwrap().success(),
                            "fresh Prepared-choice retry failed"
                        );
                    }
                    assert_eq!(fs::read(&state).unwrap(), original_sql);
                    assert_eq!(bytes(&archive), before);
                    assert_eq!(bytes(&candidate), original_candidate);
                    assert_eq!(bytes(&prior), original_prior);
                }
            }
        }
        return;
    }
    if matches!(case, Case::Capture) {
        let original_sql = fs::read(&state).unwrap();
        let original_prior = bytes(&prior);
        let forbidden = prior.join("must-not-write-capture");
        assert!(
            operator
                .with_quarantined_source_inspection(
                    i64::MAX / 2,
                    Default::default(),
                    |inspection| {
                        crate::chunks::canonical_transfer::export_quarantined(
                            inspection,
                            &forbidden,
                            &embedding,
                            Default::default(),
                        )
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)
                    },
                )
                .is_err()
        );
        assert!(
            !forbidden.exists(),
            "refusal must precede retained native mutation"
        );
        assert_eq!(bytes(&prior), original_prior);
        assert_eq!(bytes(&archive), before);
        assert_eq!(fs::read(&state).unwrap(), original_sql);
        let output = parent.path().join("quarantined-canonical-rows");
        let captured = operator
            .with_quarantined_source_inspection(i64::MAX / 2, Default::default(), |inspection| {
                assert_eq!(inspection.binding(), &source);
                assert_eq!(inspection.index_directory(), archive);
                crate::chunks::canonical_transfer::export_quarantined(
                    inspection,
                    &output,
                    &embedding,
                    Default::default(),
                )
                .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .unwrap();
        assert_eq!(
            captured.metadata.source.snapshot_sha256,
            metadata.current.metadata.source.snapshot_sha256
        );
        assert_eq!(
            captured.metadata.source.rows,
            metadata.current.metadata.source.rows
        );
        let mut count = 0;
        crate::chunks::canonical_transfer::read(&output, &captured, Default::default(), |batch| {
            count += batch.num_rows() as u64;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, metadata.current.metadata.source.rows);
        assert_eq!(fs::read(&state).unwrap(), original_sql);
        assert_eq!(bytes(&archive), before);
        assert_eq!(bytes(&prior), original_prior);
        assert_eq!(operator.owner.runtime_record().unwrap(), record);
        assert!(current.search_guard().is_err());
        return;
    }
    let record = operator.refence_target(i64::MAX / 2).unwrap();
    assert_eq!(record.phase, RuntimePhase::TargetRefenced);
    assert_eq!(
        record.refenced.as_ref().unwrap().snapshot_sha256,
        metadata.current.metadata.source.snapshot_sha256
    );
    assert_eq!(bytes(&archive), before);
    let record = operator.install_target(i64::MAX / 2).unwrap();
    assert_eq!(record.phase, RuntimePhase::TargetInstalled);
    operator.publish_derived(i64::MAX / 2).unwrap();
    let record = operator.complete_publication(i64::MAX / 2).unwrap();
    assert_eq!(record.phase, RuntimePhase::Complete);
    assert_eq!(bytes(&archive), before);
    drop(operator);
    let reopened = RuntimeTransitionOperator::open(
        &runtime,
        asset.as_ref(),
        current_config,
        &record,
        Default::default(),
        i64::MAX / 2,
    )
    .unwrap();
    assert_eq!(reopened.owner.runtime_record().unwrap(), record);
    let outcome = reopened.finish(i64::MAX / 2).unwrap();
    assert!(outcome.history.exists());
    assert_eq!(
        outcome.observation.source.snapshot_sha256,
        metadata.current.metadata.source.snapshot_sha256
    );
    assert!(
        !crate::commit::transition::journal_path(&state)
            .unwrap()
            .exists()
    );
    assert!(IndexCommitCoordinator::open_read_only(&index, &state, target.clone()).is_ok());

    assert_eq!(bytes(&archive), before);
    assert!(
        current.search_guard().is_err(),
        "stale current worker admitted after rollback"
    );
    let old_fence = probe::fixture_configuration(!bound).unwrap().0;
    assert!(
        IndexCommitCoordinator::open_read_only(&index, &state, old_fence).is_err(),
        "reused prior physical index must not reuse its old serving fence"
    );
    let final_sql = fs::read(&state).unwrap();
    assert!(
        RuntimeTransitionOperator::start(
            &runtime,
            asset.as_ref(),
            current_config,
            &prepared_path,
            ActivationPaths {
                archive: &archive,
                backup: &backup
            },
            Default::default(),
            i64::MAX / 2,
        )
        .is_err(),
        "stale Prepared work was accepted after publication"
    );
    assert_eq!(fs::read(&state).unwrap(), final_sql);
    assert_eq!(bytes(&archive), before);
    let published = IndexCommitCoordinator::open_existing(&index, &state, target).unwrap();
    published
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            let latest = owner
                .prior_archive(std::slice::from_ref(&outcome.history))
                .unwrap();
            assert_eq!(latest.binding, source);
            assert_eq!(latest.path, archive);
            assert_eq!(latest.device, record.prepared.current.device);
            assert_eq!(latest.inode, record.prepared.current.inode);
            assert_eq!(
                latest.snapshot_sha256,
                metadata.current.metadata.source.snapshot_sha256
            );
            if newer {
                // Explicit mixed v2/v1 lineage remains usable after a rollback that
                // built a fresh target and retained the original prior archive.
                let mut lineage = vec![outcome.history.clone()];
                lineage.extend(record.lineage_paths.clone());
                let oldest = owner.prior_archive(&lineage).unwrap();
                let expected = probe::fixture_configuration(!bound).unwrap().0;
                assert_eq!(oldest.binding, expected);
                assert_eq!(oldest.path, prior);
                assert_eq!(oldest.device, record.prepared.prior_archive.device);
                assert_eq!(oldest.inode, record.prepared.prior_archive.inode);
                assert_eq!(
                    oldest.snapshot_sha256,
                    metadata.prior.source.snapshot_sha256
                );
            }
            Ok(())
        })
        .unwrap();
}

#[test]
#[ignore = "requires matching normal runtime; kills actual publication subprocesses"]
fn runtime_publication_recovers_legacy_after_killed_processes() {
    run_case(false, true, Case::Crash);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn runtime_publication_recovers_bound_archive_after_killed_processes() {
    run_case(true, false, Case::Crash);
}

#[test]
#[ignore = "publication subprocess entrypoint"]
fn runtime_publication_process_child() {
    let Some(input) = std::env::var_os("NK_RUNTIME_PUBLICATION_TEST_INPUT") else {
        return;
    };
    let value: serde_json::Value = serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    let path = |name: &str| PathBuf::from(value[name].as_str().unwrap());
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = std::env::var("NK_RUNTIME_PUBLICATION_TEST_SHA").unwrap();
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let asset = value["asset"].as_str().map(|p| {
        let sha = format!("{:x}", Sha256::digest(fs::read(p).unwrap()));
        PinnedAsset::pin(Path::new(p), &sha).unwrap()
    });
    let binding =
        CommitBinding::parse_retained(value["source"].as_str().unwrap().as_bytes()).unwrap();
    let embedding = serde_json::from_value(value["embedding"].clone()).unwrap();
    let index = path("index");
    let state = path("state");
    if let Some(history) = value["history"].as_str()
        && !crate::commit::transition::journal_path(&state)
            .unwrap()
            .exists()
    {
        RuntimeTransitionOperator::verify_completed(
            &runtime,
            asset.as_ref(),
            CurrentSource {
                index: &index,
                state: &state,
                binding: &binding,
                embedding: &embedding,
            },
            Path::new(history),
            Default::default(),
            i64::MAX / 2,
        )
        .unwrap();
        return;
    }
    let operator = RuntimeTransitionOperator::start(
        &runtime,
        asset.as_ref(),
        CurrentSource {
            index: &index,
            state: &state,
            binding: &binding,
            embedding: &embedding,
        },
        &path("prepared"),
        ActivationPaths {
            archive: &path("archive"),
            backup: &path("backup"),
        },
        Default::default(),
        i64::MAX / 2,
    )
    .unwrap();
    operator.reconcile(i64::MAX / 2).unwrap();
}
struct PublicationChild(std::process::Child);
impl Drop for PublicationChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn killed_publication_retries(
    input: &Path,
    root: &Path,
    runtime: &Path,
    sha: &str,
    original: &std::collections::BTreeMap<PathBuf, Vec<u8>>,
    archive: &Path,
) -> PathBuf {
    let mut history = None;
    for phase in [
        "runtime-prior-history-retained",
        "runtime-Prepared-staged",
        "runtime-Prepared-renamed",
        "runtime-quarantine-before-commit",
        "quarantine-sql",
        "runtime-Quarantined-renamed",
        "source-renamed",
        "runtime-SourceRetained-renamed",
        "runtime-TargetRefenced-staged",
        "runtime-TargetRefenced-renamed",
        "target-renamed",
        "runtime-TargetInstalled-renamed",
        "runtime-adoption-before-commit",
        "runtime-adoption-committed",
        "anchor-installed",
        "runtime-Published-renamed",
        "runtime-Complete-renamed",
        "runtime-complete-history-retained",
        "runtime-complete-unlinked",
        "",
        "",
    ] {
        if phase == "runtime-complete-unlinked" {
            let record = RuntimeJournalRecord::read_active(&root.join("state.sqlite"))
                .unwrap()
                .unwrap();
            let path = record.completed_history_path().unwrap();
            let mut config: serde_json::Value =
                serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
            config["history"] = serde_json::to_value(&path).unwrap();
            fs::write(input, serde_json::to_vec(&config).unwrap()).unwrap();
            history = Some(path);
        }
        eprintln!("publication interruption checkpoint: {phase}");
        let mut child = PublicationChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "rollback_runtime::publication::tests::runtime_publication_process_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env_clear()
                .env("NK_LEXICAL_RUNTIME", runtime)
                .env("NK_RUNTIME_PUBLICATION_TEST_INPUT", input)
                .env("NK_RUNTIME_PUBLICATION_TEST_SHA", sha)
                .env("NK_TRANSITION_TEST_ROOT", root)
                .env("NK_TRANSITION_TEST_PHASE", phase)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        if phase.is_empty() {
            assert!(
                child.0.wait().unwrap().success(),
                "fresh completed retry failed"
            );
            continue;
        }
        let marker = root.join("observed-transition");
        let started = std::time::Instant::now();
        loop {
            if fs::read_to_string(&marker).is_ok_and(|v| v == phase) {
                break;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(300),
                "child timed out before {phase}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        fs::remove_file(marker).unwrap();
        if archive.exists() {
            assert_eq!(&bytes(archive), original);
        }
        if phase.ends_with("before-commit") {
            let journal = root.join("state.sqlite-journal");
            assert!(fs::metadata(journal).unwrap().len() > 512);
        }
    }
    history.unwrap()
}

#[test]
#[ignore = "Prepared-choice publication subprocess entrypoint"]
fn choice_prepared_process_child() {
    use crate::rollback_runtime::choice::{self, Choice, SelectionInputs};
    let Some(input) = std::env::var_os("NK_CHOICE_INPUT") else {
        return;
    };
    let value: serde_json::Value = serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let path = |name: &str| PathBuf::from(value[name].as_str().unwrap());
    let source =
        CommitBinding::parse_retained(value["source"].as_str().unwrap().as_bytes()).unwrap();
    let desired =
        CommitBinding::parse_retained(value["desired"].as_str().unwrap().as_bytes()).unwrap();
    let embedding = serde_json::from_value(value["embedding"].clone()).unwrap();
    let record = serde_json::from_value(value["record"].clone()).unwrap();
    let index = path("index");
    let state = path("state");
    let operator = RuntimeTransitionOperator::open(
        &runtime,
        None,
        CurrentSource {
            index: &index,
            state: &state,
            binding: &source,
            embedding: &embedding,
        },
        &record,
        Default::default(),
        i64::MAX / 2,
    )
    .unwrap();
    operator
        .with_quarantined_source_inspection(i64::MAX / 2, Default::default(), |inspection| {
            let proof = choice::reopen(
                inspection,
                SelectionInputs {
                    choice: Choice::PreparedTarget,
                    runtime: &runtime,
                    asset: None,
                    source_evidence: None,
                    desired: &desired,
                },
                &path("prepared"),
                Default::default(),
            )
            .unwrap();
            proof
                .publish(&PathBuf::from(
                    std::env::var_os("NK_CHOICE_DESTINATION").unwrap(),
                ))
                .unwrap();
            Ok(())
        })
        .unwrap();
}
