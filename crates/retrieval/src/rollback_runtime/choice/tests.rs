use super::*;
use crate::{
    chunks::{OfflineLexicalSource, TransitionCandidateInputs},
    commit::{
        IndexCommitCoordinator,
        transition::{self, adoption::AdoptionLimits, manifest::ManifestLimits},
    },
    rollback_runtime::{
        evidence, probe,
        verification::{self, VerificationLimits},
    },
};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

struct HandoffChild(std::process::Child);
impl Drop for HandoffChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub(crate) fn kill_reservation_windows(
    root: &Path,
    binary: &Path,
    input: &Path,
    entry: &str,
    input_env: &str,
) {
    for phase in [
        "choice-publication-start-staged",
        "choice-publication-start-renamed",
        "choice-publication-start-durable",
        "choice-reservation-before-commit",
        "choice-reservation-committed",
        "",
    ] {
        let mut child = HandoffChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", entry, "--ignored", "--nocapture"])
                .env_clear()
                .env("NK_LEXICAL_RUNTIME", binary)
                .env(input_env, input)
                .env("NK_CHOICE_RESERVE", "1")
                .env("NK_TRANSITION_TEST_ROOT", root)
                .env("NK_TRANSITION_TEST_PHASE", phase)
                .spawn()
                .unwrap(),
        );
        if phase.is_empty() {
            assert!(
                child.0.wait().unwrap().success(),
                "fresh reservation retry failed"
            );
            continue;
        }
        let marker = root.join("observed-transition");
        let started = std::time::Instant::now();
        while !fs::read_to_string(&marker).is_ok_and(|value| value == phase) {
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
        if phase == "choice-reservation-before-commit" {
            assert!(
                fs::metadata(root.join("state.sqlite-journal"))
                    .unwrap()
                    .len()
                    > 512
            );
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        fs::remove_file(marker).unwrap();
    }
}

pub(crate) fn assert_reserved_choice_sql(
    state: &Path,
    desired: &CommitBinding,
    handoff: &ChoiceHandoffRecord,
) {
    let mut db = rusqlite::Connection::open(state).unwrap();
    let tx = db.transaction().unwrap();
    let before = transition::manifest::reservation::capture_reservation_preimage(
        &tx,
        desired,
        Default::default(),
    )
    .unwrap();
    assert_eq!(before, handoff.prepared.adoption);
    let after = transition::manifest::capture_sql_manifest(&tx, Default::default()).unwrap();
    assert_eq!(after.rows, before.rows + 1);
}

#[test]
#[ignore = "source handoff subprocess entrypoint"]
fn source_handoff_process_child() {
    let Some(input) = std::env::var_os("NK_SOURCE_HANDOFF_INPUT") else {
        return;
    };
    let value: serde_json::Value = serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let path = |name: &str| PathBuf::from(value[name].as_str().unwrap());
    let binding =
        CommitBinding::parse_retained(value["source"].as_str().unwrap().as_bytes()).unwrap();
    let target =
        CommitBinding::parse_retained(value["target"].as_str().unwrap().as_bytes()).unwrap();
    let desired =
        CommitBinding::parse_retained(value["desired"].as_str().unwrap().as_bytes()).unwrap();
    let embedding = serde_json::from_value(value["embedding"].clone()).unwrap();
    let origin = serde_json::from_value(value["origin"].clone()).unwrap();
    let state = path("state");
    let output = path("prepared");
    let evidence = path("evidence");
    let asset = value["asset"].as_str().map(|path| {
        PinnedAsset::pin(Path::new(path), value["asset_sha256"].as_str().unwrap()).unwrap()
    });
    let preparation_backup = path("preparation_backup");
    let dictionary = if let Some(bundle) = value["target_dictionary"].as_str() {
        crate::lexical_dictionary::Dictionary::load(
            Path::new(bundle),
            value["asset_sha256"].as_str().unwrap(),
        )
        .unwrap()
    } else {
        crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")])
    };
    let candidate = TransitionCandidateInputs {
        source: &binding,
        target: &target,
        embedding: &embedding,
        dictionary: &dictionary,
        preparation_backup: &preparation_backup,
        limits: Default::default(),
    };
    let inputs = || SelectionInputs {
        choice: if value["choice"].as_str() == Some("prepared_target") {
            Choice::PreparedTarget
        } else {
            Choice::OriginalSource
        },
        runtime: &runtime,
        asset: asset.as_ref(),
        source_evidence: if value["choice"].as_str() == Some("prepared_target") {
            None
        } else {
            Some(&evidence)
        },
        desired: &desired,
    };
    if let Ok(handoff) = ChoiceHandoffRecord::read_active_metadata(&state) {
        let action = if std::env::var_os("NK_CHOICE_RESERVE").is_some() {
            reserve_activation_publication
        } else {
            recover_activation_handoff
        };
        action(
            &handoff,
            candidate,
            inputs(),
            Default::default(),
            Default::default(),
            i64::MAX / 2,
        )
        .unwrap();
    } else {
        let owner = transition::TransitionOwnership::acquire(&state, &origin).unwrap();
        owner
            .handoff_quarantined_choice(i64::MAX / 2, candidate, Default::default(), |inspection| {
                let proof = reopen(inspection, inputs(), &output, Default::default()).unwrap();
                proof
                    .stage_handoff(&output)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .unwrap();
    }
}

#[test]
#[ignore = "requires a normal NK_LEXICAL_RUNTIME"]
fn explicit_source_selection_verifies_latest_rows_and_preserves_quarantine() {
    source_choice_case(false, false, false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn explicit_bound_source_selection_preserves_latest_rows_and_quarantine() {
    source_choice_case(true, false, false);
}
#[test]
#[ignore = "requires a normal NK_LEXICAL_RUNTIME"]
fn explicit_source_handoff_recovers_with_legacy_runtime() {
    source_choice_case(false, true, false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn explicit_source_handoff_recovers_with_bound_runtime() {
    source_choice_case(true, true, false);
}
#[test]
#[ignore = "requires normal runtime and kills handoff processes"]
fn explicit_source_handoff_recovers_after_killed_processes() {
    source_choice_case(false, true, true);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn explicit_target_handoff_recovers_from_activation_origin() {
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let parent = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let root = parent.path().join("installation");
    let bundle = parent.path().join("target.bundle");
    let info = crate::lexical_dictionary::prepare_archive(
        Path::new(&std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").unwrap()),
        &bundle,
    )
    .unwrap();
    let asset = PinnedAsset::pin(&bundle, &info.sha256).unwrap();
    let dictionary = crate::lexical_dictionary::Dictionary::load(&bundle, asset.sha256()).unwrap();
    runtime
        .run(
            &[
                "probe-create".into(),
                root.as_os_str().to_owned(),
                "legacy".into(),
            ],
            Duration::from_secs(90),
            65536,
        )
        .unwrap();
    let (binding, embedding) = probe::fixture_configuration(true).unwrap();
    let index = root.join("index");
    let state = root.join("state.sqlite");
    let coordinator =
        IndexCommitCoordinator::open_existing(&index, &state, binding.clone()).unwrap();
    let target =
        crate::rollback_runtime::candidate::tests::next(&binding, "target-choice-original");
    let fresh = crate::rollback_runtime::candidate::tests::next(&target, "target-choice-final");
    let target_path = root.join("target-choice-candidate");
    let preparation_backup = parent.path().join("target-choice-preparation.sqlite");
    OfflineLexicalSource::bind(
        &index,
        binding.table(),
        embedding.clone(),
        coordinator.clone(),
    )
    .unwrap()
    .prepare(
        &target_path,
        &preparation_backup,
        &target,
        &dictionary,
        i64::MAX / 2,
        Default::default(),
    )
    .unwrap();
    let candidate = TransitionCandidateInputs {
        source: &binding,
        target: &target,
        embedding: &embedding,
        dictionary: &dictionary,
        preparation_backup: &preparation_backup,
        limits: Default::default(),
    };
    let record = transition::preparation::prepare_activation(
        &coordinator,
        i64::MAX / 2,
        candidate,
        transition::preparation::ActivationPaths {
            candidate: &target_path,
            archive: &root.join("target-choice-archive"),
            backup: &parent.path().join("target-choice-activation.sqlite"),
            quarantine_generation: "target-choice-quarantine",
        },
        AdoptionLimits::default(),
        ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    let owner = transition::TransitionOwnership::acquire(&state, &record).unwrap();
    owner.quarantine(i64::MAX / 2).unwrap();
    let sql = fs::read(&state).unwrap();
    let original = crate::rollback_runtime::candidate::tests::bytes(&index);
    let output = parent.path().join("target-choice.json");
    let inputs = || SelectionInputs {
        choice: Choice::PreparedTarget,
        runtime: &runtime,
        asset: Some(&asset),
        source_evidence: None,
        desired: &fresh,
    };
    owner
        .handoff_quarantined_choice(i64::MAX / 2, candidate, Default::default(), |inspection| {
            let selected = select(inspection, inputs(), Default::default()).unwrap();
            let proof = selected
                .prepare(PreparationPaths {
                    transfer: &parent.path().join("target-choice-rows"),
                    candidate: None,
                    backup: &parent.path().join("target-choice-state.sqlite"),
                })
                .unwrap();
            proof.publish(&output).unwrap();
            proof
                .stage_handoff(&output)
                .map_err(|_| crate::commit::CommitError::BindingMismatch)
        })
        .unwrap();
    let handoff = ChoiceHandoffRecord::read_active_metadata(&state).unwrap();
    drop(owner);
    recover_activation_handoff(
        &handoff,
        candidate,
        inputs(),
        Default::default(),
        Default::default(),
        i64::MAX / 2,
    )
    .unwrap();
    assert_eq!(fs::read(&state).unwrap(), sql);
    assert_eq!(
        crate::rollback_runtime::candidate::tests::bytes(&index),
        original
    );
    assert!(coordinator.search_guard().is_err());
    let input = parent.path().join("target-reservation-child.json");
    let Origin::Activation(origin) = &handoff.prepared.selection.origin else {
        panic!("activation origin required")
    };
    fs::write(&input, serde_json::to_vec(&serde_json::json!({
        "origin": origin, "source": serde_json::to_string(&binding).unwrap(),
        "target": serde_json::to_string(&target).unwrap(), "desired": serde_json::to_string(&fresh).unwrap(),
        "embedding": embedding, "state": state, "prepared": output, "evidence": output,
        "preparation_backup": preparation_backup, "choice": "prepared_target",
        "asset": bundle, "target_dictionary": bundle, "asset_sha256": asset.sha256(),
    })).unwrap()).unwrap();
    fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
    kill_reservation_windows(
        &root,
        &binary,
        &input,
        "rollback_runtime::choice::tests::source_handoff_process_child",
        "NK_SOURCE_HANDOFF_INPUT",
    );
    assert_reserved_choice_sql(&state, &fresh, &handoff);
    assert_eq!(
        crate::rollback_runtime::candidate::tests::bytes(&index),
        original
    );
    assert!(coordinator.search_guard().is_err());
}
fn source_choice_case(bound: bool, handoff_case: bool, crash: bool) {
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let sha = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &sha, 1 << 30).unwrap();
    let parent = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let root = parent.path().join("installation");
    let asset = bound.then(|| {
        let path = parent.path().join("source.bundle");
        let info = crate::lexical_dictionary::prepare_archive(
            Path::new(&std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").unwrap()),
            &path,
        )
        .unwrap();
        PinnedAsset::pin(&path, &info.sha256).unwrap()
    });
    let source_dictionary = asset.as_ref().map(|asset| {
        crate::lexical_dictionary::Dictionary::load(
            &parent.path().join("source.bundle"),
            asset.sha256(),
        )
        .unwrap()
    });
    runtime
        .run(
            &[
                vec!["probe-create".into(), root.as_os_str().to_owned()],
                asset
                    .as_ref()
                    .map(PinnedAsset::mode)
                    .unwrap_or_else(|| vec!["legacy".into()]),
            ]
            .concat(),
            Duration::from_secs(90),
            65536,
        )
        .unwrap();
    let (binding, embedding) = probe::fixture_configuration(!bound).unwrap();
    let index = root.join("index");
    let state = root.join("state.sqlite");
    let coordinator =
        IndexCommitCoordinator::open_existing(&index, &state, binding.clone()).unwrap();
    let retained = parent.path().join("original-source-evidence.json");
    coordinator
        .with_owned_offline_inspection(20, |owner| {
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
    let old_snapshot =
        inspect_runtime_source(&index, &embedding, &binding, Default::default()).unwrap();
    probe::commit_fixture_row(&root, binding.clone(), source_dictionary.as_ref()).unwrap();
    let target = crate::rollback_runtime::candidate::tests::next(&binding, "original-target");
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let native_candidate = root.join("original-target-candidate");
    let preparation_backup = parent.path().join("original-preparation.sqlite");
    OfflineLexicalSource::bind(
        &index,
        binding.table(),
        embedding.clone(),
        coordinator.clone(),
    )
    .unwrap()
    .prepare(
        &native_candidate,
        &preparation_backup,
        &target,
        &dictionary,
        i64::MAX / 2,
        Default::default(),
    )
    .unwrap();
    let candidate = TransitionCandidateInputs {
        source: &binding,
        target: &target,
        embedding: &embedding,
        dictionary: &dictionary,
        preparation_backup: &preparation_backup,
        limits: Default::default(),
    };
    let record = transition::preparation::prepare_activation(
        &coordinator,
        i64::MAX / 2,
        candidate,
        transition::preparation::ActivationPaths {
            candidate: &native_candidate,
            archive: &root.join("original-source-archive"),
            backup: &parent.path().join("activation.sqlite"),
            quarantine_generation: "original-quarantine",
        },
        AdoptionLimits::default(),
        ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    let owner = transition::TransitionOwnership::acquire(&state, &record).unwrap();
    owner.quarantine(i64::MAX / 2).unwrap();
    let fresh = crate::rollback_runtime::candidate::tests::next(&target, "chosen-source");
    let before = crate::rollback_runtime::candidate::tests::bytes(&index);
    let sql = fs::read(&state).unwrap();
    let mut history_for_retry = None;
    let selected = owner
        .with_quarantined_source_inspection(
            i64::MAX / 2,
            candidate,
            Default::default(),
            |inspection| {
                assert!(
                    select(
                        inspection,
                        SelectionInputs {
                            choice: Choice::OriginalSource,
                            runtime: &runtime,
                            asset: asset.as_ref(),
                            source_evidence: None,
                            desired: &fresh
                        },
                        Default::default()
                    )
                    .is_err()
                );
                assert!(
                    select(
                        inspection,
                        SelectionInputs {
                            choice: Choice::OriginalSource,
                            runtime: &runtime,
                            asset: asset.as_ref(),
                            source_evidence: Some(&retained),
                            desired: &target
                        },
                        Default::default()
                    )
                    .is_err()
                );
                let original_evidence = fs::read(&retained).unwrap();
                for field in ["runtime_sha256", "asset_sha256", "authority_sha256"] {
                    let mut value: serde_json::Value =
                        serde_json::from_slice(&original_evidence).unwrap();
                    value[field] = serde_json::Value::String("0".repeat(64));
                    let mismatched: evidence::RetainedEvidence =
                        serde_json::from_value(value).unwrap();
                    fs::write(&retained, serde_json::to_vec(&mismatched).unwrap()).unwrap();
                    assert!(
                        select(
                            inspection,
                            SelectionInputs {
                                choice: Choice::OriginalSource,
                                runtime: &runtime,
                                asset: asset.as_ref(),
                                source_evidence: Some(&retained),
                                desired: &fresh,
                            },
                            Default::default()
                        )
                        .is_err(),
                        "mismatched {field} was accepted"
                    );
                    fs::write(&retained, &original_evidence).unwrap();
                }
                let selected = select(
                    inspection,
                    SelectionInputs {
                        choice: Choice::OriginalSource,
                        runtime: &runtime,
                        asset: asset.as_ref(),
                        source_evidence: Some(&retained),
                        desired: &fresh,
                    },
                    Default::default(),
                )
                .unwrap();
                selected.revalidate().unwrap();
                assert_eq!(selected.metadata().choice, Choice::OriginalSource);
                assert_ne!(
                    selected.metadata().current_native.snapshot_sha256,
                    old_snapshot.snapshot_sha256
                );
                assert_eq!(
                    selected.metadata().current_native.snapshot_sha256,
                    selected.metadata().observation.source.snapshot_sha256
                );
                assert_eq!(
                    Origin::parse_metadata(&selected.metadata().origin.canonical_bytes().unwrap())
                        .unwrap(),
                    selected.metadata().origin
                );
                let selection_metadata = selected.metadata().clone();
                let prepared = selected
                    .prepare(PreparationPaths {
                        transfer: &parent.path().join("choice-canonical-rows"),
                        candidate: Some(&root.join("chosen-source-candidate")),
                        backup: &parent.path().join("choice-state.sqlite"),
                    })
                    .unwrap();
                prepared.revalidate().unwrap();
                assert!(prepared.metadata().built.is_some());
                assert_eq!(
                    prepared.metadata().candidate_report.source.snapshot_sha256,
                    selection_metadata.current_native.snapshot_sha256
                );
                assert_eq!(
                    PreparedChoiceMetadata::parse_metadata(
                        &prepared.metadata().canonical_bytes().unwrap()
                    )
                    .unwrap(),
                    *prepared.metadata()
                );
                let output = parent.path().join("prepared-choice.json");
                prepared.publish(&output).unwrap();
                let handoff = prepared.retain_handoff_origin(&output).unwrap();
                history_for_retry = Some(handoff.origin_history_path.clone());
                handoff.verify_retained_files().unwrap();
                assert_eq!(prepared.retain_handoff_origin(&output).unwrap(), handoff);
                assert!(matches!(
                    &handoff.prepared.selection.origin,
                    Origin::Activation(_)
                ));
                let bytes = handoff.canonical_bytes().unwrap();
                assert_eq!(
                    ChoiceHandoffRecord::parse_metadata(&bytes).unwrap(),
                    handoff
                );
                assert!(serde_json::from_slice::<transition::TransitionRecord>(&bytes).is_err());
                assert!(
                    transition::runtime_journal::RuntimeJournalRecord::parse_metadata(&bytes)
                        .is_err()
                );
                let mut invalid = handoff.clone();
                invalid.prepared_sha256 = "0".repeat(64);
                assert!(invalid.canonical_bytes().is_err());
                let mut invalid = handoff.clone();
                invalid.origin_history_path = output.clone();
                assert!(invalid.canonical_bytes().is_err());
                assert!(
                    ChoiceHandoffRecord::parse_metadata(&[bytes, b"\n".to_vec()].concat()).is_err()
                );
                assert_eq!(
                    PreparedChoiceMetadata::read(&output).unwrap(),
                    *prepared.metadata()
                );
                let mut forged = prepared.metadata().clone();
                forged.selection.operation_sha256 = "0".repeat(64);
                assert!(forged.canonical_bytes().is_err());
                let mut forged = prepared.metadata().clone();
                forged.transfer.metadata.authority_json =
                    selection_metadata.desired_authority_json.clone();
                assert!(forged.canonical_bytes().is_err());
                // Retained descriptions cannot hide missing or changed artifacts.
                for artifact in [
                    &prepared.metadata().transfer_path,
                    &prepared.metadata().backup_path,
                    &retained,
                ] {
                    let original = fs::read(artifact).unwrap();
                    fs::write(artifact, b"corrupt retained artifact").unwrap();
                    assert!(prepared.revalidate().is_err());
                    assert!(handoff.verify_retained_files().is_err());
                    fs::write(artifact, original).unwrap();
                    let moved = artifact.with_extension("temporarily-missing");
                    fs::rename(artifact, &moved).unwrap();
                    assert!(prepared.revalidate().is_err());
                    assert!(
                        !artifact.exists(),
                        "verification recreated an absent artifact"
                    );
                    fs::rename(moved, artifact).unwrap();
                }
                drop(prepared);
                let original_record = fs::read(&output).unwrap();
                fs::write(&output, b"corrupt Prepared record").unwrap();
                assert!(handoff.verify_retained_files().is_err());
                assert!(
                    reopen(
                        inspection,
                        SelectionInputs {
                            choice: Choice::OriginalSource,
                            runtime: &runtime,
                            asset: asset.as_ref(),
                            source_evidence: Some(&retained),
                            desired: &fresh,
                        },
                        &output,
                        Default::default(),
                    )
                    .is_err()
                );
                fs::write(&output, original_record).unwrap();
                let reopened = reopen(
                    inspection,
                    SelectionInputs {
                        choice: Choice::OriginalSource,
                        runtime: &runtime,
                        asset: asset.as_ref(),
                        source_evidence: Some(&retained),
                        desired: &fresh,
                    },
                    &output,
                    Default::default(),
                )
                .unwrap();
                reopened.publish(&output).unwrap();
                if !handoff_case {
                    let candidate_path = &reopened.metadata().candidate.path;
                    let moved_candidate = candidate_path.with_extension("original-inode");
                    fs::rename(candidate_path, &moved_candidate).unwrap();
                    fs::create_dir(candidate_path).unwrap();
                    assert!(reopened.revalidate().is_err());
                    fs::remove_dir(candidate_path).unwrap();
                    fs::rename(moved_candidate, candidate_path).unwrap();
                    // Rename changes the inventory even after the inode returns.
                    assert!(reopened.revalidate().is_err());
                }
                Ok(selection_metadata)
            },
        )
        .unwrap();
    if handoff_case {
        let output = parent.path().join("prepared-choice.json");
        if crash {
            fs::remove_file(history_for_retry.as_ref().unwrap()).unwrap();
            let input = parent.path().join("source-handoff-child.json");
            let origin = owner.current_record().unwrap().unwrap();
            fs::write(
                &input,
                serde_json::to_vec(&serde_json::json!({
                    "origin": origin,
                    "source": serde_json::to_string(&binding).unwrap(),
                    "target": serde_json::to_string(&target).unwrap(),
                    "desired": serde_json::to_string(&fresh).unwrap(),
                    "embedding": embedding,
                    "state": state,
                    "prepared": output,
                    "evidence": retained,
                    "preparation_backup": preparation_backup,
                }))
                .unwrap(),
            )
            .unwrap();
            fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
            drop(owner);
            for phase in [
                "choice-origin-renamed",
                "choice-handoff-staged",
                "choice-handoff-renamed",
                "",
            ] {
                let mut child = HandoffChild(
                    std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "rollback_runtime::choice::tests::source_handoff_process_child",
                            "--ignored",
                            "--nocapture",
                        ])
                        .env_clear()
                        .env("NK_LEXICAL_RUNTIME", &binary)
                        .env("NK_SOURCE_HANDOFF_INPUT", &input)
                        .env("NK_TRANSITION_TEST_ROOT", &root)
                        .env("NK_TRANSITION_TEST_PHASE", phase)
                        .spawn()
                        .unwrap(),
                );
                if phase.is_empty() {
                    assert!(
                        child.0.wait().unwrap().success(),
                        "fresh source handoff recovery failed"
                    );
                } else {
                    let marker = root.join("observed-transition");
                    let started = std::time::Instant::now();
                    while !fs::read_to_string(&marker).is_ok_and(|v| v == phase) {
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
                }
                assert_eq!(fs::read(&state).unwrap(), sql);
                assert_eq!(
                    crate::rollback_runtime::candidate::tests::bytes(&index),
                    before
                );
                assert!(coordinator.search_guard().is_err());
            }
        } else {
            owner
                .handoff_quarantined_choice(
                    i64::MAX / 2,
                    candidate,
                    Default::default(),
                    |inspection| {
                        let proof = reopen(
                            inspection,
                            SelectionInputs {
                                choice: Choice::OriginalSource,
                                runtime: &runtime,
                                asset: asset.as_ref(),
                                source_evidence: Some(&retained),
                                desired: &fresh,
                            },
                            &output,
                            Default::default(),
                        )
                        .unwrap();
                        let chosen_candidate = proof.metadata().candidate.path.clone();
                        proof
                            .stage_handoff(&output)
                            .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                        drop(proof);
                        let competing = fs::File::open(&chosen_candidate).unwrap();
                        assert!(
                            competing.try_lock().is_err(),
                            "the staged handoff must retain the candidate lock"
                        );
                        Ok(())
                    },
                )
                .unwrap();
            assert!(transition::TransitionRecord::read(&state).is_err());
            let handoff = ChoiceHandoffRecord::read_active_metadata(&state).unwrap();
            drop(owner);
            recover_activation_handoff(
                &handoff,
                candidate,
                SelectionInputs {
                    choice: Choice::OriginalSource,
                    runtime: &runtime,
                    asset: asset.as_ref(),
                    source_evidence: Some(&retained),
                    desired: &fresh,
                },
                Default::default(),
                Default::default(),
                i64::MAX / 2,
            )
            .unwrap();
        }
    }
    assert_eq!(
        selected.desired_authority_json,
        serde_json::to_string(&fresh).unwrap()
    );
    assert_eq!(fs::read(&state).unwrap(), sql);
    assert_eq!(
        crate::rollback_runtime::candidate::tests::bytes(&index),
        before
    );
    assert!(coordinator.search_guard().is_err());
    let db = rusqlite::Connection::open(&state).unwrap();
    let reserved: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1)",
            [fresh.generation()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        !reserved,
        "read-only selection must not reserve its fresh plan"
    );
    drop(db);
    if handoff_case {
        let handoff = ChoiceHandoffRecord::read_active_metadata(&state).unwrap();
        if !crash {
            let Origin::Activation(origin) = &handoff.prepared.selection.origin else {
                panic!("activation origin required")
            };
            let input = parent.path().join("reservation-child.json");
            fs::write(&input, serde_json::to_vec(&serde_json::json!({
                "origin": origin, "source": serde_json::to_string(&binding).unwrap(),
                "target": serde_json::to_string(&target).unwrap(), "desired": serde_json::to_string(&fresh).unwrap(),
                "embedding": embedding, "state": state, "prepared": handoff.prepared_path,
                "evidence": retained, "preparation_backup": preparation_backup,
                "asset": asset.as_ref().map(|_| parent.path().join("source.bundle")),
                "asset_sha256": asset.as_ref().map(PinnedAsset::sha256),
            })).unwrap()).unwrap();
            fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
            kill_reservation_windows(
                &root,
                &binary,
                &input,
                "rollback_runtime::choice::tests::source_handoff_process_child",
                "NK_SOURCE_HANDOFF_INPUT",
            );
        }
        let inputs = || SelectionInputs {
            choice: Choice::OriginalSource,
            runtime: &runtime,
            asset: asset.as_ref(),
            source_evidence: Some(&retained),
            desired: &fresh,
        };
        for _ in 0..2 {
            reserve_activation_publication(
                &handoff,
                candidate,
                inputs(),
                Default::default(),
                Default::default(),
                i64::MAX / 2,
            )
            .unwrap();
        }
        recover_activation_handoff(
            &handoff,
            candidate,
            inputs(),
            Default::default(),
            Default::default(),
            i64::MAX / 2,
        )
        .unwrap();
        let mut db = rusqlite::Connection::open(&state).unwrap();
        let tx = db.transaction().unwrap();
        assert_eq!(
            transition::manifest::reservation::capture_reservation_preimage(
                &tx,
                &fresh,
                Default::default()
            )
            .unwrap(),
            handoff.prepared.adoption
        );
        assert_eq!(
            crate::rollback_runtime::candidate::tests::bytes(&index),
            before
        );
        assert!(coordinator.search_guard().is_err());
    }
}
