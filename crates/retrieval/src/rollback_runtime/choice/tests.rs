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

#[test]
#[ignore = "requires a normal NK_LEXICAL_RUNTIME"]
fn explicit_source_selection_verifies_latest_rows_and_preserves_quarantine() {
    source_choice_case(false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public archive"]
fn explicit_bound_source_selection_preserves_latest_rows_and_quarantine() {
    source_choice_case(true);
}
fn source_choice_case(bound: bool) {
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
                let handoff = prepared.describe_handoff(&output).unwrap();
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
                let candidate_path = &reopened.metadata().candidate.path;
                let moved_candidate = candidate_path.with_extension("original-inode");
                fs::rename(candidate_path, &moved_candidate).unwrap();
                fs::create_dir(candidate_path).unwrap();
                assert!(reopened.revalidate().is_err());
                fs::remove_dir(candidate_path).unwrap();
                fs::rename(moved_candidate, candidate_path).unwrap();
                // Rename changes the complete native inventory even after the
                // inode is returned. An old proof must continue to refuse it.
                assert!(reopened.revalidate().is_err());
                Ok(selection_metadata)
            },
        )
        .unwrap();
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
}
