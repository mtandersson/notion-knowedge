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
        run_case(false, newer);
    }
}
#[test]
#[ignore = "requires matching normal runtime and pinned NK_LEXICAL_TEST_ARCHIVE"]
fn runtime_journal_bound_handoff_quarantine_and_retention_preserve_source() {
    for newer in [false, true] {
        run_case(true, newer);
    }
}
fn run_case(bound: bool, newer: bool) {
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
}
