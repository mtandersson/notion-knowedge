use super::owner::{PreparedInputs, PreparedLimits};
use super::*;
use crate::{
    commit::IndexCommitCoordinator,
    lexical_dictionary::{prepare_archive, tests::dictionary},
    rollback_runtime::{
        PinnedRuntime,
        candidate::{
            self, PreparationPaths, PriorInputs, RetainedCandidate,
            tests::{activate, bytes, next},
        },
        evidence, probe,
        verification::{self, PinnedAsset, VerificationLimits},
    },
};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
#[test]
#[ignore = "requires matching normal NK_LEXICAL_RUNTIME"]
fn prepared_legacy_pair_preserves_exact_archive_and_newer_acknowledged_source() {
    run(false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned NK_LEXICAL_TEST_ARCHIVE"]
fn prepared_bound_pair_preserves_exact_archive_and_newer_acknowledged_source() {
    run(true);
}
fn run(bound: bool) {
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
    let (prior_binding, embedding) = probe::fixture_configuration(!bound).unwrap();
    let old = IndexCommitCoordinator::open_existing(
        root.join("index"),
        root.join("state.sqlite"),
        prior_binding,
    )
    .unwrap();
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
    let current_dictionary = dictionary(&[("cinnamon roll", "current only expansion")]);
    let current_binding = activate(&root, &old, &embedding, &current_dictionary);
    drop(old);
    let current = IndexCommitCoordinator::open_existing(
        root.join("index"),
        root.join("state.sqlite"),
        current_binding.clone(),
    )
    .unwrap();
    let lineage =
        vec![crate::commit::transition::journal_path(&root.join("state.sqlite")).unwrap()];
    let archive = root.join("selected-prior-archive");
    let target = next(&current_binding, "rollback-prepared");
    let candidate_path = root.join("rollback-candidate");
    let inputs = || PriorInputs {
        runtime: &runtime,
        asset: asset.as_ref(),
        evidence: &retained,
        lineage: &lineage,
        archive: &archive,
    };
    for newer in [false, true] {
        if newer {
            probe::commit_fixture_row(&root, current_binding.clone(), Some(&current_dictionary))
                .unwrap();
        }
        let transfer = parent
            .path()
            .join(if newer { "latest.rows" } else { "exact.rows" });
        let metadata = current
            .with_owned_offline_inspection(i64::MAX / 2, |owner| {
                let proof = candidate::prepare(
                    owner,
                    inputs(),
                    &embedding,
                    &target,
                    PreparationPaths {
                        transfer: &transfer,
                        candidate: &candidate_path,
                    },
                    Default::default(),
                    Duration::from_secs(300),
                )
                .unwrap();
                Ok(proof.metadata().clone())
            })
            .unwrap();
        let before = bytes(&root);
        let backup = parent.path().join(if newer {
            "latest.sqlite"
        } else {
            "exact.sqlite"
        });
        current.with_owned_offline_inspection(i64::MAX/2, |owner| {
            let prepared = prepare(owner, PreparedInputs { prior: inputs(),
                retained: RetainedCandidate { transfer: &transfer, metadata: &metadata },
                target: &target, quarantine_generation: "rollback-quarantine" },
                &embedding, &backup, PreparedLimits::default()).unwrap();
            let record = prepared.metadata();
            assert_eq!(record.candidate.kind, if newer { candidate::CandidateKind::Rebuilt } else { candidate::CandidateKind::ExactArchive });
            assert_eq!(record.observed_target_authority_json == record.candidate.target_authority_json, newer);
            assert_eq!(record.backup, metadata.current.metadata.operational);
            assert_eq!(PreparedRollbackMetadata::parse_metadata(&record.canonical_bytes().unwrap()).unwrap(), *record);
            let db = owner.owned_connection().unwrap();
            let reserved: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation='rollback-prepared' OR generation='rollback-quarantine')", [], |r|r.get(0)).unwrap();
            assert!(!reserved);
            let temp: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name='nk_adoption_owned_pages')", [], |r|r.get(0)).unwrap();
            assert!(!temp);
            prepared.revalidate().unwrap();
            let original = fs::read(&backup).unwrap();
            fs::write(&backup, b"corrupt immutable backup").unwrap();
            assert!(prepared.revalidate().is_err());
            fs::write(&backup, original).unwrap();
            Ok(())
        }).unwrap();
        assert_eq!(bytes(&root), before);
    }
}
