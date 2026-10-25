use super::*;
use crate::{
    chunks::{CandidateLimits, OfflineLexicalSource, TransitionCandidateInputs},
    commit::{
        IndexCommitCoordinator,
        transition::{
            self, TransitionOwnership,
            adoption::AdoptionLimits,
            manifest::ManifestLimits,
            preparation::{self, ActivationPaths},
            publication,
        },
    },
    lexical_dictionary::{prepare_archive, tests::dictionary},
    rollback_runtime::{
        evidence, probe,
        verification::{self, VerificationLimits},
    },
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, os::unix::fs::DirBuilderExt, sync::Arc};
fn bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, at: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if path == root.join("rollback-candidate") {
                continue;
            }
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().into(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
fn next(binding: &CommitBinding, generation: &str) -> CommitBinding {
    let json = serde_json::to_string(binding)
        .unwrap()
        .replace(
            &format!("\"generation\":\"{}\"", binding.generation()),
            &format!("\"generation\":\"{generation}\""),
        )
        .replace(
            &format!("\"epoch\":{}", binding.epoch()),
            &format!("\"epoch\":{}", binding.epoch() + 2),
        );
    CommitBinding::parse_retained(json.as_bytes()).unwrap()
}
fn activate(
    root: &Path,
    coordinator: &Arc<IndexCommitCoordinator>,
    embedding: &EmbeddingMetadata,
    current_dictionary: &crate::lexical_dictionary::Dictionary,
) -> CommitBinding {
    let source = coordinator.binding();
    let target = next(source, "active-current");
    let preparation_backup = root.join("current-preparation.sqlite");
    let candidate = root.join("current-candidate");
    let archive = root.join("selected-prior-archive");
    OfflineLexicalSource::bind(
        &root.join("index"),
        source.table(),
        embedding.clone(),
        coordinator.clone(),
    )
    .unwrap()
    .prepare(
        &candidate,
        &preparation_backup,
        &target,
        current_dictionary,
        20,
        CandidateLimits::default(),
    )
    .unwrap();
    let input = TransitionCandidateInputs {
        source,
        target: &target,
        embedding,
        dictionary: current_dictionary,
        preparation_backup: &preparation_backup,
        limits: CandidateLimits::default(),
    };
    let record = preparation::prepare_activation(
        coordinator,
        20,
        input,
        ActivationPaths {
            candidate: &candidate,
            archive: &archive,
            backup: &root.join("current-activation.sqlite"),
            quarantine_generation: "current-quarantine",
        },
        AdoptionLimits::default(),
        ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    let owner = TransitionOwnership::acquire(&root.join("state.sqlite"), &record).unwrap();
    publication::reconcile_forward(
        &owner,
        20,
        input,
        AdoptionLimits::default(),
        ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    target
}
#[test]
#[ignore = "requires matching normal NK_LEXICAL_RUNTIME"]
fn owned_legacy_archive_reuse_then_latest_acknowledged_rows_rebuild() {
    run(false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned NK_LEXICAL_TEST_ARCHIVE"]
fn owned_bound_archive_reuse_then_latest_acknowledged_rows_rebuild() {
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
    runtime
        .run(
            &[
                vec!["probe-create".into(), root.as_os_str().to_owned()],
                mode(asset.as_ref()),
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
    let lineage = vec![transition::journal_path(&root.join("state.sqlite")).unwrap()];
    let archive = root.join("selected-prior-archive");
    let target = next(&current_binding, "rollback-target");
    let candidate = root.join("rollback-candidate");
    let inputs = || PriorInputs {
        runtime: &runtime,
        asset: asset.as_ref(),
        evidence: &retained,
        lineage: &lineage,
        archive: &archive,
    };
    let before = bytes(&root);
    let exact_metadata = current
        .with_owned_offline_inspection(30, |owner| {
            // The original source lock is already held; nested owners refuse before
            // trying to acquire the same SQL mutex. The controller never nests one.
            assert!(
                current
                    .with_owned_offline_inspection::<()>(30, |_| panic!("held owner admitted"))
                    .is_err()
            );
            let proof = prepare(
                owner,
                inputs(),
                &embedding,
                &target,
                PreparationPaths {
                    transfer: &parent.path().join("exact.rows"),
                    candidate: &candidate,
                },
                TransferLimits::default(),
                Duration::from_secs(300),
            )
            .unwrap();
            assert_eq!(proof.metadata().kind, CandidateKind::ExactArchive);
            assert_eq!(proof.metadata().candidate.path, archive);
            assert!(proof.metadata().built.is_none());
            assert!(!candidate.exists());
            assert_eq!(proof.metadata().prior.descriptor.is_legacy(), !bound);
            assert_ne!(
                proof.metadata().current.metadata.source.lexical,
                proof.metadata().prior.source.lexical
            );
            proof.revalidate().unwrap();
            Ok(proof.metadata().clone())
        })
        .unwrap();
    assert_eq!(bytes(&root), before);
    current
        .with_owned_offline_inspection(30, |owner| {
            let refuse = |selected: PriorInputs<'_>| {
                let output = parent.path().join("refused.rows");
                assert!(
                    prepare(
                        owner,
                        selected,
                        &embedding,
                        &target,
                        PreparationPaths {
                            transfer: &output,
                            candidate: &candidate
                        },
                        TransferLimits::default(),
                        Duration::from_secs(300)
                    )
                    .is_err()
                );
                assert!(!output.exists());
                assert!(!candidate.exists());
            };
            refuse(PriorInputs {
                lineage: &[],
                ..inputs()
            });
            refuse(PriorInputs {
                evidence: &parent.path().join("missing-evidence"),
                ..inputs()
            });
            refuse(PriorInputs {
                lineage: std::slice::from_ref(&retained),
                ..inputs()
            });
            let held = File::open(&archive).unwrap();
            held.try_lock().unwrap();
            refuse(inputs());
            drop(held);
            let moved = root.join("moved-prior");
            fs::rename(&archive, &moved).unwrap();
            fs::DirBuilder::new().mode(0o700).create(&archive).unwrap();
            refuse(inputs());
            fs::remove_dir(&archive).unwrap();
            fs::rename(&moved, &archive).unwrap();
            let original = fs::read(&retained).unwrap();
            fs::write(&retained, b"{}").unwrap();
            refuse(inputs());
            fs::write(&retained, original).unwrap();
            if let Some(asset) = asset.as_ref() {
                refuse(PriorInputs {
                    asset: None,
                    ..inputs()
                });
                let path = parent.path().join("prior.bundle");
                let original = fs::read(&path).unwrap();
                fs::write(&path, b"corrupt prior asset").unwrap();
                refuse(inputs());
                fs::write(&path, original).unwrap();
                asset.revalidate().unwrap();
            }
            // Refuse derived transfer output inside the retained prior generation
            // before creating any file, even when its name is currently unused.
            let forbidden = archive.join("forbidden-transfer");
            assert!(
                prepare(
                    owner,
                    inputs(),
                    &embedding,
                    &target,
                    PreparationPaths {
                        transfer: &forbidden,
                        candidate: &candidate
                    },
                    TransferLimits::default(),
                    Duration::from_secs(300)
                )
                .is_err()
            );
            assert!(!forbidden.exists());
            Ok(())
        })
        .unwrap();
    assert_eq!(bytes(&root), before);
    probe::commit_fixture_row(&root, current_binding, Some(&current_dictionary)).unwrap();
    let before = bytes(&root);
    let latest_metadata = current
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            assert!(
                verify_retained(
                    owner,
                    inputs(),
                    &embedding,
                    &target,
                    RetainedCandidate {
                        transfer: &parent.path().join("exact.rows"),
                        metadata: &exact_metadata
                    },
                    TransferLimits::default(),
                    Duration::from_secs(300)
                )
                .is_err()
            );
            let proof = prepare(
                owner,
                inputs(),
                &embedding,
                &target,
                PreparationPaths {
                    transfer: &parent.path().join("latest.rows"),
                    candidate: &candidate,
                },
                TransferLimits::default(),
                Duration::from_secs(300),
            )
            .unwrap();
            assert_eq!(proof.metadata().kind, CandidateKind::Rebuilt);
            assert_eq!(proof.metadata().candidate.path, candidate);
            let built = proof.metadata().built.as_ref().unwrap();
            assert_eq!(
                built.snapshot.snapshot_sha256,
                proof.metadata().current.metadata.source.snapshot_sha256
            );
            assert_ne!(
                built.snapshot.snapshot_sha256,
                proof.metadata().prior.source.snapshot_sha256
            );
            assert_eq!(
                built.snapshot.rows,
                proof.metadata().current.metadata.source.rows
            );
            assert_eq!(built.snapshot.lexical.is_some(), bound);
            proof.revalidate().unwrap();
            // Retained descriptive evidence cannot create a new owned proof after
            // its original input changes or disappears.
            let original = fs::read(&retained).unwrap();
            fs::write(&retained, b"{}").unwrap();
            assert!(proof.revalidate().is_err());
            fs::write(&retained, original).unwrap();
            assert!(
                !candidate
                    .join(".nk-lexical-candidate-ready-v1.json")
                    .exists()
            );
            Ok(proof.metadata().clone())
        })
        .unwrap();
    assert_eq!(bytes(&root), before);
    current
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            let mut forged = latest_metadata.clone();
            forged.kind = CandidateKind::ExactArchive;
            assert!(
                verify_retained(
                    owner,
                    inputs(),
                    &embedding,
                    &target,
                    RetainedCandidate {
                        transfer: &parent.path().join("latest.rows"),
                        metadata: &forged
                    },
                    TransferLimits::default(),
                    Duration::from_secs(300)
                )
                .is_err()
            );
            let proof = verify_retained(
                owner,
                inputs(),
                &embedding,
                &target,
                RetainedCandidate {
                    transfer: &parent.path().join("latest.rows"),
                    metadata: &latest_metadata,
                },
                TransferLimits::default(),
                Duration::from_secs(300),
            )
            .unwrap();
            assert_eq!(proof.metadata(), &latest_metadata);
            Ok(())
        })
        .unwrap();
    assert_eq!(bytes(&root), before);
}
