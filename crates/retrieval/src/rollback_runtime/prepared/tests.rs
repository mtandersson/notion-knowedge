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
        let record_path = parent.path().join(if newer {
            "latest-prepared.json"
        } else {
            "exact-prepared.json"
        });
        let prepared_metadata = current.with_owned_offline_inspection(i64::MAX/2, |owner| {
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
            prepared.publish(&record_path).unwrap();
            let original = fs::read(&backup).unwrap();
            fs::write(&backup, b"corrupt immutable backup").unwrap();
            assert!(prepared.revalidate().is_err());
            fs::write(&backup, original).unwrap();
            Ok(record.clone())
        }).unwrap();
        assert_eq!(bytes(&root), before);
        assert_eq!(
            PreparedRollbackMetadata::read(&record_path).unwrap(),
            prepared_metadata
        );
        let alias = parent
            .path()
            .join(if newer { "latest-alias" } else { "exact-alias" });
        std::os::unix::fs::symlink(&record_path, &alias).unwrap();
        assert!(PreparedRollbackMetadata::read(&alias).is_err());
        if newer && !bound {
            killed_publication_retries(&record_path, &binary, &sha, &prepared_metadata);
            assert_eq!(bytes(&root), before);
        }
        // Retained reopening never creates a missing immutable backup.
        let saved = fs::read(&backup).unwrap();
        fs::remove_file(&backup).unwrap();
        current
            .with_owned_offline_inspection(i64::MAX / 2, |owner| {
                assert!(
                    reopen(
                        owner,
                        PreparedInputs {
                            prior: inputs(),
                            retained: RetainedCandidate {
                                transfer: &transfer,
                                metadata: &metadata
                            },
                            target: &target,
                            quarantine_generation: "rollback-quarantine"
                        },
                        &embedding,
                        &record_path,
                        PreparedLimits::default()
                    )
                    .is_err()
                );
                assert!(!backup.exists());
                Ok(())
            })
            .unwrap();
        fs::write(&backup, saved).unwrap();
        fs::set_permissions(&backup, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(bytes(&root), before);
    }
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn_record_child(
    input: &Path,
    output: &Path,
    runtime: &Path,
    sha: &str,
    phase: &str,
) -> Child {
    Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "rollback_runtime::prepared::tests::prepared_record_process_child",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .env("NK_RUNTIME_PREPARED_TEST_INPUT", input)
            .env("NK_RUNTIME_PREPARED_TEST_OUTPUT", output)
            .env("NK_RUNTIME_PREPARED_TEST_PHASE", phase)
            .env("NK_RUNTIME_PREPARED_TEST_RUNTIME", runtime)
            .env("NK_RUNTIME_PREPARED_TEST_SHA", sha)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    )
}
fn killed_publication_retries(
    input: &Path,
    runtime: &Path,
    sha: &str,
    metadata: &PreparedRollbackMetadata,
) {
    let parent = input.parent().unwrap();
    let marker = parent.join("observed-runtime-prepared");
    for phase in ["stage-synced", "record-renamed", "record-published"] {
        let output = parent.join(format!("interrupted-{phase}.json"));
        let mut child = spawn_record_child(input, &output, runtime, sha, phase);
        let started = std::time::Instant::now();
        loop {
            if fs::read_to_string(&marker).is_ok_and(|value| value == phase) {
                break;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "child timed out before {phase}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        fs::remove_file(&marker).unwrap();
        if phase == "stage-synced" {
            assert!(!output.exists());
            assert!(fs::read_dir(parent).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".nk-runtime-prepared-")
            }));
        } else {
            assert_eq!(PreparedRollbackMetadata::read(&output).unwrap(), *metadata);
        }
        for _ in 0..2 {
            assert!(
                spawn_record_child(input, &output, runtime, sha, "")
                    .0
                    .wait()
                    .unwrap()
                    .success()
            );
            assert_eq!(PreparedRollbackMetadata::read(&output).unwrap(), *metadata);
        }
    }
}
#[test]
#[ignore = "only launched by Prepared durability process tests"]
fn prepared_record_process_child() {
    let input = PathBuf::from(std::env::var_os("NK_RUNTIME_PREPARED_TEST_INPUT").unwrap());
    let output = PathBuf::from(std::env::var_os("NK_RUNTIME_PREPARED_TEST_OUTPUT").unwrap());
    let metadata = PreparedRollbackMetadata::read(&input).unwrap();
    let runtime = PinnedRuntime::pin(
        Path::new(&std::env::var_os("NK_RUNTIME_PREPARED_TEST_RUNTIME").unwrap()),
        &std::env::var("NK_RUNTIME_PREPARED_TEST_SHA").unwrap(),
        1 << 30,
    )
    .unwrap();
    assert!(metadata.candidate.asset_sha256.is_none());
    let binding = CommitBinding::parse_retained(
        metadata
            .candidate
            .current
            .metadata
            .authority_json
            .as_bytes(),
    )
    .unwrap();
    let target =
        CommitBinding::parse_retained(metadata.candidate.target_authority_json.as_bytes()).unwrap();
    let current = IndexCommitCoordinator::open_existing(
        &metadata.current.path,
        &metadata.state_path,
        binding,
    )
    .unwrap();
    current
        .with_owned_offline_inspection(i64::MAX / 2, |owner| {
            let proof = reopen(
                owner,
                PreparedInputs {
                    prior: PriorInputs {
                        runtime: &runtime,
                        asset: None,
                        evidence: &metadata.prior_evidence_path,
                        lineage: &metadata.lineage_paths,
                        archive: &metadata.prior_archive.path,
                    },
                    retained: RetainedCandidate {
                        transfer: &metadata.transfer_path,
                        metadata: &metadata.candidate,
                    },
                    target: &target,
                    quarantine_generation: &metadata.quarantine_generation,
                },
                &metadata.candidate.current.metadata.embedding,
                &input,
                PreparedLimits::default(),
            )
            .unwrap();
            proof.publish(&output).unwrap();
            Ok(())
        })
        .unwrap();
}
