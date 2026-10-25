use super::*;
use crate::{
    chunks::{
        canonical_transfer::{self, TransferLimits},
        lexical_binding::runtime::RuntimeDescriptor,
        runtime_builder::{CandidateRequest, DirectoryIdentity},
    },
    commit::{CommitError, IndexCommitCoordinator},
    lexical_dictionary::prepare_archive,
    rollback_runtime::probe,
};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn bytes(root: &Path) -> Vec<(PathBuf, String)> {
    fn walk(path: &Path, result: &mut Vec<(PathBuf, String)>) {
        for entry in fs::read_dir(path).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                walk(&p, result);
            } else {
                result.push((
                    p.clone(),
                    format!("{:x}", Sha256::digest(fs::read(p).unwrap())),
                ));
            }
        }
    }
    let mut result = Vec::new();
    walk(root, &mut result);
    result.sort();
    result
}
fn store(path: &Path, body: &[u8]) {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    f.write_all(body).unwrap();
}
#[test]
#[ignore = "requires freshly built NK_LEXICAL_RUNTIME"]
fn pinned_runtime_builds_and_reopens_legacy_candidate_from_bound_current_rows() {
    run_case(false);
}
#[test]
#[ignore = "requires matching normal runtime and pinned public NK_LEXICAL_TEST_ARCHIVE"]
fn pinned_runtime_builds_and_reopens_bound_prior_candidate_with_distinct_current_plan() {
    run_case(true);
}
fn run_case(bound: bool) {
    let binary = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let hash = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let runtime = PinnedRuntime::pin(&binary, &hash, 1 << 30).unwrap();
    assert!(PinnedRuntime::pin(&binary, &"0".repeat(64), 1 << 30).is_err());
    let parent = private();
    let root = parent.path().join("current");
    // Current native fixture has an active plan unavailable to the selected
    // prior image. Its actual canonical data must still be usable neutrally.
    let current_dictionary =
        crate::lexical_dictionary::tests::dictionary(&[("cinnamon roll", "current only phrase")]);
    probe::create(&root, Some(&current_dictionary)).unwrap();
    probe::commit_new_fixture_row(&root, Some(&current_dictionary)).unwrap();
    let (binding, embedding) = probe::fixture_configuration(false).unwrap();
    let coordinator = IndexCommitCoordinator::open_existing(
        root.join("index"),
        root.join("state.sqlite"),
        binding.clone(),
    )
    .unwrap();
    let pinned_asset = bound.then(|| {
        let asset = parent.path().join("prior.bundle");
        let info = prepare_archive(
            Path::new(&std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").unwrap()),
            &asset,
        )
        .unwrap();
        PinnedAsset::pin(&asset, &info.sha256).unwrap()
    });
    let mode = pinned_asset
        .as_ref()
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    let descriptor = RuntimeDescriptor::parse_metadata(
        &runtime
            .run(
                &[vec!["describe".into()], mode.clone()].concat(),
                Duration::from_secs(30),
                65536,
            )
            .unwrap(),
    )
    .unwrap();
    let before = bytes(&root);
    coordinator
        .with_owned_offline_inspection(20, |owner| {
            let path = parent.path().join("rows");
            let limits = TransferLimits::default();
            let transfer = canonical_transfer::export(owner, &path, &embedding, limits)
                .map_err(|_| CommitError::BindingMismatch)?;
            assert!(
                !descriptor
                    .describes_bound_source(transfer.metadata.source.lexical.as_ref().unwrap())
            );
            // CommitBinding parsing requires its original canonical field order.
            let original = String::from_utf8(serde_json::to_vec(&binding).unwrap()).unwrap();
            let original = original
                .replace(
                    &format!("\"generation\":\"{}\"", binding.generation()),
                    "\"generation\":\"rollback-target\"",
                )
                .replace(
                    &format!("\"epoch\":{}", binding.epoch()),
                    &format!("\"epoch\":{}", binding.epoch() + 2),
                );
            let target_binding =
                crate::commit::CommitBinding::parse_retained(original.as_bytes()).unwrap();
            let mut request = CandidateRequest {
                version: 1,
                source: DirectoryIdentity::capture(owner.index_directory()).unwrap(),
                transfer_path: path.clone(),
                transfer,
                target_authority_json: String::from_utf8(
                    serde_json::to_vec(&target_binding).unwrap(),
                )
                .unwrap(),
                candidate: root.join("prior-candidate"),
                runtime: descriptor.clone(),
                limits,
            };
            let result = build(
                &runtime,
                pinned_asset.as_ref(),
                &request,
                Duration::from_secs(300),
            )
            .unwrap();
            assert_eq!(result.runtime, descriptor);
            assert_eq!(result.snapshot.lexical.is_some(), bound);
            assert_eq!(
                result.snapshot.snapshot_sha256,
                request.transfer.metadata.source.snapshot_sha256
            );
            assert_eq!(result.snapshot.rows, request.transfer.metadata.source.rows);
            assert!(!request.candidate.join(".nk-operational-binding").exists());
            assert!(
                !request
                    .candidate
                    .join(".nk-lexical-candidate-ready-v1.json")
                    .exists()
            );
            let config = parent.path().join("request.json");
            store(&config, &request.canonical_bytes().unwrap());
            let run = |action: &str| {
                runtime.run(
                    &[
                        vec![OsString::from(action), config.as_os_str().to_owned()],
                        mode.clone(),
                    ]
                    .concat(),
                    Duration::from_secs(60),
                    131072,
                )
            };
            let reopened =
                CandidateReport::parse_metadata(&run("candidate-reopen").unwrap()).unwrap();
            assert_eq!(reopened, result);
            assert!(run("candidate-create").is_err());
            let guard = fs::File::open(&request.candidate).unwrap();
            guard.try_lock().unwrap();
            assert!(run("candidate-reopen").is_err());
            drop(guard);
            if !bound {
                coupled_runtime_refused(&binary, parent.path(), &request);
            }
            request.candidate = root.join("invalid");
            request.target_authority_json =
                String::from_utf8(serde_json::to_vec(&binding).unwrap()).unwrap();
            fs::write(&config, request.canonical_bytes().unwrap()).unwrap();
            assert!(run("candidate-create").is_err());
            assert!(!request.candidate.exists());
            request.target_authority_json = result.target_authority_json.clone();
            fs::write(&path, b"corrupt transfer").unwrap();
            fs::write(&config, request.canonical_bytes().unwrap()).unwrap();
            assert!(run("candidate-create").is_err());
            assert!(!request.candidate.exists());
            assert_eq!(
                bytes(&root)
                    .into_iter()
                    .filter(|(p, _)| !p.starts_with(root.join("prior-candidate")))
                    .collect::<Vec<_>>(),
                before
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        bytes(&root)
            .into_iter()
            .filter(|(p, _)| !p.starts_with(root.join("prior-candidate")))
            .collect::<Vec<_>>(),
        before
    );
}
fn coupled_runtime_refused(binary: &Path, parent: &Path, request: &CandidateRequest) {
    // This assertion uses a real executable rejecting separated physical
    // fencing before any candidate can be created; capability strings alone
    // cannot turn that refusal into a usable builder.
    let source = parent.join("coupled.rs");
    fs::write(&source, include_str!("../verification/refusal_fixture.rs")).unwrap();
    let executable = parent.join("coupled");
    let marker = parent.join("coupled-marker");
    assert!(
        std::process::Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .env("NK_VERIFIER_RUNTIME", binary)
            .env("NK_VERIFIER_BEHAVIOR", "coupled")
            .env("NK_VERIFIER_MARKER", &marker)
            .args(["--edition=2024", "-C", "debuginfo=0", "-o"])
            .arg(&executable)
            .arg(&source)
            .status()
            .unwrap()
            .success()
    );
    let sha = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
    let runtime = PinnedRuntime::pin(&executable, &sha, 16 << 20).unwrap();
    let mut denied: CandidateRequest =
        serde_json::from_slice(&request.canonical_bytes().unwrap()).unwrap();
    denied.candidate = request
        .source
        .path
        .parent()
        .unwrap()
        .join("coupled-candidate");
    assert!(build(&runtime, None, &denied, Duration::from_secs(10)).is_err());
    assert!(!denied.candidate.exists());
    assert!(marker.exists());
}
