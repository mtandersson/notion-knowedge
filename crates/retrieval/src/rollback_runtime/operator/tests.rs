use super::*;
use crate::rollback_runtime::{evidence, probe};
use notion_knowledge_core::reconciliation::ReconciliationJournal;
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn store(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
}
fn snapshot(root: &Path) -> Vec<(PathBuf, String)> {
    fn walk(root: &Path, result: &mut Vec<(PathBuf, String)>) {
        for entry in fs::read_dir(root).unwrap() {
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
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
#[ignore = "invoked only by the parent crash contract test"]
fn evidence_interruption_child() {
    let Some(config) = std::env::var_os("NK_EVIDENCE_CHILD_CONFIG") else {
        return;
    };
    let output = std::env::var_os("NK_EVIDENCE_CHILD_OUTPUT").unwrap();
    let marker = PathBuf::from(std::env::var_os("NK_EVIDENCE_CHILD_MARKER").unwrap());
    let phase = std::env::var("NK_EVIDENCE_CHILD_PHASE").unwrap();
    evidence::CHECKPOINT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |observed| {
            if observed == phase {
                store(&marker, observed.as_bytes());
                loop {
                    std::thread::park_timeout(Duration::from_secs(1));
                }
            }
        }))
    });
    verify(Path::new(&config), Path::new(&output), false, true).unwrap();
    panic!("interruption checkpoint was not reached");
}
#[test]
#[ignore = "requires freshly built NK_LEXICAL_RUNTIME and NK_LEXICAL_VERIFY"]
fn offline_verification_command_legacy_retries_killed_publication() {
    command_case(false);
}
#[test]
#[ignore = "requires matching normal binaries and pinned public NK_LEXICAL_TEST_ARCHIVE"]
fn offline_verification_command_bound_retries_killed_publication() {
    command_case(true);
}
fn command_case(bound: bool) {
    let parent = private();
    let root = parent.path().join("source");
    let runtime = PathBuf::from(std::env::var_os("NK_LEXICAL_RUNTIME").unwrap());
    let operator = PathBuf::from(std::env::var_os("NK_LEXICAL_VERIFY").unwrap());
    let runtime_sha256 = format!("{:x}", Sha256::digest(fs::read(&runtime).unwrap()));
    let pinned = PinnedRuntime::pin(&runtime, &runtime_sha256, 1 << 30).unwrap();
    let asset = bound.then(|| {
        let path = parent.path().join("prior.bundle");
        let info = crate::lexical_dictionary::prepare_archive(
            Path::new(&std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").unwrap()),
            &path,
        )
        .unwrap();
        AssetConfig {
            path,
            sha256: info.sha256,
        }
    });
    let mut args = vec!["probe-create".into(), root.as_os_str().to_owned()];
    if let Some(a) = &asset {
        args.extend([
            "expansion".into(),
            a.path.as_os_str().to_owned(),
            a.sha256.clone().into(),
        ]);
    } else {
        args.push("legacy".into());
    }
    pinned.run(&args, Duration::from_secs(60), 65536).unwrap();
    let (binding, embedding) = probe::fixture_configuration(!bound).unwrap();
    let config = VerificationConfig {
        version: 1,
        index_directory: root.join("index"),
        state_file: root.join("state.sqlite"),
        authority_json: String::from_utf8(serde_json::to_vec(&binding).unwrap()).unwrap(),
        embedding,
        runtime,
        runtime_sha256,
        asset,
    };
    let config_path = parent.path().join("config.json");
    store(&config_path, &canonical(&config, 65536).unwrap());
    let run = |output: &Path, retained: bool, ack: bool| {
        let mut command = Command::new(&operator);
        command.arg(&config_path);
        if ack {
            command.arg("--all-source-processes-stopped");
        }
        command
            .arg(if retained { "--retained" } else { "--evidence" })
            .arg(output)
            .output()
            .unwrap()
    };
    let before = snapshot(&root);
    let direct = parent.path().join("direct.json");
    assert!(!run(&direct, false, false).status.success());
    assert!(!direct.exists());
    assert!(!run(&direct, true, true).status.success());
    let held = fs::File::open(&config.index_directory).unwrap();
    held.try_lock().unwrap();
    assert!(!run(&direct, false, true).status.success());
    drop(held);
    let coordinator =
        IndexCommitCoordinator::open_existing(&config.index_directory, &config.state_file, binding)
            .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let lease = coordinator.state().acquire_lease(now, 100).unwrap();
    assert!(!run(&direct, false, true).status.success());
    coordinator.state().release_lease(&lease, now).unwrap();
    let baseline = snapshot(&root);
    assert_ne!(before, baseline); // lease bookkeeping is deliberately acknowledged before verification
    let ok = run(&direct, false, true);
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let original = fs::read(&direct).unwrap();
    assert!(!run(&direct, false, true).status.success());
    assert!(run(&direct, true, true).status.success());
    assert_eq!(fs::read(&direct).unwrap(), original);
    for phase in ["stage-synced", "renamed", "published"] {
        let output = parent.path().join(format!("{phase}.json"));
        let marker = parent.path().join(format!("{phase}.marker"));
        let mut child = OwnedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "rollback_runtime::operator::tests::evidence_interruption_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("NK_EVIDENCE_CHILD_CONFIG", &config_path)
                .env("NK_EVIDENCE_CHILD_OUTPUT", &output)
                .env("NK_EVIDENCE_CHILD_MARKER", &marker)
                .env("NK_EVIDENCE_CHILD_PHASE", phase)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(300);
        while !marker.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(fs::read_to_string(&marker).unwrap(), phase);
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let stages: Vec<_> = fs::read_dir(parent.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(".runtime-evidence-")
            })
            .collect();
        if phase == "stage-synced" {
            assert!(!output.exists());
            assert!(!stages.is_empty());
        } else {
            assert!(output.exists());
        }
        let diagnostics: Vec<_> = stages
            .iter()
            .map(|p| (p.clone(), fs::read(p).unwrap()))
            .collect();
        let result = run(&output, output.exists(), true);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(run(&output, true, true).status.success());
        assert_eq!(fs::read(&output).unwrap(), original);
        assert_eq!(snapshot(&root), baseline);
        for (p, bytes) in diagnostics {
            assert_eq!(fs::read(p).unwrap(), bytes);
        }
    }
    // Commit a real newer native row and receipt through the production guard.
    let dictionary = config
        .asset
        .as_ref()
        .map(|a| crate::lexical_dictionary::Dictionary::load(&a.path, &a.sha256).unwrap());
    probe::commit_new_fixture_row(&root, dictionary.as_ref()).unwrap();
    let latest = snapshot(&root);
    assert!(!run(&direct, true, true).status.success());
    assert_eq!(fs::read(&direct).unwrap(), original);
    assert_eq!(snapshot(&root), latest);
    let fresh_path = parent.path().join("new-acknowledged.json");
    let fresh = run(&fresh_path, false, true);
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    assert_ne!(fs::read(fresh_path).unwrap(), original);
    fs::write(&direct, b"{").unwrap();
    assert!(!run(&direct, true, true).status.success());
    assert_eq!(fs::read(&direct).unwrap(), b"{");
    assert_eq!(snapshot(&root), latest);
}
