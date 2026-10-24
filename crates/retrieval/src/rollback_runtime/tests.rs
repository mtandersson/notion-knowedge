use super::*;
use std::os::unix::fs::PermissionsExt;

fn selected() -> (tempfile::TempDir, std::path::PathBuf, String) {
    // Compile one tiny standalone fixture. Native LanceDB test executables can
    // contain gigabytes of debug information; copying them tests debug payload
    // size rather than the executable transport and wastes CI memory.
    static FIXTURE: std::sync::OnceLock<(tempfile::TempDir, std::path::PathBuf)> =
        std::sync::OnceLock::new();
    let (_, fixture) = FIXTURE.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("fixture.rs");
        std::fs::write(&source, include_str!("fixture.rs")).unwrap();
        let binary = dir.path().join("fixture");
        let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .args(["--edition=2024", "-C", "debuginfo=0", "-o"])
            .arg(&binary)
            .arg(&source)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "the supported Rust toolchain must compile the fixture"
        );
        (dir, binary)
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("selected-runtime");
    std::fs::copy(fixture, &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o500)).unwrap();
    let digest = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
    (dir, path, digest)
}
fn args(name: &str) -> Vec<OsString> {
    vec![name.into()]
}
#[test]
fn pinned_execution_retains_exact_bytes_after_source_path_replacement() {
    let (_dir, path, sha) = selected();
    let runtime = PinnedRuntime::pin(&path, &sha, MAX_EXECUTABLE).unwrap();
    assert_eq!(runtime.sha256(), sha);
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, b"replaced executable").unwrap();
    let bytes = runtime
        .run(&args("clean"), Duration::from_secs(10), 4096)
        .unwrap();
    assert_eq!(bytes, b"clean");
    // A sealed executable cannot change even through its retained descriptor.
    let mut file = runtime.executable.try_clone().unwrap();
    assert!(std::io::Write::write_all(&mut file, b"changed").is_err());
}
#[test]
fn runtime_selection_refuses_unpinned_oversized_writable_and_symlink_inputs() {
    let (dir, path, sha) = selected();
    assert!(PinnedRuntime::pin(&path, &"0".repeat(64), MAX_EXECUTABLE).is_err());
    assert!(PinnedRuntime::pin(&path, &sha, 3).is_err());
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(PinnedRuntime::pin(&link, &sha, MAX_EXECUTABLE).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(PinnedRuntime::pin(&path, &sha, MAX_EXECUTABLE).is_err());
    let script = dir.path().join("script");
    std::fs::write(&script, b"#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o500)).unwrap();
    let sha = format!("{:x}", Sha256::digest(std::fs::read(&script).unwrap()));
    assert!(PinnedRuntime::pin(&script, &sha, MAX_EXECUTABLE).is_err());
}
#[test]
fn subprocess_failures_timeouts_and_excess_output_never_return_evidence() {
    let (_dir, path, sha) = selected();
    let runtime = PinnedRuntime::pin(&path, &sha, MAX_EXECUTABLE).unwrap();
    for (name, timeout, cap) in [
        ("nonzero", Duration::from_secs(10), 4096),
        ("output", Duration::from_secs(10), 512),
        ("timeout", Duration::from_millis(100), 4096),
    ] {
        assert_eq!(runtime.run(&args(name), timeout, cap), Err(RuntimeError));
    }
}

#[test]
fn exited_group_leader_does_not_leave_descendants_or_output_pipes_running() {
    let (dir, path, sha) = selected();
    let runtime = PinnedRuntime::pin(&path, &sha, MAX_EXECUTABLE).unwrap();
    let pid_file = dir.path().join("descendant.pid");
    let args = vec!["descendant".into(), pid_file.clone().into_os_string()];
    assert_eq!(
        runtime.run(&args, Duration::from_secs(1), 4096),
        Err(RuntimeError)
    );
    let pid: u32 = std::fs::read_to_string(pid_file).unwrap().parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Ok(stat) if stat.split_once(") ").unwrap().1.starts_with("Z ") => break,
            _ => {
                assert!(
                    Instant::now() < deadline,
                    "the subprocess group must be terminated"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}
