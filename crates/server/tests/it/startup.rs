use std::process::{Command, Output};

fn run(settings: &[(&str, &str)], check: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
    command.env_clear();
    for (key, value) in settings {
        command.env(key, value);
    }
    if check {
        command.arg("--check");
    }
    command.output().unwrap()
}

#[test]
fn smoke_check_accepts_defaults_and_valid_integration_configuration() {
    for settings in [
        vec![],
        vec![
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "secret-sentinel"),
            ("NK_HTTP_HOST", "::1"),
            ("NK_HTTP_PORT", "4444"),
        ],
    ] {
        let output = run(&settings, true);
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("bootstrap ready"));
        assert!(!stderr.contains("secret-sentinel"));
    }
}

#[test]
fn startup_and_smoke_check_fail_before_readiness_for_invalid_configuration() {
    for settings in [
        vec![("NK_NOTION_AUTH", "integration")],
        vec![("NK_NOTION_AUTH", "integration"), ("NOTION_TOKEN", "")],
        vec![
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "secret sentinel"),
        ],
        vec![("NK_NOTION_AUTH", "secret-sentinel")],
        vec![("NK_HTTP_HOST", "secret-sentinel")],
        vec![("NK_HTTP_PORT", "secret-sentinel")],
    ] {
        for check in [false, true] {
            let output = run(&settings, check);
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(stderr.contains("Configuration error:"));
            assert!(!stderr.contains("bootstrap ready"));
            assert!(!stderr.contains("secret"));
        }
    }
}

#[test]
fn one_shot_diagnostics_expose_only_identity_and_honest_dependency_states() {
    let sentinel = "https://private.example/file?X-Amz-Signature=secret-sentinel";
    for (auth, expected) in [("none", "unconfigured"), ("integration", "unavailable")] {
        let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .env_clear()
            .env("NK_NOTION_AUTH", auth)
            .env("NOTION_TOKEN", sentinel)
            .arg("--diagnostics")
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("private.example"));
        assert!(!text.contains("secret-sentinel"));
        assert!(output.stderr.is_empty());
        let report: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            report,
            serde_json::json!({
                "server": {"name": "notion-knowledge", "version": env!("CARGO_PKG_VERSION")},
                "transport": "one-shot", "status": "degraded",
                "access": {"read_only": true},
                "dependencies": {"notion": expected, "index": "unavailable"}
            })
        );
    }
}

#[test]
fn writable_mode_requires_explicit_opt_in_and_is_reported() {
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .env("NK_READ_ONLY", "false")
        .arg("--diagnostics")
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["access"]["read_only"], false);
    assert!(output.stderr.is_empty());

    for invalid in ["", "0", "FALSE", "private-sentinel"] {
        let check = run(&[("NK_READ_ONLY", invalid)], true);
        assert_eq!(check.status.code(), Some(2));
        let error = String::from_utf8(check.stderr).unwrap();
        assert!(error.contains("NK_READ_ONLY"));
        assert!(!error.contains("private-sentinel"));
    }
}

#[test]
fn release_identity_is_available_without_runtime_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .env("NK_HTTP_PORT", "invalid")
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("notion-knowledge {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn identity_probe_requires_explicit_integration_configuration_without_serving() {
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .arg("--notion-identity")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires NK_NOTION_AUTH=integration"));
    assert!(!stderr.contains("bootstrap ready"));
}

#[test]
fn discovery_dry_run_validates_explicit_roots_and_auth_before_starting_a_transport() {
    for args in [
        vec!["--crawl-dry-run"],
        vec!["--crawl-dry-run", "private-invalid-root"],
        vec!["--crawl-dry-run", "00000000-0000-0000-0000-000000000001"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .env_clear()
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let diagnostic = String::from_utf8(output.stderr).unwrap();
        assert!(!diagnostic.contains("private-invalid-root"));
        assert!(!diagnostic.contains("Serving MCP"));
        assert!(!diagnostic.contains("bootstrap ready"));
    }
}

#[test]
fn invalid_exclusion_options_fail_before_credentials_or_network_and_do_not_echo_values() {
    for options in [
        vec!["--exclude-page", "private-invalid-id"],
        vec!["--exclude-descendants"],
        vec!["--exclude-source-type", "private-unknown-type"],
        vec!["--unknown-rule"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .env_clear()
            .args(["--crawl-dry-run", "00000000000000000000000000000001"])
            .args(options)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("Invalid discovery scope"));
        assert!(!stderr.contains("private"));
        assert!(!stderr.contains("Discovery requires"));
    }
}

#[test]
fn ingestion_admission_fails_at_startup_without_echoing_values() {
    for settings in [
        vec![("NK_MAX_UPLOAD_FILES", "secret-invalid-count")],
        vec![("NK_MAX_UPLOAD_FILES", "0")],
        vec![("NK_MAX_UPLOAD_FILES", "17")],
        vec![("NK_FILE_INGESTION", "true")],
        vec![("NK_FILE_SOURCE_HOSTS", "secret-host-value")],
        vec![("NK_FILE_INGESTION", "invalid-secret")],
    ] {
        for check in [false, true] {
            let output = run(&settings, check);
            assert_eq!(output.status.code(), Some(2));
            let error = String::from_utf8(output.stderr).unwrap();
            assert!(error.contains("NK_"));
            for sentinel in [
                "secret-invalid-count",
                "secret-host-value",
                "invalid-secret",
            ] {
                assert!(!error.contains(sentinel));
            }
            assert!(output.stdout.is_empty());
        }
    }
}

#[cfg(unix)]
#[test]
fn check_validates_enabled_ingestion_bounds_and_private_audit_path_before_serving() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("audit.sqlite");
    let settings = vec![
        ("NK_FILE_INGESTION", "true"),
        ("NK_READ_ONLY", "false"),
        ("NK_NOTION_AUTH", "integration"),
        ("NOTION_TOKEN", "secret-sentinel"),
        (
            "NK_NOTION_SCOPE_ROOTS",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ),
        (
            "NK_NOTION_SCOPE_WORKSPACE_ID",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ),
        ("NK_FILE_SOURCE_HOSTS", "files.example.test"),
        ("NK_FILE_AUDIT_FILE", path.to_str().unwrap()),
    ];
    let output = run(&settings, true);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !path.exists(),
        "check must not initialize durable runtime storage"
    );
    for (key, value) in [
        ("NK_FILE_TIMEOUT_SECONDS", "0"),
        ("NK_FILE_PLAN", "secret-invalid-plan"),
        ("NK_FILE_MAX_BYTES", "5242881"),
        ("NK_FILE_AUDIT_RETENTION_DAYS", "3651"),
        (
            "NK_FILE_SOURCE_HOSTS",
            "https://private-sentinel.test/?token=secret-sentinel",
        ),
        ("NK_FILE_AUDIT_FILE", "/tmp/private-sentinel.sqlite"),
        ("NK_FILE_MULTIPART_THRESHOLD_BYTES", "5242880"),
    ] {
        let mut invalid = settings.clone();
        invalid.retain(|(name, _)| *name != key);
        invalid.push((key, value));
        for check in [false, true] {
            let output = run(&invalid, check);
            assert_eq!(output.status.code(), Some(2), "{key}");
            assert!(output.stdout.is_empty());
            let diagnostic = String::from_utf8(output.stderr).unwrap();
            assert!(diagnostic.contains(key));
            for secret in ["private-sentinel", "secret-sentinel", "secret-invalid-plan"] {
                assert!(!diagnostic.contains(secret));
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn removed_oauth_settings_and_old_grant_files_warn_once_and_never_block_startup() {
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let key = directory.path().join("key");
    let state = directory.path().join("state");
    // Old sealed grant files are left on disk and never read or decrypted.
    std::fs::write(&key, b"key-private-sentinel").unwrap();
    std::fs::write(&state, b"corrupt-private-sentinel").unwrap();
    let removed = [
        ("NK_OAUTH_ISSUER", "https://auth.example.com"),
        ("NK_OAUTH_RESOURCE", "https://knowledge.example.com/mcp"),
        ("NK_OAUTH_CLIENT_ID", "chatgpt"),
        (
            "NK_OAUTH_CLIENT_REDIRECT_URI",
            "https://chatgpt.example.com/callback",
        ),
        ("NK_OAUTH_LOCAL_HTTP", "true"),
        ("NK_NOTION_OAUTH_CLIENT_SECRET", "client-private-sentinel"),
        ("NK_NOTION_GRANT_KEY_FILE", key.to_str().unwrap()),
        ("NK_NOTION_GRANT_STATE_FILE", state.to_str().unwrap()),
    ];
    let execute = |arguments: &[&str]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
        command.env_clear().args(arguments).stdin(Stdio::null());
        for (name, value) in removed {
            command.env(name, value);
        }
        command.output().unwrap()
    };
    let output = execute(&["--check"]);
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let warnings: Vec<_> = stderr
        .lines()
        .filter(|line| line.contains("ignoring removed settings"))
        .collect();
    assert_eq!(warnings.len(), 1, "{stderr}");
    for (name, _) in removed {
        assert!(warnings[0].contains(name), "{name}");
    }
    assert!(warnings[0].contains("may be deleted"));
    assert!(stderr.contains("bootstrap ready"));
    assert!(!stderr.contains("private"));
    assert!(!stderr.contains(directory.path().to_str().unwrap()));

    for argument in ["--notion-oauth-forget", "--notion-oauth-identity"] {
        let output = execute(&[argument]);
        assert_eq!(output.status.code(), Some(2), "{argument}");
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("no longer supported")
        );
    }
    assert_eq!(std::fs::read(&key).unwrap(), b"key-private-sentinel");
    assert_eq!(std::fs::read(&state).unwrap(), b"corrupt-private-sentinel");
}
