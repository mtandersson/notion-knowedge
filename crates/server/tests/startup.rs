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

#[tokio::test]
async fn sealed_grant_startup_rejects_unsafe_state_before_check_or_transport() {
    use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
    use std::{fs, os::unix::fs::PermissionsExt, process::Stdio};

    // A version-1 compatibility fixture, not a production credential or key.
    // Fixed key/nonce are confined to test-only envelopes with fake tokens.
    fn envelope(record: &serde_json::Value) -> Vec<u8> {
        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[42u8; 32]).unwrap());
        let mut payload = serde_json::to_vec(record).unwrap();
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key([7u8; 12]),
            Aad::from(b"notion-knowledge/notion-oauth-grant/v1"),
            &mut payload,
        )
        .unwrap();
        let mut file = b"NKGRANT1".to_vec();
        file.extend_from_slice(&[7u8; 12]);
        file.extend_from_slice(&payload);
        file
    }
    async fn execute(settings: &[(&str, String)], arguments: &[&str]) -> Output {
        let mut command =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
        command
            .env_clear()
            .args(arguments)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        for (key, value) in settings {
            command.env(key, value);
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap()
    }
    let record = serde_json::json!({
        "schema":1, "grant_id":"0123456789abcdef0123456789abcdef", "epoch":1,
        "notion_client_id":"notion-client", "workspace_id":"workspace-private", "owner_user_id":"user-private",
        "bot_id":"bot-private", "issued_at_unix":1, "expires_at_unix":null,
        "access_token":"access-private-sentinel", "refresh_token":"refresh-private-sentinel",
    });
    let directory = tempfile::tempdir().unwrap();
    let key = directory.path().join("key");
    let state = directory.path().join("state");
    let settings = vec![
        ("NK_OAUTH_ISSUER", "https://auth.example.com".into()),
        (
            "NK_OAUTH_RESOURCE",
            "https://knowledge.example.com/mcp".into(),
        ),
        ("NK_OAUTH_CLIENT_ID", "chatgpt".into()),
        (
            "NK_OAUTH_CLIENT_REDIRECT_URI",
            "https://chatgpt.example.com/callback".into(),
        ),
        ("NK_NOTION_OAUTH_CLIENT_ID", "notion-client".into()),
        (
            "NK_NOTION_OAUTH_REDIRECT_URI",
            "https://auth.example.com/oauth/notion/callback".into(),
        ),
        (
            "NK_NOTION_OAUTH_CLIENT_SECRET",
            "client-private-sentinel".into(),
        ),
        ("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-private".into()),
        ("NK_NOTION_ALLOWED_USER_ID", "user-private".into()),
        ("NK_NOTION_GRANT_KEY_FILE", key.to_str().unwrap().into()),
        ("NK_NOTION_GRANT_STATE_FILE", state.to_str().unwrap().into()),
    ];
    fs::write(&key, [42u8; 32]).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    // Missing state is safe, and the independently constructed valid envelope
    // is accepted. Neither success establishes an authenticated MCP principal.
    assert!(execute(&settings, &["--check"]).await.status.success());
    fs::write(&state, envelope(&record)).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(execute(&settings, &["--check"]).await.status.success());

    for case in [
        "missing-key",
        "wrong-key",
        "corrupt-state",
        "foreign-workspace",
        "foreign-user",
        "foreign-client",
        "unknown-schema",
        "unsafe-state-mode",
    ] {
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&state, envelope(&record)).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(execute(&settings, &["--check"]).await.status.success());
        let mut changed = record.clone();
        match case {
            "foreign-workspace" => changed["workspace_id"] = "other-private-workspace".into(),
            "foreign-user" => changed["owner_user_id"] = "other-private-user".into(),
            "foreign-client" => changed["notion_client_id"] = "other-private-client".into(),
            "unknown-schema" => changed["schema"] = 2.into(),
            _ => {}
        }
        fs::write(&state, envelope(&changed)).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
        match case {
            "missing-key" => fs::remove_file(&key).unwrap(),
            "wrong-key" => fs::write(&key, [41u8; 32]).unwrap(),
            "corrupt-state" => fs::write(&state, b"corrupt-private-sentinel").unwrap(),
            "unsafe-state-mode" => {
                fs::set_permissions(&state, fs::Permissions::from_mode(0o644)).unwrap()
            }
            _ => {}
        }
        for arguments in [vec!["--check"], vec!["--http"], vec![]] {
            let output = execute(&settings, &arguments).await;
            assert_eq!(output.status.code(), Some(2), "{case}: {arguments:?}");
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(stderr.contains("Notion grant state error:"));
            assert!(!stderr.contains("bootstrap ready"));
            assert!(!stderr.contains("Serving MCP"));
            assert!(!stderr.contains("private"));
            assert!(!stderr.contains(directory.path().to_str().unwrap()));
        }
    }
}
