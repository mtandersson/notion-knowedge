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
