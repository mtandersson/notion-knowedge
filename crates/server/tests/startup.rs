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
