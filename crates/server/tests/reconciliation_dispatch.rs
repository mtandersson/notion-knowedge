//! Test the real binary's operator selection before configuration or transports.
use std::process::Command;
#[test]
fn manual_dispatch_rejects_unknown_mixed_and_invalid_arguments_without_echoing_input() {
    for args in [
        vec!["--reconcile"],
        vec!["--reconcile", "unknown"],
        vec!["--reconcile", "dry-run", "--http"],
        vec!["--version", "--reconcile", "status", "run"],
        vec!["--webhook-status", "--reconcile", "status", "run"],
        vec!["--reconcile", "apply", "secret?token=fixture-sensitive"],
        vec!["--reconcile", "retry", "run", "fixture-sensitive"],
        vec!["--reconcile-typo", "fixture-sensitive"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .env_clear()
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
        let diagnostic = String::from_utf8(output.stderr).unwrap();
        assert_eq!(diagnostic, "Reconciliation argument error.\n");
    }
}
#[test]
fn manual_configuration_failure_does_not_start_stdio_or_create_local_paths() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .args(["--reconcile", "dry-run"])
        .env("NK_NOTION_AUTH", "integration")
        .env("NOTION_TOKEN", "fixture-sensitive")
        .env(
            "NK_NOTION_SCOPE_WORKSPACE_ID",
            "44444444-4444-4444-4444-444444444444",
        )
        .env(
            "NK_NOTION_SCOPE_ROOTS",
            "22222222-2222-2222-2222-222222222222",
        )
        .env("NK_WEBHOOK_INDEX_DIR", dir.path().join("index"))
        .env("NK_WEBHOOK_STATE_FILE", dir.path().join("state.sqlite"))
        .env("NK_WEBHOOK_QWEN_ASSETS", dir.path().join("missing-assets"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!all.contains("fixture-sensitive"));
    assert!(!all.contains("Serving MCP"));
    #[cfg(all(unix, feature = "webhook-worker"))]
    assert!(all.contains("model_configuration"));
}
