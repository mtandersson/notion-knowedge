//! Shipping executable dispatch must remain exact and redact invalid inputs.
use std::process::Command;

#[test]
fn index_operator_rejects_mixed_unknown_and_payload_arguments_before_configuration() {
    for args in [
        vec!["--index"],
        vec!["--index", "unknown-sensitive"],
        vec!["--index", "ensure", "--http"],
        vec!["--version", "--index", "rebuild"],
        vec!["--index", "ensure", "--reconcile", "dry-run"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .env_clear()
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, b"Index argument error.\n");
    }
}

#[test]
fn index_operator_refuses_missing_model_before_creating_state_or_serving() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .args(["--index", "ensure"])
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
    #[cfg(all(unix, feature = "webhook-worker"))]
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"operation":"index","status":"model_configuration"})
    );
    #[cfg(not(all(unix, feature = "webhook-worker")))]
    assert!(output.stdout.is_empty());
}
