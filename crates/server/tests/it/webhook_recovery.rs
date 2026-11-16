use notion_knowledge_core::webhook::*;
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use std::process::Command;
const ID: &str = "13950b26-c203-4f3b-b97d-93ec06319565";
fn command(path: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .args(args)
        .arg(path)
        .output()
        .unwrap()
}
#[test]
fn production_operator_inspects_and_requeues_only_selected_failed_generation() {
    let path = std::env::temp_dir().join(format!(
        "nk247-cli-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = SqliteSyncStateStore::open(&path).unwrap();
    let event = WebhookEvent {
        id: ID.into(),
        timestamp: "2026-10-08T00:00:00Z".into(),
        workspace_id: ID.into(),
        subscription_id: ID.into(),
        integration_id: ID.into(),
        event_type: "page.content_updated".into(),
        entity_id: ID.into(),
        entity_type: "page".into(),
        attempt_number: 9,
    };
    store.receive(&event).unwrap();
    let scope = InboxScope {
        workspace_id: ID.into(),
        subscription_id: ID.into(),
    };
    let (claim, _) = store.claim(&scope, 1, 10).unwrap().unwrap();
    store.finish(&claim, 2, Some(EventFailure::Source)).unwrap();
    drop(store);
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .args(args)
            .output()
            .unwrap()
    };
    let file = path.to_str().unwrap();
    let listed = run(&["--webhook-failed", file, ID, ID, "1"]);
    assert!(listed.status.success());
    let report: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(report["events"][0]["processing_attempts"], 1);
    assert_eq!(report["events"][0]["failure"], "Source");
    let inspect = run(&["--webhook-inspect", file, ID, ID, ID]);
    assert!(inspect.status.success());
    let report: serde_json::Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(report["notion_attempt_number"], 9);
    let bad = run(&["--webhook-retry", file, ID, ID, ID, "1", "0", "1", "5"]);
    assert!(!bad.status.success());
    let extras = run(&[
        "--webhook-retry",
        file,
        ID,
        ID,
        ID,
        "1",
        "2",
        "1",
        "5",
        "--http",
    ]);
    assert!(!extras.status.success());
    let retry = run(&["--webhook-retry", file, ID, ID, ID, "1", "2", "1", "5"]);
    assert!(retry.status.success());
    assert!(
        !run(&["--webhook-retry", file, ID, ID, ID, "1", "2", "1", "5"])
            .status
            .success()
    );
    let store = SqliteSyncStateStore::open(&path).unwrap();
    let pending = store.event(&event.key()).unwrap().unwrap();
    assert_eq!(pending.state, EventState::Pending);
    assert_eq!(pending.cycle_attempts, 0);
    assert_eq!(pending.lifetime_attempts, 1);
    assert_eq!(pending.event, event);
    assert_eq!(store.finish(&claim, 3, None), Err(InboxError::ClaimLost));
    drop(store);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn typo_state_path_and_invalid_modes_do_not_create_state() {
    let path = std::env::temp_dir().join(format!("nk247-missing-{}.sqlite", std::process::id()));
    let file = path.to_str().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .args(["--webhook-failed", file, ID, ID, "10"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!path.exists());
    let output = command(file, &["--webhook-unknown"]);
    assert!(!output.status.success());
    assert!(!path.exists());
}
