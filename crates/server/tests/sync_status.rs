use notion_knowledge_core::{sync_status::SyncStatusSource, webhook::*};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use notion_knowledge_server::{config::Config, sync_status::LocalStatus};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
const W: &str = "abcdefab-1111-1111-1111-111111111111";
const S: &str = "abcdefac-2222-2222-2222-222222222222";
const I: &str = "abcdefad-3333-3333-3333-333333333333";
fn config(path: &std::path::Path) -> Config {
    Config::from_lookup(|name| match name {
        "NK_NOTION_AUTH" => Some("integration".into()),
        "NOTION_TOKEN" => Some("notion-token-sentinel".into()),
        "NK_WEBHOOK_MODE" => Some("verified".into()),
        "NK_WEBHOOK_WORKSPACE_ID" => Some(W.into()),
        "NK_WEBHOOK_SUBSCRIPTION_ID" => Some(S.into()),
        "NK_WEBHOOK_INTEGRATION_ID" => Some(I.into()),
        "NK_WEBHOOK_VERIFICATION_TOKEN" => Some("private-token-sentinel".into()),
        "NK_WEBHOOK_STATE_FILE" => Some(path.as_os_str().into()),
        _ => None,
    })
    .unwrap()
}
fn event(n: u8) -> WebhookEvent {
    WebhookEvent {
        id: format!("aaaaaaaa-aaaa-aaaa-aaaa-{n:012}"),
        workspace_id: W.into(),
        subscription_id: S.into(),
        integration_id: I.into(),
        timestamp: "2026-10-13T10:00:00Z".into(),
        event_type: "page.created".into(),
        entity_id: "44444444-4444-4444-4444-444444444444".into(),
        entity_type: "page".into(),
        attempt_number: 1,
    }
}
async fn wire(config: &Config) -> Vec<Value> {
    let handler = notion_knowledge_server::root_scope::attach_root_scope(
        notion_knowledge_mcp::KnowledgeServer::default(),
        config,
    )
    .unwrap()
    .with_capability_policy(config.capability_policy.clone());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        handler
            .serve(server)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);
    let mut responses = vec![];
    for request in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"status-contract","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_sync_status","arguments":{}}}),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"knowledge_sync_status","arguments":{"workspace":"private-payload-sentinel"}}}),
    ] {
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        responses.push(serde_json::from_str(&line).unwrap());
        if request["id"] == 1 {
            write
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                .await
                .unwrap();
        }
    }
    drop(write);
    task.abort();
    responses
}
#[tokio::test]
async fn installed_mcp_reports_current_scoped_states_after_restart_without_writes_or_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let store = SqliteSyncStateStore::open(&path).unwrap();
    for n in 1..=4 {
        store.receive(&event(n)).unwrap();
    }
    let scope = InboxScope {
        workspace_id: W.into(),
        subscription_id: S.into(),
    };
    let (claim, _) = store.claim(&scope, 10, 30).unwrap().unwrap();
    store
        .finish(&claim, 10, Some(EventFailure::Source))
        .unwrap();
    store.claim(&scope, 10, 30).unwrap().unwrap();
    let mut foreign = event(5);
    foreign.subscription_id = "55555555-5555-5555-5555-555555555555".into();
    store.receive(&foreign).unwrap();
    foreign = event(6);
    foreign.workspace_id = "66666666-6666-6666-6666-666666666666".into();
    store.receive(&foreign).unwrap();
    foreign = event(7);
    foreign.integration_id = "77777777-7777-7777-7777-777777777777".into();
    store.receive(&foreign).unwrap();
    drop(store);
    let before = std::fs::read(&path).unwrap();
    let names = std::fs::read_dir(dir.path()).unwrap().count();
    let responses = wire(&config(&path)).await;
    let catalog = responses[1]["result"]["tools"].as_array().unwrap();
    let tool = catalog
        .iter()
        .find(|t| t["name"] == "knowledge_sync_status")
        .unwrap();
    assert_eq!(tool["annotations"]["readOnlyHint"], true);
    let result = &responses[2]["result"]["structuredContent"];
    assert_eq!(
        result["webhooks"],
        json!({"pending":2,"running":1,"failed":1})
    );
    assert_eq!(result["operational_schema_version"], 13);
    assert!(result["index"].is_null());
    assert!(result["last_successful_crawl_unix_seconds"].is_null());
    assert!(result["last_successful_reconciliation_unix_seconds"].is_null());
    assert!(responses[3]["error"].is_object());
    for marker in [
        "private-token-sentinel",
        "private-payload-sentinel",
        W,
        S,
        I,
    ] {
        assert!(!serde_json::to_string(&responses).unwrap().contains(marker));
    }
    assert_eq!(before, std::fs::read(&path).unwrap());
    assert_eq!(names, std::fs::read_dir(dir.path()).unwrap().count());
    assert_eq!(
        wire(&config(&path)).await[2]["result"]["structuredContent"],
        *result
    );
    let mut uppercase = config(&path);
    if let notion_knowledge_server::webhook::WebhookConfig::Verified {
        workspace_id,
        subscription_id,
        integration_id,
        ..
    } = &mut uppercase.webhook
    {
        workspace_id.make_ascii_uppercase();
        subscription_id.make_ascii_uppercase();
        integration_id.make_ascii_uppercase();
    }
    assert_eq!(
        wire(&uppercase).await[2]["result"]["structuredContent"],
        *result
    );
    assert_eq!(before, std::fs::read(&path).unwrap());
}
#[tokio::test]
async fn missing_or_legacy_or_wal_state_is_honestly_unavailable_and_never_created_or_migrated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.sqlite");
    let source = LocalStatus::from_config(&config(&path));
    assert_eq!(source.status().await, Default::default());
    assert!(!path.exists());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE private_payload(value TEXT); INSERT INTO private_payload VALUES('secret-sentinel')").unwrap();
    drop(connection);
    let before = std::fs::read(&path).unwrap();
    assert_eq!(source.status().await, Default::default());
    assert_eq!(before, std::fs::read(&path).unwrap());
    std::fs::remove_file(&path).unwrap();
    drop(SqliteSyncStateStore::open(&path).unwrap());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("PRAGMA journal_mode=WAL;")
        .unwrap();
    drop(connection);
    let names = std::fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(source.status().await, Default::default());
    assert_eq!(names, std::fs::read_dir(dir.path()).unwrap().count());
}

#[cfg(all(unix, feature = "webhook-worker"))]
fn snapshot(path: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn read(
        root: &std::path::Path,
        dir: &std::path::Path,
        out: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for item in std::fs::read_dir(dir).unwrap() {
            let item = item.unwrap();
            if item.file_type().unwrap().is_dir() {
                read(root, &item.path(), out);
            } else {
                out.insert(
                    item.path().strip_prefix(root).unwrap().into(),
                    std::fs::read(item.path()).unwrap(),
                );
            }
        }
    }
    let mut out = Default::default();
    read(path, path, &mut out);
    out
}
#[cfg(all(unix, feature = "webhook-worker"))]
#[tokio::test]
async fn installed_status_validates_real_index_metadata_without_model_assets_or_startup_repairs() {
    use notion_knowledge_retrieval::{
        chunks::LanceChunkTable,
        commit::{CommitBinding, IndexCommitCoordinator},
    };
    for compatible in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.sqlite");
        let index = dir.path().join("index");
        std::fs::create_dir(&index).unwrap();
        let mut config = config(&state);
        let scope = notion_knowledge_core::lifecycle::LifecycleScope {
            workspace_id: W.into(),
            generation: 1,
            roots: vec![notion_knowledge_core::backend::PageId(
                "44444444-4444-4444-4444-444444444444".into(),
            )],
            exclusions: Default::default(),
        };
        let journal = notion_knowledge_retrieval::planner::production_scope(&scope).unwrap();
        let binding = CommitBinding::new("chunks", W, &journal, "1", 1).unwrap();
        let coordinator = IndexCommitCoordinator::initialize(&index, &state, binding).unwrap();
        let metadata = if compatible {
            notion_knowledge_retrieval::qwen::metadata()
        } else {
            notion_knowledge_core::embedding::EmbeddingMetadata::new(
                "private-provider-sentinel".into(),
                "private-model-sentinel".into(),
                "secret-version-sentinel".into(),
                3,
            )
            .unwrap()
        };
        let table = LanceChunkTable::create(&index, "chunks", metadata)
            .await
            .unwrap();
        drop(table);
        if compatible {
            use notion_knowledge_core::reconciliation::ReconciliationJournal;
            let lease = coordinator.state().acquire_lease(10, 100).unwrap();
            for (run, completed_at, clean) in [
                ("success", 20, true),
                ("held", 30, false),
                ("incomplete", 40, false),
            ] {
                coordinator
                    .state()
                    .start_or_resume(&lease, 10, run, &journal)
                    .unwrap();
                let mut plan = notion_knowledge_retrieval::planner::ReconciliationPlan {
                    binding: coordinator.binding().clone(),
                    index_version: 1,
                    local: Default::default(),
                    pages: vec![],
                    errors: Default::default(),
                    run_errors: vec![],
                    scanned: 0,
                    inspected_ids: Default::default(),
                    traversal_complete: run != "incomplete",
                };
                if !clean && run == "held" {
                    plan.errors.insert(
                        "private-page-sentinel".into(),
                        notion_knowledge_retrieval::planner::PlanError::Permission,
                    );
                }
                coordinator
                    .record_reconciliation_plan(&lease, 10, run, &plan)
                    .unwrap();
                coordinator
                    .complete_reconciliation_plan(&lease, completed_at, run)
                    .unwrap();
            }
            coordinator.state().release_lease(&lease, 40).unwrap();
        }
        drop(coordinator);
        config.notion_scope = Some(scope);
        config.webhook_worker = Some(notion_knowledge_server::webhook_worker::WorkerSettings {
            index: index.clone(),
            assets: dir.path().join("no-model-assets"),
            poll_ms: 10,
            timeout_seconds: 1,
        });
        let before = snapshot(dir.path());
        let responses = wire(&config).await;
        let result = &responses[2]["result"]["structuredContent"];
        if compatible {
            assert!(result["index"]["version"].as_u64().unwrap() > 0);
            assert_eq!(result["last_successful_reconciliation_unix_seconds"], 20);
            assert_eq!(result["index"]["chunk_schema_version"], 1);
            assert_eq!(result["index"]["canonical_schema_version"], 1);
            assert_eq!(result["index"]["model"], "qwen3_embedding06b");
            assert_eq!(
                result["index"]["model_version"],
                notion_knowledge_retrieval::qwen::metadata().model_version()
            );
        } else {
            assert!(result["index"].is_null());
        }
        assert_eq!(before, snapshot(dir.path()));
        assert!(!config.webhook_worker.as_ref().unwrap().assets.exists());
        let serialized = serde_json::to_string(&responses).unwrap();
        for marker in [
            W,
            "private-provider-sentinel",
            "private-model-sentinel",
            "secret-version-sentinel",
            "notion-token-sentinel",
        ] {
            assert!(!serialized.contains(marker));
        }
        assert_eq!(
            wire(&config).await[2]["result"]["structuredContent"],
            *result
        );
        config.notion_scope.as_mut().unwrap().generation = 2;
        assert!(wire(&config).await[2]["result"]["structuredContent"]["index"].is_null());
        assert_eq!(before, snapshot(dir.path()));
    }
}

#[tokio::test]
async fn sync_status_obeys_read_category_and_exact_deny_policy_on_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(&dir.path().join("absent.sqlite"));
    config.capability_policy = notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(
        Some("read"),
        Some("knowledge_sync_status"),
    )
    .unwrap();
    let responses = wire(&config).await;
    assert!(
        responses[1]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool["name"] != "knowledge_sync_status")
    );
    assert!(responses[2]["error"].is_object());
    assert!(!dir.path().join("absent.sqlite").exists());
}
