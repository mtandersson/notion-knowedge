//! Real private HTTP composition; the shared local provider needs no OAuth login.
use super::installed_writes::{call, connect, rpc};
use super::*;
use crate::{
    index_runtime::{IndexRuntime, OpenMode},
    webhook_worker::runtime::Worker,
};
use notion_knowledge_core::{
    backend::PageId, lifecycle::LifecycleScope, reconciliation::ReconciliationJournal,
};
use notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};
use tokio_util::sync::CancellationToken;

const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const FOREIGN: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const TOKEN: &str = "private-integration-local-http-token";
fn parse(values: &BTreeMap<String, String>) -> crate::config::Config {
    crate::config::Config::from_lookup(|key| values.get(key).map(Into::into)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_integration_http_guards_writes_and_refreshes_once_without_oauth() {
    let scope = LifecycleScope {
        workspace_id: WORKSPACE.into(),
        roots: vec![PageId(ROOT.into())],
        generation: 1,
        exclusions: Default::default(),
    };
    let h = Harness::with_content(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        None,
        Duration::ZERO,
        Some(scope.clone()),
    )
    .await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    *h.document.lock().unwrap() = Some(("Saffron private evidence".into(), 0));
    let seed = content_index::seed(h._dir.path(), &scope, PAGE).await;
    let mut values: BTreeMap<String, String> = [
        ("NK_NOTION_AUTH", "integration"),
        ("NOTION_TOKEN", "provider-access-secret"),
        ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
        ("NK_NOTION_SCOPE_ROOTS", ROOT),
        ("NK_HTTP_AUTH", "bearer"),
        ("NK_HTTP_BEARER_TOKEN", TOKEN),
        ("NK_READ_ONLY", "false"),
        ("NK_RETRIEVAL", "true"),
        ("NK_INTEGRATION_WRITES", "true"),
        ("NK_RECONCILIATION_INTERVAL_SECONDS", "3600"),
        ("NK_WEBHOOK_POLL_MS", "20"),
        ("NK_WEBHOOK_TIMEOUT_SECONDS", "10"),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.into()))
    .collect();
    let ledger = h._dir.path().join("integration-ledger.sqlite");
    let audit = h._dir.path().join("integration-audit.sqlite");
    for (key, path) in [
        ("NK_WEBHOOK_STATE_FILE", seed.state.clone()),
        ("NK_WEBHOOK_INDEX_DIR", seed.index.clone()),
        ("NK_WEBHOOK_QWEN_ASSETS", h._dir.path().join("unused-model")),
        ("NK_INTEGRATION_WRITE_LEDGER_FILE", ledger.clone()),
        ("NK_INTEGRATION_WRITE_AUDIT_FILE", audit.clone()),
    ] {
        values.insert(key.into(), path.to_str().unwrap().into());
    }
    for (key, replacement) in [
        ("NK_READ_ONLY", "true"),
        ("NK_INTEGRATION_WRITES", "false"),
        ("NK_HTTP_AUTH", "none"),
        ("NK_RETRIEVAL", "false"),
    ] {
        let mut bad = values.clone();
        bad.insert(key.into(), replacement.into());
        assert!(
            crate::config::Config::from_lookup(|k| bad.get(k).map(Into::into)).is_err(),
            "{key}"
        );
    }
    for key in [
        "NK_INTEGRATION_WRITE_LEDGER_FILE",
        "NK_INTEGRATION_WRITE_AUDIT_FILE",
        "NK_RECONCILIATION_INTERVAL_SECONDS",
    ] {
        let mut bad = values.clone();
        bad.remove(key);
        assert!(
            crate::config::Config::from_lookup(|k| bad.get(k).map(Into::into)).is_err(),
            "{key}"
        );
    }
    assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
    let mut config = parse(&values);
    drop(SqliteIdempotencyStore::initialize_new(&ledger).unwrap());
    let source = Arc::new(
        notion_knowledge_notion::NotionClient::integration("provider-access-secret")
            .unwrap()
            .with_loopback_fixture(h.provider_port),
    );
    let index = IndexRuntime::compose(
        config.retrieval.as_ref().unwrap(),
        &seed.state,
        scope,
        source.clone(),
        seed.provider,
        OpenMode::ExistingWrite,
    )
    .await
    .unwrap();
    let scheduled = Arc::new(crate::scheduled_reconciliation::runtime::Worker {
        index,
        settings: config.reconciliation_schedule.clone().unwrap(),
        poll: Duration::from_millis(20),
        timeout: Duration::from_secs(10),
        inventory_timeout: Duration::from_secs(30),
    });
    let worker = Arc::new(Worker {
        webhook_enabled: false,
        scheduled: Some(scheduled.clone()),
        state: scheduled.index.coordinator.state().clone(),
        scope: notion_knowledge_core::webhook::InboxScope {
            workspace_id: WORKSPACE.into(),
            subscription_id: "periodic".into(),
        },
        integration: String::new(),
        coordinator: scheduled.index.coordinator.clone(),
        handler: scheduled.index.handler.clone(),
        provider: scheduled.index.provider.clone(),
        poll: Duration::from_millis(20),
        timeout: Duration::from_secs(10),
        log_enabled: false,
    });
    let handler = crate::integration_writes::compose(
        notion_knowledge_mcp::KnowledgeServer::default(),
        &config,
        &worker,
        source,
    )
    .await
    .unwrap();
    let pause = worker
        .state
        .acquire_lease(crate::webhook_worker::runtime::now_ms() / 1000, 120)
        .unwrap();
    let now = crate::webhook_worker::runtime::now_ms() / 1000;
    let policy = &scheduled.settings;
    // Keep the full inventory scheduled for later. Mutation refresh must run
    // promptly between periodic inventories, with webhook ingress disabled.
    worker
        .coordinator
        .register_schedule(
            &pause,
            now,
            crate::scheduled_reconciliation::runtime::ID,
            notion_knowledge_retrieval::schedule::SchedulePolicy::new(
                policy.interval,
                policy.attempts,
                policy.backoff,
                policy.maximum_backoff,
            )
            .unwrap(),
            now + policy.interval,
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.http_bind = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{}/mcp", config.http_bind);
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let shared = worker.clone();
    let serving = tokio::spawn(async move {
        crate::http::serve_installed(config, handler, Some(shared), server_stop).await
    });
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(
            endpoint
                .trim_start_matches("http://")
                .trim_end_matches("/mcp"),
        )
        .await
        .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let source_reads = h.page_reads.load(Ordering::SeqCst);
    let denied=rpc(&h,&endpoint,"incorrect",None,json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"denied","version":"1"}}})).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(h.page_reads.load(Ordering::SeqCst), source_reads);
    assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
    let session = connect(&h, &endpoint, TOKEN).await;
    let listing = resource::reply(
        rpc(
            &h,
            &endpoint,
            TOKEN,
            Some(&session),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .await,
    )
    .await;
    let names: Vec<_> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"knowledge_create_page") && names.contains(&"knowledge_append"),
        "{listing}"
    );
    assert!(!names.contains(&"knowledge_update_section"));
    let rejected=call(&h,&endpoint,TOKEN,&session,"knowledge_append",json!({"root_page_id":ROOT,"page_id":FOREIGN,"markdown":"denied","expected_last_edited_time":"2026-10-17T00:00:00Z","idempotency_key":"integration-denied-append"})).await;
    assert_eq!(
        rejected["result"]["structuredContent"]["write"]["status"], "not_authorized",
        "{rejected}"
    );
    assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
    let create = json!({"root_page_id":ROOT,"parent_page_id":ROOT,"title":"Saffron private evidence","markdown":"Saffron integration created content","idempotency_key":"integration-create-once"});
    let created = call(
        &h,
        &endpoint,
        TOKEN,
        &session,
        "knowledge_create_page",
        create.clone(),
    )
    .await;
    assert_eq!(
        created["result"]["structuredContent"]["write"]["status"], "verified",
        "{created}"
    );
    let revision = created["result"]["structuredContent"]["write"]["last_edited_time"]
        .as_str()
        .unwrap();
    let append = json!({"root_page_id":ROOT,"page_id":PAGE,"markdown":"\nUnique appended content\n","expected_last_edited_time":revision,"idempotency_key":"integration-append-once"});
    let appended = call(
        &h,
        &endpoint,
        TOKEN,
        &session,
        "knowledge_append",
        append.clone(),
    )
    .await;
    assert_eq!(
        appended["result"]["structuredContent"]["write"]["status"], "verified",
        "{appended}"
    );
    for (tool, args) in [
        ("knowledge_create_page", create),
        ("knowledge_append", append),
    ] {
        let replay = call(&h, &endpoint, TOKEN, &session, tool, args).await;
        assert_eq!(
            replay["result"]["structuredContent"]["write"]["replayed"], true,
            "{replay}"
        );
    }
    assert_eq!(h.mutations.load(Ordering::SeqCst), 2);
    worker
        .state
        .release_lease(&pause, crate::webhook_worker::runtime::now_ms() / 1000)
        .unwrap();
    let mut found = false;
    for _ in 0..100 {
        let result = call(
            &h,
            &endpoint,
            TOKEN,
            &session,
            "knowledge_search",
            json!({"query":"Unique appended","limit":3,"mode":"lexical"}),
        )
        .await;
        if result.to_string().contains("Unique appended content") {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(found, "shared worker did not publish the append");
    assert_eq!(h.mutations.load(Ordering::SeqCst), 2);
    let ledger = rusqlite::Connection::open(ledger).unwrap();
    assert_eq!(
        ledger
            .query_row(
                "SELECT count(*) FROM mutation_claims WHERE status='committed'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    let audit = rusqlite::Connection::open(audit).unwrap();
    assert!(audit.query_row("SELECT count(*) FROM agent_audit_events WHERE actor='automation' AND outcome='succeeded'",[],|r|r.get::<_,i64>(0)).unwrap()>=2);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0, "no OAuth token exchange");
    assert_eq!(
        h.probes.load(Ordering::SeqCst),
        0,
        "no OAuth grant introspection"
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
