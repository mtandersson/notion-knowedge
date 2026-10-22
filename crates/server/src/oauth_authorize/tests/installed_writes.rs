//! Installed production factory and HTTP/SDK transport, with local providers only.
use super::*;
use notion_knowledge_core::{
    backend::PageId, lifecycle::LifecycleScope, sync_state::SyncStateStore,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tokio_util::sync::CancellationToken;
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: Default::default(),
    }
}
pub(super) fn config_values(h: &Harness, seed: &content_index::Seeded) -> BTreeMap<String, String> {
    let mut values = BTreeMap::<String, String>::new();
    for (key, value) in [
        ("NK_OAUTH_ISSUER", "https://auth.example.com"),
        ("NK_OAUTH_RESOURCE", "https://knowledge.example.com/mcp"),
        ("NK_NOTION_OAUTH_CLIENT_ID", "notion-client"),
        (
            "NK_NOTION_OAUTH_REDIRECT_URI",
            "https://auth.example.com/oauth/notion/callback",
        ),
        ("NK_NOTION_OAUTH_CLIENT_SECRET", "client-secret"),
        ("NK_NOTION_ALLOWED_WORKSPACE_ID", WORKSPACE),
        ("NK_NOTION_ALLOWED_USER_ID", "user-a"),
        ("NK_OAUTH_CLIENT_ID", "chatgpt"),
        ("NK_OAUTH_CLIENT_REDIRECT_URI", CLIENT_REDIRECT),
        ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
        ("NK_NOTION_SCOPE_ROOTS", ROOT),
        ("NK_OAUTH_WRITES", "true"),
        ("NK_READ_ONLY", "false"),
        ("NK_DESTRUCTIVE_WRITES", "true"),
        ("NK_RETRIEVAL", "true"),
        ("NK_WEBHOOK_POLL_MS", "100"),
        ("NK_WEBHOOK_TIMEOUT_SECONDS", "20"),
    ] {
        values.insert(key.into(), value.into());
    }
    for (key, path) in [
        ("NK_NOTION_GRANT_KEY_FILE", h._dir.path().join("key")),
        ("NK_NOTION_GRANT_STATE_FILE", h._dir.path().join("grant")),
        (
            "NK_OAUTH_WRITE_LEDGER_FILE",
            h._dir.path().join("write-ledger.sqlite"),
        ),
        (
            "NK_OAUTH_WRITE_AUDIT_FILE",
            h._dir.path().join("write-audit.sqlite"),
        ),
        ("NK_WEBHOOK_STATE_FILE", seed.state.clone()),
        ("NK_WEBHOOK_INDEX_DIR", seed.index.clone()),
        ("NK_WEBHOOK_QWEN_ASSETS", h._dir.path().join("unused-model")),
    ] {
        values.insert(key.into(), path.to_str().unwrap().into());
    }
    values
}
pub(super) fn config(h: &Harness, seed: &content_index::Seeded) -> Config {
    let values = config_values(h, seed);
    for absent in [
        "NK_OAUTH_WRITE_LEDGER_FILE",
        "NK_OAUTH_WRITE_AUDIT_FILE",
        "NK_READ_ONLY",
        "NK_RETRIEVAL",
    ] {
        assert!(
            Config::from_lookup(|key| if key == absent {
                None
            } else {
                values.get(key).map(Into::into)
            })
            .is_err(),
            "missing {absent}"
        );
    }
    Config::from_lookup(|key| values.get(key).map(Into::into)).unwrap()
}
pub(super) async fn rpc(
    h: &Harness,
    url: &str,
    token: &str,
    session: Option<&str>,
    message: Value,
) -> reqwest::Response {
    let mut request = h
        .client
        .post(url)
        .bearer_auth(token)
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-03-26")
        .json(&message);
    if let Some(session) = session {
        request = request.header("mcp-session-id", session);
    }
    request.send().await.unwrap()
}
pub(super) async fn connect(h: &Harness, url: &str, token: &str) -> String {
    let response = rpc(h,url,token,None,json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{"elicitation":{"form":{}}},"clientInfo":{"name":"installed-oauth-fixture","version":"1"}}})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let session = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(resource::reply(response).await["id"], 1);
    assert!(
        rpc(
            h,
            url,
            token,
            Some(&session),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .await
        .status()
        .is_success()
    );
    session
}
pub(super) async fn call(
    h: &Harness,
    url: &str,
    token: &str,
    session: &str,
    name: &str,
    args: Value,
) -> Value {
    resource::reply(rpc(h,url,token,Some(session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":name,"arguments":args}})).await).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_oauth_create_refresh_search_and_replay_share_durable_authority() {
    let h = Harness::with_authority(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        None,
        Duration::ZERO,
        Some(scope()),
        true,
    )
    .await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    *h.document.lock().unwrap() = Some(("Saffron private evidence fresh content".into(), 0));
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let model = Arc::new(FaultModel {
        inner: seed.provider.clone(),
        fail: AtomicBool::new(false),
    });
    let mut settings = config(&h, &seed);
    let ledger = &settings.oauth_write_stores.as_ref().unwrap().ledger;
    // Startup must not silently create a lost idempotency ledger.
    assert!(
        crate::oauth_writes::compose(
            notion_knowledge_mcp::KnowledgeServer::default(),
            &settings,
            h.login.callback.as_ref().unwrap().credentials.clone(),
            seed.provider.clone()
        )
        .await
        .is_err()
    );
    assert!(!ledger.exists());
    drop(
        notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(ledger)
            .unwrap(),
    );
    let runtime = Arc::new(
        crate::oauth_writes::compose(
            notion_knowledge_mcp::KnowledgeServer::default(),
            &settings,
            h.login.callback.as_ref().unwrap().credentials.clone(),
            model.clone(),
        )
        .await
        .unwrap(),
    );
    let before = runtime
        .coordinator
        .state()
        .page_state(PAGE)
        .unwrap()
        .unwrap();
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    settings.http_bind = socket.local_addr().unwrap();
    let url = format!("http://{}/mcp", settings.http_bind);
    let stop = CancellationToken::new();
    let router = crate::http::mcp_transport(
        &settings,
        runtime.handler.clone(),
        Arc::new(crate::session_limits::BoundedSessions::new(
            8,
            Duration::from_secs(30),
        )),
        stop.clone(),
        Some(h.login.clone()),
    );
    let serving = tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    let code = h.approved_code().await;
    let readonly = resource::redeem(&h, &code).await;
    let session = connect(&h, &url, &readonly).await;
    let listed = resource::reply(
        rpc(
            &h,
            &url,
            &readonly,
            Some(&session),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .await,
    )
    .await;
    assert!(
        !listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "knowledge_create_page")
    );
    let args = json!({"root_page_id":ROOT,"parent_page_id":ROOT,"title":"Saffron private evidence","markdown":"## Target\n\nSaffron private evidence fresh content\n\n## Keep\n\nPreserved\n","idempotency_key":"installed-create-451"});
    let denied = call(
        &h,
        &url,
        &readonly,
        &session,
        "knowledge_create_page",
        args.clone(),
    )
    .await;
    assert!(denied.get("error").is_some());
    assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
    let code = h
        .approved_code_with_scope("knowledge:read knowledge:write")
        .await;
    let token = resource::redeem(&h, &code).await;
    let session = connect(&h, &url, &token).await;
    let written = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_create_page",
        args.clone(),
    )
    .await;
    assert_eq!(
        written["result"]["structuredContent"]["write"]["status"], "verified",
        "{written}"
    );
    assert_eq!(
        written["result"]["structuredContent"]["index"]["status"],
        "queued"
    );
    let running = runtime.clone();
    let worker_stop = stop.clone();
    let worker = tokio::spawn(async move { running.run(worker_stop).await });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let replay = call(
                &h,
                &url,
                &token,
                &session,
                "knowledge_create_page",
                args.clone(),
            )
            .await;
            assert_eq!(
                replay["result"]["structuredContent"]["write"]["replayed"], true,
                "{replay}"
            );
            if replay["result"]["structuredContent"]["index"]["status"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let after = runtime
        .coordinator
        .state()
        .page_state(PAGE)
        .unwrap()
        .unwrap();
    assert_ne!(before.content_hash(), after.content_hash());
    let search = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_search",
        json!({"query":"fresh content","mode":"lexical","limit":5}),
    )
    .await;
    assert!(search.to_string().contains("fresh content"), "{search}");
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    let current_hash = || {
        use sha2::{Digest, Sha256};
        format!(
            "{:x}",
            Sha256::digest(h.document.lock().unwrap().as_ref().unwrap().0.as_bytes())
        )
    };
    let append = json!({"root_page_id":ROOT,"page_id":PAGE,"markdown":"\nAdditional verified paragraph\n","idempotency_key":"installed-append-451","expected_markdown_sha256":current_hash()});
    let appended = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_append",
        append.clone(),
    )
    .await;
    assert_eq!(
        appended["result"]["structuredContent"]["write"]["status"], "verified",
        "{appended}"
    );
    let replay = call(&h, &url, &token, &session, "knowledge_append", append).await;
    assert_eq!(
        replay["result"]["structuredContent"]["write"]["replayed"], true,
        "{replay}"
    );
    let mut section = json!({"root_page_id":ROOT,"page_id":PAGE,"section_anchor":{"level":2,"heading":"Target"},"markdown":"Approved bounded replacement\n\n","idempotency_key":"installed-section-451","expected_markdown_sha256":current_hash()});
    let declined = section_call(&h, &url, &token, &session, section.clone(), false).await;
    assert_eq!(
        declined["result"]["structuredContent"]["write"]["status"], "not_authorized",
        "{declined}"
    );
    assert_eq!(h.mutations.load(Ordering::SeqCst), 2);
    // A reserved declined attempt cannot silently become a later effect.
    let declined_replay = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_update_section",
        section.clone(),
    )
    .await;
    assert_eq!(
        declined_replay["result"]["structuredContent"]["write"]["status"],
        "outcome_unknown"
    );
    section["idempotency_key"] = json!("installed-section-approved-451");
    let approved = section_call(&h, &url, &token, &session, section.clone(), true).await;
    assert_eq!(
        approved["result"]["structuredContent"]["write"]["status"], "verified",
        "{approved}"
    );
    let document = h.document.lock().unwrap().as_ref().unwrap().0.clone();
    assert!(document.contains("Approved bounded replacement"));
    assert!(document.contains("## Keep\n\nPreserved\n\nAdditional verified paragraph\n"));
    assert_eq!(h.mutations.load(Ordering::SeqCst), 3);
    for name in [
        "knowledge_upload_file",
        "knowledge_archive_page",
        "knowledge_replace_page",
    ] {
        assert!(
            call(&h, &url, &token, &session, name, json!({}))
                .await
                .get("error")
                .is_some()
        );
    }
    // Index failure preserves the verified upstream mutation and durable receipt.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let replay = call(
                &h,
                &url,
                &token,
                &session,
                "knowledge_update_section",
                section.clone(),
            )
            .await;
            assert_eq!(
                replay["result"]["structuredContent"]["write"]["replayed"], true,
                "{replay}"
            );
            if replay["result"]["structuredContent"]["index"]["status"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let successful_hash = runtime.coordinator.state().page_state(PAGE).unwrap();
    model.fail.store(true, Ordering::SeqCst);
    let failed_index = json!({"root_page_id":ROOT,"parent_page_id":ROOT,"title":"Saffron private evidence","markdown":"Index failure must not roll back authoritative content","idempotency_key":"installed-index-failure-451"});
    let written = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_create_page",
        failed_index.clone(),
    )
    .await;
    assert_eq!(
        written["result"]["structuredContent"]["write"]["status"], "verified",
        "{written}"
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let replay = call(
                &h,
                &url,
                &token,
                &session,
                "knowledge_create_page",
                failed_index.clone(),
            )
            .await;
            assert_eq!(
                replay["result"]["structuredContent"]["write"]["replayed"], true,
                "{replay}"
            );
            if replay["result"]["structuredContent"]["index"]["status"] == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.mutations.load(Ordering::SeqCst), 4);
    assert!(
        h.document
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .contains("must not roll back")
    );
    // No successful index commit may expose the failed embedding result.
    assert_eq!(
        runtime.coordinator.state().page_state(PAGE).unwrap(),
        successful_hash
    );
    h.login.flow.revoke("chatgpt", &token).unwrap();
    assert_eq!(rpc(&h,&url,&token,Some(&session),json!({"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"knowledge_create_page","arguments":failed_index}})).await.status(),StatusCode::UNAUTHORIZED);
    assert_eq!(h.mutations.load(Ordering::SeqCst), 4);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    runtime.coordinator.drain_owned().await.unwrap();
    serving.abort();
}

async fn section_call(
    h: &Harness,
    url: &str,
    token: &str,
    session: &str,
    args: Value,
    approve: bool,
) -> Value {
    let mut response = rpc(h,url,token,Some(session),json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"knowledge_update_section","arguments":args}})).await;
    let mut pending = String::new();
    tokio::time::timeout(Duration::from_secs(5),async {
        while let Some(chunk) = response.chunk().await.unwrap() {
            pending.push_str(std::str::from_utf8(&chunk).unwrap());
            while let Some(end) = pending.find('\n') {
                let line: String = pending.drain(..=end).collect();
                if let Some(data) = line.trim().strip_prefix("data: ")
                    && let Ok(frame) = serde_json::from_str::<Value>(data) {
                        if frame["method"]=="elicitation/create" {
                            let prompt = frame["params"]["message"].as_str().unwrap();
                            assert!(prompt.contains("Saffron private evidence fresh content"));
                            assert!(prompt.contains("Approved bounded replacement"));
                            assert!(rpc(h,url,token,Some(session),json!({"jsonrpc":"2.0","id":frame["id"],"result":{"action":if approve {"accept"} else {"decline"},"content":{"approve":approve}}})).await.status().is_success());
                        } else if frame["id"] == 4 { return frame; }
                }
            }
        }
        panic!("missing section result")
    }).await.unwrap()
}

struct FaultModel {
    inner: Arc<dyn notion_knowledge_core::embedding::EmbeddingProvider>,
    fail: AtomicBool,
}
impl notion_knowledge_core::embedding::EmbeddingProvider for FaultModel {
    fn metadata(&self) -> &notion_knowledge_core::embedding::EmbeddingMetadata {
        self.inner.metadata()
    }
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [String],
    ) -> notion_knowledge_core::embedding::EmbeddingFuture<'a> {
        Box::pin(async move {
            if self.fail.load(Ordering::SeqCst) {
                return Err(notion_knowledge_core::embedding::EmbeddingError::Unavailable);
            }
            self.inner.embed_batch(inputs).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_write_cancellation_logout_and_grant_replacement_never_authorize_replay() {
    for invalidation in ["cancel", "logout", "replacement"] {
        let h = Harness::with_authority(
            grant(WORKSPACE, "user-a"),
            StatusCode::OK,
            None,
            Duration::ZERO,
            Some(scope()),
            true,
        )
        .await;
        fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let mut settings = config(&h, &seed);
        drop(
            notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(
                &settings.oauth_write_stores.as_ref().unwrap().ledger,
            )
            .unwrap(),
        );
        let runtime = crate::oauth_writes::compose(
            notion_knowledge_mcp::KnowledgeServer::default(),
            &settings,
            h.login.callback.as_ref().unwrap().credentials.clone(),
            seed.provider.clone(),
        )
        .await
        .unwrap();
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        settings.http_bind = socket.local_addr().unwrap();
        let url = format!("http://{}/mcp", settings.http_bind);
        let stop = CancellationToken::new();
        let router = crate::http::mcp_transport(
            &settings,
            runtime.handler.clone(),
            Arc::new(crate::session_limits::BoundedSessions::new(
                8,
                Duration::from_secs(30),
            )),
            stop.clone(),
            Some(h.login.clone()),
        );
        let serving = tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        });
        let code = h
            .approved_code_with_scope("knowledge:read knowledge:write")
            .await;
        let token = resource::redeem(&h, &code).await;
        let session = connect(&h, &url, &token).await;
        let args = json!({"root_page_id":ROOT,"parent_page_id":ROOT,"title":"Saffron private evidence","markdown":"Saffron private evidence fresh content","idempotency_key":"installed-held-effect-451"});
        let hold = h.mutation_gate.acquire().await.unwrap();
        let pending = async {
            let response = rpc(&h,&url,&token,Some(&session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_create_page","arguments":args.clone()}})).await;
            optional_reply(response).await
        };
        let invalidate = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while h.mutations.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            match invalidation {
                "cancel" => {
                    assert!(rpc(&h,&url,&token,Some(&session),json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3,"reason":"user cancelled"}})).await.status().is_success());
                }
                "logout" => {
                    h.login.flow.revoke("chatgpt", &token).unwrap();
                }
                _ => {
                    h.approved_code().await;
                }
            }
            drop(hold);
        };
        let (response, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(pending, invalidate)
        })
        .await
        .unwrap();
        if let Some(response) = response {
            assert_ne!(
                response["result"]["structuredContent"]["write"]["status"], "verified",
                "{invalidation}: {response}"
            );
        }
        // SDK cancellation also closes a pending reply when its authorization
        // is revoked. The durable replay assertion below proves this absence
        // can never authorize a second upstream effect.
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        // A fresh authenticated session must reconcile the original durable key,
        // even after replacing its grant; it must never send a second effect.
        let code = h
            .approved_code_with_scope("knowledge:read knowledge:write")
            .await;
        let token = resource::redeem(&h, &code).await;
        let session = connect(&h, &url, &token).await;
        let replay = call(&h, &url, &token, &session, "knowledge_create_page", args).await;
        assert_eq!(
            replay["result"]["structuredContent"]["write"]["status"], "outcome_unknown",
            "{invalidation}: {replay}"
        );
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        stop.cancel();
        runtime.coordinator.drain_owned().await.unwrap();
        serving.abort();
    }
}

async fn optional_reply(mut response: reqwest::Response) -> Option<Value> {
    if response
        .headers()
        .get("content-type")
        .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"))
    {
        return Some(response.json().await.unwrap());
    }
    let mut pending = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        pending.push_str(std::str::from_utf8(&chunk).unwrap());
        while let Some(end) = pending.find('\n') {
            let line: String = pending.drain(..=end).collect();
            if let Some(data) = line.trim().strip_prefix("data: ")
                && let Ok(value) = serde_json::from_str::<Value>(data)
                && value.get("id").is_some()
            {
                return Some(value);
            }
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shipping_http_lifecycle_installs_dispatcher_and_drains_on_shutdown() {
    let h = Harness::with_authority(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        None,
        Duration::ZERO,
        Some(scope()),
        true,
    )
    .await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let mut settings = config(&h, &seed);
    drop(
        notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(
            &settings.oauth_write_stores.as_ref().unwrap().ledger,
        )
        .unwrap(),
    );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    settings.http_bind = socket.local_addr().unwrap();
    let address = settings.http_bind;
    drop(socket);
    let url = format!("http://{address}/mcp");
    let stop = CancellationToken::new();
    let credentials = h.login.callback.as_ref().unwrap().credentials.clone();
    let login = h.login.clone();
    let provider = seed.provider.clone();
    let server_stop = stop.clone();
    // This is the same startup, owned worker, router and bounded cleanup used
    // by serve_installed; only credentials and embeddings use typed local ports.
    let server = tokio::spawn(async move {
        crate::http::serve_with_oauth(
            settings,
            notion_knowledge_mcp::KnowledgeServer::default(),
            None,
            server_stop,
            Some(credentials),
            Some(login),
            Some(provider),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let code = h
        .approved_code_with_scope("knowledge:read knowledge:write")
        .await;
    let token = resource::redeem(&h, &code).await;
    let session = connect(&h, &url, &token).await;
    let args = json!({"root_page_id":ROOT,"parent_page_id":ROOT,"title":"Saffron private evidence","markdown":"Saffron private evidence fresh content","idempotency_key":"shipping-owned-worker-451"});
    let written = call(
        &h,
        &url,
        &token,
        &session,
        "knowledge_create_page",
        args.clone(),
    )
    .await;
    assert_eq!(
        written["result"]["structuredContent"]["write"]["status"], "verified",
        "{written}"
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let replay = call(
                &h,
                &url,
                &token,
                &session,
                "knowledge_create_page",
                args.clone(),
            )
            .await;
            assert_eq!(
                replay["result"]["structuredContent"]["write"]["replayed"], true,
                "{replay}"
            );
            if replay["result"]["structuredContent"]["index"]["status"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    let policy = notion_knowledge_retrieval::planner::production_scope(&scope()).unwrap();
    let binding = notion_knowledge_retrieval::commit::CommitBinding::new(
        "chunks", WORKSPACE, &policy, "1", 1,
    )
    .unwrap();
    let reopened = notion_knowledge_retrieval::commit::IndexCommitCoordinator::open_existing(
        &seed.index,
        &seed.state,
        binding,
    )
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    use notion_knowledge_core::reconciliation::ReconciliationJournal;
    let lease = reopened.state().acquire_lease(now, 30).unwrap();
    reopened.state().release_lease(&lease, now).unwrap();
}
