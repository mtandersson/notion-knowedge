//! Black-box tests for the installed rmcp session and SSE capacity guards (#398).
use std::{process::Stdio, time::Duration};

use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    time::{sleep, timeout},
};

const TOKEN: &str = "valid-local-fixture-bearer-token-3981234";

async fn start(extra: &[(&str, &str)]) -> (Child, String) {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
    command
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_HOST", "127.0.0.1")
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_HTTP_AUTH", "bearer")
        .env("NK_HTTP_BEARER_TOKEN", TOKEN)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in extra {
        command.env(key, value);
    }
    let mut child = command.spawn().unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    timeout(Duration::from_secs(15), async {
        loop {
            let mut line = String::new();
            assert_ne!(
                stderr.read_line(&mut line).await.unwrap(),
                0,
                "startup failed"
            );
            assert!(!line.contains(TOKEN));
            if line.contains("Serving MCP over Streamable HTTP") {
                break;
            }
        }
    })
    .await
    .unwrap();
    (child, format!("http://127.0.0.1:{port}/mcp"))
}

async fn rpc_response(mut response: Response) -> Value {
    let mut buffer = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        buffer.push_str(std::str::from_utf8(&chunk).unwrap());
        for line in buffer
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
        {
            if let Ok(json) = serde_json::from_str::<Value>(line)
                && json.get("id").is_some()
            {
                return json;
            }
        }
    }
    panic!("no MCP reply");
}

fn initialize(client: &Client, url: &str, id: u32) -> reqwest::RequestBuilder {
    client
        .post(url)
        .bearer_auth(TOKEN)
        .header("accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc":"2.0","id":id,"method":"initialize","params":{
                "protocolVersion":"2025-03-26","capabilities":{},
                "clientInfo":{"name":"quota-fixture","version":"1"}
            }}),
        )
}

fn post(client: &Client, url: &str, session: &str, id: u32) -> reqwest::RequestBuilder {
    client
        .post(url)
        .bearer_auth(TOKEN)
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", session)
        .header("mcp-protocol-version", "2025-03-26")
        .json(&json!({"jsonrpc":"2.0","id":id,"method":"ping"}))
}

#[tokio::test]
async fn authenticated_session_quota_delete_recovery_and_no_secret_leak() {
    timeout(Duration::from_secs(25), async {
        let (mut child, url) =
            start(&[("NK_MCP_MAX_SESSIONS", "1"), ("NK_MCP_MAX_STREAMS", "2")]).await;
        let client = Client::builder().no_proxy().build().unwrap();
        assert_eq!(
            initialize(&client, &url, 0)
                .bearer_auth("wrong-token")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let first = initialize(&client, &url, 1).send().await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let session = first.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(rpc_response(first).await["id"], 1);
        let extra = initialize(&client, &url, 2).send().await.unwrap();
        assert_eq!(extra.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(extra.headers()["retry-after"], "1");
        let denied = extra.text().await.unwrap();
        assert!(!denied.contains(&session));
        assert!(!denied.contains(TOKEN));
        assert!(denied.contains("capacity exceeded"));
        let ping = post(&client, &url, &session, 3).send().await.unwrap();
        assert_eq!(rpc_response(ping).await["id"], 3);
        let closed = client
            .delete(&url)
            .bearer_auth(TOKEN)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .send()
            .await
            .unwrap();
        assert_eq!(closed.status(), StatusCode::ACCEPTED);
        assert_eq!(
            post(&client, &url, &session, 4)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let second = initialize(&client, &url, 5).send().await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(rpc_response(second).await["id"], 5);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_session_is_evicted_and_releases_capacity() {
    timeout(Duration::from_secs(25), async {
        let (mut child, url) = start(&[
            ("NK_MCP_MAX_SESSIONS", "1"),
            ("NK_MCP_SESSION_IDLE_SECONDS", "1"),
        ])
        .await;
        let client = Client::builder().no_proxy().build().unwrap();
        let first = initialize(&client, &url, 1).send().await.unwrap();
        let old = first.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        rpc_response(first).await;
        sleep(Duration::from_millis(1600)).await;
        let next = initialize(&client, &url, 2).send().await.unwrap();
        assert_eq!(next.status(), StatusCode::OK);
        rpc_response(next).await;
        assert_eq!(
            post(&client, &url, &old, 3).send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn installed_sse_stream_quota_disconnect_and_delete_bypass() {
    timeout(Duration::from_secs(25), async {
        let (mut child, url) = start(&[
            ("NK_MCP_MAX_SESSIONS", "2"),
            ("NK_MCP_MAX_STREAMS", "1"),
            ("NK_MCP_STREAM_MAX_SECONDS", "2"),
        ])
        .await;
        let client = Client::builder().no_proxy().build().unwrap();
        let init = initialize(&client, &url, 1).send().await.unwrap();
        let session = init.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        rpc_response(init).await;
        let get = || {
            client
                .get(&url)
                .bearer_auth(TOKEN)
                .header("accept", "text/event-stream")
                .header("mcp-session-id", &session)
                .header("mcp-protocol-version", "2025-03-26")
        };
        let stream = get().send().await.unwrap();
        assert_eq!(stream.status(), StatusCode::OK);
        let busy = get().send().await.unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(busy.headers()["retry-after"], "1");
        // POST also acquires a stream permit before dispatching any tool.
        let post_busy = post(&client, &url, &session, 10).send().await.unwrap();
        assert_eq!(post_busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(stream);
        let mut recovered = false;
        for _ in 0..20 {
            let response = get().send().await.unwrap();
            if response.status() == StatusCode::OK {
                recovered = true;
                drop(response);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(recovered, "disconnect must drop stream permit");
        let closed = client
            .delete(&url)
            .bearer_auth(TOKEN)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .send()
            .await
            .unwrap();
        assert_eq!(closed.status(), StatusCode::ACCEPTED);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn sse_hard_deadline_ends_stream_and_releases_capacity() {
    timeout(Duration::from_secs(20), async {
        let (mut child, url) = start(&[
            ("NK_MCP_MAX_SESSIONS", "2"),
            ("NK_MCP_MAX_STREAMS", "1"),
            ("NK_MCP_STREAM_MAX_SECONDS", "1"),
        ])
        .await;
        let client = Client::builder().no_proxy().build().unwrap();
        let init = initialize(&client, &url, 1).send().await.unwrap();
        let session = init.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        rpc_response(init).await;
        let get = || {
            client
                .get(&url)
                .bearer_auth(TOKEN)
                .header("accept", "text/event-stream")
                .header("mcp-session-id", &session)
                .header("mcp-protocol-version", "2025-03-26")
        };
        let mut stream = get().send().await.unwrap();
        assert_eq!(stream.status(), StatusCode::OK);
        assert_eq!(
            get().send().await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // The timeout applies to an already accepted, long-lived response.
        // A disconnected client cannot pin a permit indefinitely.
        let ended = timeout(Duration::from_secs(3), async {
            loop {
                match stream.chunk().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => break true,
                    Err(_) => break true,
                }
            }
        })
        .await
        .unwrap();
        assert!(ended, "stream must stop at its server-side hard deadline");
        drop(stream);
        let response = get().send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn authenticated_burst_is_bounded_while_webhook_and_management_remain_available() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    timeout(Duration::from_secs(25), async {
        let state = tempfile::tempdir().unwrap();
        let path = state.path().join("state.db");
        let id = "11111111-1111-4111-8111-111111111111";
        let (mut child, url) = start(&[
            ("NK_MCP_MAX_SESSIONS", "2"),
            ("NK_MCP_MAX_STREAMS", "2"),
            ("NK_MCP_MAX_IN_FLIGHT", "32"),
            ("NK_MCP_STREAM_MAX_SECONDS", "10"),
            ("NK_WEBHOOK_MODE", "verified"),
            ("NK_WEBHOOK_VERIFICATION_TOKEN", "load-fixture-signing-secret"),
            ("NK_WEBHOOK_WORKSPACE_ID", id),
            ("NK_WEBHOOK_INTEGRATION_ID", id),
            ("NK_WEBHOOK_SUBSCRIPTION_ID", id),
            ("NK_WEBHOOK_STATE_FILE", path.to_str().unwrap()),
        ]).await;
        let client = Client::builder().no_proxy().build().unwrap();
        let mut burst = tokio::task::JoinSet::new();
        for n in 0..32 {
            let request = initialize(&client, &url, n);
            burst.spawn(async move {
                let response = request.send().await.unwrap();
                if response.status() == StatusCode::OK {
                    let session = response.headers()["mcp-session-id"].to_str().unwrap().to_owned();
                    rpc_response(response).await;
                    Some(session)
                } else {
                    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                    assert_eq!(response.headers()["retry-after"], "1");
                    assert!(!response.text().await.unwrap().contains(TOKEN));
                    None
                }
            });
        }
        let mut sessions = vec![];
        while let Some(result) = burst.join_next().await {
            if let Some(session) = result.unwrap() { sessions.push(session); }
        }
        assert_eq!(sessions.len(), 2);
        let mut streams = vec![];
        for session in &sessions {
            let response = client.get(&url).bearer_auth(TOKEN)
                .header("accept", "text/event-stream")
                .header("mcp-session-id", session)
                .header("mcp-protocol-version", "2025-03-26").send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            streams.push(response); // Intentionally do not consume the SSE body.
        }
        for name in ["knowledge_search", "knowledge_related", "knowledge_upload_file", "knowledge_sync_status"] {
            for n in 0..8 {
                let request = post(&client, &url, &sessions[0], n).json(&json!({
                    "jsonrpc":"2.0","id":n,"method":"tools/call",
                    "params":{"name":name,"arguments":{"private":"signed-url-fixture"}}
                }));
                burst.spawn(async move {
                    let response = request.send().await.unwrap();
                    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                    let body = response.text().await.unwrap();
                    assert!(!body.contains("signed-url-fixture"));
                    assert!(!body.contains(TOKEN));
                    None::<String>
                });
            }
        }
        while let Some(result) = burst.join_next().await { result.unwrap(); }
        let base = url.strip_suffix("/mcp").unwrap();
        assert_eq!(client.get(format!("{base}/livez")).send().await.unwrap().status(), StatusCode::OK);
        let event = json!({"id":id,"timestamp":"2026-10-17T00:00:00Z","workspace_id":id,"integration_id":id,"subscription_id":id,"type":"page.content_updated","entity":{"id":id,"type":"page"},"attempt_number":1}).to_string();
        let mut mac = Hmac::<Sha256>::new_from_slice(b"load-fixture-signing-secret").unwrap();
        mac.update(event.as_bytes());
        let signature = format!("sha256={:x}", mac.finalize().into_bytes());
        assert_eq!(client.post(format!("{base}/webhooks/notion")).header("x-notion-signature",signature).body(event).send().await.unwrap().status(), StatusCode::OK);
        let metrics = client.get(format!("{base}/metrics")).send().await.unwrap().text().await.unwrap();
        for line in ["nk_resource_active{resource=\"session\"} 2", "nk_resource_peak{resource=\"session\"} 2", "nk_resource_active{resource=\"stream\"} 2", "nk_resource_peak{resource=\"stream\"} 2"] {
            assert!(metrics.lines().any(|value| value == line), "missing {line} in {metrics}");
        }
        let rejected = metrics.lines().find_map(|line| line.strip_prefix("nk_resource_rejected_total{resource=\"stream\"} ")).unwrap().parse::<u64>().unwrap();
        assert!(rejected >= 32, "all saturated tool calls must count as rejected");
        assert!(!metrics.contains(TOKEN));
        assert!(!metrics.contains(&sessions[0]));
        drop(streams);
        for session in sessions {
            assert_eq!(client.delete(&url).bearer_auth(TOKEN).header("mcp-session-id",session).header("mcp-protocol-version","2025-03-26").send().await.unwrap().status(), StatusCode::ACCEPTED);
        }
        let recovered = initialize(&client, &url, 100).send().await.unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
        rpc_response(recovered).await;
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }).await.unwrap();
}

#[path = "../../mcp/tests/scope_fixture.rs"]
mod scope_fixture;

struct LoadGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
}
impl LoadGate {
    fn new() -> Self {
        Self {
            entered: Default::default(),
            release: Default::default(),
            calls: Default::default(),
        }
    }
    async fn wait(&self) {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
    }
}
impl notion_knowledge_core::search::SemanticSearch for LoadGate {
    fn search(
        &self,
        _: notion_knowledge_core::search::SemanticQuery,
    ) -> notion_knowledge_core::search::SearchFuture<'_> {
        Box::pin(async move {
            self.wait().await;
            Ok(vec![])
        })
    }
}
impl notion_knowledge_mcp::upload::FileIngestion for LoadGate {
    fn ingest<'a>(
        &'a self,
        _: notion_knowledge_mcp::upload::UploadFileRequest,
        _: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, &'static str>> + Send + 'a>>
    {
        Box::pin(async move {
            self.wait().await;
            Ok(json!({"outcome":"completed"}))
        })
    }
}

#[tokio::test]
async fn installed_http_mixed_search_upload_and_graph_share_weighted_work_capacity() {
    use std::sync::{Arc, atomic::Ordering};
    timeout(Duration::from_secs(20), async {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let config = notion_knowledge_server::config::Config::from_lookup(|name| match name {
            "NK_HTTP_PORT" => Some(port.to_string().into()),
            "NK_HTTP_AUTH" => Some("bearer".into()),
            "NK_HTTP_BEARER_TOKEN" => Some(TOKEN.into()),
            "NK_READ_ONLY" => Some("false".into()),
            "NK_MCP_TOOL_CAPACITY" => Some("12".into()),
            _ => None,
        }).unwrap();
        let search = Arc::new(LoadGate::new());
        let upload = Arc::new(LoadGate::new());
        let graph = Arc::new(notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open_in_memory().unwrap());
        let handler = scope_fixture::secured(notion_knowledge_mcp::KnowledgeServer::with_search(search.clone()))
            .and_related_graph(graph).unwrap().and_file_ingestion(upload.clone()).unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let signal = stop.clone();
        let server = tokio::spawn(async move {
            notion_knowledge_server::http::serve_installed(config, handler,
                #[cfg(all(unix, feature = "webhook-worker"))] None,
                signal).await.unwrap();
        });
        let url = format!("http://127.0.0.1:{port}/mcp");
        let client = Client::builder().no_proxy().build().unwrap();
        loop {
            if client.get(format!("http://127.0.0.1:{port}/livez")).send().await.is_ok() { break; }
            sleep(Duration::from_millis(10)).await;
        }
        let init = initialize(&client, &url, 1).send().await.unwrap();
        let session = init.headers()["mcp-session-id"].to_str().unwrap().to_owned();
        rpc_response(init).await;
        let call = |id, name: &str, args: Value| post(&client,&url,&session,id).json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}));
        let request = call(2, "knowledge_search", json!({"query":"fixture","limit":1,"mode":"semantic"}));
        let first = tokio::spawn(async move { rpc_response(request.send().await.unwrap()).await });
        search.entered.notified().await;
        let request = call(3, "knowledge_upload_file", json!({"target":"11111111-1111-1111-1111-111111111111","file":{"file_id":"fixture","download_url":"https://fixture.invalid/private"}}));
        let second = tokio::spawn(async move { rpc_response(request.send().await.unwrap()).await });
        upload.entered.notified().await;
        for name in ["knowledge_related", "knowledge_search", "knowledge_upload_file", "knowledge_sync_status"] {
            let response = rpc_response(call(4, name, json!({})).send().await.unwrap()).await;
            assert_eq!(response["result"]["content"][0]["text"], "tool_busy: retry later");
        }
        assert_eq!(search.calls.load(Ordering::SeqCst), 1);
        assert_eq!(upload.calls.load(Ordering::SeqCst), 1);
        let metrics_url = format!("http://127.0.0.1:{port}/metrics");
        let metrics = client.get(&metrics_url).send().await.unwrap().text().await.unwrap();
        for line in ["nk_resource_active{resource=\"tool_work\"} 12", "nk_resource_peak{resource=\"tool_work\"} 12", "nk_resource_rejected_total{resource=\"tool_work\"} 4"] {
            assert!(metrics.lines().any(|value| value == line), "missing {line}");
        }
        search.release.notify_one();
        assert_ne!(first.await.unwrap()["result"]["isError"], true);
        // Upload still owns 8 units; graph can use the remaining 4 after search ends.
        let related = rpc_response(call(5,"knowledge_related",json!({"page_id":"11111111-1111-1111-1111-111111111111"})).send().await.unwrap()).await;
        assert_ne!(related["result"]["isError"], true, "{related}");
        assert!(related["error"].is_null(), "{related}");
        upload.release.notify_one();
        assert_ne!(second.await.unwrap()["result"]["isError"], true);
        let recovered = client.get(metrics_url).send().await.unwrap().text().await.unwrap();
        assert!(recovered.lines().any(|line| line == "nk_resource_active{resource=\"tool_work\"} 0"));
        stop.cancel();
        server.await.unwrap();
    }).await.unwrap();
}
