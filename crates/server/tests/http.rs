mod common;
use std::{process::Stdio, time::Duration};

use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

async fn frame(response: Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    let mut response = response;
    let mut data = String::new();
    // A stateful response is SSE and may include a priming event before the
    // JSON-RPC response. Read only until the response, never await stream EOF.
    while let Some(chunk) = response.chunk().await.unwrap() {
        data.push_str(std::str::from_utf8(&chunk).unwrap());
        for line in data.lines().filter_map(|line| line.strip_prefix("data: ")) {
            if let Ok(value) = serde_json::from_str::<Value>(line)
                && value.get("id").is_some()
            {
                return value;
            }
        }
    }
    panic!("missing JSON-RPC response: {data}");
}

#[tokio::test]
async fn http_client_initializes_discovers_calls_and_receives_structured_errors() {
    // Select an available port and exercise the binary's production CLI and
    // environment wiring rather than mounting a separate test server.
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .args(["--http"])
        .env_clear()
        .env("NK_HTTP_HOST", "127.0.0.1")
        .env("NK_HTTP_PORT", port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let result = timeout(Duration::from_secs(15), async {
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(stderr.read_line(&mut line).await.unwrap(), 0, "server exited before listening");
            if line.contains("Serving MCP over Streamable HTTP") { break; }
        }
        let client = Client::builder().no_proxy().build().unwrap();
        let live_url = format!("http://127.0.0.1:{port}/livez");
        let live = client.get(&live_url).send().await.unwrap();
        assert_eq!(live.status(), StatusCode::OK);
        assert_eq!(live.headers()["cache-control"], "no-store");
        assert_eq!(
            live.json::<Value>().await.unwrap(),
            json!({
                "server": {"name":"notion-knowledge", "version":env!("CARGO_PKG_VERSION")},
                "transport":"http", "status":"alive"
            })
        );
        let ready_url = format!("http://127.0.0.1:{port}/readyz");
        let ready = client.get(&ready_url).send().await.unwrap();
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ready.headers()["cache-control"], "no-store");
        assert_eq!(
            ready.json::<Value>().await.unwrap(),
            json!({
                "server": {"name":"notion-knowledge", "version":env!("CARGO_PKG_VERSION")},
                "transport":"http", "status":"not_ready",
                "dependencies":{"index":"unavailable"}
            })
        );
        let health_url = format!("http://127.0.0.1:{port}/health");
        let health = client.get(&health_url).send().await.unwrap();
        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(health.headers()["cache-control"], "no-store");
        let health: Value = health.json().await.unwrap();
        assert_eq!(health, json!({
            "server": {"name":"notion-knowledge", "version":env!("CARGO_PKG_VERSION")},
            "transport":"http", "status":"degraded",
            "access":{"read_only":true},
            "dependencies":{"notion":"unconfigured", "index":"unavailable"}
        }));
        for url in [&live_url, &ready_url, &health_url] {
            for (header, value) in [("origin", "https://untrusted.example"), ("host", "untrusted.example")] {
                assert_eq!(client.get(url).header(header,value).send().await.unwrap().status(), StatusCode::FORBIDDEN);
            }
        }
        let url = format!("http://127.0.0.1:{port}/mcp");
        let before_initialize = client.post(&url)
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0", "id":0, "method":"tools/list"}))
            .send().await.unwrap();
        assert_eq!(before_initialize.status(), StatusCode::BAD_REQUEST);
        let error: Value = before_initialize.json().await.unwrap();
        assert_eq!(error["id"], 0);
        assert_eq!(error["error"]["code"], -32600);
        let response = client.post(&url)
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{
                "protocolVersion":"2025-03-26", "capabilities":{},
                "clientInfo":{"name":"http-smoke", "version":"1.0.0"}
            }})).send().await.unwrap();
        let session = response.headers()["mcp-session-id"].to_str().unwrap().to_owned();
        let initialized = frame(response).await;
        assert_eq!(initialized["id"], 1);
        assert_eq!(initialized["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(initialized["result"]["serverInfo"]["name"], "notion-knowledge");
        assert_eq!(initialized["result"]["serverInfo"]["version"], health["server"]["version"]);
        assert!(initialized["result"]["capabilities"]["tools"].is_object());
        let post = || client.post(&url)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26");
        let notification = post().json(&json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
            .send().await.unwrap();
        assert_eq!(notification.status(), StatusCode::ACCEPTED);
        let tools = frame(post().json(&json!({"jsonrpc":"2.0", "id":2, "method":"tools/list"}))
            .send().await.unwrap()).await;
        assert_eq!(tools["id"], 2);
        common::assert_search_catalog(&tools["result"]["tools"]);
        for mode in ["semantic", "lexical", "hybrid"] {
            let result = frame(post().json(&json!({"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"knowledge_search","arguments":{"query":"notes","limit":1,"mode":mode}}})).send().await.unwrap()).await;
            assert_eq!(result["result"]["isError"], true);
            assert!(result["result"]["content"][0]["text"].as_str().unwrap().starts_with("retrieval_unavailable:"));
            assert!(result["result"].get("structuredContent").is_none());
        }
        for arguments in [json!({"query":"secret-input","limit":1,"mode":"hybrid","filters":null}),json!({"query":"secret-input","limit":1,"mode":"hybrid","filters":{"page_ids":null}}),json!({"query":" ","limit":1,"mode":"hybrid"}),json!({"query":"secret-input","limit":0,"mode":"hybrid"}),json!({"query":"secret-input","limit":101,"mode":"hybrid"}),json!({"query":"secret-input","limit":1,"mode":"sql"}),json!({"query":"secret-input","limit":1,"mode":"hybrid","filters":{"page_ids":[]}}),json!({"query":"secret-input","limit":1,"mode":"hybrid","filters":{"sql":"DROP"}})] {
            let result = frame(post().json(&json!({"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"knowledge_search","arguments":arguments}})).send().await.unwrap()).await;
            assert_eq!(result["error"]["code"], -32602);
            assert!(!result.to_string().contains("secret-input"));
        }
        // The public MCP wire path rejects writes and uploads even when the
        // client bypasses tools/list and tries a direct call.
        for attempted_write in ["notion_create_page", "notion_upload_file"] {
            let rejected = frame(post().json(&json!({
                "jsonrpc":"2.0", "id":30, "method":"tools/call",
                "params":{"name":attempted_write, "arguments":{}}
            })).send().await.unwrap()).await;
            assert!(rejected["error"]["code"].is_number());
        }
        let called = frame(post().json(&json!({"jsonrpc":"2.0", "id":3, "method":"tools/call", "params":{"name":"unknown-tool", "arguments":{}}}))
            .send().await.unwrap()).await;
        assert_eq!(called["id"], 3);
        assert!(called["error"]["code"].is_number());
        assert!(called["error"]["message"].is_string());
        let malformed = post().body("{").header("content-type", "application/json")
            .send().await.unwrap();
        let status = malformed.status();
        let body = malformed.text().await.unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let error: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(error["jsonrpc"], "2.0");
        assert_eq!(error["error"]["code"], -32700);
        assert!(error["error"]["message"].is_string());
        let invalid = post().json(&json!({"jsonrpc":"2.0", "id":6, "method":"unknown-method", "params":{}}))
            .send().await.unwrap();
        let invalid = frame(invalid).await;
        assert_eq!(invalid["id"], 6);
        assert_eq!(invalid["error"]["code"], -32601);
        let invalid_envelope = post().json(&json!([])).send().await.unwrap();
        assert_eq!(invalid_envelope.status(), StatusCode::BAD_REQUEST);
        let error: Value = invalid_envelope.json().await.unwrap();
        assert_eq!(error["error"]["code"], -32600);
        let media = post().body("{").header("content-type", "text/plain")
            .send().await.unwrap();
        assert_eq!(media.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        // Both declared JSON and unsupported media must be bounded before
        // SDK parsing; oversized failures use a structured, redacted envelope.
        for media_type in ["application/json", "text/plain"] {
            let response = post()
                .header("content-type", media_type)
                .body(vec![b'x'; 4 * 1024 * 1024 + 1])
                .send().await.unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            let body: Value = response.json().await.unwrap();
            assert_eq!(body["jsonrpc"], "2.0");
            assert_eq!(body["id"], Value::Null);
            assert_eq!(body["error"]["code"], -32000);
            assert!(!body.to_string().contains("xxx"));
        }
        let stream = client.get(&url).header("accept", "text/event-stream")
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26").send().await.unwrap();
        assert_eq!(stream.status(), StatusCode::OK);
        assert!(stream.headers()["content-type"].to_str().unwrap().starts_with("text/event-stream"));
        drop(stream);
        let bad_version = client.post(&url)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "unsupported")
            .json(&json!({"jsonrpc":"2.0", "id":7, "method":"ping"})).send().await.unwrap();
        assert_eq!(bad_version.status(), StatusCode::BAD_REQUEST);
        let error: Value = bad_version.json().await.unwrap();
        assert_eq!(error["id"], 7);
        assert_eq!(error["error"]["code"], -32600);
        for (header, value) in [("origin", "https://untrusted.example"), ("host", "untrusted.example")] {
            let response = post().header(header, value).json(&json!({"jsonrpc":"2.0", "id":4, "method":"ping"}))
                .send().await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let malformed = post().body("{").header("content-type", "application/json")
                .header(header, value).send().await.unwrap();
            assert_eq!(malformed.status(), StatusCode::FORBIDDEN);
        }
        let closed = client.delete(&url).header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26").send().await.unwrap();
        assert_eq!(closed.status(), StatusCode::ACCEPTED);
        let expired = post().json(&json!({"jsonrpc":"2.0", "id":5, "method":"ping"})).send().await.unwrap();
        assert_eq!(expired.status(), StatusCode::NOT_FOUND);
    }).await;
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    result.expect("HTTP conversation must finish within fifteen seconds");
}

#[tokio::test]
async fn production_webhook_acknowledges_committed_hints_and_deduplicates_after_restart() {
    use hmac::{Hmac, Mac};
    use notion_knowledge_core::webhook::{
        EventKey, EventState, InboxScope, ProcessingOutcome, WebhookDebounce, WebhookInbox,
    };
    use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
    use sha2::Sha256;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let id = "13950b26-c203-4f3b-b97d-93ec06319565";
    let key = "fixture-production-webhook-key";
    let event_key = EventKey {
        workspace_id: id.into(),
        subscription_id: id.into(),
        event_id: id.into(),
    };
    let sign = |body: &str| {
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
        mac.update(body.as_bytes());
        format!("sha256={:x}", mac.finalize().into_bytes())
    };
    let client = Client::builder().no_proxy().build().unwrap();
    for attempt in [1, 9] {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
            .arg("--http")
            .env_clear()
            .env("NK_HTTP_PORT", port.to_string())
            .env("NK_WEBHOOK_MODE", "verified")
            .env("NK_WEBHOOK_VERIFICATION_TOKEN", key)
            .env("NK_WEBHOOK_WORKSPACE_ID", id)
            .env("NK_WEBHOOK_INTEGRATION_ID", id)
            .env("NK_WEBHOOK_SUBSCRIPTION_ID", id)
            .env("NK_WEBHOOK_STATE_FILE", &path)
            .env("NK_WEBHOOK_DEBOUNCE_MS", "1000")
            .env("NK_WEBHOOK_MAX_DELAY_MS", "3000")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        timeout(Duration::from_secs(15),async {
            let mut stderr=BufReader::new(child.stderr.take().unwrap());
            loop {let mut line=String::new();assert_ne!(stderr.read_line(&mut line).await.unwrap(),0);assert!(!line.contains(key));if line.contains("Serving MCP over Streamable HTTP") {break;}}
            let body=format!(r#"{{"id":"{id}","timestamp":"2026-10-08T00:00:00Z","workspace_id":"{id}","integration_id":"{id}","subscription_id":"{id}","type":"page.content_updated","entity":{{"id":"{id}","type":"page"}},"attempt_number":{attempt},"data":{{"private":"fixture-private-content"}}}}"#);
            let url=format!("http://127.0.0.1:{port}/webhooks/notion");
            for _ in 0..2 {
                assert_eq!(client.post(&url).header("host","public-webhook.example").header("origin","https://api.notion.com").header("x-notion-signature",sign(&body)).body(body.clone()).send().await.unwrap().status(),StatusCode::OK);
            }
            // Opening another connection after the ACK sees a complete durable row.
            let store=SqliteSyncStateStore::open(&path).unwrap();let saved=store.event(&event_key).unwrap().unwrap();
            assert_eq!(saved.state,EventState::Pending);assert_eq!(saved.event.attempt_number,1);assert_eq!(saved.event.event_type,"page.content_updated");
            if attempt==1 {
                let second=body.replacen(&format!("\"id\":\"{id}\""),"\"id\":\"367cba44-b6f3-4c92-81e7-6a2e9659efd4\"",1).replace("page.content_updated","page.properties_updated").replace("00:00:00Z","00:00:01Z");
                let third=second.replacen("367cba44-b6f3-4c92-81e7-6a2e9659efd4","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",1).replace(&format!("\"entity\":{{\"id\":\"{id}\""),"\"entity\":{\"id\":\"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"");
                for hint in [second,third] {
                    assert_eq!(client.post(&url).header("x-notion-signature",sign(&hint)).body(hint).send().await.unwrap().status(),StatusCode::OK);
                }
            }
            let conflict=body.replace("page.content_updated","page.deleted");
            assert_eq!(client.post(&url).header("x-notion-signature",sign(&conflict)).body(conflict).send().await.unwrap().status(),StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(store.event(&event_key).unwrap().unwrap(),saved);
            let mut headers=reqwest::header::HeaderMap::new();headers.append("x-notion-signature",sign(&body).parse().unwrap());headers.append("x-notion-signature","sha256=00".parse().unwrap());
            assert_eq!(client.post(&url).headers(headers).body(body.clone()).send().await.unwrap().status(),StatusCode::UNAUTHORIZED);
            assert_eq!(client.post(&url).header("content-encoding","gzip").body(body.clone()).send().await.unwrap().status(),StatusCode::UNSUPPORTED_MEDIA_TYPE);
            assert_eq!(client.post(&url).body(body).send().await.unwrap().status(),StatusCode::UNAUTHORIZED);
            assert_eq!(client.post(&url).body(vec![b' ';64*1024+1]).send().await.unwrap().status(),StatusCode::PAYLOAD_TOO_LARGE);
        }).await.unwrap();
        // Abrupt termination immediately after receipt retains pending work.
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        assert_eq!(
            SqliteSyncStateStore::open(&path)
                .unwrap()
                .event(&event_key)
                .unwrap()
                .unwrap()
                .state,
            EventState::Pending
        );
    }
    // Actual HTTP composition retained one page batch across restart, and kept
    // a different page independently claimable. Use an injected future clock;
    // no wall-clock sleep is required to verify persisted quiet-period wiring.
    let store = SqliteSyncStateStore::open(&path).unwrap();
    let scope = InboxScope {
        workspace_id: id.into(),
        subscription_id: id.into(),
    };
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 4000;
    assert!(store.claim(&scope, now / 1000, 10).unwrap().is_none());
    let first = store.claim_page(&scope, now, 10).unwrap().unwrap();
    let second = store.claim_page(&scope, now, 10).unwrap().unwrap();
    assert_ne!(first.key.page_id, second.key.page_id);
    let same = if first.key.page_id == id {
        &first
    } else {
        &second
    };
    assert_eq!(same.events.len(), 2);
    assert_eq!(same.newest.event_type, "page.properties_updated");
    store
        .complete_page(&first, now + 1, ProcessingOutcome::Succeeded)
        .unwrap();
    store
        .complete_page(&second, now + 1, ProcessingOutcome::Succeeded)
        .unwrap();
    assert!(store.claim_page(&scope, now + 2, 10).unwrap().is_none());
    let bytes = std::fs::read(&path).unwrap();
    for private in [key, "fixture-private-content", "sha256="] {
        assert!(
            !bytes
                .windows(private.len())
                .any(|window| window == private.as_bytes())
        );
    }
}
