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
        assert_eq!(tools["result"]["tools"], json!([]));
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
