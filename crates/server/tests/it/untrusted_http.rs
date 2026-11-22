//! HTTP without a bearer, or off loopback, is not a trusted transport: it
//! serves no content-bearing tool however completely content is configured.
use std::{process::Stdio, time::Duration};

use reqwest::Client;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
const TOKEN_SENTINEL: &str = "notion-token-private-sentinel";

async fn frame(response: reqwest::Response) -> Value {
    let text = response.text().await.unwrap();
    if let Ok(value) = serde_json::from_str(&text) {
        return value;
    }
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .unwrap_or_else(|| panic!("unexpected HTTP response: {text}"))
}

/// Starts the real binary with every content source configured and returns
/// its startup stderr, the served tool catalog and each content call's result.
/// The bearer-on-loopback counterpart that does serve content is
/// `webhook_worker::authenticated_http_serves_committed_hybrid_and_graph_and_denies_untrusted_session_requests`.
async fn exercise(host: &str) -> (String, Vec<Value>) {
    let directory = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let path = |name: &str| directory.path().join(name).to_str().unwrap().to_owned();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
    command
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_HOST", host)
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_NOTION_AUTH", "integration")
        .env("NOTION_TOKEN", TOKEN_SENTINEL)
        .env("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE)
        .env("NK_NOTION_SCOPE_ROOTS", ROOT)
        .env("NK_READ_ONLY", "false")
        .env("NK_FILE_INGESTION", "true")
        .env("NK_FILE_SOURCE_HOSTS", "files.example.test")
        .env("NK_FILE_AUDIT_FILE", path("audit.sqlite"))
        .env("NK_WEBHOOK_STATE_FILE", path("state.sqlite"))
        .env("NK_RETRIEVAL", "true")
        .env("NK_GRAPH", "true")
        .env("NK_WEBHOOK_INDEX_DIR", path("index"))
        .env("NK_WEBHOOK_QWEN_ASSETS", path("model"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let result = timeout(Duration::from_secs(15), async {
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        let mut startup = String::new();
        loop {
            let mut line = String::new();
            assert_ne!(stderr.read_line(&mut line).await.unwrap(), 0, "{startup}");
            startup.push_str(&line);
            if line.contains("Serving MCP over Streamable HTTP") {
                break;
            }
        }
        let client = Client::builder().no_proxy().build().unwrap();
        let url = format!("http://127.0.0.1:{port}/mcp");
        let post = || {
            client
                .post(&url)
                .header("accept", "application/json, text/event-stream")
        };
        let response = post()
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"untrusted-http","version":"1"}}}))
            .send()
            .await
            .unwrap();
        let session = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        frame(response).await;
        let session_post = || {
            post()
                .header("mcp-session-id", &session)
                .header("mcp-protocol-version", "2025-03-26")
        };
        session_post()
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        let mut responses = vec![];
        for request in [
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            call(3, "knowledge_search", json!({"query":"saffron","limit":5,"mode":"lexical"})),
            call(4, "knowledge_get", json!({"refs":[{"kind":"page","id":ROOT}],"max_chars":100,"freshness":"fresh"})),
            call(5, "knowledge_related", json!({"page_id":ROOT})),
            call(6, "knowledge_upload_file", json!({"target":ROOT,"file":{"download_url":"https://files.example.test/a.txt","file_id":"file-1"}})),
        ] {
            responses.push(frame(session_post().json(&request).send().await.unwrap()).await);
        }
        (startup, responses)
    })
    .await;
    child.kill().await.unwrap();
    let (startup, responses) = result.unwrap();
    assert!(!startup.contains(TOKEN_SENTINEL));
    // Untrusted startup never creates private read state.
    for name in ["state.sqlite", "index", "audit.sqlite"] {
        assert!(!directory.path().join(name).exists(), "{name}");
    }
    (startup, responses)
}

fn call(id: u64, name: &str, arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}})
}

#[tokio::test]
async fn untrusted_http_serves_no_content_tool_and_says_so_at_startup() {
    for host in ["127.0.0.1", "0.0.0.0"] {
        let (startup, responses) = exercise(host).await;
        let names: Vec<_> = responses[0]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        // Search, get and upload stay advertised as unbacked placeholders;
        // graph reads are not offered. Integration writes cannot be enabled
        // here at all: configuration refuses them without a bearer.
        for absent in ["knowledge_related", "knowledge_file_refresh"] {
            assert!(!names.contains(&absent), "{host}: {absent} in {names:?}");
        }
        for (response, unavailable) in responses[1..].iter().zip([
            "retrieval_unavailable:",
            "notion_unavailable:",
            "root_scope_unavailable:",
            "file_upload_unavailable:",
        ]) {
            assert_eq!(response["result"]["isError"], true, "{host}: {response}");
            let text = response["result"]["content"][0]["text"].as_str().unwrap();
            assert!(text.starts_with(unavailable), "{host}: {text}");
            assert!(response["result"]["structuredContent"].is_null(), "{host}");
        }
        assert_eq!(
            startup
                .lines()
                .filter(
                    |line| *line == "HTTP MCP runs without a bearer; content tools are disabled."
                )
                .count(),
            1,
            "{host}: {startup}"
        );
    }
}
