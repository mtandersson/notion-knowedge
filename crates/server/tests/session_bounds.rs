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
