//! Installed transport regression tests for #397 admission (partial #396).
use std::{process::Stdio, time::Duration};

use reqwest::{Client, StatusCode};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};

async fn server(extra: &[(&str, String)]) -> (Child, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
    command
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_HOST", "127.0.0.1")
        .env("NK_HTTP_PORT", port.to_string())
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
            assert_ne!(stderr.read_line(&mut line).await.unwrap(), 0);
            if line.contains("Serving MCP over Streamable HTTP") {
                break;
            }
        }
    })
    .await
    .unwrap();
    (child, port)
}

/// Keep a real incomplete POST in the body-reading state, after HTTP admission.
async fn hold_body(port: u16, path: &str) -> TcpStream {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let headers = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: 256\r\nConnection: keep-alive\r\n\r\n{{"
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.flush().await.unwrap();
    // Ensure server had time to accept the incomplete body and acquire its
    // permit before issuing the independent overload request.
    sleep(Duration::from_millis(150)).await;
    socket
}

#[tokio::test]
async fn installed_http_mcp_busy_response_does_not_block_health_and_recovers() {
    timeout(Duration::from_secs(20), async {
        let (mut child, port) = server(&[("NK_MCP_MAX_IN_FLIGHT", "1".into())]).await;
        let blocked = hold_body(port, "/mcp").await;
        let client = Client::builder().no_proxy().build().unwrap();
        let base = format!("http://127.0.0.1:{port}");
        let request = || {
            client.post(format!("{base}/mcp")).json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/list"
            }))
        };
        for _ in 0..3 {
            let response = request().send().await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["cache-control"], "no-store");
            assert_eq!(response.headers()["retry-after"], "1");
            let body = response.text().await.unwrap();
            assert!(body.contains("MCP server busy"));
            assert!(!body.contains("tools/list"));
        }
        assert_eq!(
            client
                .get(format!("{base}/livez"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        drop(blocked);
        let mut recovered = false;
        for _ in 0..20 {
            if request().send().await.unwrap().status() != StatusCode::SERVICE_UNAVAILABLE {
                recovered = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(recovered, "release after incomplete request disconnected");
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn installed_webhook_admission_is_independent_and_fails_closed() {
    timeout(Duration::from_secs(20), async {
        let dir = tempfile::tempdir().unwrap();
        let id = "11111111-1111-4111-8111-111111111111";
        let state = dir.path().join("state.db");
        let (mut child, port) = server(&[
            ("NK_MCP_MAX_IN_FLIGHT", "1".into()),
            ("NK_WEBHOOK_MAX_IN_FLIGHT", "1".into()),
            ("NK_WEBHOOK_MODE", "verified".into()),
            (
                "NK_WEBHOOK_VERIFICATION_TOKEN",
                "test-verification-secret".into(),
            ),
            ("NK_WEBHOOK_WORKSPACE_ID", id.into()),
            ("NK_WEBHOOK_INTEGRATION_ID", id.into()),
            ("NK_WEBHOOK_SUBSCRIPTION_ID", id.into()),
            (
                "NK_WEBHOOK_STATE_FILE",
                state.to_string_lossy().into_owned(),
            ),
        ])
        .await;
        let blocked = hold_body(port, "/webhooks/notion").await;
        let client = Client::builder().no_proxy().build().unwrap();
        let base = format!("http://127.0.0.1:{port}");
        let hook = format!("{base}/webhooks/notion");
        let rejected = client
            .post(&hook)
            .body("private-fixture-body")
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = rejected.text().await.unwrap();
        assert!(!response.contains("private-fixture-body"));
        assert!(!response.contains("test-verification-secret"));
        assert_eq!(
            client
                .get(format!("{base}/livez"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        drop(blocked);
        let mut recovered = false;
        for _ in 0..20 {
            let response = client.post(&hook).body("{}").send().await.unwrap();
            if response.status() == StatusCode::UNAUTHORIZED {
                recovered = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(
            recovered,
            "webhook must release slot after cancelled request"
        );
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    })
    .await
    .unwrap();
}
