use std::{process::Stdio, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

const TOKEN: &str = "local-dev-0123456789-abcdefghijklmnopqrstuvwxyz";
const OTHER: &str = "local-dev-0123456789-abcdefghijklmnopqrstuvwxyZ";

async fn exercise(health_protected: bool) {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut command = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
    command
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_HTTP_AUTH", "bearer")
        .env("NK_HTTP_BEARER_TOKEN", TOKEN)
        .env(
            "NK_HTTP_HEALTH_AUTH",
            if health_protected { "bearer" } else { "none" },
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let result = timeout(Duration::from_secs(15), async {
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(stderr.read_line(&mut line).await.unwrap(), 0);
            assert!(!line.contains(TOKEN));
            if line.contains("Serving MCP over Streamable HTTP") {
                break;
            }
        }

        let client = Client::builder().no_proxy().build().unwrap();
        let endpoint = format!("http://127.0.0.1:{port}/mcp");
        let health = format!("http://127.0.0.1:{port}/livez");
        let request = json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "dev-bearer-test", "version": "1"}
            }
        });
        let unauthorized = client
            .post(&endpoint)
            .header("accept", "application/json, text/event-stream")
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized.headers()["cache-control"], "no-store");
        assert_eq!(
            unauthorized.headers()["www-authenticate"],
            "Bearer realm=\"notion-knowledge-dev\""
        );
        assert!(!unauthorized.text().await.unwrap().contains(TOKEN));

        for credential in [
            "Basic local",
            "Bearer wrong",
            "Bearer ",
            "Bearer wrong extra",
        ] {
            let bad = client
                .post(&endpoint)
                .header("authorization", credential)
                .header("accept", "application/json, text/event-stream")
                .json(&request)
                .send()
                .await
                .unwrap();
            assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);
        }
        let wrong = client
            .post(&endpoint)
            .bearer_auth(OTHER)
            .header("accept", "application/json, text/event-stream")
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        let mut duplicate = reqwest::header::HeaderMap::new();
        duplicate.append("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        duplicate.append("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        let duplicate = client
            .post(&endpoint)
            .headers(duplicate)
            .header("accept", "application/json, text/event-stream")
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);

        let accepted = client
            .post(&endpoint)
            .bearer_auth(TOKEN)
            .header("accept", "application/json, text/event-stream")
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let session = accepted.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert!(!accepted.text().await.unwrap().contains(TOKEN));

        for method in ["GET", "DELETE"] {
            let response = client
                .request(method.parse().unwrap(), &endpoint)
                .header("mcp-session-id", &session)
                .header("mcp-protocol-version", "2025-03-26")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        let denied = client
            .post(&endpoint)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc": "2.0", "id": 2, "method": "ping"}))
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let initialized = client
            .post(&endpoint)
            .bearer_auth(TOKEN)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        assert_eq!(initialized.status(), StatusCode::ACCEPTED);

        let ping = client
            .post(&endpoint)
            .bearer_auth(TOKEN)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc": "2.0", "id": 3, "method": "ping"}))
            .send()
            .await
            .unwrap();
        assert_eq!(ping.status(), StatusCode::OK);
        assert!(!ping.text().await.unwrap().contains(TOKEN));

        for (header, value) in [
            ("host", "untrusted.example"),
            ("origin", "https://untrusted.example"),
        ] {
            let denied = client
                .post(&endpoint)
                .bearer_auth(TOKEN)
                .header(header, value)
                .header("accept", "application/json, text/event-stream")
                .json(&request)
                .send()
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        }

        let live_without = client.get(&health).send().await.unwrap();
        assert_eq!(
            live_without.status(),
            if health_protected {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::OK
            }
        );
        let live_with = client.get(&health).bearer_auth(TOKEN).send().await.unwrap();
        assert_eq!(live_with.status(), StatusCode::OK);
        let ready = client
            .get(format!("http://127.0.0.1:{port}/readyz"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            ready.status(),
            if health_protected {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        );

        let closed = client
            .delete(&endpoint)
            .bearer_auth(TOKEN)
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
            .send()
            .await
            .unwrap();
        assert_eq!(closed.status(), StatusCode::ACCEPTED);
    })
    .await;
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    result.expect("bearer integration smoke must finish within fifteen seconds");
}

#[tokio::test]
async fn fallback_bearer_authenticates_every_mcp_request_without_affecting_health() {
    exercise(false).await;
}

#[tokio::test]
async fn health_can_be_independently_protected_by_the_development_bearer() {
    exercise(true).await;
}
