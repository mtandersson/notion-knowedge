//! Full production HTTP discovery path exercised like a generic OAuth client.
//! No real tokens, Notion credentials or externally reachable listener needed.
use std::{process::Stdio, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

#[tokio::test]
async fn generic_oauth_client_discovers_metadata_but_cannot_access_unimplemented_auth() {
    let port_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = port_socket.local_addr().unwrap().port();
    drop(port_socket);

    let issuer = "https://auth.example.com";
    let resource = "https://knowledge.example.com/mcp";
    let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_HOST", "127.0.0.1")
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_OAUTH_ISSUER", issuer)
        .env("NK_OAUTH_RESOURCE", resource)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    timeout(Duration::from_secs(15), async {
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
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

    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let protected = client
        .get(format!("{base}/.well-known/oauth-protected-resource/mcp"))
        .send()
        .await
        .unwrap();
    assert_eq!(protected.status(), StatusCode::OK);
    assert_eq!(protected.headers()["cache-control"], "no-store");
    assert_eq!(protected.headers()["access-control-allow-origin"], "*");
    let protected: Value = protected.json().await.unwrap();
    assert_eq!(protected["resource"], resource);
    assert_eq!(protected["authorization_servers"], json!([issuer]));

    // RFC 9728 permits root discovery as an alternative entrypoint.
    let root: Value = client
        .get(format!("{base}/.well-known/oauth-protected-resource"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(root, protected);

    // A generic client follows the advertised issuer to its RFC 8414
    // well-known metadata, using local host routing only in this test.
    let server = client
        .get(format!("{base}/.well-known/oauth-authorization-server"))
        .send()
        .await
        .unwrap();
    assert_eq!(server.status(), StatusCode::OK);
    assert_eq!(server.headers()["cache-control"], "no-store");
    let server: Value = server.json().await.unwrap();
    assert_eq!(server["issuer"], issuer);
    assert_eq!(server["authorization_endpoint"], format!("{issuer}/authorize"));
    assert_eq!(server["token_endpoint"], format!("{issuer}/token"));
    assert_eq!(server["revocation_endpoint"], format!("{issuer}/revoke"));
    assert_eq!(server["response_types_supported"], json!(["code"]));
    assert_eq!(server["grant_types_supported"], json!(["authorization_code"]));
    assert_eq!(server["code_challenge_methods_supported"], json!(["S256"]));
    assert_eq!(server["token_endpoint_auth_methods_supported"], json!(["none"]));
    assert_eq!(server["protected_resources"], json!([resource]));

    // Advertised endpoints are deliberately unavailable, with no success or
    // token issuance until the remaining OAuth tasks are implemented.
    for (method, path) in [
        ("GET", "/authorize"),
        ("POST", "/token"),
        ("POST", "/revoke"),
    ] {
        let response = client
            .request(method.parse().unwrap(), format!("{base}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
    }

    // All MCP entry methods must deny unauthenticated calls, a forged
    // Authorization bearer and a reused session ID. Discovery isn't a login.
    for method in ["GET", "POST", "DELETE"] {
        for bearer in [None, Some("Bearer not-an-oauth-token")] {
            let mut request = client
                .request(method.parse().unwrap(), format!("{base}/mcp"))
                .header("mcp-session-id", "fake-session");
            if let Some(bearer) = bearer {
                request = request.header("authorization", bearer);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(response.headers()["cache-control"], "no-store");
            assert_eq!(
                response.headers()["www-authenticate"],
                "Bearer resource_metadata=\"https://knowledge.example.com/.well-known/oauth-protected-resource/mcp\""
            );
        }
    }

    // Host/Origin checks must also cover new metadata and token routes;
    // reverse proxies must rewrite Host to the trusted backend authority.
    for path in [
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-protected-resource/mcp",
        "/token",
    ] {
        for (header, value) in [
            ("host", "evil.example.com"),
            ("origin", "https://evil.example.com"),
        ] {
            let response = client
                .get(format!("{base}{path}"))
                .header(header, value)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
    child.kill().await.unwrap();
    let _ = child.wait().await.unwrap();
}

#[tokio::test]
async fn incomplete_or_non_https_discovery_configuration_fails_before_binding() {
    for (issuer, resource) in [
        (Some("http://auth.example.com"), Some("https://mcp.example.com/mcp")),
        (Some("https://auth.example.com"), None),
        (None, Some("https://mcp.example.com/mcp")),
        (Some("https://auth.example.com"), Some("https://other.example.com/private")),
        (Some("https://auth.example.com#fragment"), Some("https://mcp.example.com/mcp")),
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
        child.arg("--check").env_clear();
        if let Some(issuer) = issuer {
            child.env("NK_OAUTH_ISSUER", issuer);
        }
        if let Some(resource) = resource {
            child.env("NK_OAUTH_RESOURCE", resource);
        }
        let output = child.output().await.unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(!output.stdout.starts_with(b"{"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("https://other.example.com"));
    }
}
