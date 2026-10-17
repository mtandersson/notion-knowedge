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
    assert_eq!(
        server["authorization_endpoint"],
        format!("{issuer}/authorize")
    );
    assert_eq!(server["token_endpoint"], format!("{issuer}/token"));
    assert_eq!(server["revocation_endpoint"], format!("{issuer}/revoke"));
    assert_eq!(server["response_types_supported"], json!(["code"]));
    assert_eq!(
        server["grant_types_supported"],
        json!(["authorization_code"])
    );
    assert_eq!(server["code_challenge_methods_supported"], json!(["S256"]));
    assert_eq!(
        server["token_endpoint_auth_methods_supported"],
        json!(["none"])
    );
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
        (
            Some("http://auth.example.com"),
            Some("https://mcp.example.com/mcp"),
        ),
        (Some("https://auth.example.com"), None),
        (None, Some("https://mcp.example.com/mcp")),
        (
            Some("https://auth.example.com"),
            Some("https://other.example.com/private"),
        ),
        (
            Some("https://auth.example.com#fragment"),
            Some("https://mcp.example.com/mcp"),
        ),
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

#[tokio::test]
async fn registered_browser_authorization_redirects_only_valid_pkce_requests_to_notion() {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_OAUTH_ISSUER", "https://auth.example.com")
        .env("NK_OAUTH_RESOURCE", "https://knowledge.example.com/mcp")
        .env("NK_OAUTH_CLIENT_ID", "chatgpt")
        .env(
            "NK_OAUTH_CLIENT_REDIRECT_URI",
            "https://chatgpt.example.com/callback",
        )
        .env("NK_NOTION_OAUTH_CLIENT_ID", "notion-client")
        .env(
            "NK_NOTION_OAUTH_REDIRECT_URI",
            "https://auth.example.com/oauth/notion/callback",
        )
        .env("NK_NOTION_OAUTH_CLIENT_SECRET", "fixture-server-secret")
        .env("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-a")
        .env("NK_NOTION_ALLOWED_USER_ID", "user-a")
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
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let params = [
        ("response_type", "code"),
        ("client_id", "chatgpt"),
        ("redirect_uri", "https://chatgpt.example.com/callback"),
        ("resource", "https://knowledge.example.com/mcp"),
        ("state", "client-state-123456789"),
        (
            "code_challenge",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        ),
        ("code_challenge_method", "S256"),
        ("scope", "knowledge:read"),
    ];
    let authorize_url = |params: &[(&str, &str)]| {
        let mut url = reqwest::Url::parse(&format!("{base}/authorize")).unwrap();
        url.query_pairs_mut().extend_pairs(params.iter().copied());
        url
    };
    let mut upstream_states = Vec::new();
    for _ in 0..2 {
        let response = client.get(authorize_url(&params)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["referrer-policy"], "no-referrer");
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        assert!(cookie.starts_with("__Host-nk-oauth="));
        for flag in [
            "Path=/",
            "Secure",
            "HttpOnly",
            "SameSite=Lax",
            "Max-Age=120",
        ] {
            assert!(cookie.contains(flag));
        }
        let location = response.headers()["location"].to_str().unwrap();
        assert!(!location.contains("fixture-server-secret"));
        assert!(!location.contains(cookie.split(';').next().unwrap().split('=').nth(1).unwrap()));
        let url = reqwest::Url::parse(location).unwrap();
        assert_eq!(url.origin().ascii_serialization(), "https://api.notion.com");
        assert_eq!(url.path(), "/v1/oauth/authorize");
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.len(), 5);
        assert_eq!(query["owner"], "user");
        assert_eq!(query["client_id"], "notion-client");
        assert_eq!(
            query["redirect_uri"],
            "https://auth.example.com/oauth/notion/callback"
        );
        assert_eq!(query["response_type"], "code");
        assert_eq!(query["state"].len(), 43);
        assert_ne!(query["state"], params[4].1);
        upstream_states.push(query["state"].clone());
    }
    assert_ne!(upstream_states[0], upstream_states[1]);
    for (key, invalid) in [
        ("client_id", "unregistered"),
        ("redirect_uri", "https://attacker.example.com/callback"),
        ("resource", "https://foreign.example.com/mcp"),
        ("code_challenge_method", "plain"),
        ("code_challenge", "bad"),
        ("scope", "knowledge:write"),
        ("response_type", "token"),
    ] {
        let mut denied = params;
        denied.iter_mut().find(|(k, _)| *k == key).unwrap().1 = invalid;
        let response = client.get(authorize_url(&denied)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{key}");
        assert!(!response.headers().contains_key("location"));
        assert!(!response.headers().contains_key("set-cookie"));
    }
    let valid_url = authorize_url(&params).to_string();
    for suffix in [
        "&client_id=chatgpt",
        "&%63lient_id=chatgpt",
        "&extra=value",
        "&state=%GG",
    ] {
        let response = client
            .get(format!("{valid_url}{suffix}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let spoofed = client
        .get(&valid_url)
        .header("host", "evil.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(spoofed.status(), StatusCode::FORBIDDEN);
    for path in ["/token", "/revoke"] {
        assert_eq!(
            client
                .post(format!("{base}{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(
        client
            .post(format!("{base}/mcp"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}
