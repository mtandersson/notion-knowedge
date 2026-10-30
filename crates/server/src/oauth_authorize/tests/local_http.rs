use super::*;

#[tokio::test]
async fn local_http_browser_flow_keeps_cookie_pkce_and_owner_binding() {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = socket.local_addr().unwrap();
    let issuer = format!("http://localhost:{}", bind.port());
    let resource = format!("{issuer}/mcp");
    let callback = format!("{issuer}/oauth/notion/callback");
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("key");
    fs::write(&key, [42u8; 32]).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let grant_path = dir.path().join("grant");
    let settings = [
        ("NK_OAUTH_LOCAL_HTTP", "true".to_owned()),
        ("NK_HTTP_PORT", bind.port().to_string()),
        ("NK_OAUTH_ISSUER", issuer.clone()),
        ("NK_OAUTH_RESOURCE", resource.clone()),
        ("NK_NOTION_OAUTH_CLIENT_ID", "notion-client".into()),
        ("NK_NOTION_OAUTH_CLIENT_SECRET", "client-secret".into()),
        ("NK_NOTION_OAUTH_REDIRECT_URI", callback.clone()),
        ("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-a".into()),
        ("NK_NOTION_ALLOWED_USER_ID", "user-a".into()),
        ("NK_OAUTH_CLIENT_ID", "local-test".into()),
        (
            "NK_OAUTH_CLIENT_REDIRECT_URI",
            "http://localhost:3301/callback".into(),
        ),
        ("NK_NOTION_GRANT_KEY_FILE", key.display().to_string()),
        (
            "NK_NOTION_GRANT_STATE_FILE",
            grant_path.display().to_string(),
        ),
    ];
    let config = Config::from_lookup(|name| {
        settings
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.clone())
    })
    .unwrap();
    let runtime = NotionOAuthRuntime::from_config(&config).unwrap().unwrap();
    let mut login = Login::from_config(&config, Some(&runtime))
        .unwrap()
        .unwrap();
    let provider_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/oauth/token",
        provider_socket.local_addr().unwrap()
    );
    let expected_callback = callback.clone();
    let provider = tokio::spawn(async move {
        axum::serve(
            provider_socket,
            Router::new()
                .route(
                    "/v1/oauth/token",
                    post(move |axum::Json(request): axum::Json<serde_json::Value>| {
                        let expected_callback = expected_callback.clone();
                        async move {
                            assert_eq!(request["redirect_uri"], expected_callback);
                            axum::Json(grant("workspace-a", "user-a"))
                        }
                    }),
                )
                .route(
                    "/v1/oauth/introspect",
                    post(|| async { axum::Json(serde_json::json!({"active": true})) }),
                ),
        )
        .await
        .unwrap();
    });
    let provider_client = NotionTokenClient::new(
        config.notion_oauth_redirect.as_ref().unwrap(),
        "client-secret",
    )
    .unwrap()
    .for_local_fixture(&endpoint);
    let callback_runtime = Arc::get_mut(&mut login).unwrap().callback.as_mut().unwrap();
    callback_runtime.client = provider_client.clone();
    callback_runtime.credentials = NotionOAuthRuntime::fixture_with_clock(
        runtime.grant_store(),
        provider_client,
        config
            .notion_oauth_callback
            .as_ref()
            .unwrap()
            .allowed
            .clone(),
        Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        }),
    );
    let router = crate::oauth_discovery::router_with_login(
        bind,
        config.oauth_discovery.unwrap(),
        Some(login.clone()),
    );
    let server = tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let mut url = reqwest::Url::parse(&format!("{issuer}/authorize")).unwrap();
    url.query_pairs_mut().extend_pairs([
        ("client_id", "local-test"),
        ("redirect_uri", "http://localhost:3301/callback"),
        ("response_type", "code"),
        ("resource", resource.as_str()),
        ("scope", "knowledge:read"),
        ("state", "original-client-state-123"),
        ("code_challenge", CHALLENGE),
        ("code_challenge_method", "S256"),
    ]);
    let denied = client
        .get(url.clone())
        .header("host", "evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let denied = client
        .get(url.clone())
        .header("origin", "http://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let start = client.get(url).send().await.unwrap();
    assert_eq!(start.status(), StatusCode::FOUND);
    let cookie = start.headers()["set-cookie"].to_str().unwrap();
    assert!(cookie.starts_with("nk-oauth-local="));
    assert!(cookie.contains("HttpOnly; SameSite=Lax"));
    assert!(!cookie.contains("Secure"));
    let upstream = reqwest::Url::parse(start.headers()["location"].to_str().unwrap()).unwrap();
    assert_eq!(
        upstream.origin().ascii_serialization(),
        "https://api.notion.com"
    );
    let upstream_state = upstream
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let callback_url = format!("{callback}?code=provider-code&state={upstream_state}");
    let missing = client.get(&callback_url).send().await.unwrap();
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let response = client
        .get(&callback_url)
        .header("cookie", cookie.split(';').next().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert!(
        response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .starts_with("nk-oauth-local=;")
    );
    let destination =
        reqwest::Url::parse(response.headers()["location"].to_str().unwrap()).unwrap();
    assert_eq!(
        destination.origin().ascii_serialization(),
        "http://localhost:3301"
    );
    assert!(runtime.grant_store().load_for_refresh().unwrap().is_some());
    let code = destination
        .query_pairs()
        .find(|(k, _)| k == "code")
        .unwrap()
        .1
        .into_owned();
    let mut body = reqwest::Url::parse("https://form.invalid").unwrap();
    body.query_pairs_mut().extend_pairs([
        ("grant_type", "authorization_code"),
        ("client_id", "local-test"),
        ("redirect_uri", "http://localhost:3301/callback"),
        ("resource", resource.as_str()),
        ("code", code.as_str()),
        ("code_verifier", VERIFIER),
    ]);
    let form = body.query().unwrap();
    let redeemed = client
        .post(format!("{issuer}/token"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(form.to_owned())
        .send()
        .await
        .unwrap();
    assert_eq!(redeemed.status(), StatusCode::OK);
    let token: serde_json::Value = redeemed.json().await.unwrap();
    assert!(
        token["access_token"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(token["token_type"], "Bearer");
    let replayed_code = client
        .post(format!("{issuer}/token"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(form.to_owned())
        .send()
        .await
        .unwrap();
    assert_eq!(replayed_code.status(), StatusCode::BAD_REQUEST);
    let replay = client
        .get(&callback_url)
        .header("cookie", cookie.split(';').next().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    server.abort();
    provider.abort();
}
