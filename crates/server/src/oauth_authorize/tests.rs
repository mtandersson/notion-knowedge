use super::*;
use crate::{
    notion_oauth_redirect::NotionOAuthConfig, oauth_code::ApprovedNotionGrant,
    oauth_discovery::OAuthDiscovery,
};
use axum::{Router, routing::post};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const CLIENT_REDIRECT: &str = "https://chatgpt.example.com/callback?registered=value";

struct Harness {
    _dir: tempfile::TempDir,
    login: Arc<Login>,
    store: Arc<GrantStore>,
    base: String,
    client: reqwest::Client,
    calls: Arc<AtomicUsize>,
    clock: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
    provider: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        self.provider.abort();
    }
}
fn grant(workspace: &str, user: &str) -> serde_json::Value {
    serde_json::json!({"access_token":"provider-access-secret", "refresh_token":"provider-refresh-secret",
        "token_type":"bearer", "workspace_id":workspace, "owner":{"type":"user","user":{"id":user}},
        "bot_id":"bot-a", "expires_in":3600})
}
impl Harness {
    async fn new(body: serde_json::Value, status: StatusCode) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let policy = NotionOwnerPolicy::new("workspace-a", "user-a").unwrap();
        let store = Arc::new(
            GrantStore::open(
                &key,
                &dir.path().join("grant"),
                "notion-client",
                policy.clone(),
            )
            .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let provider_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/oauth/token",
            provider_socket.local_addr().unwrap()
        );
        let provider_router = Router::new().route(
            "/v1/oauth/token",
            post(
                move |headers: HeaderMap, axum::Json(request): axum::Json<serde_json::Value>| {
                    let seen = seen.clone();
                    let body = body.clone();
                    async move {
                        assert_eq!(
                            headers["authorization"],
                            "Basic bm90aW9uLWNsaWVudDpjbGllbnQtc2VjcmV0"
                        );
                        assert_eq!(request["grant_type"], "authorization_code");
                        assert_eq!(request["code"], "provider-code");
                        assert_eq!(
                            request["redirect_uri"],
                            "https://auth.example.com/oauth/notion/callback"
                        );
                        assert_eq!(request.as_object().unwrap().len(), 3);
                        seen.fetch_add(1, Ordering::SeqCst);
                        (status, axum::Json(body))
                    }
                },
            ),
        );
        let provider = tokio::spawn(async move {
            axum::serve(provider_socket, provider_router).await.unwrap();
        });
        let registration = NotionOAuthConfig::new(
            "notion-client",
            "https://auth.example.com/oauth/notion/callback",
            "https://auth.example.com",
        )
        .unwrap();
        let clock = Arc::new(AtomicU64::new(0));
        let elapsed = clock.clone();
        let started = std::time::Instant::now();
        let login = Arc::new(Login {
            flow: AuthorizationFlow::new(
                Client::new(
                    "chatgpt",
                    CLIENT_REDIRECT,
                    "https://knowledge.example.com/mcp",
                )
                .unwrap(),
                policy.authorization_identity(),
            )
            .with_clock(Arc::new(move || {
                started + std::time::Duration::from_secs(elapsed.load(Ordering::SeqCst))
            })),
            redirect: NotionOAuthRedirect::new(registration.clone()),
            callback: Some(CallbackRuntime {
                store: store.clone(),
                client: NotionTokenClient::new(&registration, "client-secret")
                    .unwrap()
                    .for_local_fixture(&endpoint),
                allowed: policy,
                admission: Arc::new(tokio::sync::Semaphore::new(4)),
            }),
        });
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let base = format!("http://{bind}");
        let router = crate::oauth_discovery::router_with_login(
            bind,
            OAuthDiscovery::new(
                "https://auth.example.com",
                "https://knowledge.example.com/mcp",
            )
            .unwrap(),
            Some(login.clone()),
        );
        let router = router.layer(axum::middleware::from_fn(crate::http::correlate_request));
        let server = tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        });
        Self {
            _dir: dir,
            login,
            store,
            base,
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
            calls,
            clock,
            server,
            provider,
        }
    }
    fn token_request(&self, body: String) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}/token", self.base))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
    }
    async fn approved_code(&self) -> String {
        let (callback, cookie) = self.begin().await;
        let response = self
            .client
            .get(callback)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let url =
            reqwest::Url::parse(response.headers()[header::LOCATION].to_str().unwrap()).unwrap();
        url.query_pairs()
            .find(|(key, _)| key == "code")
            .unwrap()
            .1
            .into_owned()
    }
    async fn begin(&self) -> (String, String) {
        let mut url = reqwest::Url::parse(&format!("{}/authorize", self.base)).unwrap();
        url.query_pairs_mut().extend_pairs([
            ("client_id", "chatgpt"),
            ("redirect_uri", CLIENT_REDIRECT),
            ("response_type", "code"),
            ("resource", "https://knowledge.example.com/mcp"),
            ("scope", "knowledge:read"),
            ("state", "original-client-state-123"),
            ("code_challenge", CHALLENGE),
            ("code_challenge_method", "S256"),
        ]);
        let response = self.client.get(url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let cookie = response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let upstream =
            reqwest::Url::parse(response.headers()["location"].to_str().unwrap()).unwrap();
        let state = upstream
            .query_pairs()
            .find(|(k, _)| k == "state")
            .unwrap()
            .1
            .into_owned();
        (
            format!(
                "{}/oauth/notion/callback?code=provider-code&state={state}",
                self.base
            ),
            cookie,
        )
    }
}

#[tokio::test]
async fn browser_callback_persists_allowlisted_grant_then_redirects_with_bound_code() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    let success = h
        .client
        .get(&callback)
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(success.status(), StatusCode::FOUND);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    assert_eq!(success.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(success.headers()[header::REFERRER_POLICY], "no-referrer");
    assert!(
        success.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    let location = success.headers()[header::LOCATION].to_str().unwrap();
    for secret in [
        "provider-access-secret",
        "provider-refresh-secret",
        "client-secret",
        "provider-code",
    ] {
        assert!(!location.contains(secret));
    }
    let destination = reqwest::Url::parse(location).unwrap();
    assert_eq!(
        destination.origin().ascii_serialization(),
        "https://chatgpt.example.com"
    );
    assert_eq!(destination.path(), "/callback");
    let params: std::collections::HashMap<_, _> = destination.query_pairs().into_owned().collect();
    assert_eq!(params.len(), 3);
    assert_eq!(params["registered"], "value");
    assert_eq!(params["state"], "original-client-state-123");
    assert_eq!(params["code"].len(), 43);
    let stored = h.store.load().unwrap().unwrap();
    assert_eq!(stored.workspace_id(), "workspace-a");
    assert_eq!(stored.owner_user_id(), "user-a");
    let form = encode_form(&token_params(&params["code"]));
    let response = h.token_request(form.clone()).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(response.headers()[header::PRAGMA], "no-cache");
    let issued: serde_json::Value = response.json().await.unwrap();
    assert_eq!(issued.as_object().unwrap().len(), 4);
    assert_eq!(issued["token_type"], "Bearer");
    assert_eq!(issued["expires_in"], 600);
    assert_eq!(issued["scope"], "knowledge:read");
    let bearer = issued["access_token"].as_str().unwrap();
    assert_eq!(bearer.len(), 43);
    for secret in [
        "provider-access-secret",
        "provider-refresh-secret",
        "client-secret",
    ] {
        assert!(!issued.to_string().contains(secret));
    }
    let binding = ApprovedNotionGrant {
        grant_id: stored.grant_id(),
        epoch: stored.epoch(),
        workspace_id: stored.workspace_id(),
        owner_type: "user",
        owner_user_id: stored.owner_user_id(),
    };
    assert!(h.login.flow.verify(bearer, &binding));
    let replayed = h.token_request(form).send().await.unwrap();
    assert_eq!(replayed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        replayed.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_grant"
    );
    for method in [
        reqwest::Method::POST,
        reqwest::Method::GET,
        reqwest::Method::DELETE,
    ] {
        assert_eq!(
            h.client
                .request(method, format!("{}/mcp", h.base))
                .bearer_auth(bearer)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let replay = h
        .client
        .get(callback)
        .header(header::COOKIE, cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.client
            .post(format!("{}/token", h.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        h.client
            .post(format!("{}/mcp", h.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn invalid_browser_correlation_never_exchanges_or_consumes_another_login() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    for candidate in [
        None,
        Some("__Host-nk-oauth=wrong".to_owned()),
        Some(format!("{cookie}; {cookie}")),
    ] {
        let mut request = h.client.get(&callback);
        if let Some(candidate) = candidate {
            request = request.header(header::COOKIE, candidate);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(header::LOCATION));
    }
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    assert!(h.store.load().unwrap().is_none());
    let allowed = h
        .client
        .get(callback)
        .header(header::COOKIE, cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::FOUND);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn denied_provider_identity_and_errors_never_persist_or_redirect() {
    let mut missing_workspace = grant("workspace-a", "user-a");
    missing_workspace
        .as_object_mut()
        .unwrap()
        .remove("workspace_id");
    let mut missing_user = grant("workspace-a", "user-a");
    missing_user["owner"]["user"] = serde_json::Value::Null;
    let mut wrong_owner = grant("workspace-a", "user-a");
    wrong_owner["owner"]["type"] = "workspace".into();
    for (body, status) in [
        (grant("foreign-workspace", "user-a"), StatusCode::OK),
        (grant("workspace-a", "foreign-user"), StatusCode::OK),
        (missing_workspace, StatusCode::OK),
        (missing_user, StatusCode::OK),
        (wrong_owner, StatusCode::OK),
        (
            serde_json::json!({"error":"provider-access-secret"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let h = Harness::new(body, status).await;
        let (callback, cookie) = h.begin().await;
        let response = h
            .client
            .get(callback)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(!response.headers().contains_key(header::LOCATION));
        assert_eq!(
            response.text().await.unwrap(),
            "Notion authorization could not be completed. Please start again."
        );
        assert!(h.store.load().unwrap().is_none());
        assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn callback_rejects_query_ambiguity_and_host_spoofing_before_provider_io() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    for suffix in [
        "&code=other",
        "&%73tate=other",
        "&error=access_denied",
        "&unknown=value",
        "&state=%GG",
    ] {
        let response = h
            .client
            .get(format!("{callback}{suffix}"))
            .header(header::COOKIE, &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let spoofed = h
        .client
        .get(&callback)
        .header(header::COOKIE, &cookie)
        .header(header::HOST, "evil.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(spoofed.status(), StatusCode::FORBIDDEN);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    assert!(h.store.load().unwrap().is_none());
    let permits = h
        .login
        .callback
        .as_ref()
        .unwrap()
        .admission
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    let overloaded = h
        .client
        .get(&callback)
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    drop(permits);
    assert_eq!(
        h.client
            .get(callback)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FOUND
    );
}

#[tokio::test]
async fn failed_sealed_commit_never_returns_an_mcp_authorization_code() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    // Corrupt state after startup: callback must not overwrite unknown state.
    let state_path = h._dir.path().join("grant");
    fs::write(&state_path, b"corrupt-state").unwrap();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
    let response = h
        .client
        .get(callback)
        .header(header::COOKIE, cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key(header::LOCATION));
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fs::read(state_path).unwrap(), b"corrupt-state");
}

#[tokio::test]
async fn concurrent_callback_replay_can_exchange_and_commit_at_most_once() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    let a = h.client.get(&callback).header(header::COOKIE, &cookie);
    let b = h.client.get(&callback).header(header::COOKIE, &cookie);
    let (a, b) = tokio::join!(a.send(), b.send());
    let mut statuses = [a.unwrap().status().as_u16(), b.unwrap().status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [302, 400]);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.store.load().unwrap().unwrap().epoch(), 1);
}

#[tokio::test]
async fn provider_consent_denial_consumes_state_without_exchanging_credentials() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let (callback, cookie) = h.begin().await;
    let denied = callback.replace(
        "code=provider-code",
        "error=access_denied&error_description=provider-secret",
    );
    let response = h
        .client
        .get(denied)
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.text().await.unwrap().contains("provider-secret"));
    let replay = h
        .client
        .get(callback)
        .header(header::COOKIE, cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    assert!(h.store.load().unwrap().is_none());
}

#[tokio::test]
async fn identity_policy_denials_have_correlated_payload_free_logs() {
    // Run the real HTTP policy-denial cases in a separate test process so
    // stderr is observed without a global mutable log sink or parallel-test
    // interference. This is the same production router and request scope.
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "oauth_authorize::tests::denied_provider_identity_and_errors_never_persist_or_redirect",
            "--nocapture",
        ])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(15), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "policy fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    for secret in [
        "provider-access-secret",
        "provider-refresh-secret",
        "client-secret",
        "provider-code",
        "foreign-workspace",
        "foreign-user",
        "workspace-a",
        "user-a",
        "__Host-nk-oauth",
        "code=",
        "state=",
    ] {
        assert!(!stderr.contains(secret));
    }
    let events: Vec<serde_json::Value> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let denials: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event["operation"].as_str(),
                Some("notion_oauth_workspace" | "notion_oauth_user")
            )
        })
        .collect();
    assert_eq!(denials.len(), 2);
    for operation in ["notion_oauth_workspace", "notion_oauth_user"] {
        let event = denials
            .iter()
            .find(|event| event["operation"] == operation)
            .unwrap();
        assert_eq!(event["outcome"], "rejected");
        assert_eq!(event["level"], "warn");
        assert!(event["http_status"].is_null());
        assert_eq!(event.as_object().unwrap().len(), 7);
        assert!(events.iter().any(|http| http["operation"] == "http_request"
            && http["http_status"] == 400
            && http["correlation_id"] == event["correlation_id"]));
    }
}

fn token_params(code: &str) -> Vec<(&'static str, String)> {
    vec![
        ("grant_type", "authorization_code".into()),
        ("code", code.into()),
        ("client_id", "chatgpt".into()),
        ("redirect_uri", CLIENT_REDIRECT.into()),
        ("resource", "https://knowledge.example.com/mcp".into()),
        ("code_verifier", VERIFIER.into()),
    ]
}
fn encode_form(params: &[(&str, String)]) -> String {
    let mut url = reqwest::Url::parse("https://form.invalid").unwrap();
    url.query_pairs_mut()
        .extend_pairs(params.iter().map(|(key, value)| (*key, value.as_str())));
    url.query().unwrap().to_owned()
}

#[tokio::test]
async fn token_endpoint_rejects_bound_field_changes_and_expired_or_replayed_codes() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    for (key, wrong) in [
        ("grant_type", "refresh_token"),
        ("client_id", "other-client"),
        ("redirect_uri", "https://evil.example.com/cb"),
        ("resource", "https://foreign.example.com/mcp"),
        (
            "code_verifier",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        ("code", "not-an-issued-code"),
    ] {
        let code = h.approved_code().await;
        let mut params = token_params(&code);
        params.iter_mut().find(|(name, _)| *name == key).unwrap().1 = wrong.into();
        let response = h.token_request(encode_form(&params)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{key}");
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert!(body.get("access_token").is_none());
        if key == "code_verifier" {
            assert_eq!(
                h.token_request(encode_form(&token_params(&code)))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
    }
    let code = h.approved_code().await;
    h.clock.store(121, Ordering::SeqCst);
    let expired = h
        .token_request(encode_form(&token_params(&code)))
        .send()
        .await
        .unwrap();
    assert_eq!(expired.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        expired.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_grant"
    );
}

#[tokio::test]
async fn token_endpoint_requires_unambiguous_bounded_form_body_and_current_sealed_grant() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let code = h.approved_code().await;
    let form = encode_form(&token_params(&code));
    for suffix in [
        "&client_id=chatgpt",
        "&%63ode=duplicate",
        "&extra=value",
        "&scope=knowledge:write",
        "&code=%GG",
    ] {
        assert_eq!(
            h.token_request(format!("{form}{suffix}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let missing = encode_form(&token_params(&code)[..5]);
    assert_eq!(
        h.token_request(missing).send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    for content_type in [
        "application/json",
        "text/plain",
        "application/x-www-form-urlencoded; charset=invalid",
    ] {
        let response = h
            .client
            .post(format!("{}/token", h.base))
            .header(header::CONTENT_TYPE, content_type)
            .body(form.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    for header in [header::AUTHORIZATION, header::CONTENT_ENCODING] {
        assert_eq!(
            h.token_request(form.clone())
                .header(header, "unsupported")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let oversized = h.token_request("x".repeat(8193)).send().await.unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        oversized.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_request"
    );
    let query = h
        .client
        .post(format!("{}/token?code=ignored", h.base))
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(form.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::BAD_REQUEST);
    // Valid code is still single-use after malformed envelope requests.
    assert_eq!(
        h.token_request(form).send().await.unwrap().status(),
        StatusCode::OK
    );
    let old = h.approved_code().await;
    let new = h.approved_code().await;
    let replaced = h
        .token_request(encode_form(&token_params(&old)))
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        h.token_request(encode_form(&token_params(&new)))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let corrupt_code = h.approved_code().await;
    fs::write(h._dir.path().join("grant"), b"corrupt").unwrap();
    let corrupt = h
        .token_request(encode_form(&token_params(&corrupt_code)))
        .send()
        .await
        .unwrap();
    assert_eq!(corrupt.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        corrupt.json::<serde_json::Value>().await.unwrap()["error"],
        "temporarily_unavailable"
    );
}

#[tokio::test]
async fn concurrent_token_redemption_issues_at_most_one_bearer() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let code = h.approved_code().await;
    let form = encode_form(&token_params(&code));
    let (a, b) = tokio::join!(
        h.token_request(form.clone()).send(),
        h.token_request(form).send()
    );
    let mut statuses = [a.unwrap().status().as_u16(), b.unwrap().status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [200, 400]);
}
