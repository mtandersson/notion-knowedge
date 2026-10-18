use super::*;
use crate::{
    notion_oauth_redirect::NotionOAuthConfig, oauth_code::ApprovedNotionGrant,
    oauth_discovery::OAuthDiscovery,
};
use axum::{Router, routing::post};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
    available: Arc<AtomicBool>,
    mutation_status: Arc<AtomicUsize>,
    mutations: Arc<AtomicUsize>,
    mutation_gate: Arc<tokio::sync::Semaphore>,
    mutation_started: Arc<tokio::sync::Notify>,
    page_status: Arc<AtomicUsize>,
    page_reads: Arc<AtomicUsize>,
    page_gate: Arc<tokio::sync::Semaphore>,
    pause_page: Arc<AtomicBool>,
    probes: Arc<AtomicUsize>,
    active: Arc<AtomicBool>,
    probe_gate: Arc<tokio::sync::Semaphore>,
    probe_started: Arc<tokio::sync::Notify>,
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
        Self::installed(body, status, None, std::time::Duration::ZERO).await
    }
    async fn installed(
        body: serde_json::Value,
        status: StatusCode,
        limits: Option<(usize, usize, usize)>,
        clock_age: std::time::Duration,
    ) -> Self {
        Self::with_content(body, status, limits, clock_age, None).await
    }
    async fn with_content(
        body: serde_json::Value,
        status: StatusCode,
        limits: Option<(usize, usize, usize)>,
        clock_age: std::time::Duration,
        content_scope: Option<notion_knowledge_core::lifecycle::LifecycleScope>,
    ) -> Self {
        Self::with_authority(body, status, limits, clock_age, content_scope, false).await
    }
    async fn with_authority(
        body: serde_json::Value,
        status: StatusCode,
        limits: Option<(usize, usize, usize)>,
        clock_age: std::time::Duration,
        content_scope: Option<notion_knowledge_core::lifecycle::LifecycleScope>,
        writes: bool,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let policy = NotionOwnerPolicy::new(
            content_scope
                .as_ref()
                .map(|scope| scope.workspace_id.as_str())
                .unwrap_or("workspace-a"),
            "user-a",
        )
        .unwrap();
        let store = Arc::new(
            GrantStore::open(
                &key,
                &dir.path().join("grant"),
                "notion-client",
                policy.clone(),
            )
            .unwrap(),
        );
        let available = Arc::new(AtomicBool::new(true));
        let token_available = available.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let provider_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_port = provider_socket.local_addr().unwrap().port();
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
                    let available = token_available.clone();
                    async move {
                        assert_eq!(
                            headers["authorization"],
                            "Basic bm90aW9uLWNsaWVudDpjbGllbnQtc2VjcmV0"
                        );
                        let mut response = body;
                        if request["grant_type"] == "authorization_code" {
                            assert_eq!(request["code"], "provider-code");
                            assert_eq!(
                                request["redirect_uri"],
                                "https://auth.example.com/oauth/notion/callback"
                            );
                            assert_eq!(request.as_object().unwrap().len(), 3);
                        } else {
                            assert_eq!(request["grant_type"], "refresh_token");
                            assert_eq!(request["refresh_token"], "provider-refresh-secret");
                            assert_eq!(request.as_object().unwrap().len(), 2);
                            response["access_token"] = "rotated-access-secret".into();
                            response["refresh_token"] = "rotated-refresh-secret".into();
                            response.as_object_mut().unwrap().remove("expires_in");
                        }
                        seen.fetch_add(1, Ordering::SeqCst);
                        (
                            if available.load(Ordering::SeqCst) {
                                status
                            } else {
                                StatusCode::SERVICE_UNAVAILABLE
                            },
                            axum::Json(response),
                        )
                    }
                },
            ),
        );
        let probes = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicBool::new(true));
        let probe_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let probe_started = Arc::new(tokio::sync::Notify::new());
        let gate = probe_gate.clone();
        let started_probe = probe_started.clone();
        let seen_probe = probes.clone();
        let provider_active = active.clone();
        let introspect_available = available.clone();
        let provider_router = provider_router.route(
            "/v1/oauth/introspect",
            post(
                move |headers: HeaderMap, axum::Json(request): axum::Json<serde_json::Value>| {
                    let gate = gate.clone();
                    let started_probe = started_probe.clone();
                    let seen_probe = seen_probe.clone();
                    let provider_active = provider_active.clone();
                    let available = introspect_available.clone();
                    async move {
                        assert_eq!(
                            headers["authorization"],
                            "Basic bm90aW9uLWNsaWVudDpjbGllbnQtc2VjcmV0"
                        );
                        assert_eq!(headers["notion-version"], "2026-03-11");
                        assert!(matches!(
                            request["token"].as_str(),
                            Some("provider-access-secret" | "rotated-access-secret")
                        ));
                        assert_eq!(request.as_object().unwrap().len(), 1);
                        seen_probe.fetch_add(1, Ordering::SeqCst);
                        started_probe.notify_one();
                        let _permit = gate.acquire().await.unwrap();
                        (if available.load(Ordering::SeqCst) { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE }, axum::Json(
                            serde_json::json!({"active":provider_active.load(Ordering::SeqCst)}),
                        ))
                    }
                },
            ),
        );
        let page_status = Arc::new(AtomicUsize::new(200));
        let page_reads = Arc::new(AtomicUsize::new(0));
        let page_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let pause_page = Arc::new(AtomicBool::new(false));
        let page_state = page_status.clone();
        let reads = page_reads.clone();
        let page_wait = page_gate.clone();
        let pause = pause_page.clone();
        let root = content_scope
            .as_ref()
            .map(|scope| scope.roots[0].0.clone())
            .unwrap_or("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into());
        let provider_router = provider_router.route("/v1/pages/{id}", axum::routing::get(move |axum::extract::Path(id): axum::extract::Path<String>, headers: HeaderMap| {
            let root = root.clone(); let status = page_state.clone(); let reads = reads.clone();
            let wait = page_wait.clone(); let pause = pause.clone();
            async move {
                assert!(matches!(headers["authorization"].to_str().unwrap(), "Bearer provider-access-secret" | "Bearer rotated-access-secret"));
                reads.fetch_add(1, Ordering::SeqCst);
                if pause.swap(false, Ordering::SeqCst) { let _hold = wait.acquire().await.unwrap(); }
                (StatusCode::from_u16(status.load(Ordering::SeqCst) as u16).unwrap(), axum::Json(serde_json::json!({
                    "object":"page", "id":id, "url":format!("https://www.notion.so/{id}"), "last_edited_time":"2026-10-17T00:00:00.000Z", "in_trash":false,
                    "parent":if id == root { serde_json::json!({"type":"workspace","workspace":true}) } else { serde_json::json!({"type":"page_id","page_id":root}) },
                    "properties":{"Title":{"id":"title","type":"title","title":[{"plain_text":"Saffron private evidence"}]}}
                })))
            }
        })).route("/v1/pages/{id}/markdown", axum::routing::get(|axum::extract::Path(id):axum::extract::Path<String>, headers:HeaderMap| async move {
            assert!(matches!(headers["authorization"].to_str().unwrap(), "Bearer provider-access-secret" | "Bearer rotated-access-secret"));
            axum::Json(serde_json::json!({"object":"page_markdown","id":id,"markdown":"Saffron private evidence fresh content", "truncated":false,"unknown_block_ids":[]}))
        }));
        let mutation_status = Arc::new(AtomicUsize::new(200));
        let mutations = Arc::new(AtomicUsize::new(0));
        let mutation_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let mutation_started = Arc::new(tokio::sync::Notify::new());
        let effects = mutations.clone();
        let held = mutation_gate.clone();
        let signal = mutation_started.clone();
        let effect_status = mutation_status.clone();
        let patch = move |axum::extract::Path(id): axum::extract::Path<String>,
                          axum::Json(body): axum::Json<serde_json::Value>| {
            let effects = effects.clone();
            let held = held.clone();
            let signal = signal.clone();
            let status = effect_status.clone();
            async move {
                assert!(matches!(
                    body["type"].as_str(),
                    Some("insert_content" | "update_content")
                ));
                effects.fetch_add(1, Ordering::SeqCst);
                signal.notify_one();
                let _permit = held.acquire().await.unwrap();
                (
                    StatusCode::from_u16(status.load(Ordering::SeqCst) as u16).unwrap(),
                    [("Retry-After", "1")],
                    axum::Json(
                        serde_json::json!({"object":"page_markdown","id":id,"truncated":false,"unknown_block_ids":[]}),
                    ),
                )
            }
        };
        let effects = mutations.clone();
        let held = mutation_gate.clone();
        let signal = mutation_started.clone();
        let provider_router = provider_router.route("/v1/pages/{id}/markdown", axum::routing::patch(patch))
            .route("/v1/pages", axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let effects = effects.clone(); let held = held.clone(); let signal = signal.clone();
                async move {
                    assert!(body["parent"]["page_id"].is_string());
                    effects.fetch_add(1, Ordering::SeqCst); signal.notify_one();
                    let _permit = held.acquire().await.unwrap();
                    axum::Json(serde_json::json!({"object":"page","id":"cccccccc-cccc-cccc-cccc-cccccccccccc","url":"https://www.notion.so/cccccccc-cccc-cccc-cccc-cccccccccccc"}))
                }
            }));
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
        let started = std::time::Instant::now() - clock_age;
        let wall_origin = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let refresh_elapsed = clock.clone();
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
            .with_write_policy(writes)
            .with_clock(Arc::new(move || {
                started + std::time::Duration::from_secs(elapsed.load(Ordering::SeqCst))
            })),
            redirect: NotionOAuthRedirect::new(registration.clone()),
            sessions: std::sync::Mutex::new(None),
            revocation_admission: Arc::new(tokio::sync::Semaphore::new(4)),
            cleanup_gate: std::sync::Mutex::new(None),
            callback: Some(CallbackRuntime {
                store: store.clone(),
                credentials: NotionOAuthRuntime::fixture_with_clock(
                    store.clone(),
                    NotionTokenClient::new(&registration, "client-secret")
                        .unwrap()
                        .for_local_fixture(&endpoint),
                    policy.clone(),
                    Arc::new(move || wall_origin + refresh_elapsed.load(Ordering::SeqCst)),
                )
                .with_read_fixture(provider_port),
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
        let discovery = OAuthDiscovery::new(
            "https://auth.example.com",
            "https://knowledge.example.com/mcp",
        )
        .unwrap()
        .with_write_policy(writes);
        let transport = if let Some((requests, sessions, streams)) = limits {
            let mut settings = Config::from_lookup(|_| None).unwrap();
            settings.http_bind = bind;
            settings.oauth_discovery = Some(discovery.clone());
            settings.read_only = false;
            settings.destructive_writes_enabled = true;
            settings.mcp_max_in_flight = requests;
            settings.mcp_max_sessions = sessions;
            settings.mcp_max_streams = streams;
            settings.notion_scope = content_scope.clone();
            if content_scope.is_some() {
                settings.mcp_tool_capacity = 2;
            }
            let handler = notion_knowledge_mcp::KnowledgeServer::default();
            #[cfg(all(unix, feature = "webhook-worker"))]
            let handler = if let Some(scope) = &content_scope {
                let seeded =
                    content_index::seed(dir.path(), scope, "cccccccc-cccc-cccc-cccc-cccccccccccc")
                        .await;
                settings.graph_index = Some(seeded.index.clone());
                settings.graph_boost.enabled = true;
                settings.webhook_state_file = Some(seeded.state);
                settings.retrieval = Some(crate::webhook_worker::WorkerSettings {
                    alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![])
                        .unwrap(),
                    index: seeded.index,
                    assets: "/tmp/unused-oauth-provider".into(),
                    poll_ms: 100,
                    timeout_seconds: 30,
                });
                crate::retrieval_runtime::compose_oauth_queries(handler, &settings, seeded.provider)
                    .await
                    .unwrap()
            } else {
                handler
            };
            #[cfg(not(all(unix, feature = "webhook-worker")))]
            let handler = if content_scope.is_some() {
                handler.with_request_bound_content()
            } else {
                handler
            };
            Some(crate::http::mcp_transport(
                &settings,
                handler,
                Arc::new(crate::session_limits::BoundedSessions::new(
                    sessions,
                    std::time::Duration::from_secs(30),
                )),
                tokio_util::sync::CancellationToken::new(),
                Some(login.clone()),
            ))
        } else {
            None
        };
        let router = crate::oauth_discovery::router_with_mcp(
            bind,
            discovery,
            Some(login.clone()),
            transport,
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
            available,
            mutation_status,
            mutations,
            mutation_gate,
            mutation_started,
            page_status,
            page_reads,
            page_gate,
            pause_page,
            probes,
            active,
            probe_gate,
            probe_started,
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
        self.approved_code_with_scope("knowledge:read").await
    }
    async fn approved_code_with_scope(&self, scope: &str) -> String {
        let (callback, cookie) = self.begin_with_scope(scope).await;
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
        self.begin_with_scope("knowledge:read").await
    }
    async fn begin_with_scope(&self, scope: &str) -> (String, String) {
        let mut url = reqwest::Url::parse(&format!("{}/authorize", self.base)).unwrap();
        url.query_pairs_mut().extend_pairs([
            ("client_id", "chatgpt"),
            ("redirect_uri", CLIENT_REDIRECT),
            ("response_type", "code"),
            ("resource", "https://knowledge.example.com/mcp"),
            ("scope", scope),
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
async fn unknown_and_cross_login_states_preserve_both_legitimate_browser_transactions() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        std::time::Duration::ZERO,
    )
    .await;
    let (first_callback, first_cookie) = h.begin().await;
    let (second_callback, second_cookie) = h.begin().await;
    assert_ne!(first_callback, second_callback);
    assert_ne!(first_cookie, second_cookie);
    let mut unknown = reqwest::Url::parse(&first_callback).unwrap();
    unknown.query_pairs_mut().clear().extend_pairs([
        ("code", "provider-code"),
        ("state", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    ]);
    assert_ne!(unknown.as_str(), first_callback);
    for (callback, cookie) in [
        (unknown.as_str(), first_cookie.as_str()),
        (first_callback.as_str(), second_cookie.as_str()),
        (second_callback.as_str(), first_cookie.as_str()),
    ] {
        let denied = h
            .client
            .get(callback)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(denied.headers()[header::CACHE_CONTROL], "no-store");
        assert!(!denied.headers().contains_key(header::LOCATION));
        assert_eq!(
            denied.text().await.unwrap(),
            "Notion authorization could not be completed. Please start again."
        );
    }
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.probes.load(Ordering::SeqCst), 0);
    assert!(h.store.load().unwrap().is_none());
    for (index, (callback, cookie)) in [
        (first_callback, first_cookie),
        (second_callback, second_cookie),
    ]
    .into_iter()
    .enumerate()
    {
        let accepted = h
            .client
            .get(callback)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::FOUND);
        let redirect =
            reqwest::Url::parse(accepted.headers()[header::LOCATION].to_str().unwrap()).unwrap();
        let code = redirect
            .query_pairs()
            .find(|(key, _)| key == "code")
            .unwrap()
            .1
            .into_owned();
        let token = resource::redeem(&h, &code).await;
        let (session, _) = resource::initialize(&h, &token).await;
        assert!(!session.is_empty());
        assert_eq!(h.calls.load(Ordering::SeqCst), index + 1);
    }
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
    assert_eq!(h.probes.load(Ordering::SeqCst), 0);
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
    assert_eq!(h.probes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn token_minting_requires_live_provider_state_and_rejects_failed_code_reuse() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let code = h.approved_code().await;
    h.active.store(false, Ordering::SeqCst);
    let denied = h
        .token_request(encode_form(&token_params(&code)))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        denied.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"error":"invalid_grant"})
    );
    assert_eq!(h.probes.load(Ordering::SeqCst), 1);
    h.active.store(true, Ordering::SeqCst);
    let replay = h
        .token_request(encode_form(&token_params(&code)))
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(h.probes.load(Ordering::SeqCst), 1);
    let valid = h.approved_code().await;
    assert_eq!(
        h.token_request(encode_form(&token_params(&valid)))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn token_exchange_supports_documented_unknown_expiry_and_atomic_refresh() {
    for expiry in [None, Some(1u64)] {
        let mut provider_grant = grant("workspace-a", "user-a");
        match expiry {
            Some(seconds) => provider_grant["expires_in"] = seconds.into(),
            None => {
                provider_grant.as_object_mut().unwrap().remove("expires_in");
            }
        }
        let h = Harness::new(provider_grant, StatusCode::OK).await;
        let code = h.approved_code().await;
        let before = h.store.load_for_refresh().unwrap().unwrap();
        let issued = h
            .token_request(encode_form(&token_params(&code)))
            .send()
            .await
            .unwrap();
        assert_eq!(issued.status(), StatusCode::OK);
        let result: serde_json::Value = issued.json().await.unwrap();
        assert_eq!(result["token_type"], "Bearer");
        assert!(h.probes.load(Ordering::SeqCst) > 0);
        let after = h.store.load_for_refresh().unwrap().unwrap();
        assert_eq!(after.expires_at_unix(), None);
        assert_eq!(after.grant_id(), before.grant_id());
        assert_eq!(after.epoch(), before.epoch());
        assert_eq!(
            h.calls.load(Ordering::SeqCst),
            if expiry.is_some() { 2 } else { 1 }
        );
        for secret in [
            "provider-access-secret",
            "provider-refresh-secret",
            "rotated-access-secret",
            "rotated-refresh-secret",
        ] {
            assert!(!result.to_string().contains(secret));
        }
    }
}

#[tokio::test]
async fn provider_unavailability_never_mints_or_reuses_a_claimed_code() {
    let mut h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    let code = h.approved_code().await;
    h.provider.abort();
    let _ = (&mut h.provider).await;
    let form = encode_form(&token_params(&code));
    let unavailable = h.token_request(form.clone()).send().await.unwrap();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        unavailable.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"error":"temporarily_unavailable"})
    );
    let replay = h.token_request(form).send().await.unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"error":"invalid_grant"})
    );
}

#[tokio::test]
async fn pending_provider_verification_cannot_outlive_code_or_grant() {
    for replace_grant in [false, true] {
        let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
        let code = h.approved_code().await;
        let permit = h.probe_gate.clone().acquire_owned().await.unwrap();
        let request = h.token_request(encode_form(&token_params(&code)));
        let pending = tokio::spawn(async move { request.send().await.unwrap() });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            h.probe_started.notified(),
        )
        .await
        .expect("claimed code must reach provider verification");
        let replacement = if replace_grant {
            Some(h.approved_code().await)
        } else {
            h.clock.store(121, Ordering::SeqCst);
            None
        };
        drop(permit);
        let denied = pending.await.unwrap();
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            denied.json::<serde_json::Value>().await.unwrap(),
            serde_json::json!({"error":"invalid_grant"})
        );
        let replay = h
            .token_request(encode_form(&token_params(&code)))
            .send()
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
        assert_eq!(h.probes.load(Ordering::SeqCst), 1);
        if let Some(code) = replacement {
            assert_eq!(
                h.token_request(encode_form(&token_params(&code)))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
    }
}

#[path = "tests/resource.rs"]
mod resource;

#[path = "tests/content.rs"]
mod content;
#[cfg(all(unix, feature = "webhook-worker"))]
#[path = "tests/content_index.rs"]
mod content_index;

mod revocation;

#[path = "tests/write_authority.rs"]
mod write_authority;
