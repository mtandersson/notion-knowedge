//! Logout is local, immediate, and independent of provider availability.
use super::resource::{continuation, initialize, mint, rpc};
use super::*;
use reqwest::Method;
use serde_json::json;
use std::time::Duration;

fn form(params: &[(&str, &str)]) -> String {
    encode_form(
        &params
            .iter()
            .map(|(key, value)| (*key, (*value).to_owned()))
            .collect::<Vec<_>>(),
    )
}
fn revoke(h: &Harness, token: &str) -> reqwest::RequestBuilder {
    h.client
        .post(format!("{}/revoke", h.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(form(&[("client_id", "chatgpt"), ("token", token)]))
}

#[tokio::test]
async fn logout_ends_existing_streams_and_denies_every_session_without_provider_io() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 2, 4)),
        Duration::ZERO,
    )
    .await;
    let token = mint(&h).await;
    let (first, _) = initialize(&h, &token).await;
    let (second, _) = initialize(&h, &token).await;
    let mut stream = continuation(&h, &token, &first, Method::GET)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    h.available.store(false, Ordering::SeqCst);
    let before = h.probes.load(Ordering::SeqCst);
    let response = revoke(&h, &token).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(response.bytes().await.unwrap().is_empty());
    tokio::time::timeout(Duration::from_secs(2), async {
        while stream.chunk().await.unwrap().is_some() {}
    })
    .await
    .expect("logout must wake an existing SSE stream");
    for session in [&first, &second] {
        for method in [Method::GET, Method::DELETE] {
            assert_eq!(
                continuation(&h, &token, session, method)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
        }
        assert_eq!(
            rpc(
                &h,
                &token,
                Some(session),
                json!({"jsonrpc":"2.0","id":3,"method":"ping"})
            )
            .send()
            .await
            .unwrap()
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
    for value in [&token, "unknown-token"] {
        let response = revoke(&h, value).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.bytes().await.unwrap().is_empty());
    }
    h.available.store(true, Ordering::SeqCst);
    let fresh = mint(&h).await;
    initialize(&h, &fresh).await;
    initialize(&h, &fresh).await;
}

#[tokio::test]
async fn malformed_revoke_envelopes_do_not_revoke_a_valid_bearer() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
    )
    .await;
    let token = mint(&h).await;
    let requests = [
        h.client
            .post(format!("{}/revoke?ignored=value", h.base))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form(&[("client_id", "chatgpt"), ("token", token.as_str())])),
        revoke(&h, &token).header("content-encoding", "gzip"),
        revoke(&h, &token).bearer_auth("unsupported"),
        h.client
            .post(format!("{}/revoke", h.base))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form(&[("client_id", "foreign"), ("token", token.as_str())])),
    ];
    for request in requests {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.text().await.unwrap().contains(&token));
    }
    initialize(&h, &token).await;
    let response = revoke(&h, &token)
        .body(form(&[
            ("client_id", "chatgpt"),
            ("token", token.as_str()),
            ("token_type_hint", "unknown"),
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn disconnected_logout_observer_cannot_abandon_session_cleanup_or_its_admission() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 2, 4)),
        Duration::ZERO,
    )
    .await;
    let token = mint(&h).await;
    initialize(&h, &token).await;
    initialize(&h, &token).await;
    let gate = Arc::new(CleanupGate {
        started: tokio::sync::Notify::new(),
        proceed: tokio::sync::Semaphore::new(0),
    });
    *h.login.cleanup_gate.lock().unwrap() = Some(gate.clone());
    // Drop the actual revoke future after local cancellation, while its owned
    // cleanup is paused. The same future is used by the HTTP handler.
    let login = h.login.clone();
    let bearer = token.clone();
    let pending = tokio::spawn(async move {
        let permit = login
            .revocation_admission
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        login.revoke_token("chatgpt", &bearer, permit).await
    });
    gate.started.notified().await;
    pending.abort();
    let _ = pending.await;
    assert_eq!(h.login.revocation_admission.available_permits(), 3);
    assert!(h.login.local_resource_authorization(&token).is_none());
    gate.proceed.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.login.revocation_admission.available_permits() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup retains admission until all SDK workers close");
    let fresh = mint(&h).await;
    initialize(&h, &fresh).await;
    initialize(&h, &fresh).await;
}

async fn same_grant_token(h: &Harness) -> String {
    let stored = h.store.load_for_refresh().unwrap().unwrap();
    let pending = h
        .login
        .flow
        .begin(&AuthorizationRequest {
            response_type: "code",
            client_id: "chatgpt",
            redirect_uri: CLIENT_REDIRECT,
            resource: "https://knowledge.example.com/mcp",
            state: "second-client-state-123",
            code_challenge: CHALLENGE,
            code_challenge_method: "S256",
            scope: "knowledge:read",
        })
        .unwrap();
    let next = h
        .login
        .flow
        .approve(
            pending.expose(),
            &ApprovedNotionGrant {
                grant_id: stored.grant_id(),
                epoch: stored.epoch(),
                workspace_id: stored.workspace_id(),
                owner_type: "user",
                owner_user_id: stored.owner_user_id(),
            },
        )
        .unwrap();
    super::resource::redeem(h, next.code.expose()).await
}

#[tokio::test]
async fn logout_revokes_only_the_exact_token_and_leaves_same_grant_peers_live() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
    )
    .await;
    let first = mint(&h).await;
    let second = same_grant_token(&h).await;
    let (first_session, _) = initialize(&h, &first).await;
    let (second_session, _) = initialize(&h, &second).await;
    assert_eq!(
        revoke(&h, &first).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        rpc(
            &h,
            &first,
            Some(&first_session),
            json!({"jsonrpc":"2.0","id":2,"method":"ping"})
        )
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = rpc(
        &h,
        &second,
        Some(&second_session),
        json!({"jsonrpc":"2.0","id":3,"method":"ping"}),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        super::resource::reply(response)
            .await
            .get("result")
            .is_some()
    );
}

#[tokio::test]
async fn revoked_token_cannot_complete_authorization_that_was_waiting_on_provider() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
    )
    .await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let block = h.probe_gate.acquire().await.unwrap();
    let before = h.probes.load(Ordering::SeqCst);
    let request = rpc(
        &h,
        &token,
        Some(&session),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/list"}),
    );
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.probes.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        revoke(&h, &token).send().await.unwrap().status(),
        StatusCode::OK
    );
    drop(block);
    let response = pending.await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!response.text().await.unwrap().contains(&token));
}

#[tokio::test]
async fn logout_before_initialize_response_cannot_leave_an_unbound_sdk_worker() {
    use axum::{Router, middleware, response::IntoResponse, routing::post};
    use rmcp::transport::streamable_http_server::session::SessionManager;
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
    )
    .await;
    let token = mint(&h).await;
    let sessions = Arc::new(crate::session_limits::BoundedSessions::new(
        1,
        Duration::from_secs(60),
    ));
    h.login.attach_sessions(&sessions);
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = crate::oauth_mcp::ResourceGate {
        login: Some(h.login.clone()),
        sessions: sessions.clone(),
        admission: Arc::new(tokio::sync::Semaphore::new(4)),
        metadata_url: "https://knowledge.example.com/.well-known/oauth-protected-resource/mcp"
            .into(),
        content: None,
    };
    let app = Router::new()
        .route(
            "/mcp",
            post({
                let sessions = sessions.clone();
                let entered = entered.clone();
                let resume = resume.clone();
                move || {
                    let sessions = sessions.clone();
                    let entered = entered.clone();
                    let resume = resume.clone();
                    async move {
                        // Hold the SDK response at the exact allocation -> middleware-bind boundary.
                        let (id, _transport) = sessions.create_session().await.unwrap();
                        entered.notify_one();
                        let _release = resume.acquire().await.unwrap();
                        (
                            StatusCode::OK,
                            [("mcp-session-id", id.to_string())],
                            "initialized",
                        )
                            .into_response()
                    }
                }
            }),
        )
        .layer(middleware::from_fn_with_state(
            gate,
            crate::oauth_mcp::require_authorization,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let request = h
        .client
        .post(format!("http://{address}/mcp"))
        .bearer_auth(&token);
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(!sessions.has_capacity());
    assert_eq!(
        revoke(&h, &token).send().await.unwrap().status(),
        StatusCode::OK
    );
    resume.add_permits(1);
    let denied = pending.await.unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert!(!denied.headers().contains_key("mcp-session-id"));
    assert!(
        sessions.has_capacity(),
        "late initialization must close its orphan and release capacity"
    );
    server.abort();
}
