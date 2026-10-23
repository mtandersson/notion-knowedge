//! Real HTTP tests through the same OAuth router, SDK, session and budget
//! composition as production. Every provider is a private local fake.
use super::*;
use reqwest::Method;
use serde_json::{Value, json};
use std::time::Duration;

async fn harness(limits: (usize, usize, usize)) -> Harness {
    Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some(limits),
        Duration::ZERO,
    )
    .await
}
pub(super) async fn mint(h: &Harness) -> String {
    let code = h.approved_code().await;
    redeem(h, &code).await
}
pub(super) async fn redeem(h: &Harness, code: &str) -> String {
    let response = h
        .token_request(encode_form(&token_params(code)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    value["access_token"].as_str().unwrap().to_owned()
}
pub(super) fn rpc(
    h: &Harness,
    bearer: &str,
    session: Option<&str>,
    message: Value,
) -> reqwest::RequestBuilder {
    let request = h
        .client
        .post(format!("{}/mcp", h.base))
        .bearer_auth(bearer)
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-03-26")
        .json(&message);
    if let Some(session) = session {
        request.header("mcp-session-id", session)
    } else {
        request
    }
}
pub(super) async fn reply(mut response: reqwest::Response) -> Value {
    if response
        .headers()
        .get("content-type")
        .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"))
    {
        return response.json().await.unwrap();
    }
    let mut text = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        text.push_str(std::str::from_utf8(&chunk).unwrap());
        for value in text.lines().filter_map(|line| line.strip_prefix("data: ")) {
            if let Ok(value) = serde_json::from_str::<Value>(value)
                && value.get("id").is_some()
            {
                return value;
            }
        }
    }
    panic!("missing RPC reply");
}
pub(super) async fn initialize(h: &Harness, token: &str) -> (String, Value) {
    let response = rpc(h, token, None, json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"oauth-fixture","version":"1"}
    }})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let session = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    (session, reply(response).await)
}
pub(super) fn continuation(
    h: &Harness,
    token: &str,
    session: &str,
    method: Method,
) -> reqwest::RequestBuilder {
    h.client
        .request(method, format!("{}/mcp", h.base))
        .bearer_auth(token)
        .header("mcp-session-id", session)
        .header("mcp-protocol-version", "2025-03-26")
        .header("accept", "application/json, text/event-stream")
}

#[tokio::test]
async fn live_oauth_initialization_tool_dispatch_notifications_and_delete_share_binding() {
    let h = harness((4, 4, 4)).await;
    let token = mint(&h).await;
    let (session, initialized) = initialize(&h, &token).await;
    assert_eq!(initialized["id"], 1);
    let response = rpc(
        &h,
        &token,
        Some(&session),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let listed = reply(
        rpc(
            &h,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .send()
        .await
        .unwrap(),
    )
    .await;
    let tools = listed["result"]["tools"]
        .as_array()
        .expect("typed auth evidence reached SDK context");
    assert!(tools.iter().any(|tool| tool["name"] == "knowledge_search"));
    assert!(tools.iter().all(|tool| matches!(
        tool["name"].as_str().unwrap(),
        "knowledge_search" | "knowledge_get" | "knowledge_related" | "knowledge_sync_status"
    )));
    let denied_write = reply(rpc(&h,&token,Some(&session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{}}})).send().await.unwrap()).await;
    assert!(denied_write.get("error").is_some());
    let read = reply(rpc(&h,&token,Some(&session),json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"knowledge_get","arguments":{"refs":[{"kind":"page","id":"11111111-1111-1111-1111-111111111111"}],"max_chars":1000}}})).send().await.unwrap()).await;
    assert!(
        read.to_string().contains("root_scope_unavailable"),
        "{read}"
    );
    let stream = continuation(&h, &token, &session, Method::GET)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    drop(stream);
    assert_eq!(
        continuation(&h, &token, &session, Method::DELETE)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    let before = h.probes.load(Ordering::SeqCst);
    assert_eq!(
        rpc(
            &h,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":5,"method":"ping"})
        )
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn malformed_unknown_expired_and_cross_token_sessions_deny_before_provider_work() {
    let h = harness((4, 4, 4)).await;
    let code = h.approved_code().await;
    let first = redeem(&h, &code).await;
    let (session, _) = initialize(&h, &first).await;
    // A second code approved from the SAME persisted grant creates a distinct
    // bearer without performing a new callback that would replace the grant.
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
    let second = redeem(&h, next.code.expose()).await;
    let before = h.probes.load(Ordering::SeqCst);
    for method in [Method::POST, Method::GET, Method::DELETE] {
        assert_eq!(
            continuation(&h, &second, &session, method)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for bearer in [
        "",
        "unknown-token",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        assert_eq!(
            rpc(
                &h,
                bearer,
                Some(&session),
                json!({"jsonrpc":"2.0","id":2,"method":"ping"})
            )
            .send()
            .await
            .unwrap()
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
    h.clock.store(601, Ordering::SeqCst);
    assert_eq!(
        continuation(&h, &first, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn grant_replacement_and_inactivity_deny_session_continuations() {
    let h = harness((4, 4, 4)).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    h.active.store(false, Ordering::SeqCst);
    for method in [Method::POST, Method::GET, Method::DELETE] {
        assert_eq!(
            continuation(&h, &token, &session, method)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    h.active.store(true, Ordering::SeqCst);
    h.approved_code().await;
    let before = h.probes.load(Ordering::SeqCst);
    assert_eq!(
        continuation(&h, &token, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn stalled_provider_admission_is_finite_for_all_methods() {
    let h = harness((1, 4, 4)).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let block = h.probe_gate.acquire().await.unwrap();
    let notified = h.probe_started.notified();
    // Drain any prior permit in Notify before starting this request.
    let before = h.probes.load(Ordering::SeqCst);
    let request = continuation(&h, &token, &session, Method::GET);
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.probes.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(notified);
    for method in [Method::POST, Method::GET, Method::DELETE] {
        let response = continuation(&h, &token, &session, method)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.text().await.unwrap();
        assert!(!body.contains(&token));
        assert!(!body.contains(&session));
    }
    assert_eq!(h.probes.load(Ordering::SeqCst), before + 1);
    drop(block);
    let response = pending.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
}

#[tokio::test]
async fn existing_sse_ends_at_token_expiry_and_cannot_be_reconnected() {
    let h = Harness::installed(
        grant("workspace-a", "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::from_secs(599),
    )
    .await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let deadline = h
        .login
        .local_resource_authorization(&token)
        .unwrap()
        .expires_at();
    let mut stream = continuation(&h, &token, &session, Method::GET)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    tokio::time::timeout(
        deadline.saturating_duration_since(std::time::Instant::now()) + Duration::from_millis(300),
        async { while stream.chunk().await.unwrap().is_some() {} },
    )
    .await
    .expect("an already open SSE body must end at bearer expiry");
    assert!(std::time::Instant::now() >= deadline);
    assert_eq!(
        continuation(&h, &token, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn oauth_session_stream_and_body_budgets_remain_enforced() {
    let h = harness((4, 1, 1)).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let extra = rpc(&h,&token,None,json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{
        "protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"extra","version":"1"}
    }})).send().await.unwrap();
    assert_eq!(extra.status(), StatusCode::SERVICE_UNAVAILABLE);
    let stream = continuation(&h, &token, &session, Method::GET)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    assert_eq!(
        continuation(&h, &token, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    // DELETE never consumes an SSE permit and can terminate an occupied session.
    assert_eq!(
        continuation(&h, &token, &session, Method::DELETE)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    drop(stream);
    let response = h
        .client
        .post(format!("{}/mcp", h.base))
        .bearer_auth(&token)
        .header("content-type", "text/plain")
        .body(vec![b'x'; 4 * 1024 * 1024 + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn provider_errors_and_failed_resource_refresh_never_dispatch_or_change_session_binding() {
    let mut response = grant("workspace-a", "user-a");
    response["expires_in"] = 360.into();
    let h = Harness::installed(response, StatusCode::OK, Some((4, 4, 4)), Duration::ZERO).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let stored = h.store.load_for_refresh().unwrap().unwrap();
    h.available.store(false, Ordering::SeqCst);
    assert_eq!(
        continuation(&h, &token, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let calls = h.calls.load(Ordering::SeqCst);
    // The MCP token remains valid; only the provider token crosses its refresh
    // boundary. An unavailable refresh must not issue stale credentials.
    h.set_refresh_clock_at_boundary(stored.expires_at_unix().unwrap());
    let denied = rpc(
        &h,
        &token,
        Some(&session),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/list"}),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(denied.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(h.calls.load(Ordering::SeqCst), calls + 1);
    let current = h.store.load_for_refresh().unwrap().unwrap();
    assert_eq!(current.grant_id(), stored.grant_id());
    assert_eq!(current.refresh_token(), stored.refresh_token());
    h.available.store(true, Ordering::SeqCst);
    let resumed = reply(
        rpc(
            &h,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":5,"method":"tools/list"}),
        )
        .send()
        .await
        .unwrap(),
    )
    .await;
    assert!(resumed["result"]["tools"].is_array());
    let refreshed = h.store.load_for_refresh().unwrap().unwrap();
    assert_eq!(refreshed.grant_id(), stored.grant_id());
    assert_eq!(refreshed.epoch(), stored.epoch());
    assert_eq!(refreshed.expires_at_unix(), None);
    assert_eq!(refreshed.refresh_token(), Some("rotated-refresh-secret"));
}

#[tokio::test]
async fn pending_resource_verification_cannot_outlive_token_or_approved_grant() {
    for replace in [false, true] {
        let h = harness((4, 4, 4)).await;
        let token = mint(&h).await;
        let (session, _) = initialize(&h, &token).await;
        let block = h.probe_gate.acquire().await.unwrap();
        let before = h.probes.load(Ordering::SeqCst);
        let request = rpc(
            &h,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
        );
        let pending = tokio::spawn(async move { request.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), async {
            while h.probes.load(Ordering::SeqCst) == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if replace {
            h.approved_code().await;
        } else {
            h.clock.store(601, Ordering::SeqCst);
        }
        drop(block);
        let denied = pending.await.unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let body = denied.text().await.unwrap();
        for secret in [
            &token,
            &session,
            "provider-access-secret",
            "provider-refresh-secret",
            "workspace-a",
            "user-a",
        ] {
            assert!(!body.contains(secret));
        }
    }
}

#[tokio::test]
async fn missing_or_duplicate_credentials_never_use_session_ids_as_authority() {
    let h = harness((4, 4, 4)).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let before = h.probes.load(Ordering::SeqCst);
    let missing = h
        .client
        .get(format!("{}/mcp", h.base))
        .header("mcp-session-id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    let duplicate = continuation(&h, &token, &session, Method::GET)
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);
    let duplicate = continuation(&h, &token, &session, Method::GET)
        .header("mcp-session-id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(h.probes.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn dropped_sse_body_releases_stream_capacity_for_authenticated_reconnect() {
    let h = harness((4, 4, 1)).await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let mut first = continuation(&h, &token, &session, Method::GET)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let _ = first.chunk().await.unwrap();
    assert_eq!(
        continuation(&h, &token, &session, Method::GET)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(first);
    let resumed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = continuation(&h, &token, &session, Method::GET)
                .header("last-event-id", "0")
                .send()
                .await
                .unwrap();
            if response.status() == StatusCode::OK {
                break response;
            }
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect must release the body-lifetime stream permit");
    drop(resumed);
}
