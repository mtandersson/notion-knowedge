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
