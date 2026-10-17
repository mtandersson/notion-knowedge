//! Production OAuth credential, HTTP SDK, physical authorization and guarded
//! storage path. Fixtures change only the provider socket/model, never policy.
use super::resource::mint;
use super::resource::{initialize, reply, rpc};
use super::*;
use notion_knowledge_core::{
    backend::{BackendErrorKind, PageId},
    lifecycle::LifecycleScope,
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: Default::default(),
    }
}
async fn harness() -> Harness {
    let mut body = grant(WORKSPACE, "user-a");
    body["expires_in"] = 360.into();
    Harness::with_content(
        body,
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
        Some(scope()),
    )
    .await
}
fn tool(name: &str, arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":name,"arguments":arguments}})
}
fn get() -> Value {
    tool(
        "knowledge_get",
        json!({"refs":[{"kind":"page","id":PAGE}],"max_chars":2000}),
    )
}

#[tokio::test]
async fn request_owned_source_preserves_errors_and_rejects_wrong_or_expired_binding() {
    let h = harness().await;
    let token = mint(&h).await;
    let auth = h.login.local_resource_authorization(&token).unwrap();
    let factory = h.login.content_factory(scope()).unwrap();
    let adapters = factory.bind(&auth).unwrap();
    let page = adapters
        .source
        .fetch_page(&PageId(PAGE.into()))
        .await
        .unwrap();
    assert_eq!(page.id.0, PAGE);
    for (status, kind) in [
        (403, BackendErrorKind::PermissionDenied),
        (404, BackendErrorKind::NotFound),
        (429, BackendErrorKind::RateLimited),
        (503, BackendErrorKind::Unavailable),
    ] {
        h.page_status.store(status, Ordering::SeqCst);
        assert_eq!(
            adapters
                .source
                .fetch_page(&PageId(PAGE.into()))
                .await
                .unwrap_err()
                .kind,
            kind
        );
    }
    h.page_status.store(200, Ordering::SeqCst);
    let mut wrong = scope();
    wrong.generation += 1;
    assert_eq!(
        adapters
            .source
            .lifecycle(&PageId(PAGE.into()), &wrong)
            .await
            .unwrap_err()
            .kind,
        BackendErrorKind::PermissionDenied
    );
    let before = h.page_reads.load(Ordering::SeqCst);
    for (grant_id, expiry) in [
        ("foreign-grant", Instant::now() + Duration::from_secs(60)),
        (auth.grant_id(), Instant::now() - Duration::from_secs(1)),
    ] {
        let wrong = notion_knowledge_mcp::HttpAuthorization::new(
            [0; 32],
            grant_id.into(),
            auth.epoch(),
            WORKSPACE.into(),
            "user-a".into(),
            expiry,
        );
        assert_eq!(
            factory
                .bind(&wrong)
                .unwrap()
                .source
                .fetch_page(&PageId(PAGE.into()))
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Unauthenticated
        );
    }
    assert_eq!(h.page_reads.load(Ordering::SeqCst), before);
    let wrong = notion_knowledge_mcp::HttpAuthorization::new(
        [0; 32],
        auth.grant_id().into(),
        auth.epoch(),
        "foreign-workspace".into(),
        "user-a".into(),
        auth.expires_at(),
    );
    assert!(factory.bind(&wrong).is_err());
}

#[tokio::test]
async fn bound_backend_rejects_callback_replacement_while_read_is_pending() {
    let h = harness().await;
    let token = mint(&h).await;
    let auth = h.login.local_resource_authorization(&token).unwrap();
    let adapters = h
        .login
        .content_factory(scope())
        .unwrap()
        .bind(&auth)
        .unwrap();
    let hold = h.page_gate.acquire().await.unwrap();
    h.pause_page.store(true, Ordering::SeqCst);
    let before = h.page_reads.load(Ordering::SeqCst);
    let pending =
        tokio::spawn(async move { adapters.source.fetch_page(&PageId(PAGE.into())).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.page_reads.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.approved_code().await;
    drop(hold);
    assert_eq!(
        pending.await.unwrap().unwrap_err().kind,
        BackendErrorKind::Unauthenticated
    );
}

#[cfg(all(unix, feature = "webhook-worker"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_oauth_lifecycle_reads_refreshes_revokes_and_reauthorizes() {
    let h = harness().await;
    let before_state = fs::read(h._dir.path().join("content-state.sqlite")).unwrap();
    let before_pages = h.page_reads.load(Ordering::SeqCst);
    assert_eq!(
        h.client
            .post(format!("{}/mcp", h.base))
            .json(&get())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(h.page_reads.load(Ordering::SeqCst), before_pages);
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    for mode in ["lexical", "semantic", "hybrid"] {
        let response = rpc(
            &h,
            &token,
            Some(&session),
            tool(
                "knowledge_search",
                json!({"query":"Saffron","limit":5,"mode":mode,"filters":{}}),
            ),
        )
        .send()
        .await
        .unwrap();
        let value = reply(response).await;
        assert!(
            value.to_string().contains("Saffron private evidence"),
            "{mode}: {value}"
        );
    }
    let value = reply(rpc(&h, &token, Some(&session), get()).send().await.unwrap()).await;
    assert!(
        value.to_string().contains("Saffron private evidence"),
        "{value}"
    );
    let fresh = tool(
        "knowledge_get",
        json!({"refs":[{"kind":"page","id":PAGE}],"max_chars":2000,"freshness":"fresh"}),
    );
    let value = reply(rpc(&h, &token, Some(&session), fresh).send().await.unwrap()).await;
    assert!(
        value
            .to_string()
            .contains("Saffron private evidence fresh content"),
        "{value}"
    );
    let stored = h.store.load_for_refresh().unwrap().unwrap();
    h.clock.store(61, Ordering::SeqCst);
    let value = reply(rpc(&h, &token, Some(&session), get()).send().await.unwrap()).await;
    assert!(
        value.to_string().contains("Saffron private evidence"),
        "{value}"
    );
    let rotated = h.store.load_for_refresh().unwrap().unwrap();
    assert_eq!(rotated.grant_id(), stored.grant_id());
    assert_eq!(rotated.epoch(), stored.epoch());
    assert_eq!(rotated.expires_at_unix(), None);
    assert_eq!(rotated.access_token(), "rotated-access-secret");
    assert_ne!(rotated.access_token(), stored.access_token());
    assert_eq!(rotated.refresh_token(), Some("rotated-refresh-secret"));
    let revoked = h
        .client
        .post(format!("{}/revoke", h.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(encode_form(&[
            ("client_id", "chatgpt".into()),
            ("token", token.clone()),
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    assert!(revoked.bytes().await.unwrap().is_empty());
    let before_denied = (
        h.probes.load(Ordering::SeqCst),
        h.page_reads.load(Ordering::SeqCst),
        h.calls.load(Ordering::SeqCst),
    );
    let denied = rpc(&h, &token, Some(&session), get()).send().await.unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let body = denied.text().await.unwrap();
    for private in [
        "Saffron private evidence",
        PAGE,
        ROOT,
        WORKSPACE,
        token.as_str(),
    ] {
        assert!(!body.contains(private));
    }
    assert_eq!(
        (
            h.probes.load(Ordering::SeqCst),
            h.page_reads.load(Ordering::SeqCst),
            h.calls.load(Ordering::SeqCst)
        ),
        before_denied
    );
    let fresh_token = mint(&h).await;
    assert_ne!(fresh_token, token);
    let (fresh_session, _) = initialize(&h, &fresh_token).await;
    let value = reply(
        rpc(&h, &fresh_token, Some(&fresh_session), get())
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert!(
        value.to_string().contains("Saffron private evidence"),
        "{value}"
    );
    assert_eq!(
        rpc(&h, &token, Some(&session), get())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        fs::read(h._dir.path().join("content-state.sqlite")).unwrap(),
        before_state,
        "read composition must not install writers or mutate authority"
    );
}

#[cfg(all(unix, feature = "webhook-worker"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_index_content_is_not_disclosed_on_permission_loss_or_callback_replacement() {
    let h = harness().await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    h.page_status.store(403, Ordering::SeqCst);
    let denied = reply(rpc(&h, &token, Some(&session), get()).send().await.unwrap()).await;
    assert!(!denied.to_string().contains("Saffron private evidence"));
    h.page_status.store(200, Ordering::SeqCst);
    h.active.store(false, Ordering::SeqCst);
    assert_eq!(
        rpc(&h, &token, Some(&session), get())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    h.active.store(true, Ordering::SeqCst);
    let block = h.page_gate.acquire().await.unwrap();
    h.pause_page.store(true, Ordering::SeqCst);
    let before = h.page_reads.load(Ordering::SeqCst);
    let request = rpc(&h, &token, Some(&session), get());
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.page_reads.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.approved_code().await;
    drop(block);
    let value = reply(pending.await.unwrap()).await;
    assert!(value.get("error").is_some(), "{value}");
    for private in ["Saffron private evidence", PAGE, ROOT, WORKSPACE] {
        assert!(!value.to_string().contains(private));
    }
    assert_eq!(
        rpc(&h, &token, Some(&session), get())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn scoped_tool_budget_covers_provider_validation_before_backend_dispatch() {
    let h = harness().await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let (other_session, _) = initialize(&h, &token).await;
    let hold = h.page_gate.acquire().await.unwrap();
    h.pause_page.store(true, Ordering::SeqCst);
    let before = h.page_reads.load(Ordering::SeqCst);
    let request = rpc(&h, &token, Some(&session), get());
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.page_reads.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let probes = h.probes.load(Ordering::SeqCst);
    let busy = reply(
        rpc(&h, &token, Some(&other_session), get())
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert!(busy.to_string().contains("tool_busy"), "{busy}");
    // Only outer HTTP authentication may introspect. A rejected tool must not
    // start request-scoped validation or buffer private data outside its budget.
    assert_eq!(h.probes.load(Ordering::SeqCst), probes + 1);
    assert_eq!(h.page_reads.load(Ordering::SeqCst), before + 1);
    drop(hold);
    let _ = reply(pending.await.unwrap()).await;
}

#[tokio::test]
async fn upstream_inactivity_during_authoritative_io_denies_completed_tool_result() {
    let h = harness().await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let hold = h.page_gate.acquire().await.unwrap();
    h.pause_page.store(true, Ordering::SeqCst);
    let before = h.page_reads.load(Ordering::SeqCst);
    let request = rpc(&h, &token, Some(&session), get());
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.page_reads.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.active.store(false, Ordering::SeqCst);
    drop(hold);
    let value = reply(pending.await.unwrap()).await;
    assert!(value.get("error").is_some(), "{value}");
    assert!(!value.to_string().contains("Saffron private evidence"));
}

#[tokio::test]
async fn ordinary_http_entrypoint_accepts_oauth_scope_without_static_integration() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("key");
    fs::write(&key, [42; 32]).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let state = dir.path().join("grant");
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let values = std::collections::HashMap::<String, String>::from_iter(
        [
            ("NK_HTTP_HOST", "127.0.0.1"),
            ("NK_HTTP_PORT", &bind.port().to_string()),
            ("NK_OAUTH_ISSUER", "https://auth.example.com"),
            ("NK_OAUTH_RESOURCE", "https://knowledge.example.com/mcp"),
            ("NK_NOTION_OAUTH_CLIENT_ID", "notion-client"),
            (
                "NK_NOTION_OAUTH_REDIRECT_URI",
                "https://auth.example.com/oauth/notion/callback",
            ),
            ("NK_NOTION_OAUTH_CLIENT_SECRET", "client-secret"),
            ("NK_NOTION_ALLOWED_WORKSPACE_ID", WORKSPACE),
            ("NK_NOTION_ALLOWED_USER_ID", "user-a"),
            ("NK_NOTION_GRANT_KEY_FILE", key.to_str().unwrap()),
            ("NK_NOTION_GRANT_STATE_FILE", state.to_str().unwrap()),
            ("NK_OAUTH_CLIENT_ID", "chatgpt"),
            ("NK_OAUTH_CLIENT_REDIRECT_URI", CLIENT_REDIRECT),
            ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
            ("NK_NOTION_SCOPE_ROOTS", ROOT),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    );
    let settings =
        Config::from_lookup(|key| values.get(key).map(std::ffi::OsString::from)).unwrap();
    assert!(matches!(
        settings.notion_auth,
        crate::config::NotionAuth::None
    ));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        crate::http::serve_composed(
            settings,
            notion_knowledge_mcp::KnowledgeServer::default(),
            #[cfg(all(unix, feature = "webhook-worker"))]
            None,
            stop,
        )
        .await
    });
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(response) = client.get(format!("http://{bind}/mcp")).send().await {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
                break;
            }
            assert!(
                !server.is_finished(),
                "ordinary production entrypoint rejected OAuth-only scope"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn revoked_token_cannot_disclose_private_content_from_an_already_started_read() {
    let h = harness().await;
    let token = mint(&h).await;
    let (session, _) = initialize(&h, &token).await;
    let block = h.page_gate.acquire().await.unwrap();
    h.pause_page.store(true, Ordering::SeqCst);
    let before = h.page_reads.load(Ordering::SeqCst);
    let request = rpc(&h, &token, Some(&session), get());
    let pending = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.page_reads.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let response = h
        .client
        .post(format!("{}/revoke", h.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(encode_form(&[
            ("client_id", "chatgpt".into()),
            ("token", token.clone()),
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(block);
    // Closing the SDK worker may close the stream before an RPC error can be
    // delivered. Both outcomes must withhold every private content byte.
    if let Ok(response) = pending.await.unwrap() {
        let text = response.text().await.unwrap_or_default();
        for private in ["Saffron private evidence", PAGE, ROOT, WORKSPACE] {
            assert!(!text.contains(private));
        }
    }
    assert!(h.login.local_resource_authorization(&token).is_none());
}
