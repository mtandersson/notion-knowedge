//! Real OAuth issuance, sealed grant and loopback provider boundary fixtures.
use super::*;
use notion_knowledge_core::{
    backend::{AppendPageContent, BackendErrorKind, CreatePage, PageId, UpdateSectionContent},
    lifecycle::LifecycleScope,
};
use notion_knowledge_mcp::{HttpAuthorization, RequestScopeAdapters};
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
    Harness::with_authority(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        Some((4, 4, 4)),
        Duration::ZERO,
        Some(scope()),
        true,
    )
    .await
}
async fn writable(h: &Harness) -> (String, HttpAuthorization, RequestScopeAdapters) {
    let code = h
        .approved_code_with_scope("knowledge:write knowledge:read")
        .await;
    let token = resource::redeem(h, &code).await;
    let auth = h.login.local_resource_authorization(&token).unwrap();
    assert!(auth.permissions().can_write());
    let adapters = h
        .login
        .content_factory(scope())
        .unwrap()
        .bind(&auth)
        .unwrap();
    (token, auth, adapters)
}
async fn effect(
    adapters: &RequestScopeAdapters,
    operation: &str,
) -> Result<(), notion_knowledge_core::backend::BackendError> {
    let ports = adapters.source.mutation_ports().unwrap();
    match operation {
        "create" => ports
            .create_page(CreatePage {
                parent_page_id: PageId(ROOT.into()),
                title: "Example".into(),
                markdown: "body".into(),
            })
            .await
            .map(|_| ()),
        "append" => ports
            .append_content(AppendPageContent {
                page_id: PageId(PAGE.into()),
                markdown: "addition".into(),
            })
            .await
            .map(|_| ()),
        _ => {
            ports
                .update_section(UpdateSectionContent {
                    page_id: PageId(PAGE.into()),
                    old_str: "old".into(),
                    new_str: "new".into(),
                })
                .await
        }
    }
}
#[tokio::test]
async fn explicitly_issued_write_permissions_bind_ports_but_do_not_install_oauth_tools() {
    let h = harness().await;
    let token = resource::mint(&h).await;
    let read = h.login.local_resource_authorization(&token).unwrap();
    assert!(!read.permissions().can_write());
    assert!(
        h.login
            .content_factory(scope())
            .unwrap()
            .bind(&read)
            .unwrap()
            .source
            .mutation_ports()
            .is_none()
    );
    let (token, _, adapters) = writable(&h).await;
    for operation in ["create", "append", "section"] {
        effect(&adapters, operation).await.unwrap();
    }
    assert_eq!(h.mutations.load(Ordering::SeqCst), 3);
    let (session, _) = resource::initialize(&h, &token).await;
    let tools = resource::reply(
        resource::rpc(
            &h,
            &token,
            Some(&session),
            serde_json::json!({"jsonrpc":"2.0","id":8,"method":"tools/list"}),
        )
        .send()
        .await
        .unwrap(),
    )
    .await;
    for name in [
        "knowledge_create_page",
        "knowledge_append",
        "knowledge_update_section",
        "knowledge_upload_file",
    ] {
        assert!(!tools.to_string().contains(name), "{tools}");
    }
}
#[tokio::test]
async fn invalidated_authority_before_effect_sends_no_provider_mutation() {
    for invalidation in [
        "logout",
        "replacement",
        "expiry",
        "workspace",
        "person",
        "epoch",
        "grant",
    ] {
        let h = harness().await;
        let (token, auth, adapters) = writable(&h).await;
        let adapters = match invalidation {
            "logout" => {
                h.login.flow.revoke("chatgpt", &token).unwrap();
                adapters
            }
            "replacement" => {
                h.approved_code().await;
                adapters
            }
            _ => {
                let bad = HttpAuthorization::new(
                    [1; 32],
                    if invalidation == "grant" {
                        "foreign"
                    } else {
                        auth.grant_id()
                    }
                    .into(),
                    auth.epoch() + u64::from(invalidation == "epoch"),
                    if invalidation == "workspace" {
                        "foreign"
                    } else {
                        WORKSPACE
                    }
                    .into(),
                    if invalidation == "person" {
                        "foreign"
                    } else {
                        "user-a"
                    }
                    .into(),
                    if invalidation == "expiry" {
                        Instant::now() - Duration::from_secs(1)
                    } else {
                        auth.expires_at()
                    },
                )
                .with_permissions(notion_knowledge_mcp::OAuthPermissions::ReadWrite);
                match h.login.content_factory(scope()).unwrap().bind(&bad) {
                    Ok(bound) => bound,
                    Err(_) => {
                        assert_eq!(invalidation, "workspace");
                        continue;
                    }
                }
            }
        };
        for operation in ["create", "append", "section"] {
            assert!(
                effect(&adapters, operation).await.is_err(),
                "{invalidation}/{operation}"
            );
        }
        assert_eq!(h.mutations.load(Ordering::SeqCst), 0, "{invalidation}");
    }
}
#[tokio::test]
async fn logout_or_replacement_during_effect_never_reports_success_or_repeats_mutation() {
    for operation in ["create", "append", "section"] {
        for invalidation in ["logout", "replacement", "expiry"] {
            let h = harness().await;
            let (token, auth, adapters) = writable(&h).await;
            let auth = if invalidation == "expiry" {
                HttpAuthorization::new(
                    [2; 32],
                    auth.grant_id().into(),
                    auth.epoch(),
                    WORKSPACE.into(),
                    "user-a".into(),
                    Instant::now() + Duration::from_millis(200),
                )
                .with_permissions(notion_knowledge_mcp::OAuthPermissions::ReadWrite)
            } else {
                auth
            };
            let adapters = if invalidation == "expiry" {
                h.login
                    .content_factory(scope())
                    .unwrap()
                    .bind(&auth)
                    .unwrap()
            } else {
                adapters
            };
            let hold = h.mutation_gate.acquire().await.unwrap();
            let bound = adapters.clone();
            let pending = tokio::spawn(async move { effect(&bound, operation).await });
            tokio::time::timeout(Duration::from_secs(3), h.mutation_started.notified())
                .await
                .unwrap();
            match invalidation {
                "logout" => {
                    h.login.flow.revoke("chatgpt", &token).unwrap();
                }
                "replacement" => {
                    h.approved_code().await;
                }
                _ => tokio::time::sleep_until(auth.expires_at().into()).await,
            }
            drop(hold);
            let error = pending.await.unwrap().unwrap_err();
            assert_eq!(
                error.kind,
                BackendErrorKind::Unauthenticated,
                "{operation}/{invalidation}"
            );
            assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
            assert!(effect(&adapters, operation).await.is_err());
            assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        }
    }
}
#[tokio::test]
async fn revocation_during_successful_effect_readback_preserves_known_target_and_denies_success() {
    for operation in ["create", "append"] {
        let h = harness().await;
        let (token, _, adapters) = writable(&h).await;
        let hold = h.page_gate.acquire().await.unwrap();
        h.pause_page.store(true, Ordering::SeqCst);
        let before = h.page_reads.load(Ordering::SeqCst);
        let bound = adapters.clone();
        let pending = tokio::spawn(async move { effect(&bound, operation).await });
        tokio::time::timeout(Duration::from_secs(3), async {
            while h.page_reads.load(Ordering::SeqCst) == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        h.login.flow.revoke("chatgpt", &token).unwrap();
        drop(hold);
        let error = pending.await.unwrap().unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::Unauthenticated);
        assert_eq!(error.committed_page_id, Some(PageId(PAGE.into())));
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
        assert!(effect(&adapters, operation).await.is_err());
        assert_eq!(h.mutations.load(Ordering::SeqCst), 1);
    }
}
