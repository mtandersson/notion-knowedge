//! Explicitly installed, one-use user confirmation on the authenticated MCP peer.
use crate::{HttpAuthorization, RequestScopeAdapters};
use notion_knowledge_core::destructive::{
    ConfirmationDecision, ConfirmationError, ConfirmationFuture, ConfirmationProvider,
    ConfirmationRequest, WriteOperation, confirmation_scope,
};
use rmcp::{
    RoleServer,
    model::{ElicitRequestParams, ElicitationAction},
    service::{Peer, RequestContext},
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

/// Construct per tool invocation from trusted middleware evidence. Tool JSON
/// cannot select the peer, authenticated session or approval provider.
pub struct ElicitationConfirmation {
    peer: Peer<RoleServer>,
    adapters: RequestScopeAdapters,
    authorization: HttpAuthorization,
    scope_identity: String,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: Instant,
    used: AtomicBool,
}
impl ElicitationConfirmation {
    pub async fn bind(
        context: &RequestContext<RoleServer>,
        timeout: Duration,
    ) -> Result<Self, ConfirmationError> {
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(ConfirmationError::InvalidRequest);
        }
        let deadline = Instant::now() + timeout;
        let parts = context
            .extensions
            .get::<http::request::Parts>()
            .ok_or(ConfirmationError::Unavailable)?;
        let auth = parts
            .extensions
            .get::<HttpAuthorization>()
            .ok_or(ConfirmationError::Unavailable)?
            .clone();
        let adapters = parts
            .extensions
            .get::<RequestScopeAdapters>()
            .ok_or(ConfirmationError::Unavailable)?
            .clone();
        if !auth.is_active()
            || !auth.permissions().can_write()
            || !auth.same_session(&adapters.authorization)
            || auth.workspace_id() != adapters.scope.workspace_id
            || !context
                .peer
                .supported_elicitation_modes()
                .contains(&rmcp::service::ElicitationMode::Form)
        {
            return Err(ConfirmationError::Disabled);
        }
        let scope_identity = confirmation_scope(&adapters.scope)?;
        let provider = Self {
            peer: context.peer.clone(),
            adapters,
            authorization: auth,
            scope_identity,
            cancellation: context.ct.clone(),
            deadline,
            used: AtomicBool::new(false),
        };
        if !provider.active().await {
            return Err(ConfirmationError::Unavailable);
        }
        Ok(provider)
    }
    fn locally_active(&self) -> bool {
        self.authorization.is_active()
            && !self.cancellation.is_cancelled()
            && Instant::now() < self.deadline
    }
    async fn active(&self) -> bool {
        if !self.locally_active() {
            return false;
        }
        let deadline = self.deadline.min(self.authorization.expires_at());
        let token_cancel = self.authorization.cancellation();
        let result = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return false,
            _ = token_cancel.cancelled() => return false,
            result = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), self.adapters.source.validate()) => result,
        };
        self.locally_active() && matches!(result, Ok(Ok(())))
    }
}
impl ConfirmationProvider for ElicitationConfirmation {
    fn decide<'a>(&'a self, request: &'a ConfirmationRequest) -> ConfirmationFuture<'a> {
        Box::pin(async move {
            if self.used.swap(true, Ordering::SeqCst)
                || !self.active().await
                || request.operation() != WriteOperation::UpdateSection
                || request.trusted_scope() != self.scope_identity
            {
                return ConfirmationDecision::Unavailable;
            }
            let Some(preview) = request.section_preview() else {
                return ConfirmationDecision::Unavailable;
            };
            let schema = match serde_json::from_value(serde_json::json!({
                "type":"object", "properties":{"approve":{"type":"boolean","title":"Approve this exact section replacement","default":false}},
                "required":["approve"],
            })) {
                Ok(schema) => schema,
                Err(_) => return ConfirmationDecision::Unavailable,
            };
            let message = format!(
                "Replace the body of section {} on Notion page {}?\n\nCurrent section body:\n{}\n\nProposed section body:\n{}",
                preview.section_anchor,
                request.target_id(),
                preview.original_body,
                preview.replacement_body
            );
            let params = ElicitRequestParams::FormElicitationParams {
                meta: None,
                message,
                requested_schema: schema,
            };
            let token_cancel = self.authorization.cancellation();
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            let result = tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return ConfirmationDecision::Unavailable,
                _ = token_cancel.cancelled() => return ConfirmationDecision::Unavailable,
                result = tokio::time::timeout(remaining, self.peer.create_elicitation(params)) => result,
            };
            if !self.active().await {
                return ConfirmationDecision::Unavailable;
            }
            match result {
                Ok(Ok(reply))
                    if reply.action == ElicitationAction::Accept
                        && reply.content.as_ref().is_some_and(|content| {
                            content == &serde_json::json!({"approve":true})
                        }) =>
                {
                    ConfirmationDecision::Approved
                }
                Ok(Ok(_)) => ConfirmationDecision::Declined,
                _ => ConfirmationDecision::Unavailable,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OAuthPermissions, RequestBoundSource};
    use notion_knowledge_core::{
        backend::*, destructive::SectionConfirmationPreview, lifecycle::*,
    };
    use rmcp::{ServerHandler, ServiceExt};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    struct Source(Arc<AtomicBool>, bool, std::sync::atomic::AtomicUsize);
    impl NotionRead for Source {
        fn fetch_page<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, Page> {
            Box::pin(async { panic!("confirmation must not read or mutate Notion") })
        }
        fn read_content<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, PageContent> {
            Box::pin(async { panic!("confirmation must not read or mutate Notion") })
        }
    }
    impl PageLifecycle for Source {
        fn lifecycle<'a>(
            &'a self,
            _: &'a PageId,
            _: &'a LifecycleScope,
        ) -> BackendFuture<'a, LifecycleEvidence> {
            Box::pin(async { panic!("unexpected lifecycle call") })
        }
        fn revalidate_lifecycle<'a>(
            &'a self,
            _: &'a LifecycleEvidence,
            _: &'a LifecycleScope,
        ) -> BackendFuture<'a, ()> {
            Box::pin(async { panic!("unexpected lifecycle call") })
        }
    }
    impl RequestBoundSource for Source {
        fn validate(&self) -> BackendFuture<'_, ()> {
            Box::pin(async {
                if self.1 && self.2.fetch_add(1, Ordering::SeqCst) >= 2 {
                    std::future::pending::<()>().await;
                }
                if self.0.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(BackendError {
                        kind: BackendErrorKind::Unauthenticated,
                        operation: "confirmation.fixture",
                        retry_after: None,
                        committed_page_id: None,
                    })
                }
            })
        }
    }
    #[derive(Clone)]
    struct Server {
        active: Arc<AtomicBool>,
        mismatch: bool,
        scenario: &'static str,
        token_cancel: tokio_util::sync::CancellationToken,
    }
    impl ServerHandler for Server {
        fn get_info(&self) -> rmcp::model::ServerConfig {
            let mut info = rmcp::model::ServerConfig::default();
            info.capabilities = rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .build();
            info
        }
        async fn call_tool(
            &self,
            _: rmcp::model::CallToolRequestParams,
            mut context: RequestContext<RoleServer>,
        ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
            let auth = HttpAuthorization::new(
                [1; 32],
                "grant".into(),
                1,
                "workspace".into(),
                "owner".into(),
                if self.scenario == "expired" {
                    Instant::now() - Duration::from_secs(1)
                } else {
                    Instant::now() + Duration::from_secs(60)
                },
            )
            .with_permissions(if self.scenario == "readonly" {
                OAuthPermissions::Read
            } else {
                OAuthPermissions::ReadWrite
            })
            .with_cancellation(self.token_cancel.clone());
            let scope = LifecycleScope {
                workspace_id: "workspace".into(),
                generation: 1,
                roots: vec![PageId("root".into())],
                exclusions: Default::default(),
            };
            let mut adapters = RequestScopeAdapters {
                authorization: auth.clone(),
                scope: scope.clone(),
                source: Arc::new(Source(
                    self.active.clone(),
                    self.scenario == "held_validation",
                    0.into(),
                )),
            };
            if self.mismatch {
                adapters.authorization = HttpAuthorization::new(
                    [2; 32],
                    "grant".into(),
                    1,
                    "workspace".into(),
                    "owner".into(),
                    auth.expires_at(),
                )
                .with_permissions(OAuthPermissions::ReadWrite);
            }
            let (mut parts, _) = http::Request::new(()).into_parts();
            parts.extensions.insert(auth);
            parts.extensions.insert(adapters);
            context.extensions.insert(parts);
            let provider = match ElicitationConfirmation::bind(
                &context,
                if matches!(self.scenario, "timeout" | "held_validation") {
                    Duration::from_millis(10)
                } else {
                    Duration::from_secs(5)
                },
            )
            .await
            {
                Ok(provider) => provider,
                Err(_) => {
                    return Ok(rmcp::model::CallToolResult::success(vec![
                        rmcp::model::ContentBlock::text("unavailable"),
                    ])
                    .into());
                }
            };
            let intended_scope = confirmation_scope(&scope).unwrap();
            let intended = ConfirmationRequest::new(
                WriteOperation::UpdateSection,
                if self.scenario == "scope" {
                    "scope-mismatched"
                } else {
                    &intended_scope
                },
                "page",
                &"a".repeat(64),
                "fixture-idempotency-key",
            )
            .unwrap()
            .with_section_preview(SectionConfirmationPreview {
                section_anchor: "## Target".into(),
                original_body: "Old section".into(),
                replacement_body: "New section".into(),
            })
            .unwrap();
            let decision = provider.decide(&intended).await;
            assert_eq!(
                provider.decide(&intended).await,
                ConfirmationDecision::Unavailable,
                "approval provider is consumed even on refusal"
            );
            Ok(
                rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                    format!("{decision:?}"),
                )])
                .into(),
            )
        }
    }
    async fn exchange(reply: Value, invalidate: bool, mismatch: bool) -> (bool, Value) {
        exchange_scenario(reply, invalidate, mismatch, "normal").await
    }
    async fn exchange_scenario(
        reply: Value,
        invalidate: bool,
        mismatch: bool,
        scenario: &'static str,
    ) -> (bool, Value) {
        let active = Arc::new(AtomicBool::new(true));
        let token_cancel = tokio_util::sync::CancellationToken::new();
        let server = Server {
            active: active.clone(),
            mismatch,
            scenario,
            token_cancel: token_cancel.clone(),
        };
        let (client, transport) = tokio::io::duplex(65536);
        let task =
            tokio::spawn(async move { server.serve(transport).await.unwrap().waiting().await });
        let (reader, mut writer) = tokio::io::split(client);
        let mut reader = BufReader::new(reader);
        let capabilities = if scenario == "unsupported" {
            json!({})
        } else {
            json!({"elicitation":{"form":{}}})
        };
        writer.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":capabilities,"clientInfo":{"name":"confirmation-fixture","version":"1"}}})).as_bytes()).await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        writer
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        writer.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"fixture\",\"arguments\":{\"approved\":true,\"confirmation_id\":\"caller-selected\"}}}\n").await.unwrap();
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let mut response: Value = serde_json::from_str(&line).unwrap();
        let elicited = response["method"] == "elicitation/create";
        if elicited {
            let message = response["params"]["message"].as_str().unwrap();
            assert!(
                message.contains("Old section")
                    && message.contains("New section")
                    && message.contains("## Target")
            );
            if invalidate {
                active.store(false, Ordering::SeqCst);
            }
            if scenario == "revoked" {
                token_cancel.cancel();
            }
            if scenario == "timeout" {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            writer
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":response["id"],"result":reply})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            response = serde_json::from_str(&line).unwrap();
        }
        task.abort();
        (elicited, response)
    }
    #[tokio::test]
    async fn explicit_user_accept_is_required_and_provider_cannot_replay() {
        for (reply, expected) in [
            (
                json!({"action":"accept","content":{"approve":true}}),
                "Approved",
            ),
            (
                json!({"action":"accept","content":{"approve":false}}),
                "Declined",
            ),
            (json!({"action":"decline"}), "Declined"),
            (json!({"action":"cancel"}), "Declined"),
            (
                json!({"action":"accept","content":{"approve":true,"extra":true}}),
                "Declined",
            ),
        ] {
            let (elicited, response) = exchange(reply, false, false).await;
            assert!(elicited);
            assert_eq!(response["result"]["content"][0]["text"], expected);
        }
    }
    #[tokio::test]
    async fn grant_invalidation_during_prompt_and_foreign_session_deny_approval() {
        let (elicited, response) = exchange(
            json!({"action":"accept","content":{"approve":true}}),
            true,
            false,
        )
        .await;
        assert!(elicited);
        assert_eq!(response["result"]["content"][0]["text"], "Unavailable");
        let (elicited, response) = exchange(
            json!({"action":"accept","content":{"approve":true}}),
            false,
            true,
        )
        .await;
        assert!(!elicited);
        assert_eq!(response["result"]["content"][0]["text"], "unavailable");
    }

    #[tokio::test]
    async fn unsupported_expired_readonly_and_changed_scope_never_prompt() {
        for scenario in ["unsupported", "expired", "readonly", "scope"] {
            let (elicited, response) = exchange_scenario(
                json!({"action":"accept","content":{"approve":true}}),
                false,
                false,
                scenario,
            )
            .await;
            assert!(!elicited, "{scenario}");
            assert_ne!(response["result"]["content"][0]["text"], "Approved");
        }
    }

    #[tokio::test]
    async fn revoked_session_and_expired_prompt_reject_late_acceptance() {
        for scenario in ["revoked", "timeout", "held_validation"] {
            let (elicited, response) = exchange_scenario(
                json!({"action":"accept","content":{"approve":true}}),
                false,
                false,
                scenario,
            )
            .await;
            assert!(elicited);
            assert_eq!(response["result"]["content"][0]["text"], "Unavailable");
        }
    }

    #[tokio::test]
    async fn concurrent_sessions_keep_independent_user_decisions() {
        let (approved, declined) = tokio::join!(
            exchange(
                json!({"action":"accept","content":{"approve":true}}),
                false,
                false
            ),
            exchange(json!({"action":"decline"}), false, false),
        );
        assert!(approved.0 && declined.0);
        assert_eq!(approved.1["result"]["content"][0]["text"], "Approved");
        assert_eq!(declined.1["result"]["content"][0]["text"], "Declined");
    }
}
