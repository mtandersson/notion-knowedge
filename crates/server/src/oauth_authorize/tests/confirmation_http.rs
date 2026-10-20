//! Real SDK Streamable HTTP peer and production OAuth admission; no write tools installed.
use super::*;
use notion_knowledge_core::{
    backend::PageId,
    destructive::{
        ConfirmationDecision, ConfirmationProvider, ConfirmationRequest,
        SectionConfirmationPreview, WriteOperation, confirmation_scope,
    },
    lifecycle::LifecycleScope,
};
use notion_knowledge_mcp::confirmation::ElicitationConfirmation;
use rmcp::{
    RoleServer, ServerHandler,
    model::*,
    service::RequestContext,
    transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService},
};
use serde_json::{Value, json};
use std::{collections::VecDeque, time::Duration};
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into())],
        exclusions: Default::default(),
    }
}
#[derive(Clone)]
struct ConfirmationPeer;
impl ServerHandler for ConfirmationPeer {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        // Caller arguments never supply approval or the intended replacement.
        let provider = ElicitationConfirmation::bind(&context, Duration::from_secs(1))
            .await
            .unwrap();
        let request = ConfirmationRequest::new(
            WriteOperation::UpdateSection,
            &confirmation_scope(&scope()).unwrap(),
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
            &"a".repeat(64),
            "trusted-http-intent",
        )
        .unwrap()
        .with_section_preview(SectionConfirmationPreview {
            section_anchor: "## Target".into(),
            original_body: "Original private body".into(),
            replacement_body: "Trusted replacement".into(),
        })
        .unwrap();
        let decision = provider.decide(&request).await;
        assert_eq!(
            provider.decide(&request).await,
            ConfirmationDecision::Unavailable
        );
        Ok(CallToolResult::success(vec![ContentBlock::text(format!("{decision:?}"))]).into())
    }
}
struct Events {
    response: reqwest::Response,
    pending: String,
    frames: VecDeque<Value>,
}
impl Events {
    async fn next(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = self.frames.pop_front() {
                    return frame;
                }
                let chunk = self
                    .response
                    .chunk()
                    .await
                    .unwrap()
                    .expect("SSE ended before response");
                self.pending.push_str(std::str::from_utf8(&chunk).unwrap());
                while let Some(end) = self.pending.find('\n') {
                    let line: String = self.pending.drain(..=end).collect();
                    if let Some(data) = line.trim().strip_prefix("data: ")
                        && let Ok(value) = serde_json::from_str::<Value>(data)
                    {
                        self.frames.push_back(value);
                    }
                }
            }
        })
        .await
        .expect("bounded HTTP MCP response")
    }
}
struct Peer {
    client: reqwest::Client,
    url: String,
    token: String,
    session: String,
}
impl Peer {
    async fn post(&self, value: Value) -> reqwest::Response {
        self.client
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &self.session)
            .header("mcp-protocol-version", "2025-11-25")
            .json(&value)
            .send()
            .await
            .unwrap()
    }
    async fn call(&self) -> Events {
        let response = self.post(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"confirmation-fixture","arguments":{"approved":true,"confirmation_id":"caller-selected","replacement":"attacker-selected"}}})).await;
        assert_eq!(response.status(), StatusCode::OK);
        Events {
            response,
            pending: String::new(),
            frames: VecDeque::new(),
        }
    }
}
async fn connect(h: &Harness, url: &str) -> Peer {
    let code = h
        .approved_code_with_scope("knowledge:read knowledge:write")
        .await;
    let token = resource::redeem(h, &code).await;
    initialize(h, url, token).await
}
async fn initialize(h: &Harness, url: &str, token: String) -> Peer {
    let response = h.client.post(url).bearer_auth(&token).header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{"form":{}}},"clientInfo":{"name":"trusted-http-fixture","version":"1"}}})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let session = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let mut events = Events {
        response,
        pending: String::new(),
        frames: VecDeque::new(),
    };
    assert_eq!(events.next().await["id"], 1);
    let peer = Peer {
        client: h.client.clone(),
        url: url.into(),
        token,
        session,
    };
    assert!(
        peer.post(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await
            .status()
            .is_success()
    );
    peer
}
async fn transport(h: &Harness) -> (String, tokio::task::JoinHandle<()>) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = socket.local_addr().unwrap();
    let sessions = Arc::new(crate::session_limits::BoundedSessions::new(
        4,
        Duration::from_secs(30),
    ));
    let mut config = StreamableHttpServerConfig::default();
    config.allowed_hosts.push("127.0.0.1".into());
    config.allowed_origins.push(format!("http://{bind}"));
    let service = StreamableHttpService::new(|| Ok(ConfirmationPeer), sessions.clone(), config);
    let gate = crate::oauth_mcp::ResourceGate {
        login: Some(h.login.clone()),
        sessions,
        admission: Arc::new(tokio::sync::Semaphore::new(4)),
        metadata_url: "https://knowledge.example.com/.well-known/oauth-protected-resource".into(),
        content: Some(h.login.content_factory(scope()).unwrap()),
    };
    let router =
        Router::new()
            .nest_service("/mcp", service)
            .layer(axum::middleware::from_fn_with_state(
                gate,
                crate::oauth_mcp::require_authorization,
            ));
    let task = tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    (format!("http://{bind}/mcp"), task)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_http_peer_requires_explicit_prompt_reply_and_live_original_oauth_authority() {
    for scenario in ["accept", "decline", "logout", "replacement", "expiry"] {
        let h = Harness::with_authority(
            grant(WORKSPACE, "user-a"),
            StatusCode::OK,
            None,
            Duration::ZERO,
            Some(scope()),
            true,
        )
        .await;
        let (url, server) = transport(&h).await;
        let peer = connect(&h, &url).await;
        let mut events = peer.call().await;
        let prompt = events.next().await;
        assert_eq!(prompt["method"], "elicitation/create", "{scenario}");
        let message = prompt["params"]["message"].as_str().unwrap();
        assert!(
            message.contains("Original private body")
                && message.contains("Trusted replacement")
                && !message.contains("attacker-selected")
        );
        if scenario == "accept" {
            // A second MCP peer shares the same authenticated bearer but owns
            // none of this peer's pending prompts, even if its request ID matches.
            let other = initialize(&h, &url, peer.token.clone()).await;
            assert_ne!(peer.session, other.session);
            let _unrelated = other.post(json!({"jsonrpc":"2.0","id":prompt["id"],"result":{"action":"accept","content":{"approve":true}}})).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(50), events.next())
                    .await
                    .is_err(),
                "other MCP peer completed the pending prompt"
            );
        }
        match scenario {
            "logout" => {
                h.login.flow.revoke("chatgpt", &peer.token).unwrap();
            }
            "replacement" => {
                h.approved_code().await;
            }
            "expiry" => {
                h.clock.store(4000, Ordering::SeqCst);
            }
            _ => {}
        }
        let response = peer.post(json!({"jsonrpc":"2.0","id":prompt["id"],"result":{"action":if scenario == "decline" {"decline"} else {"accept"},"content":{"approve":true}}})).await;
        let expected = match scenario {
            "accept" => "Approved",
            "decline" => "Declined",
            _ => "Unavailable",
        };
        if matches!(scenario, "accept" | "decline") {
            assert!(
                response.status().is_success(),
                "{scenario}: {}",
                response.status()
            );
        } else {
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{scenario}");
        }
        let result = events.next().await;
        assert_eq!(
            result["result"]["content"][0]["text"], expected,
            "{scenario}: {result}"
        );
        assert_eq!(h.mutations.load(Ordering::SeqCst), 0);
        server.abort();
    }
}
