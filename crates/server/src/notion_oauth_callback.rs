//! Fail-closed Notion OAuth callback verifier. This is a server-side component,
//! NOT a mounted HTTP login. #123/#127 must persist verified grants and enforce
//! revocation before the MCP AuthorizationFlow can issue any usable code/token.
use std::{fmt, time::Duration};

use reqwest::{Client, Url, redirect::Policy};
use serde::Deserialize;
use serde_json::json;

use crate::{
    notion_oauth_redirect::{NotionOAuthConfig, NotionOAuthRedirect},
    oauth_code::Secret,
};

const TOKEN_ENDPOINT: &str = "https://api.notion.com/v1/oauth/token";
const MAX_TOKEN_RESPONSE: usize = 64 * 1024;
const MAX_CALLBACK_QUERY: usize = 4096;

/// All failures have safe static representations. Never expose an upstream
/// error body, code, state, access/refresh token or client secret to callers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallbackError {
    InvalidCallback,
    InvalidState,
    RejectedByNotion,
    TokenExchangeFailed,
    InvalidGrant,
    IdentityMismatch,
}
impl CallbackError {
    pub fn user_message(self) -> &'static str {
        "Notion authorization could not be completed. Please start again."
    }
}

/// Immutable single-owner policy provisioned by the operator, never
/// discovered from the first successful OAuth response.
#[derive(Clone)]
pub struct NotionOwnerPolicy {
    workspace_id: String,
    owner_user_id: String,
}
impl NotionOwnerPolicy {
    pub(crate) fn matches(&self, workspace: &str, user: &str) -> bool {
        self.workspace_id == workspace && self.owner_user_id == user
    }

    pub fn new(workspace_id: &str, owner_user_id: &str) -> Result<Self, &'static str> {
        if !valid_id(workspace_id) {
            return Err("NK_NOTION_ALLOWED_WORKSPACE_ID");
        }
        if !valid_id(owner_user_id) {
            return Err("NK_NOTION_ALLOWED_USER_ID");
        }
        Ok(Self {
            workspace_id: workspace_id.to_owned(),
            owner_user_id: owner_user_id.to_owned(),
        })
    }
}
impl fmt::Debug for NotionOwnerPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotionOwnerPolicy([REDACTED])")
    }
}

/// Only a server operator may construct this confidential client. The
/// production endpoint is immutable, HTTPS-only, and redirects are disabled.
pub struct NotionTokenClient {
    http: Client,
    client_id: String,
    client_secret: Secret,
    redirect_uri: String,
    token_endpoint: String,
}
impl fmt::Debug for NotionTokenClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotionTokenClient([REDACTED])")
    }
}
impl NotionTokenClient {
    pub fn new(
        registration: &NotionOAuthConfig,
        client_secret: &str,
    ) -> Result<Self, &'static str> {
        if !valid_secret(client_secret) {
            return Err("NK_NOTION_OAUTH_CLIENT_SECRET");
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(12))
            .redirect(Policy::none())
            .build()
            .map_err(|_| "NK_NOTION_OAUTH_CLIENT_SECRET")?;
        Ok(Self {
            http,
            client_id: registration.client_id().to_owned(),
            client_secret: Secret::from_internal(client_secret.to_owned()),
            redirect_uri: registration.redirect_uri().to_owned(),
            token_endpoint: TOKEN_ENDPOINT.to_owned(),
        })
    }

    #[cfg(test)]
    fn for_local_fixture(mut self, endpoint: &str) -> Self {
        self.token_endpoint = endpoint.to_owned();
        self
    }

    async fn exchange(&self, code: &str) -> Result<NotionGrant, CallbackError> {
        if !valid_code(code) {
            return Err(CallbackError::InvalidCallback);
        }
        self.token_request(json!({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri
        })).await
    }

    /// Server-only refresh; never send the refresh token to a browser, log or
    /// MCP output. The response is parsed through the SAME strict typed grant
    /// validator used by initial authorization.
    pub(crate) async fn refresh(&self, refresh_token: &str) -> Result<NotionGrant, CallbackError> {
        if !valid_secret(refresh_token) {
            return Err(CallbackError::InvalidGrant);
        }
        self.token_request(json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        })).await
    }

    async fn token_request(&self, params: serde_json::Value) -> Result<NotionGrant, CallbackError> {
        // Never log the reqwest request or propagate its error: Basic auth,
        // refresh tokens and provider bodies are confidential.
        let mut response = self
            .http
            .post(&self.token_endpoint)
            .basic_auth(&self.client_id, Some(self.client_secret.expose()))
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&params)
            .send()
            .await
            .map_err(|_| CallbackError::TokenExchangeFailed)?;

        if !response.status().is_success() {
            // DO NOT read or display Notion's raw error response.
            return Err(CallbackError::TokenExchangeFailed);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CallbackError::TokenExchangeFailed)?
        {
            if chunk.len() > MAX_TOKEN_RESPONSE.saturating_sub(body.len()) {
                return Err(CallbackError::InvalidGrant);
            }
            body.extend_from_slice(&chunk);
        }
        NotionGrant::parse(&body)
    }
}

/// Tokens are never Debug/Serialize; they stay in the server process until
/// #123 persists them safely. Do NOT pass them to MCP, ChatGPT, telemetry or
/// the retrieval index. No `grant_id`/epoch is assumed before persistence.
pub struct NotionGrant {
    access_token: Secret,
    refresh_token: Option<Secret>,
    workspace_id: String,
    owner_user_id: String,
    bot_id: String,
    expires_in: Option<u64>,
}
impl fmt::Debug for NotionGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotionGrant([REDACTED])")
    }
}
impl NotionGrant {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn owner_user_id(&self) -> &str {
        &self.owner_user_id
    }
    pub fn bot_id(&self) -> &str {
        &self.bot_id
    }
    pub fn expires_in(&self) -> Option<u64> {
        self.expires_in
    }
    #[cfg(test)]
    pub(crate) fn fixture(workspace: &str, owner: &str, expires_in: Option<u64>) -> Self {
        Self {
            access_token: Secret::from_internal("test-access-secret".to_owned()),
            refresh_token: Some(Secret::from_internal("test-refresh-secret".to_owned())),
            workspace_id: workspace.to_owned(),
            owner_user_id: owner.to_owned(),
            bot_id: "bot-test".to_owned(),
            expires_in,
        }
    }
    /// Only the future protected grant-store adapter may read these.
    pub fn access_token(&self) -> &str {
        self.access_token.expose()
    }
    pub fn refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_ref().map(Secret::expose)
    }

    fn parse(payload: &[u8]) -> Result<Self, CallbackError> {
        let response: TokenResponse =
            serde_json::from_slice(payload).map_err(|_| CallbackError::InvalidGrant)?;
        if !response.token_type.eq_ignore_ascii_case("bearer")
            || !valid_secret(&response.access_token)
            || !valid_id(&response.workspace_id)
            || !valid_id(&response.bot_id)
            || response.owner.kind != "user"
            || response
                .owner
                .user
                .as_ref()
                .is_none_or(|user| !valid_id(&user.id))
            || response
                .expires_in
                .is_some_and(|seconds| seconds == 0 || seconds > 31_536_000)
            || response
                .refresh_token
                .as_ref()
                .is_some_and(|token| !valid_secret(token))
        {
            return Err(CallbackError::InvalidGrant);
        }
        let owner_user_id = response.owner.user.ok_or(CallbackError::InvalidGrant)?.id;
        Ok(Self {
            access_token: Secret::from_internal(response.access_token),
            refresh_token: response.refresh_token.map(Secret::from_internal),
            workspace_id: response.workspace_id,
            owner_user_id,
            bot_id: response.bot_id,
            expires_in: response.expires_in,
        })
    }
}
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    workspace_id: String,
    bot_id: String,
    owner: NotionOwner,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}
#[derive(Deserialize)]
struct NotionOwner {
    #[serde(rename = "type")]
    kind: String,
    user: Option<NotionUser>,
}
#[derive(Deserialize)]
struct NotionUser {
    id: String,
}

/// A verified provider exchange is NOT an approved MCP grant. The returned
/// MCP correlation handle is still unapproved and must be persisted with a
/// matching immutable Notion identity plus epoch (#123) before #120 approve().
pub struct VerifiedCallback {
    pub mcp_transaction: Secret,
    pub grant: NotionGrant,
}
impl fmt::Debug for VerifiedCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedCallback([REDACTED])")
    }
}

/// The caller must bind this to the exact canonical Notion callback only.
/// There is NO public HTTP callback route or implicit activation in #122.
pub struct NotionCallback<'a> {
    redirect: &'a NotionOAuthRedirect,
    exchanger: &'a NotionTokenClient,
    allowed: &'a NotionOwnerPolicy,
}
impl<'a> NotionCallback<'a> {
    pub fn new(
        redirect: &'a NotionOAuthRedirect,
        exchanger: &'a NotionTokenClient,
        allowed: &'a NotionOwnerPolicy,
    ) -> Self {
        Self {
            redirect,
            exchanger,
            allowed,
        }
    }

    /// Parse raw query with a strict duplicate-parameter policy; consume
    /// single-use state BEFORE network I/O and BEFORE interpreting a provider
    /// error. Return only an independently validated provider identity.
    pub async fn complete(&self, raw_query: &str) -> Result<VerifiedCallback, CallbackError> {
        let params = CallbackParams::parse(raw_query)?;
        let transaction = self
            .redirect
            .take(&params.state)
            .ok_or(CallbackError::InvalidState)?;
        if params.error {
            return Err(CallbackError::RejectedByNotion);
        }
        let code = params.code.ok_or(CallbackError::InvalidCallback)?;
        let grant = self.exchanger.exchange(&code).await?;
        if grant.workspace_id != self.allowed.workspace_id
            || grant.owner_user_id != self.allowed.owner_user_id
        {
            return Err(CallbackError::IdentityMismatch);
        }
        Ok(VerifiedCallback {
            mcp_transaction: transaction,
            grant,
        })
    }
}

struct CallbackParams {
    state: String,
    code: Option<String>,
    error: bool,
}
impl CallbackParams {
    fn parse(raw_query: &str) -> Result<Self, CallbackError> {
        if raw_query.is_empty()
            || raw_query.len() > MAX_CALLBACK_QUERY
            || raw_query.contains('#')
            || !raw_query.is_ascii()
            || raw_query.bytes().any(|b| b.is_ascii_control())
            || !valid_percent_encoding(raw_query)
        {
            return Err(CallbackError::InvalidCallback);
        }
        let url = Url::parse(&format!("https://callback.invalid/?{raw_query}"))
            .map_err(|_| CallbackError::InvalidCallback)?;
        let mut state = None;
        let mut code = None;
        let mut error = None;
        let mut error_description = false;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "state" if state.is_none() => state = Some(value.into_owned()),
                "code" if code.is_none() => code = Some(value.into_owned()),
                "error" if error.is_none() => error = Some(value.into_owned()),
                "error_description" if !error_description => error_description = true,
                _ => return Err(CallbackError::InvalidCallback),
            }
        }
        let state = state.ok_or(CallbackError::InvalidCallback)?;
        if state.len() != 43
            || !state
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(CallbackError::InvalidCallback);
        }
        if error.is_some() && code.is_some() {
            return Err(CallbackError::InvalidCallback);
        }
        if error.is_none() && code.as_deref().is_none_or(|value| !valid_code(value)) {
            return Err(CallbackError::InvalidCallback);
        }
        Ok(Self {
            state,
            code,
            error: error.is_some(),
        })
    }
}
fn valid_percent_encoding(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len()
                || !bytes[i + 1].is_ascii_hexdigit()
                || !bytes[i + 2].is_ascii_hexdigit()
            {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
fn valid_secret(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 4096
        && value.is_ascii()
        && !value
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
}
fn valid_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 2048
        && value.is_ascii()
        && !value
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth_code::{
        AllowedNotionIdentity, AuthorizationFlow, AuthorizationRequest, Client as OAuthClient,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const ISSUER: &str = "https://auth.example.com";
    const CALLBACK: &str = "https://auth.example.com/oauth/notion/callback";
    const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
    const SECRET: &str = "private-client-secret";

    fn setup() -> (
        NotionOAuthRedirect,
        AuthorizationFlow,
        NotionOAuthConfig,
        NotionOwnerPolicy,
    ) {
        let registration = NotionOAuthConfig::new("test-client", CALLBACK, ISSUER).unwrap();
        let flow = AuthorizationFlow::new(
            OAuthClient::new(
                "chatgpt",
                "https://chatgpt.example.com/cb",
                "https://mcp.example.com/mcp",
            )
            .unwrap(),
            AllowedNotionIdentity::new("workspace-123", "user-456").unwrap(),
        );
        (
            NotionOAuthRedirect::new(registration.clone()),
            flow,
            registration,
            NotionOwnerPolicy::new("workspace-123", "user-456").unwrap(),
        )
    }
    fn request<'a>() -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            response_type: "code",
            client_id: "chatgpt",
            redirect_uri: "https://chatgpt.example.com/cb",
            resource: "https://mcp.example.com/mcp",
            state: "chatgpt-state-123456789",
            code_challenge: CHALLENGE,
            code_challenge_method: "S256",
            scope: "knowledge:read",
        }
    }
    fn state(url: &str) -> &str {
        url.split("&state=").nth(1).unwrap()
    }
    fn grant_json(workspace: &str, kind: &str, user_id: &str) -> String {
        json!({
            "access_token": "secret_access_value",
            "refresh_token": "secret_refresh_value",
            "token_type": "bearer",
            "bot_id": "bot-123",
            "workspace_id": workspace,
            "owner": {"type":kind,"user":{"object":"user","id":user_id}},
        })
        .to_string()
    }

    async fn token_server(body: String, status: u16) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = Vec::new();
            let mut part = [0u8; 1024];
            loop {
                let count = stream.read(&mut part).await.unwrap();
                if count == 0 {
                    break;
                }
                input.extend_from_slice(&part[..count]);
                if let Some(headers_end) = input.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&input[..headers_end]);
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if input.len() >= headers_end + 4 + length {
                        break;
                    }
                }
                assert!(input.len() < 128 * 1024);
            }
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(input).unwrap()
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn correct_callback_exchanges_server_side_and_retains_only_pending_grant() {
        let (redirect, flow, registration, allowed) = setup();
        let start = redirect.begin(&flow, &request()).unwrap();
        let s = state(start.expose());
        let (endpoint, task) =
            token_server(grant_json("workspace-123", "user", "user-456"), 200).await;
        let client = NotionTokenClient::new(&registration, SECRET)
            .unwrap()
            .for_local_fixture(&endpoint);
        let result = NotionCallback::new(&redirect, &client, &allowed)
            .complete(&format!("code=notion-code&state={s}"))
            .await
            .unwrap();
        assert_eq!(result.grant.workspace_id(), "workspace-123");
        assert_eq!(result.grant.owner_user_id(), "user-456");
        assert_eq!(result.grant.bot_id(), "bot-123");
        assert_eq!(result.grant.access_token(), "secret_access_value");
        assert_eq!(result.grant.refresh_token(), Some("secret_refresh_value"));
        assert!(!result.mcp_transaction.expose().is_empty());
        assert!(format!("{result:?}").contains("REDACTED"));
        assert!(format!("{client:?}").contains("REDACTED"));
        let observed = task.await.unwrap();
        assert!(observed.starts_with("POST /v1/oauth/token HTTP/1.1"));
        assert!(
            observed
                .to_ascii_lowercase()
                .contains("authorization: basic ")
        );
        assert!(observed.contains("\"grant_type\":\"authorization_code\""));
        assert!(
            observed
                .contains("\"redirect_uri\":\"https://auth.example.com/oauth/notion/callback\"")
        );
        assert!(!observed.contains("chatgpt-state-"));
        // Successfully exchanging a token NEVER approves the MCP grant.
        assert!(redirect.take(s).is_none());
    }

    #[tokio::test]
    async fn wrong_user_or_workspace_is_denied_after_provider_exchange() {
        for (ws, owner) in [
            ("evil-workspace", "user-456"),
            ("workspace-123", "evil-user"),
        ] {
            let (redirect, flow, registration, allowed) = setup();
            let start = redirect.begin(&flow, &request()).unwrap();
            let (endpoint, task) = token_server(grant_json(ws, "user", owner), 200).await;
            let client = NotionTokenClient::new(&registration, SECRET)
                .unwrap()
                .for_local_fixture(&endpoint);
            assert!(matches!(
                NotionCallback::new(&redirect, &client, &allowed)
                    .complete(&format!("state={}&code=code-123", state(start.expose())))
                    .await,
                Err(CallbackError::IdentityMismatch)
            ));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn bad_provider_response_and_denial_never_release_a_grant() {
        for (payload,status,expected) in [
            (grant_json("workspace-123","workspace","user-456"),200,CallbackError::InvalidGrant),
            (json!({"access_token":"secret","token_type":"bearer","workspace_id":"workspace-123","bot_id":"bot-123","owner":{"type":"user"}}).to_string(),200,CallbackError::InvalidGrant),
            (json!({"error":"unauthorized","access_token":"SHOULD_NEVER_LEAK"}).to_string(),401,CallbackError::TokenExchangeFailed),
            ("not-json".to_owned(),200,CallbackError::InvalidGrant),
        ] {
            let (redirect,flow,registration,allowed)=setup();
            let start=redirect.begin(&flow,&request()).unwrap();
            let (endpoint, task)=token_server(payload,status).await;
            let client=NotionTokenClient::new(&registration,SECRET).unwrap().for_local_fixture(&endpoint);
            let error=NotionCallback::new(&redirect,&client,&allowed)
                .complete(&format!("state={}&code=provider-code",state(start.expose()))).await.err().unwrap();
            assert_eq!(error,expected);
            assert!(!format!("{error:?}").contains("SHOULD_NEVER_LEAK"));
            task.await.unwrap();
            assert!(redirect.take(state(start.expose())).is_none());
        }
    }

    #[tokio::test]
    async fn forged_or_replayed_state_is_denied_before_contacting_provider() {
        let (redirect, flow, registration, allowed) = setup();
        let start = redirect.begin(&flow, &request()).unwrap();
        let s = state(start.expose());
        let client = NotionTokenClient::new(&registration, SECRET)
            .unwrap()
            .for_local_fixture("http://127.0.0.1:1/v1/oauth/token");
        let callback = NotionCallback::new(&redirect, &client, &allowed);
        assert!(matches!(
            callback
                .complete("code=a&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .await,
            Err(CallbackError::InvalidState)
        ));
        assert!(matches!(
            callback
                .complete(&format!("error=access_denied&state={s}"))
                .await,
            Err(CallbackError::RejectedByNotion)
        ));
        assert!(matches!(
            callback.complete(&format!("code=a&state={s}")).await,
            Err(CallbackError::InvalidState)
        ));
    }

    #[test]
    fn strict_callback_query_denies_duplicate_params_and_injected_values() {
        const STATE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        for query in [
            "",
            "state=x&code=y",
            "code=x",
            "code=x&state=too-short",
            "code=x&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "code=x&code=y&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "code=x&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&error=access_denied",
            "code=x&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&redirect_uri=https://evil.example",
            "code=%GG&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "code=%00&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "code=x&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#fragment",
        ] {
            assert!(
                CallbackParams::parse(query).is_err(),
                "bad query not rejected"
            );
        }
        assert!(CallbackParams::parse(&format!("state={STATE}&code=abc%2D123")).is_ok());
        assert!(
            CallbackParams::parse(&format!(
                "state={STATE}&error=access_denied&error_description=ignored"
            ))
            .is_ok()
        );
    }

    #[test]
    fn malformed_tokens_and_configuration_are_rejected_without_secret_reflection() {
        for data in [
            json!({"access_token":"abc","token_type":"Basic","workspace_id":"workspace-123","bot_id":"bot-123","owner":{"type":"user","user":{"id":"user-456"}}}),
            json!({"access_token":"abc","token_type":"bearer","workspace_id":"workspace-123","bot_id":"bot-123","owner":{"type":"bot","user":{"id":"user-456"}}}),
            json!({"access_token":"abc","token_type":"bearer","workspace_id":"workspace-123","bot_id":"bot-123","owner":{"type":"user","user":{"id":""}}}),
            json!({"access_token":"abc","token_type":"bearer","workspace_id":"workspace-123","bot_id":"bot-123","owner":{"type":"user","user":{"id":"user-456"}},"refresh_token":"bad token"}),
        ] {
            assert!(matches!(
                NotionGrant::parse(data.to_string().as_bytes()),
                Err(CallbackError::InvalidGrant)
            ));
        }
        let (_, _, registration, _) = setup();
        for secret in ["", "bad secret", "broken\nsecret"] {
            assert!(NotionTokenClient::new(&registration, secret).is_err());
        }
        assert!(NotionOwnerPolicy::new("", "user-456").is_err());
        assert!(NotionOwnerPolicy::new("workspace-123", "").is_err());
    }
}
