//! Outbound Notion OAuth redirect, bound to a pending MCP PKCE transaction.
//! No public login/callback is enabled until #122-#128 enforce the grant.
use std::{collections::HashMap, fmt, sync::Mutex, time::{Duration, Instant}};
use sha2::{Digest, Sha256};
use crate::oauth_code::{AuthorizationFlow, AuthorizationRequest, Error, Secret};

pub const NOTION_AUTHORIZE: &str = "https://api.notion.com/v1/oauth/authorize";
const STATE_TTL: Duration = Duration::from_secs(120);
const MAX_PENDING: usize = 256;

/// Fixed, operator-controlled Notion client configuration. The callback
/// MUST be the canonical issuer's pre-registered HTTPS callback.
#[derive(Clone, Debug)]
pub struct NotionOAuthConfig { client_id: String, redirect_uri: String }
impl NotionOAuthConfig {
    pub fn new(client_id: &str, redirect_uri: &str, issuer: &str) -> Result<Self, &'static str> {
        if client_id.is_empty() || client_id.len() > 128
            || !client_id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        { return Err("NK_NOTION_OAUTH_CLIENT_ID"); }
        if redirect_uri != format!("{issuer}/oauth/notion/callback") {
            return Err("NK_NOTION_OAUTH_REDIRECT_URI");
        }
        Ok(Self { client_id: client_id.to_owned(), redirect_uri: redirect_uri.to_owned() })
    }
}

/// Redacted because the redirect URL contains an unguessable one-time state.
pub struct AuthorizationUrl(Secret);
impl AuthorizationUrl {
    /// To be used only as the Location of a trusted authorization response.
    pub fn expose(&self) -> &str { self.0.expose() }
}
impl fmt::Debug for AuthorizationUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthorizationUrl([REDACTED])")
    }
}

struct Pending { mcp_transaction: Secret, expires: Instant }
/// In-memory state correlation; #122 consumes it BEFORE upstream code exchange.
pub struct NotionOAuthRedirect {
    config: NotionOAuthConfig,
    pending: Mutex<HashMap<[u8;32], Pending>>,
}
impl NotionOAuthRedirect {
    pub fn new(config: NotionOAuthConfig) -> Self {
        Self { config, pending: Mutex::new(HashMap::new()) }
    }
    /// Creates BOTH an unapproved MCP transaction and a distinct anti-CSRF
    /// state. No user-controlled Notion redirect/client identifier is accepted.
    pub fn begin(&self, flow: &AuthorizationFlow, request: &AuthorizationRequest<'_>) -> Result<AuthorizationUrl, Error> {
        self.begin_at(flow, request, Instant::now())
    }
    fn begin_at(&self, flow: &AuthorizationFlow, request: &AuthorizationRequest<'_>, now: Instant) -> Result<AuthorizationUrl, Error> {
        let mut pending = self.pending.lock().map_err(|_| Error::TemporarilyUnavailable)?;
        pending.retain(|_, p| p.expires > now);
        if pending.len() >= MAX_PENDING { return Err(Error::TemporarilyUnavailable); }
        let state = crate::oauth_code::random_secret()?;
        let digest: [u8;32] = Sha256::digest(state.expose().as_bytes()).into();
        if pending.contains_key(&digest) { return Err(Error::TemporarilyUnavailable); }
        // The MCP client, redirect, resource and S256 must pass #120 first.
        let mcp_transaction = flow.begin(request)?;
        pending.insert(digest, Pending { mcp_transaction, expires: now + STATE_TTL });
        // Notion capabilities are selected on the integration and consent
        // screens; an arbitrary OAuth scope is NOT requested in this URL.
        let url = format!(
            "{NOTION_AUTHORIZE}?owner=user&client_id={}&redirect_uri={}&response_type=code&state={}",
            pct_encode(&self.config.client_id),
            pct_encode(&self.config.redirect_uri),
            pct_encode(state.expose()),
        );
        Ok(AuthorizationUrl(Secret::from_internal(url)))
    }
    /// Single-use correlation, NOT authentication. #122 must exchange the
    /// Notion code and validate the live user/workspace BEFORE calling
    /// AuthorizationFlow::approve with this returned MCP transaction.
    pub fn take(&self, state: &str) -> Option<Secret> { self.take_at(state, Instant::now()) }
    fn take_at(&self, state: &str, now: Instant) -> Option<Secret> {
        if state.len() != 43 || !state.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') { return None; }
        let digest: [u8;32] = Sha256::digest(state.as_bytes()).into();
        let mut pending = self.pending.lock().ok()?;
        let value = pending.remove(&digest)?;
        (value.expires > now).then_some(value.mcp_transaction)
    }
}
fn pct_encode(s: &str) -> String {
    const HEX: &[u8;16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-'|b'.'|b'_'|b'~') { out.push(b as char); }
        else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 15) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth_code::{AllowedNotionIdentity, Client};
    const ISSUER: &str = "https://auth.example.com";
    const CALLBACK: &str = "https://auth.example.com/oauth/notion/callback";
    const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
    fn setup() -> (NotionOAuthRedirect, AuthorizationFlow) {
        (NotionOAuthRedirect::new(NotionOAuthConfig::new("integration-id", CALLBACK, ISSUER).unwrap()),
         AuthorizationFlow::new(Client::new("chatgpt","https://chatgpt.example.com/callback","https://mcp.example.com/mcp").unwrap(),
         AllowedNotionIdentity::new("workspace-123","user-456").unwrap()))
    }
    fn request<'a>() -> AuthorizationRequest<'a> {
        AuthorizationRequest { response_type:"code",client_id:"chatgpt",redirect_uri:"https://chatgpt.example.com/callback",
            resource:"https://mcp.example.com/mcp",state:"chatgpt-state-123456789",code_challenge:CHALLENGE,
            code_challenge_method:"S256",scope:"knowledge:read" }
    }
    fn state(url: &str) -> &str { url.split("&state=").nth(1).expect("state") }

    #[test]
    fn fixed_encoded_redirect_is_bounded_to_distinct_pending_mcp_flows() {
        let (redirect, flow) = setup();
        let first = redirect.begin(&flow, &request()).unwrap();
        let second = redirect.begin(&flow, &request()).unwrap();
        assert!(first.expose().starts_with("https://api.notion.com/v1/oauth/authorize?owner=user&client_id=integration-id&redirect_uri=https%3A%2F%2Fauth.example.com%2Foauth%2Fnotion%2Fcallback&response_type=code&state="));
        assert_eq!(first.expose().matches('&').count(), 4);
        assert!(!first.expose().contains("scope="));
        assert!(format!("{first:?}").contains("REDACTED"));
        let a = state(first.expose());
        let b = state(second.expose());
        assert_eq!(a.len(),43);
        assert_ne!(a,b);
        assert!(redirect.take("incorrect-state").is_none());
        let one = redirect.take(a).unwrap();
        let two = redirect.take(b).unwrap();
        assert_ne!(one.expose(), two.expose());
        assert!(redirect.take(a).is_none());
    }

    #[test]
    fn invalid_mcp_pkce_or_client_never_creates_notions_authorization() {
        let (redirect, flow) = setup();
        let wrong_client = AuthorizationRequest { client_id: "unregistered", ..request() };
        let wrong_pkce = AuthorizationRequest { code_challenge_method: "plain", ..request() };
        assert!(redirect.begin(&flow, &wrong_client).is_err());
        assert!(redirect.begin(&flow, &wrong_pkce).is_err());
        assert!(redirect.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn state_expires_and_is_single_use() {
        let (redirect, flow) = setup();
        let now = Instant::now();
        let first = redirect.begin_at(&flow, &request(), now).unwrap();
        assert!(redirect.take_at(state(first.expose()), now + STATE_TTL).is_none());
        assert!(redirect.take_at(state(first.expose()), now).is_none());
        let second = redirect.begin_at(&flow, &request(), now).unwrap();
        assert!(redirect.take_at(state(second.expose()), now + STATE_TTL - Duration::from_nanos(1)).is_some());
        assert!(redirect.take_at(state(second.expose()), now).is_none());
    }

    #[test]
    fn registered_callback_and_client_cannot_be_redirected_or_injected() {
        assert!(NotionOAuthConfig::new("client_123", CALLBACK, ISSUER).is_ok());
        for id in ["","a&owner=workspace","contains space","a?scope=write"] {
            assert!(NotionOAuthConfig::new(id,CALLBACK,ISSUER).is_err());
        }
        for callback in ["http://auth.example.com/oauth/notion/callback","https://attacker.example.com/oauth/notion/callback",
            "https://auth.example.com/oauth/notion/callback?next=evil","https://auth.example.com/oauth/notion/callback/"] {
            assert!(NotionOAuthConfig::new("client",callback,ISSUER).is_err());
        }
        assert_eq!(pct_encode("https://example.com/a?b=1&c=2"),"https%3A%2F%2Fexample.com%2Fa%3Fb%3D1%26c%3D2");
    }
    #[test]
    fn restart_invalidates_correlation() {
        let (redirect, flow) = setup();
        let first = redirect.begin(&flow, &request()).unwrap();
        let (other, _) = setup();
        assert!(other.take(state(first.expose())).is_none());
        assert!(redirect.take(&format!("{}x", state(first.expose()))).is_none());
    }
}
