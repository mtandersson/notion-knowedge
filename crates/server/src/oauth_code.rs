//! RFC 7636 Authorization Code + PKCE engine for the approved-grant OAuth flow.
//!
//! Deliberately not mounted as a public login yet: #121-#128 must supply a
//! verified upstream Notion grant, persistence and live resource authorization.
//! A Notion access token never enters this component or its issued MCP tokens.
use std::{
    collections::HashMap,
    fmt,
    io::Read,
    sync::Mutex,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

const CODE_TTL: Duration = Duration::from_secs(120);
const ACCESS_TTL: Duration = Duration::from_secs(600);
const PENDING_LIMIT: usize = 512;
const CODE_LIMIT: usize = 512;
const TOKEN_LIMIT: usize = 2048;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidRequest,
    InvalidClient,
    InvalidTarget,
    InvalidGrant,
    AccessDenied,
    TemporarilyUnavailable,
}
impl Error {
    pub fn oauth_code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidClient => "invalid_client",
            Self::InvalidTarget => "invalid_target",
            Self::InvalidGrant => "invalid_grant",
            Self::AccessDenied => "access_denied",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
        }
    }
}

/// Registered public-client identity; configured out of band and immutable
/// for each server instance. No dynamic/first-login registration is allowed.
#[derive(Clone)]
pub struct Client {
    client_id: String,
    redirect_uri: String,
    resource: String,
}
impl Client {
    pub fn new(client_id: &str, redirect_uri: &str, resource: &str) -> Result<Self, Error> {
        if !opaque_identifier(client_id, 1, 128) || !https_url(redirect_uri) || !https_url(resource)
        {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            client_id: client_id.to_owned(),
            redirect_uri: redirect_uri.to_owned(),
            resource: resource.to_owned(),
        })
    }
}

fn opaque_identifier(value: &str, min: usize, max: usize) -> bool {
    (min..=max).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
}

fn https_url(value: &str) -> bool {
    // Uri rejects URL fragments; the exact client registration is compared,
    // so we need not canonicalize arbitrary redirect URLs at exchange time.
    if value.len() > 2048 || !value.is_ascii() || value.contains('#') {
        return false;
    }
    let Ok(uri) = value.parse::<axum::http::Uri>() else {
        return false;
    };
    uri.scheme_str() == Some("https")
        && uri
            .authority()
            .is_some_and(|a| !a.host().is_empty() && !a.as_str().contains('@'))
}

#[derive(Clone)]
pub struct AllowedNotionIdentity {
    workspace_id: String,
    owner_user_id: String,
}
impl AllowedNotionIdentity {
    pub fn new(workspace_id: &str, owner_user_id: &str) -> Result<Self, Error> {
        if !opaque_identifier(workspace_id, 1, 128) || !opaque_identifier(owner_user_id, 1, 128) {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            workspace_id: workspace_id.to_owned(),
            owner_user_id: owner_user_id.to_owned(),
        })
    }

    fn matches(&self, grant: &ApprovedNotionGrant<'_>) -> bool {
        grant.owner_type == "user"
            && !grant.grant_id.is_empty()
            && grant.epoch > 0
            && grant.workspace_id == self.workspace_id
            && grant.owner_user_id == self.owner_user_id
    }
}

/// Input to the grant identity gate, to be populated ONLY from a verified
/// authoritative Notion OAuth callback or an already approved durable record.
/// This shape alone does not authenticate a caller.
pub struct ApprovedNotionGrant<'a> {
    pub grant_id: &'a str,
    pub epoch: u64,
    pub workspace_id: &'a str,
    pub owner_type: &'a str,
    pub owner_user_id: &'a str,
}

pub struct AuthorizationRequest<'a> {
    pub response_type: &'a str,
    pub client_id: &'a str,
    pub redirect_uri: &'a str,
    pub resource: &'a str,
    pub state: &'a str,
    pub code_challenge: &'a str,
    pub code_challenge_method: &'a str,
    pub scope: &'a str,
}
pub struct TokenRequest<'a> {
    pub grant_type: &'a str,
    pub code: &'a str,
    pub client_id: &'a str,
    pub redirect_uri: &'a str,
    pub resource: &'a str,
    pub code_verifier: &'a str,
}

#[derive(Clone)]
struct Binding {
    grant_id: String,
    epoch: u64,
    workspace_id: String,
    owner_user_id: String,
}
impl Binding {
    fn matches(&self, grant: &ApprovedNotionGrant<'_>) -> bool {
        self.grant_id == grant.grant_id
            && self.epoch == grant.epoch
            && self.workspace_id == grant.workspace_id
            && self.owner_user_id == grant.owner_user_id
    }
}
struct Pending {
    challenge: String,
    redirect_uri: String,
    resource: String,
    state: String,
    scope: String,
    expires: Instant,
}
struct IssuedCode {
    challenge: String,
    redirect_uri: String,
    resource: String,
    scope: String,
    binding: Binding,
    expires: Instant,
}
struct IssuedToken {
    binding: Binding,
    resource: String,
    scope: String,
    expires: Instant,
}
#[derive(Default)]
struct Records {
    pending: HashMap<[u8; 32], Pending>,
    codes: HashMap<[u8; 32], IssuedCode>,
    tokens: HashMap<[u8; 32], IssuedToken>,
}
impl Records {
    fn sweep(&mut self, now: Instant) {
        self.pending.retain(|_, p| p.expires > now);
        self.codes.retain(|_, c| c.expires > now);
        self.tokens.retain(|_, t| t.expires > now);
    }
}

/// Secrets are held in private fields so Debug output and ordinary errors
/// never serialize OAuth codes or MCP bearer tokens.
pub struct Secret(String);
impl Secret {
    /// For constructing an outbound redirect URL that must itself be redacted.
    pub(crate) fn from_internal(value: String) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

pub struct Redirect {
    pub code: Secret,
    pub state: String,
    pub redirect_uri: String,
}
pub struct AccessToken {
    pub access_token: Secret,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub scope: String,
}

/// Process-local, lock-serialized transaction boundary. Restart intentionally
/// invalidates pending codes/tokens. #123/#127 must provide durable approval,
/// revocation and multi-process behavior before public production use.
pub struct AuthorizationFlow {
    client: Client,
    allowed: AllowedNotionIdentity,
    records: Mutex<Records>,
}
impl AuthorizationFlow {
    pub fn new(client: Client, allowed: AllowedNotionIdentity) -> Self {
        Self {
            client,
            allowed,
            records: Mutex::new(Records::default()),
        }
    }

    /// Start an unapproved transaction. Does NOT issue a code or authorize a
    /// Notion grant. The returned opaque handle belongs to the future server-
    /// side Notion callback correlation, never to a client as proof of login.
    pub fn begin(&self, request: &AuthorizationRequest<'_>) -> Result<Secret, Error> {
        self.begin_at(request, Instant::now())
    }
    fn begin_at(&self, request: &AuthorizationRequest<'_>, now: Instant) -> Result<Secret, Error> {
        if request.client_id != self.client.client_id {
            return Err(Error::InvalidClient);
        }
        if request.redirect_uri != self.client.redirect_uri
            || request.response_type != "code"
            || request.code_challenge_method != "S256"
            || !valid_challenge(request.code_challenge)
            || !opaque_identifier(request.state, 16, 256)
            || request.scope != "knowledge:read"
        {
            return Err(Error::InvalidRequest);
        }
        if request.resource != self.client.resource {
            return Err(Error::InvalidTarget);
        }
        let handle = random_secret()?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::TemporarilyUnavailable)?;
        records.sweep(now);
        if records.pending.len() >= PENDING_LIMIT {
            return Err(Error::TemporarilyUnavailable);
        }
        records.pending.insert(
            digest(handle.expose()),
            Pending {
                challenge: request.code_challenge.to_owned(),
                redirect_uri: request.redirect_uri.to_owned(),
                resource: request.resource.to_owned(),
                state: request.state.to_owned(),
                scope: request.scope.to_owned(),
                expires: now + CODE_TTL,
            },
        );
        Ok(handle)
    }

    /// Only the upstream Notion callback adapter (#122) may invoke this after
    /// verifying its own separate OAuth state and authoritative grant response.
    /// The allowlist is checked AGAIN here and AGAIN at exchange/verification.
    pub fn approve(
        &self,
        transaction: &str,
        grant: &ApprovedNotionGrant<'_>,
    ) -> Result<Redirect, Error> {
        self.approve_at(transaction, grant, Instant::now())
    }
    fn approve_at(
        &self,
        transaction: &str,
        grant: &ApprovedNotionGrant<'_>,
        now: Instant,
    ) -> Result<Redirect, Error> {
        if !self.allowed.matches(grant) {
            return Err(Error::AccessDenied);
        }
        let code = random_secret()?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::TemporarilyUnavailable)?;
        records.sweep(now);
        let Some(pending) = records.pending.remove(&digest(transaction)) else {
            return Err(Error::InvalidGrant);
        };
        if records.codes.len() >= CODE_LIMIT {
            return Err(Error::TemporarilyUnavailable);
        }
        records.codes.insert(
            digest(code.expose()),
            IssuedCode {
                challenge: pending.challenge,
                redirect_uri: pending.redirect_uri.clone(),
                resource: pending.resource,
                scope: pending.scope,
                binding: Binding {
                    grant_id: grant.grant_id.to_owned(),
                    epoch: grant.epoch,
                    workspace_id: grant.workspace_id.to_owned(),
                    owner_user_id: grant.owner_user_id.to_owned(),
                },
                expires: now + CODE_TTL,
            },
        );
        Ok(Redirect {
            code,
            state: pending.state,
            redirect_uri: pending.redirect_uri,
        })
    }

    pub fn redeem(
        &self,
        request: &TokenRequest<'_>,
        current_grant: &ApprovedNotionGrant<'_>,
    ) -> Result<AccessToken, Error> {
        self.redeem_at(request, current_grant, Instant::now())
    }
    fn redeem_at(
        &self,
        request: &TokenRequest<'_>,
        current_grant: &ApprovedNotionGrant<'_>,
        now: Instant,
    ) -> Result<AccessToken, Error> {
        if request.grant_type != "authorization_code"
            || request.client_id != self.client.client_id
            || !valid_verifier(request.code_verifier)
        {
            return Err(Error::InvalidGrant);
        }
        if request.resource != self.client.resource {
            return Err(Error::InvalidTarget);
        }
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::TemporarilyUnavailable)?;
        records.sweep(now);
        // Consume atomically, even after an incorrect verifier. This prevents
        // unlimited brute-force attempts and replay by parallel callers.
        let Some(code) = records.codes.remove(&digest(request.code)) else {
            return Err(Error::InvalidGrant);
        };
        if code.redirect_uri != request.redirect_uri
            || code.resource != request.resource
            || !constant_time_eq(
                code.challenge.as_bytes(),
                pkce_challenge(request.code_verifier).as_bytes(),
            )
            || !self.allowed.matches(current_grant)
            || !code.binding.matches(current_grant)
        {
            return Err(Error::InvalidGrant);
        }
        if records.tokens.len() >= TOKEN_LIMIT {
            return Err(Error::TemporarilyUnavailable);
        }
        let token = random_secret()?;
        records.tokens.insert(
            digest(token.expose()),
            IssuedToken {
                binding: code.binding,
                resource: code.resource,
                scope: code.scope.clone(),
                expires: now + ACCESS_TTL,
            },
        );
        Ok(AccessToken {
            access_token: token,
            token_type: "Bearer",
            expires_in: ACCESS_TTL.as_secs(),
            scope: code.scope,
        })
    }

    /// A token can only authorize after consulting the CURRENT approved
    /// grant. In-memory token presence alone never establishes identity.
    /// Actual MCP route enforcement still belongs to #127.
    pub fn verify(&self, bearer: &str, current_grant: &ApprovedNotionGrant<'_>) -> bool {
        self.verify_at(bearer, current_grant, Instant::now())
    }
    fn verify_at(
        &self,
        bearer: &str,
        current_grant: &ApprovedNotionGrant<'_>,
        now: Instant,
    ) -> bool {
        if !self.allowed.matches(current_grant) {
            return false;
        }
        let Ok(records) = self.records.lock() else {
            return false;
        };
        records.tokens.get(&digest(bearer)).is_some_and(|record| {
            record.expires > now
                && record.resource == self.client.resource
                && record.scope == "knowledge:read"
                && record.binding.matches(current_grant)
        })
    }
}

fn digest(input: &str) -> [u8; 32] {
    Sha256::digest(input.as_bytes()).into()
}
fn pkce_challenge(verifier: &str) -> String {
    base64_url(&digest(verifier))
}
fn valid_verifier(value: &str) -> bool {
    opaque_identifier(value, 43, 128)
}
fn valid_challenge(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
fn base64_url(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let a = chunk[0];
        output.push(ALPHABET[(a >> 2) as usize] as char);
        let second = ((a & 3) << 4) | (chunk.get(1).copied().unwrap_or(0) >> 4);
        output.push(ALPHABET[second as usize] as char);
        if let Some(b) = chunk.get(1) {
            let third = ((b & 15) << 2) | (chunk.get(2).copied().unwrap_or(0) >> 6);
            output.push(ALPHABET[third as usize] as char);
        }
        if let Some(b) = chunk.get(2) {
            output.push(ALPHABET[(b & 63) as usize] as char);
        }
    }
    output
}

pub(crate) fn random_secret() -> Result<Secret, Error> {
    let mut bytes = [0u8; 32];
    // The supported Linux/macOS service runtime uses the OS CSPRNG.
    // A failed entropy source aborts the operation; never mint a weak token.
    std::fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|_| Error::TemporarilyUnavailable)?;
    Ok(Secret(base64_url(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
    const CLIENT: &str = "registered-client";
    const REDIRECT: &str = "https://chatgpt.example.com/callback";
    const RESOURCE: &str = "https://knowledge.example.com/mcp";
    const STATE: &str = "random-client-state-123";

    fn flow() -> AuthorizationFlow {
        AuthorizationFlow::new(
            Client::new(CLIENT, REDIRECT, RESOURCE).unwrap(),
            AllowedNotionIdentity::new("workspace-123", "user-456").unwrap(),
        )
    }
    fn authorization<'a>() -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            response_type: "code",
            client_id: CLIENT,
            redirect_uri: REDIRECT,
            resource: RESOURCE,
            state: STATE,
            code_challenge: CHALLENGE,
            code_challenge_method: "S256",
            scope: "knowledge:read",
        }
    }
    fn grant<'a>() -> ApprovedNotionGrant<'a> {
        ApprovedNotionGrant {
            grant_id: "server-side-verified-grant",
            epoch: 1,
            workspace_id: "workspace-123",
            owner_type: "user",
            owner_user_id: "user-456",
        }
    }
    fn exchange<'a>(code: &'a str) -> TokenRequest<'a> {
        TokenRequest {
            grant_type: "authorization_code",
            code,
            client_id: CLIENT,
            redirect_uri: REDIRECT,
            resource: RESOURCE,
            code_verifier: VERIFIER,
        }
    }

    #[test]
    fn rfc7636_known_s256_vector_and_successful_code_exchange() {
        assert_eq!(pkce_challenge(VERIFIER), CHALLENGE);
        let flow = flow();
        let start = flow.begin(&authorization()).unwrap();
        assert!(format!("{start:?}").contains("REDACTED"));
        let redirect = flow.approve(start.expose(), &grant()).unwrap();
        assert_eq!(redirect.state, STATE);
        assert_eq!(redirect.redirect_uri, REDIRECT);
        assert_ne!(redirect.code.expose(), start.expose());
        let token = flow
            .redeem(&exchange(redirect.code.expose()), &grant())
            .unwrap();
        assert_eq!(token.token_type, "Bearer");
        assert_eq!(token.expires_in, 600);
        assert_eq!(token.scope, "knowledge:read");
        assert!(flow.verify(token.access_token.expose(), &grant()));
        assert!(!token.access_token.expose().contains("workspace"));
        assert!(!token.access_token.expose().contains("server-side"));
        assert!(format!("{:?}", token.access_token).contains("REDACTED"));
        assert!(matches!(
            flow.redeem(&exchange(redirect.code.expose()), &grant()),
            Err(Error::InvalidGrant)
        ));
    }

    #[test]
    fn all_authorization_inputs_are_bound_and_no_plain_pkce() {
        let flow = flow();
        for invalid in [
            AuthorizationRequest {
                response_type: "token",
                ..authorization()
            },
            AuthorizationRequest {
                client_id: "wrong",
                ..authorization()
            },
            AuthorizationRequest {
                redirect_uri: "https://attacker.example.com/cb",
                ..authorization()
            },
            AuthorizationRequest {
                resource: "https://wrong.example.com/mcp",
                ..authorization()
            },
            AuthorizationRequest {
                state: "tiny",
                ..authorization()
            },
            AuthorizationRequest {
                code_challenge_method: "plain",
                ..authorization()
            },
            AuthorizationRequest {
                code_challenge_method: "",
                ..authorization()
            },
            AuthorizationRequest {
                code_challenge: "?",
                ..authorization()
            },
            AuthorizationRequest {
                scope: "admin",
                ..authorization()
            },
        ] {
            assert!(flow.begin(&invalid).is_err());
        }
        assert!(Client::new(CLIENT, "http://attacker.example.com/cb", RESOURCE).is_err());
        assert!(Client::new(CLIENT, "https://user@attacker.example.com/cb", RESOURCE).is_err());
        assert!(Client::new(CLIENT, "https://chatgpt.example.com/cb#x", RESOURCE).is_err());
    }

    #[test]
    fn grant_allowlist_is_mandatory_before_code_and_at_exchange() {
        let flow = flow();
        let pending = flow.begin(&authorization()).unwrap();
        let wrong_user = ApprovedNotionGrant {
            owner_user_id: "attacker",
            ..grant()
        };
        let wrong_workspace = ApprovedNotionGrant {
            workspace_id: "attacker",
            ..grant()
        };
        let invalid_owner = ApprovedNotionGrant {
            owner_type: "bot",
            ..grant()
        };
        for invalid in [&wrong_user, &wrong_workspace, &invalid_owner] {
            assert!(matches!(
                flow.approve(pending.expose(), invalid),
                Err(Error::AccessDenied)
            ));
        }
        let redirect = flow.approve(pending.expose(), &grant()).unwrap();
        assert!(matches!(
            flow.redeem(&exchange(redirect.code.expose()), &wrong_workspace),
            Err(Error::InvalidGrant)
        ));
        assert!(matches!(
            flow.redeem(&exchange(redirect.code.expose()), &grant()),
            Err(Error::InvalidGrant)
        ));
    }

    #[test]
    fn rejected_verifier_or_redirect_consumes_code() {
        let flow = flow();
        let pending = flow.begin(&authorization()).unwrap();
        let redirect = flow.approve(pending.expose(), &grant()).unwrap();
        let wrong_verifier = TokenRequest {
            code_verifier: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ..exchange(redirect.code.expose())
        };
        assert!(matches!(
            flow.redeem(&wrong_verifier, &grant()),
            Err(Error::InvalidGrant)
        ));
        assert!(matches!(
            flow.redeem(&exchange(redirect.code.expose()), &grant()),
            Err(Error::InvalidGrant)
        ));
        let pending = flow.begin(&authorization()).unwrap();
        let redirect = flow.approve(pending.expose(), &grant()).unwrap();
        let wrong_redirect = TokenRequest {
            redirect_uri: "https://evil.example.com/",
            ..exchange(redirect.code.expose())
        };
        assert!(flow.redeem(&wrong_redirect, &grant()).is_err());
        assert!(
            flow.redeem(&exchange(redirect.code.expose()), &grant())
                .is_err()
        );
    }

    #[test]
    fn expiry_and_epoch_rotation_fails_closed() {
        let flow = flow();
        let now = Instant::now();
        let pending = flow.begin_at(&authorization(), now).unwrap();
        assert!(matches!(
            flow.approve_at(pending.expose(), &grant(), now + CODE_TTL),
            Err(Error::InvalidGrant)
        ));
        let pending = flow.begin_at(&authorization(), now).unwrap();
        let redirect = flow
            .approve_at(pending.expose(), &grant(), now + Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            flow.redeem_at(
                &exchange(redirect.code.expose()),
                &grant(),
                now + CODE_TTL + Duration::from_secs(1)
            ),
            Err(Error::InvalidGrant)
        ));
        let pending = flow.begin_at(&authorization(), now).unwrap();
        let redirect = flow.approve_at(pending.expose(), &grant(), now).unwrap();
        let token = flow
            .redeem_at(&exchange(redirect.code.expose()), &grant(), now)
            .unwrap();
        let next_epoch = ApprovedNotionGrant {
            epoch: 2,
            ..grant()
        };
        assert!(!flow.verify_at(token.access_token.expose(), &next_epoch, now));
        assert!(!flow.verify_at(token.access_token.expose(), &grant(), now + ACCESS_TTL));
        assert!(!flow.verify_at("unissued-token", &grant(), now));
    }

    #[test]
    fn callback_transactions_and_codes_are_single_use_even_under_replay() {
        let authorization_flow = flow();
        let pending = authorization_flow.begin(&authorization()).unwrap();
        let redirect = authorization_flow
            .approve(pending.expose(), &grant())
            .unwrap();
        assert!(matches!(
            authorization_flow.approve(pending.expose(), &grant()),
            Err(Error::InvalidGrant)
        ));
        let token = authorization_flow
            .redeem(&exchange(redirect.code.expose()), &grant())
            .unwrap();
        assert!(authorization_flow.verify(token.access_token.expose(), &grant()));
        let restarted = flow();
        assert!(!restarted.verify(token.access_token.expose(), &grant()));
        assert!(matches!(
            restarted.redeem(&exchange(redirect.code.expose()), &grant()),
            Err(Error::InvalidGrant)
        ));
    }
}
