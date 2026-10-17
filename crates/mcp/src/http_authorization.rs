//! Trusted HTTP authorization evidence. Never construct from MCP JSON or headers.
//! HTTP middleware supplies this through SDK request Parts extensions. It owns
//! no upstream or MCP credentials and must never be serialized to a client.
use std::{fmt, time::Instant};

#[derive(Clone)]
pub struct HttpAuthorization {
    token_digest: [u8; 32],
    grant_id: String,
    epoch: u64,
    workspace_id: String,
    owner_user_id: String,
    expires: Instant,
    cancellation: tokio_util::sync::CancellationToken,
}
impl HttpAuthorization {
    /// Only trusted server authorization composition may construct evidence.
    pub fn new(
        token_digest: [u8; 32],
        grant_id: String,
        epoch: u64,
        workspace_id: String,
        owner_user_id: String,
        expires: Instant,
    ) -> Self {
        Self {
            token_digest,
            grant_id,
            epoch,
            workspace_id,
            owner_user_id,
            expires,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }
    pub fn with_cancellation(mut self, cancellation: tokio_util::sync::CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }
    pub fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation.clone()
    }
    pub fn is_active(&self) -> bool {
        !self.cancellation.is_cancelled() && self.expires > Instant::now()
    }
    pub fn matches_token_digest(&self, digest: &[u8; 32]) -> bool {
        &self.token_digest == digest
    }
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn owner_user_id(&self) -> &str {
        &self.owner_user_id
    }
    pub fn expires_at(&self) -> Instant {
        self.expires
    }
    pub fn same_session(&self, other: &Self) -> bool {
        self.token_digest == other.token_digest
            && self.grant_id == other.grant_id
            && self.epoch == other.epoch
            && self.workspace_id == other.workspace_id
            && self.owner_user_id == other.owner_user_id
            && self.expires == other.expires
    }
}
impl fmt::Debug for HttpAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HttpAuthorization([REDACTED])")
    }
}
