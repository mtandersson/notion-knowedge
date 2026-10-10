//! Server-owned, single-process Notion OAuth request composition (#299).
//! This is never an MCP login/authorization layer.
use std::{fmt, future::Future, sync::Arc};

use notion_knowledge_core::backend::BackendError;
use notion_knowledge_notion::NotionClient;

use crate::{
    config::Config,
    notion_grant_store::{GrantStore, StoreError},
    notion_oauth_callback::NotionTokenClient,
    notion_oauth_refresh::{NotionRefresh, RefreshError},
};

/// No secret-bearing inner errors are ever formatted for transport or logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialError {
    Configuration,
    NoGrant,
    ReauthorizationRequired,
    IdentityMismatch,
    Unavailable,
}
impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Notion OAuth credentials unavailable; authorization required")
    }
}
impl std::error::Error for CredentialError {}

fn map_refresh(error: RefreshError) -> CredentialError {
    match error {
        RefreshError::NoGrant => CredentialError::NoGrant,
        RefreshError::ReauthorizationRequired => CredentialError::ReauthorizationRequired,
        RefreshError::InvalidIdentity => CredentialError::IdentityMismatch,
        RefreshError::StoreUnavailable | RefreshError::ProviderUnavailable => {
            CredentialError::Unavailable
        }
    }
}

/// One owned coordinator, initialized ONCE by the composition root.
/// Cloning this wrapper shares the same refresh lock; no per-request
/// coordinator, stale integration-token fallback or public auth routes.
#[derive(Clone)]
pub struct NotionOAuthRuntime {
    coordinator: Arc<NotionRefresh<'static>>,
    store: Arc<GrantStore>,
}
impl fmt::Debug for NotionOAuthRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotionOAuthRuntime([REDACTED])")
    }
}

impl NotionOAuthRuntime {
    pub fn from_config(config: &Config) -> Result<Option<Self>, CredentialError> {
        let Some(settings) = &config.notion_grant_store else {
            return Ok(None);
        };
        let callback = config
            .notion_oauth_callback
            .as_ref()
            .ok_or(CredentialError::Configuration)?;
        let registration = config
            .notion_oauth_redirect
            .as_ref()
            .ok_or(CredentialError::Configuration)?;
        let store = GrantStore::open(
            &settings.key_file,
            &settings.state_file,
            registration.client_id(),
            callback.allowed.clone(),
        )
        .map_err(|_| CredentialError::Unavailable)?;
        // A missing or expired grant is never considered authenticated.
        // The dedicated coordinator can refresh a valid sealed expiry.
        store
            .load_for_refresh()
            .map_err(|_| CredentialError::Unavailable)?;
        let http = NotionTokenClient::new(registration, callback.client_secret.expose_secret())
            .map_err(|_| CredentialError::Configuration)?;
        let store = Arc::new(store);
        Ok(Some(Self {
            coordinator: Arc::new(NotionRefresh::shared(
                store.clone(),
                Arc::new(http),
                Arc::new(callback.allowed.clone()),
            )),
            store,
        }))
    }

    /// Acquire a fresh verified credential for EVERY server-side Notion
    /// operation. The passed backend client is never cached in the runtime.
    /// No request is made after a failed refresh or a mismatched identity.
    pub async fn with_client<T, F, Fut>(&self, operation: F) -> Result<T, CredentialError>
    where
        F: FnOnce(NotionClient) -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        let grant = self.coordinator.ensure_fresh().await.map_err(map_refresh)?;
        if !self
            .store
            .still_current(&grant)
            .map_err(|_| CredentialError::Unavailable)?
        {
            return Err(CredentialError::IdentityMismatch);
        }
        let client = NotionClient::integration(grant.access_token())
            .map_err(|_| CredentialError::Unavailable)?;
        operation(client)
            .await
            .map_err(|_| CredentialError::Unavailable)
    }

    /// Concrete read-only Notion request: verify the live bot ID equals the
    /// sealed callback's immutable integration identity. Never return tokens.
    pub async fn verify_bot_identity(&self) -> Result<(), CredentialError> {
        let grant = self.coordinator.ensure_fresh().await.map_err(map_refresh)?;
        if !self
            .store
            .still_current(&grant)
            .map_err(|_| CredentialError::Unavailable)?
        {
            return Err(CredentialError::IdentityMismatch);
        }
        let client = NotionClient::integration(grant.access_token())
            .map_err(|_| CredentialError::Unavailable)?;
        let remote = client
            .identity()
            .await
            .map_err(|_| CredentialError::Unavailable)?;
        if remote.id != grant.bot_id() {
            return Err(CredentialError::IdentityMismatch);
        }
        Ok(())
    }
}
