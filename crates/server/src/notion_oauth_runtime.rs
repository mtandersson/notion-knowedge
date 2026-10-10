//! Server-owned, single-process Notion OAuth request composition (#299).
//! This is never an MCP login/authorization layer.
use std::{fmt, future::Future, sync::Arc};

use notion_knowledge_core::backend::BackendError;
use notion_knowledge_notion::NotionClient;

use crate::{
    config::Config,
    notion_grant_store::GrantStore,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        notion_oauth_callback::{NotionGrant, NotionOwnerPolicy},
        notion_oauth_redirect::NotionOAuthConfig,
    };
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::atomic::{AtomicBool, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn setup() -> (
        tempfile::TempDir,
        PathBuf,
        PathBuf,
        Arc<GrantStore>,
        Arc<NotionOwnerPolicy>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let state = dir.path().join("state");
        let policy = Arc::new(NotionOwnerPolicy::new("workspace-a", "user-a").unwrap());
        let store =
            Arc::new(GrantStore::open(&key, &state, "client-a", policy.as_ref().clone()).unwrap());
        (dir, key, state, store, policy)
    }

    fn build_runtime(
        store: Arc<GrantStore>,
        policy: Arc<NotionOwnerPolicy>,
        endpoint: &str,
    ) -> NotionOAuthRuntime {
        let registration = NotionOAuthConfig::new(
            "client-a",
            "https://auth.example.com/oauth/notion/callback",
            "https://auth.example.com",
        )
        .unwrap();
        let http = NotionTokenClient::new(&registration, "server-secret")
            .unwrap()
            .for_local_fixture(endpoint);
        NotionOAuthRuntime {
            coordinator: Arc::new(NotionRefresh::shared(store.clone(), Arc::new(http), policy)),
            store,
        }
    }

    async fn provider() -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut data = [0u8; 2048];
                let n = stream.read(&mut data).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&data[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..end]);
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let body = serde_json::json!({
                "token_type":"bearer",
                "access_token":"rotated-access",
                "refresh_token":"rotated-refresh",
                "workspace_id":"workspace-a",
                "owner":{"type":"user","user":{"id":"user-a"}},
                "bot_id":"bot-test",
                "expires_in":3600
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });
        (url, server)
    }

    #[tokio::test]
    async fn healthy_grant_used_per_operation_without_provider_fallback() {
        let (_dir, _key, _state, store, policy) = setup();
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let credentials = build_runtime(store.clone(), policy, "http://127.0.0.1:1/v1/oauth/token");
        let called = AtomicBool::new(false);
        let called_ref = &called;
        credentials
            .with_client(|client| async move {
                assert!(format!("{client:?}").contains("REDACTED"));
                called_ref.store(true, Ordering::SeqCst);
                Ok::<_, BackendError>(())
            })
            .await
            .unwrap();
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(store.load_for_refresh().unwrap().unwrap().epoch(), 1);
    }

    #[tokio::test]
    async fn missing_expired_unknown_and_wrong_owner_never_run_requests() {
        let (_dir, _key, _state, store, policy) = setup();
        let runtime = build_runtime(
            store.clone(),
            policy.clone(),
            "http://127.0.0.1:1/v1/oauth/token",
        );
        let invoked = AtomicBool::new(false);
        assert_eq!(
            runtime
                .with_client(|_| async {
                    invoked.store(true, Ordering::SeqCst);
                    Ok::<_, BackendError>(())
                })
                .await,
            Err(CredentialError::NoGrant)
        );
        assert!(!invoked.load(Ordering::SeqCst));
        for expiry in [Some(1), None] {
            store
                .save(&NotionGrant::fixture("workspace-a", "user-a", expiry))
                .unwrap();
            assert_eq!(
                runtime
                    .with_client(|_| async {
                        invoked.store(true, Ordering::SeqCst);
                        Ok::<_, BackendError>(())
                    })
                    .await,
                Err(CredentialError::Unavailable)
            );
            assert!(!invoked.load(Ordering::SeqCst));
        }
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let foreign = Arc::new(NotionOwnerPolicy::new("wrong-workspace", "user-a").unwrap());
        let runtime = build_runtime(store, foreign, "http://127.0.0.1:1/v1/oauth/token");
        assert_eq!(
            runtime
                .with_client(|_| async {
                    invoked.store(true, Ordering::SeqCst);
                    Ok::<_, BackendError>(())
                })
                .await,
            Err(CredentialError::IdentityMismatch)
        );
        assert!(!invoked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn one_shared_runtime_coalesces_refresh_and_restarts_from_rotated_grant() {
        let (_dir, key, state, store, policy) = setup();
        let old = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
            .unwrap();
        let (endpoint, provider) = provider().await;
        let runtime = build_runtime(store.clone(), policy.clone(), &endpoint);
        let a = runtime.clone();
        let b = runtime.clone();
        let (x, y) = tokio::join!(
            a.with_client(|_| async { Ok::<_, BackendError>(()) }),
            b.with_client(|_| async { Ok::<_, BackendError>(()) }),
        );
        x.unwrap();
        y.unwrap();
        let seen = provider.await.unwrap();
        assert!(seen.starts_with("POST /v1/oauth/token HTTP/1.1"));
        assert!(!seen.contains("rotated"));
        let fresh = store.load_for_refresh().unwrap().unwrap();
        assert_eq!(fresh.epoch(), old.epoch());
        assert_eq!(fresh.grant_id(), old.grant_id());
        assert_ne!(fresh.issued_at_unix(), 0);
        assert_eq!(fresh.refresh_token(), Some("rotated-refresh"));
        let ciphertext = fs::read(&state).unwrap();
        assert!(!ciphertext.windows(15).any(|b| b == b"rotated-refresh"));
        let reopened =
            Arc::new(GrantStore::open(&key, &state, "client-a", policy.as_ref().clone()).unwrap());
        build_runtime(reopened, policy, "http://127.0.0.1:1/v1/oauth/token")
            .with_client(|_| async { Ok::<_, BackendError>(()) })
            .await
            .unwrap();
    }

    #[test]
    fn replaced_grant_epoch_and_token_snapshot_are_rejected() {
        let (_dir, _key, _state, store, _policy) = setup();
        let old = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        assert!(store.still_current(&old).unwrap());
        let next = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        assert_ne!(old.epoch(), next.epoch());
        assert!(!store.still_current(&old).unwrap());
        assert!(store.still_current(&next).unwrap());
    }

    #[test]
    fn errors_and_runtime_debug_do_not_contain_credentials() {
        let (_dir, _key, _state, store, policy) = setup();
        let runtime = build_runtime(store, policy, "http://127.0.0.1:1/v1/oauth/token");
        assert_eq!(format!("{runtime:?}"), "NotionOAuthRuntime([REDACTED])");
        let error = CredentialError::Unavailable;
        assert!(!format!("{error:?} {error}").contains("server-secret"));
    }
}
