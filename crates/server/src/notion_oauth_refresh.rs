//! Serialized, fail-closed Notion OAuth refresh. This does not enable MCP
//! authentication or bypass grant revocation. A single coordinator must be
//! shared by all callers of a given GrantStore within ONE server process.
use crate::{
    notion_grant_store::{GrantStore, StoreError, StoredGrant},
    notion_oauth_callback::{NotionOwnerPolicy, NotionTokenClient},
};
use std::{
    fmt,
    ops::Deref,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// Borrowed for existing local callers, owned for a long-lived process
/// coordinator. Owned parts have 'static lifetime and can be shared safely.
enum Part<'a, T> {
    Borrowed(&'a T),
    Owned(Arc<T>),
}
impl<T> Deref for Part<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value.as_ref(),
        }
    }
}

/// Refresh proactively before the token expires to avoid requests crossing
/// the provider's expiry boundary. Unknown expiry requires active introspection.
pub const REFRESH_SKEW_SECONDS: u64 = 300;
/// Server-owned rotation cadence, never an assertion of provider expiry.
pub const PROACTIVE_ROTATION_SECONDS: u64 = 24 * 60 * 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshError {
    NoGrant,
    ReauthorizationRequired,
    ProviderUnavailable,
    InvalidIdentity,
    StoreUnavailable,
}
impl fmt::Display for RefreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Notion authorization needs attention; no credentials were issued")
    }
}
impl std::error::Error for RefreshError {}

/// Never store/emit provider HTTP errors, secret tokens or raw JSON.
/// Mutex acquisition spans upstream exchange and encrypted atomic write:
/// concurrent requests coalesce rather than submitting the same one-time
/// refresh token twice. No second refresh happens when the first succeeded.
pub struct NotionRefresh<'a> {
    store: Part<'a, GrantStore>,
    client: Part<'a, NotionTokenClient>,
    policy: Part<'a, NotionOwnerPolicy>,
    serial: Mutex<()>,
    #[cfg(test)]
    test_clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
}
impl fmt::Debug for NotionRefresh<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotionRefresh([REDACTED])")
    }
}
impl<'a> NotionRefresh<'a> {
    pub fn new(
        store: &'a GrantStore,
        client: &'a NotionTokenClient,
        policy: &'a NotionOwnerPolicy,
    ) -> Self {
        Self {
            store: Part::Borrowed(store),
            client: Part::Borrowed(client),
            policy: Part::Borrowed(policy),
            serial: Mutex::new(()),
            #[cfg(test)]
            test_clock: None,
        }
    }

    /// Always reads the live sealed grant *after* taking the shared lock.
    /// A valid access token is returned as a private, server-side grant only.
    /// Failure yields no stale credentials, even when an old token might
    /// otherwise still work. Never silently change owner or workspace.
    pub async fn ensure_fresh(&self) -> Result<StoredGrant, RefreshError> {
        self.ensure_fresh_with_clock(|| self.now_unix()).await
    }

    /// Shared wall clock for credential validity at provider attempt boundaries.
    pub(crate) fn now_unix(&self) -> Result<u64, RefreshError> {
        #[cfg(test)]
        if let Some(clock) = &self.test_clock {
            return Ok(clock());
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|time| time.as_secs())
            .map_err(|_| RefreshError::StoreUnavailable)
    }

    #[cfg(test)]
    pub(crate) fn with_test_clock(mut self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.test_clock = Some(clock);
        self
    }

    #[cfg(test)]
    async fn ensure_fresh_at(&self, now: u64) -> Result<StoredGrant, RefreshError> {
        self.ensure_fresh_with_clock(|| Ok(now)).await
    }

    async fn ensure_fresh_with_clock(
        &self,
        clock: impl FnOnce() -> Result<u64, RefreshError>,
    ) -> Result<StoredGrant, RefreshError> {
        let _guard = self.serial.lock().await;
        let now = clock()?;
        let existing = self
            .store
            .load_for_refresh()
            .map_err(map_store)?
            .ok_or(RefreshError::NoGrant)?;
        // The coordinator's allowlist is an independent trust boundary from
        // the storage policy. Check it even when no network refresh is needed.
        if !self
            .policy
            .matches(existing.workspace_id(), existing.owner_user_id())
        {
            return Err(RefreshError::InvalidIdentity);
        }
        if existing.expires_at_unix().is_none() {
            self.verify_active(&existing).await?;
            if now.saturating_sub(existing.issued_at_unix()) < PROACTIVE_ROTATION_SECONDS {
                return Ok(existing);
            }
        }
        if existing
            .expires_at_unix()
            .is_some_and(|expiry| expiry > now.saturating_add(REFRESH_SKEW_SECONDS))
        {
            return Ok(existing);
        }
        let refresh_token = existing
            .refresh_token()
            .ok_or(RefreshError::ReauthorizationRequired)?;
        let replacement = self
            .client
            .refresh(refresh_token)
            .await
            .map_err(|_| RefreshError::ProviderUnavailable)?;
        if replacement.refresh_token().is_none() {
            return Err(RefreshError::ReauthorizationRequired);
        }
        if !self
            .policy
            .matches(replacement.workspace_id(), replacement.owner_user_id())
            || existing.workspace_id() != replacement.workspace_id()
            || existing.owner_user_id() != replacement.owner_user_id()
            || existing.bot_id() != replacement.bot_id()
        {
            return Err(RefreshError::InvalidIdentity);
        }
        // An upstream rotation has already taken place. Do not return
        // either token pair until the replacement is safely persisted.
        let rotated = self
            .store
            .rotate_refresh(&existing, &replacement)
            .map_err(map_store)?;
        if rotated.expires_at_unix().is_none() {
            self.verify_active(&rotated).await?;
        }
        Ok(rotated)
    }

    async fn verify_active(&self, grant: &StoredGrant) -> Result<(), RefreshError> {
        if !self
            .client
            .is_active(grant.access_token())
            .await
            .map_err(|_| RefreshError::ProviderUnavailable)?
        {
            return Err(RefreshError::ReauthorizationRequired);
        }
        if !self.store.still_current(grant).map_err(map_store)? {
            return Err(RefreshError::InvalidIdentity);
        }
        Ok(())
    }

    /// Fresh credentials plus a live provider active-state check, including
    /// known-expiry grants. Intended for per-request MCP authorization.
    pub async fn ensure_active(&self) -> Result<StoredGrant, RefreshError> {
        let grant = self.ensure_fresh().await?;
        self.verify_active(&grant).await?;
        Ok(grant)
    }
}
/// Construct exactly once in the HTTP server composition root and share the
/// returned Arc across every Notion request. No self-references or leaks.
impl NotionRefresh<'static> {
    pub fn shared(
        store: Arc<GrantStore>,
        client: Arc<NotionTokenClient>,
        policy: Arc<NotionOwnerPolicy>,
    ) -> Self {
        Self {
            store: Part::Owned(store),
            client: Part::Owned(client),
            policy: Part::Owned(policy),
            serial: Mutex::new(()),
            #[cfg(test)]
            test_clock: None,
        }
    }
}

fn map_store(error: StoreError) -> RefreshError {
    match error {
        StoreError::IdentityMismatch => RefreshError::InvalidIdentity,
        _ => RefreshError::StoreUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{notion_oauth_callback::NotionGrant, notion_oauth_redirect::NotionOAuthConfig};
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const CALLBACK: &str = "https://auth.example.com/oauth/notion/callback";
    const ISSUER: &str = "https://auth.example.com";
    fn setup() -> (TempDir, GrantStore, NotionOwnerPolicy, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let state = dir.path().join("state");
        let owner = NotionOwnerPolicy::new("workspace-a", "user-a").unwrap();
        let store = GrantStore::open(&key, &state, "client-a", owner.clone()).unwrap();
        (dir, store, owner, state)
    }
    fn success(
        workspace: &str,
        owner: &str,
        bot: &str,
        refresh: Option<&str>,
        expires: Option<u64>,
    ) -> String {
        let mut v = serde_json::json!({
            "token_type":"bearer",
            "access_token":"rotated-access-secret",
            "workspace_id":workspace,
            "owner":{"type":"user","user":{"id":owner}},
            "bot_id":bot,
        });
        if let Some(r) = refresh {
            v["refresh_token"] = r.into();
        }
        if let Some(exp) = expires {
            v["expires_in"] = exp.into();
        }
        v.to_string()
    }
    async fn stub(body: String, status: u16) -> (String, tokio::task::JoinHandle<String>) {
        scripted(vec![(body, status)], None).await
    }

    async fn scripted(
        responses: Vec<(String, u16)>,
        before_response: Option<Box<dyn FnOnce() + Send>>,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut observed = String::new();
            let mut hook = before_response;
            for (body, status) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut part = [0u8; 1024];
                loop {
                    let n = stream.read(&mut part).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&part[..n]);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|value| value.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + len {
                            break;
                        }
                    }
                    assert!(bytes.len() < 128 * 1024);
                }
                if let Some(callback) = hook.take() {
                    callback();
                }
                let msg = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(msg.as_bytes()).await.unwrap();
                observed.push_str(&String::from_utf8(bytes).unwrap());
            }
            observed
        });
        (addr, task)
    }
    fn client(endpoint: &str) -> NotionTokenClient {
        let config = NotionOAuthConfig::new("client-a", CALLBACK, ISSUER).unwrap();
        NotionTokenClient::new(&config, "private-secret")
            .unwrap()
            .for_local_fixture(endpoint)
    }

    #[tokio::test]
    async fn rotation_is_atomic_and_concurrent_callers_make_one_provider_request() {
        let (_dir, store, policy, state) = setup();
        let before = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
            .unwrap();
        let encrypted_before = fs::read(&state).unwrap();
        let (url, task) = stub(
            success(
                "workspace-a",
                "user-a",
                "bot-test",
                Some("rotated-refresh-secret"),
                Some(3600),
            ),
            200,
        )
        .await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        let (a, b) = tokio::join!(refresh.ensure_fresh(), refresh.ensure_fresh());
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.grant_id(), before.grant_id());
        assert_eq!(a.epoch(), before.epoch());
        assert_eq!(b.grant_id(), before.grant_id());
        assert!(a.expires_at_unix().unwrap() > before.expires_at_unix().unwrap());
        assert_eq!(
            store.load_for_refresh().unwrap().unwrap().refresh_token(),
            Some("rotated-refresh-secret")
        );
        assert_ne!(fs::read(&state).unwrap(), encrypted_before);
        assert!(
            !fs::read(&state)
                .unwrap()
                .windows("rotated-refresh-secret".len())
                .any(|b| b == b"rotated-refresh-secret")
        );
        let observed = task.await.unwrap();
        assert!(observed.starts_with("POST /v1/oauth/token HTTP/1.1"));
        assert!(
            observed
                .to_ascii_lowercase()
                .contains("authorization: basic ")
        );
        assert!(observed.contains("\"grant_type\":\"refresh_token\""));
        assert!(observed.contains("\"refresh_token\":\"test-refresh-secret\""));
        assert!(!observed.contains("\"code\":"));
        assert!(format!("{refresh:?}").contains("REDACTED"));
    }

    #[tokio::test]
    async fn queued_request_evaluates_expiry_when_it_acquires_the_lock() {
        use std::{
            future::Future,
            sync::atomic::{AtomicU64, Ordering},
            task::Poll,
        };
        let (_dir, store, policy, _) = setup();
        let before = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let (url, task) = stub(
            success(
                "workspace-a",
                "user-a",
                "bot-test",
                Some("rotated-refresh-secret"),
                Some(3600),
            ),
            200,
        )
        .await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        let guard = refresh.serial.lock().await;
        let clock = AtomicU64::new(before.issued_at_unix());
        let queued = refresh.ensure_fresh_with_clock(|| Ok(clock.load(Ordering::SeqCst)));
        tokio::pin!(queued);
        std::future::poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        clock.store(before.expires_at_unix().unwrap() + 1, Ordering::SeqCst);
        drop(guard);
        assert_eq!(
            queued.await.unwrap().access_token(),
            "rotated-access-secret"
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn healthy_unexpired_grant_skips_refresh_without_network() {
        let (_dir, store, policy, _) = setup();
        let before = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let http = client("http://127.0.0.1:1/v1/oauth/token");
        let refresh = NotionRefresh::new(&store, &http, &policy);
        let result = refresh.ensure_fresh().await.unwrap();
        assert_eq!(result.epoch(), before.epoch());
        assert_eq!(result.grant_id(), before.grant_id());
    }

    #[tokio::test]
    async fn mismatched_coordinator_policy_denies_even_healthy_cached_grant() {
        let (_dir, store, _policy, _) = setup();
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        // A different runtime policy must never bypass checks just because
        // a sealed token is not close to expiration.
        let incompatible = NotionOwnerPolicy::new("different-workspace", "user-a").unwrap();
        let http = client("http://127.0.0.1:1/v1/oauth/token");
        let refresh = NotionRefresh::new(&store, &http, &incompatible);
        assert!(matches!(
            refresh.ensure_fresh().await,
            Err(RefreshError::InvalidIdentity)
        ));
    }

    #[tokio::test]
    async fn unknown_expiry_requires_positive_live_active_state() {
        for (body, accepted) in [
            (r#"{"active":true,"scope":"","iat":123}"#, true),
            (r#"{"active":false}"#, false),
            (r#"{}"#, false),
            (r#"{"active":"true"}"#, false),
        ] {
            let (_dir, store, policy, path) = setup();
            let before = store
                .save(&NotionGrant::fixture("workspace-a", "user-a", None))
                .unwrap();
            let snapshot = fs::read(&path).unwrap();
            let (url, task) = stub(body.to_owned(), 200).await;
            let http = client(&url);
            let refresh = NotionRefresh::new(&store, &http, &policy);
            let result = refresh.ensure_fresh().await;
            assert_eq!(result.is_ok(), accepted);
            if let Ok(grant) = result {
                assert_eq!(grant.grant_id(), before.grant_id());
                assert_eq!(grant.expires_at_unix(), None);
            }
            assert_eq!(fs::read(&path).unwrap(), snapshot);
            let observed = task.await.unwrap();
            assert!(observed.starts_with("POST /v1/oauth/introspect HTTP/1.1"));
            assert!(observed.contains("\"token\":\"test-access-secret\""));
            assert!(!observed.contains("refresh_token"));
        }
    }

    #[tokio::test]
    async fn provider_documented_rotation_preserves_unknown_expiry_and_full_binding() {
        let (_dir, store, policy, _) = setup();
        let before = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
            .unwrap();
        let (url, task) = scripted(
            vec![
                (
                    success(
                        "workspace-a",
                        "user-a",
                        "bot-test",
                        Some("rotated-refresh-secret"),
                        None,
                    ),
                    200,
                ),
                (r#"{"active":true,"scope":"","iat":123}"#.into(), 200),
                (r#"{"active":true}"#.into(), 200),
            ],
            None,
        )
        .await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        let (a, b) = tokio::join!(refresh.ensure_fresh(), refresh.ensure_fresh());
        for grant in [a.unwrap(), b.unwrap()] {
            assert_eq!(grant.grant_id(), before.grant_id());
            assert_eq!(grant.epoch(), before.epoch());
            assert_eq!(grant.expires_at_unix(), None);
            assert_eq!(grant.access_token(), "rotated-access-secret");
            assert_eq!(grant.refresh_token(), Some("rotated-refresh-secret"));
        }
        let observed = task.await.unwrap();
        assert_eq!(observed.matches("POST /v1/oauth/token HTTP/1.1").count(), 1);
        assert_eq!(
            observed
                .matches("POST /v1/oauth/introspect HTTP/1.1")
                .count(),
            2
        );
        assert!(observed.contains("\"token\":\"rotated-access-secret\""));
    }

    #[tokio::test]
    async fn local_rotation_age_never_masks_inactive_unknown_expiry() {
        for active in [true, false] {
            let (_dir, store, policy, path) = setup();
            let before = store
                .save(&NotionGrant::fixture("workspace-a", "user-a", None))
                .unwrap();
            let snapshot = fs::read(&path).unwrap();
            let mut replies = vec![(serde_json::json!({"active":active}).to_string(), 200)];
            if active {
                replies.push((
                    success(
                        "workspace-a",
                        "user-a",
                        "bot-test",
                        Some("rotated-refresh-secret"),
                        None,
                    ),
                    200,
                ));
                replies.push((r#"{"active":true}"#.into(), 200));
            }
            let (url, task) = scripted(replies, None).await;
            let http = client(&url);
            let refresh = NotionRefresh::new(&store, &http, &policy);
            let result = refresh
                .ensure_fresh_at(before.issued_at_unix() + PROACTIVE_ROTATION_SECONDS)
                .await;
            assert_eq!(result.is_ok(), active);
            let observed = task.await.unwrap();
            assert!(observed.starts_with("POST /v1/oauth/introspect HTTP/1.1"));
            assert_eq!(observed.contains("POST /v1/oauth/token HTTP/1.1"), active);
            if !active {
                assert_eq!(fs::read(path).unwrap(), snapshot);
            }
        }
    }

    #[tokio::test]
    async fn active_verification_rejects_callback_replacement_during_provider_request() {
        let (_dir, store, policy, _) = setup();
        let store = Arc::new(store);
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let writer = store.clone();
        let (url, task) = scripted(
            vec![(r#"{"active":true}"#.into(), 200)],
            Some(Box::new(move || {
                writer
                    .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
                    .unwrap();
            })),
        )
        .await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        assert!(matches!(
            refresh.ensure_active().await,
            Err(RefreshError::InvalidIdentity)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn expired_rotation_rejects_authorization_replacement_during_exchange() {
        let (_dir, store, policy, _) = setup();
        let store = Arc::new(store);
        let before = store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
            .unwrap();
        let writer = store.clone();
        let (url, task) = scripted(
            vec![(
                success(
                    "workspace-a",
                    "user-a",
                    "bot-test",
                    Some("rotated-refresh-secret"),
                    None,
                ),
                200,
            )],
            Some(Box::new(move || {
                writer
                    .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
                    .unwrap();
            })),
        )
        .await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        assert!(matches!(
            refresh
                .ensure_fresh_at(before.expires_at_unix().unwrap() + 1)
                .await,
            Err(RefreshError::InvalidIdentity)
        ));
        let current = store.load_for_refresh().unwrap().unwrap();
        assert_ne!(current.grant_id(), before.grant_id());
        assert_eq!(current.access_token(), "test-access-secret");
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rotated_unknown_expiry_must_be_active_before_credentials_are_issued() {
        for (body, status) in [
            (r#"{"active":false}"#, 200),
            (r#"{}"#, 200),
            ("unavailable", 503),
        ] {
            let (_dir, store, policy, _) = setup();
            let before = store
                .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
                .unwrap();
            let (url, task) = scripted(
                vec![
                    (
                        success(
                            "workspace-a",
                            "user-a",
                            "bot-test",
                            Some("rotated-refresh-secret"),
                            None,
                        ),
                        200,
                    ),
                    (body.into(), status),
                ],
                None,
            )
            .await;
            let http = client(&url);
            let refresh = NotionRefresh::new(&store, &http, &policy);
            assert!(refresh.ensure_fresh().await.is_err());
            // Preserve the provider's rotated pair atomically even when active
            // verification fails; never issue it or reuse the consumed old pair.
            let current = store.load_for_refresh().unwrap().unwrap();
            assert_eq!(current.grant_id(), before.grant_id());
            assert_eq!(current.expires_at_unix(), None);
            assert_eq!(current.refresh_token(), Some("rotated-refresh-secret"));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn explicit_active_verification_denies_revoked_known_expiry_grant() {
        let (_dir, store, policy, _) = setup();
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(3600)))
            .unwrap();
        let (url, task) = stub(r#"{"active":false}"#.into(), 200).await;
        let http = client(&url);
        let refresh = NotionRefresh::new(&store, &http, &policy);
        assert!(matches!(
            refresh.ensure_active().await,
            Err(RefreshError::ReauthorizationRequired)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn refresh_denials_identity_changes_and_missing_rotation_fail_without_overwriting() {
        for (payload, status, error) in [
            (
                success(
                    "wrong-workspace",
                    "user-a",
                    "bot-test",
                    Some("next"),
                    Some(3600),
                ),
                200,
                RefreshError::InvalidIdentity,
            ),
            (
                success(
                    "workspace-a",
                    "wrong-user",
                    "bot-test",
                    Some("next"),
                    Some(3600),
                ),
                200,
                RefreshError::InvalidIdentity,
            ),
            (
                success(
                    "workspace-a",
                    "user-a",
                    "wrong-bot",
                    Some("next"),
                    Some(3600),
                ),
                200,
                RefreshError::InvalidIdentity,
            ),
            (
                success("workspace-a", "user-a", "bot-test", None, Some(3600)),
                200,
                RefreshError::ReauthorizationRequired,
            ),
            (
                r#"{"error":"invalid_grant","secret":"NEVER-PRINT-THIS"}"#.to_owned(),
                401,
                RefreshError::ProviderUnavailable,
            ),
            (
                "malformed".to_owned(),
                200,
                RefreshError::ProviderUnavailable,
            ),
        ] {
            let (_dir, store, policy, path) = setup();
            store
                .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
                .unwrap();
            let snapshot = fs::read(&path).unwrap();
            let (url, task) = stub(payload, status).await;
            let http = client(&url);
            let service = NotionRefresh::new(&store, &http, &policy);
            let failure = service.ensure_fresh().await.err().unwrap();
            assert_eq!(failure, error);
            assert_eq!(fs::read(&path).unwrap(), snapshot);
            assert!(!format!("{failure:?} {failure}").contains("NEVER-PRINT-THIS"));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn unavailable_provider_never_returns_stale_token() {
        let (_dir, store, policy, path) = setup();
        store
            .save(&NotionGrant::fixture("workspace-a", "user-a", Some(120)))
            .unwrap();
        let snapshot = fs::read(&path).unwrap();
        let http = client("http://127.0.0.1:1/v1/oauth/token");
        let refresh = NotionRefresh::new(&store, &http, &policy);
        assert!(matches!(
            refresh.ensure_fresh().await,
            Err(RefreshError::ProviderUnavailable)
        ));
        assert_eq!(fs::read(&path).unwrap(), snapshot);
    }

    #[tokio::test]
    async fn missing_state_is_not_authorization() {
        let (_dir, store, policy, _) = setup();
        let http = client("http://127.0.0.1:1/v1/oauth/token");
        let refresh = NotionRefresh::new(&store, &http, &policy);
        assert!(matches!(
            refresh.ensure_fresh().await,
            Err(RefreshError::NoGrant)
        ));
    }
}
