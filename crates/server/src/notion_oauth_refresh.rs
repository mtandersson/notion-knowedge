//! Serialized, fail-closed Notion OAuth refresh. This does not enable MCP
//! authentication or bypass grant revocation. A single coordinator must be
//! shared by all callers of a given GrantStore within ONE server process.
use std::{fmt, time::{SystemTime, UNIX_EPOCH}};
use tokio::sync::Mutex;
use crate::{
    notion_grant_store::{GrantStore, StoreError, StoredGrant},
    notion_oauth_callback::{NotionTokenClient, NotionOwnerPolicy},
};

/// Refresh proactively before the token expires to avoid requests crossing
/// the provider's expiry boundary. No expiry means "must refresh", not valid.
pub const REFRESH_SKEW_SECONDS: u64 = 300;

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
    store: &'a GrantStore,
    client: &'a NotionTokenClient,
    policy: &'a NotionOwnerPolicy,
    serial: Mutex<()>,
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
        Self { store, client, policy, serial: Mutex::new(()) }
    }

    /// Always reads the live sealed grant *after* taking the shared lock.
    /// A valid access token is returned as a private, server-side grant only.
    /// Failure yields no stale credentials, even when an old token might
    /// otherwise still work. Never silently change owner or workspace.
    pub async fn ensure_fresh(&self) -> Result<StoredGrant, RefreshError> {
        let _guard = self.serial.lock().await;
        let existing = self.store.load_for_refresh()
            .map_err(map_store)?.ok_or(RefreshError::NoGrant)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|_| RefreshError::StoreUnavailable)?.as_secs();
        if existing.expires_at_unix()
            .is_some_and(|expiry| expiry > now.saturating_add(REFRESH_SKEW_SECONDS))
        {
            return Ok(existing);
        }
        let refresh_token = existing.refresh_token()
            .ok_or(RefreshError::ReauthorizationRequired)?;
        let replacement = self.client.refresh(refresh_token).await
            .map_err(|_| RefreshError::ProviderUnavailable)?;
        if replacement.refresh_token().is_none() || replacement.expires_in().is_none() {
            return Err(RefreshError::ReauthorizationRequired);
        }
        if !self.policy.matches(replacement.workspace_id(), replacement.owner_user_id())
            || existing.workspace_id() != replacement.workspace_id()
            || existing.owner_user_id() != replacement.owner_user_id()
            || existing.bot_id() != replacement.bot_id()
        {
            return Err(RefreshError::InvalidIdentity);
        }
        // An upstream rotation has already taken place. Do not return
        // either token pair until the replacement is safely persisted.
        self.store.rotate_refresh(&existing, &replacement).map_err(map_store)
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
    use crate::{
        notion_oauth_callback::NotionGrant,
        notion_oauth_redirect::NotionOAuthConfig,
    };
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
    use tempfile::TempDir;
    use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

    const CALLBACK: &str = "https://auth.example.com/oauth/notion/callback";
    const ISSUER: &str = "https://auth.example.com";
    fn setup() -> (TempDir, GrantStore, NotionOwnerPolicy, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key,[42u8;32]).unwrap();
        fs::set_permissions(&key,fs::Permissions::from_mode(0o600)).unwrap();
        let state = dir.path().join("state");
        let owner = NotionOwnerPolicy::new("workspace-a","user-a").unwrap();
        let store = GrantStore::open(&key,&state,"client-a",owner.clone()).unwrap();
        (dir,store,owner,state)
    }
    fn success(workspace: &str, owner: &str, bot: &str, refresh: Option<&str>, expires: Option<u64>) -> String {
        let mut v = serde_json::json!({
            "token_type":"bearer",
            "access_token":"rotated-access-secret",
            "workspace_id":workspace,
            "owner":{"type":"user","user":{"id":owner}},
            "bot_id":bot,
        });
        if let Some(r) = refresh { v["refresh_token"]=r.into(); }
        if let Some(exp) = expires { v["expires_in"]=exp.into(); }
        v.to_string()
    }
    async fn stub(body: String, status: u16) -> (String, tokio::task::JoinHandle<String>) {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}/v1/oauth/token",listener.local_addr().unwrap());
        let task=tokio::spawn(async move {
            let (mut stream,_) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut part=[0u8;1024];
            loop {
                let n=stream.read(&mut part).await.unwrap();
                if n==0 { break; }
                bytes.extend_from_slice(&part[..n]);
                if let Some(end) = bytes.windows(4).position(|b|b==b"\r\n\r\n") {
                    let header=String::from_utf8_lossy(&bytes[..end]);
                    let len=header.lines().find_map(|line| {
                        line.to_ascii_lowercase().strip_prefix("content-length: ")
                            .and_then(|value|value.parse::<usize>().ok())
                    }).unwrap_or(0);
                    if bytes.len()>=end+4+len {break;}
                }
                assert!(bytes.len()<128*1024);
            }
            let msg=format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
            stream.write_all(msg.as_bytes()).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });
        (addr,task)
    }
    fn client(endpoint: &str) -> NotionTokenClient {
        let config=NotionOAuthConfig::new("client-a",CALLBACK,ISSUER).unwrap();
        NotionTokenClient::new(&config,"private-secret").unwrap().for_local_fixture(endpoint)
    }

    #[tokio::test]
    async fn rotation_is_atomic_and_concurrent_callers_make_one_provider_request() {
        let (_dir,store,policy,state) = setup();
        let before=store.save(&NotionGrant::fixture("workspace-a","user-a",Some(120))).unwrap();
        let encrypted_before=fs::read(&state).unwrap();
        let (url,task)=stub(success("workspace-a","user-a","bot-test",Some("rotated-refresh-secret"),Some(3600)),200).await;
        let http=client(&url);
        let refresh=NotionRefresh::new(&store,&http,&policy);
        let (a,b)=tokio::join!(refresh.ensure_fresh(),refresh.ensure_fresh());
        let (a,b)=(a.unwrap(),b.unwrap());
        assert_eq!(a.grant_id(),before.grant_id());
        assert_eq!(a.epoch(),before.epoch());
        assert_eq!(b.grant_id(),before.grant_id());
        assert!(a.expires_at_unix().unwrap()>before.expires_at_unix().unwrap());
        assert_eq!(store.load_for_refresh().unwrap().unwrap().refresh_token(),Some("rotated-refresh-secret"));
        assert_ne!(fs::read(&state).unwrap(),encrypted_before);
        assert!(!fs::read(&state).unwrap().windows("rotated-refresh-secret".len())
            .any(|b| b==b"rotated-refresh-secret"));
        let observed=task.await.unwrap();
        assert!(observed.starts_with("POST /v1/oauth/token HTTP/1.1"));
        assert!(observed.to_ascii_lowercase().contains("authorization: basic "));
        assert!(observed.contains("\"grant_type\":\"refresh_token\""));
        assert!(observed.contains("\"refresh_token\":\"test-refresh-secret\""));
        assert!(!observed.contains("\"code\":"));
        assert!(format!("{refresh:?}").contains("REDACTED"));
    }

    #[tokio::test]
    async fn healthy_unexpired_grant_skips_refresh_without_network() {
        let (_dir,store,policy,_) = setup();
        let before=store.save(&NotionGrant::fixture("workspace-a","user-a",Some(3600))).unwrap();
        let http=client("http://127.0.0.1:1/v1/oauth/token");
        let refresh=NotionRefresh::new(&store,&http,&policy);
        let result=refresh.ensure_fresh().await.unwrap();
        assert_eq!(result.epoch(),before.epoch());
        assert_eq!(result.grant_id(),before.grant_id());
    }

    #[tokio::test]
    async fn unknown_expiry_forces_refresh_instead_of_trusting_old_token() {
        let (_dir,store,policy,_) = setup();
        store.save(&NotionGrant::fixture("workspace-a","user-a",None)).unwrap();
        let (url,task)=stub(success("workspace-a","user-a","bot-test",Some("rotated-refresh-secret"),Some(3600)),200).await;
        let http=client(&url);
        let refresh=NotionRefresh::new(&store,&http,&policy);
        assert!(refresh.ensure_fresh().await.unwrap().expires_at_unix().is_some());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn refresh_denials_identity_changes_and_missing_rotation_fail_without_overwriting() {
        for (payload,status,error) in [
            (success("wrong-workspace","user-a","bot-test",Some("next"),Some(3600)),200,RefreshError::InvalidIdentity),
            (success("workspace-a","wrong-user","bot-test",Some("next"),Some(3600)),200,RefreshError::InvalidIdentity),
            (success("workspace-a","user-a","wrong-bot",Some("next"),Some(3600)),200,RefreshError::InvalidIdentity),
            (success("workspace-a","user-a","bot-test",None,Some(3600)),200,RefreshError::ReauthorizationRequired),
            (success("workspace-a","user-a","bot-test",Some("next"),None),200,RefreshError::ReauthorizationRequired),
            (r#"{"error":"invalid_grant","secret":"NEVER-PRINT-THIS"}"#.to_owned(),401,RefreshError::ProviderUnavailable),
            ("malformed".to_owned(),200,RefreshError::ProviderUnavailable),
        ] {
            let (_dir,store,policy,path)=setup();
            store.save(&NotionGrant::fixture("workspace-a","user-a",Some(120))).unwrap();
            let snapshot=fs::read(&path).unwrap();
            let (url,task)=stub(payload,status).await;
            let http=client(&url);
            let service=NotionRefresh::new(&store,&http,&policy);
            let failure=service.ensure_fresh().await.err().unwrap();
            assert_eq!(failure,error);
            assert_eq!(fs::read(&path).unwrap(),snapshot);
            assert!(!format!("{failure:?} {failure}").contains("NEVER-PRINT-THIS"));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn unavailable_provider_never_returns_stale_token() {
        let (_dir,store,policy,path)=setup();
        store.save(&NotionGrant::fixture("workspace-a","user-a",Some(120))).unwrap();
        let snapshot=fs::read(&path).unwrap();
        let http=client("http://127.0.0.1:1/v1/oauth/token");
        let refresh=NotionRefresh::new(&store,&http,&policy);
        assert!(matches!(refresh.ensure_fresh().await,Err(RefreshError::ProviderUnavailable)));
        assert_eq!(fs::read(&path).unwrap(),snapshot);
    }

    #[tokio::test]
    async fn missing_state_is_not_authorization() {
        let (_dir,store,policy,_) = setup();
        let http=client("http://127.0.0.1:1/v1/oauth/token");
        let refresh=NotionRefresh::new(&store,&http,&policy);
        assert!(matches!(refresh.ensure_fresh().await,Err(RefreshError::NoGrant)));
    }
}
