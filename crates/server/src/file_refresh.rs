//! Authorized source ownership for future installed file effects. No upload or
//! HTTP installation occurs here. Expired ownership requires explicit trusted
//! quiescence reconciliation, never automatic release based on a clock alone.
use crate::{notion_oauth_runtime::NotionOAuthRuntime, oauth_content::RequestGrantAuthority};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    lifecycle::LifecycleScope,
    root_scope::RootScopeGate,
    write_refresh::{GrantRefreshAuthority, RefreshKey},
};
use notion_knowledge_mcp::HttpAuthorization;
use notion_knowledge_notion::{NotionClient, RequestAuthority};
use notion_knowledge_retrieval::{
    file_refresh_intents::FileRefreshIntent, planner::production_scope,
    sync_state::SqliteSyncStateStore,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio_util::sync::CancellationToken;
#[derive(Default)]
struct Local {
    attempts: usize,
    settling: bool,
    settled: Option<bool>,
    mutation_attempted: bool,
}
struct Attempt(Arc<Mutex<Local>>);
impl Drop for Attempt {
    fn drop(&mut self) {
        let mut local = self.0.lock().unwrap_or_else(|e| e.into_inner());
        local.attempts -= 1;
    }
}
type Registry = BTreeMap<String, Weak<Mutex<Local>>>;
fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}
fn identity(key: &RefreshKey) -> String {
    format!("{}:{}", key.workspace_id, key.mutation_digest)
}
fn denied() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unauthenticated,
        operation: "notion.oauth.file_intent",
        retry_after: None,
        committed_page_id: None,
    }
}
fn now() -> i64 {
    crate::webhook_worker::runtime::now_ms() / 1000
}
/// Bound to the original live request, one accepted page and an opaque owner.
/// Dropping this never asserts that an upstream effect rolled back or settled.
pub struct AuthorizedFileIntent {
    state: Arc<SqliteSyncStateStore>,
    intent: FileRefreshIntent,
    request: Arc<RequestGrantAuthority>,
    client: NotionClient,
    cancel: CancellationToken,
    local: Arc<Mutex<Local>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}
impl AuthorizedFileIntent {
    pub async fn begin(
        state: Arc<SqliteSyncStateStore>,
        runtime: &NotionOAuthRuntime,
        auth: &HttpAuthorization,
        scope: &LifecycleScope,
        page: &PageId,
        cancel: CancellationToken,
        lease_seconds: i64,
    ) -> Result<Arc<Self>, BackendError> {
        Self::begin_clocked(
            state,
            runtime,
            auth,
            scope,
            page,
            cancel,
            lease_seconds,
            Arc::new(now),
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin_clocked(
        state: Arc<SqliteSyncStateStore>,
        runtime: &NotionOAuthRuntime,
        auth: &HttpAuthorization,
        scope: &LifecycleScope,
        page: &PageId,
        cancel: CancellationToken,
        lease_seconds: i64,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Arc<Self>, BackendError> {
        if !auth.permissions().can_write()
            || auth.workspace_id() != scope.workspace_id
            || cancel.is_cancelled()
        {
            return Err(denied());
        }
        let request = RequestGrantAuthority::acquire(runtime, auth).await?;
        let client = request.client()?;
        let gate =
            RootScopeGate::new(Arc::new(client.clone()), scope.clone()).map_err(|_| denied())?;
        let permit = gate.authorize(page).await.map_err(|_| denied())?;
        gate.revalidate(&permit, scope)
            .await
            .map_err(|_| denied())?;
        let policy = production_scope(scope).map_err(|_| denied())?;
        let authority = GrantRefreshAuthority::new(
            scope.workspace_id.clone(),
            auth.grant_id().into(),
            auth.epoch(),
            auth.owner_user_id().into(),
            policy.identity().into(),
        )
        .map_err(|_| denied())?;
        let nonce = notion_knowledge_core::logging::CorrelationId::new();
        let key = RefreshKey {
            workspace_id: scope.workspace_id.clone(),
            page_id: page.0.clone(),
            mutation_digest: format!("{:x}", Sha256::digest(nonce.as_str().as_bytes())),
        };
        request.validate(true)?;
        if cancel.is_cancelled() {
            return Err(denied());
        }
        let intent = state
            .begin_file_refresh_intent(&key, &authority, clock(), lease_seconds)
            .map_err(|_| denied())?;
        let local = Arc::new(Mutex::new(Local::default()));
        {
            let mut registry = registry().lock().map_err(|_| denied())?;
            registry.retain(|_, weak| weak.strong_count() > 0);
            registry.insert(identity(&key), Arc::downgrade(&local));
        }
        let owned = Arc::new(Self {
            state,
            intent,
            request,
            client,
            cancel,
            local,
            clock,
        });
        owned.validate(true)?;
        Ok(owned)
    }
    pub fn key(&self) -> &RefreshKey {
        self.intent.key()
    }
    pub fn authority(&self) -> &GrantRefreshAuthority {
        self.intent.authority()
    }
    pub fn client(self: &Arc<Self>) -> Result<NotionClient, BackendError> {
        self.validate(true)?;
        Ok(self.client.clone().with_request_authority(self.clone()))
    }
    pub fn renew(&self, seconds: i64) -> Result<(), BackendError> {
        self.validate(true)?;
        self.state
            .renew_file_source_owner(&self.intent, (self.clock)(), seconds)
            .map_err(|_| denied())
    }
    /// Call only after source work has returned. An accepted/uncertain effect is
    /// released for read-only refresh; a proven no-effect outcome is discarded.
    pub fn settle(&self, possible_effect: bool) -> Result<(), BackendError> {
        let mut local = self.local.lock().map_err(|_| denied())?;
        if local.attempts != 0 || local.settling || (!possible_effect && local.mutation_attempted) {
            return Err(denied());
        }
        if let Some(prior) = local.settled {
            self.request.validate(true)?;
            return if prior == possible_effect {
                Ok(())
            } else {
                Err(denied())
            };
        }
        self.validate_unlocked()?;
        local.settling = true;
        let result = self
            .state
            .settle_file_refresh_intent(&self.intent, (self.clock)(), possible_effect)
            .map_err(|_| denied());
        local.settling = false;
        if result.is_ok() {
            local.settled = Some(possible_effect);
        }
        result
    }
    fn validate_unlocked(&self) -> Result<(), BackendError> {
        self.request.validate(true)?;
        if self.cancel.is_cancelled()
            || !self
                .state
                .file_source_owner_is_current(&self.intent, (self.clock)())
                .map_err(|_| denied())?
        {
            return Err(denied());
        }
        Ok(())
    }
}
impl RequestAuthority for AuthorizedFileIntent {
    fn validate(&self, _: bool) -> Result<(), BackendError> {
        let local = self.local.lock().map_err(|_| denied())?;
        if local.settling {
            return Err(denied());
        }
        self.validate_unlocked()
    }
    fn start_attempt(&self, mutation: bool) -> Result<Option<Arc<dyn Send + Sync>>, BackendError> {
        let mut local = self.local.lock().map_err(|_| denied())?;
        if local.settling {
            return Err(denied());
        }
        self.validate_unlocked()?;
        local.attempts = local.attempts.checked_add(1).ok_or_else(denied)?;
        local.mutation_attempted |= mutation;
        Ok(Some(Arc::new(Attempt(self.local.clone()))))
    }
}
/// Trusted operator assertion, never supplied by a tool argument. The caller
/// must stop other source owners/processes and establish provider quiescence.
pub enum SourceQuiescence {
    OperatorConfirmed,
}
/// Reconcile only an already accepted expired intent under the same live grant
/// and current exact physical root. This creates no new source authority.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_quiescent(
    state: &SqliteSyncStateStore,
    runtime: &NotionOAuthRuntime,
    auth: &HttpAuthorization,
    scope: &LifecycleScope,
    key: &RefreshKey,
    _proof: SourceQuiescence,
) -> Result<(), BackendError> {
    reconcile_clocked(state, runtime, auth, scope, key, now()).await
}
pub(crate) async fn reconcile_clocked(
    state: &SqliteSyncStateStore,
    runtime: &NotionOAuthRuntime,
    auth: &HttpAuthorization,
    scope: &LifecycleScope,
    key: &RefreshKey,
    at: i64,
) -> Result<(), BackendError> {
    let id = identity(key);
    let local = registry()
        .lock()
        .map_err(|_| denied())?
        .get(&id)
        .and_then(Weak::upgrade);
    if let Some(local) = local {
        let local = local.lock().map_err(|_| denied())?;
        if local.attempts != 0 || local.settling {
            return Err(denied());
        }
    }
    if !auth.permissions().can_write()
        || auth.workspace_id() != scope.workspace_id
        || key.workspace_id != scope.workspace_id
    {
        return Err(denied());
    }
    let request = RequestGrantAuthority::acquire(runtime, auth).await?;
    let policy = production_scope(scope).map_err(|_| denied())?;
    let authority = GrantRefreshAuthority::new(
        scope.workspace_id.clone(),
        auth.grant_id().into(),
        auth.epoch(),
        auth.owner_user_id().into(),
        policy.identity().into(),
    )
    .map_err(|_| denied())?;
    let gate =
        RootScopeGate::new(Arc::new(request.client()?), scope.clone()).map_err(|_| denied())?;
    let permit = gate
        .authorize(&PageId(key.page_id.clone()))
        .await
        .map_err(|_| denied())?;
    gate.revalidate(&permit, scope)
        .await
        .map_err(|_| denied())?;
    request.validate(true)?;
    state
        .reconcile_quiescent_file_intent(key, &authority, at)
        .map_err(|_| denied())
}
