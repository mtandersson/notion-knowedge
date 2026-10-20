//! Grant-bound derived refresh, explicitly installed by OAuth write composition.
//! Admission requires live request write authority. Accepted jobs subsequently
//! use durable grant read authority, independently of the originating bearer.
#![cfg(all(unix, feature = "webhook-worker"))]
use crate::{
    notion_grant_store::StoredGrant, notion_oauth_runtime::NotionOAuthRuntime,
    oauth_content::RequestGrantAuthority,
};
use notion_knowledge_core::{
    aliases::AliasSources,
    backend::{BackendError, BackendErrorKind, PageId},
    chunking::ChunkConfig,
    embedding::EmbeddingProvider,
    lifecycle::LifecycleScope,
    reconciliation::{JournalError, ReconciliationJournal},
    write_refresh::{
        GrantRefreshAuthority, IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError,
    },
};
use notion_knowledge_mcp::HttpAuthorization;
use notion_knowledge_notion::{NotionClient, RequestAuthority};
use notion_knowledge_retrieval::{
    chunks::GuardedChunkTable,
    commit::IndexCommitCoordinator,
    mutation_refresh::{GrantMutationRefreshQueue, MutationRefreshClaim},
    planner::production_scope,
    refresh::AuthoritativePageRefresh,
    sync_state::SqliteSyncStateStore,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

fn denied() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unauthenticated,
        operation: "notion.oauth.refresh",
        retry_after: None,
        committed_page_id: None,
    }
}
fn matches(expected: &GrantRefreshAuthority, grant: &StoredGrant) -> bool {
    expected.grant_id() == grant.grant_id()
        && expected.epoch() == grant.epoch()
        && expected.workspace_id() == grant.workspace_id()
        && expected.owner_user_id() == grant.owner_user_id()
}
/// One shared admission contract for semantic writes and future native files.
/// No credential or bearer identifier is serialized into the queue.
pub struct AuthorizedRefreshQueue {
    request: Arc<RequestGrantAuthority>,
    queue: GrantMutationRefreshQueue,
}
impl AuthorizedRefreshQueue {
    pub async fn bind(
        state: Arc<SqliteSyncStateStore>,
        runtime: &NotionOAuthRuntime,
        auth: &HttpAuthorization,
        scope: &LifecycleScope,
    ) -> Result<Self, BackendError> {
        if !auth.permissions().can_write() || auth.workspace_id() != scope.workspace_id {
            return Err(denied());
        }
        let request = RequestGrantAuthority::acquire(runtime, auth).await?;
        let policy = production_scope(scope).map_err(|_| denied())?;
        let authority = GrantRefreshAuthority::new(
            auth.workspace_id().into(),
            auth.grant_id().into(),
            auth.epoch(),
            auth.owner_user_id().into(),
            policy.identity().into(),
        )
        .map_err(|_| denied())?;
        let queue = GrantMutationRefreshQueue::new(state, authority).map_err(|_| denied())?;
        request.validate(true)?;
        Ok(Self { request, queue })
    }
    pub fn authority(&self) -> &GrantRefreshAuthority {
        self.queue.authority()
    }
}
impl MutationRefreshQueue for AuthorizedRefreshQueue {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, RefreshQueueError> {
        self.request
            .validate(true)
            .map_err(|_| RefreshQueueError::Unavailable)?;
        self.queue.admit(key)
    }
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, RefreshQueueError> {
        self.request
            .validate(true)
            .map_err(|_| RefreshQueueError::Unavailable)?;
        self.queue.status(key)
    }
}
/// Server-only temporary read credential. It cannot perform provider mutations.
struct GrantReadAuthority {
    runtime: NotionOAuthRuntime,
    grant: StoredGrant,
    deadline: Instant,
    cancel: CancellationToken,
}
impl GrantReadAuthority {
    async fn acquire(
        runtime: &NotionOAuthRuntime,
        expected: &GrantRefreshAuthority,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, BackendError> {
        expected.validate().map_err(|_| denied())?;
        // Never refresh or introspect a replacement grant on behalf of old work.
        let current = runtime
            .grant_store()
            .load_for_refresh()
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        if !matches(expected, &current) || cancel.is_cancelled() || Instant::now() >= deadline {
            return Err(denied());
        }
        let grant = runtime
            .ensure_active_for(expected)
            .await
            .map_err(|_| denied())?;
        if !matches(expected, &grant) {
            return Err(denied());
        }
        let authority = Arc::new(Self {
            runtime: runtime.clone(),
            grant,
            deadline,
            cancel,
        });
        authority.validate(false)?;
        Ok(authority)
    }
    fn client(self: &Arc<Self>) -> Result<NotionClient, BackendError> {
        self.validate(false)?;
        Ok(self
            .runtime
            .read_client(self.grant.access_token())?
            .with_request_authority(self.clone()))
    }
}
impl RequestAuthority for GrantReadAuthority {
    fn validate(&self, mutation: bool) -> Result<(), BackendError> {
        if mutation
            || self.cancel.is_cancelled()
            || Instant::now() >= self.deadline
            || self
                .grant
                .expires_at_unix()
                .is_some_and(|expiry| self.runtime.now_unix().map_or(true, |now| expiry <= now))
            || !self
                .runtime
                .grant_store()
                .still_current(&self.grant)
                .map_err(|_| denied())?
        {
            return Err(denied());
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct GrantRefreshSettings {
    pub scope: LifecycleScope,
    pub chunk_config: ChunkConfig,
    pub alias_sources: Option<AliasSources>,
    pub timeout: Duration,
}
/// A bounded dispatcher over OAuth-only rows. It holds no mutation capability,
/// never manufactures a webhook identity; HTTP composition owns its lifecycle.
pub struct GrantRefreshWorker {
    runtime: NotionOAuthRuntime,
    settings: GrantRefreshSettings,
    guard: GuardedChunkTable,
    coordinator: Arc<IndexCommitCoordinator>,
    provider: Arc<dyn EmbeddingProvider>,
}
impl GrantRefreshWorker {
    pub fn new(
        runtime: NotionOAuthRuntime,
        settings: GrantRefreshSettings,
        guard: GuardedChunkTable,
        coordinator: Arc<IndexCommitCoordinator>,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<Self, &'static str> {
        let scope = production_scope(&settings.scope).map_err(|_| "invalid refresh scope")?;
        let binding = coordinator.binding();
        if !guard.is_bound_to(&coordinator)
            || binding.epoch() != settings.scope.generation
            || binding.workspace() != settings.scope.workspace_id
            || binding.scope() != scope.identity()
            || binding.generation() != settings.scope.generation.to_string()
            || settings.timeout.is_zero()
            || settings.timeout > Duration::from_secs(3570)
            || settings.chunk_config.target_chars == 0
            || settings.chunk_config.overlap_chars >= settings.chunk_config.target_chars
        {
            return Err("invalid refresh authority");
        }
        Ok(Self {
            runtime,
            settings,
            guard,
            coordinator,
            provider,
        })
    }
    pub async fn tick(&self, stop: CancellationToken) -> Result<bool, &'static str> {
        if stop.is_cancelled() {
            return Ok(false);
        }
        let state = self.coordinator.state().clone();
        let lease_seconds = self.settings.timeout.as_secs() as i64 + 30;
        let lease_state = state.clone();
        let lease = match blocking(move || lease_state.acquire_lease(now(), lease_seconds)).await? {
            Ok(lease) => lease,
            Err(JournalError::Busy) => return Ok(false),
            Err(_) => return Err("index lease unavailable"),
        };
        let result = self.dispatch(lease.clone(), lease_seconds, stop).await;
        match blocking(move || state.release_lease(&lease, now())).await? {
            Ok(()) | Err(JournalError::LeaseLost) => {}
            Err(_) => return Err("index lease release failed"),
        }
        result
    }
    async fn dispatch(
        &self,
        lease: notion_knowledge_core::reconciliation::Lease,
        lease_seconds: i64,
        stop: CancellationToken,
    ) -> Result<bool, &'static str> {
        let coordinator = self.coordinator.clone();
        let owned_lease = lease.clone();
        match blocking(move || coordinator.ensure_no_active_reconciliation(&owned_lease, now()))
            .await?
        {
            Ok(()) => {}
            Err(JournalError::Busy) => return Ok(false),
            Err(_) => return Err("index authority unavailable"),
        }
        if stop.is_cancelled() {
            return Ok(false);
        }
        let state = self.coordinator.state().clone();
        let claiming = state.clone();
        let workspace = self.settings.scope.workspace_id.clone();
        let admission_stop = stop.clone();
        let Some(claim) = blocking(move || {
            if admission_stop.is_cancelled() {
                return Ok(None);
            }
            claiming.claim_grant_mutation_refresh(&workspace, now(), lease_seconds)
        })
        .await?
        .map_err(|_| "refresh claim unavailable")?
        else {
            return Ok(false);
        };
        let processing = stop.child_token();
        let _cancel_on_drop = processing.clone().drop_guard();
        let deadline = Instant::now() + self.settings.timeout;
        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(true), // expired lease resumes, never repeats source writes
            _ = tokio::time::sleep(self.settings.timeout) => false,
            result = self.refresh(&claim, lease, deadline, processing.clone()) => result,
        };
        processing.cancel();
        match blocking(move || state.complete_mutation_refresh(&claim, now(), result)).await? {
            Ok(()) | Err(RefreshQueueError::ClaimLost) => Ok(true),
            Err(_) => Err("refresh completion unavailable"),
        }
    }
    async fn refresh(
        &self,
        claim: &MutationRefreshClaim,
        lease: notion_knowledge_core::reconciliation::Lease,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> bool {
        let Some(expected) = &claim.authority else {
            return false;
        };
        let Ok(policy) = production_scope(&self.settings.scope) else {
            return false;
        };
        if expected.workspace_id() != self.settings.scope.workspace_id
            || expected.policy_identity() != policy.identity()
        {
            return false;
        }
        let Ok(authority) =
            GrantReadAuthority::acquire(&self.runtime, expected, deadline, cancel.clone()).await
        else {
            return false;
        };
        let Ok(client) = authority.client() else {
            return false;
        };
        // Construct BOTH the primary source and relation adapter from this exact
        // temporary OAuth client. There is no integration fallback/prototype.
        let Ok(mut handler) = AuthoritativePageRefresh::new(
            Arc::new(client),
            self.guard.clone(),
            self.coordinator.clone(),
            self.settings.scope.clone(),
            &policy,
            self.settings.chunk_config,
        ) else {
            return false;
        };
        if let Some(aliases) = &self.settings.alias_sources {
            handler = handler.with_alias_sources(aliases.clone());
        }
        let state = self.coordinator.state().clone();
        let owned = claim.clone();
        let fence = authority.clone();
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
            let now = now();
            if fence.validate(false).is_err()
                || !state
                    .mutation_refresh_is_current(&owned, now)
                    .unwrap_or(false)
            {
                i64::MAX
            } else {
                now
            }
        });
        let operation = format!(
            "oauth-refresh-{}-{}",
            claim.key.workspace_id, claim.key.mutation_digest
        );
        let result = handler
            .apply_lifecycle(
                &PageId(claim.key.page_id.clone()),
                &operation,
                self.provider.as_ref(),
                lease,
                clock,
            )
            .await;
        result.is_ok() && authority.validate(false).is_ok()
    }
}
fn now() -> i64 {
    crate::webhook_worker::runtime::now_ms() / 1000
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, &'static str> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| "refresh storage unavailable")
}
