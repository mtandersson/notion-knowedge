//! A request owns its stable binding. Temporary clients never adopt a new
//! callback grant silently, and no integration token is consulted.
use crate::notion_oauth_runtime::{CredentialError, NotionOAuthRuntime};
use notion_knowledge_core::{
    backend::{
        BackendError, BackendErrorKind, BackendFuture, NotionRead, Page, PageContent, PageId,
    },
    lifecycle::{LifecycleEvidence, LifecycleScope, PageLifecycle},
};
use notion_knowledge_mcp::{HttpAuthorization, RequestBoundSource, RequestScopeAdapters};
use notion_knowledge_notion::NotionClient;
use std::{future::Future, sync::Arc, time::Instant};

fn failure(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.oauth.read",
        retry_after: None,
        committed_page_id: None,
    }
}
fn credential(error: CredentialError) -> BackendError {
    failure(match error {
        CredentialError::Unavailable | CredentialError::Configuration => {
            BackendErrorKind::Unavailable
        }
        _ => BackendErrorKind::Unauthenticated,
    })
}
#[derive(Clone)]
pub(crate) struct ContentFactory {
    pub runtime: NotionOAuthRuntime,
    pub scope: LifecycleScope,
}
impl ContentFactory {
    pub fn bind(
        &self,
        authorization: &HttpAuthorization,
    ) -> Result<RequestScopeAdapters, BackendError> {
        if self.scope.workspace_id != authorization.workspace_id() {
            return Err(failure(BackendErrorKind::PermissionDenied));
        }
        Ok(RequestScopeAdapters {
            authorization: authorization.clone(),
            scope: self.scope.clone(),
            source: Arc::new(BoundSource {
                runtime: self.runtime.clone(),
                authorization: authorization.clone(),
                scope: self.scope.clone(),
            }),
        })
    }
}
struct BoundSource {
    runtime: NotionOAuthRuntime,
    authorization: HttpAuthorization,
    scope: LifecycleScope,
}
impl BoundSource {
    async fn grant(&self) -> Result<crate::notion_grant_store::StoredGrant, BackendError> {
        if self.authorization.expires_at() <= Instant::now() {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        let grant = self.runtime.ensure_active().await.map_err(credential)?;
        if grant.grant_id() != self.authorization.grant_id()
            || grant.epoch() != self.authorization.epoch()
            || grant.workspace_id() != self.authorization.workspace_id()
            || grant.owner_user_id() != self.authorization.owner_user_id()
        {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        self.current(&grant)?;
        Ok(grant)
    }
    fn current(&self, grant: &crate::notion_grant_store::StoredGrant) -> Result<(), BackendError> {
        if self.authorization.expires_at() <= Instant::now()
            || !self
                .runtime
                .grant_store()
                .still_current(grant)
                .map_err(|_| failure(BackendErrorKind::Unavailable))?
        {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        Ok(())
    }
    async fn operation<T, F, Fut>(&self, operation: F) -> Result<T, BackendError>
    where
        F: FnOnce(NotionClient) -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        let grant = self.grant().await?;
        let client = self.runtime.read_client(grant.access_token())?;
        let result = operation(client).await;
        self.current(&grant)?;
        result
    }
    fn scope_matches(&self, scope: &LifecycleScope) -> Result<(), BackendError> {
        if scope != &self.scope {
            return Err(failure(BackendErrorKind::PermissionDenied));
        }
        Ok(())
    }
}
impl RequestBoundSource for BoundSource {
    fn validate(&self) -> BackendFuture<'_, ()> {
        Box::pin(async move { self.grant().await.map(|_| ()) })
    }
}
impl NotionRead for BoundSource {
    fn fetch_page<'a>(&'a self, page: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(
            self.operation(
                move |client| async move { NotionRead::fetch_page(&client, page).await },
            ),
        )
    }
    fn read_content<'a>(&'a self, page: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(
            self.operation(
                move |client| async move { NotionRead::read_content(&client, page).await },
            ),
        )
    }
}
impl PageLifecycle for BoundSource {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            self.scope_matches(scope)?;
            self.operation(move |client| async move { client.lifecycle(page, scope).await })
                .await
        })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.scope_matches(scope)?;
            self.operation(move |client| async move {
                client.revalidate_lifecycle(evidence, scope).await
            })
            .await
        })
    }
}
