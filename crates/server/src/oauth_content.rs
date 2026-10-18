//! A request owns its stable binding. Temporary clients never adopt a new
//! callback grant silently, and no integration token is consulted.
use crate::notion_oauth_runtime::{CredentialError, NotionOAuthRuntime};
use notion_knowledge_core::{
    backend::{
        AppendPageContent, BackendError, BackendErrorKind, BackendFuture, CreatePage, NotionRead,
        NotionSectionWrite, NotionWrite, Page, PageContent, PageId, ReplacePageContent,
        UpdateSectionContent,
    },
    lifecycle::{LifecycleEvidence, LifecycleScope, PageLifecycle},
};
use notion_knowledge_mcp::{HttpAuthorization, RequestBoundSource, RequestScopeAdapters};
use notion_knowledge_notion::{NotionClient, RequestAuthority};
use std::{future::Future, sync::Arc};

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
    async fn operation<T, F, Fut>(&self, operation: F) -> Result<T, BackendError>
    where
        F: FnOnce(NotionClient) -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        let authority = RequestGrantAuthority::acquire(&self.runtime, &self.authorization).await?;
        let result = operation(authority.client()?).await;
        if let Err(mut error) = authority.validate(false) {
            // Preserve a known target if post-write metadata verification failed.
            if let Err(previous) = &result {
                error.committed_page_id = previous.committed_page_id.clone();
                error.operation = previous.operation;
            }
            return Err(error);
        }
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
    fn mutation_ports(&self) -> Option<&dyn notion_knowledge_mcp::RequestBoundMutations> {
        self.authorization.permissions().can_write().then_some(self)
    }

    fn validate(&self) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            RequestGrantAuthority::acquire(&self.runtime, &self.authorization)
                .await
                .map(|_| ())
        })
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

/// Reusable server-only ephemeral authority for future request-bound file ports.
/// Captures the exact credential snapshot; never silently adopts a replacement
/// grant or a credential rotated while a provider admission wait was pending.
pub(crate) struct RequestGrantAuthority {
    runtime: NotionOAuthRuntime,
    authorization: HttpAuthorization,
    grant: crate::notion_grant_store::StoredGrant,
}
impl RequestGrantAuthority {
    pub(crate) async fn acquire(
        runtime: &NotionOAuthRuntime,
        authorization: &HttpAuthorization,
    ) -> Result<Arc<Self>, BackendError> {
        if !authorization.is_active() {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        let grant = runtime.ensure_active().await.map_err(credential)?;
        if grant.grant_id() != authorization.grant_id()
            || grant.epoch() != authorization.epoch()
            || grant.workspace_id() != authorization.workspace_id()
            || grant.owner_user_id() != authorization.owner_user_id()
        {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        let authority = Arc::new(Self {
            runtime: runtime.clone(),
            authorization: authorization.clone(),
            grant,
        });
        authority.validate(false)?;
        Ok(authority)
    }
    pub(crate) fn client(self: &Arc<Self>) -> Result<NotionClient, BackendError> {
        self.validate(false)?;
        Ok(self
            .runtime
            .read_client(self.grant.access_token())?
            .with_request_authority(self.clone()))
    }
}
impl RequestAuthority for RequestGrantAuthority {
    fn validate(&self, mutation: bool) -> Result<(), BackendError> {
        if mutation && !self.authorization.permissions().can_write() {
            return Err(failure(BackendErrorKind::PermissionDenied));
        }
        if !self.authorization.is_active()
            || !self
                .runtime
                .grant_store()
                .still_current(&self.grant)
                .map_err(|_| failure(BackendErrorKind::Unavailable))?
        {
            return Err(failure(BackendErrorKind::Unauthenticated));
        }
        Ok(())
    }
}
impl NotionWrite for BoundSource {
    fn replace_content(&self, _request: ReplacePageContent) -> BackendFuture<'_, Page> {
        // Whole-page replacement is not an implemented semantic capability.
        Box::pin(async { Err(failure(BackendErrorKind::PermissionDenied)) })
    }
    fn append_content(&self, request: AppendPageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            if !self.authorization.permissions().can_write() {
                return Err(failure(BackendErrorKind::PermissionDenied));
            }
            self.operation(move |client| async move {
                NotionWrite::append_content(&client, request).await
            })
            .await
        })
    }
    fn create_page(&self, request: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            if !self.authorization.permissions().can_write() {
                return Err(failure(BackendErrorKind::PermissionDenied));
            }
            self.operation(
                move |client| async move { NotionWrite::create_page(&client, request).await },
            )
            .await
        })
    }
}
impl NotionSectionWrite for BoundSource {
    fn update_section(&self, request: UpdateSectionContent) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            if !self.authorization.permissions().can_write() {
                return Err(failure(BackendErrorKind::PermissionDenied));
            }
            self.operation(move |client| async move { client.update_section(request).await })
                .await
        })
    }
}
