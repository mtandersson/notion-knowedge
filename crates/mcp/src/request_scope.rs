//! Per-request authoritative ports supplied by trusted HTTP composition.
use crate::HttpAuthorization;
use notion_knowledge_core::{
    backend::{BackendFuture, NotionRead, NotionSectionWrite, NotionWrite},
    lifecycle::{LifecycleScope, PageLifecycle},
};
use std::sync::Arc;

pub trait RequestBoundMutations: NotionWrite + NotionSectionWrite {}
impl<T: NotionWrite + NotionSectionWrite> RequestBoundMutations for T {}

pub trait RequestBoundSource: NotionRead + PageLifecycle {
    /// Recheck the exact request binding before disclosing a completed result.
    fn validate(&self) -> BackendFuture<'_, ()>;
    /// Obtain an owned source only from trusted request binding, never tool JSON.
    fn mutation_source(self: Arc<Self>) -> Option<Arc<dyn crate::mutations::MutationSource>> {
        None
    }
    /// Optional request-bound ports; does not install mutation tools.
    fn mutation_ports(&self) -> Option<&dyn RequestBoundMutations> {
        None
    }
}
#[derive(Clone)]
pub struct RequestScopeAdapters {
    pub authorization: HttpAuthorization,
    pub scope: LifecycleScope,
    pub source: Arc<dyn RequestBoundSource>,
}
