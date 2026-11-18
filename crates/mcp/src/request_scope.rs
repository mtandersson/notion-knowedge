//! Per-request authoritative ports supplied by trusted HTTP composition.
use crate::HttpAuthorization;
use notion_knowledge_core::{
    backend::{BackendFuture, NotionRead},
    lifecycle::{LifecycleScope, PageLifecycle},
};
use std::sync::Arc;

/// Reads are authorized from the page inventory; the lifecycle port remains
/// only for the write gate.
pub trait RequestBoundSource: NotionRead + PageLifecycle {
    /// Recheck the exact request binding before disclosing a completed result.
    fn validate(&self) -> BackendFuture<'_, ()>;
}
#[derive(Clone)]
pub struct RequestScopeAdapters {
    pub authorization: HttpAuthorization,
    pub scope: LifecycleScope,
    pub source: Arc<dyn RequestBoundSource>,
}
