//! Authoritative page contracts. API JSON, credentials and transport errors stay
//! inside adapters; these types are not derived search/index records.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::indexed::PropertyValue;

/// Owned, Send future supports both real asynchronous I/O and object-safe mocks.
pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BackendError>> + Send + 'a>>;

/// Stable authoritative identity (not an indexed chunk ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageId(pub String);

/// Fresh authoritative metadata; no cache hash or embedding state is required.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub id: PageId,
    pub url: String,
    pub title: String,
    pub last_edited_time: String,
    pub archived: bool,
    pub properties: BTreeMap<String, PropertyValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PageContent {
    pub page: Page,
    pub markdown: String,
}

/// Explicit replacement of one page's content. Adapters must reject unsupported
/// destructive conversions rather than silently dropping content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacePageContent {
    pub page_id: PageId,
    pub markdown: String,
}

/// Append preserves existing content; it never implies replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendPageContent {
    pub page_id: PageId,
    pub markdown: String,
}

/// The caller supplies an explicit authorized parent; adapters enforce access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePage {
    pub parent_page_id: PageId,
    pub title: String,
    pub markdown: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Unauthenticated,
    PermissionDenied,
    NotFound,
    InvalidInput,
    UnsupportedContent,
    Conflict,
    RateLimited,
    Unavailable,
    Internal,
}

/// Sanitized adapter failure. Never include credentials, raw upstream bodies or
/// page content in this model. retry_after is advisory, not a retry instruction;
/// callers must not blindly retry non-idempotent writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    pub operation: &'static str,
    pub retry_after: Option<Duration>,
    /// Target from a validated successful mutation receipt when later metadata
    /// verification fails. Reconcile this page; never replay the mutation.
    /// None does not prove that a failed/ambiguous mutation made no change.
    pub committed_page_id: Option<PageId>,
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} failed: {:?}", self.operation, self.kind)
    }
}

impl std::error::Error for BackendError {}

/// Fresh reads, independent of the disposable retrieval cache.
pub trait NotionRead: Send + Sync {
    fn fetch_page<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, Page>;
    fn read_content<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, PageContent>;
}

/// Separate capability allows read-only consumers to omit write authority.
/// Implementations must enforce write controls before upstream mutations.
pub trait NotionWrite: Send + Sync {
    fn replace_content(&self, request: ReplacePageContent) -> BackendFuture<'_, Page>;
    fn append_content(&self, request: AppendPageContent) -> BackendFuture<'_, Page>;
    fn create_page(&self, request: CreatePage) -> BackendFuture<'_, Page>;
}

/// Full authoritative adapter, usable behind a trait object at composition time.
pub trait NotionBackend: NotionRead + NotionWrite {}

impl<T: NotionRead + NotionWrite + ?Sized> NotionBackend for T {}
