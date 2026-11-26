//! Authoritative page contracts. API JSON, credentials and transport errors stay
//! inside adapters; these types are not derived search/index records.

use std::borrow::Cow;
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
    /// Authoritative relation labels keyed by stable property ID.
    pub relation_names: BTreeMap<String, String>,
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

impl BackendErrorKind {
    /// Fixed lowercase label for operator logs.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::InvalidInput => "invalid_input",
            Self::UnsupportedContent => "unsupported_content",
            Self::Conflict => "conflict",
            Self::RateLimited => "rate_limited",
            Self::Unavailable => "unavailable",
            Self::Internal => "internal",
        }
    }
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

/// A provider answer that proves a write was refused before any effect: the
/// request never reached a state Notion could have committed. Timeouts,
/// 5xx, rate limits and transport failures are never rejections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRejection {
    /// The target or parent does not exist or is not shared with the integration.
    NotFound,
    PermissionDenied,
    /// Notion refused the integration's credentials (401). Like a 403, the
    /// request was never executed.
    Unauthenticated,
    /// Notion's request validation refused the payload.
    InvalidRequest,
    /// A create named a database as its parent page.
    ParentIsDatabase,
}

impl WriteRejection {
    /// Classify an effect failure. A failure that names a committed page (a
    /// verify read after a successful write) is never a rejection.
    pub fn of(error: &BackendError) -> Option<Self> {
        if error.committed_page_id.is_some() {
            return None;
        }
        match error.kind {
            BackendErrorKind::NotFound => Some(Self::NotFound),
            BackendErrorKind::PermissionDenied => Some(Self::PermissionDenied),
            BackendErrorKind::Unauthenticated => Some(Self::Unauthenticated),
            BackendErrorKind::InvalidInput => Some(Self::InvalidRequest),
            _ => None,
        }
    }

    /// Fixed lowercase label for logs and tool responses.
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::Unauthenticated => "unauthenticated",
            Self::InvalidRequest => "invalid_request",
            Self::ParentIsDatabase => "parent_is_database",
        }
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} failed: {:?}", self.operation, self.kind)
    }
}

impl std::error::Error for BackendError {}

/// Fresh reads, independent of the disposable retrieval cache.
pub trait NotionRead: Send + Sync {
    /// Local comparison view; only provider-generated transport credentials may
    /// be projected. Preserve meaningful content and resource identity. Never
    /// use this view for raw hash preconditions, payloads or fresh read results.
    /// The default compares every original byte.
    fn comparison_markdown<'a>(&self, markdown: &'a str) -> Cow<'a, str> {
        Cow::Borrowed(markdown)
    }

    fn fetch_page<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, Page>;
    fn read_content<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, PageContent>;

    /// Page identity and citation metadata for verifying a read, independent
    /// of database properties used by indexing or writes.
    fn fetch_source_page<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, Page> {
        self.fetch_page(page_id)
    }

    /// Complete Markdown and the same metadata returned by `fetch_source_page`.
    fn read_source_content<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, PageContent> {
        self.read_content(page_id)
    }

    /// Whether this id names a database the integration can read. Used only
    /// to explain a refused create; an adapter without the lookup says no.
    fn is_database<'a>(&'a self, _id: &'a PageId) -> BackendFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }
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

/// Exact, nonempty and unique source range for the provider's bounded Markdown
/// search-and-replace API. NEVER a whole-page replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSectionContent {
    pub page_id: PageId,
    pub old_str: String,
    pub new_str: String,
}

/// Kept separate from ordinary write ports so callers must opt into the
/// targeted mutation capability. Authorization and ledger are enforced by
/// the application workflow, not by possession of a PageId.
pub trait NotionSectionWrite: Send + Sync {
    fn update_section(&self, request: UpdateSectionContent) -> BackendFuture<'_, ()>;
}
