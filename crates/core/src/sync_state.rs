//! Durable operational state contracts for local synchronization.

/// Payload-free failures so storage errors cannot expose page content, event IDs,
/// cursors, or other local operational values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStateError {
    Unavailable,
    InvalidInput,
    CorruptState,
    UnsupportedSchema,
}

impl std::fmt::Display for SyncStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sync state failed: {self:?}")
    }
}

impl std::error::Error for SyncStateError {}

/// Whether the latest known page is present or has been removed from the
/// authoritative source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageSyncStatus {
    Present { content_hash: String },
    Tombstone,
}

/// Page-level crawl/index state. This contains only derived operational data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageSyncState {
    page_id: String,
    last_edited_time: Option<String>,
    status: PageSyncStatus,
}

impl PageSyncState {
    pub fn present(
        page_id: String,
        content_hash: String,
        last_edited_time: Option<String>,
    ) -> Result<Self, SyncStateError> {
        validate_identifier(&page_id)?;
        validate_identifier(&content_hash)?;
        validate_optional_value(last_edited_time.as_deref())?;
        Ok(Self {
            page_id,
            last_edited_time,
            status: PageSyncStatus::Present { content_hash },
        })
    }

    pub fn tombstone(
        page_id: String,
        last_edited_time: Option<String>,
    ) -> Result<Self, SyncStateError> {
        validate_identifier(&page_id)?;
        validate_optional_value(last_edited_time.as_deref())?;
        Ok(Self {
            page_id,
            last_edited_time,
            status: PageSyncStatus::Tombstone,
        })
    }

    pub fn page_id(&self) -> &str {
        &self.page_id
    }

    pub fn last_edited_time(&self) -> Option<&str> {
        self.last_edited_time.as_deref()
    }

    pub fn status(&self) -> &PageSyncStatus {
        &self.status
    }

    pub fn content_hash(&self) -> Option<&str> {
        match &self.status {
            PageSyncStatus::Present { content_hash } => Some(content_hash),
            PageSyncStatus::Tombstone => None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.status, PageSyncStatus::Tombstone)
    }
}

/// Opaque checkpoint for a named crawl scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlCheckpoint {
    key: String,
    cursor: Option<String>,
}

impl CrawlCheckpoint {
    pub fn new(key: String, cursor: Option<String>) -> Result<Self, SyncStateError> {
        validate_identifier(&key)?;
        validate_optional_value(cursor.as_deref())?;
        Ok(Self { key, cursor })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

/// Opaque version marker for a derived local index or schema generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexVersion {
    index_name: String,
    version: String,
}

impl IndexVersion {
    pub fn new(index_name: String, version: String) -> Result<Self, SyncStateError> {
        validate_identifier(&index_name)?;
        validate_identifier(&version)?;
        Ok(Self {
            index_name,
            version,
        })
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

/// Operational state is intentionally independent from Notion and retrieval
/// implementation types. Implementations must make each mutation atomic.
pub trait SyncStateStore: Send + Sync {
    fn page_state(&self, page_id: &str) -> Result<Option<PageSyncState>, SyncStateError>;
    fn put_page_state(&self, state: &PageSyncState) -> Result<(), SyncStateError>;

    fn checkpoint(&self, key: &str) -> Result<Option<CrawlCheckpoint>, SyncStateError>;
    fn put_checkpoint(&self, checkpoint: &CrawlCheckpoint) -> Result<(), SyncStateError>;

    /// Returns true only for the first successful insertion of an event ID.
    fn register_webhook_event(&self, event_id: &str) -> Result<bool, SyncStateError>;

    fn index_version(&self, index_name: &str) -> Result<Option<IndexVersion>, SyncStateError>;
    fn put_index_version(&self, version: &IndexVersion) -> Result<(), SyncStateError>;
}

pub fn validate_identifier(value: &str) -> Result<(), SyncStateError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(SyncStateError::InvalidInput);
    }
    Ok(())
}

fn validate_optional_value(value: Option<&str>) -> Result<(), SyncStateError> {
    if value.is_some_and(|value| value.chars().any(char::is_control)) {
        return Err(SyncStateError::InvalidInput);
    }
    Ok(())
}
