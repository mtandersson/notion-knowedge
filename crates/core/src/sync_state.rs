//! Provider-independent durable operational state used by sync/index orchestration.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageSyncState {
    pub page_id: String,
    pub content_hash: Option<String>,
    pub notion_last_edited_ms: Option<i64>,
    pub synced_at_ms: i64,
    pub tombstoned_at_ms: Option<i64>,
}

pub trait SyncStateStore {
    type Error;

    fn page(&self, page_id: &str) -> Result<Option<PageSyncState>, Self::Error>;
    fn upsert_page(&mut self, state: &PageSyncState) -> Result<(), Self::Error>;

    fn checkpoint(&self, key: &str) -> Result<Option<String>, Self::Error>;
    fn put_checkpoint(
        &mut self,
        key: &str,
        value: &str,
        updated_at_ms: i64,
    ) -> Result<(), Self::Error>;

    fn record_webhook_event(
        &mut self,
        event_id: &str,
        received_at_ms: i64,
    ) -> Result<bool, Self::Error>;

    fn index_version(&self, name: &str) -> Result<Option<String>, Self::Error>;
    fn set_index_version(
        &mut self,
        name: &str,
        version: &str,
        updated_at_ms: i64,
    ) -> Result<(), Self::Error>;
}
