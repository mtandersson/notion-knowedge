//! Payload-free status supplied by trusted local composition.
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStatus {
    pub last_successful_crawl_unix_seconds: Option<i64>,
    pub last_successful_reconciliation_unix_seconds: Option<i64>,
    pub webhooks: Option<WebhookCounts>,
    pub operational_schema_version: Option<i64>,
    pub index: Option<IndexStatus>,
    pub inventory: Option<InventoryStatus>,
    #[serde(default)]
    pub source_documents: Option<u64>,
    #[serde(default)]
    pub lexical_projection: Option<LexicalProjectionStatus>,
    #[serde(default)]
    pub semantic_projection: Option<SemanticProjectionStatus>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LexicalProjectionStatus {
    pub version: u64,
    pub chunks: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticProjectionStatus {
    pub vectors: u64,
    pub queued_pages: u64,
    pub model_unavailable_pages: u64,
    pub input_limit_pages: u64,
    pub rejected_pages: u64,
}
/// Page inventory counts, the last sync's drift and the out-of-sync pages.
/// Ids and typed reasons only: never titles or content.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct InventoryStatus {
    pub total: u64,
    pub indexed: u64,
    pub skipped: u64,
    pub failed: u64,
    pub pending: u64,
    /// `None` before the first sync. Only a complete walk removes pages.
    pub walk_complete: Option<bool>,
    pub held: u64,
    pub drift: Drift,
    pub out_of_sync: Vec<OutOfSyncPage>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Drift {
    pub added: u64,
    pub changed: u64,
    pub moved: u64,
    pub removed: u64,
    pub trashed: u64,
    pub unplaced: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutOfSyncPage {
    pub page_id: String,
    /// `skipped:<reason>` or `failed:<class>`.
    pub outcome: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebhookCounts {
    pub pending: u64,
    pub running: u64,
    pub failed: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexStatus {
    pub version: u64,
    pub chunk_schema_version: u32,
    pub canonical_schema_version: u32,
    pub model: ModelIdentity,
    pub model_version: String,
    pub provider: String,
}
/// Only audited model identities may cross the diagnostic boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelIdentity {
    Qwen3Embedding06b,
}
pub trait SyncStatusSource: Send + Sync {
    fn status(&self) -> Pin<Box<dyn Future<Output = SyncStatus> + Send + '_>>;
}
