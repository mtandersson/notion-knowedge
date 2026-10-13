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
