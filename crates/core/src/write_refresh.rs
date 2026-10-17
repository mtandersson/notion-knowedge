//! Durable local refresh admission after an authoritative verified mutation.
//! This is trusted application wiring, never an authenticated webhook receipt.
use crate::idempotency::VerifiedReceipt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshKey {
    pub workspace_id: String,
    /// Server-derived stable mutation claim digest, never a provider event ID.
    pub mutation_digest: String,
    pub page_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStatus {
    Queued,
    Running,
    Completed,
    Failed,
    /// Admission/status could not be durably established. The write remains valid.
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshQueueError {
    InvalidInput,
    Conflict,
    Unavailable,
    ClaimLost,
}

/// Bound to trusted local storage. Admission must be durable, idempotent and
/// collision-safe; repeated admission must not erase terminal status or repeat
/// an upstream mutation. The caller has already verified Notion read-back.
pub trait MutationRefreshQueue: Send + Sync {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, RefreshQueueError>;
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, RefreshQueueError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteCompletion {
    pub receipt: VerifiedReceipt,
    pub replayed: bool,
    /// A verified write is independent of this derived local index status.
    pub index: IndexStatus,
}

pub fn admit_verified(
    queue: &dyn MutationRefreshQueue,
    key: &RefreshKey,
    receipt: VerifiedReceipt,
    replayed: bool,
) -> WriteCompletion {
    let index = if key.page_id == receipt.page_id {
        queue.admit(key).unwrap_or(IndexStatus::Unavailable)
    } else {
        IndexStatus::Unavailable
    };
    WriteCompletion {
        receipt,
        replayed,
        index,
    }
}
