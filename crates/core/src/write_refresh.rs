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
    pub refresh: RefreshKey,
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
        refresh: key.clone(),
    }
}

/// Durable, non-secret authority for derived OAuth refresh work. This is not
/// bearer evidence and cannot authorize source writes or enroll a new grant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantRefreshAuthority {
    workspace_id: String,
    grant_id: String,
    epoch: u64,
    owner_user_id: String,
    policy_identity: String,
}
impl GrantRefreshAuthority {
    pub fn new(
        workspace_id: String,
        grant_id: String,
        epoch: u64,
        owner_user_id: String,
        policy_identity: String,
    ) -> Result<Self, RefreshQueueError> {
        let value = Self {
            workspace_id,
            grant_id,
            epoch,
            owner_user_id,
            policy_identity,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), RefreshQueueError> {
        let opaque = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        };
        if !opaque(&self.workspace_id)
            || !opaque(&self.grant_id)
            || !opaque(&self.owner_user_id)
            || self.epoch == 0
            || self.epoch > i64::MAX as u64
            || self.policy_identity.len() != 64
            || !self
                .policy_identity
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RefreshQueueError::InvalidInput);
        }
        Ok(())
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn owner_user_id(&self) -> &str {
        &self.owner_user_id
    }
    pub fn policy_identity(&self) -> &str {
        &self.policy_identity
    }
}
