//! Authenticated webhook hints. Admission is separate from authoritative refresh.
use std::{future::Future, pin::Pin};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookEvent {
    pub id: String,
    pub timestamp: String,
    pub workspace_id: String,
    pub subscription_id: String,
    pub integration_id: String,
    pub event_type: String,
    pub entity_id: String,
    pub entity_type: String,
    pub attempt_number: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Unavailable,
}

/// Success means durable admission (including an already admitted duplicate).
/// Implementations must not acknowledge merely buffered or discarded hints.
pub trait WebhookAdmission: Send + Sync {
    fn admit(
        &self,
        event: WebhookEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + '_>>;
}

/// Durable inbox identity. IDs are canonicalized before storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventKey {
    pub workspace_id: String,
    pub subscription_id: String,
    pub event_id: String,
}
impl WebhookEvent {
    pub fn valid_timestamp(&self) -> bool {
        chrono::DateTime::parse_from_rfc3339(&self.timestamp).is_ok()
    }
    pub fn key(&self) -> EventKey {
        EventKey {
            workspace_id: self.workspace_id.to_ascii_lowercase(),
            subscription_id: self.subscription_id.to_ascii_lowercase(),
            event_id: self.id.to_ascii_lowercase(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventState {
    Pending,
    Running,
    Succeeded,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventFailure {
    Source,
    Index,
    Conflict,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxEvent {
    pub event: WebhookEvent,
    pub state: EventState,
    pub generation: i64,
    pub lease_until: Option<i64>,
    pub failure: Option<EventFailure>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxScope {
    pub workspace_id: String,
    pub subscription_id: String,
}
/// The generation fences completion after another worker recovers an expired claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventClaim {
    pub key: EventKey,
    pub generation: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboxError {
    InvalidInput,
    Conflict,
    ClaimLost,
    Unavailable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Receipt {
    Inserted,
    Duplicate,
}
/// Synchronous durable commands; adapters must isolate blocking I/O from async transports.
/// This inbox does not grant permission to mutate indexes. Workers must separately
/// acquire the shared indexing fence. Failed work is retained; retry policy is separate.
pub trait WebhookInbox: Send + Sync {
    fn receive(&self, event: &WebhookEvent) -> Result<Receipt, InboxError>;
    fn event(&self, key: &EventKey) -> Result<Option<InboxEvent>, InboxError>;
    /// Select pending or expired-running work; failed and succeeded work remain inert.
    fn claim(
        &self,
        scope: &InboxScope,
        now: i64,
        lease_seconds: i64,
    ) -> Result<Option<(EventClaim, InboxEvent)>, InboxError>;
    fn finish(
        &self,
        claim: &EventClaim,
        now: i64,
        failure: Option<EventFailure>,
    ) -> Result<(), InboxError>;
}
