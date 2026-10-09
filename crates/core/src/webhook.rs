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
    pub fn timestamp_order(&self) -> Option<(i64, u32)> {
        let time = chrono::DateTime::parse_from_rfc3339(&self.timestamp).ok()?;
        Some((time.timestamp(), time.timestamp_subsec_nanos()))
    }
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
    pub cycle_attempts: i64,
    pub lifetime_attempts: i64,
    pub retry_at: Option<i64>,
    pub last_failure: Option<EventFailure>,
    pub policy: RetryPolicy,
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

/// Policy is persisted per event; requeue explicitly starts a new bounded cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: i64,
    pub base_seconds: i64,
    pub max_seconds: i64,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_seconds: 5,
            max_seconds: 300,
        }
    }
}
impl RetryPolicy {
    pub fn validate(self) -> Result<Self, InboxError> {
        if !(1..=100).contains(&self.max_attempts)
            || !(1..=86400).contains(&self.base_seconds)
            || !(self.base_seconds..=86400).contains(&self.max_seconds)
        {
            return Err(InboxError::InvalidInput);
        }
        Ok(self)
    }
    pub fn deadline(self, now: i64, attempts: i64) -> Result<i64, InboxError> {
        self.validate()?;
        if now < 0 || attempts < 1 {
            return Err(InboxError::InvalidInput);
        }
        let factor = 1_i64
            .checked_shl((attempts - 1).min(62) as u32)
            .unwrap_or(i64::MAX);
        now.checked_add(
            self.base_seconds
                .saturating_mul(factor)
                .min(self.max_seconds),
        )
        .ok_or(InboxError::InvalidInput)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessingOutcome {
    Succeeded,
    Retryable(EventFailure),
    Permanent(EventFailure),
}
/// Retry paths share the same persisted claim budget as the original inbox API.
pub trait WebhookRecovery: WebhookInbox {
    fn complete(
        &self,
        claim: &EventClaim,
        now: i64,
        outcome: ProcessingOutcome,
    ) -> Result<(), InboxError>;
    fn failed(&self, scope: &InboxScope, limit: i64) -> Result<Vec<InboxEvent>, InboxError>;
    fn requeue(
        &self,
        key: &EventKey,
        generation: i64,
        now: i64,
        policy: RetryPolicy,
    ) -> Result<(), InboxError>;
}

/// Trailing-edge quiet period, bounded by a maximum burst delay. Unix milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DebounceWindow {
    pub quiet_ms: i64,
    pub max_delay_ms: i64,
}
impl Default for DebounceWindow {
    fn default() -> Self {
        Self {
            quiet_ms: 5000,
            max_delay_ms: 30000,
        }
    }
}
impl DebounceWindow {
    pub fn validate(self) -> Result<Self, InboxError> {
        if !(1..=60000).contains(&self.quiet_ms)
            || !(self.quiet_ms..=300000).contains(&self.max_delay_ms)
        {
            return Err(InboxError::InvalidInput);
        }
        Ok(self)
    }
}

/// Page identity is additionally isolated by workspace and subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageWorkKey {
    pub scope: InboxScope,
    pub page_id: String,
}
/// Immutable set of events covered by one refresh. Arrivals after claim belong
/// to a successor batch and cannot be acknowledged by this ownership token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageWorkClaim {
    pub key: PageWorkKey,
    pub generation: i64,
    pub events: Vec<EventClaim>,
    pub newest: WebhookEvent,
}
/// Durable page coalescing is separate from authoritative reads and index effects.
/// A page claim is not the shared index-writer fence.
pub trait WebhookDebounce: WebhookRecovery {
    /// Receipt and page membership commit together. Only page content/property
    /// update hints are grouped; unsupported event kinds remain normal inbox work.
    fn receive_debounced(
        &self,
        event: &WebhookEvent,
        now_ms: i64,
        window: DebounceWindow,
    ) -> Result<Receipt, InboxError>;
    fn claim_page(
        &self,
        scope: &InboxScope,
        now_ms: i64,
        lease_seconds: i64,
    ) -> Result<Option<PageWorkClaim>, InboxError>;
    /// Applies the same bounded event recovery policy atomically to the snapshot.
    fn complete_page(
        &self,
        claim: &PageWorkClaim,
        now_ms: i64,
        outcome: ProcessingOutcome,
    ) -> Result<(), InboxError>;
}
