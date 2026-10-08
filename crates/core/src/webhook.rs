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
    pub attempt_number: u8,
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
