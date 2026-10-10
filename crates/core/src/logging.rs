//! Allowlisted, payload-free JSON event logs with task-scoped correlation IDs.
//! Never add arbitrary user input, URLs, page identifiers or error strings to events.

use std::{
    future::Future,
    sync::atomic::{AtomicU64, AtomicU8, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static LOG_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

tokio::task_local! {
    static REQUEST_ID: CorrelationId;
}

/// The only accepted event fields are fixed enums, numeric durations/statuses
/// and an internally generated opaque ID. No payload or URL input is accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationId(String);

impl CorrelationId {
    pub fn new() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let number = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(format!("{millis:016x}{:08x}{number:016x}", std::process::id()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

pub fn current_id() -> CorrelationId {
    REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| CorrelationId::new())
}

/// Preserve the same correlation ID across awaited HTTP/Notion operations.
/// Detached workers must capture the ID explicitly before spawning.
pub async fn scope<T>(id: CorrelationId, future: impl Future<Output = T>) -> T {
    REQUEST_ID.scope(id, future).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Level {
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl Level {
    pub fn parse(text: &str) -> Option<Self> {
        match text.to_ascii_lowercase().as_str() {
            "off" => Some(Self::Off),
            "error" => Some(Self::Error),
            "warn" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }
}

pub fn set_level(level: Level) {
    LOG_LEVEL.store(level as u8, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    HttpRequest,
    McpTool,
    NotionApi,
    WebhookAdmission,
    IndexCommit,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::HttpRequest => "http_request",
            Self::McpTool => "mcp_tool",
            Self::NotionApi => "notion_api",
            Self::WebhookAdmission => "webhook_admission",
            Self::IndexCommit => "index_commit",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Completed,
    Success,
    Rejected,
    Failed,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }
}

/// Pure serializer permits privacy regression tests without capturing stderr.
pub fn event_json(
    id: &CorrelationId,
    operation: Operation,
    outcome: Outcome,
    elapsed: Duration,
    status: Option<u16>,
    level: Level,
) -> String {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    serde_json::json!({
        "timestamp_unix_ms": timestamp_ms,
        "level": level.label(),
        "correlation_id": id.as_str(),
        "operation": operation.label(),
        "duration_ms": elapsed.as_millis(),
        "outcome": outcome.label(),
        "http_status": status,
    })
    .to_string()
}

pub fn emit(
    id: &CorrelationId,
    operation: Operation,
    outcome: Outcome,
    elapsed: Duration,
    status: Option<u16>,
    level: Level,
) {
    if level != Level::Off && level as u8 <= LOG_LEVEL.load(Ordering::Relaxed) {
        eprintln!("{}", event_json(id, operation, outcome, elapsed, status, level));
    }
}

/// A guard records a bounded event even when a call returns early or is cancelled.
pub struct EventGuard {
    id: CorrelationId,
    operation: Operation,
    started: Instant,
    outcome: Outcome,
}

impl EventGuard {
    pub fn new(operation: Operation) -> Self {
        Self::with_id(operation, current_id())
    }

    pub fn with_id(operation: Operation, id: CorrelationId) -> Self {
        Self {
            id,
            operation,
            started: Instant::now(),
            outcome: Outcome::Completed,
        }
    }

    pub fn finish(&mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}

impl Drop for EventGuard {
    fn drop(&mut self) {
        emit(
            &self.id,
            self.operation,
            self.outcome,
            self.started.elapsed(),
            None,
            Level::Info,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_events_have_only_safe_allowlisted_fields() {
        let id = CorrelationId::new();
        let line = event_json(
            &id,
            Operation::NotionApi,
            Outcome::Failed,
            Duration::from_millis(13),
            Some(503),
            Level::Warn,
        );
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["correlation_id"], id.as_str());
        assert_eq!(parsed["operation"], "notion_api");
        assert_eq!(parsed["outcome"], "failed");
        assert_eq!(parsed["http_status"], 503);
        assert_eq!(parsed["duration_ms"], 13);
        assert_eq!(parsed.as_object().unwrap().len(), 7);
        assert!(!line.contains("authorization"));
        assert!(!line.contains("token"));
        assert!(!line.contains("url"));
    }

    #[test]
    fn level_parser_is_strict_and_ids_are_unique() {
        assert_eq!(Level::parse("DEBUG"), Some(Level::Debug));
        assert_eq!(Level::parse("INFO private"), None);
        assert_eq!(Level::parse("trace"), None);
        let a = CorrelationId::new();
        let b = CorrelationId::new();
        assert_ne!(a, b);
        assert!(a.as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn task_scope_propagates_but_does_not_leak_across_requests() {
        let requested = CorrelationId::new();
        let actual = scope(requested.clone(), async {
            let same = current_id();
            tokio::task::yield_now().await;
            assert_eq!(same, current_id());
            same
        })
        .await;
        assert_eq!(actual, requested);
        assert_ne!(current_id(), requested);
    }
}
