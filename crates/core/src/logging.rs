//! Allowlisted, payload-free JSON event logs with task-scoped correlation IDs.
//! Never add arbitrary user input, URLs, page identifiers or error strings to events.

use std::{
    future::Future,
    io::Write,
    sync::atomic::{AtomicU8, AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod sync_progress;
pub use sync_progress::{
    SyncContext, SyncCount, current_sync_progress, sync_context, sync_count, sync_total_pages,
    with_sync_context, with_sync_progress,
};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static LOG_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

tokio::task_local! {
    static REQUEST_ID: CorrelationId;
    static SYNC_TASK: (SyncPhase, usize);
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
        Self(format!(
            "{millis:016x}{:08x}{number:016x}",
            std::process::id()
        ))
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

#[derive(Clone, Copy)]
pub enum SyncPhase {
    Discovery,
    Planning,
    Indexing,
    PreparingSearch,
}

impl SyncPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Planning => "planning",
            Self::Indexing => "indexing",
            Self::PreparingSearch => "preparing_search",
        }
    }
}

/// An async fetch branch may move between runtime threads. Keep its numeric
/// worker label and correlation ID stable across its HTTP requests.
pub async fn sync_task<T>(phase: SyncPhase, task: usize, future: impl Future<Output = T>) -> T {
    SYNC_TASK
        .scope((phase, task), scope(CorrelationId::new(), future))
        .await
}

fn sync_worker() -> Option<String> {
    SYNC_TASK
        .try_with(|(phase, task)| format!("{}-{task}", phase.label()))
        .ok()
        .or_else(sync_progress::thread_worker)
}

/// Periodic operator progress contains counts only, never source identifiers.
pub struct SyncProgressLog {
    phase: SyncPhase,
    started: Instant,
    last: Option<Instant>,
}

impl SyncProgressLog {
    /// A fixed local admission reason, never model errors or source text.
    pub fn embedding_input_limit(&self) {
        if LOG_LEVEL.load(Ordering::Relaxed) < Level::Info as u8 {
            return;
        }
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            serde_json::json!({
                "operation": "cache_sync", "phase": self.phase.label(),
                "status": "page_skipped", "reason": "embedding_input_limit",
                "sync_progress": current_sync_progress(),
            }),
        );
    }

    /// Fixed per-page deadline class; never contains source identifiers or errors.
    pub fn page_timeout(&self) {
        if LOG_LEVEL.load(Ordering::Relaxed) < Level::Info as u8 {
            return;
        }
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            serde_json::json!({
                "operation": "cache_sync", "phase": self.phase.label(),
                "status": "page_skipped", "reason": "page_timeout",
                "sync_progress": current_sync_progress(),
            })
        );
    }

    pub fn new(phase: SyncPhase) -> Self {
        Self {
            phase,
            started: Instant::now(),
            last: None,
        }
    }

    pub fn report(
        &mut self,
        processed: usize,
        total: Option<usize>,
        pages: usize,
        skipped: usize,
        finished: bool,
    ) {
        sync_progress::sync_phase(self.phase);
        let now = Instant::now();
        if !finished
            && self
                .last
                .is_some_and(|last| now.duration_since(last) < Duration::from_secs(10))
        {
            return;
        }
        self.last = Some(now);
        if LOG_LEVEL.load(Ordering::Relaxed) < Level::Info as u8 {
            return;
        }
        let phase = self.phase.label();
        let memory = ResidentMemory::current();
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            serde_json::json!({
                "operation": "cache_sync", "phase": phase,
                "sync_progress": current_sync_progress(),
                "status": if finished { "completed" } else { "running" },
                "processed": processed, "total": total, "pages": pages,
                "skipped": skipped, "elapsed_seconds": self.started.elapsed().as_secs(),
            "runtime_thread": format!("{:?}", std::thread::current().id()),
                // Resident-memory trend across long syncs (#656); null off Linux.
                "rss_kib": memory.map(|m| m.rss_kib), "rss_peak_kib": memory.map(|m| m.peak_kib),
            })
        );
    }
}

/// Process resident memory from the kernel's own accounting, in KiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResidentMemory {
    pub rss_kib: u64,
    pub peak_kib: u64,
}
impl ResidentMemory {
    /// `None` where `/proc/self/status` is unavailable or unparsable.
    pub fn current() -> Option<Self> {
        Self::parse(&std::fs::read_to_string("/proc/self/status").ok()?)
    }
    fn parse(status: &str) -> Option<Self> {
        let field = |name: &str| {
            status.lines().find_map(|line| {
                line.strip_prefix(name)?
                    .trim()
                    .strip_suffix("kB")?
                    .trim()
                    .parse()
                    .ok()
            })
        };
        Some(Self {
            rss_kib: field("VmRSS:")?,
            peak_kib: field("VmHWM:")?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    HttpRequest,
    McpTool,
    NotionApi,
    NotionOAuthWorkspace,
    NotionOAuthUser,
    WebhookAdmission,
    IndexCommit,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::HttpRequest => "http_request",
            Self::McpTool => "mcp_tool",
            Self::NotionApi => "notion_api",
            Self::NotionOAuthWorkspace => "notion_oauth_workspace",
            Self::NotionOAuthUser => "notion_oauth_user",
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
    pub fn level(self) -> Level {
        match self {
            Self::Completed | Self::Success => Level::Info,
            Self::Rejected => Level::Warn,
            Self::Failed => Level::Error,
        }
    }

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
    let mut event = serde_json::json!({
        "timestamp_unix_ms": timestamp_ms,
        "level": level.label(),
        "correlation_id": id.as_str(),
        "operation": operation.label(),
        "duration_ms": elapsed.as_millis(),
        "outcome": outcome.label(),
        "http_status": status,
        "worker": sync_worker(),
        "runtime_thread": format!("{:?}", std::thread::current().id()),
    });
    if let Some(progress) = current_sync_progress() {
        event["sync_progress"] = progress;
    }
    event.to_string()
}

pub fn emit(
    id: &CorrelationId,
    operation: Operation,
    outcome: Outcome,
    elapsed: Duration,
    status: Option<u16>,
    level: Level,
) {
    crate::metrics::observe_event(operation, outcome, elapsed);
    if level != Level::Off && level as u8 <= LOG_LEVEL.load(Ordering::Relaxed) {
        // Logging must never abort request processing if a supervisor closes
        // stderr (or a test stops consuming its pipe).
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "{}",
            event_json(id, operation, outcome, elapsed, status, level)
        );
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
            self.outcome.level(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_memory_reads_current_and_peak_kernel_accounting() {
        let status = "Name:\tnk\nVmHWM:\t 4183040 kB\nVmRSS:\t 2540000 kB\nThreads:\t9\n";
        assert_eq!(
            ResidentMemory::parse(status),
            Some(ResidentMemory {
                rss_kib: 2_540_000,
                peak_kib: 4_183_040
            })
        );
        assert_eq!(ResidentMemory::parse("Name:\tnk\nVmRSS:\t12 kB\n"), None);
        #[cfg(target_os = "linux")]
        assert!(ResidentMemory::current().is_some_and(|m| m.rss_kib > 0));
    }

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
        assert_eq!(parsed.as_object().unwrap().len(), 9);
        assert!(parsed["worker"].is_null());
        assert!(parsed["runtime_thread"].as_str().is_some());
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
        sync_task(SyncPhase::Discovery, 3, async {
            assert_eq!(sync_worker().as_deref(), Some("discovery-3"));
            let id = current_id();
            tokio::task::yield_now().await;
            assert_eq!(sync_worker().as_deref(), Some("discovery-3"));
            assert_eq!(current_id(), id);
        })
        .await;
        assert!(sync_worker().is_none());
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
    #[tokio::test]
    async fn sync_counts_follow_async_and_owned_threads_without_leaking_to_other_work() {
        assert!(current_sync_progress().is_none());
        with_sync_progress(async {
            sync_count(SyncCount::DiscoveredPage, 2);
            sync_count(SyncCount::ContentRead, 1);
            sync_count(SyncCount::EmbeddedChunk, 3);
            let captured = sync_context();
            let event = std::thread::spawn(move || {
                with_sync_context(captured, || {
                    sync_count(SyncCount::CommittedPage, 1);
                    event_json(
                        &CorrelationId::new(),
                        Operation::IndexCommit,
                        Outcome::Success,
                        Duration::ZERO,
                        None,
                        Level::Info,
                    )
                })
            })
            .join()
            .unwrap();
            let event: serde_json::Value = serde_json::from_str(&event).unwrap();
            assert_eq!(event["sync_progress"]["discovered_pages"], 2);
            assert_eq!(event["sync_progress"]["content_reads"], 1);
            assert_eq!(event["sync_progress"]["embedded_chunks"], 3);
            assert_eq!(event["sync_progress"]["committed_pages"], 1);
            assert!(event["sync_progress"]["total_pages"].is_null());
            sync_total_pages(2);
            assert_eq!(current_sync_progress().unwrap()["total_pages"], 2);
        })
        .await;
        assert!(current_sync_progress().is_none());
    }
}
