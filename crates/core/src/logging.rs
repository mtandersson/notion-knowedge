//! Allowlisted, payload-free JSON event logs with task-scoped correlation IDs.
//! Never add arbitrary user input, URLs, page identifiers or error strings to events.

use std::{
    future::Future,
    io::Write,
    sync::atomic::{AtomicU8, AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub mod page_timings;
mod sync_progress;
pub use sync_progress::{
    SyncContext, SyncCount, current_sync_progress, in_sync_context, sync_context, sync_count,
    sync_total_pages, with_sync_context, with_sync_progress,
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
    /// Running sums of the per-page figures, folded in by the consumer.
    pages: u64,
    totals: std::collections::BTreeMap<String, u64>,
}

/// How a page ended, as the fixed status and reason of its one log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageEnd {
    Committed,
    Unchanged,
    /// A fixed reason class, never an error string.
    Skipped(&'static str),
}

/// The one per-page line. Only the fixed phase, status and reason plus numeric
/// durations and counts: no page title, ID or content can be expressed here.
pub fn page_line_json(
    phase: SyncPhase,
    end: PageEnd,
    fields: &[(String, u64)],
    progress: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut line = serde_json::json!({
        "operation": "cache_sync", "phase": phase.label(),
        "status": match end {
            PageEnd::Committed => "page_committed",
            PageEnd::Unchanged => "page_unchanged",
            PageEnd::Skipped(_) => "page_skipped",
        },
        "sync_progress": progress,
    });
    if let PageEnd::Skipped(reason) = end {
        line["reason"] = reason.into();
    }
    for (name, value) in fields {
        line[name.as_str()] = (*value).into();
    }
    line
}

impl SyncProgressLog {
    /// Emit this page's one line and fold its figures into the running totals
    /// that the periodic progress line reports.
    pub fn page(&mut self, end: PageEnd, fields: &[(String, u64)]) {
        self.pages += 1;
        for (name, value) in fields {
            let total = self.totals.entry(name.clone()).or_default();
            *total = total.saturating_add(*value);
        }
        if LOG_LEVEL.load(Ordering::Relaxed) < Level::Info as u8 {
            return;
        }
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            page_line_json(self.phase, end, fields, current_sync_progress()),
        );
    }

    pub fn new(phase: SyncPhase) -> Self {
        Self {
            phase,
            started: Instant::now(),
            last: None,
            pages: 0,
            totals: Default::default(),
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
        let mut line = serde_json::json!({
            "operation": "cache_sync", "phase": phase,
            "sync_progress": current_sync_progress(),
            "status": if finished { "completed" } else { "running" },
            "processed": processed, "total": total, "pages": pages,
            "skipped": skipped, "elapsed_seconds": self.started.elapsed().as_secs(),
        "runtime_thread": format!("{:?}", std::thread::current().id()),
            // Resident-memory trend across long syncs (#656); null off Linux.
            "rss_kib": memory.map(|m| m.rss_kib), "rss_peak_kib": memory.map(|m| m.peak_kib),
        });
        // Running sums of the per-page figures: divide by `pages` for averages.
        if self.pages > 0 {
            let mut totals = serde_json::json!({ "pages": self.pages });
            for (name, value) in &self.totals {
                totals[name.as_str()] = (*value).into();
            }
            line["page_totals"] = totals;
        }
        let _ = writeln!(std::io::stderr().lock(), "{line}");
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
    FileIngest,
    WebhookAdmission,
    IndexCommit,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::HttpRequest => "http_request",
            Self::McpTool => "mcp_tool",
            Self::NotionApi => "notion_api",
            Self::FileIngest => "file_ingest",
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
    event_value(id, operation, outcome, elapsed, status, level).to_string()
}

fn event_value(
    id: &CorrelationId,
    operation: Operation,
    outcome: Outcome,
    elapsed: Duration,
    status: Option<u16>,
    level: Level,
) -> serde_json::Value {
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
    event
}

fn enabled(level: Level) -> bool {
    level != Level::Off && level as u8 <= LOG_LEVEL.load(Ordering::Relaxed)
}

fn write_event(event: impl std::fmt::Display) {
    // A closed stderr must never abort request processing.
    let _ = writeln!(std::io::stderr().lock(), "{event}");
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
    if enabled(level) {
        write_event(event_value(id, operation, outcome, elapsed, status, level));
    }
}

/// Record which stage left a guarded write unproven. Fixed labels only: no
/// page ids, titles or Markdown. The caller still sees plain `outcome_unknown`.
pub fn emit_write_outcome_unknown(outcome: Outcome, stage: &'static str, detail: &'static str) {
    let level = outcome.level();
    if enabled(level) {
        write_event(write_unknown_value(&current_id(), outcome, stage, detail));
    }
}

fn write_unknown_value(
    id: &CorrelationId,
    outcome: Outcome,
    stage: &'static str,
    detail: &'static str,
) -> serde_json::Value {
    let mut event = event_value(
        id,
        Operation::McpTool,
        outcome,
        Duration::ZERO,
        None,
        outcome.level(),
    );
    event["tool"] = "guarded_write".into();
    event["result"] = "outcome_unknown".into();
    event["stage"] = stage.into();
    if !detail.is_empty() {
        event["detail"] = detail.into();
    }
    event
}

/// Record that Notion definitely refused a guarded write before any effect,
/// and whether its idempotency claim was released. Fixed labels only.
pub fn emit_write_rejected(stage: &'static str, detail: &'static str, released: bool) {
    let outcome = Outcome::Rejected;
    if enabled(outcome.level()) {
        write_event(write_rejected_value(&current_id(), stage, detail, released));
    }
}

fn write_rejected_value(
    id: &CorrelationId,
    stage: &'static str,
    detail: &'static str,
    released: bool,
) -> serde_json::Value {
    let outcome = Outcome::Rejected;
    let mut event = event_value(
        id,
        Operation::McpTool,
        outcome,
        Duration::ZERO,
        None,
        outcome.level(),
    );
    event["tool"] = "guarded_write".into();
    event["result"] = "rejected".into();
    event["stage"] = stage.into();
    if !detail.is_empty() {
        event["detail"] = detail.into();
    }
    event["claim"] = if released { "released" } else { "retained" }.into();
    event
}

/// Record why a saved-view query failed using fixed classifications only: the
/// failure kind and the stage that produced it, never an id or upstream text.
pub fn emit_view_query_failure(
    outcome: Outcome,
    elapsed: Duration,
    kind: &'static str,
    stage: &'static str,
) {
    let level = outcome.level();
    if enabled(level) {
        write_event(view_query_failure_value(
            &current_id(),
            outcome,
            elapsed,
            kind,
            stage,
        ));
    }
}

fn view_query_failure_value(
    id: &CorrelationId,
    outcome: Outcome,
    elapsed: Duration,
    kind: &'static str,
    stage: &'static str,
) -> serde_json::Value {
    let mut event = event_value(
        id,
        Operation::McpTool,
        outcome,
        elapsed,
        None,
        outcome.level(),
    );
    event["tool"] = "saved_view_query".into();
    event["kind"] = kind.into();
    event["stage"] = stage.into();
    event
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FileIngestPhase {
    Admission,
    SourceDownload,
    FileValidation,
    NotionUpload,
    NotionAttachment,
    Audit,
}

pub(crate) struct FileIngestDiagnostic {
    pub file_index: usize,
    pub phase: FileIngestPhase,
    /// Fixed error classification, never a user value or upstream error string.
    pub reason: &'static str,
    pub failure: &'static str,
    pub terminal_audited: bool,
    pub effect_possible: bool,
}

pub(crate) fn emit_file_ingest_failure(
    id: &CorrelationId,
    outcome: Outcome,
    elapsed: Duration,
    diagnostic: FileIngestDiagnostic,
) {
    let level = outcome.level();
    if enabled(level) {
        let mut event = event_value(id, Operation::FileIngest, outcome, elapsed, None, level);
        event["file_index"] = diagnostic.file_index.into();
        event["phase"] = serde_json::json!(diagnostic.phase);
        event["reason"] = diagnostic.reason.into();
        event["failure"] = diagnostic.failure.into();
        event["terminal_audited"] = diagnostic.terminal_audited.into();
        event["effect_possible"] = diagnostic.effect_possible.into();
        write_event(event);
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
    fn page_lines_carry_only_fixed_words_and_numbers() {
        let fields = vec![("wall_ms".to_owned(), 7), ("embedded_chunks".to_owned(), 2)];
        let line = page_line_json(
            SyncPhase::Indexing,
            PageEnd::Skipped("page_timeout"),
            &fields,
            None,
        );
        assert_eq!(line["status"], "page_skipped");
        assert_eq!(line["reason"], "page_timeout");
        for (name, value) in line.as_object().unwrap() {
            let fixed = ["operation", "phase", "status", "reason", "sync_progress"];
            assert!(fixed.contains(&name.as_str()) || value.is_u64(), "{name}");
        }
        let committed = page_line_json(SyncPhase::Indexing, PageEnd::Committed, &fields, None);
        assert!(committed.get("reason").is_none());
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

    #[test]
    fn rejected_write_event_names_the_stage_and_claim_without_content() {
        let event = write_rejected_value(&CorrelationId::new(), "create.post", "not_found", true);
        assert_eq!(event["result"], "rejected");
        assert_eq!(event["stage"], "create.post");
        assert_eq!(event["detail"], "not_found");
        assert_eq!(event["claim"], "released");
        assert_eq!(event["outcome"], "rejected");
        let retained = write_rejected_value(&CorrelationId::new(), "append.patch", "", false);
        assert_eq!(retained["claim"], "retained");
        assert!(retained.get("detail").is_none());
    }

    #[test]
    fn unknown_write_event_names_the_stage_without_content() {
        let event = write_unknown_value(
            &CorrelationId::new(),
            Outcome::Failed,
            "create.readback_mismatch",
            "title",
        );
        let mut keys: Vec<&str> = event
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "correlation_id",
                "detail",
                "duration_ms",
                "http_status",
                "level",
                "operation",
                "outcome",
                "result",
                "runtime_thread",
                "stage",
                "timestamp_unix_ms",
                "tool",
                "worker"
            ]
        );
        assert_eq!(event["result"], "outcome_unknown");
        assert_eq!(event["stage"], "create.readback_mismatch");
        let cancelled = write_unknown_value(
            &CorrelationId::new(),
            Outcome::Rejected,
            "mutation.caller_cancelled",
            "",
        );
        assert!(cancelled["detail"].is_null());
        assert_eq!(cancelled["level"], "warn");
    }

    #[test]
    fn view_failure_event_names_kind_and_stage_without_identifiers() {
        let event = view_query_failure_value(
            &CorrelationId::new(),
            Outcome::Rejected,
            Duration::from_millis(7),
            "permission_denied",
            "scope.excluded",
        );
        assert_eq!(event["tool"], "saved_view_query");
        assert_eq!(event["kind"], "permission_denied");
        assert_eq!(event["stage"], "scope.excluded");
        assert_eq!(event["outcome"], "rejected");
        assert_eq!(event["level"], "warn");
        // Exactly these fields, so nothing identifying can ride along.
        let mut keys: Vec<&str> = event
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut expected = vec![
            "correlation_id",
            "duration_ms",
            "http_status",
            "kind",
            "level",
            "operation",
            "outcome",
            "runtime_thread",
            "stage",
            "timestamp_unix_ms",
            "tool",
            "worker",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected);
    }
}
