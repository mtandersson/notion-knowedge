//! Process-local Prometheus metrics. All labels are fixed enums, never user input.
//! Counters are monotonic for one process lifetime; restart resets them.
use std::{
    fmt::Write,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::logging::{Operation, Outcome};

const BUCKETS: [f64; 9] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
const BUCKET_LABELS: [&str; 9] = ["0.01", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10"];
const OUTCOMES: [&str; 3] = ["success", "rejected", "failed"];
const MODES: [&str; 3] = ["semantic", "lexical", "hybrid"];
const RESULTS: [&str; 2] = ["success", "failed"];

#[derive(Default)]
struct Histogram {
    count: AtomicU64,
    sum_micros: AtomicU64,
    buckets: [AtomicU64; 9],
}

impl Histogram {
    fn record(&self, elapsed: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        for (bucket, limit) in self.buckets.iter().zip(BUCKETS) {
            if elapsed.as_secs_f64() <= limit {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Default)]
struct Metrics {
    http_counts: [AtomicU64; 3],
    http_latency: Histogram,
    notion_counts: [AtomicU64; 3],
    notion_attempts: AtomicU64,
    notion_retries: AtomicU64,
    notion_rate_limits: AtomicU64,
    search_counts: [[AtomicU64; 2]; 3],
    search_latency: [Histogram; 3],
    webhook_counts: [AtomicU64; 3],
    index_counts: [AtomicU64; 3],
    last_index_commit_seconds: AtomicU64,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

fn outcome_index(outcome: Outcome) -> usize {
    match outcome {
        Outcome::Completed | Outcome::Success => 0,
        Outcome::Rejected => 1,
        Outcome::Failed => 2,
    }
}

/// Called regardless of the configured log level. Does not retain a request ID,
/// path, status text, URL, page ID or error body.
pub fn observe_event(operation: Operation, outcome: Outcome, elapsed: Duration) {
    let metrics = metrics();
    let index = outcome_index(outcome);
    match operation {
        Operation::HttpRequest => {
            metrics.http_counts[index].fetch_add(1, Ordering::Relaxed);
            metrics.http_latency.record(elapsed);
        }
        Operation::NotionApi => {
            metrics.notion_counts[index].fetch_add(1, Ordering::Relaxed);
        }
        Operation::WebhookAdmission => {
            metrics.webhook_counts[index].fetch_add(1, Ordering::Relaxed);
        }
        Operation::IndexCommit => {
            metrics.index_counts[index].fetch_add(1, Ordering::Relaxed);
            if index == 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                metrics
                    .last_index_commit_seconds
                    .store(now, Ordering::Relaxed);
            }
        }
        Operation::McpTool => {}
    }
}

pub fn notion_attempt() {
    metrics().notion_attempts.fetch_add(1, Ordering::Relaxed);
}

pub fn notion_retry() {
    metrics().notion_retries.fetch_add(1, Ordering::Relaxed);
}

pub fn notion_rate_limited() {
    metrics().notion_rate_limits.fetch_add(1, Ordering::Relaxed);
}

/// Finite labels, independent of the search query and caller-controlled filters.
#[derive(Clone, Copy)]
pub enum SearchKind {
    Semantic,
    Lexical,
    Hybrid,
}

impl SearchKind {
    fn index(self) -> usize {
        match self {
            Self::Semantic => 0,
            Self::Lexical => 1,
            Self::Hybrid => 2,
        }
    }
}

/// Records cancelled, denied and failed queries as failures on drop.
pub struct SearchTimer {
    started: Instant,
    kind: SearchKind,
    success: bool,
}

impl SearchTimer {
    pub fn start(kind: SearchKind) -> Self {
        Self {
            started: Instant::now(),
            kind,
            success: false,
        }
    }

    pub fn succeed(&mut self) {
        self.success = true;
    }
}

impl Drop for SearchTimer {
    fn drop(&mut self) {
        let m = metrics();
        let index = self.kind.index();
        let result = if self.success { 0 } else { 1 };
        m.search_counts[index][result].fetch_add(1, Ordering::Relaxed);
        m.search_latency[index].record(self.started.elapsed());
    }
}

fn histogram(output: &mut String, name: &str, h: &Histogram, label: Option<(&str, &str)>) {
    let base = label.map_or(String::new(), |(key, value)| format!("{key}=\"{value}\""));
    let prefix = if base.is_empty() {
        String::new()
    } else {
        format!("{base},")
    };
    for (bucket, limit) in h.buckets.iter().zip(BUCKET_LABELS) {
        let _ = writeln!(
            output,
            "{name}_bucket{{{prefix}le=\"{limit}\"}} {}",
            bucket.load(Ordering::Relaxed)
        );
    }
    let _ = writeln!(
        output,
        "{name}_bucket{{{prefix}le=\"+Inf\"}} {}",
        h.count.load(Ordering::Relaxed)
    );
    let suffix = if base.is_empty() {
        String::new()
    } else {
        format!("{{{base}}}")
    };
    let _ = writeln!(
        output,
        "{name}_sum{suffix} {:.6}",
        h.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    let _ = writeln!(
        output,
        "{name}_count{suffix} {}",
        h.count.load(Ordering::Relaxed)
    );
}

/// OpenMetrics-compatible Prometheus text exposition (version 0.0.4).
/// Static metric names and controlled enum labels prevent high-cardinality leaks.
pub fn render() -> String {
    let m = metrics();
    let mut out = String::with_capacity(4096);
    out.push_str("# HELP nk_http_requests_total Process HTTP responses by outcome.\n");
    out.push_str("# TYPE nk_http_requests_total counter\n");
    for (i, label) in OUTCOMES.iter().enumerate() {
        let _ = writeln!(
            out,
            "nk_http_requests_total{{outcome=\"{label}\"}} {}",
            m.http_counts[i].load(Ordering::Relaxed)
        );
    }
    out.push_str("# HELP nk_http_request_duration_seconds HTTP request latency.\n");
    out.push_str("# TYPE nk_http_request_duration_seconds histogram\n");
    histogram(
        &mut out,
        "nk_http_request_duration_seconds",
        &m.http_latency,
        None,
    );
    out.push_str("# HELP nk_notion_requests_total Notion API calls by outcome.\n");
    out.push_str("# TYPE nk_notion_requests_total counter\n");
    for (i, label) in OUTCOMES.iter().enumerate() {
        let _ = writeln!(
            out,
            "nk_notion_requests_total{{outcome=\"{label}\"}} {}",
            m.notion_counts[i].load(Ordering::Relaxed)
        );
    }
    for (name, value) in [
        ("nk_notion_attempts_total", &m.notion_attempts),
        ("nk_notion_retries_total", &m.notion_retries),
        ("nk_notion_rate_limits_total", &m.notion_rate_limits),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {}", value.load(Ordering::Relaxed));
    }
    out.push_str("# TYPE nk_search_requests_total counter\n");
    out.push_str("# TYPE nk_search_duration_seconds histogram\n");
    for (mode, label) in MODES.iter().enumerate() {
        for (result, outcome) in RESULTS.iter().enumerate() {
            let _ = writeln!(
                out,
                "nk_search_requests_total{{mode=\"{label}\",outcome=\"{outcome}\"}} {}",
                m.search_counts[mode][result].load(Ordering::Relaxed)
            );
        }
        histogram(
            &mut out,
            "nk_search_duration_seconds",
            &m.search_latency[mode],
            Some(("mode", label)),
        );
    }
    for (name, counts) in [
        ("nk_webhook_admissions_total", &m.webhook_counts),
        ("nk_index_commits_total", &m.index_counts),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter");
        for (i, label) in OUTCOMES.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{outcome=\"{label}\"}} {}",
                counts[i].load(Ordering::Relaxed)
            );
        }
    }
    out.push_str("# TYPE nk_index_last_commit_timestamp_seconds gauge\n");
    let _ = writeln!(
        out,
        "nk_index_last_commit_timestamp_seconds {}",
        m.last_index_commit_seconds.load(Ordering::Relaxed)
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograms_are_cumulative_and_bounded() {
        let h = Histogram::default();
        h.record(Duration::from_millis(200));
        h.record(Duration::from_secs(12));
        assert_eq!(h.count.load(Ordering::Relaxed), 2);
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 0);
        assert_eq!(h.buckets[3].load(Ordering::Relaxed), 1);
        assert_eq!(h.buckets[8].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn exporter_has_fixed_labels_and_valid_histogram_series() {
        let out = render();
        assert!(out.contains("nk_search_requests_total{mode=\"lexical\",outcome=\"failed\"}"));
        assert!(out.contains("nk_http_request_duration_seconds_bucket{le=\"+Inf\"}"));
        assert!(out.contains("nk_notion_rate_limits_total"));
        assert!(!out.contains("page_id"));
        assert!(!out.contains("query="));
        assert!(!out.contains("token"));
    }

    #[test]
    fn aborted_search_is_recorded_as_failure() {
        let before = metrics().search_counts[2][1].load(Ordering::Relaxed);
        drop(SearchTimer::start(SearchKind::Hybrid));
        assert!(metrics().search_counts[2][1].load(Ordering::Relaxed) > before);
    }
}
