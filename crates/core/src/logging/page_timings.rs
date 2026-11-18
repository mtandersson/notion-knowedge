//! Content-free, per-page phase timings (#662).
//!
//! One [`PageTimings`] follows a page from the walk that read it, through
//! preparation and the queue, to its commit. Every figure is a duration or a
//! count held in a relaxed atomic: recording takes no lock, makes no request
//! and is a no-op outside a page's scope. Nothing here accepts text, so a
//! page title, ID or body cannot reach a log line through it.
use std::{
    cell::RefCell,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

/// Which part of a page's life a Notion request belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiPhase {
    /// The walk's own read of the page: authorization and lifecycle metadata,
    /// Markdown content and child discovery.
    Walk = 0,
    /// Relation and link target authorization while preparing the page.
    Prepare = 1,
    /// Requests made on the commit path. Sync makes none; the field proves it.
    Commit = 2,
}
const API_PHASES: [(ApiPhase, &str); 3] = [
    (ApiPhase::Walk, "walk"),
    (ApiPhase::Prepare, "prepare"),
    (ApiPhase::Commit, "commit"),
];

/// Local time buckets measured inside the consumer's wall time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bucket {
    Embed = 0,
    LanceMerge = 1,
    FtsOptimize = 2,
    VectorOptimize = 3,
    Receipt = 4,
    /// Time spent in the shared Notion limiter's `admit()`; part of the API
    /// time, not an additional bucket.
    LimiterWait = 5,
}
const BUCKETS: usize = 6;

#[derive(Default)]
struct Inner {
    phase: AtomicU8,
    api_ops: [AtomicU64; 3],
    api_nanos: [AtomicU64; 3],
    nanos: [AtomicU64; BUCKETS],
}

/// A cheap handle to one page's accumulator. Equality ignores the figures: a
/// page is the same page whatever it measured.
#[derive(Clone, Default)]
pub struct PageTimings(Arc<Inner>);

impl std::fmt::Debug for PageTimings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PageTimings")
    }
}
impl PartialEq for PageTimings {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

tokio::task_local! { static CURRENT: PageTimings; }
thread_local! { static THREAD: RefCell<Option<PageTimings>> = const { RefCell::new(None) }; }

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl PageTimings {
    /// Requests made while `future` runs count under `phase`. The phase is
    /// stored on the handle, so it also covers owned threads that received
    /// the handle through [`with_thread`]. A page's phases run one after the
    /// other, so one current phase per page is exact.
    pub async fn scope<T>(&self, phase: ApiPhase, future: impl Future<Output = T>) -> T {
        self.0.phase.store(phase as u8, Relaxed);
        CURRENT.scope(self.clone(), future).await
    }

    /// One Notion request that took `elapsed` including any limiter wait.
    pub fn record_api(&self, elapsed: Duration) {
        let phase = usize::from(self.0.phase.load(Relaxed)).min(2);
        self.0.api_ops[phase].fetch_add(1, Relaxed);
        self.0.api_nanos[phase].fetch_add(nanos(elapsed), Relaxed);
    }

    pub fn add(&self, bucket: Bucket, elapsed: Duration) {
        self.0.nanos[bucket as usize].fetch_add(nanos(elapsed), Relaxed);
    }

    /// Records the time until it is dropped, so an early return still counts.
    pub fn timer(&self, bucket: Bucket) -> Timer {
        Timer {
            timings: Some(self.clone()),
            bucket,
            started: Instant::now(),
        }
    }

    pub fn snapshot(&self) -> PageSnapshot {
        let load =
            |cells: &[AtomicU64]| -> Vec<u64> { cells.iter().map(|c| c.load(Relaxed)).collect() };
        let bucket = |b: Bucket| self.0.nanos[b as usize].load(Relaxed);
        PageSnapshot {
            api_ops: load(&self.0.api_ops),
            api_nanos: load(&self.0.api_nanos),
            embed: bucket(Bucket::Embed),
            lance_merge: bucket(Bucket::LanceMerge),
            fts_optimize: bucket(Bucket::FtsOptimize),
            vector_optimize: bucket(Bucket::VectorOptimize),
            receipt: bucket(Bucket::Receipt),
            limiter_wait: bucket(Bucket::LimiterWait),
        }
    }
}

/// Guard returned by [`PageTimings::timer`] and [`timer`].
pub struct Timer {
    timings: Option<PageTimings>,
    bucket: Bucket,
    started: Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(timings) = &self.timings {
            timings.add(self.bucket, self.started.elapsed());
        }
    }
}

/// The page whose work is running on this task or owned thread, if any.
pub fn current() -> Option<PageTimings> {
    CURRENT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD.with(|t| t.borrow().clone()))
}

/// Time the current page's `bucket`; a no-op guard outside a page's scope.
pub fn timer(bucket: Bucket) -> Timer {
    let timings = current();
    Timer {
        timings,
        bucket,
        started: Instant::now(),
    }
}

/// Run owned-thread work with `timings` current, restoring on unwind.
pub fn with_thread<T>(timings: Option<PageTimings>, work: impl FnOnce() -> T) -> T {
    struct Reset(Option<PageTimings>);
    impl Drop for Reset {
        fn drop(&mut self) {
            THREAD.with(|t| t.replace(self.0.take()));
        }
    }
    let _reset = Reset(THREAD.with(|t| t.replace(timings)));
    work()
}

/// A point-in-time copy of one page's figures, in nanoseconds.
#[derive(Clone, Debug, Default)]
pub struct PageSnapshot {
    api_ops: Vec<u64>,
    api_nanos: Vec<u64>,
    embed: u64,
    lance_merge: u64,
    fts_optimize: u64,
    vector_optimize: u64,
    receipt: u64,
    limiter_wait: u64,
}

/// What the consumer measured around one page, beside the page's own figures.
#[derive(Clone, Copy, Debug, Default)]
pub struct PageWait {
    /// Wall time from the consumer asking for a page to its settlement.
    pub wall: Duration,
    /// Part of `wall` the consumer spent waiting for a prepared page.
    pub consumer_wait: Duration,
    /// How long the prepared page sat queued for the consumer. Spent on the
    /// producer side while earlier pages were consumed, so outside `wall`.
    pub queue_wait: Duration,
    pub embedded_chunks: usize,
    pub skipped_chunks: usize,
}

const MS: u64 = 1_000_000;

/// The ordered, allowlisted numeric fields of one per-page line.
/// Every value is a duration in milliseconds or a count.
///
/// `wall_ms` is the consumer's wall time for the page. The fields
/// `consumer_wait_ms`, `embed_ms`, `lance_merge_ms`, `fts_optimize_ms`,
/// `vector_optimize_ms` and `receipt_ms` are disjoint parts of it, and
/// `other_ms` is the remainder, so they sum to `wall_ms` exactly. The `*_api_*`
/// fields, `limiter_wait_ms` and `queue_wait_ms` overlap other pages' work and
/// are reported beside that identity, not inside it.
pub fn page_fields(snapshot: &PageSnapshot, wait: &PageWait) -> Vec<(String, u64)> {
    let wall = nanos(wait.wall);
    let consumer_wait = nanos(wait.consumer_wait).min(wall);
    let parts = [
        ("consumer_wait_ms", consumer_wait),
        ("embed_ms", snapshot.embed),
        ("lance_merge_ms", snapshot.lance_merge),
        ("fts_optimize_ms", snapshot.fts_optimize),
        ("vector_optimize_ms", snapshot.vector_optimize),
        ("receipt_ms", snapshot.receipt),
    ];
    let inside: u64 = parts.iter().map(|(_, n)| n).sum();
    let mut fields = vec![("wall_ms".to_owned(), wall / MS)];
    fields.extend(parts.iter().map(|(name, n)| ((*name).to_owned(), n / MS)));
    // Truncating each part never lets their sum exceed the truncated total.
    let summed: u64 = fields[1..].iter().map(|(_, ms)| ms).sum();
    let other = if inside <= wall {
        fields[0].1 - summed
    } else {
        0
    };
    fields.push(("other_ms".to_owned(), other));
    fields.push(("queue_wait_ms".to_owned(), nanos(wait.queue_wait) / MS));
    for (index, (_, name)) in API_PHASES.iter().enumerate() {
        fields.push((format!("{name}_api_ops"), snapshot.api_ops[index]));
        fields.push((format!("{name}_api_ms"), snapshot.api_nanos[index] / MS));
    }
    fields.push(("limiter_wait_ms".to_owned(), snapshot.limiter_wait / MS));
    fields.push(("embedded_chunks".to_owned(), wait.embedded_chunks as u64));
    fields.push(("skipped_chunks".to_owned(), wait.skipped_chunks as u64));
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn requests_count_under_the_current_phase_and_only_inside_a_scope() {
        let timings = PageTimings::default();
        assert!(current().is_none());
        timings
            .scope(ApiPhase::Walk, async {
                current().unwrap().record_api(Duration::from_millis(5));
            })
            .await;
        timings
            .scope(ApiPhase::Prepare, async {
                current().unwrap().record_api(Duration::from_millis(7));
                current().unwrap().record_api(Duration::from_millis(1));
            })
            .await;
        let snapshot = timings.snapshot();
        assert_eq!(snapshot.api_ops, [1, 2, 0]);
        assert_eq!(snapshot.api_nanos[1], 8 * MS);
    }

    #[test]
    fn owned_threads_record_through_the_handle_and_restore_afterward() {
        let timings = PageTimings::default();
        with_thread(Some(timings.clone()), || {
            let _timer = timer(Bucket::Receipt);
            std::thread::sleep(Duration::from_millis(2));
        });
        assert!(current().is_none());
        assert!(timings.snapshot().receipt >= 2 * MS);
    }

    #[test]
    fn parts_and_the_remainder_sum_to_the_wall_time_exactly() {
        let timings = PageTimings::default();
        timings.add(Bucket::Embed, Duration::from_micros(1_900));
        timings.add(Bucket::LanceMerge, Duration::from_micros(2_900));
        timings.add(Bucket::Receipt, Duration::from_micros(3_900));
        let wait = PageWait {
            wall: Duration::from_micros(40_100),
            consumer_wait: Duration::from_micros(9_900),
            queue_wait: Duration::from_millis(3),
            embedded_chunks: 2,
            skipped_chunks: 5,
        };
        let fields: std::collections::BTreeMap<_, _> = page_fields(&timings.snapshot(), &wait)
            .into_iter()
            .collect();
        let parts = [
            "consumer_wait_ms",
            "embed_ms",
            "lance_merge_ms",
            "fts_optimize_ms",
            "vector_optimize_ms",
            "receipt_ms",
            "other_ms",
        ];
        assert_eq!(
            parts.iter().map(|k| fields[*k]).sum::<u64>(),
            fields["wall_ms"]
        );
        assert_eq!(fields["wall_ms"], 40);
        assert_eq!(fields["queue_wait_ms"], 3);
        assert_eq!(
            (fields["embedded_chunks"], fields["skipped_chunks"]),
            (2, 5)
        );
    }
}
