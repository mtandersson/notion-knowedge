//! Counts-only context shared by one startup sync's async and owned-thread work.
use super::SyncPhase;
use std::{
    cell::RefCell,
    future::Future,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Counts {
    discovered_pages: usize,
    content_reads: usize,
    embedded_chunks: usize,
    committed_pages: usize,
    skipped_pages: usize,
    total_pages: Option<usize>,
    phase: Option<SyncPhase>,
}
#[derive(Clone, Default)]
pub struct SyncContext {
    counts: Arc<Mutex<Counts>>,
    worker: Option<(SyncPhase, usize)>,
}
tokio::task_local! { static CURRENT: SyncContext; }
thread_local! { static THREAD: RefCell<Option<SyncContext>> = const { RefCell::new(None) }; }

pub async fn with_sync_progress<T>(future: impl Future<Output = T>) -> T {
    CURRENT.scope(SyncContext::default(), future).await
}
pub fn sync_context() -> Option<SyncContext> {
    let mut context = CURRENT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD.with(|c| c.borrow().clone()))?;
    if let Ok(worker) = super::SYNC_TASK.try_with(|worker| *worker) {
        context.worker = Some(worker);
    }
    Some(context)
}
/// Propagate counts to an already owned commit thread, restoring on unwind.
pub fn with_sync_context<T>(context: Option<SyncContext>, work: impl FnOnce() -> T) -> T {
    struct Reset(Option<SyncContext>);
    impl Drop for Reset {
        fn drop(&mut self) {
            THREAD.with(|c| {
                c.replace(self.0.take());
            });
        }
    }
    let _reset = Reset(THREAD.with(|c| c.replace(context)));
    work()
}
pub(super) fn thread_worker() -> Option<String> {
    THREAD.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|c| c.worker)
            .map(|(p, n)| format!("{}-{n}", p.label()))
    })
}
fn update(work: impl FnOnce(&mut Counts)) {
    if let Some(context) = sync_context()
        && let Ok(mut counts) = context.counts.lock()
    {
        work(&mut counts);
    }
}
pub enum SyncCount {
    DiscoveredPage,
    ContentRead,
    EmbeddedChunk,
    CommittedPage,
    SkippedPage,
}
pub fn sync_count(counter: SyncCount, amount: usize) {
    update(|c| {
        let field = match counter {
            SyncCount::DiscoveredPage => &mut c.discovered_pages,
            SyncCount::ContentRead => &mut c.content_reads,
            SyncCount::EmbeddedChunk => &mut c.embedded_chunks,
            SyncCount::CommittedPage => &mut c.committed_pages,
            SyncCount::SkippedPage => &mut c.skipped_pages,
        };
        *field = field.saturating_add(amount);
    });
}
pub fn sync_total_pages(total: usize) {
    update(|c| c.total_pages = Some(total));
}
pub(super) fn sync_phase(phase: SyncPhase) {
    update(|c| c.phase = Some(phase));
}
pub fn current_sync_progress() -> Option<serde_json::Value> {
    let context = sync_context()?;
    let c = context.counts.lock().ok()?;
    Some(serde_json::json!({
        "phase": context.worker.map(|w| w.0).or(c.phase).map(|p|p.label()),
        "discovered_pages": c.discovered_pages, "total_pages": c.total_pages,
        "content_reads": c.content_reads, "embedded_chunks": c.embedded_chunks,
        "committed_pages": c.committed_pages, "skipped_pages": c.skipped_pages,
    }))
}
