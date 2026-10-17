//! Shared zero-queue, weighted budget for every installed MCP tool call.
//!
//! Owned guards cover the *whole* handler future, including root authorization,
//! retrieval, Notion requests and upload receipt completion. The budget is
//! transport-independent: stdio and HTTP handlers are clones of one server.
use notion_knowledge_core::metrics::{Resource, ResourceLease};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const DEFAULT_CAPACITY: usize = 16;
pub(crate) const MAX_CAPACITY: usize = 128;

#[derive(Debug)]
pub(crate) struct ToolBudget {
    permits: Arc<Semaphore>,
    capacity: u32,
}

pub(crate) struct ToolPermit {
    _metric: ResourceLease,
    _permit: OwnedSemaphorePermit,
}

impl ToolBudget {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!((1..=MAX_CAPACITY).contains(&capacity));
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
            capacity: capacity as u32,
        }
    }

    fn weight(tool: &str) -> u32 {
        match tool {
            "knowledge_sync_status" => 1,
            "knowledge_get" => 2,
            "knowledge_related" => 3,
            "knowledge_search" => 4,
            // Uploads and prospective mutation tools must never be timed out
            // or retried by this budget: admission is a *pre-effect* decision.
            "knowledge_upload_file" => 8,
            _ => 8,
        }
    }

    pub(crate) fn try_acquire(&self, tool: &str) -> Option<ToolPermit> {
        let units = Self::weight(tool).min(self.capacity);
        match self.permits.clone().try_acquire_many_owned(units) {
            Ok(permit) => Some(ToolPermit {
                _permit: permit,
                _metric: Resource::ToolWork.enter(units.into()),
            }),
            Err(_) => {
                Resource::ToolWork.reject();
                None
            }
        }
    }

    #[cfg(test)]
    fn available(&self) -> usize {
        self.permits.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_share_one_finite_weighted_pool() {
        let budget = ToolBudget::new(8);
        let search = budget.try_acquire("knowledge_search").unwrap();
        assert_eq!(budget.available(), 4);
        let related = budget.try_acquire("knowledge_related").unwrap();
        let status = budget.try_acquire("knowledge_sync_status").unwrap();
        assert_eq!(budget.available(), 0);
        assert!(budget.try_acquire("knowledge_upload_file").is_none());
        assert!(budget.try_acquire("knowledge_search").is_none());
        drop(status);
        assert!(budget.try_acquire("knowledge_sync_status").is_some());
        drop(search);
        drop(related);
        assert_eq!(budget.available(), 8);
        let upload = budget.try_acquire("knowledge_upload_file").unwrap();
        assert_eq!(budget.available(), 0);
        drop(upload);
        assert_eq!(budget.available(), 8);
    }

    #[tokio::test]
    async fn failed_admission_never_queues_or_starts_a_second_effect() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let budget = Arc::new(ToolBudget::new(2));
        let starts = Arc::new(AtomicUsize::new(0));
        let (release, proceed) = tokio::sync::oneshot::channel::<()>();
        let first_budget = budget.clone();
        let started = starts.clone();
        let (ready, receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _guard = first_budget.try_acquire("knowledge_search").unwrap();
            started.fetch_add(1, Ordering::SeqCst);
            ready.send(()).unwrap();
            let _ = proceed.await;
        });
        receiver.await.unwrap();
        assert!(budget.try_acquire("knowledge_get").is_none());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        task.await.unwrap();
        assert!(budget.try_acquire("knowledge_get").is_some());
    }

    #[test]
    fn cancellation_releases_read_permits_without_retries() {
        let budget = ToolBudget::new(1);
        let guard = budget.try_acquire("knowledge_upload_file").unwrap();
        assert!(budget.try_acquire("knowledge_sync_status").is_none());
        // Dropping the handler future drops its owned guard. No operation is
        // retried or timeout-cancelled by the admission layer.
        drop(guard);
        assert_eq!(budget.available(), 1);
        assert!(budget.try_acquire("knowledge_upload_file").is_some());
    }
}
