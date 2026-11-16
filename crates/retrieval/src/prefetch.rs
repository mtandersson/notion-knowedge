//! Shared retained-byte budget for bounded source read-ahead.
use std::sync::Arc;

/// One retained-byte budget for read-ahead. A preparation reserves the
/// worst case before its first request, then keeps only its measured share
/// while queued and committing; dropping the permit returns the bytes.
#[derive(Clone)]
pub struct PrefetchBudget {
    kib: Arc<tokio::sync::Semaphore>,
    reserve_kib: usize,
}
impl PrefetchBudget {
    pub fn new(total_kib: usize, reserve_kib: usize) -> Self {
        assert!(reserve_kib > 0 && reserve_kib <= total_kib);
        Self {
            kib: Arc::new(tokio::sync::Semaphore::new(total_kib)),
            reserve_kib,
        }
    }
    pub async fn reserve(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.kib
            .clone()
            .acquire_many_owned(u32::try_from(self.reserve_kib).ok()?)
            .await
            .ok()
    }
    /// Shrink a reservation to a measured size, or `None` if it exceeds it.
    pub fn retain(
        &self,
        mut reservation: tokio::sync::OwnedSemaphorePermit,
        bytes: usize,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let kib = bytes.div_ceil(1024).max(1);
        if kib > self.reserve_kib {
            return None;
        }
        drop(reservation.split(self.reserve_kib - kib));
        Some(reservation)
    }
}
