//! Bounded local observations shared by tools and HTTP diagnostics.
use notion_knowledge_core::health::{DependencyState, HealthProbe};
use notion_knowledge_retrieval::chunks::GuardedChunkTable;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const INTERVAL: Duration = Duration::from_secs(30);
const TIMEOUT: Duration = Duration::from_secs(5);
const STALE_AFTER: Duration = Duration::from_secs(90);

struct Snapshot {
    latest: Mutex<(DependencyState, Instant)>,
}

impl Snapshot {
    fn state_at(&self, now: Instant) -> DependencyState {
        match self.latest.lock() {
            Ok(latest) if now.saturating_duration_since(latest.1) <= STALE_AFTER => latest.0,
            _ => DependencyState::Unknown,
        }
    }
}

pub(crate) struct IndexReadiness {
    snapshot: Arc<Snapshot>,
    observer: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    table: GuardedChunkTable,
    #[cfg(test)]
    exact_id: bool,
}

impl Drop for IndexReadiness {
    fn drop(&mut self) {
        self.observer.abort();
    }
}

impl HealthProbe for IndexReadiness {
    fn state(&self) -> DependencyState {
        self.snapshot.state_at(Instant::now())
    }
}

async fn refresh(snapshot: &Snapshot, table: &GuardedChunkTable, exact_id: bool) {
    let check = async {
        table.search_snapshot().await?;
        if exact_id {
            table.probe_lexical_cache().await?;
        }
        Ok::<_, notion_knowledge_retrieval::chunks::ChunkTableError>(())
    };
    let state = match tokio::time::timeout(TIMEOUT, check).await {
        Ok(Ok(())) => DependencyState::Healthy,
        _ => DependencyState::Unavailable,
    };
    if let Ok(mut latest) = snapshot.latest.lock() {
        *latest = (state, Instant::now());
    }
}

#[cfg(test)]
impl IndexReadiness {
    pub(crate) async fn check_now(&self) {
        refresh(&self.snapshot, &self.table, self.exact_id).await;
    }
}

pub(crate) fn observe(table: GuardedChunkTable, exact_id: bool) -> Arc<IndexReadiness> {
    // Composition has already opened and validated this exact index.
    let snapshot = Arc::new(Snapshot {
        latest: Mutex::new((DependencyState::Healthy, Instant::now())),
    });
    let latest = snapshot.clone();
    #[cfg(test)]
    let test_table = table.clone();
    let observer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            refresh(&latest, &table, exact_id).await;
        }
    });
    Arc::new(IndexReadiness {
        snapshot,
        observer,
        #[cfg(test)]
        table: test_table,
        #[cfg(test)]
        exact_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_expire_without_waiting_or_reusing_old_health() {
        let checked = Instant::now();
        for state in [DependencyState::Healthy, DependencyState::Unavailable] {
            let snapshot = Snapshot {
                latest: Mutex::new((state, checked)),
            };
            assert_eq!(snapshot.state_at(checked), state);
            assert_eq!(
                snapshot.state_at(checked + STALE_AFTER + Duration::from_nanos(1)),
                DependencyState::Unknown
            );
        }
    }
}
