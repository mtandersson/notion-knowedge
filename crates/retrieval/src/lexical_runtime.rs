//! Trusted configured expansion. Callers cannot select assets or policies.
use crate::chunks::{
    ChunkTableError, GuardedChunkTable, LanceChunkTable, LanceSemanticSearch,
    bounded::{CandidateBudget, CandidateReservation},
    lexical_binding::LexicalCachePlan,
};
use notion_knowledge_core::{
    embedding::EmbeddingProvider,
    hybrid::{HybridFusion, ReciprocalRankFusion},
    search::{
        HybridSearch, LexicalQuery, LexicalSearch, SearchFuture, SearchFutureWithFailure,
        SearchHit, SearchUnavailable, SemanticQuery, SemanticSearch,
    },
};
use std::{future::Future, sync::Arc, time::Duration};
type SharedReservation = Arc<tokio::sync::Mutex<CandidateReservation>>;

/// One immutable verified plan and one process-local candidate pool, shared by
/// lexical and hybrid calls. Default-off serving keeps its existing adapters.
pub struct LexicalSearchRuntime {
    table: GuardedChunkTable,
    provider: Arc<dyn EmbeddingProvider>,
    plan: LexicalCachePlan,
    pool: CandidateBudget,
    timeout: Duration,
}
impl LexicalSearchRuntime {
    pub fn new(
        table: GuardedChunkTable,
        provider: Arc<dyn EmbeddingProvider>,
        plan: LexicalCachePlan,
        pool: CandidateBudget,
        timeout: Duration,
    ) -> Result<Self, ChunkTableError> {
        table.require_lexical_plan(&plan)?;
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(ChunkTableError::InvalidRows(
                "invalid search deadline".into(),
            ));
        }
        Ok(Self {
            table,
            provider,
            plan,
            pool,
            timeout,
        })
    }

    pub fn plan(&self) -> &LexicalCachePlan {
        &self.plan
    }

    async fn run<F, Fut, E>(&self, operation: F) -> Result<Vec<SearchHit>, E>
    where
        F: FnOnce(SharedReservation, tokio::time::Instant) -> Fut,
        Fut: Future<Output = Result<Vec<SearchHit>, E>>,
        E: From<SearchUnavailable>,
    {
        // Admit before holding the read fence or waiting on model execution.
        // Keep the same reservation alive through native and RRF merging.
        let reservation = Arc::new(tokio::sync::Mutex::new(
            self.pool.try_reserve().map_err(|_| SearchUnavailable)?,
        ));
        let shared = reservation.clone();
        let deadline = tokio::time::Instant::now() + self.timeout;
        let result = tokio::time::timeout_at(deadline, Box::pin(operation(shared, deadline)))
            .await
            .map_err(|_| SearchUnavailable)?;
        drop(reservation);
        result
    }

    fn lexical(
        &self,
        snapshot: Arc<LanceChunkTable>,
        reservation: SharedReservation,
    ) -> SnapshotLexical {
        SnapshotLexical {
            snapshot,
            plan: self.plan.clone(),
            reservation,
        }
    }
}

struct SnapshotLexical {
    snapshot: Arc<LanceChunkTable>,
    plan: LexicalCachePlan,
    reservation: SharedReservation,
}
impl LexicalSearch for SnapshotLexical {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            let mut reservation = self.reservation.lock().await;
            let result = self
                .snapshot
                .expanded_lexical_reserved(query, self.plan.dictionary(), &mut reservation)
                .await
                .map_err(|_| SearchUnavailable)?;
            result
                .candidates
                .into_iter()
                .map(|candidate| candidate.into_search_hit())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| SearchUnavailable)
        })
    }
}

impl SemanticSearch for LexicalSearchRuntime {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            SemanticSearch::search_with_failure(self, query)
                .await
                .map_err(|_| SearchUnavailable)
        })
    }

    fn search_with_failure(&self, query: SemanticQuery) -> SearchFutureWithFailure<'_> {
        Box::pin(async move {
            self.run(|_reservation, deadline| {
                self.table
                    .search_with_snapshots(move |snapshot, canonical| async move {
                        LanceSemanticSearch::new(Arc::new(snapshot), self.provider.clone(), 1)
                            .map_err(|_| SearchUnavailable)?
                            .with_embedding_deadline(deadline)
                            .with_canonical_text(Arc::new(canonical))
                            .search_with_failure(query)
                            .await
                    })
            })
            .await
        })
    }
}
impl LexicalSearch for LexicalSearchRuntime {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            self.run(|reservation, _deadline| {
                self.table
                    .search_with_snapshot(false, |snapshot| async move {
                        self.lexical(Arc::new(snapshot), reservation)
                            .search(query)
                            .await
                    })
            })
            .await
        })
    }
}
impl HybridSearch for LexicalSearchRuntime {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            HybridSearch::search_with_failure(self, query)
                .await
                .map_err(|_| SearchUnavailable)
        })
    }

    fn search_with_failure(&self, query: SemanticQuery) -> SearchFutureWithFailure<'_> {
        Box::pin(async move {
            self.run(|reservation, deadline| {
                self.table
                    .search_with_snapshots(move |semantic, lexical| async move {
                        let lexical = Arc::new(lexical);
                        let semantic = Arc::new(
                            LanceSemanticSearch::new(Arc::new(semantic), self.provider.clone(), 1)
                                .map_err(|_| SearchUnavailable)?
                                .with_embedding_deadline(deadline)
                                .with_canonical_text(lexical.clone()),
                        );
                        let lexical = Arc::new(self.lexical(lexical, reservation));
                        HybridFusion::new(semantic, lexical, ReciprocalRankFusion::default())?
                            .search_with_failure(query)
                            .await
                    })
            })
            .await
        })
    }
}
