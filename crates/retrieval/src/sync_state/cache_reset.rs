//! Reset derived retrieval bookkeeping without discarding admitted upstream work.
use super::SqliteSyncStateStore;
use notion_knowledge_core::reconciliation::{JournalError, Lease};
use rusqlite::TransactionBehavior;

impl SqliteSyncStateStore {
    /// Called with serving stopped and the index directory exclusively locked.
    /// The caller must discard the corresponding derived rows and perform a new
    /// authoritative inventory before marking the cache ready.
    pub fn reset_retrieval_cache(&self, lease: &Lease, now: i64) -> Result<(), JournalError> {
        let mut db = self
            .lock_connection()
            .map_err(|_| JournalError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JournalError::Unavailable)?;
        crate::reconciliation::guard(&tx, lease, now)?;
        // Children precede parents. This allowlist deliberately leaves webhook
        // admission, mutation/file refresh intents, credentials and unknown
        // application tables untouched.
        tx.execute_batch(
            "
            DELETE FROM reconciliation_schedule_events;
            DELETE FROM reconciliation_schedules;
            DELETE FROM reconciliation_retry_proposals;
            DELETE FROM reconciliation_plan_progress;
            DELETE FROM reconciliation_plan_run_failures;
            DELETE FROM reconciliation_plans;
            DELETE FROM reconciliation_inventory;
            DELETE FROM reconciliation_success;
            DELETE FROM reconciliation_failure_history;
            DELETE FROM reconciliation_runs;
            DELETE FROM relation_operation_proposals;
            DELETE FROM alias_operation_proposals;
            DELETE FROM page_relation_projections;
            DELETE FROM page_alias_projections;
            DELETE FROM page_metadata_identity;
            DELETE FROM graph_edges;
            DELETE FROM graph_aliases;
            DELETE FROM index_commit_receipts;
            DELETE FROM index_commit_binding;
            DELETE FROM index_commit_generations;
            DELETE FROM page_sync_state;
            DELETE FROM crawl_checkpoints;
            DELETE FROM index_versions;
            INSERT INTO index_versions(index_name,version) VALUES('cache-rebuild','pending');
        ",
        )
        .map_err(|_| JournalError::Unavailable)?;
        tx.commit().map_err(|_| JournalError::Unavailable)
    }

    pub fn cache_rebuild_pending(&self) -> Result<bool, JournalError> {
        self.lock_connection().map_err(|_| JournalError::Unavailable)?
            .query_row("SELECT EXISTS(SELECT 1 FROM index_versions WHERE index_name='cache-rebuild' AND version='pending')", [], |row| row.get(0))
            .map_err(|_| JournalError::Unavailable)
    }

    /// Completion is recorded only after a successful fresh inventory and all
    /// its page refreshes. This is a sync flag, not a cache handoff journal.
    pub fn finish_cache_rebuild(&self, lease: &Lease, now: i64) -> Result<(), JournalError> {
        let mut db = self
            .lock_connection()
            .map_err(|_| JournalError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JournalError::Unavailable)?;
        crate::reconciliation::guard(&tx, lease, now)?;
        tx.execute(
            "DELETE FROM index_versions WHERE index_name='cache-rebuild'",
            [],
        )
        .map_err(|_| JournalError::Unavailable)?;
        tx.commit().map_err(|_| JournalError::Unavailable)
    }
}
