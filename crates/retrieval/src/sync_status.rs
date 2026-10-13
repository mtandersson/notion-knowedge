//! Privacy-limited reads of persisted current state. No retry/lease/migration effects.
use crate::sync_state::SqliteSyncStateStore;
use notion_knowledge_core::{sync_state::SyncStateError, sync_status::WebhookCounts};
use rusqlite::params;
impl SqliteSyncStateStore {
    pub fn scoped_webhook_counts(
        &self,
        workspace: &str,
        subscription: &str,
        integration: &str,
    ) -> Result<WebhookCounts, SyncStateError> {
        let db = self.lock_connection()?;
        db.query_row("SELECT COALESCE(SUM(state=0),0),COALESCE(SUM(state=1),0),COALESCE(SUM(state=3),0) FROM webhook_inbox WHERE workspace_id=?1 AND subscription_id=?2 AND integration_id=?3",params![workspace.to_ascii_lowercase(),subscription.to_ascii_lowercase(),integration.to_ascii_lowercase()],|r|Ok(WebhookCounts{pending:r.get::<_,i64>(0)? as u64,running:r.get::<_,i64>(1)? as u64,failed:r.get::<_,i64>(2)? as u64})).map_err(|_|SyncStateError::Unavailable)
    }
}
#[cfg(unix)]
impl crate::commit::IndexCommitCoordinator {
    pub fn last_successful_reconciliation(
        &self,
    ) -> Result<Option<i64>, crate::commit::CommitError> {
        self.identities()?;
        let db = self
            .state()
            .lock_connection()
            .map_err(|_| crate::commit::CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        let b = self.binding();
        db.query_row("SELECT MAX(completed_at_seconds) FROM reconciliation_success s JOIN reconciliation_runs r USING(run_id) WHERE s.table_name=?1 AND s.workspace=?2 AND s.scope=?3 AND s.generation=?4 AND s.epoch=?5 AND r.phase=2 AND r.abandoned=0 AND r.scope=s.scope AND r.failure IS NULL",params![b.table(),b.workspace(),b.scope(),b.generation(),b.epoch() as i64],|r|r.get(0)).map_err(|_|crate::commit::CommitError::Unavailable)
    }
}
