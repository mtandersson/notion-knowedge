//! Durable, payload-free SQLite audit trail for agent mutations.
//!
//! Separate storage avoids changing the existing operational-state migration
//! history. Only the allowlisted core audit fields can be persisted.
use std::{
    fs,
    path::Path,
    sync::Mutex,
    time::Duration,
};

use notion_knowledge_core::audit::{AuditError, AuditEvent, AuditSink};
use rusqlite::{Connection, TransactionBehavior, params};

/// Bounded retention; the caller supplies a trusted monotonically increasing
/// wall-clock Unix timestamp so expiration is deterministic in tests.
pub struct SqliteAuditStore {
    connection: Mutex<Connection>,
    retention_seconds: i64,
}

impl SqliteAuditStore {
    pub fn open(path: impl AsRef<Path>, retention_days: u32) -> Result<Self, AuditError> {
        Self::validate_retention(retention_days)?;
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|_| AuditError::Unavailable)?;
        }
        // The parent directory must already be protected by the operator:
        // SQLite can create journal / WAL sidecar files next to the database.
        let connection = Connection::open(path).map_err(|_| AuditError::Unavailable)?;
        Self::initialize(connection, retention_days)
    }

    pub fn open_in_memory(retention_days: u32) -> Result<Self, AuditError> {
        Self::validate_retention(retention_days)?;
        let connection = Connection::open_in_memory().map_err(|_| AuditError::Unavailable)?;
        Self::initialize(connection, retention_days)
    }

    fn validate_retention(days: u32) -> Result<(), AuditError> {
        if !(1..=3650).contains(&days) {
            return Err(AuditError::InvalidMetadata);
        }
        Ok(())
    }

    fn initialize(connection: Connection, days: u32) -> Result<Self, AuditError> {
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|_| AuditError::Unavailable)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS agent_mutation_audit (
                    id INTEGER PRIMARY KEY,
                    happened_at INTEGER NOT NULL,
                    actor_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    file_size_bytes INTEGER,
                    file_mime TEXT
                );
                CREATE INDEX IF NOT EXISTS agent_mutation_audit_timestamp
                  ON agent_mutation_audit(happened_at);",
            )
            .map_err(|_| AuditError::Unavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
            retention_seconds: i64::from(days) * 86_400,
        })
    }

    /// Read-only operational count; does not expose any private identifiers.
    pub fn count(&self) -> Result<u64, AuditError> {
        let connection = self.connection.lock().map_err(|_| AuditError::Unavailable)?;
        connection
            .query_row("SELECT COUNT(*) FROM agent_mutation_audit", [], |row| row.get(0))
            .map_err(|_| AuditError::Unavailable)
    }

    /// Expire old events even if no new mutations arrive. Use a trusted clock.
    pub fn prune(&self, now_unix_seconds: i64) -> Result<usize, AuditError> {
        if now_unix_seconds < 0 {
            return Err(AuditError::InvalidMetadata);
        }
        let connection = self.connection.lock().map_err(|_| AuditError::Unavailable)?;
        let cutoff = now_unix_seconds.saturating_sub(self.retention_seconds);
        connection
            .execute(
                "DELETE FROM agent_mutation_audit WHERE happened_at < ?1",
                params![cutoff],
            )
            .map_err(|_| AuditError::Unavailable)
    }
}

impl AuditSink for SqliteAuditStore {
    fn append(&self, event: &AuditEvent, now_unix_seconds: i64) -> Result<(), AuditError> {
        if now_unix_seconds < 0
            || event.at_unix_seconds > now_unix_seconds.saturating_add(300)
        {
            return Err(AuditError::InvalidMetadata);
        }
        let file_size = event
            .file
            .as_ref()
            .map(|file| i64::try_from(file.bytes).map_err(|_| AuditError::InvalidMetadata))
            .transpose()?;
        let file_mime = event.file.as_ref().map(|file| file.mime_type.as_str());
        let mut connection = self.connection.lock().map_err(|_| AuditError::Unavailable)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| AuditError::Unavailable)?;
        tx.execute(
            "INSERT INTO agent_mutation_audit
               (happened_at, actor_id, action, target_id, outcome, correlation_id,
                file_size_bytes, file_mime)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.at_unix_seconds,
                event.actor.as_str(),
                event.action.as_str(),
                event.target.as_str(),
                event.outcome.as_str(),
                event.correlation_id.as_str(),
                file_size,
                file_mime
            ],
        )
        .map_err(|_| AuditError::Unavailable)?;
        tx.execute(
            "DELETE FROM agent_mutation_audit WHERE happened_at < ?1",
            params![now_unix_seconds.saturating_sub(self.retention_seconds)],
        )
        .map_err(|_| AuditError::Unavailable)?;
        tx.commit().map_err(|_| AuditError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::audit::{
        AuditAction, AuditOutcome, FileAuditMetadata,
    };

    fn event(at: i64) -> AuditEvent {
        AuditEvent::new(
            at,
            "agent-1",
            AuditAction::PageAppend,
            "page-1",
            AuditOutcome::Applied,
            "correlation-1",
            None,
        )
        .unwrap()
    }

    #[test]
    fn retention_prunes_old_records_and_preserves_newer_records() {
        let store = SqliteAuditStore::open_in_memory(1).unwrap();
        store.append(&event(1), 1).unwrap();
        store.append(&event(86_401), 86_401).unwrap();
        assert_eq!(store.count().unwrap(), 2);
        store.append(&event(86_402), 86_402).unwrap();
        assert_eq!(store.count().unwrap(), 2);
        assert_eq!(store.prune(172_803).unwrap(), 2);
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn file_metadata_is_bounded_and_no_payload_columns_exist() {
        let store = SqliteAuditStore::open_in_memory(7).unwrap();
        let metadata = FileAuditMetadata::new(42, "image/png").unwrap();
        let event = AuditEvent::new(
            12,
            "agent-1",
            AuditAction::FileAttach,
            "file-1",
            AuditOutcome::Applied,
            "corr-1",
            Some(metadata),
        )
        .unwrap();
        store.append(&event, 12).unwrap();
        let conn = store.connection.lock().unwrap();
        let (bytes, mime): (i64, String) = conn.query_row(
            "SELECT file_size_bytes, file_mime FROM agent_mutation_audit",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!((bytes, mime.as_str()), (42, "image/png"));
        let mut schema = conn.prepare("PRAGMA table_info(agent_mutation_audit)").unwrap();
        let names = schema.query_map([], |row| row.get::<_, String>(1)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        for forbidden in ["body", "text", "content", "token", "url", "filename", "path"] {
            assert!(!names.iter().any(|name| name == forbidden));
        }
    }

    #[test]
    fn audit_failure_is_visible_without_leaking_sql_errors() {
        let store = SqliteAuditStore::open_in_memory(1).unwrap();
        store.connection.lock().unwrap()
            .execute_batch("DROP TABLE agent_mutation_audit").unwrap();
        let error = store.append(&event(1), 1).unwrap_err();
        assert_eq!(error, AuditError::Unavailable);
        assert_eq!(error.to_string(), "audit store unavailable");
        assert!(SqliteAuditStore::open_in_memory(0).is_err());
    }
}
