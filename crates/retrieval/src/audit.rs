//! Separate, durable and intentionally narrow SQLite audit trail.
//! The audit database must live in an operator-restricted directory. It never
//! accepts arbitrary message text, URLs, request bodies or file contents.

use std::{fs, path::Path, sync::Mutex, time::Duration};

use notion_knowledge_core::audit::{AuditError, AuditEvent, AuditRetention, AuditStore};
use rusqlite::{Connection, TransactionBehavior, params};

pub struct SqliteAuditStore {
    connection: Mutex<Connection>,
    retention: AuditRetention,
}

impl SqliteAuditStore {
    /// Use a separate protected state file, not the disposable search index.
    /// A failed open or schema initialization never silently disables audit.
    pub fn open(path: impl AsRef<Path>, retention: AuditRetention) -> Result<Self, AuditError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|_| AuditError::Unavailable)?;
        }
        #[cfg(unix)]
        {
            use std::{io::ErrorKind, os::unix::fs::OpenOptionsExt};
            // SQLite's default CREATE mode can briefly be readable before a
            // subsequent chmod. Pre-create privately, even under a permissive
            // process umask. Reject pre-existing symlinks; the operator-private
            // parent directory must prevent path replacement during open.
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
            {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    if !fs::symlink_metadata(path)
                        .is_ok_and(|metadata| metadata.file_type().is_file())
                    {
                        return Err(AuditError::Unavailable);
                    }
                }
                Err(_) => return Err(AuditError::Unavailable),
            }
        }
        let connection = Connection::open(path).map_err(|_| AuditError::Unavailable)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .map_err(|_| AuditError::Unavailable)?;
        }
        Self::from_connection(connection, retention)
    }

    pub fn open_in_memory(retention: AuditRetention) -> Result<Self, AuditError> {
        Self::from_connection(
            Connection::open_in_memory().map_err(|_| AuditError::Unavailable)?,
            retention,
        )
    }

    fn from_connection(
        connection: Connection,
        retention: AuditRetention,
    ) -> Result<Self, AuditError> {
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|_| AuditError::Unavailable)?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys=ON;
                 CREATE TABLE IF NOT EXISTS agent_audit_events (
                     id INTEGER PRIMARY KEY,
                     occurred_at_unix INTEGER NOT NULL CHECK(occurred_at_unix >= 0),
                     actor TEXT NOT NULL CHECK(actor IN ('approved_user','automation','server')),
                     tool TEXT NOT NULL CHECK(tool IN (
                         'page_create','page_append','page_replace','page_delete','page_move','file_attach')),
                     target_page_id TEXT,
                     outcome TEXT NOT NULL CHECK(outcome IN ('attempted','succeeded','denied','failed','indeterminate')),
                     correlation_id TEXT NOT NULL,
                     file_kind TEXT CHECK(file_kind IS NULL OR file_kind IN ('image','other')),
                     file_size_bytes INTEGER CHECK(file_size_bytes IS NULL OR file_size_bytes >= 0),
                     CHECK ((file_kind IS NULL) = (file_size_bytes IS NULL)),
                     CHECK (file_kind IS NULL OR tool = 'file_attach')
                 );
                 CREATE INDEX IF NOT EXISTS agent_audit_events_retention
                   ON agent_audit_events(occurred_at_unix);",
            )
            .map_err(|_| AuditError::Unavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
            retention,
        })
    }
}

impl AuditStore for SqliteAuditStore {
    fn record(&self, event: &AuditEvent, trusted_now_unix: i64) -> Result<(), AuditError> {
        let cutoff = self.retention.cutoff(trusted_now_unix)?;
        if event.occurred_at_unix() <= cutoff || event.occurred_at_unix() > trusted_now_unix {
            return Err(AuditError::InvalidInput);
        }
        let file_size = event
            .file()
            .map(|file| i64::try_from(file.size_bytes()).map_err(|_| AuditError::InvalidInput))
            .transpose()?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| AuditError::Unavailable)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| AuditError::Unavailable)?;
        tx.execute(
            "DELETE FROM agent_audit_events WHERE occurred_at_unix <= ?1",
            params![cutoff],
        )
        .map_err(|_| AuditError::Unavailable)?;
        tx.execute(
            "INSERT INTO agent_audit_events
               (occurred_at_unix, actor, tool, target_page_id, outcome, correlation_id,
                file_kind, file_size_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.occurred_at_unix(),
                event.actor().as_str(),
                event.tool().as_str(),
                event.target_page_id(),
                event.outcome().as_str(),
                event.correlation_id(),
                event.file().map(|file| file.kind().as_str()),
                file_size
            ],
        )
        .map_err(|_| AuditError::Unavailable)?;
        tx.commit().map_err(|_| AuditError::Unavailable)?;
        Ok(())
    }

    fn prune(&self, trusted_now_unix: i64) -> Result<u64, AuditError> {
        let cutoff = self.retention.cutoff(trusted_now_unix)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuditError::Unavailable)?;
        let count = connection
            .execute(
                "DELETE FROM agent_audit_events WHERE occurred_at_unix <= ?1",
                params![cutoff],
            )
            .map_err(|_| AuditError::Unavailable)?;
        u64::try_from(count).map_err(|_| AuditError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::audit::{
        AuditActor, AuditOutcome, AuditTool, FileKind, SafeFileFacts,
    };

    fn event(when: i64) -> AuditEvent {
        AuditEvent::new(
            when,
            AuditActor::ApprovedUser,
            AuditTool::FileAttach,
            Some("01234567-89ab-cdef-0123-456789abcdef".into()),
            AuditOutcome::Indeterminate,
            "request_0123456789abcdef".into(),
            Some(SafeFileFacts::new(FileKind::Image, 1024)),
        )
        .unwrap()
    }

    fn count(store: &SqliteAuditStore) -> i64 {
        store
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM agent_audit_events", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    #[test]
    fn append_and_prune_use_one_durable_boundary() {
        let store = SqliteAuditStore::open_in_memory(AuditRetention::days(1).unwrap()).unwrap();
        store.record(&event(100_000), 100_100).unwrap();
        assert_eq!(count(&store), 1);
        store.record(&event(200_000), 200_001).unwrap();
        assert_eq!(count(&store), 1); // previous event expired during append
        assert_eq!(store.prune(300_000), Ok(1));
        assert_eq!(count(&store), 0);
        assert_eq!(
            store.record(&event(10), 300_000),
            Err(AuditError::InvalidInput)
        );
        assert_eq!(count(&store), 0);
    }

    #[test]
    fn failed_append_rolls_back_retention_and_reports_only_a_safe_error() {
        let store = SqliteAuditStore::open_in_memory(AuditRetention::days(1).unwrap()).unwrap();
        store.record(&event(100_000), 100_001).unwrap();
        store
            .connection
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_audit BEFORE INSERT ON agent_audit_events
             BEGIN SELECT RAISE(ABORT, 'private-upstream-sentinel'); END;",
            )
            .unwrap();
        let error = store.record(&event(200_000), 200_001).unwrap_err();
        assert_eq!(error, AuditError::Unavailable);
        assert!(!format!("{error:?} {error}").contains("private-upstream-sentinel"));
        assert_eq!(
            count(&store),
            1,
            "failed append must roll back its retention deletion"
        );
        store
            .connection
            .lock()
            .unwrap()
            .execute_batch("PRAGMA query_only=ON;")
            .unwrap();
        assert_eq!(store.prune(200_001), Err(AuditError::Unavailable));
        assert_eq!(
            count(&store),
            1,
            "failed pruning must preserve the existing record"
        );
    }

    #[test]
    fn never_record_unauditable_or_future_events() {
        let store = SqliteAuditStore::open_in_memory(AuditRetention::default()).unwrap();
        assert_eq!(
            store.record(&event(500), 499),
            Err(AuditError::InvalidInput)
        );
        let huge = AuditEvent::new(
            500,
            AuditActor::Server,
            AuditTool::FileAttach,
            None,
            AuditOutcome::Failed,
            "request_0123456789abcdef".into(),
            Some(SafeFileFacts::new(FileKind::Other, u64::MAX)),
        )
        .unwrap();
        assert_eq!(store.record(&huge, 501), Err(AuditError::InvalidInput));
        assert_eq!(count(&store), 0);
    }

    #[test]
    fn persists_across_reopen_without_private_payload_columns() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("nk-audit-{}-{nonce}.sqlite", std::process::id()));
        {
            let store = SqliteAuditStore::open(&path, AuditRetention::default()).unwrap();
            store.record(&event(100), 101).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
        {
            let store = SqliteAuditStore::open(&path, AuditRetention::default()).unwrap();
            assert_eq!(count(&store), 1);
            let names: String = store
                .connection
                .lock()
                .unwrap()
                .prepare("SELECT name FROM pragma_table_info('agent_audit_events') ORDER BY cid")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .map(|item| item.unwrap())
                .collect::<Vec<_>>()
                .join(",");
            assert!(!names.contains("body"));
            assert!(!names.contains("url"));
            assert!(!names.contains("filename"));
            assert!(!names.contains("token"));
        }
        let _ = fs::remove_file(path);
    }
}
