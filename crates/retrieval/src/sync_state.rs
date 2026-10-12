//! SQLite implementation of the core operational-state port.

use std::{
    fs,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use notion_knowledge_core::sync_state::{
    CrawlCheckpoint, IndexVersion, PageSyncState, SyncStateError, SyncStateStore,
    validate_identifier,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

pub const LATEST_SCHEMA_VERSION: i64 = 11;

const MIGRATIONS: &[(i64, &str, &str)] = &[
    (
        1,
        "initial_sync_state",
        include_str!("../migrations/0001_initial.sql"),
    ),
    (
        2,
        "reconciliation_journal",
        include_str!("../migrations/0002_reconciliation.sql"),
    ),
    (
        3,
        "webhook_inbox",
        include_str!("../migrations/0003_webhook_inbox.sql"),
    ),
    (
        4,
        "webhook_recovery",
        include_str!("../migrations/0004_webhook_recovery.sql"),
    ),
    (
        5,
        "webhook_debounce",
        include_str!("../migrations/0005_webhook_debounce.sql"),
    ),
    (
        6,
        "index_commit_coordinator",
        include_str!("../migrations/0006_index_commit.sql"),
    ),
    (
        7,
        "knowledge_graph_edges",
        include_str!("../migrations/0007_graph_edges.sql"),
    ),
    (
        8,
        "knowledge_graph_aliases",
        include_str!("../migrations/0008_graph_aliases.sql"),
    ),
    (
        9,
        "page_metadata_identity",
        include_str!("../migrations/0009_page_metadata_identity.sql"),
    ),
    (
        10,
        "authoritative_reconciliation_plan",
        include_str!("../migrations/0010_reconciliation_plan.sql"),
    ),
    (
        11,
        "reconciliation_retry",
        include_str!("../migrations/0011_reconciliation_retry.sql"),
    ),
];

/// A read-only snapshot of durable, identity-free operational state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalMetrics {
    /// Pending and running inbox events, including leased work.
    pub webhook_outstanding: u64,
    pub oldest_webhook_age_seconds: Option<u64>,
    /// Age of the newest successfully persisted page/index-version update.
    pub index_change_age_seconds: Option<u64>,
}

/// One process-local connection guarded by a mutex. SQLite transactions provide
/// the durable atomicity boundary; the mutex prevents concurrent use of one
/// connection object while still allowing callers to share the adapter.
pub struct SqliteSyncStateStore {
    connection: Mutex<Connection>,
}

impl SqliteSyncStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SyncStateError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|_| SyncStateError::Unavailable)?;
        }

        let connection = Connection::open(path).map_err(map_sqlite_error)?;
        Self::from_connection(connection)
    }

    /// Inspect an existing current-schema database without creation or migration.
    /// SQLite enforces read-only access even if a caller reaches a mutation API.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, SyncStateError> {
        Self::open_current(path.as_ref(), true)
    }
    /// Open an existing current-schema writer without creation or migration.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, SyncStateError> {
        Self::open_current(path.as_ref(), false)
    }
    fn open_current(path: &Path, read_only: bool) -> Result<Self, SyncStateError> {
        if read_only {
            // A SQLite READ_ONLY query may still create WAL/SHM auxiliaries.
            // Production uses the default rollback journal. Refuse WAL (or
            // invalid headers) BEFORE opening SQLite; immutable mode would
            // incorrectly hide authoritative live WAL data.
            use std::io::Read;
            let mut header = [0u8; 20];
            fs::File::open(path)
                .and_then(|mut file| file.read_exact(&mut header))
                .map_err(|_| SyncStateError::Unavailable)?;
            if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
                return Err(SyncStateError::UnsupportedSchema);
            }
        }
        let connection = Connection::open_with_flags(
            path,
            if read_only {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            } else {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            },
        )
        .map_err(map_sqlite_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(map_sqlite_error)?;
        connection
            .execute_batch(if read_only {
                "PRAGMA query_only = ON;"
            } else {
                "PRAGMA foreign_keys = ON;"
            })
            .map_err(map_sqlite_error)?;
        let applied = {
            let mut statement = connection
                .prepare("SELECT version, name FROM schema_migrations ORDER BY version")
                .map_err(map_sqlite_error)?;
            let rows = statement
                .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .map_err(map_sqlite_error)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(map_sqlite_error)?
        };
        if applied.len() != MIGRATIONS.len()
            || applied.iter().zip(MIGRATIONS).any(
                |((version, name), (expected_version, expected_name, _))| {
                    version != expected_version || name != expected_name
                },
            )
        {
            return Err(SyncStateError::UnsupportedSchema);
        }
        let store = Self {
            connection: Mutex::new(connection),
        };
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, SyncStateError> {
        let connection = Connection::open_in_memory().map_err(map_sqlite_error)?;
        Self::from_connection(connection)
    }

    fn from_connection(mut connection: Connection) -> Result<Self, SyncStateError> {
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(map_sqlite_error)?;
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(map_sqlite_error)?;
        migrate(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Read durable queue backlog and index state; never expose event/page identities.
    /// Missing index state is None rather than a misleading zero-age sample.
    pub fn operational_metrics(&self, now_unix: i64) -> Result<OperationalMetrics, SyncStateError> {
        if now_unix < 0 {
            return Err(SyncStateError::Unavailable);
        }
        let db = self.lock_connection()?;
        let (count, oldest): (i64, Option<i64>) = db
            .query_row(
                "SELECT COUNT(*), MIN(received_at_unix) FROM webhook_inbox WHERE state IN (0, 1)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(map_sqlite_error)?;
        let updated: Option<i64> = db
            .query_row(
                "SELECT MAX(stamp) FROM (
                    SELECT updated_at_unix AS stamp FROM page_sync_state
                    UNION ALL
                    SELECT updated_at_unix AS stamp FROM index_versions
                )",
                [],
                |row| row.get(0),
            )
            .map_err(map_sqlite_error)?;
        let age = |stamp: i64| u64::try_from(now_unix.saturating_sub(stamp).max(0)).unwrap_or(0);
        Ok(OperationalMetrics {
            webhook_outstanding: u64::try_from(count).map_err(|_| SyncStateError::CorruptState)?,
            oldest_webhook_age_seconds: oldest.map(age),
            index_change_age_seconds: updated.map(age),
        })
    }

    pub fn schema_version(&self) -> Result<i64, SyncStateError> {
        let connection = self.lock_connection()?;
        connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(map_sqlite_error)
    }

    pub(crate) fn lock_connection(&self) -> Result<MutexGuard<'_, Connection>, SyncStateError> {
        self.connection
            .lock()
            .map_err(|_| SyncStateError::Unavailable)
    }
}

impl SyncStateStore for SqliteSyncStateStore {
    fn page_state(&self, page_id: &str) -> Result<Option<PageSyncState>, SyncStateError> {
        validate_identifier(page_id)?;
        let connection = self.lock_connection()?;
        let row = connection
            .query_row(
                "SELECT page_id, content_hash, last_edited_time, tombstoned
                 FROM page_sync_state
                 WHERE page_id = ?1",
                params![page_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sqlite_error)?;

        row.map(
            |(page_id, content_hash, last_edited_time, tombstoned)| match tombstoned {
                0 => PageSyncState::present(
                    page_id,
                    content_hash.ok_or(SyncStateError::CorruptState)?,
                    last_edited_time,
                )
                .map_err(|_| SyncStateError::CorruptState),
                1 => PageSyncState::tombstone(page_id, last_edited_time)
                    .map_err(|_| SyncStateError::CorruptState),
                _ => Err(SyncStateError::CorruptState),
            },
        )
        .transpose()
    }

    fn put_page_state(&self, state: &PageSyncState) -> Result<(), SyncStateError> {
        let connection = self.lock_connection()?;
        let tombstoned = if state.is_tombstone() { 1_i64 } else { 0_i64 };
        connection
            .execute(
                "INSERT INTO page_sync_state (
                    page_id, content_hash, last_edited_time, tombstoned, updated_at_unix
                 )
                 VALUES (?1, ?2, ?3, ?4, unixepoch())
                 ON CONFLICT(page_id) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    last_edited_time = excluded.last_edited_time,
                    tombstoned = excluded.tombstoned,
                    updated_at_unix = excluded.updated_at_unix",
                params![
                    state.page_id(),
                    state.content_hash(),
                    state.last_edited_time(),
                    tombstoned
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn checkpoint(&self, key: &str) -> Result<Option<CrawlCheckpoint>, SyncStateError> {
        validate_identifier(key)?;
        let connection = self.lock_connection()?;
        let row = connection
            .query_row(
                "SELECT checkpoint_key, cursor
                 FROM crawl_checkpoints
                 WHERE checkpoint_key = ?1",
                params![key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(map_sqlite_error)?;

        row.map(|(key, cursor)| {
            CrawlCheckpoint::new(key, cursor).map_err(|_| SyncStateError::CorruptState)
        })
        .transpose()
    }

    fn put_checkpoint(&self, checkpoint: &CrawlCheckpoint) -> Result<(), SyncStateError> {
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO crawl_checkpoints (checkpoint_key, cursor, updated_at_unix)
                 VALUES (?1, ?2, unixepoch())
                 ON CONFLICT(checkpoint_key) DO UPDATE SET
                    cursor = excluded.cursor,
                    updated_at_unix = excluded.updated_at_unix",
                params![checkpoint.key(), checkpoint.cursor()],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn register_webhook_event(&self, event_id: &str) -> Result<bool, SyncStateError> {
        validate_identifier(event_id)?;
        let connection = self.lock_connection()?;
        let changed = connection
            .execute(
                "INSERT OR IGNORE INTO webhook_events (event_id) VALUES (?1)",
                params![event_id],
            )
            .map_err(map_sqlite_error)?;
        Ok(changed == 1)
    }

    fn index_version(&self, index_name: &str) -> Result<Option<IndexVersion>, SyncStateError> {
        validate_identifier(index_name)?;
        let connection = self.lock_connection()?;
        let row = connection
            .query_row(
                "SELECT index_name, version
                 FROM index_versions
                 WHERE index_name = ?1",
                params![index_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(map_sqlite_error)?;

        row.map(|(index_name, version)| {
            IndexVersion::new(index_name, version).map_err(|_| SyncStateError::CorruptState)
        })
        .transpose()
    }

    fn put_index_version(&self, version: &IndexVersion) -> Result<(), SyncStateError> {
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO index_versions (index_name, version, updated_at_unix)
                 VALUES (?1, ?2, unixepoch())
                 ON CONFLICT(index_name) DO UPDATE SET
                    version = excluded.version,
                    updated_at_unix = excluded.updated_at_unix",
                params![version.index_name(), version.version()],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }
}

fn migrate(connection: &mut Connection) -> Result<(), SyncStateError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(map_sqlite_error)?;

    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                applied_at_unix INTEGER NOT NULL
            );",
        )
        .map_err(map_sqlite_error)?;

    let applied = {
        let mut statement = transaction
            .prepare("SELECT version, name FROM schema_migrations ORDER BY version")
            .map_err(map_sqlite_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)?
    };

    for (position, (version, name)) in applied.iter().enumerate() {
        if *version > LATEST_SCHEMA_VERSION {
            return Err(SyncStateError::UnsupportedSchema);
        }
        let Some((expected_version, expected_name, _)) = MIGRATIONS.get(position) else {
            return Err(SyncStateError::UnsupportedSchema);
        };
        if version != expected_version || name != expected_name {
            return Err(SyncStateError::CorruptState);
        }
    }

    let current = applied.last().map_or(0, |(version, _)| *version);

    for (version, name, sql) in MIGRATIONS
        .iter()
        .filter(|(version, _, _)| *version > current)
    {
        transaction.execute_batch(sql).map_err(map_sqlite_error)?;
        transaction
            .execute(
                "INSERT INTO schema_migrations (version, name, applied_at_unix)
                 VALUES (?1, ?2, unixepoch())",
                params![*version, *name],
            )
            .map_err(map_sqlite_error)?;
    }

    transaction.commit().map_err(map_sqlite_error)
}

fn map_sqlite_error(_: rusqlite::Error) -> SyncStateError {
    SyncStateError::Unavailable
}

#[cfg(test)]
mod operational_metrics_tests {
    use super::*;

    #[test]
    fn empty_state_omits_unavailable_age_and_never_leaks_ids() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        assert_eq!(
            store.operational_metrics(1_000).unwrap(),
            OperationalMetrics {
                webhook_outstanding: 0,
                oldest_webhook_age_seconds: None,
                index_change_age_seconds: None,
            }
        );
        assert!(store.operational_metrics(-1).is_err());
    }

    #[test]
    fn queue_counts_running_work_and_latest_index_update() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        {
            let db = store.lock_connection().unwrap();
            db.execute(
                "INSERT INTO webhook_inbox
                 (workspace_id, subscription_id, event_id, integration_id, event_timestamp,
                  event_type, entity_id, entity_type, attempt_number, received_at_unix)
                 VALUES ('a', 'b', 'c', 'd', '2026-01-01', 'page.created', 'e', 'page', 1, 80)",
                [],
            )
            .unwrap();
            db.execute(
                "INSERT INTO index_versions(index_name, version, updated_at_unix)
                 VALUES ('index', 'v1', 90)",
                [],
            )
            .unwrap();
        }
        let snapshot = store.operational_metrics(100).unwrap();
        assert_eq!(snapshot.webhook_outstanding, 1);
        assert_eq!(snapshot.oldest_webhook_age_seconds, Some(20));
        assert_eq!(snapshot.index_change_age_seconds, Some(10));
    }
}
