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

pub const LATEST_SCHEMA_VERSION: i64 = 4;

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
];

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
