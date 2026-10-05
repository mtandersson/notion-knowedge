use std::fmt;
use std::path::Path;

use notion_knowledge_core::sync_state::{PageSyncState, SyncStateStore};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

const MIGRATIONS: &[(i64, &str)] = &[(1, include_str!("sync_state/migrations/0001_sync_state.sql"))];

pub struct SqliteSyncStateStore {
    connection: Connection,
}

#[derive(Debug)]
pub enum SqliteSyncStateError {
    Sqlite(rusqlite::Error),
    UnsupportedSchema { found: i64, supported: i64 },
}

impl fmt::Display for SqliteSyncStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sync-state database error: {error}"),
            Self::UnsupportedSchema { found, supported } => write!(
                f,
                "sync-state schema version {found} is newer than supported version {supported}"
            ),
        }
    }
}

impl std::error::Error for SqliteSyncStateError {}

impl From<rusqlite::Error> for SqliteSyncStateError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

impl SqliteSyncStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteSyncStateError> {
        Self::from_connection(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self, SqliteSyncStateError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(mut connection: Connection) -> Result<Self, SqliteSyncStateError> {
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;")?;
        migrate(&mut connection)?;
        Ok(Self { connection })
    }

    pub fn schema_version(&self) -> Result<i64, SqliteSyncStateError> {
        schema_version(&self.connection).map_err(Into::into)
    }
}

impl SyncStateStore for SqliteSyncStateStore {
    type Error = SqliteSyncStateError;

    fn page(&self, page_id: &str) -> Result<Option<PageSyncState>, Self::Error> {
        self.connection
            .query_row(
                "SELECT page_id, content_hash, notion_last_edited_ms, synced_at_ms, tombstoned_at_ms
                 FROM page_sync_state WHERE page_id = ?1",
                [page_id],
                |row| {
                    Ok(PageSyncState {
                        page_id: row.get(0)?,
                        content_hash: row.get(1)?,
                        notion_last_edited_ms: row.get(2)?,
                        synced_at_ms: row.get(3)?,
                        tombstoned_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn upsert_page(&mut self, state: &PageSyncState) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO page_sync_state (
                 page_id, content_hash, notion_last_edited_ms, synced_at_ms, tombstoned_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(page_id) DO UPDATE SET
                 content_hash = excluded.content_hash,
                 notion_last_edited_ms = excluded.notion_last_edited_ms,
                 synced_at_ms = excluded.synced_at_ms,
                 tombstoned_at_ms = excluded.tombstoned_at_ms",
            params![
                &state.page_id,
                &state.content_hash,
                state.notion_last_edited_ms,
                state.synced_at_ms,
                state.tombstoned_at_ms
            ],
        )?;
        Ok(())
    }

    fn checkpoint(&self, key: &str) -> Result<Option<String>, Self::Error> {
        self.connection
            .query_row(
                "SELECT checkpoint_value FROM crawl_checkpoints WHERE checkpoint_key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn put_checkpoint(
        &mut self,
        key: &str,
        value: &str,
        updated_at_ms: i64,
    ) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO crawl_checkpoints (checkpoint_key, checkpoint_value, updated_at_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(checkpoint_key) DO UPDATE SET
                 checkpoint_value = excluded.checkpoint_value,
                 updated_at_ms = excluded.updated_at_ms",
            params![key, value, updated_at_ms],
        )?;
        Ok(())
    }

    fn record_webhook_event(
        &mut self,
        event_id: &str,
        received_at_ms: i64,
    ) -> Result<bool, Self::Error> {
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO webhook_events (event_id, received_at_ms) VALUES (?1, ?2)",
            params![event_id, received_at_ms],
        )?;
        Ok(inserted == 1)
    }

    fn index_version(&self, name: &str) -> Result<Option<String>, Self::Error> {
        self.connection
            .query_row(
                "SELECT version FROM index_versions WHERE index_name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn set_index_version(
        &mut self,
        name: &str,
        version: &str,
        updated_at_ms: i64,
    ) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO index_versions (index_name, version, updated_at_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(index_name) DO UPDATE SET
                 version = excluded.version,
                 updated_at_ms = excluded.updated_at_ms",
            params![name, version, updated_at_ms],
        )?;
        Ok(())
    }
}

fn schema_version(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )
}

fn migrate(connection: &mut Connection) -> Result<(), SqliteSyncStateError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version INTEGER PRIMARY KEY NOT NULL
         ) STRICT;",
    )?;

    let current = schema_version(connection)?;
    let supported = MIGRATIONS.last().map_or(0, |migration| migration.0);
    if current > supported {
        return Err(SqliteSyncStateError::UnsupportedSchema {
            found: current,
            supported,
        });
    }

    for &(version, sql) in MIGRATIONS.iter().filter(|migration| migration.0 > current) {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations (version) VALUES (?1)",
            [version],
        )?;
        transaction.commit()?;
    }

    Ok(())
}
