//! Persistent, cross-process write idempotency ledger.
//! It is intentionally SEPARATE from the disposable search/sync SQLite state.
//! Never auto-recreate a missing production ledger after a restart.

use std::{fs, path::Path, sync::Mutex, time::Duration};

use notion_knowledge_core::idempotency::{
    IdempotencyError, MutationClaim, Reservation, VerifiedReceipt, WriteIdempotencyStore,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params};

pub struct SqliteIdempotencyStore {
    connection: Mutex<Connection>,
}

type Stored = (String, String, Option<String>, Option<String>, Option<String>);

fn unavailable(_: rusqlite::Error) -> IdempotencyError {
    IdempotencyError::Unavailable
}

fn existing(
    transaction: &Transaction<'_>,
    key_digest: &str,
) -> Result<Option<Stored>, IdempotencyError> {
    transaction
        .query_row(
            "SELECT request_digest, status, page_id, url, last_edited_time
             FROM mutation_claims WHERE key_digest = ?1",
            params![key_digest],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()
        .map_err(unavailable)
}

fn checked(
    stored: Option<Stored>,
    claim: &MutationClaim,
) -> Result<Stored, IdempotencyError> {
    let row = stored.ok_or(IdempotencyError::NotReserved)?;
    if row.0 != claim.request_digest() {
        return Err(IdempotencyError::KeyConflict);
    }
    Ok(row)
}

impl SqliteIdempotencyStore {
    /// Explicit first-time initialization only. Production startups must use
    /// open_existing; rebuilding a lost ledger could duplicate writes.
    pub fn initialize_new(path: impl AsRef<Path>) -> Result<Self, IdempotencyError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|_| IdempotencyError::Unavailable)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .map_err(|_| IdempotencyError::Unavailable)?;
        }
        #[cfg(not(unix))]
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|_| IdempotencyError::Unavailable)?;
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(unavailable)?;
        connection
            .execute_batch(
                "PRAGMA synchronous=FULL;
                 CREATE TABLE mutation_claims (
                     key_digest TEXT PRIMARY KEY NOT NULL,
                     request_digest TEXT NOT NULL,
                     status TEXT NOT NULL CHECK (status IN ('pending','unknown','committed')),
                     page_id TEXT,
                     url TEXT,
                     last_edited_time TEXT,
                     created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
                     changed_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
                     CHECK (
                         (status = 'committed' AND page_id IS NOT NULL
                             AND url IS NOT NULL AND last_edited_time IS NOT NULL)
                         OR (status != 'committed' AND page_id IS NULL
                             AND url IS NULL AND last_edited_time IS NULL)
                     )
                 );
                 PRAGMA user_version=1;",
            )
            .map_err(unavailable)?;
        Self::from_connection(connection)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, IdempotencyError> {
        let path = path.as_ref();
        if !fs::symlink_metadata(path)
            .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            return Err(IdempotencyError::Unavailable);
        }
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(unavailable)?;
        Self::from_connection(connection)
    }

    pub fn open_in_memory() -> Result<Self, IdempotencyError> {
        let connection = Connection::open_in_memory().map_err(unavailable)?;
        connection
            .execute_batch(
                "CREATE TABLE mutation_claims (
                     key_digest TEXT PRIMARY KEY NOT NULL,
                     request_digest TEXT NOT NULL,
                     status TEXT NOT NULL CHECK (status IN ('pending','unknown','committed')),
                     page_id TEXT,
                     url TEXT,
                     last_edited_time TEXT,
                     created_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
                     changed_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 PRAGMA user_version=1;",
            )
            .map_err(unavailable)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: Connection) -> Result<Self, IdempotencyError> {
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(unavailable)?;
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(unavailable)?;
        if version != 1 {
            return Err(IdempotencyError::CorruptState);
        }
        connection
            .query_row("SELECT COUNT(*) FROM mutation_claims", [], |row| row.get::<_, i64>(0))
            .map_err(|_| IdempotencyError::CorruptState)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn transaction(&self) -> Result<std::sync::MutexGuard<'_, Connection>, IdempotencyError> {
        self.connection.lock().map_err(|_| IdempotencyError::Unavailable)
    }
}

impl WriteIdempotencyStore for SqliteIdempotencyStore {
    fn begin(&self, claim: &MutationClaim) -> Result<Reservation, IdempotencyError> {
        let mut connection = self.transaction()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let state = existing(&tx, claim.key_digest())?;
        let result = if let Some(row) = state {
            let (digest, status, page_id, url, edited) = row;
            if digest != claim.request_digest() {
                return Err(IdempotencyError::KeyConflict);
            }
            match status.as_str() {
                "pending" | "unknown" => Reservation::Reconcile,
                "committed" => Reservation::Replay(
                    VerifiedReceipt::from_readback(
                        page_id.ok_or(IdempotencyError::CorruptState)?,
                        url.ok_or(IdempotencyError::CorruptState)?,
                        edited.ok_or(IdempotencyError::CorruptState)?,
                    )
                    .map_err(|_| IdempotencyError::CorruptState)?,
                ),
                _ => return Err(IdempotencyError::CorruptState),
            }
        } else {
            tx.execute(
                "INSERT INTO mutation_claims (key_digest, request_digest, status)
                 VALUES (?1, ?2, 'pending')",
                params![claim.key_digest(), claim.request_digest()],
            )
            .map_err(unavailable)?;
            Reservation::ExecuteOnce
        };
        tx.commit().map_err(unavailable)?;
        Ok(result)
    }

    fn mark_uncertain(&self, claim: &MutationClaim) -> Result<(), IdempotencyError> {
        let mut connection = self.transaction()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let row = checked(existing(&tx, claim.key_digest())?, claim)?;
        match row.1.as_str() {
            "pending" => {
                tx.execute(
                    "UPDATE mutation_claims SET status='unknown', changed_at_unix=unixepoch()
                     WHERE key_digest=?1",
                    params![claim.key_digest()],
                )
                .map_err(unavailable)?;
            }
            "unknown" => {}
            "committed" => return Err(IdempotencyError::AlreadyCommitted),
            _ => return Err(IdempotencyError::CorruptState),
        }
        tx.commit().map_err(unavailable)?;
        Ok(())
    }

    fn record_verified(
        &self,
        claim: &MutationClaim,
        receipt: &VerifiedReceipt,
    ) -> Result<(), IdempotencyError> {
        let mut connection = self.transaction()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let row = checked(existing(&tx, claim.key_digest())?, claim)?;
        match row.1.as_str() {
            "pending" | "unknown" => {
                tx.execute(
                    "UPDATE mutation_claims
                     SET status='committed', page_id=?2, url=?3, last_edited_time=?4,
                         changed_at_unix=unixepoch()
                     WHERE key_digest=?1",
                    params![
                        claim.key_digest(),
                        receipt.page_id,
                        receipt.url,
                        receipt.last_edited_time
                    ],
                )
                .map_err(unavailable)?;
            }
            "committed" => {
                if row.2.as_deref() != Some(&receipt.page_id)
                    || row.3.as_deref() != Some(&receipt.url)
                    || row.4.as_deref() != Some(&receipt.last_edited_time)
                {
                    return Err(IdempotencyError::KeyConflict);
                }
            }
            _ => return Err(IdempotencyError::CorruptState),
        }
        tx.commit().map_err(unavailable)?;
        Ok(())
    }
}
