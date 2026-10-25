//! Offline index preparation owns both filesystem admission and operational SQL.
//! No scope changes, receipt supersession, or external source effects occur here.
use super::*;
use sha2::{Digest, Sha256};
use std::io::Seek;
use std::os::unix::fs::OpenOptionsExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OfflineBackup {
    pub sha256: String,
    pub bytes: u64,
}
pub(super) fn hash_file(file: &mut File) -> Result<OfflineBackup, CommitError> {
    hash_file_bounded(file, u64::MAX)
}
fn hash_file_bounded(file: &mut File, limit: u64) -> Result<OfflineBackup, CommitError> {
    file.rewind().map_err(io)?;
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let remaining = limit
            .saturating_sub(bytes)
            .saturating_add(1)
            .min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..remaining]).map_err(io)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        bytes = bytes
            .checked_add(count as u64)
            .ok_or(CommitError::Unavailable)?;
        if bytes > limit {
            return Err(CommitError::InvalidInput);
        }
    }
    Ok(OfflineBackup {
        sha256: format!("{:x}", hash.finalize()),
        bytes,
    })
}
pub(super) fn private_parent(path: &Path) -> Result<(), CommitError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(CommitError::InvalidInput);
    }
    let own = fs::metadata("/proc/self").map_err(io)?.uid();
    let parent = path.parent().ok_or(CommitError::InvalidInput)?;
    let metadata = fs::symlink_metadata(parent).map_err(io)?;
    if !metadata.is_dir() || metadata.uid() != own || metadata.mode() & 0o077 != 0 {
        return Err(CommitError::InvalidInput);
    }
    for ancestor in parent.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(io)?;
        if !metadata.is_dir()
            || (metadata.uid() != own && metadata.uid() != 0)
            || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
        {
            return Err(CommitError::InvalidInput);
        }
    }
    Ok(())
}
impl OfflineBackup {
    /// Verify a retained regular private recovery file without opening SQL writable.
    pub fn verify(&self, path: &Path) -> Result<(), CommitError> {
        private_parent(path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(io)?;
        let metadata = file.metadata().map_err(io)?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
            || metadata.len() != self.bytes
        {
            return Err(CommitError::BindingMismatch);
        }
        if hash_file(&mut file)? != *self {
            return Err(CommitError::BindingMismatch);
        }
        Ok(())
    }
}
/// Read-only evidence access while the original index lock, SQL transaction and
/// state descriptor are retained. No connection or mutation API is exposed.
/// This value cannot be returned from its higher-ranked owning callback.
///
/// ```compile_fail
/// use notion_knowledge_retrieval::commit::{IndexCommitCoordinator, OwnedOfflineInspection};
/// fn escape<'a>(c: &'a IndexCommitCoordinator) -> &'a OwnedOfflineInspection<'a> {
///     c.with_owned_offline_inspection(10, |owner| Ok(owner)).unwrap()
/// }
/// ```
pub struct OwnedOfflineInspection<'owner> {
    coordinator: &'owner IndexCommitCoordinator,
    connection: &'owner Connection,
    state: File,
}
impl OwnedOfflineInspection<'_> {
    pub fn binding(&self) -> &CommitBinding {
        self.coordinator.binding()
    }
    pub fn index_directory(&self) -> &Path {
        self.coordinator.index_directory()
    }

    /// Revalidate both the retained descriptor and SQLite's actual VFS file.
    /// Scope, state token, operational anchor and physical authority must still
    /// be exactly those captured by the original coordinator.
    pub fn revalidate(&self) -> Result<(), CommitError> {
        self.coordinator.identities()?;
        validate_original_state(&self.state, self.coordinator)?;
        transition::validate_connection_file(
            self.connection,
            &self.state,
            &self.coordinator.state_path,
        )?;
        self.coordinator.validate_binding(self.connection)
    }
    /// An opaque digest of the exact SQL token/inode and index/binding tuple.
    /// No token bytes escape into evidence or subprocess configuration.
    pub fn authority_sha256(&self) -> Result<String, CommitError> {
        self.revalidate()?;
        let c = self.coordinator;
        let bytes = serde_json::to_vec(&(
            &c.binding,
            &c.index_path,
            &c.state_path,
            c.index_identity.device,
            c.index_identity.inode,
            c.state_identity.device,
            c.state_identity.inode,
            &c.state_token,
        ))
        .map_err(|_| CommitError::InvalidInput)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
    /// Hash the original operational bytes under the retained exclusive SQL
    /// transaction. This creates no backup and changes no acknowledgements.
    pub fn state_snapshot(&self, max_bytes: u64) -> Result<OfflineBackup, CommitError> {
        if max_bytes == 0 {
            return Err(CommitError::InvalidInput);
        }
        self.revalidate()?;
        let snapshot = hash_file_bounded(&mut self.state.try_clone().map_err(io)?, max_bytes)?;
        self.revalidate()?;
        Ok(snapshot)
    }
}
fn validate_original_state(
    file: &File,
    coordinator: &IndexCommitCoordinator,
) -> Result<(), CommitError> {
    use std::io::{Seek, SeekFrom};
    let metadata = file.metadata().map_err(io)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != fs::metadata("/proc/self").map_err(io)?.uid()
        || Identity::file(file)? != coordinator.state_identity
    {
        return Err(CommitError::BindingMismatch);
    }
    let mut file = file.try_clone().map_err(io)?;
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let mut header = [0; 100];
    file.read_exact(&mut header).map_err(io)?;
    if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
        return Err(CommitError::InvalidInput);
    }
    Ok(())
}
impl IndexCommitCoordinator {
    /// Read-only offline verification under the same index/SQL exclusion used
    /// during preparation. No backup, schema migration, lease or receipt writes.
    pub fn with_offline_inspection<T>(
        &self,
        now: i64,
        inspect: impl FnOnce() -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        self.with_owned_offline_inspection(now, |_| inspect())
    }
    /// Hold original index/SQL ownership throughout one read-only verification.
    /// All source processes must already be stopped. The callback must use this
    /// context rather than reacquire the coordinator's SQL mutex or index lock.
    pub fn with_owned_offline_inspection<T>(
        &self,
        now: i64,
        inspect: impl for<'owner> FnOnce(&OwnedOfflineInspection<'owner>) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        if now < 0 {
            return Err(CommitError::InvalidInput);
        }
        private_parent(&self.state_path)?;
        if self.state_path.starts_with(&self.index_path) {
            return Err(CommitError::InvalidInput);
        }
        let _index = self.try_acquire()?;
        self.identities()?;
        let state = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.state_path)
            .map_err(io)?;
        validate_original_state(&state, self)?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let transaction = db
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(sql)?;
        let owner = OwnedOfflineInspection {
            coordinator: self,
            connection: &transaction,
            state,
        };
        owner.revalidate()?;
        let active: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_lease WHERE expires_at>?1) OR EXISTS(SELECT 1 FROM file_refresh_intents WHERE phase='held' AND lease_until>?1)", [now], |r|r.get(0)).map_err(sql)?;
        if active {
            return Err(CommitError::Conflict);
        }
        let result = inspect(&owner)?;
        owner.revalidate()?;
        drop(owner);
        transaction.commit().map_err(sql)?;
        Ok(result)
    }
    /// Synchronous trusted offline boundary; caller must stop the serving runtime.
    /// A nonblocking exclusive index lock prevents new guarded reads/writes, and
    /// an exclusive SQLite transaction excludes all participating operational
    /// writers for the entire candidate operation. Live lifecycle/file leases
    /// reject admission. Pending acknowledged work is neither changed nor replayed.
    ///
    /// Only rollback-journal SQLite supports this raw, verified backup protocol.
    /// The complete backup remains on any later preparation error for recovery.
    pub fn with_offline_backup<T>(
        &self,
        backup: &Path,
        now: i64,
        prepare: impl FnOnce(&OfflineBackup) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        self.with_offline_backup_bounded(backup, now, u64::MAX, prepare)
    }

    /// Refuse oversized state before creating a backup, while holding the same
    /// index and SQL ownership used by the ordinary offline snapshot path.
    pub fn with_offline_backup_bounded<T>(
        &self,
        backup: &Path,
        now: i64,
        max_bytes: u64,
        prepare: impl FnOnce(&OfflineBackup) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        if now < 0 || max_bytes == 0 {
            return Err(CommitError::InvalidInput);
        }
        private_parent(backup)?;
        let backup_parent =
            fs::canonicalize(backup.parent().ok_or(CommitError::InvalidInput)?).map_err(io)?;
        if backup_parent.starts_with(&self.index_path) {
            return Err(CommitError::InvalidInput);
        }
        private_parent(&self.state_path)?;
        if self.state_path.starts_with(&self.index_path) {
            return Err(CommitError::InvalidInput);
        }
        let _index = self.try_acquire()?;
        self.identities()?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let transaction = db
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(sql)?;
        self.validate_binding(&transaction)?;
        let active: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM reconciliation_lease WHERE expires_at>?1)",
                [now],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let files: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM file_refresh_intents WHERE phase='held' AND lease_until>?1)", [now], |r| r.get(0)).map_err(sql)?;
        if active || files {
            return Err(CommitError::Conflict);
        }
        let check: String = transaction
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(sql)?;
        if check != "ok" {
            return Err(CommitError::Unavailable);
        }
        let mut source = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.state_path)
            .map_err(io)?;
        let opened = source.metadata().map_err(io)?;
        if !opened.is_file()
            || opened.nlink() != 1
            || opened.mode() & 0o077 != 0
            || Identity::file(&source)? != self.state_identity
        {
            return Err(CommitError::BindingMismatch);
        }
        if opened.len() > max_bytes {
            return Err(CommitError::InvalidInput);
        }
        let mut header = [0u8; 100];
        source.read_exact(&mut header).map_err(io)?;
        if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
            return Err(CommitError::InvalidInput);
        }
        source.rewind().map_err(io)?;
        let mut destination = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(backup)
            .map_err(io)?;
        let copied = std::io::copy(
            &mut (&mut source).take(max_bytes.saturating_add(1)),
            &mut destination,
        )
        .map_err(io)?;
        if copied != opened.len() {
            return Err(CommitError::BindingMismatch);
        }
        destination.sync_all().map_err(io)?;
        File::open(backup.parent().ok_or(CommitError::InvalidInput)?)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        let digest = hash_file_bounded(&mut source, max_bytes)?;
        if hash_file_bounded(&mut destination, max_bytes)? != digest {
            return Err(CommitError::Unavailable);
        }
        let retained = Connection::open_with_flags(
            backup,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sql)?;
        let check: String = retained
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(sql)?;
        if check != "ok" {
            return Err(CommitError::Unavailable);
        }
        drop(retained);
        let result = prepare(&digest)?;
        // Uncooperative physical replacement cannot turn preparation into authority.
        self.identities()?;
        self.validate_binding(&transaction)?;
        if hash_file_bounded(&mut source, max_bytes)? != digest {
            return Err(CommitError::BindingMismatch);
        }
        transaction.commit().map_err(sql)?;
        Ok(result)
    }
}
