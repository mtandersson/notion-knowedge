//! Durable transition ownership, record storage and coordinator admission.
//! Native publication and forward completion live in `publication`.
//! The offline transition owner must hold state/index/SQL exclusion throughout
//! publication; recovery must inspect physical and SQL authority, not phase alone.
use super::*;
use std::os::unix::fs::OpenOptionsExt;
#[cfg(feature = "local-lancedb")]
pub mod adoption;
#[cfg(feature = "local-lancedb")]
pub mod manifest;
#[cfg(feature = "local-lancedb")]
pub mod preparation;
#[cfg(feature = "local-lancedb")]
pub mod publication;
#[cfg(all(feature = "local-lancedb", target_os = "linux"))]
pub mod runtime_journal;

const MAX_RECORD_BYTES: u64 = 65_536;

#[cfg(test)]
pub(super) fn transition_checkpoint(owner: &TransitionOwnership, phase: &str) {
    let Some(root) = std::env::var_os("NK_TRANSITION_TEST_ROOT").map(PathBuf::from) else {
        return;
    };
    if owner.state_path.parent() != Some(root.as_path())
        || std::env::var("NK_TRANSITION_TEST_PHASE").ok().as_deref() != Some(phase)
    {
        return;
    }
    fs::write(root.join("observed-transition"), phase).unwrap();
    loop {
        std::thread::park();
    }
}

/// Stable ownership across directory renames. This excludes other transition
/// operators and guarded index consumers; an exclusive SQLite transaction is
/// still required to exclude operational writers before any durable effects.
pub struct TransitionOwnership {
    state_path: PathBuf,
    state: File,
    source: File,
    target: File,
    record: TransitionRecord,
    #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
    runtime: Option<runtime_journal::RuntimeJournalRecord>,
}

fn exclusive(file: &File) -> Result<(), CommitError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(CommitError::Conflict),
        Err(std::fs::TryLockError::Error(error)) => Err(io(error)),
    }
}

pub(crate) fn validate_connection_file(
    db: &Connection,
    file: &File,
    path: &Path,
) -> Result<(), CommitError> {
    let database = db.path().ok_or(CommitError::BindingMismatch)?;
    if fs::canonicalize(database).map_err(io)? != fs::canonicalize(path).map_err(io)?
        || Identity::read(path)? != Identity::file(file)?
    {
        return Err(CommitError::BindingMismatch);
    }
    let mut moved: i32 = 0;
    // Check SQLite's own VFS descriptor as well as our retained descriptor, so
    // opening a transient replacement and restoring the path cannot pass.
    let result = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            db.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_HAS_MOVED,
            (&mut moved as *mut i32).cast(),
        )
    };
    if result != rusqlite::ffi::SQLITE_OK || moved != 0 {
        return Err(CommitError::BindingMismatch);
    }
    Ok(())
}

fn owned_directory(path: &Path) -> Result<File, CommitError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_NONBLOCK)
        .open(path)
        .map_err(io)?;
    let metadata = file.metadata().map_err(io)?;
    if !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != fs::metadata("/proc/self").map_err(io)?.uid()
    {
        return Err(CommitError::BindingMismatch);
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn rename_unused(source: &Path, target: &Path) -> Result<(), CommitError> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())
        .map_err(|_| CommitError::InvalidInput)?;
    let target = std::ffi::CString::new(target.as_os_str().as_bytes())
        .map_err(|_| CommitError::InvalidInput)?;
    // Both complete paths are owned private siblings. RENAME_NOREPLACE keeps
    // even an unexpected existing recovery artifact intact atomically.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io(std::io::Error::last_os_error()))
    }
}

#[cfg(not(target_os = "linux"))]
fn rename_unused(_source: &Path, _target: &Path) -> Result<(), CommitError> {
    Err(CommitError::Unavailable)
}

fn locate(record: &TransitionRecord, source: bool) -> Result<File, CommitError> {
    let endpoint = if source {
        &record.source
    } else {
        &record.target
    };
    let alternate = if source {
        &record.archive_path
    } else {
        &record.candidate_path
    };
    let expected = Identity {
        device: endpoint.device,
        inode: endpoint.inode,
    };
    let mut found = None;
    for path in [&endpoint.path, alternate] {
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io(error)),
            Ok(metadata) if !metadata.is_dir() => return Err(CommitError::BindingMismatch),
            Ok(_) => {}
        }
        let file = owned_directory(path)?;
        if Identity::file(&file)? == expected {
            if found.is_some() {
                return Err(CommitError::BindingMismatch);
            }
            found = Some(file);
        } else if path == alternate {
            return Err(CommitError::BindingMismatch);
        } else {
            let other = if source {
                &record.target
            } else {
                &record.source
            };
            if Identity::file(&file)?
                != (Identity {
                    device: other.device,
                    inode: other.inode,
                })
            {
                return Err(CommitError::BindingMismatch);
            }
        }
    }
    found.ok_or(CommitError::BindingMismatch)
}

impl TransitionOwnership {
    /// Immutable identity captured when this owner acquired its physical locks.
    pub fn record(&self) -> &TransitionRecord {
        &self.record
    }

    pub(crate) fn validate_owned_transaction(&self, db: &Connection) -> Result<(), CommitError> {
        self.revalidate()?;
        validate_connection_file(db, &self.state, &self.state_path)?;
        self.validate_sql(db)
    }

    /// Read this owner's actual journal dialect. Runtime metadata is projected
    /// only for the shared physical/SQL engine; it is never serialized as v1.
    pub(crate) fn current_record(&self) -> Result<Option<TransitionRecord>, CommitError> {
        #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
        if let Some(base) = &self.runtime {
            return runtime_journal::RuntimeJournalRecord::read_active(&self.state_path)?
                .map(|record| {
                    record.same_operation(base)?;
                    record.layout()
                })
                .transpose();
        }
        TransitionRecord::read(&self.state_path)
    }
    fn persist_record(
        &self,
        record: &TransitionRecord,
        previous: &TransitionRecord,
    ) -> Result<(), CommitError> {
        #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
        if self.runtime.is_some() {
            return runtime_journal::advance_layout(self, record, previous);
        }
        record.persist(self, Some(previous))
    }

    /// Resolve captured physical identities, including rename-before-journal
    /// windows. Returned paths are usable only while this owner remains held.
    pub fn physical_paths(&self) -> Result<(PathBuf, PathBuf), CommitError> {
        self.revalidate()?;
        let find = |file: &File, paths: [&Path; 2]| -> Result<PathBuf, CommitError> {
            for path in paths {
                match owned_directory(path) {
                    Ok(current) if Identity::file(&current)? == Identity::file(file)? => {
                        return Ok(path.to_owned());
                    }
                    Ok(_) => {}
                    Err(_) if !path.try_exists().map_err(io)? => {}
                    Err(error) => return Err(error),
                }
            }
            Err(CommitError::BindingMismatch)
        };
        let source = find(
            &self.source,
            [&self.record.source.path, &self.record.archive_path],
        )?;
        let target = find(
            &self.target,
            [&self.record.target.path, &self.record.candidate_path],
        )?;
        self.revalidate()?;
        Ok((source, target))
    }
    /// Acquire in stable state/source/target order without waiting. Recovery
    /// validates the original state token directly; it never opens a coordinator
    /// that could admit a quarantined generation or initialize missing state.
    pub fn acquire(state_path: &Path, record: &TransitionRecord) -> Result<Self, CommitError> {
        canonical(record)?;
        validate_artifact_paths(state_path, record)?;
        super::offline::private_parent(state_path)?;
        for path in [
            &record.source.path,
            &record.candidate_path,
            &record.archive_path,
        ] {
            super::offline::private_parent(path)?;
            if state_path.starts_with(path) {
                return Err(CommitError::InvalidInput);
            }
        }
        let state = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(state_path)
            .map_err(io)?;
        let metadata = state.metadata().map_err(io)?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
            || metadata.uid() != fs::metadata("/proc/self").map_err(io)?.uid()
            || Identity::file(&state)?
                != (Identity {
                    device: record.state_device,
                    inode: record.state_inode,
                })
        {
            return Err(CommitError::BindingMismatch);
        }
        exclusive(&state)?;
        let source = locate(record, true)?;
        exclusive(&source)?;
        let target = locate(record, false)?;
        exclusive(&target)?;
        let owned = Self {
            state_path: state_path.to_owned(),
            state,
            source,
            target,
            record: record.clone(),
            #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
            runtime: None,
        };
        owned.revalidate()?;
        // An interrupted publication can leave a hot rollback journal. SQLite
        // must recover that journal before reading identity/schema. Only allow
        // its ordinary pager recovery after all physical locks are retained;
        // never restore an application backup or migrate/create a database.
        let mut header = [0u8; 20];
        std::os::unix::fs::FileExt::read_exact_at(&owned.state, &mut header, 0).map_err(io)?;
        if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
            return Err(CommitError::BindingMismatch);
        }
        let mut recovery = Connection::open_with_flags(
            state_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sql)?;
        recovery
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(sql)?;
        validate_connection_file(&recovery, &owned.state, state_path)?;
        let transaction = recovery
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(sql)?;
        validate_connection_file(&transaction, &owned.state, state_path)?;
        let recovered_token: String = transaction
            .query_row(
                "SELECT identity FROM index_commit_state_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if recovered_token != record.state_token {
            return Err(CommitError::BindingMismatch);
        }
        transaction.commit().map_err(sql)?;
        drop(recovery);
        let store = SqliteSyncStateStore::open_read_only(state_path)
            .map_err(|_| CommitError::Unavailable)?;
        let db = store
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        validate_connection_file(&db, &owned.state, state_path)?;
        let token: String = db
            .query_row(
                "SELECT identity FROM index_commit_state_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if token != record.state_token {
            return Err(CommitError::BindingMismatch);
        }
        owned.revalidate()?;
        Ok(owned)
    }

    pub fn revalidate(&self) -> Result<(), CommitError> {
        for path in [
            &self.state_path,
            &self.record.source.path,
            &self.record.candidate_path,
            &self.record.archive_path,
        ] {
            super::offline::private_parent(path)?;
        }
        let metadata = fs::symlink_metadata(&self.state_path).map_err(io)?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
            || metadata.uid() != fs::metadata("/proc/self").map_err(io)?.uid()
            || Identity::read(&self.state_path)? != Identity::file(&self.state)?
            || Identity::file(&locate(&self.record, true)?)? != Identity::file(&self.source)?
            || Identity::file(&locate(&self.record, false)?)? != Identity::file(&self.target)?
        {
            return Err(CommitError::BindingMismatch);
        }
        self.verify_source_anchor()?;
        Ok(())
    }

    fn verify_source_anchor(&self) -> Result<(), CommitError> {
        self.verify_anchor(&self.source)
    }

    fn verify_anchor(&self, directory: &File) -> Result<(), CommitError> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let name = std::ffi::CString::new(ANCHOR).map_err(|_| CommitError::InvalidInput)?;
        // Resolve through the retained directory descriptor, including after a
        // rename, and never follow a replaceable anchor symlink.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if fd < 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let metadata = file.metadata().map_err(io)?;
        if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > 128 {
            return Err(CommitError::BindingMismatch);
        }
        let mut actual = String::new();
        file.take(129).read_to_string(&mut actual).map_err(io)?;
        let expected = format!(
            "nk-index-binding-v1\n{}\n{}\n{}\n",
            self.record.state_device, self.record.state_inode, self.record.state_token
        );
        if actual != expected {
            return Err(CommitError::BindingMismatch);
        }
        Ok(())
    }

    /// Own the operational database without reconstructing a serving
    /// coordinator. The caller still verifies native state before publication.
    pub fn with_exclusive_sql<T>(
        &self,
        now: i64,
        operation: impl FnOnce(&mut rusqlite::Transaction<'_>) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        if now < 0 {
            return Err(CommitError::InvalidInput);
        }
        self.revalidate()?;
        let store = SqliteSyncStateStore::open_existing(&self.state_path)
            .map_err(|_| CommitError::Unavailable)?;
        let mut db = store
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        db.busy_timeout(std::time::Duration::ZERO).map_err(sql)?;
        let mut transaction = db
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(sql)?;
        self.validate_owned_transaction(&transaction)?;
        let live: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_lease WHERE expires_at>?1) OR EXISTS(SELECT 1 FROM file_refresh_intents WHERE phase='held' AND lease_until>?1)", [now], |r| r.get(0)).map_err(sql)?;
        if live {
            return Err(CommitError::Conflict);
        }
        let result = operation(&mut transaction)?;
        self.revalidate()?;
        self.validate_owned_transaction(&transaction)?;
        transaction.commit().map_err(sql)?;
        Ok(result)
    }

    fn validate_sql(&self, db: &Connection) -> Result<(), CommitError> {
        let source: RecordedBinding = serde_json::from_str(&self.record.source.binding)
            .map_err(|_| CommitError::InvalidInput)?;
        let target: RecordedBinding = serde_json::from_str(&self.record.target.binding)
            .map_err(|_| CommitError::InvalidInput)?;
        let quarantine_epoch =
            i64::try_from(self.record.quarantine_epoch).map_err(|_| CommitError::InvalidInput)?;
        let valid: bool = db.query_row("SELECT identity=?1 AND state_device=?2 AND state_inode=?3 AND table_name=?4 AND workspace=?5 AND scope=?6 AND ((index_device=?7 AND index_inode=?8 AND ((generation=?9 AND epoch=?10) OR (generation=?11 AND epoch=?12))) OR (index_device=?13 AND index_inode=?14 AND generation=?15 AND epoch=?16)) FROM index_commit_binding CROSS JOIN index_commit_state_identity WHERE index_commit_binding.singleton=1 AND index_commit_state_identity.singleton=1",
            params![self.record.state_token,self.record.state_device.to_string(),self.record.state_inode.to_string(),source.table,source.workspace,source.scope,self.record.source.device.to_string(),self.record.source.inode.to_string(),source.generation,source.epoch,self.record.quarantine_generation,quarantine_epoch,self.record.target.device.to_string(),self.record.target.inode.to_string(),target.generation,target.epoch], |r|r.get(0)).optional().map_err(sql)?.unwrap_or(false);
        if valid {
            Ok(())
        } else {
            Err(CommitError::BindingMismatch)
        }
    }

    /// Reserve quarantine and final generation identities atomically before any
    /// rename. The caller must first verify the complete candidate under this
    /// ownership. Prepared must already contain a fresh activation-time backup.
    /// Recovery may repeat this after SQL committed but journal fsync failed.
    pub fn quarantine(&self, now: i64) -> Result<TransitionRecord, CommitError> {
        self.quarantine_inner(now, |_| Ok(()))
    }
    #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
    pub(crate) fn quarantine_checked(
        &self,
        now: i64,
        verify: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(), CommitError>,
    ) -> Result<TransitionRecord, CommitError> {
        self.quarantine_inner(now, verify)
    }
    fn quarantine_inner(
        &self,
        now: i64,
        mut verify: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(), CommitError>,
    ) -> Result<TransitionRecord, CommitError> {
        let previous = self.current_record()?.ok_or(CommitError::Conflict)?;
        let mut expected = self.record.clone();
        expected.phase = previous.phase;
        if expected != previous
            || !matches!(
                previous.phase,
                TransitionPhase::Prepared | TransitionPhase::Quarantined
            )
        {
            return Err(CommitError::Conflict);
        }
        let source: RecordedBinding = serde_json::from_str(&previous.source.binding)
            .map_err(|_| CommitError::InvalidInput)?;
        let target: RecordedBinding = serde_json::from_str(&previous.target.binding)
            .map_err(|_| CommitError::InvalidInput)?;
        let quarantine_epoch =
            i64::try_from(previous.quarantine_epoch).map_err(|_| CommitError::InvalidInput)?;
        self.with_exclusive_sql(now, |transaction| {
            verify(transaction)?;
            if self.current_record()?.as_ref() != Some(&previous) {
                return Err(CommitError::Conflict);
            }
            previous.backup.verify(&previous.backup_path)?;
            let (generation, epoch): (String, i64) = transaction.query_row("SELECT generation,epoch FROM index_commit_binding WHERE singleton=1", [], |r| Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
            if generation == previous.quarantine_generation && epoch == quarantine_epoch {
                for (name, epoch) in [(&previous.quarantine_generation, quarantine_epoch), (&target.generation, target.epoch)] {
                    let reserved: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 AND scope=?2 AND epoch=?3)", params![name,source.scope,epoch], |r|r.get(0)).map_err(sql)?;
                    if !reserved { return Err(CommitError::BindingMismatch); }
                }
                super::alias_projection::ensure_history_index(transaction)?;
                return Ok(());
            }
            if previous.phase != TransitionPhase::Prepared || generation != source.generation || epoch != source.epoch {
                return Err(CommitError::BindingMismatch);
            }
            let current = super::offline::hash_file(&mut self.state.try_clone().map_err(io)?)?;
            if current != previous.backup { return Err(CommitError::BindingMismatch); }
            let journal: String = transaction.query_row("PRAGMA journal_mode", [], |r|r.get(0)).map_err(sql)?;
            if journal != "delete" { return Err(CommitError::InvalidInput); }
            let collision: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation IN (?1,?2) OR epoch IN (?3,?4))", params![previous.quarantine_generation,target.generation,quarantine_epoch,target.epoch], |r|r.get(0)).map_err(sql)?;
            if collision { return Err(CommitError::Conflict); }
            super::alias_projection::ensure_history_index(transaction)?;
            for (name, epoch) in [(&previous.quarantine_generation, quarantine_epoch), (&target.generation, target.epoch)] {
                transaction.execute("INSERT INTO index_commit_generations VALUES(?1,?2,?3)", params![name,source.scope,epoch]).map_err(sql)?;
            }
            transaction.execute("UPDATE index_commit_binding SET generation=?1,epoch=?2 WHERE singleton=1", params![previous.quarantine_generation,quarantine_epoch]).map_err(sql)?;
            verify(transaction)?;
            Ok(())
        })?;
        #[cfg(test)]
        transition_checkpoint(self, "quarantine-sql");
        if previous.phase == TransitionPhase::Quarantined {
            return Ok(previous);
        }
        let mut next = previous.clone();
        next.phase = TransitionPhase::Quarantined;
        self.persist_record(&next, &previous)?;
        Ok(next)
    }

    /// Retain the old physical index and install the candidate while SQL remains
    /// quarantined. Restart uses captured inodes to recognize each completed
    /// rename before advancing its durable phase. This does not publish serving
    /// authority, create an anchor, or replace full installed native validation.
    pub fn install_candidate(&self, now: i64) -> Result<TransitionRecord, CommitError> {
        self.install_candidate_inner(now, false, |_| Ok(()))
    }
    #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
    pub(crate) fn retain_source_checked(
        &self,
        now: i64,
        verify: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(), CommitError>,
    ) -> Result<TransitionRecord, CommitError> {
        self.install_candidate_inner(now, true, verify)
    }
    #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
    pub(crate) fn install_candidate_checked(
        &self,
        now: i64,
        verify: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(), CommitError>,
    ) -> Result<TransitionRecord, CommitError> {
        self.install_candidate_inner(now, false, verify)
    }
    fn install_candidate_inner(
        &self,
        now: i64,
        retain_only: bool,
        mut verify: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(), CommitError>,
    ) -> Result<TransitionRecord, CommitError> {
        let mut current = self.current_record()?.ok_or(CommitError::Conflict)?;
        let mut expected = self.record.clone();
        expected.phase = current.phase;
        if expected != current
            || !matches!(
                current.phase,
                TransitionPhase::Quarantined
                    | TransitionPhase::SourceRetained
                    | TransitionPhase::TargetInstalled
            )
        {
            return Err(CommitError::Conflict);
        }
        self.with_exclusive_sql(now, |transaction| {
            verify(transaction)?;
            if self.current_record()?.as_ref() != Some(&current) { return Err(CommitError::Conflict); }
            current.backup.verify(&current.backup_path)?;
            let target: RecordedBinding = serde_json::from_str(&current.target.binding).map_err(|_| CommitError::InvalidInput)?;
            let quarantine_epoch = i64::try_from(current.quarantine_epoch).map_err(|_| CommitError::InvalidInput)?;
            let quarantined: bool = transaction.query_row("SELECT generation=?1 AND epoch=?2 FROM index_commit_binding WHERE singleton=1", params![current.quarantine_generation,quarantine_epoch], |r|r.get(0)).map_err(sql)?;
            let reserved: i64 = transaction.query_row("SELECT COUNT(*) FROM index_commit_generations WHERE scope=?1 AND ((generation=?2 AND epoch=?3) OR (generation=?4 AND epoch=?5))", params![target.scope,current.quarantine_generation,quarantine_epoch,target.generation,target.epoch], |r|r.get(0)).map_err(sql)?;
            if !quarantined || reserved != 2 { return Err(CommitError::BindingMismatch); }
            match fs::symlink_metadata(&current.archive_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if current.phase != TransitionPhase::Quarantined { return Err(CommitError::BindingMismatch); }
                    self.revalidate()?;
                    rename_unused(&current.source.path, &current.archive_path)?;
                }
                Err(error) => return Err(io(error)),
                Ok(_) => {}
            }
            self.revalidate()?;
            File::open(current.source.path.parent().ok_or(CommitError::InvalidInput)?).map_err(io)?.sync_all().map_err(io)?;
            #[cfg(test)]
            transition_checkpoint(self, "source-renamed");
            if current.phase == TransitionPhase::Quarantined {
                let previous = current.clone();
                current.phase = TransitionPhase::SourceRetained;
                self.persist_record(&current, &previous)?;
            }
            verify(transaction)?;
            if retain_only { return Ok(current.clone()); }
            #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
            if self.runtime.is_some() {
                let actual = runtime_journal::RuntimeJournalRecord::read_active(&self.state_path)?.ok_or(CommitError::Conflict)?;
                if !matches!(actual.phase, runtime_journal::RuntimePhase::TargetRefenced | runtime_journal::RuntimePhase::TargetInstalled) {
                    return Err(CommitError::Conflict);
                }
            }
            match fs::symlink_metadata(&current.candidate_path) {
                Ok(_) => {
                    if current.phase != TransitionPhase::SourceRetained { return Err(CommitError::BindingMismatch); }
                    self.revalidate()?;
                    rename_unused(&current.candidate_path, &current.target.path)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io(error)),
            }
            self.revalidate()?;
            if Identity::file(&owned_directory(&current.target.path)?)? != Identity::file(&self.target)? {
                return Err(CommitError::BindingMismatch);
            }
            File::open(current.target.path.parent().ok_or(CommitError::InvalidInput)?).map_err(io)?.sync_all().map_err(io)?;
            #[cfg(test)]
            transition_checkpoint(self, "target-renamed");
            verify(transaction)?;
            if current.phase == TransitionPhase::SourceRetained {
                let previous = current.clone();
                current.phase = TransitionPhase::TargetInstalled;
                self.persist_record(&current, &previous)?;
            }
            Ok(current.clone())
        })
    }
}

// Decoded journal data never becomes a runtime CommitBinding.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedBinding {
    table: String,
    workspace: String,
    roots: Vec<String>,
    scope: String,
    generation: String,
    epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransitionPhase {
    Prepared,
    Quarantined,
    SourceRetained,
    TargetInstalled,
    Published,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionEndpoint {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    /// Canonical serialized CommitBinding, retained without inferring authority.
    pub binding: String,
}
impl TransitionEndpoint {
    fn matches(&self, coordinator: &IndexCommitCoordinator) -> Result<bool, CommitError> {
        Ok(self.path == coordinator.index_path
            && self.device == coordinator.index_identity.device
            && self.inode == coordinator.index_identity.inode
            && self.binding
                == serde_json::to_string(coordinator.binding())
                    .map_err(|_| CommitError::Unavailable)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionRecord {
    pub version: u32,
    pub phase: TransitionPhase,
    pub state_device: u64,
    pub state_inode: u64,
    pub state_token: String,
    pub source: TransitionEndpoint,
    pub target: TransitionEndpoint,
    pub candidate_path: PathBuf,
    pub archive_path: PathBuf,
    pub quarantine_generation: String,
    pub quarantine_epoch: u64,
    pub snapshot_sha256: String,
    pub asset_sha256: String,
    pub ready_sha256: String,
    pub adoption_sha256: String,
    pub backup_path: PathBuf,
    pub backup: OfflineBackup,
}

/// Append to the complete filename, avoiding collisions between state.db/state.sql.
pub fn journal_path(state: &Path) -> Result<PathBuf, CommitError> {
    let mut name = state
        .file_name()
        .ok_or(CommitError::InvalidInput)?
        .to_os_string();
    name.push(".lexical-transition-v1.json");
    Ok(state.with_file_name(name))
}

/// Keep the retained SQLite backup distinct from operational state, the
/// transition journal and either physical index generation. A misplaced
/// "backup" must never become a recovery authority for those live artifacts.
fn validate_artifact_paths(state: &Path, record: &TransitionRecord) -> Result<(), CommitError> {
    validate_artifact_locations(
        state,
        [
            &record.source.path,
            &record.candidate_path,
            &record.archive_path,
        ],
        &record.backup_path,
    )
}

fn validate_artifact_locations(
    state: &Path,
    directories: [&Path; 3],
    backup: &Path,
) -> Result<(), CommitError> {
    let journal = journal_path(state)?;
    if backup == state || backup == journal {
        return Err(CommitError::InvalidInput);
    }
    for directory in directories {
        if state.starts_with(directory)
            || journal.starts_with(directory)
            || backup.starts_with(directory)
        {
            return Err(CommitError::InvalidInput);
        }
    }
    Ok(())
}

fn canonical(record: &TransitionRecord) -> Result<Vec<u8>, CommitError> {
    let source: RecordedBinding =
        serde_json::from_str(&record.source.binding).map_err(|_| CommitError::InvalidInput)?;
    let target: RecordedBinding =
        serde_json::from_str(&record.target.binding).map_err(|_| CommitError::InvalidInput)?;
    let digest = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    for value in [
        &record.snapshot_sha256,
        &record.asset_sha256,
        &record.ready_sha256,
        &record.adoption_sha256,
        &record.backup.sha256,
    ] {
        if !digest(value) {
            return Err(CommitError::InvalidInput);
        }
    }
    for path in [
        &record.source.path,
        &record.target.path,
        &record.candidate_path,
        &record.archive_path,
        &record.backup_path,
    ] {
        if !path.is_absolute()
            || path.file_name().is_none()
            || path.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            })
        {
            return Err(CommitError::InvalidInput);
        }
    }
    let quarantine = source
        .epoch
        .checked_add(1)
        .ok_or(CommitError::InvalidInput)?;
    if source.epoch < 0
        || quarantine as u64 != record.quarantine_epoch
        || target.epoch <= quarantine
        || source.table != target.table
        || source.workspace != target.workspace
        || source.scope != target.scope
        || source.roots != target.roots
        || source.generation == target.generation
        || record.quarantine_generation == source.generation
        || record.quarantine_generation == target.generation
        || validate(&record.quarantine_generation).is_err()
        || record.state_token.len() != 32
        || !record.state_token.bytes().all(|b| b.is_ascii_hexdigit())
        || record.source.path != record.target.path
        || record.candidate_path == record.source.path
        || record.archive_path == record.source.path
        || record.candidate_path == record.archive_path
        || record.candidate_path.parent() != record.source.path.parent()
        || record.archive_path.parent() != record.source.path.parent()
        || (record.source.device, record.source.inode)
            == (record.target.device, record.target.inode)
        || serde_json::to_string(&source).map_err(|_| CommitError::InvalidInput)?
            != record.source.binding
        || serde_json::to_string(&target).map_err(|_| CommitError::InvalidInput)?
            != record.target.binding
    {
        return Err(CommitError::InvalidInput);
    }
    let bytes = serde_json::to_vec(record).map_err(|_| CommitError::InvalidInput)?;
    if record.version != 1 || bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(CommitError::InvalidInput);
    }
    Ok(bytes)
}

impl TransitionRecord {
    /// Exact recovery filename, not permission to retire this record.
    pub fn aborted_history_path(&self, state: &Path) -> Result<PathBuf, CommitError> {
        if self.phase != TransitionPhase::Prepared {
            return Err(CommitError::BindingMismatch);
        }
        retained_history_path(state, self, true)
    }

    /// Read only explicitly selected canonical aborted history. Its filename,
    /// contents and state location must agree; never discover history by scan.
    pub fn read_aborted(state: &Path, path: &Path) -> Result<Self, CommitError> {
        let record = Self::read_path(state, path)?.ok_or(CommitError::BindingMismatch)?;
        if record.aborted_history_path(state)? != path {
            return Err(CommitError::BindingMismatch);
        }
        Ok(record)
    }

    /// Read an explicitly named completed active record or its exact retained
    /// history filename. This is descriptive; current ownership must validate
    /// SQL identity and physical endpoint lineage before using it as provenance.
    #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
    pub(crate) fn read_completed(state: &Path, path: &Path) -> Result<Self, CommitError> {
        let record = Self::read_path(state, path)?.ok_or(CommitError::BindingMismatch)?;
        if record.phase != TransitionPhase::Complete
            || (path != journal_path(state)?
                && path != retained_history_path(state, &record, false)?)
        {
            return Err(CommitError::BindingMismatch);
        }
        Ok(record)
    }

    /// Bounded no-follow read. Unknown fields, noncanonical bytes and unsafe
    /// ownership are errors; an absent record is distinct from a corrupt one.
    pub fn read(state: &Path) -> Result<Option<Self>, CommitError> {
        let path = journal_path(state)?;
        Self::read_path(state, &path)
    }

    fn read_path(state: &Path, path: &Path) -> Result<Option<Self>, CommitError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io(error)),
        };
        super::offline::private_parent(path)?;
        let metadata = file.metadata().map_err(io)?;
        let owner = fs::metadata("/proc/self").map_err(io)?.uid();
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != owner
            || metadata.mode() & 0o077 != 0
            || metadata.len() > MAX_RECORD_BYTES
        {
            return Err(CommitError::BindingMismatch);
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        let record: Self =
            serde_json::from_slice(&bytes).map_err(|_| CommitError::BindingMismatch)?;
        if canonical(&record)? != bytes || Identity::read(path)? != Identity::file(&file)? {
            return Err(CommitError::BindingMismatch);
        }
        validate_artifact_paths(state, &record)?;
        Ok(Some(record))
    }

    /// Atomically persist a complete record and sync its directory. `previous`
    /// is the exact record expected by the exclusive offline owner; None permits
    /// only initial Prepared publication. Requires retained transition ownership;
    /// the caller must additionally hold exclusive SQL ownership during effects.
    /// An I/O error after publication can mean the new record already exists;
    /// reread actual state before recovery instead of retrying a predecessor.
    pub fn persist(
        &self,
        owner: &TransitionOwnership,
        previous: Option<&Self>,
    ) -> Result<(), CommitError> {
        #[cfg(all(feature = "local-lancedb", target_os = "linux"))]
        if owner.runtime.is_some() {
            return Err(CommitError::InvalidInput);
        }
        owner.revalidate()?;
        let mut expected = owner.record.clone();
        expected.phase = self.phase;
        if expected != *self {
            return Err(CommitError::BindingMismatch);
        }
        let state = &owner.state_path;
        let path = journal_path(state)?;
        super::offline::private_parent(&path)?;
        let bytes = canonical(self)?;
        validate_artifact_paths(state, self)?;
        if Self::read(state)?.as_ref() != previous {
            return Err(CommitError::Conflict);
        }
        if let Some(previous) = previous {
            let mut expected = previous.clone();
            expected.phase = self.phase;
            let valid = matches!(
                (previous.phase, self.phase),
                (TransitionPhase::Prepared, TransitionPhase::Quarantined)
                    | (
                        TransitionPhase::Quarantined,
                        TransitionPhase::SourceRetained
                    )
                    | (
                        TransitionPhase::SourceRetained,
                        TransitionPhase::TargetInstalled
                    )
                    | (TransitionPhase::TargetInstalled, TransitionPhase::Published)
                    | (TransitionPhase::Published, TransitionPhase::Complete)
            );
            if !valid || expected != *self {
                return Err(CommitError::InvalidInput);
            }
        } else if self.phase != TransitionPhase::Prepared {
            return Err(CommitError::InvalidInput);
        }
        let temporary = tempfile::Builder::new()
            .prefix(".nk-transition-")
            .tempfile_in(path.parent().ok_or(CommitError::InvalidInput)?)
            .map_err(io)?;
        temporary.as_file().write_all(&bytes).map_err(io)?;
        temporary.as_file().sync_all().map_err(io)?;
        if previous.is_none() {
            temporary
                .persist_noclobber(&path)
                .map_err(|error| io(error.error))?;
        } else {
            temporary.persist(&path).map_err(|error| io(error.error))?;
        }
        File::open(path.parent().ok_or(CommitError::InvalidInput)?)
            .map_err(io)?
            .sync_all()
            .map_err(io)?;
        #[cfg(test)]
        transition_checkpoint(
            owner,
            match self.phase {
                TransitionPhase::Prepared => "prepared",
                TransitionPhase::Quarantined => "quarantined",
                TransitionPhase::SourceRetained => "source-retained",
                TransitionPhase::TargetInstalled => "target-installed",
                TransitionPhase::Published => "published",
                TransitionPhase::Complete => "complete",
            },
        );
        Ok(())
    }
}

impl TransitionOwnership {
    /// Retain the previous completed record before this owner's next Prepared
    /// record is created. Copy/fsync history before removing the active record;
    /// retry accepts only identical already-retained history. No SQL is changed.
    pub fn retire_completed(
        &self,
        now: i64,
        previous: &TransitionRecord,
    ) -> Result<PathBuf, CommitError> {
        if previous.phase != TransitionPhase::Complete
            || previous.state_device != self.record.state_device
            || previous.state_inode != self.record.state_inode
            || previous.state_token != self.record.state_token
            || previous.target != self.record.source
        {
            return Err(CommitError::BindingMismatch);
        }
        self.retain_record(now, previous, false)
    }

    /// Abort only an unchanged Prepared source, retaining its journal and all
    /// backups/candidates as evidence. Quarantine or final SQL must reconcile;
    /// this never resets SQL or grants permission to reuse a fenced generation.
    pub fn abort_prepared(&self, now: i64) -> Result<PathBuf, CommitError> {
        let mut prepared = self.record.clone();
        prepared.phase = TransitionPhase::Prepared;
        self.retain_record(now, &prepared, true)
    }

    fn retain_record(
        &self,
        now: i64,
        previous: &TransitionRecord,
        aborted: bool,
    ) -> Result<PathBuf, CommitError> {
        let bytes = canonical(previous)?;
        let active = journal_path(&self.state_path)?;
        let history = retained_history_path(&self.state_path, previous, aborted)?;
        if history == self.record.backup_path
            || history == previous.backup_path
            || [
                &self.record.source.path,
                &self.record.candidate_path,
                &self.record.archive_path,
            ]
            .iter()
            .any(|directory| history.starts_with(directory))
        {
            return Err(CommitError::InvalidInput);
        }
        self.with_exclusive_sql(now, |transaction| {
            let expected_paths = (self.record.source.path.clone(), self.record.candidate_path.clone());
            if self.physical_paths()? != expected_paths { return Err(CommitError::BindingMismatch); }
            let source: RecordedBinding = serde_json::from_str(&self.record.source.binding).map_err(|_| CommitError::InvalidInput)?;
            let exact_source: bool = transaction.query_row(
                "SELECT index_device=?1 AND index_inode=?2 AND generation=?3 AND epoch=?4 FROM main.index_commit_binding WHERE singleton=1",
                params![self.record.source.device.to_string(),self.record.source.inode.to_string(),source.generation,source.epoch], |r|r.get(0)).map_err(sql)?;
            if !exact_source { return Err(CommitError::BindingMismatch); }
            let current = TransitionRecord::read(&self.state_path)?;
            if current.as_ref().is_some_and(|record| record != previous) { return Err(CommitError::Conflict); }
            match TransitionRecord::read_path(&self.state_path, &history)? {
                Some(record) if record == *previous => {},
                Some(_) => return Err(CommitError::BindingMismatch),
                None => {
                    if current.is_none() { return Err(CommitError::BindingMismatch); }
                    let temporary = tempfile::Builder::new().prefix(".nk-history-")
                        .tempfile_in(active.parent().ok_or(CommitError::InvalidInput)?).map_err(io)?;
                    temporary.as_file().write_all(&bytes).map_err(io)?;
                    temporary.as_file().sync_all().map_err(io)?;
                    temporary.persist_noclobber(&history).map_err(|error| io(error.error))?;
                }
            }
            let parent = File::open(active.parent().ok_or(CommitError::InvalidInput)?).map_err(io)?;
            parent.sync_all().map_err(io)?;
            #[cfg(test)]
            if aborted { transition_checkpoint(self, "abort-history-retained"); }
            if TransitionRecord::read_path(&self.state_path,&history)?.as_ref() != Some(previous)
                || TransitionRecord::read(&self.state_path)? != current { return Err(CommitError::Conflict); }
            self.revalidate()?;
            if self.physical_paths()? != expected_paths { return Err(CommitError::BindingMismatch); }
            if current.is_some() { fs::remove_file(&active).map_err(io)?; }
            parent.sync_all().map_err(io)?;
            #[cfg(test)]
            if aborted { transition_checkpoint(self, "abort-history-unlinked"); }
            Ok(history.clone())
        })
    }
}

fn retained_history_path(
    state: &Path,
    record: &TransitionRecord,
    aborted: bool,
) -> Result<PathBuf, CommitError> {
    use sha2::{Digest, Sha256};
    let active = journal_path(state)?;
    let mut name = active
        .file_name()
        .ok_or(CommitError::InvalidInput)?
        .to_os_string();
    let suffix = if aborted {
        ".prepared-aborted.history"
    } else {
        ".history"
    };
    name.push(format!(".{:x}{suffix}", Sha256::digest(canonical(record)?)));
    Ok(active.with_file_name(name))
}

pub(super) fn check_admission(coordinator: &IndexCommitCoordinator) -> Result<(), CommitError> {
    let Some(record) = TransitionRecord::read(&coordinator.state_path)? else {
        return Ok(());
    };
    if record.state_device != coordinator.state_identity.device
        || record.state_inode != coordinator.state_identity.inode
        || record.state_token != coordinator.state_token
    {
        return Err(CommitError::BindingMismatch);
    }
    let endpoint = match record.phase {
        TransitionPhase::Prepared => &record.source,
        TransitionPhase::Complete => &record.target,
        _ => return Err(CommitError::Conflict),
    };
    if endpoint.matches(coordinator)? {
        Ok(())
    } else {
        Err(CommitError::BindingMismatch)
    }
}
