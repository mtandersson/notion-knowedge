//! Version-two active publication journal. Version-one readers must refuse this
//! record while it occupies the existing admission journal path. Completion
//! retains history durably before removing that admission blocker.
use super::*;
use crate::{
    chunks::RuntimeSourceSnapshot,
    rollback_runtime::{prepared::PreparedRollbackMetadata, source::canonical as bounded_json},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
const MAX_BYTES: usize = 524288;
const PROTOCOL: &str = "selected-runtime-publication-v2";
fn invalid<T>(_: T) -> CommitError {
    CommitError::BindingMismatch
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimePhase {
    Prepared,
    Quarantined,
    SourceRetained,
    TargetRefenced,
    TargetInstalled,
    Published,
    Complete,
}
impl RuntimePhase {
    fn refenced(self) -> bool {
        matches!(
            self,
            Self::TargetRefenced | Self::TargetInstalled | Self::Published | Self::Complete
        )
    }
}
/// Descriptive private journal data. Parsing cannot create a publication owner,
/// validate a native pair or authorize SQL/rename effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeJournalRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: RuntimePhase,
    pub state_device: u64,
    pub state_inode: u64,
    pub state_token: String,
    pub prepared: PreparedRollbackMetadata,
    pub prepared_sha256: String,
    /// Explicit immutable history names after retiring the previous active
    /// completed journal. Their contents must retain the verified lineage hash.
    pub lineage_paths: Vec<PathBuf>,
    /// Fresh unused sibling where the current physical source will be retained.
    pub archive_path: PathBuf,
    pub activation_backup_path: PathBuf,
    pub activation_backup: OfflineBackup,
    pub adoption_sha256: String,
    /// Actual selected-runtime snapshot after fresh physical refencing. Rename
    /// changes later physical inventories; it never permits row/plan changes.
    pub refenced: Option<RuntimeSourceSnapshot>,
}
impl RuntimeJournalRecord {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CommitError> {
        let prepared = self.prepared.canonical_bytes().map_err(invalid)?;
        let candidate = &self.prepared.candidate;
        let absolute = |path: &Path| {
            path.is_absolute()
                && path.file_name().is_some()
                && !path.components().any(|p| {
                    matches!(
                        p,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
        };
        if self.version != 2
            || self.protocol != PROTOCOL
            || self.state_token.len() != 32
            || !self.state_token.bytes().all(|b| b.is_ascii_hexdigit())
            || self.prepared_sha256 != format!("{:x}", Sha256::digest(prepared))
            || self.lineage_paths.is_empty()
            || self.lineage_paths.len() > 64
            || self.lineage_paths.iter().any(|p| !absolute(p))
            || !absolute(&self.archive_path)
            || !absolute(&self.activation_backup_path)
            || self.archive_path.parent() != self.prepared.current.path.parent()
            || [
                &self.prepared.current.path,
                &candidate.candidate.path,
                &self.prepared.prior_archive.path,
            ]
            .contains(&&self.archive_path)
            || [
                &self.prepared.current.path,
                &candidate.candidate.path,
                &self.prepared.prior_archive.path,
                &self.archive_path,
            ]
            .iter()
            .any(|p| {
                self.activation_backup_path.starts_with(p)
                    || self.prepared.state_path.starts_with(p)
            })
            || self.activation_backup_path == self.prepared.state_path
            || self.activation_backup_path == journal_path(&self.prepared.state_path)?
            || self.activation_backup != candidate.current.metadata.operational
            || !digest(&self.activation_backup.sha256)
            || !digest(&self.adoption_sha256)
            || self.phase.refenced() != self.refenced.is_some()
        {
            return Err(CommitError::BindingMismatch);
        }
        if let Some(snapshot) = &self.refenced {
            if snapshot.version != 2
                || snapshot.source_version == 0
                || snapshot.rows != candidate.current.metadata.source.rows
                || snapshot.snapshot_sha256 != candidate.current.metadata.source.snapshot_sha256
                || !digest(&snapshot.authority_sha256)
            {
                return Err(CommitError::BindingMismatch);
            }
            let target = CommitBinding::parse_retained(candidate.target_authority_json.as_bytes())?;
            match &snapshot.lexical {
                Some(binding) if candidate.prior.descriptor.describes_bound_source(binding) => {
                    binding
                        .validate_transfer_metadata(&candidate.current.metadata.embedding, &target)
                        .map_err(invalid)?;
                }
                None if candidate.prior.descriptor.is_legacy() => (),
                _ => return Err(CommitError::BindingMismatch),
            }
        }
        bounded_json(self, MAX_BYTES).map_err(invalid)
    }
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self, CommitError> {
        if bytes.len() > MAX_BYTES {
            return Err(CommitError::InvalidInput);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(invalid)?;
        if value.canonical_bytes()? != bytes {
            return Err(CommitError::BindingMismatch);
        }
        Ok(value)
    }
}

impl RuntimeJournalRecord {
    pub(crate) fn same_operation(&self, base: &Self) -> Result<(), CommitError> {
        let mut immutable = self.clone();
        immutable.phase = base.phase;
        immutable.refenced = base.refenced.clone();
        if immutable != *base {
            return Err(CommitError::BindingMismatch);
        }
        Ok(())
    }
    /// Internal common SQL/physical layout only, never a version-one native
    /// verification record or a serializable activation artifact.
    pub(crate) fn layout(&self) -> Result<TransitionRecord, CommitError> {
        self.canonical_bytes()?;
        let candidate = &self.prepared.candidate;
        let phase = match self.phase {
            RuntimePhase::Prepared => TransitionPhase::Prepared,
            RuntimePhase::Quarantined => TransitionPhase::Quarantined,
            RuntimePhase::SourceRetained | RuntimePhase::TargetRefenced => {
                TransitionPhase::SourceRetained
            }
            RuntimePhase::TargetInstalled => TransitionPhase::TargetInstalled,
            RuntimePhase::Published => TransitionPhase::Published,
            RuntimePhase::Complete => TransitionPhase::Complete,
        };
        let record = TransitionRecord {
            version: 1,
            phase,
            state_device: self.state_device,
            state_inode: self.state_inode,
            state_token: self.state_token.clone(),
            source: TransitionEndpoint {
                path: self.prepared.current.path.clone(),
                device: self.prepared.current.device,
                inode: self.prepared.current.inode,
                binding: candidate.current.metadata.authority_json.clone(),
            },
            target: TransitionEndpoint {
                path: self.prepared.current.path.clone(),
                device: candidate.candidate.device,
                inode: candidate.candidate.inode,
                binding: candidate.target_authority_json.clone(),
            },
            candidate_path: candidate.candidate.path.clone(),
            archive_path: self.archive_path.clone(),
            quarantine_generation: self.prepared.quarantine_generation.clone(),
            quarantine_epoch: self.prepared.quarantine_epoch,
            snapshot_sha256: candidate.current.metadata.source.snapshot_sha256.clone(),
            // These projection slots are opaque to SQL/physical helpers. The
            // runtime protocol verifies explicit legacy absence and its own
            // Prepared evidence, not a v1 dictionary READY marker.
            asset_sha256: candidate
                .asset_sha256
                .clone()
                .unwrap_or_else(|| "0".repeat(64)),
            ready_sha256: self.prepared_sha256.clone(),
            adoption_sha256: self.adoption_sha256.clone(),
            backup_path: self.activation_backup_path.clone(),
            backup: self.activation_backup.clone(),
        };
        canonical(&record)?;
        Ok(record)
    }
    pub(crate) fn read_active(state: &Path) -> Result<Option<Self>, CommitError> {
        let path = journal_path(state)?;
        let Some(bytes) = read_bytes(&path)? else {
            return Ok(None);
        };
        let record = Self::parse_metadata(&bytes)?;
        if record.prepared.state_path != state {
            return Err(CommitError::BindingMismatch);
        }
        Ok(Some(record))
    }
}
fn read_bytes(path: &Path) -> Result<Option<Vec<u8>>, CommitError> {
    use crate::rollback_runtime::evidence::{check_parent, parent};
    use std::{
        io::Read,
        os::{fd::AsRawFd, unix::fs::MetadataExt},
    };
    let directory = parent(path).map_err(invalid)?;
    let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
        .join(path.file_name().ok_or(CommitError::InvalidInput)?);
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&anchored)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io(error)),
    };
    let before = file.metadata().map_err(io)?;
    if !before.is_file()
        || before.nlink() != 1
        || before.mode() & 0o077 != 0
        || before.uid() != fs::metadata("/proc/self").map_err(io)?.uid()
        || before.len() > MAX_BYTES as u64
    {
        return Err(CommitError::BindingMismatch);
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    let after = fs::symlink_metadata(&anchored).map_err(io)?;
    if !after.is_file()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.nlink() != 1
        || after.len() != before.len()
        || after.uid() != before.uid()
        || after.mode() != before.mode()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
    {
        return Err(CommitError::BindingMismatch);
    }
    check_parent(&directory, path).map_err(invalid)?;
    Ok(Some(bytes))
}
impl TransitionOwnership {
    pub(crate) fn runtime_record(&self) -> Result<RuntimeJournalRecord, CommitError> {
        let record =
            RuntimeJournalRecord::read_active(&self.state_path)?.ok_or(CommitError::Conflict)?;
        record.same_operation(self.runtime.as_ref().ok_or(CommitError::BindingMismatch)?)?;
        Ok(record)
    }
    pub(crate) fn acquire_runtime(record: &RuntimeJournalRecord) -> Result<Self, CommitError> {
        let mut owner = Self::acquire(&record.prepared.state_path, &record.layout()?)?;
        owner.runtime = Some(record.clone());
        Ok(owner)
    }
}
pub(super) fn advance_layout(
    owner: &TransitionOwnership,
    next: &TransitionRecord,
    previous: &TransitionRecord,
) -> Result<(), CommitError> {
    let mut current =
        RuntimeJournalRecord::read_active(&owner.state_path)?.ok_or(CommitError::Conflict)?;
    current.same_operation(owner.runtime.as_ref().ok_or(CommitError::BindingMismatch)?)?;
    if current.layout()? != *previous {
        return Err(CommitError::Conflict);
    }
    let old = current.clone();
    current.phase = match (current.phase, next.phase) {
        (RuntimePhase::Prepared, TransitionPhase::Quarantined) => RuntimePhase::Quarantined,
        (RuntimePhase::Quarantined, TransitionPhase::SourceRetained) => {
            RuntimePhase::SourceRetained
        }
        (RuntimePhase::TargetRefenced, TransitionPhase::TargetInstalled) => {
            RuntimePhase::TargetInstalled
        }
        (RuntimePhase::TargetInstalled, TransitionPhase::Published) => RuntimePhase::Published,
        (RuntimePhase::Published, TransitionPhase::Complete) => RuntimePhase::Complete,
        _ => return Err(CommitError::InvalidInput),
    };
    if current.layout()? != *next {
        return Err(CommitError::BindingMismatch);
    }
    persist(owner, &current, Some(&old))
}
fn persist(
    owner: &TransitionOwnership,
    next: &RuntimeJournalRecord,
    previous: Option<&RuntimeJournalRecord>,
) -> Result<(), CommitError> {
    let expected = previous.map(|p| p.canonical_bytes()).transpose()?;
    persist_expected(owner, next, expected)
}
fn persist_expected(
    owner: &TransitionOwnership,
    next: &RuntimeJournalRecord,
    expected: Option<Vec<u8>>,
) -> Result<(), CommitError> {
    use crate::rollback_runtime::evidence::{check_parent, parent};
    use std::os::{
        fd::AsRawFd,
        unix::{ffi::OsStrExt, fs::PermissionsExt},
    };
    owner.revalidate()?;
    next.same_operation(owner.runtime.as_ref().ok_or(CommitError::BindingMismatch)?)?;
    let path = journal_path(&owner.state_path)?;
    let directory = parent(&path).map_err(invalid)?;
    if read_bytes(&path)? != expected {
        return Err(CommitError::Conflict);
    }
    let root = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    let temporary = tempfile::Builder::new()
        .prefix(".nk-runtime-journal-")
        .permissions(fs::Permissions::from_mode(0o600))
        .tempfile_in(&root)
        .map_err(io)?;
    let (mut file, temporary) = temporary.keep().map_err(|e| io(e.error))?;
    file.write_all(&next.canonical_bytes()?).map_err(io)?;
    file.sync_all().map_err(io)?;
    check_parent(&directory, &path).map_err(invalid)?;
    if read_bytes(&path)? != expected {
        return Err(CommitError::Conflict);
    }
    let from = std::ffi::CString::new(
        temporary
            .file_name()
            .ok_or(CommitError::InvalidInput)?
            .as_bytes(),
    )
    .map_err(invalid)?;
    let to = std::ffi::CString::new(
        path.file_name()
            .ok_or(CommitError::InvalidInput)?
            .as_bytes(),
    )
    .map_err(invalid)?;
    // SAFETY: owned retained directory and terminated single filenames. Initial
    // publication is no-clobber; updates require the exact verified predecessor.
    let result = unsafe {
        libc::renameat2(
            directory.as_raw_fd(),
            from.as_ptr(),
            directory.as_raw_fd(),
            to.as_ptr(),
            if expected.is_none() {
                libc::RENAME_NOREPLACE
            } else {
                0
            },
        )
    };
    if result != 0 {
        return Err(io(std::io::Error::last_os_error()));
    }
    directory.sync_all().map_err(io)?;
    check_parent(&directory, &path).map_err(invalid)?;
    if RuntimeJournalRecord::read_active(&owner.state_path)?.as_ref() != Some(next) {
        return Err(CommitError::Conflict);
    }
    Ok(())
}

/// Classify only after bounded input; each branch subsequently uses its strict
/// complete parser. Unknown/corrupt records are never treated as an empty slot.
pub(crate) enum ExistingJournal {
    Missing,
    Legacy(Box<TransitionRecord>),
    Runtime(Box<RuntimeJournalRecord>),
}
pub(crate) fn existing(state: &Path) -> Result<ExistingJournal, CommitError> {
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let path = journal_path(state)?;
    let Some(bytes) = read_bytes(&path)? else {
        return Ok(ExistingJournal::Missing);
    };
    let version: Version = serde_json::from_slice(&bytes).map_err(invalid)?;
    match version.version {
        1 => Ok(ExistingJournal::Legacy(Box::new(
            TransitionRecord::read(state)?.ok_or(CommitError::Conflict)?,
        ))),
        2 => Ok(ExistingJournal::Runtime(Box::new(
            RuntimeJournalRecord::parse_metadata(&bytes)?,
        ))),
        _ => Err(CommitError::InvalidInput),
    }
}
pub(crate) fn completed_path(
    state: &Path,
    record: &TransitionRecord,
) -> Result<PathBuf, CommitError> {
    if record.phase != TransitionPhase::Complete {
        return Err(CommitError::Conflict);
    }
    retained_history_path(state, record, false)
}
impl TransitionOwnership {
    /// History is copied and synced BEFORE replacing the previous admission
    /// record. There is no missing-journal crash window between these intents.
    /// Caller must freshly verify native state in this actual transaction.
    pub(crate) fn begin_runtime(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        record: &RuntimeJournalRecord,
        previous: Option<&TransitionRecord>,
    ) -> Result<(), CommitError> {
        use crate::rollback_runtime::evidence::{check_parent, parent};
        use std::os::{fd::AsRawFd, unix::fs::PermissionsExt};
        self.validate_owned_transaction(transaction)?;
        if record.phase != RuntimePhase::Prepared
            || record.refenced.is_some()
            || record.layout()? != self.record
            || TransitionRecord::read(&self.state_path)?.as_ref() != previous
        {
            return Err(CommitError::Conflict);
        }
        let source = CommitBinding::parse_retained(self.record.source.binding.as_bytes())?;
        let exact: bool = transaction
            .query_row(
                "SELECT generation=?1 AND epoch=?2 FROM index_commit_binding WHERE singleton=1",
                params![
                    source.generation(),
                    i64::try_from(source.epoch()).map_err(invalid)?
                ],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if !exact {
            return Err(CommitError::BindingMismatch);
        }
        let expected = previous.map(canonical).transpose()?;
        if let Some(previous) = previous {
            if previous.phase != TransitionPhase::Complete
                || previous.target != self.record.source
                || previous.state_device != record.state_device
                || previous.state_inode != record.state_inode
                || previous.state_token != record.state_token
            {
                return Err(CommitError::BindingMismatch);
            }
            let history = completed_path(&self.state_path, previous)?;
            let active = journal_path(&self.state_path)?;
            if record.prepared.lineage_paths.iter().any(|p| p == &active)
                && !record.lineage_paths.iter().any(|p| p == &history)
            {
                return Err(CommitError::BindingMismatch);
            }
            let directory = parent(&history).map_err(invalid)?;
            match read_bytes(&history)? {
                Some(bytes) if expected.as_ref() == Some(&bytes) => (),
                Some(_) => return Err(CommitError::Conflict),
                None => {
                    let root = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
                    let temporary = tempfile::Builder::new()
                        .prefix(".nk-runtime-history-")
                        .permissions(fs::Permissions::from_mode(0o600))
                        .tempfile_in(&root)
                        .map_err(io)?;
                    temporary
                        .as_file()
                        .write_all(expected.as_ref().ok_or(CommitError::Conflict)?)
                        .map_err(io)?;
                    temporary.as_file().sync_all().map_err(io)?;
                    let destination =
                        root.join(history.file_name().ok_or(CommitError::InvalidInput)?);
                    temporary
                        .persist_noclobber(&destination)
                        .map_err(|e| io(e.error))?;
                }
            }
            directory.sync_all().map_err(io)?;
            check_parent(&directory, &history).map_err(invalid)?;
            if TransitionRecord::read_completed(&self.state_path, &history)? != *previous {
                return Err(CommitError::Conflict);
            }
        }
        self.validate_owned_transaction(transaction)?;
        persist_expected(self, record, expected)?;
        self.validate_owned_transaction(transaction)
    }
}
