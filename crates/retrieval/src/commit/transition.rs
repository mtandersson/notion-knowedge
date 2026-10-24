//! Durable transition record storage and ordinary coordinator admission.
//! These primitives do not verify a native candidate or activate an index.
//! The offline transition owner must hold state/index/SQL exclusion throughout
//! publication; recovery must inspect physical and SQL authority, not phase alone.
use super::*;
use std::os::unix::fs::OpenOptionsExt;

const MAX_RECORD_BYTES: u64 = 65_536;

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
    /// Bounded no-follow read. Unknown fields, noncanonical bytes and unsafe
    /// ownership are errors; an absent record is distinct from a corrupt one.
    pub fn read(state: &Path) -> Result<Option<Self>, CommitError> {
        let path = journal_path(state)?;
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io(error)),
        };
        super::offline::private_parent(&path)?;
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
        if canonical(&record)? != bytes || Identity::read(&path)? != Identity::file(&file)? {
            return Err(CommitError::BindingMismatch);
        }
        Ok(Some(record))
    }

    /// Atomically persist a complete record and sync its directory. `previous`
    /// is the exact record expected by the exclusive offline owner; None permits
    /// only initial Prepared publication. This does not acquire transition locks.
    /// An I/O error after publication can mean the new record already exists;
    /// reread actual state before recovery instead of retrying a predecessor.
    pub fn persist(&self, state: &Path, previous: Option<&Self>) -> Result<(), CommitError> {
        let path = journal_path(state)?;
        super::offline::private_parent(&path)?;
        let bytes = canonical(self)?;
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
            .map_err(io)
    }
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
