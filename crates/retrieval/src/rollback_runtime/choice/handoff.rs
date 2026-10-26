//! Descriptive version-three handoff metadata. It never grants ownership.
use super::*;
use crate::rollback_runtime::evidence::{check_parent, parent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
};

const MAX_BYTES: usize = 2 << 20;
const PROTOCOL: &str = "explicit-choice-handoff-v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandoffPhase {
    Handoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedFileIdentity {
    pub device: u64,
    pub inode: u64,
    pub mtime: i64,
    pub mtime_nsec: i64,
    pub ctime: i64,
    pub ctime_nsec: i64,
}

/// Strict metadata for a future active journal. Parsing or constructing this
/// value does not transfer the original owner or authorize any effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceHandoffRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: HandoffPhase,
    pub prepared_path: PathBuf,
    pub prepared: PreparedChoiceMetadata,
    pub prepared_sha256: String,
    pub prepared_identity: RetainedFileIdentity,
    pub transfer_identity: RetainedFileIdentity,
    pub backup_identity: RetainedFileIdentity,
    pub source_evidence_identity: Option<RetainedFileIdentity>,
    pub source_evidence_sha256: Option<String>,
    pub origin_history_path: PathBuf,
    pub origin_sha256: String,
    pub origin_identity: RetainedFileIdentity,
}

fn history_path(state: &Path, origin_sha256: &str) -> Result<PathBuf> {
    let active = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
    let mut name = active.file_name().ok_or(RuntimeError)?.to_os_string();
    name.push(format!(".{origin_sha256}.choice-origin.history"));
    Ok(active.with_file_name(name))
}

impl ChoiceHandoffRecord {
    /// Read a bounded active description. This is not an ownership proof.
    pub fn read_active_metadata(state: &Path) -> Result<Self> {
        let path = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
        let directory = parent(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(anchored(&directory, &path)?)?;
        let mut bytes = Vec::new();
        file.take((MAX_BYTES + 1) as u64).read_to_end(&mut bytes)?;
        let record = Self::parse_metadata(&bytes)?;
        if record.prepared.selection.state_path != state {
            return Err(RuntimeError);
        }
        check_parent(&directory, &path)?;
        record.verify_active()?;
        Ok(record)
    }
    pub(crate) fn verify_active(&self) -> Result<()> {
        let path = crate::commit::transition::journal_path(&self.prepared.selection.state_path)
            .map_err(|_| RuntimeError)?;
        let bytes = self.canonical_bytes()?;
        let (_, length, sha) = read_private(&parent(&path)?, &path, MAX_BYTES as u64)?;
        if length != bytes.len() as u64 || sha != format!("{:x}", Sha256::digest(bytes)) {
            return Err(RuntimeError);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let prepared = self.prepared.canonical_bytes()?;
        let state = &self.prepared.selection.state_path;
        let journal = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
        let origin = &self.prepared.selection.origin;
        let layout = match origin {
            Origin::Activation(record) => {
                use crate::commit::transition::TransitionPhase;
                if matches!(
                    record.phase,
                    TransitionPhase::Published | TransitionPhase::Complete
                ) {
                    return Err(RuntimeError);
                }
                *record.clone()
            }
            Origin::Runtime(record) => {
                use crate::commit::transition::runtime_journal::RuntimePhase;
                if record.prepared.state_path != *state
                    || matches!(
                        record.phase,
                        RuntimePhase::Published | RuntimePhase::Complete
                    )
                {
                    return Err(RuntimeError);
                }
                record.layout().map_err(|_| RuntimeError)?
            }
        };
        let mut protected = vec![
            &layout.source.path,
            &layout.candidate_path,
            &layout.archive_path,
            &self.prepared.selection.current.path,
            &self.prepared.selection.selected.path,
            &self.prepared.candidate.path,
        ];
        if let Origin::Runtime(record) = origin {
            protected.push(&record.prepared.prior_archive.path);
        }
        let valid_path = |path: &Path| {
            path.is_absolute()
                && path.file_name().is_some()
                && !path.components().any(|p| {
                    matches!(
                        p,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
        };
        if self.version != 3
            || self.protocol != PROTOCOL
            || self.phase != HandoffPhase::Handoff
            || !valid_path(&self.prepared_path)
            || self.prepared_sha256 != format!("{:x}", Sha256::digest(prepared))
            || self.origin_sha256 != self.prepared.selection.operation_sha256
            || self.prepared.source_evidence_path.is_some()
                != self.source_evidence_identity.is_some()
            || self.prepared.source_evidence_path.is_some() != self.source_evidence_sha256.is_some()
            || self.source_evidence_sha256.as_ref().is_some_and(|digest| {
                digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            || [
                &self.prepared_identity,
                &self.transfer_identity,
                &self.backup_identity,
                &self.origin_identity,
            ]
            .into_iter()
            .chain(self.source_evidence_identity.iter())
            .any(|identity| {
                !(0..1_000_000_000).contains(&identity.mtime_nsec)
                    || !(0..1_000_000_000).contains(&identity.ctime_nsec)
            })
            || self.origin_history_path != history_path(state, &self.origin_sha256)?
            || self.prepared_path == journal
            || self.prepared_path == self.origin_history_path
            || self.prepared_path == *state
            || self.prepared_path == self.prepared.transfer_path
            || self.prepared_path == self.prepared.backup_path
            || self.prepared.source_evidence_path.as_ref() == Some(&self.prepared_path)
            || protected.iter().any(|p| self.prepared_path.starts_with(p))
            || self.origin_history_path == self.prepared.transfer_path
            || self.origin_history_path == self.prepared.backup_path
            || self.prepared.source_evidence_path.as_ref() == Some(&self.origin_history_path)
            || protected
                .iter()
                .any(|p| self.origin_history_path.starts_with(p))
        {
            return Err(RuntimeError);
        }
        source::canonical(self, MAX_BYTES)
    }

    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if value.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(value)
    }
}

/// Reacquire and freshly verify a v1-origin handoff. The explicit inputs must
/// come from the caller's trusted configuration. No authority escapes this
/// check, and recovery performs no publication effects.
pub fn recover_activation_handoff(
    record: &ChoiceHandoffRecord,
    candidate: crate::chunks::TransitionCandidateInputs<'_>,
    inputs: SelectionInputs<'_>,
    limits: PreparedLimits,
    capture_limits: crate::commit::transition::source::SourceCaptureLimits,
    now: i64,
) -> Result<()> {
    if !matches!(record.prepared.selection.origin, Origin::Activation(_)) {
        return Err(RuntimeError);
    }
    let owner = crate::commit::transition::source::acquire_choice_recovery(record)
        .map_err(|_| RuntimeError)?;
    owner
        .with_quarantined_source_inspection(now, candidate, capture_limits, |inspection| {
            let proof = super::reopen(inspection, inputs, &record.prepared_path, limits)
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            if proof.metadata() != &record.prepared {
                return Err(crate::commit::CommitError::BindingMismatch);
            }
            proof
                .revalidate()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            record
                .verify_retained_files()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)
        })
        .map_err(|_| RuntimeError)?;
    let path = crate::commit::transition::journal_path(&record.prepared.selection.state_path)
        .map_err(|_| RuntimeError)?;
    parent(&path)?.sync_all()?;
    record.verify_active()?;
    record.verify_retained_files()
}

fn anchored(directory: &File, path: &Path) -> Result<PathBuf> {
    Ok(
        PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .join(path.file_name().ok_or(RuntimeError)?),
    )
}

fn read_private(
    directory: &File,
    path: &Path,
    max: u64,
) -> Result<(RetainedFileIdentity, u64, String)> {
    let at = anchored(directory, path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&at)?;
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.mode() & 0o077 != 0
        || before.uid() != fs::metadata("/proc/self")?.uid()
        || before.len() > max
    {
        return Err(RuntimeError);
    }
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let remaining = max
            .saturating_sub(bytes)
            .saturating_add(1)
            .min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..remaining])?;
        if count == 0 {
            break;
        }
        bytes = bytes.checked_add(count as u64).ok_or(RuntimeError)?;
        if bytes > max {
            return Err(RuntimeError);
        }
        hash.update(&buffer[..count]);
    }
    let after = fs::symlink_metadata(&at)?;
    if !after.is_file()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.nlink() != 1
        || after.len() != before.len()
        || after.mode() != before.mode()
        || after.uid() != before.uid()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
        || bytes != before.len()
    {
        return Err(RuntimeError);
    }
    check_parent(directory, path)?;
    Ok((
        RetainedFileIdentity {
            device: before.dev(),
            inode: before.ino(),
            mtime: before.mtime(),
            mtime_nsec: before.mtime_nsec(),
            ctime: before.ctime(),
            ctime_nsec: before.ctime_nsec(),
        },
        bytes,
        format!("{:x}", hash.finalize()),
    ))
}

impl ChoiceHandoffRecord {
    /// Verify retained bytes and physical identity. This never establishes SQL,
    /// index or runtime authority and is not a handoff capability.
    pub fn verify_retained_files(&self) -> Result<()> {
        let prepared_bytes = self.prepared.canonical_bytes()?;
        self.canonical_bytes()?;
        let files = [
            (
                &self.prepared_path,
                &self.prepared_identity,
                prepared_bytes.len() as u64,
                &self.prepared_sha256,
            ),
            (
                &self.prepared.transfer_path,
                &self.transfer_identity,
                self.prepared.transfer.bytes,
                &self.prepared.transfer.sha256,
            ),
            (
                &self.prepared.backup_path,
                &self.backup_identity,
                self.prepared.backup.bytes,
                &self.prepared.backup.sha256,
            ),
        ];
        for (path, expected_identity, expected_bytes, expected_sha) in files {
            let (identity, bytes, sha) = read_private(&parent(path)?, path, expected_bytes)?;
            if &identity != expected_identity || bytes != expected_bytes || &sha != expected_sha {
                return Err(RuntimeError);
            }
        }
        if let (Some(path), Some(expected_identity), Some(expected_sha)) = (
            &self.prepared.source_evidence_path,
            &self.source_evidence_identity,
            &self.source_evidence_sha256,
        ) {
            let (identity, _, sha) = read_private(&parent(path)?, path, 65536)?;
            if &identity != expected_identity || &sha != expected_sha {
                return Err(RuntimeError);
            }
        }
        let (origin_id, _, origin_sha) = read_private(
            &parent(&self.origin_history_path)?,
            &self.origin_history_path,
            512 << 10,
        )?;
        if origin_id != self.origin_identity || origin_sha != self.origin_sha256 {
            return Err(RuntimeError);
        }
        Ok(())
    }
}

impl VerifiedPreparedChoice<'_, '_> {
    /// Request a handoff in the enclosing handoff transaction. A regular
    /// inspection refuses staged requests and performs no active-journal swap.
    pub fn stage_handoff(&self, prepared_path: &Path) -> Result<()> {
        let record = self.retain_handoff_origin(prepared_path)?;
        self.revalidate()?;
        let guard = self.guard.as_ref().map(File::try_clone).transpose()?;
        self.selection
            .inspection
            .stage_choice_handoff(record, guard, self.selection.limits.transfer.native)
            .map_err(|_| RuntimeError)
    }
    /// Retain the exact originating journal under the original owner. The
    /// returned record is descriptive; the active journal is unchanged and
    /// no ownership transfer or serving permission occurs here.
    pub fn retain_handoff_origin(&self, prepared_path: &Path) -> Result<ChoiceHandoffRecord> {
        self.revalidate()?;
        let inspection = self.selection.inspection;
        inspection
            .choice_output(prepared_path)
            .map_err(|_| RuntimeError)?;
        let prepared_parent = parent(prepared_path)?;
        let prepared = self.metadata().clone();
        let canonical_prepared = prepared.canonical_bytes()?;
        let (prepared_identity, prepared_size, prepared_sha256) =
            read_private(&prepared_parent, prepared_path, prepared::MAX as u64)?;
        if prepared_size != canonical_prepared.len() as u64
            || prepared_sha256 != format!("{:x}", Sha256::digest(&canonical_prepared))
        {
            return Err(RuntimeError);
        }
        let (transfer_identity, transfer_bytes, transfer_sha) = read_private(
            &parent(&prepared.transfer_path)?,
            &prepared.transfer_path,
            prepared.transfer.bytes,
        )?;
        if transfer_bytes != prepared.transfer.bytes || transfer_sha != prepared.transfer.sha256 {
            return Err(RuntimeError);
        }
        let (backup_identity, backup_bytes, backup_sha) = read_private(
            &parent(&prepared.backup_path)?,
            &prepared.backup_path,
            prepared.backup.bytes,
        )?;
        if backup_bytes != prepared.backup.bytes || backup_sha != prepared.backup.sha256 {
            return Err(RuntimeError);
        }
        let (source_evidence_identity, source_evidence_sha256) =
            if let Some(path) = &prepared.source_evidence_path {
                let (identity, _, sha) = read_private(&parent(path)?, path, 65536)?;
                (Some(identity), Some(sha))
            } else {
                (None, None)
            };
        let origin_sha256 = prepared.selection.operation_sha256.clone();
        let history = history_path(&prepared.selection.state_path, &origin_sha256)?;
        inspection
            .choice_output(&history)
            .map_err(|_| RuntimeError)?;
        if history == prepared_path
            || history == prepared.transfer_path
            || history == prepared.backup_path
            || prepared.source_evidence_path.as_ref() == Some(&history)
            || history.starts_with(&prepared.candidate.path)
        {
            return Err(RuntimeError);
        }
        let origin = inspection
            .choice_operation_bytes()
            .map_err(|_| RuntimeError)?;
        if origin.len() > 512 << 10 || format!("{:x}", Sha256::digest(&origin)) != origin_sha256 {
            return Err(RuntimeError);
        }
        let directory = parent(&history)?;
        match fs::symlink_metadata(anchored(&directory, &history)?) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut temporary = tempfile::Builder::new()
                    .prefix(".nk-choice-origin-")
                    .permissions(fs::Permissions::from_mode(0o600))
                    .tempfile_in(PathBuf::from(format!(
                        "/proc/self/fd/{}",
                        directory.as_raw_fd()
                    )))?;
                temporary.write_all(&origin)?;
                temporary.as_file().sync_all()?;
                self.revalidate()?;
                check_parent(&directory, &history)?;
                let from = std::ffi::CString::new(
                    temporary.path().file_name().ok_or(RuntimeError)?.as_bytes(),
                )
                .map_err(|_| RuntimeError)?;
                let to =
                    std::ffi::CString::new(history.file_name().ok_or(RuntimeError)?.as_bytes())
                        .map_err(|_| RuntimeError)?;
                // SAFETY: retained private directory and terminated single names.
                if unsafe {
                    libc::renameat2(
                        directory.as_raw_fd(),
                        from.as_ptr(),
                        directory.as_raw_fd(),
                        to.as_ptr(),
                        libc::RENAME_NOREPLACE,
                    )
                } != 0
                {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            Err(e) => return Err(e.into()),
        }
        // An exact retry must finish the durability barrier if a predecessor
        // died after rename but before syncing this parent.
        directory.sync_all()?;
        let (origin_identity, origin_bytes, origin_digest) =
            read_private(&directory, &history, 512 << 10)?;
        if origin_bytes != origin.len() as u64 || origin_digest != origin_sha256 {
            return Err(RuntimeError);
        }
        let record = ChoiceHandoffRecord {
            version: 3,
            protocol: PROTOCOL.into(),
            phase: HandoffPhase::Handoff,
            prepared_path: prepared_path.into(),
            prepared_sha256,
            prepared_identity,
            transfer_identity,
            backup_identity,
            source_evidence_identity,
            source_evidence_sha256,
            origin_history_path: history,
            origin_sha256,
            origin_identity,
            prepared,
        };
        record.verify_retained_files()?;
        self.revalidate()?;
        record.verify_retained_files()?;
        Ok(record)
    }
}
