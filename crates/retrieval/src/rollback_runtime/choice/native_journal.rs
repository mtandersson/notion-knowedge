//! Descriptive native-phase journal. A parsed record never grants ownership.
//!
//! The v4 journal will replace the active v3 handoff only after its exact
//! contents and the reservation intent have been durably retained. Native
//! effects require fresh physical, SQL and selected-runtime verification.
use super::*;
use crate::rollback_runtime::{
    evidence::{check_parent, parent},
    source::SourceReport,
};
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

const MAX_BYTES: usize = 4 << 20;
const PROTOCOL: &str = "explicit-choice-native-v4";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChoiceNativePhase {
    Ready,
    OtherRetained,
    SourceRetained,
    TargetRefenced,
    TargetInstalled,
}

fn file_identity(file: &File) -> Result<RetainedFileIdentity> {
    let metadata = file.metadata()?;
    Ok(RetainedFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    })
}

impl VerifiedPreparedChoice<'_, '_> {
    pub(crate) fn retain_candidate_guard(&self) -> Result<Option<File>> {
        Ok(self.guard.as_ref().map(File::try_clone).transpose()?)
    }

    pub(crate) fn stage_native_start(
        &self,
        handoff: &ChoiceHandoffRecord,
    ) -> Result<crate::commit::transition::source::StagedNativeStart> {
        let record = self.retain_native_handoff(handoff)?;
        self.revalidate()?;
        self.selection
            .inspection
            .require_reserved_handoff(handoff)
            .map_err(|_| RuntimeError)?;
        Ok(crate::commit::transition::source::StagedNativeStart {
            record,
            candidate_guard: self.guard.as_ref().map(File::try_clone).transpose()?,
            embedding: self.selection.inspection.embedding().clone(),
            native_limits: self.selection.limits.transfer.native,
        })
    }

    /// Durably retain the exact active handoff as a prerequisite for a future
    /// native journal. This does not replace the active journal or modify SQL
    /// or an index. The caller still owns the original exclusive transaction.
    pub fn retain_native_handoff(
        &self,
        handoff: &ChoiceHandoffRecord,
    ) -> Result<ChoiceNativeRecord> {
        self.revalidate()?;
        if self.metadata != handoff.prepared {
            return Err(RuntimeError);
        }
        let inspection = self.selection.inspection;
        inspection
            .require_reserved_handoff(handoff)
            .map_err(|_| RuntimeError)?;
        handoff.verify_active()?;
        handoff.verify_retained_files()?;
        let reservation = PublicationStartMetadata::read(handoff)?;
        let intent_bytes = reservation.canonical_bytes(handoff)?;
        let intent_path = reservation::path(handoff)?;
        let (intent_identity, intent_size, intent_sha) =
            handoff::read_private(&parent(&intent_path)?, &intent_path, 131072)?;
        if intent_size != intent_bytes.len() as u64
            || intent_sha != format!("{:x}", Sha256::digest(&intent_bytes))
        {
            return Err(RuntimeError);
        }
        let history = ChoiceNativeRecord::handoff_history_path(handoff)?;
        inspection
            .choice_output(&history)
            .map_err(|_| RuntimeError)?;
        let directory = parent(&history)?;
        let root = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let anchored = root.join(history.file_name().ok_or(RuntimeError)?);
        let handoff_bytes = handoff.canonical_bytes()?;
        let expected_sha = format!("{:x}", Sha256::digest(&handoff_bytes));
        let make_record = |identity| ChoiceNativeRecord {
            version: 4,
            protocol: PROTOCOL.into(),
            phase: ChoiceNativePhase::Ready,
            handoff: handoff.clone(),
            reservation: reservation.clone(),
            handoff_history_path: history.clone(),
            handoff_history_identity: identity,
            reservation_identity: intent_identity.clone(),
            refenced: None,
        };
        match fs::symlink_metadata(&anchored) {
            Ok(_) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut temporary = tempfile::Builder::new()
                    .prefix(".nk-choice-native-origin-")
                    .permissions(fs::Permissions::from_mode(0o600))
                    .tempfile_in(&root)?;
                temporary.write_all(&handoff_bytes)?;
                temporary.as_file().sync_all()?;
                // Check all path and identity exclusions before publishing.
                make_record(file_identity(temporary.as_file())?).canonical_bytes()?;
                #[cfg(test)]
                inspection.choice_checkpoint("choice-native-origin-staged");
                self.revalidate()?;
                inspection
                    .require_reserved_handoff(handoff)
                    .map_err(|_| RuntimeError)?;
                handoff.verify_active()?;
                handoff.verify_retained_files()?;
                check_parent(&directory, &history)?;
                let from = std::ffi::CString::new(
                    temporary.path().file_name().ok_or(RuntimeError)?.as_bytes(),
                )
                .map_err(|_| RuntimeError)?;
                let to =
                    std::ffi::CString::new(history.file_name().ok_or(RuntimeError)?.as_bytes())
                        .map_err(|_| RuntimeError)?;
                // SAFETY: both names are single components under the retained private parent.
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
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        inspection.choice_checkpoint("choice-native-origin-renamed");
        check_parent(&directory, &history)?;
        directory.sync_all()?;
        let (history_identity, history_size, history_sha) =
            handoff::read_private(&directory, &history, 2 << 20)?;
        if history_size != handoff_bytes.len() as u64 || history_sha != expected_sha {
            return Err(RuntimeError);
        }
        let record = make_record(history_identity);
        record.canonical_bytes()?;
        self.revalidate()?;
        inspection
            .require_reserved_handoff(handoff)
            .map_err(|_| RuntimeError)?;
        handoff.verify_active()?;
        handoff.verify_retained_files()?;
        let (current_history, size, sha) = handoff::read_private(&directory, &history, 2 << 20)?;
        if current_history != record.handoff_history_identity
            || size != history_size
            || sha != history_sha
        {
            return Err(RuntimeError);
        }
        let (current_intent, size, sha) =
            handoff::read_private(&parent(&intent_path)?, &intent_path, 131072)?;
        if current_intent != intent_identity
            || size != intent_size
            || sha != intent_sha
            || PublicationStartMetadata::read(handoff)? != reservation
        {
            return Err(RuntimeError);
        }
        Ok(record)
    }
}

/// A bounded description of native work under the original quarantined owner.
/// File identities are descriptive until rechecked against retained descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceNativeRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: ChoiceNativePhase,
    pub handoff: ChoiceHandoffRecord,
    pub reservation: PublicationStartMetadata,
    pub handoff_history_path: PathBuf,
    pub handoff_history_identity: RetainedFileIdentity,
    pub reservation_identity: RetainedFileIdentity,
    pub refenced: Option<SourceReport>,
}

impl ChoiceNativeRecord {
    /// Read a bounded active description. This does not acquire ownership or
    /// establish native, SQL, or serving authority.
    pub fn read_active_metadata(state: &Path) -> Result<Self> {
        let path = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
        let directory = parent(&path)?;
        let (identity, size, sha) = handoff::read_private(&directory, &path, MAX_BYTES as u64)?;
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .join(path.file_name().ok_or(RuntimeError)?);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&anchored)?;
        if file_identity(&file)? != identity {
            return Err(RuntimeError);
        }
        let mut bytes = Vec::new();
        (&file).take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        let record = Self::parse_metadata(&bytes)?;
        let (after, after_size, after_sha) =
            handoff::read_private(&directory, &path, MAX_BYTES as u64)?;
        if file_identity(&file)? != identity
            || after != identity
            || size != bytes.len() as u64
            || after_size != size
            || after_sha != sha
            || sha != format!("{:x}", Sha256::digest(&bytes))
            || record.handoff.prepared.selection.state_path != state
        {
            return Err(RuntimeError);
        }
        check_parent(&directory, &path)?;
        Ok(record)
    }

    pub(crate) fn verify_active(&self) -> Result<()> {
        if Self::read_active_metadata(&self.handoff.prepared.selection.state_path)? != *self {
            return Err(RuntimeError);
        }
        Ok(())
    }

    /// Derive one immutable history name from the exact v3 handoff bytes.
    pub fn handoff_history_path(handoff: &ChoiceHandoffRecord) -> Result<PathBuf> {
        let digest = format!("{:x}", Sha256::digest(handoff.canonical_bytes()?));
        let active =
            crate::commit::transition::journal_path(&handoff.prepared.selection.state_path)
                .map_err(|_| RuntimeError)?;
        let mut name = active.file_name().ok_or(RuntimeError)?.to_os_string();
        name.push(format!(".{digest}.choice-native-origin.history"));
        Ok(active.with_file_name(name))
    }

    /// Validate canonical bounded description only. The caller must acquire
    /// the original physical owner and verify actual SQL, files and runtime.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.handoff.canonical_bytes()?;
        self.reservation.canonical_bytes(&self.handoff)?;
        let origin = match &self.handoff.prepared.selection.origin {
            Origin::Activation(record) => *record.clone(),
            Origin::Runtime(record) => record.layout().map_err(|_| RuntimeError)?,
        };
        let history = Self::handoff_history_path(&self.handoff)?;
        let protected = [
            &origin.source.path,
            &origin.target.path,
            &origin.candidate_path,
            &origin.archive_path,
            &self.handoff.prepared.candidate.path,
        ];
        let retained = [
            &self.handoff.prepared_path,
            &self.handoff.prepared.transfer_path,
            &self.handoff.prepared.backup_path,
            &self.handoff.origin_history_path,
            &self.handoff.prepared.selection.state_path,
            &origin.backup_path,
        ];
        let key = |identity: &RetainedFileIdentity| (identity.device, identity.inode);
        let existing = [
            &self.handoff.prepared_identity,
            &self.handoff.transfer_identity,
            &self.handoff.backup_identity,
            &self.handoff.origin_identity,
        ];
        if self.version != 4
            || self.protocol != PROTOCOL
            || self.handoff_history_path != history
            || protected.iter().any(|path| history.starts_with(path))
            || retained.contains(&&history)
            || self.handoff.prepared.source_evidence_path.as_ref() == Some(&history)
            || matches!(&self.handoff.prepared.selection.origin, Origin::Runtime(record)
                if history.starts_with(&record.prepared.prior_archive.path)
                    || record.lineage_paths.contains(&history))
            || self.handoff_history_identity.inode == 0
            || self.reservation_identity.inode == 0
            || key(&self.handoff_history_identity) == key(&self.reservation_identity)
            || existing.iter().any(|identity| {
                key(identity) == key(&self.handoff_history_identity)
                    || key(identity) == key(&self.reservation_identity)
            })
            || self
                .handoff
                .source_evidence_identity
                .as_ref()
                .is_some_and(|identity| {
                    key(identity) == key(&self.handoff_history_identity)
                        || key(identity) == key(&self.reservation_identity)
                })
            || [
                self.handoff_history_identity.mtime_nsec,
                self.handoff_history_identity.ctime_nsec,
                self.reservation_identity.mtime_nsec,
                self.reservation_identity.ctime_nsec,
            ]
            .iter()
            .any(|nanos| !(0..1_000_000_000).contains(nanos))
        {
            return Err(RuntimeError);
        }
        let has_refenced = matches!(
            self.phase,
            ChoiceNativePhase::TargetRefenced | ChoiceNativePhase::TargetInstalled
        );
        if self.refenced.is_some() != has_refenced {
            return Err(RuntimeError);
        }
        if let Some(report) = &self.refenced {
            let bytes = source::canonical(report, 65536)?;
            SourceReport::parse_metadata(&bytes)?;
            let selected = &self.handoff.prepared.selection;
            let valid_digest = |value: &str| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            };
            if report.descriptor != selected.observation.descriptor
                || report.source.source_version != selected.current_native.source_version
                || report.source.rows != selected.current_native.rows
                || report.source.snapshot_sha256 != selected.current_native.snapshot_sha256
                || !valid_digest(&report.source.authority_sha256)
                || !valid_digest(&report.source.snapshot_sha256)
                || report.source.lexical.is_some()
                    != self
                        .handoff
                        .prepared
                        .candidate_report
                        .source
                        .lexical
                        .is_some()
            {
                return Err(RuntimeError);
            }
            if let Some(binding) = &report.source.lexical {
                let desired = CommitBinding::parse_retained(
                    self.reservation.desired_authority_json.as_bytes(),
                )
                .map_err(|_| RuntimeError)?;
                binding
                    .validate_selected_metadata(
                        &self.handoff.prepared.transfer.metadata.embedding,
                        &desired,
                        &self.handoff.prepared.candidate,
                        &selected.current_native.snapshot_sha256,
                    )
                    .map_err(|_| RuntimeError)?;
                if !report.descriptor.describes_bound_source(binding) {
                    return Err(RuntimeError);
                }
            }
        }
        source::canonical(self, MAX_BYTES)
    }

    /// Parse a canonical description without opening a database or index.
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let record: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if record.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(record)
    }

    /// Check retained bytes and captured identities without treating them as
    /// authority. The original owner and actual SQL/native state must still be
    /// acquired and verified before a transition effect.
    pub fn verify_retained_files(&self) -> Result<()> {
        self.canonical_bytes()?;
        let history = &self.handoff_history_path;
        let intent_path = reservation::path(&self.handoff)?;
        let handoff_bytes = self.handoff.canonical_bytes()?;
        let intent_bytes = self.reservation.canonical_bytes(&self.handoff)?;
        let expected_history = format!("{:x}", Sha256::digest(&handoff_bytes));
        let expected_intent = format!("{:x}", Sha256::digest(&intent_bytes));
        let verify = || -> Result<()> {
            let (history_id, history_size, history_sha) =
                handoff::read_private(&parent(history)?, history, 2 << 20)?;
            let (intent_id, intent_size, intent_sha) =
                handoff::read_private(&parent(&intent_path)?, &intent_path, 131072)?;
            if history_id != self.handoff_history_identity
                || history_size != handoff_bytes.len() as u64
                || history_sha != expected_history
                || intent_id != self.reservation_identity
                || intent_size != intent_bytes.len() as u64
                || intent_sha != expected_intent
            {
                return Err(RuntimeError);
            }
            Ok(())
        };
        verify()?;
        self.handoff.verify_retained_files()?;
        verify()
    }
}

/// Recover a Ready activation-origin native journal under the original owner.
/// Trusted runtime/asset and candidate inputs are supplied by the caller;
/// this read-only verification neither advances a phase nor enables serving.
pub fn recover_activation_native_ready(
    native: &ChoiceNativeRecord,
    candidate: crate::chunks::TransitionCandidateInputs<'_>,
    inputs: SelectionInputs<'_>,
    limits: PreparedLimits,
    capture_limits: crate::commit::transition::source::SourceCaptureLimits,
    now: i64,
) -> Result<()> {
    if !matches!(
        native.handoff.prepared.selection.origin,
        Origin::Activation(_)
    ) {
        return Err(RuntimeError);
    }
    let owner = crate::commit::transition::source::acquire_choice_native_ready(native)
        .map_err(|_| RuntimeError)?;
    let _candidate_guard = owner
        .with_quarantined_source_inspection(now, candidate, capture_limits, |inspection| {
            inspection.require_reserved_handoff(&native.handoff)?;
            let proof = super::reopen(inspection, inputs, &native.handoff.prepared_path, limits)
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            if proof.metadata() != &native.handoff.prepared {
                return Err(crate::commit::CommitError::BindingMismatch);
            }
            proof
                .revalidate()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            inspection.require_reserved_handoff(&native.handoff)?;
            native
                .verify_active()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            native
                .verify_retained_files()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
            proof
                .retain_candidate_guard()
                .map_err(|_| crate::commit::CommitError::BindingMismatch)
        })
        .map_err(|_| RuntimeError)?;
    let path =
        crate::commit::transition::journal_path(&native.handoff.prepared.selection.state_path)
            .map_err(|_| RuntimeError)?;
    parent(&path)?.sync_all()?;
    #[cfg(test)]
    crate::commit::transition::transition_checkpoint(&owner, "choice-native-journal-durable");
    native.verify_active()?;
    native.verify_retained_files()?;
    Ok(())
}

/// Replace a verified v3 handoff with the durable native Ready journal while
/// retaining the original owner, selected candidate lock and exclusive SQL.
/// An exact already-installed Ready journal is reverified on retry.
pub fn start_activation_native(
    handoff: &ChoiceHandoffRecord,
    candidate: crate::chunks::TransitionCandidateInputs<'_>,
    inputs: SelectionInputs<'_>,
    limits: PreparedLimits,
    capture_limits: crate::commit::transition::source::SourceCaptureLimits,
    now: i64,
) -> Result<()> {
    if !matches!(handoff.prepared.selection.origin, Origin::Activation(_)) {
        return Err(RuntimeError);
    }
    if let Ok(native) =
        ChoiceNativeRecord::read_active_metadata(&handoff.prepared.selection.state_path)
    {
        if native.handoff != *handoff {
            return Err(RuntimeError);
        }
        return recover_activation_native_ready(
            &native,
            candidate,
            inputs,
            limits,
            capture_limits,
            now,
        );
    }
    let owner = crate::commit::transition::source::acquire_choice_recovery(handoff)
        .map_err(|_| RuntimeError)?;
    let _staged = owner
        .with_exclusive_sql(now, |transaction| {
            let verified =
                crate::chunks::verify_transition_candidate(&owner, transaction, candidate)?;
            let target = crate::commit::transition::source::TargetNativeContract {
                version: verified.target_version(),
                descriptor: crate::chunks::lexical_binding::runtime::RuntimeDescriptor::current(
                    Some(candidate.dictionary),
                )
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?,
                pinned: None,
            };
            let staged = crate::commit::transition::source::inspect(
                &owner,
                transaction,
                candidate.source,
                candidate.embedding,
                target,
                capture_limits,
                |inspection| {
                    inspection.require_reserved_handoff(handoff)?;
                    let proof = super::reopen(inspection, inputs, &handoff.prepared_path, limits)
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                    if proof.metadata() != &handoff.prepared {
                        return Err(crate::commit::CommitError::BindingMismatch);
                    }
                    proof
                        .stage_native_start(handoff)
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)
                },
            )?;
            crate::chunks::verify_transition_candidate(&owner, transaction, candidate)?;
            crate::commit::transition::source::install_choice_native_ready(
                &owner,
                transaction,
                &staged,
                capture_limits.sql,
            )?;
            crate::chunks::verify_transition_candidate(&owner, transaction, candidate)?;
            Ok(staged)
        })
        .map_err(|_| RuntimeError)?;
    Ok(())
}
