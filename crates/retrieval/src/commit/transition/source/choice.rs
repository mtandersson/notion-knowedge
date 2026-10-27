//! Internal descriptive context, freshly checked under the source inspection.
use super::*;
use crate::{
    chunks::runtime_builder::DirectoryIdentity,
    rollback_runtime::choice::{Choice, Origin},
};
use std::os::{
    fd::AsRawFd,
    unix::{ffi::OsStrExt, fs::PermissionsExt},
};

impl TransitionOwnership {
    /// Only the original choice owner may account for the one reserved tuple.
    /// The complete remaining SQL must match the freshly verified intent.
    pub(crate) fn reserved_choice(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        limits: manifest::ManifestLimits,
    ) -> Result<Option<CommitBinding>, CommitError> {
        let Some(handoff) = self.choice_recovery.as_deref() else {
            return Ok(None);
        };
        let Some(intent) =
            crate::rollback_runtime::choice::PublicationStartMetadata::read_if_present(handoff)
                .map_err(|_| CommitError::BindingMismatch)?
        else {
            return Ok(None);
        };
        let target = CommitBinding::parse_retained(intent.desired_authority_json.as_bytes())?;
        let present: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 OR epoch=?2)",
            params![target.generation(), i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?],
            |row| row.get(0),
        ).map_err(sql)?;
        let actual = if present {
            manifest::reservation::capture_reservation_preimage(transaction, &target, limits)?
        } else {
            manifest::capture_sql_manifest(transaction, limits)?
        };
        if actual != intent.before {
            return Err(CommitError::BindingMismatch);
        }
        Ok(present.then_some(target))
    }
}

/// Recover physical locks only. The returned owner stays crate-private and
/// must be used solely within an inspection that proves SQL and native state.
pub(crate) fn acquire_choice_recovery(
    handoff: &crate::rollback_runtime::choice::ChoiceHandoffRecord,
) -> Result<TransitionOwnership, CommitError> {
    handoff
        .verify_active()
        .map_err(|_| CommitError::BindingMismatch)?;
    handoff
        .verify_retained_files()
        .map_err(|_| CommitError::BindingMismatch)?;
    let mut owner = match &handoff.prepared.selection.origin {
        Origin::Activation(record) => {
            TransitionOwnership::acquire(&handoff.prepared.selection.state_path, record)?
        }
        Origin::Runtime(record) => TransitionOwnership::acquire_runtime(record)?,
    };
    owner.choice_recovery = Some(Box::new(handoff.clone()));
    owner.current_record()?.ok_or(CommitError::Conflict)?;
    Ok(owner)
}

/// Called only by a dialect-specific wrapper after its full pre-swap native
/// verification, while the original physical owner and SQL transaction remain
/// held. It never creates a gap in the active journal.
pub(crate) fn install_choice_handoff(
    owner: &TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    staged: &StagedChoiceHandoff,
) -> Result<(), CommitError> {
    use crate::rollback_runtime::evidence::{check_parent, parent};
    let record = &staged.record;
    owner.validate_owned_transaction(transaction)?;
    verify_candidate_guard(staged)?;
    record
        .verify_retained_files()
        .map_err(|_| CommitError::BindingMismatch)?;
    let previous = operation(owner)?;
    if record.prepared.selection.state_path != owner.state_path
        || format!("{:x}", Sha256::digest(&previous)) != record.origin_sha256
    {
        return Err(CommitError::BindingMismatch);
    }
    let path = journal_path(&owner.state_path)?;
    let directory = parent(&path).map_err(|_| CommitError::InvalidInput)?;
    let root = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    let mut temporary = tempfile::Builder::new()
        .prefix(".nk-choice-handoff-")
        .permissions(fs::Permissions::from_mode(0o600))
        .tempfile_in(root)
        .map_err(io)?;
    temporary
        .write_all(
            &record
                .canonical_bytes()
                .map_err(|_| CommitError::InvalidInput)?,
        )
        .map_err(io)?;
    temporary.as_file().sync_all().map_err(io)?;
    #[cfg(test)]
    transition_checkpoint(owner, "choice-handoff-staged");
    owner.validate_owned_transaction(transaction)?;
    verify_candidate_guard(staged)?;
    check_parent(&directory, &path).map_err(|_| CommitError::BindingMismatch)?;
    record
        .verify_retained_files()
        .map_err(|_| CommitError::BindingMismatch)?;
    if operation(owner)? != previous {
        return Err(CommitError::Conflict);
    }
    let from = std::ffi::CString::new(
        temporary
            .path()
            .file_name()
            .ok_or(CommitError::InvalidInput)?
            .as_bytes(),
    )
    .map_err(|_| CommitError::InvalidInput)?;
    let to = std::ffi::CString::new(
        path.file_name()
            .ok_or(CommitError::InvalidInput)?
            .as_bytes(),
    )
    .map_err(|_| CommitError::InvalidInput)?;
    // SAFETY: both names are single components in the retained private directory.
    if unsafe {
        libc::renameat2(
            directory.as_raw_fd(),
            from.as_ptr(),
            directory.as_raw_fd(),
            to.as_ptr(),
            0,
        )
    } != 0
    {
        return Err(io(std::io::Error::last_os_error()));
    }
    #[cfg(test)]
    transition_checkpoint(owner, "choice-handoff-renamed");
    directory.sync_all().map_err(io)?;
    #[cfg(test)]
    transition_checkpoint(owner, "choice-handoff-durable");
    owner.validate_owned_transaction(transaction)?;
    verify_candidate_guard(staged)?;
    check_parent(&directory, &path).map_err(|_| CommitError::BindingMismatch)?;
    record
        .verify_active()
        .map_err(|_| CommitError::BindingMismatch)?;
    record
        .verify_retained_files()
        .map_err(|_| CommitError::BindingMismatch)?;
    Ok(())
}

fn verify_candidate_guard(staged: &StagedChoiceHandoff) -> Result<(), CommitError> {
    let candidate = &staged.record.prepared.candidate;
    if DirectoryIdentity::capture(&candidate.path).map_err(|_| CommitError::BindingMismatch)?
        != *candidate
    {
        return Err(CommitError::BindingMismatch);
    }
    match (&staged.record.prepared.built, &staged.candidate_guard) {
        (Some(_), Some(guard)) => {
            let metadata = guard.metadata().map_err(io)?;
            if metadata.dev() != candidate.device || metadata.ino() != candidate.inode {
                return Err(CommitError::BindingMismatch);
            }
        }
        (None, None) => (),
        _ => return Err(CommitError::BindingMismatch),
    }
    let authority =
        CommitBinding::parse_retained(staged.record.prepared.observed_authority_json.as_bytes())?;
    let current = crate::chunks::inspect_runtime_source(
        &candidate.path,
        &staged.embedding,
        &authority,
        staged.native_limits,
    )
    .map_err(|_| CommitError::BindingMismatch)?;
    if current != staged.record.prepared.candidate_report.source {
        return Err(CommitError::BindingMismatch);
    }
    Ok(())
}

pub(crate) struct ChoiceContext {
    pub origin: Origin,
    pub operation_sha256: String,
    pub original_source_authority_sha256: String,
    pub state_path: PathBuf,
    pub source: DirectoryIdentity,
    pub selected: DirectoryIdentity,
    pub authorities: Vec<CommitBinding>,
    pub target_native: TargetNativeContract,
    pub highest_reserved_epoch: u64,
}
impl QuarantinedSourceInspection<'_> {
    /// Confirm this exact handoff's fresh authority is already reserved under
    /// the current original owner; a staged intent alone is insufficient.
    pub(crate) fn require_reserved_handoff(
        &self,
        handoff: &crate::rollback_runtime::choice::ChoiceHandoffRecord,
    ) -> Result<(), CommitError> {
        self.revalidate()?;
        if self.owner.choice_recovery.as_deref() != Some(handoff) {
            return Err(CommitError::BindingMismatch);
        }
        let desired = CommitBinding::parse_retained(
            handoff.prepared.selection.desired_authority_json.as_bytes(),
        )?;
        if self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?
            != Some(desired)
        {
            return Err(CommitError::BindingMismatch);
        }
        self.revalidate()
    }

    /// Identity of the immutable choice SQL preimage, not a hash of the live
    /// database after reservation. The complete logical delta is checked first.
    pub(crate) fn choice_operational_preimage(
        &self,
        bytes: u64,
    ) -> Result<OfflineBackup, CommitError> {
        self.revalidate()?;
        if self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?
            .is_some()
        {
            let handoff = self
                .owner
                .choice_recovery
                .as_deref()
                .ok_or(CommitError::Conflict)?;
            let expected = &handoff.prepared.selection.operational;
            if bytes == 0 || expected.bytes > bytes || handoff.prepared.backup != *expected {
                return Err(CommitError::InvalidInput);
            }
            expected.verify(&handoff.prepared.backup_path)?;
            self.revalidate()?;
            return Ok(expected.clone());
        }
        self.state_snapshot(bytes)
    }

    pub(crate) fn reserve_choice_authority(
        &self,
        handoff: &crate::rollback_runtime::choice::ChoiceHandoffRecord,
        candidate_guard: Option<File>,
    ) -> Result<(), CommitError> {
        self.revalidate()?;
        if self.owner.choice_recovery.as_deref() != Some(handoff) {
            return Err(CommitError::BindingMismatch);
        }
        if handoff.prepared.built.is_some() != candidate_guard.is_some() {
            return Err(CommitError::BindingMismatch);
        }
        {
            let mut retained = self.owner.choice_candidate_guard.borrow_mut();
            if let (Some(previous), Some(next)) = (retained.as_ref(), candidate_guard.as_ref())
                && Identity::file(previous)? != Identity::file(next)?
            {
                return Err(CommitError::BindingMismatch);
            }
            if retained.is_none() {
                *retained = candidate_guard;
            }
        }
        self.revalidate()?;
        let target = CommitBinding::parse_retained(
            handoff.prepared.selection.desired_authority_json.as_bytes(),
        )?;
        self.validate_choice_fence(&target)?;
        if self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?
            .is_none()
        {
            // The durable exact intent must precede the first SQL effect.
            crate::rollback_runtime::choice::PublicationStartMetadata::read(handoff)
                .map_err(|_| CommitError::BindingMismatch)?;
            self.transaction
                .execute(
                    "INSERT INTO index_commit_generations VALUES(?1,?2,?3)",
                    params![
                        target.generation(),
                        target.scope(),
                        i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?
                    ],
                )
                .map_err(sql)?;
        }
        if self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?
            != Some(target)
        {
            return Err(CommitError::BindingMismatch);
        }
        self.revalidate()
    }

    #[cfg(test)]
    pub(crate) fn reservation_checkpoint(&self, phase: &str) -> Result<(), CommitError> {
        super::super::sql_checkpoint(self.owner, self.transaction, phase)
    }

    #[cfg(test)]
    pub(crate) fn choice_checkpoint(&self, phase: &str) {
        transition_checkpoint(self.owner, phase);
    }
    pub(crate) fn stage_choice_handoff(
        &self,
        record: crate::rollback_runtime::choice::ChoiceHandoffRecord,
        candidate_guard: Option<File>,
        native_limits: crate::chunks::CandidateLimits,
    ) -> Result<(), CommitError> {
        self.revalidate()?;
        record
            .verify_retained_files()
            .map_err(|_| CommitError::BindingMismatch)?;
        if record.prepared.selection.operation_sha256
            != format!("{:x}", Sha256::digest(&self.operation))
            || record.prepared.selection.state_path != self.owner.state_path
        {
            return Err(CommitError::BindingMismatch);
        }
        let mut staged = self.staged_handoff.borrow_mut();
        if staged.is_some() {
            return Err(CommitError::Conflict);
        }
        let handoff = StagedChoiceHandoff {
            record,
            candidate_guard,
            embedding: self.embedding.clone(),
            native_limits,
        };
        verify_candidate_guard(&handoff)?;
        *staged = Some(handoff);
        drop(staged);
        self.revalidate()
    }
    /// Exact original active-journal bytes, usable only while this transaction
    /// and its original physical owner remain valid.
    pub(crate) fn choice_operation_bytes(&self) -> Result<Vec<u8>, CommitError> {
        self.revalidate()?;
        let bytes = self.operation.clone();
        self.revalidate()?;
        Ok(bytes)
    }

    pub(crate) fn choice_context(&self, choice: Choice) -> Result<ChoiceContext, CommitError> {
        self.revalidate()?;
        let runtime = if self.owner.runtime.is_some() {
            Some(self.owner.runtime_record()?)
        } else {
            None
        };
        let origin = match &runtime {
            Some(record) => Origin::Runtime(Box::new(record.clone())),
            None => Origin::Activation(Box::new(self.record.clone())),
        };
        let source = DirectoryIdentity::capture(&self.directory)
            .map_err(|_| CommitError::BindingMismatch)?;
        let (selected, authorities) = match choice {
            Choice::OriginalSource => (source.clone(), vec![self.binding.clone()]),
            Choice::PreparedTarget => {
                let directory = self.owner.physical_paths()?.1;
                let mut authorities = vec![self.target.clone()];
                if let Some(record) = &runtime {
                    use runtime_journal::RuntimePhase;
                    if matches!(
                        record.phase,
                        RuntimePhase::Prepared
                            | RuntimePhase::Quarantined
                            | RuntimePhase::SourceRetained
                    ) {
                        let old = CommitBinding::parse_retained(
                            record.prepared.observed_target_authority_json.as_bytes(),
                        )?;
                        authorities = vec![old];
                        // A completed refence may precede its journal advance.
                        if record.phase == RuntimePhase::SourceRetained
                            && authorities[0] != self.target
                        {
                            authorities.push(self.target.clone());
                        }
                    }
                }
                (
                    DirectoryIdentity::capture(&directory)
                        .map_err(|_| CommitError::BindingMismatch)?,
                    authorities,
                )
            }
        };
        let original = serde_json::to_vec(&(
            &self.binding,
            &self.record.source.path,
            &self.owner.state_path,
            self.record.source.device,
            self.record.source.inode,
            self.record.state_device,
            self.record.state_inode,
            &self.record.state_token,
        ))
        .map_err(|_| CommitError::InvalidInput)?;
        let reserved = self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?;
        let reserved_epoch = reserved
            .as_ref()
            .map(|binding| i64::try_from(binding.epoch()))
            .transpose()
            .map_err(|_| CommitError::InvalidInput)?;
        let highest: i64 = self
            .transaction
            .query_row(
                "SELECT coalesce(max(epoch),0) FROM index_commit_generations WHERE (?1 IS NULL OR generation<>?1) AND (?2 IS NULL OR epoch<>?2)",
                params![reserved.as_ref().map(CommitBinding::generation), reserved_epoch],
                |r| r.get(0),
            )
            .map_err(sql)?;
        self.revalidate()?;
        Ok(ChoiceContext {
            origin,
            operation_sha256: format!("{:x}", Sha256::digest(&self.operation)),
            original_source_authority_sha256: format!("{:x}", Sha256::digest(original)),
            state_path: self.owner.state_path.clone(),
            source,
            selected,
            authorities,
            target_native: self.target_native.clone(),
            highest_reserved_epoch: u64::try_from(highest)
                .map_err(|_| CommitError::InvalidInput)?
                .max(self.target.epoch()),
        })
    }
    pub(crate) fn validate_choice_fence(&self, target: &CommitBinding) -> Result<(), CommitError> {
        self.revalidate()?;
        if self.binding.table() != target.table()
            || self.binding.workspace() != target.workspace()
            || self.binding.roots() != target.roots()
            || self.binding.scope() != target.scope()
            || target.epoch() <= self.target.epoch()
        {
            return Err(CommitError::BindingMismatch);
        }
        if let Some(reserved) = self
            .owner
            .reserved_choice(self.transaction, self.limits.sql)?
        {
            if reserved != *target {
                return Err(CommitError::Conflict);
            }
            return self.revalidate();
        }
        let used: bool = self.transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 OR epoch=?2)",
            params![target.generation(), i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?],
            |row| row.get(0),
        ).map_err(sql)?;
        let highest: i64 = self
            .transaction
            .query_row(
                "SELECT coalesce(max(epoch),0) FROM index_commit_generations",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if used
            || target.epoch() <= u64::try_from(highest).map_err(|_| CommitError::InvalidInput)?
        {
            return Err(CommitError::Conflict);
        }
        self.revalidate()
    }
}

impl QuarantinedSourceInspection<'_> {
    pub(crate) fn choice_preview(
        &self,
        target: &CommitBinding,
        version: u64,
        limits: crate::rollback_runtime::prepared::PreparedLimits,
    ) -> Result<manifest::SqlManifest, CommitError> {
        self.validate_choice_fence(target)?;
        if self
            .owner
            .reserved_choice(self.transaction, limits.sql)?
            .is_some()
        {
            // The complete preimage equals the original freshly checked preview;
            // its sole delta is this exact reservation. Reusing that preview
            // does not pretend the reserved target is fresh or alter main SQL.
            return manifest::reservation::capture_reservation_preimage(
                self.transaction,
                target,
                limits.sql,
            );
        }
        // Preserve capacity for the one additional final fence; the original
        // quarantine and original target reservations already exist in SQL.
        let mut sql_limits = limits.sql;
        let bytes = 4 * (target.generation().len() as u64 + target.scope().len() as u64 + 40) + 128;
        if bytes > sql_limits.row_bytes {
            return Err(CommitError::InvalidInput);
        }
        sql_limits.rows = sql_limits
            .rows
            .checked_sub(1)
            .ok_or(CommitError::InvalidInput)?;
        sql_limits.total_bytes = sql_limits
            .total_bytes
            .checked_sub(bytes)
            .ok_or(CommitError::InvalidInput)?;
        sql_limits.digest_bytes = sql_limits
            .digest_bytes
            .checked_sub(32)
            .ok_or(CommitError::InvalidInput)?;
        let before = manifest::capture_sql_manifest(self.transaction, sql_limits)?;
        adoption::preview_connection(
            self.transaction,
            &self.binding,
            target,
            version,
            limits.adoption,
        )?;
        if manifest::capture_sql_manifest(self.transaction, sql_limits)? != before {
            return Err(CommitError::BindingMismatch);
        }
        self.revalidate()?;
        Ok(before)
    }
    pub(crate) fn choice_output(&self, path: &Path) -> Result<(), CommitError> {
        self.revalidate()?;
        if !self.allows_output(path) {
            return Err(CommitError::InvalidInput);
        }
        super::super::super::offline::private_parent(path)
    }
    pub(crate) fn choice_backup(
        &self,
        path: &Path,
        bytes: u64,
        create: bool,
    ) -> Result<OfflineBackup, CommitError> {
        use crate::rollback_runtime::evidence::{check_parent, parent};
        use std::{
            io::{Read, Seek},
            os::fd::AsRawFd,
        };
        self.choice_output(path)?;
        if create
            && self
                .owner
                .reserved_choice(self.transaction, self.limits.sql)?
                .is_some()
        {
            return Err(CommitError::Conflict);
        }
        let expected = self.choice_operational_preimage(bytes)?;
        let directory = parent(path).map_err(|_| CommitError::InvalidInput)?;
        if !create {
            expected.verify(path)?;
            check_parent(&directory, path).map_err(|_| CommitError::BindingMismatch)?;
            self.revalidate()?;
            return Ok(expected);
        }
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .join(path.file_name().ok_or(CommitError::InvalidInput)?);
        let mut output = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&anchored)
            .map_err(io)?;
        let mut original = self.owner.state.try_clone().map_err(io)?;
        original.rewind().map_err(io)?;
        let copied = std::io::copy(
            &mut (&mut original).take(bytes.saturating_add(1)),
            &mut output,
        )
        .map_err(io)?;
        if copied != expected.bytes
            || super::super::super::offline::hash_file_bounded(&mut output, bytes)? != expected
            || self.state_snapshot(bytes)? != expected
        {
            return Err(CommitError::BindingMismatch);
        }
        output.sync_all().map_err(io)?;
        directory.sync_all().map_err(io)?;
        check_parent(&directory, path).map_err(|_| CommitError::BindingMismatch)?;
        expected.verify(path)?;
        self.revalidate()?;
        Ok(expected)
    }
}
