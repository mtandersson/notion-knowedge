//! Internal descriptive context, freshly checked under the source inspection.
use super::*;
use crate::{
    chunks::runtime_builder::DirectoryIdentity,
    rollback_runtime::choice::{Choice, Origin},
};

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
        let highest: i64 = self
            .transaction
            .query_row(
                "SELECT coalesce(max(epoch),0) FROM index_commit_generations",
                [],
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
        let expected = self.state_snapshot(bytes)?;
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
