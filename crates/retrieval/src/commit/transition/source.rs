//! Read-only source capture after serving authority has been quarantined.
//! Construction requires real native verification under the same SQL owner.
use super::*;
use crate::commit::OfflineBackup;
use manifest::comparison::{ComparisonInputs, ComparisonStage, verify_backup_comparison};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
pub struct SourceCaptureLimits {
    pub sql: manifest::ManifestLimits,
    pub plan_bytes: u64,
    pub backup_bytes: u64,
}
impl Default for SourceCaptureLimits {
    fn default() -> Self {
        Self {
            sql: manifest::ManifestLimits::default(),
            plan_bytes: adoption::AdoptionLimits::default().plan_bytes,
            backup_bytes: 64 << 20,
        }
    }
}
#[derive(Clone)]
pub(crate) struct TargetNativeContract {
    pub version: u64,
    pub descriptor: crate::chunks::lexical_binding::runtime::RuntimeDescriptor,
    pub pinned: Option<(String, Option<String>)>,
}
/// Borrows actual original-state and physical ownership, never serving rights.
/// No constructor or deserializer is available to callers. Its lifetime cannot
/// escape the verified original transaction's callback.
///
/// ```compile_fail
/// use notion_knowledge_retrieval::{chunks::TransitionCandidateInputs,
///     commit::transition::{TransitionOwnership, source::{QuarantinedSourceInspection, SourceCaptureLimits}}};
/// fn escape<'a>(owner: &'a TransitionOwnership, input: TransitionCandidateInputs<'a>)
///     -> &'a QuarantinedSourceInspection<'a> {
///     owner.with_quarantined_source_inspection(12, input, SourceCaptureLimits::default(),
///         |inspection| Ok(inspection)).unwrap()
/// }
/// ```
pub struct QuarantinedSourceInspection<'owner> {
    owner: &'owner TransitionOwnership,
    transaction: &'owner rusqlite::Transaction<'owner>,
    record: TransitionRecord,
    operation: Vec<u8>,
    binding: CommitBinding,
    embedding: notion_knowledge_core::embedding::EmbeddingMetadata,
    target: CommitBinding,
    directory: PathBuf,
    retained_prior: Option<PathBuf>,
    target_native: TargetNativeContract,
    limits: SourceCaptureLimits,
    staged_handoff: std::cell::RefCell<Option<StagedChoiceHandoff>>,
}
pub(crate) struct StagedChoiceHandoff {
    record: crate::rollback_runtime::choice::ChoiceHandoffRecord,
    candidate_guard: Option<File>,
    embedding: notion_knowledge_core::embedding::EmbeddingMetadata,
    native_limits: crate::chunks::CandidateLimits,
}
fn operation(owner: &TransitionOwnership) -> Result<Vec<u8>, CommitError> {
    if owner.runtime.is_some() {
        owner.runtime_record()?.canonical_bytes()
    } else {
        canonical(&owner.current_record()?.ok_or(CommitError::Conflict)?)
    }
}
impl QuarantinedSourceInspection<'_> {
    pub fn binding(&self) -> &CommitBinding {
        &self.binding
    }
    pub fn embedding(&self) -> &notion_knowledge_core::embedding::EmbeddingMetadata {
        &self.embedding
    }
    pub fn index_directory(&self) -> &Path {
        &self.directory
    }
    pub fn revalidate(&self) -> Result<(), CommitError> {
        self.owner.validate_owned_transaction(self.transaction)?;
        if self.owner.current_record()?.as_ref() != Some(&self.record)
            || operation(self.owner)? != self.operation
            || self.owner.physical_paths()?.0 != self.directory
        {
            return Err(CommitError::BindingMismatch);
        }
        let quarantined: bool = self
            .transaction
            .query_row(
                "SELECT generation=?1 AND epoch=?2 FROM index_commit_binding WHERE singleton=1",
                params![
                    self.record.quarantine_generation,
                    i64::try_from(self.record.quarantine_epoch)
                        .map_err(|_| CommitError::InvalidInput)?
                ],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !quarantined {
            return Err(CommitError::Conflict);
        }
        let stage = match self.record.phase {
            TransitionPhase::Prepared
            | TransitionPhase::Quarantined
            | TransitionPhase::SourceRetained => ComparisonStage::BeforeInstallation,
            TransitionPhase::TargetInstalled => ComparisonStage::BeforePublication,
            _ => return Err(CommitError::Conflict),
        };
        verify_backup_comparison(
            self.owner,
            self.transaction,
            &ComparisonInputs {
                source: &self.binding,
                target: &self.target,
                target_version: self.target_native.version,
                plan_bytes: self.limits.plan_bytes,
                backup_bytes: self.limits.backup_bytes,
                limits: self.limits.sql,
            },
            stage,
        )?;
        Ok(())
    }
    pub fn authority_sha256(&self) -> Result<String, CommitError> {
        self.revalidate()?;
        Ok(format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "quarantined-source-capture-v1",
                    &self.operation,
                    &self.binding,
                    &self.directory,
                    &self.owner.state_path,
                ))
                .map_err(|_| CommitError::InvalidInput)?
            )
        ))
    }
    pub fn state_snapshot(&self, max_bytes: u64) -> Result<OfflineBackup, CommitError> {
        if max_bytes == 0 {
            return Err(CommitError::InvalidInput);
        }
        self.revalidate()?;
        let snapshot = super::super::offline::hash_file_bounded(
            &mut self.owner.state.try_clone().map_err(io)?,
            max_bytes,
        )?;
        self.revalidate()?;
        Ok(snapshot)
    }
    pub(crate) fn allows_output(&self, path: &Path) -> bool {
        let record = &self.record;
        if [
            &record.source.path,
            &record.target.path,
            &record.archive_path,
            &record.candidate_path,
        ]
        .iter()
        .any(|index| path.starts_with(index))
            || self
                .retained_prior
                .as_ref()
                .is_some_and(|prior| path.starts_with(prior))
            || path == self.owner.state_path
            || path == record.backup_path
            || journal_path(&self.owner.state_path).is_ok_and(|journal| path == journal)
        {
            return false;
        }
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut name = self.owner.state_path.as_os_str().to_os_string();
            name.push(suffix);
            if path == Path::new(&name) {
                return false;
            }
        }
        true
    }
}
/// Private construction is reached only after actual dialect-specific native
/// verification has produced the target version in this same transaction.
pub(crate) fn inspect<T>(
    owner: &TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    binding: &CommitBinding,
    embedding: &notion_knowledge_core::embedding::EmbeddingMetadata,
    target_native: TargetNativeContract,
    limits: SourceCaptureLimits,
    action: impl for<'a> FnOnce(&QuarantinedSourceInspection<'a>) -> Result<T, CommitError>,
) -> Result<T, CommitError> {
    let (result, staged) = inspect_inner(
        owner,
        transaction,
        binding,
        embedding,
        target_native,
        limits,
        action,
    )?;
    if staged.is_some() {
        return Err(CommitError::Conflict);
    }
    Ok(result)
}

/// A staged handoff is returned only to the dialect-specific wrapper, which
/// must finish its native checks under this same owner and transaction.
pub(crate) fn inspect_handoff<T>(
    owner: &TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    binding: &CommitBinding,
    embedding: &notion_knowledge_core::embedding::EmbeddingMetadata,
    target_native: TargetNativeContract,
    limits: SourceCaptureLimits,
    action: impl for<'a> FnOnce(&QuarantinedSourceInspection<'a>) -> Result<T, CommitError>,
) -> Result<(T, StagedChoiceHandoff), CommitError> {
    let (result, staged) = inspect_inner(
        owner,
        transaction,
        binding,
        embedding,
        target_native,
        limits,
        action,
    )?;
    Ok((result, staged.ok_or(CommitError::Conflict)?))
}

fn inspect_inner<T>(
    owner: &TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    binding: &CommitBinding,
    embedding: &notion_knowledge_core::embedding::EmbeddingMetadata,
    target_native: TargetNativeContract,
    limits: SourceCaptureLimits,
    action: impl for<'a> FnOnce(&QuarantinedSourceInspection<'a>) -> Result<T, CommitError>,
) -> Result<(T, Option<StagedChoiceHandoff>), CommitError> {
    let record = owner.current_record()?.ok_or(CommitError::Conflict)?;
    if serde_json::to_string(binding).map_err(|_| CommitError::InvalidInput)?
        != record.source.binding
    {
        return Err(CommitError::BindingMismatch);
    }
    let inspection = QuarantinedSourceInspection {
        owner,
        transaction,
        operation: operation(owner)?,
        binding: binding.clone(),
        embedding: embedding.clone(),
        target: CommitBinding::parse_retained(record.target.binding.as_bytes())?,
        directory: owner.physical_paths()?.0,
        retained_prior: if owner.runtime.is_some() {
            Some(owner.runtime_record()?.prepared.prior_archive.path)
        } else {
            None
        },
        target_native,
        limits,
        record,
        staged_handoff: std::cell::RefCell::new(None),
    };
    inspection.revalidate()?;
    let result = action(&inspection)?;
    inspection.revalidate()?;
    Ok((result, inspection.staged_handoff.into_inner()))
}
impl TransitionOwnership {
    /// Stage a freshly verified explicit choice and atomically replace the
    /// original v1 journal while the same physical and SQL ownership is held.
    pub fn handoff_quarantined_choice<T>(
        &self,
        now: i64,
        candidate: crate::chunks::TransitionCandidateInputs<'_>,
        limits: SourceCaptureLimits,
        action: impl for<'a> FnOnce(&QuarantinedSourceInspection<'a>) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        if self.runtime.is_some() {
            return Err(CommitError::BindingMismatch);
        }
        self.with_exclusive_sql(now, |transaction| {
            let verified =
                crate::chunks::verify_transition_candidate(self, transaction, candidate)?;
            let (result, handoff) = inspect_handoff(
                self,
                transaction,
                candidate.source,
                candidate.embedding,
                TargetNativeContract {
                    version: verified.target_version(),
                    descriptor:
                        crate::chunks::lexical_binding::runtime::RuntimeDescriptor::current(Some(
                            candidate.dictionary,
                        ))
                        .map_err(|_| CommitError::BindingMismatch)?,
                    pinned: None,
                },
                limits,
                action,
            )?;
            crate::chunks::verify_transition_candidate(self, transaction, candidate)?;
            choice::install_choice_handoff(self, transaction, &handoff)?;
            Ok(result)
        })
    }
    /// Read an unfinished v1 source under actual native/whole-SQL verification.
    /// The revoked source fence is never reopened as a serving coordinator.
    pub fn with_quarantined_source_inspection<T>(
        &self,
        now: i64,
        candidate: crate::chunks::TransitionCandidateInputs<'_>,
        limits: SourceCaptureLimits,
        action: impl for<'a> FnOnce(&QuarantinedSourceInspection<'a>) -> Result<T, CommitError>,
    ) -> Result<T, CommitError> {
        if self.runtime.is_some() {
            return Err(CommitError::BindingMismatch);
        }
        self.with_exclusive_sql(now, |transaction| {
            let verified =
                crate::chunks::verify_transition_candidate(self, transaction, candidate)?;
            let result = inspect(
                self,
                transaction,
                candidate.source,
                candidate.embedding,
                TargetNativeContract {
                    version: verified.target_version(),
                    descriptor:
                        crate::chunks::lexical_binding::runtime::RuntimeDescriptor::current(Some(
                            candidate.dictionary,
                        ))
                        .map_err(|_| CommitError::BindingMismatch)?,
                    pinned: None,
                },
                limits,
                action,
            )?;
            crate::chunks::verify_transition_candidate(self, transaction, candidate)?;
            Ok(result)
        })
    }
}

mod choice;
pub(crate) use choice::{acquire_choice_recovery, install_choice_handoff};
pub(crate) fn validate_origin_activation(record: &TransitionRecord) -> Result<(), CommitError> {
    canonical(record).map(|_| ())
}
