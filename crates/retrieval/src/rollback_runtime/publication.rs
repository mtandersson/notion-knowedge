//! Runtime publication retains physical ownership and rechecks actual native
//! and whole-SQL state inside each effect transaction. Metadata is not proof.
use super::{
    PinnedRuntime, Result, RuntimeError, builder, candidate::observe, evidence::RetainedEvidence,
    prepared::PreparedLimits, verification::PinnedAsset,
};
use crate::{
    chunks::{canonical_transfer, inspect_runtime_source, runtime_builder::DirectoryIdentity},
    commit::{
        CommitBinding, LineageContext, resolve_archive_lineage,
        transition::{
            TransitionOwnership,
            manifest::comparison::{ComparisonInputs, ComparisonStage, verify_backup_comparison},
            runtime_journal::{RuntimeJournalRecord, RuntimePhase},
        },
    },
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    time::Instant,
};
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
/// Expected values must come from the trusted original serving configuration,
/// not merely be copied from a journal being recovered.
pub struct CurrentSource<'a> {
    pub index: &'a Path,
    pub state: &'a Path,
    pub binding: &'a CommitBinding,
    pub embedding: &'a EmbeddingMetadata,
}
/// Owns descriptors, not perpetual native permission. Every effect must rerun
/// native/SQL verification in its actual transaction. No deserializer exists.
pub struct RuntimeTransitionOperator<'pair> {
    owner: TransitionOwnership,
    runtime: &'pair PinnedRuntime,
    asset: Option<&'pair PinnedAsset>,
    source: CommitBinding,
    target: CommitBinding,
    embedding: EmbeddingMetadata,
    limits: PreparedLimits,
    _prior: Option<File>,
}
impl<'pair> RuntimeTransitionOperator<'pair> {
    /// Reopen only an actual active version-two operation. This performs real
    /// selected-runtime fencing and native/SQL checks before returning.
    pub fn open(
        runtime: &'pair PinnedRuntime,
        asset: Option<&'pair PinnedAsset>,
        current: CurrentSource<'_>,
        record: &RuntimeJournalRecord,
        limits: PreparedLimits,
        now: i64,
    ) -> Result<Self> {
        let target = CommitBinding::parse_retained(
            record.prepared.candidate.target_authority_json.as_bytes(),
        )
        .map_err(err)?;
        if limits.process_budget.is_zero()
            || limits.process_budget > std::time::Duration::from_secs(300)
            || record.prepared.state_path != current.state
            || record.prepared.current.path != current.index
            || record.prepared.candidate.current.metadata.embedding != *current.embedding
            || record
                .prepared
                .candidate
                .current
                .metadata
                .authority_json
                .as_bytes()
                != serde_json::to_vec(current.binding).map_err(err)?
            || record.prepared.candidate.runtime_sha256 != runtime.sha256()
            || record.prepared.candidate.asset_sha256.as_deref() != asset.map(PinnedAsset::sha256)
        {
            return Err(RuntimeError);
        }
        builder::verify_fencing(
            runtime,
            asset,
            &record.prepared.candidate.prior.descriptor,
            limits.transfer.native,
            limits.process_budget,
        )?;
        let owner = TransitionOwnership::acquire_runtime(record).map_err(err)?;
        if owner.runtime_record().map_err(err)? != *record {
            return Err(RuntimeError);
        }
        let prior = if record.prepared.candidate.candidate == record.prepared.prior_archive {
            None
        } else {
            let identity = &record.prepared.prior_archive;
            if DirectoryIdentity::capture(&identity.path).map_err(err)? != *identity {
                return Err(RuntimeError);
            }
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&identity.path)?;
            let m = file.metadata()?;
            if m.dev() != identity.device || m.ino() != identity.inode {
                return Err(RuntimeError);
            }
            file.try_lock().map_err(err)?;
            Some(file)
        };
        let operator = Self {
            owner,
            runtime,
            asset,
            source: current.binding.clone(),
            target,
            embedding: current.embedding.clone(),
            limits,
            _prior: prior,
        };
        operator
            .owner
            .with_exclusive_sql(now, |transaction| {
                operator
                    .verify_all(transaction)
                    .map(|_| ())
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .map_err(err)?;
        Ok(operator)
    }
    /// Retain the current source only while SQL is already quarantined. Native,
    /// lineage and complete SQL checks run inside the actual rename transaction.
    pub fn retain_current_source(&self, now: i64) -> Result<RuntimeJournalRecord> {
        if self.owner.runtime_record().map_err(err)?.phase != RuntimePhase::Quarantined {
            return Err(RuntimeError);
        }
        self.owner
            .retain_source_checked(now, |transaction| {
                self.verify_all(transaction)
                    .map(|_| ())
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .map_err(err)?;
        self.owner.runtime_record().map_err(err)
    }
    fn verify_all(&self, transaction: &rusqlite::Transaction<'_>) -> Result<u64> {
        let started = Instant::now();
        let remaining = || {
            self.limits
                .process_budget
                .checked_sub(started.elapsed())
                .filter(|d| !d.is_zero())
                .ok_or(RuntimeError)
        };
        self.owner
            .validate_owned_transaction(transaction)
            .map_err(err)?;
        let record = self.owner.runtime_record().map_err(err)?;
        let prepared = &record.prepared;
        let candidate = &prepared.candidate;
        if candidate.runtime_sha256 != self.runtime.sha256()
            || candidate.asset_sha256.as_deref() != self.asset.map(PinnedAsset::sha256)
        {
            return Err(RuntimeError);
        }
        let (source, target) = self.owner.physical_paths().map_err(err)?;
        let paths = match record.phase {
            RuntimePhase::Prepared => {
                source == prepared.current.path && target == candidate.candidate.path
            }
            RuntimePhase::Quarantined => {
                (source == prepared.current.path || source == record.archive_path)
                    && target == candidate.candidate.path
            }
            RuntimePhase::SourceRetained => {
                source == record.archive_path && target == candidate.candidate.path
            }
            RuntimePhase::TargetRefenced => {
                source == record.archive_path
                    && (target == candidate.candidate.path || target == prepared.current.path)
            }
            RuntimePhase::TargetInstalled | RuntimePhase::Published | RuntimePhase::Complete => {
                source == record.archive_path && target == prepared.current.path
            }
        };
        if !paths {
            return Err(RuntimeError);
        }
        let native = inspect_runtime_source(
            &source,
            &self.embedding,
            &self.source,
            self.limits.transfer.native,
        )
        .map_err(err)?;
        let expected = &candidate.current.metadata.source;
        if native.version != expected.version
            || native.source_version != expected.source_version
            || native.rows != expected.rows
            || native.snapshot_sha256 != expected.snapshot_sha256
            || native.lexical != expected.lexical
            || (source == prepared.current.path
                && native.authority_sha256 != expected.authority_sha256)
        {
            return Err(RuntimeError);
        }
        let provenance = resolve_archive_lineage(
            LineageContext {
                binding: &self.source,
                index_path: &prepared.current.path,
                index_device: prepared.current.device,
                index_inode: prepared.current.inode,
                state_path: &prepared.state_path,
                state_device: record.state_device,
                state_inode: record.state_inode,
                state_token: &record.state_token,
            },
            &record.lineage_paths,
        )
        .map_err(err)?;
        let evidence = RetainedEvidence::read(&prepared.prior_evidence_path)?;
        if provenance.lineage_sha256 != candidate.lineage_sha256
            || provenance.path != prepared.prior_archive.path
            || provenance.device != prepared.prior_archive.device
            || provenance.inode != prepared.prior_archive.inode
            || provenance.ownership_sha256 != evidence.prior_authority_sha256()
            || provenance.snapshot_sha256 != candidate.prior.source.snapshot_sha256
            || !evidence.matches_prior_observation(
                self.runtime.sha256(),
                self.asset.map(PinnedAsset::sha256),
                &candidate.prior,
            )
        {
            return Err(RuntimeError);
        }
        let ownership = serde_json::to_vec(&(
            &self.source,
            &prepared.current.path,
            &prepared.state_path,
            prepared.current.device,
            prepared.current.inode,
            record.state_device,
            record.state_inode,
            &record.state_token,
        ))
        .map_err(err)?;
        if format!("{:x}", Sha256::digest(ownership)) != candidate.current.metadata.ownership_sha256
        {
            return Err(RuntimeError);
        }
        if candidate.kind == super::candidate::CandidateKind::ExactArchive
            && prepared.observed_target_authority_json.as_bytes()
                != serde_json::to_vec(&provenance.binding).map_err(err)?
        {
            return Err(RuntimeError);
        }
        let old = CommitBinding::parse_retained(prepared.observed_target_authority_json.as_bytes())
            .map_err(err)?;
        let observed = if matches!(
            record.phase,
            RuntimePhase::TargetRefenced
                | RuntimePhase::TargetInstalled
                | RuntimePhase::Published
                | RuntimePhase::Complete
        ) {
            &self.target
        } else {
            &old
        };
        let target_report = observe(
            self.runtime,
            self.asset,
            &target,
            observed,
            &self.embedding,
            self.limits.transfer,
            remaining()?,
        )?;
        let expected_target = record
            .refenced
            .as_ref()
            .or_else(|| candidate.built.as_ref().map(|r| &r.snapshot))
            .unwrap_or(&candidate.prior.source);
        if (matches!(
            record.phase,
            RuntimePhase::Prepared | RuntimePhase::Quarantined
        ) && target_report.source.authority_sha256 != expected_target.authority_sha256)
            || target_report.descriptor != candidate.prior.descriptor
            || target_report.source.source_version != expected_target.source_version
            || target_report.source.rows != expected.rows
            || target_report.source.snapshot_sha256 != expected.snapshot_sha256
            || target_report.source.lexical != expected_target.lexical
        {
            return Err(RuntimeError);
        }
        if self._prior.is_some() {
            let prior = observe(
                self.runtime,
                self.asset,
                &prepared.prior_archive.path,
                &provenance.binding,
                &self.embedding,
                self.limits.transfer,
                remaining()?,
            )?;
            if prior != candidate.prior
                || DirectoryIdentity::capture(&prepared.prior_archive.path).map_err(err)?
                    != prepared.prior_archive
            {
                return Err(RuntimeError);
            }
        }
        canonical_transfer::read(
            &prepared.transfer_path,
            &candidate.current,
            self.limits.transfer,
            |_| Ok(()),
        )
        .map_err(err)?;
        let generation: String = transaction
            .query_row(
                "SELECT generation FROM index_commit_binding WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(err)?;
        let stage = match record.phase {
            RuntimePhase::Prepared if generation == self.source.generation() => {
                ComparisonStage::Prepared
            }
            RuntimePhase::Prepared
            | RuntimePhase::Quarantined
            | RuntimePhase::SourceRetained
            | RuntimePhase::TargetRefenced
                if generation == prepared.quarantine_generation =>
            {
                ComparisonStage::BeforeInstallation
            }
            RuntimePhase::TargetInstalled if generation == prepared.quarantine_generation => {
                ComparisonStage::BeforePublication
            }
            RuntimePhase::TargetInstalled | RuntimePhase::Published | RuntimePhase::Complete
                if generation == self.target.generation() =>
            {
                ComparisonStage::AfterPublication
            }
            _ => return Err(RuntimeError),
        };
        verify_backup_comparison(
            &self.owner,
            transaction,
            &ComparisonInputs {
                source: &self.source,
                target: &self.target,
                target_version: target_report.source.source_version,
                plan_bytes: self.limits.adoption.plan_bytes,
                backup_bytes: self.limits.transfer.sql_bytes,
                limits: self.limits.sql,
            },
            stage,
        )
        .map_err(err)?;
        if self.owner.runtime_record().map_err(err)? != record {
            return Err(RuntimeError);
        }
        self.owner
            .validate_owned_transaction(transaction)
            .map_err(err)?;
        if let Some(asset) = self.asset {
            asset.revalidate()?;
        }
        remaining()?;
        Ok(target_report.source.source_version)
    }
}
