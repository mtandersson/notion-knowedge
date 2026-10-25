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
#[derive(Clone, Copy)]
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
    seed: RuntimeJournalRecord,
    active: bool,
}
enum JournalMode<'a> {
    Active,
    Prepared,
    Completed(&'a Path),
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
        Self::acquire(
            runtime,
            asset,
            current,
            record,
            limits,
            now,
            JournalMode::Active,
        )
    }
    fn acquire(
        runtime: &'pair PinnedRuntime,
        asset: Option<&'pair PinnedAsset>,
        current: CurrentSource<'_>,
        record: &RuntimeJournalRecord,
        limits: PreparedLimits,
        now: i64,
        mode: JournalMode<'_>,
    ) -> Result<Self> {
        let active = !matches!(mode, JournalMode::Prepared);
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
        builder::verify_publication_runtime(
            runtime,
            asset,
            &record.prepared.candidate.prior.descriptor,
            limits.transfer.native,
            limits.process_budget,
        )?;
        let owner = match mode {
            JournalMode::Completed(path) => {
                TransitionOwnership::acquire_completed_runtime(record, path)
            }
            JournalMode::Active | JournalMode::Prepared => {
                TransitionOwnership::acquire_runtime(record)
            }
        }
        .map_err(err)?;
        if active && owner.runtime_record().map_err(err)? != *record {
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
            seed: record.clone(),
            active,
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
    pub fn quarantine(&self, now: i64) -> Result<RuntimeJournalRecord> {
        if !self.active {
            return Err(RuntimeError);
        }
        self.owner
            .quarantine_checked(now, |transaction| {
                self.verify_all(transaction)
                    .map(|_| ())
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .map_err(err)?;
        self.owner.runtime_record().map_err(err)
    }
    /// Retain the current source only while SQL is already quarantined. Native,
    /// lineage and complete SQL checks run inside the actual rename transaction.
    pub fn retain_current_source(&self, now: i64) -> Result<RuntimeJournalRecord> {
        if !self.active
            || self.owner.runtime_record().map_err(err)?.phase != RuntimePhase::Quarantined
        {
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
        let record = if self.active {
            self.owner.runtime_record().map_err(err)?
        } else {
            self.seed.clone()
        };
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
            if self.active {
                &record.lineage_paths
            } else {
                &prepared.lineage_paths
            },
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
        let target_result = observe(
            self.runtime,
            self.asset,
            &target,
            observed,
            &self.embedding,
            self.limits.transfer,
            remaining()?,
        );
        let mut recovering_refence = false;
        let target_report = match target_result {
            Ok(report) => report,
            Err(_) if record.phase == RuntimePhase::SourceRetained => {
                recovering_refence = true;
                observe(
                    self.runtime,
                    self.asset,
                    &target,
                    &self.target,
                    &self.embedding,
                    self.limits.transfer,
                    remaining()?,
                )?
            }
            Err(error) => return Err(error),
        };
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
            || (!recovering_refence && target_report.source.lexical != expected_target.lexical)
        {
            return Err(RuntimeError);
        }
        if recovering_refence {
            match &target_report.source.lexical {
                Some(binding) if candidate.prior.descriptor.describes_bound_source(binding) => {
                    binding
                        .validate_transfer_metadata(&self.embedding, &self.target)
                        .map_err(err)?
                }
                None if candidate.prior.descriptor.is_legacy() => (),
                _ => return Err(RuntimeError),
            }
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
        if self.active {
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
        } else {
            use crate::commit::transition::manifest::{
                capture_sql_manifest,
                comparison::{capture_backup_manifest, preparation_limits},
            };
            let comparison = ComparisonInputs {
                source: &self.source,
                target: &self.target,
                target_version: target_report.source.source_version,
                plan_bytes: self.limits.adoption.plan_bytes,
                backup_bytes: self.limits.transfer.sql_bytes,
                limits: self.limits.sql,
            };
            let backup = capture_backup_manifest(&record.layout().map_err(err)?, &comparison)
                .map_err(err)?;
            let actual = capture_sql_manifest(
                transaction,
                preparation_limits(
                    &self.source,
                    &self.target,
                    &prepared.quarantine_generation,
                    self.limits.sql,
                )
                .map_err(err)?,
            )
            .map_err(err)?;
            if actual != backup || actual.sha256 != record.adoption_sha256 {
                return Err(RuntimeError);
            }
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

pub struct ActivationPaths<'a> {
    pub archive: &'a Path,
    pub backup: &'a Path,
}
impl<'pair> RuntimeTransitionOperator<'pair> {
    /// Freshly reverify a Prepared record, capture a fresh activation backup,
    /// then replace the previous completed journal only after retaining history.
    /// No generation is reserved and no native directory is renamed here.
    pub fn start(
        runtime: &'pair PinnedRuntime,
        asset: Option<&'pair PinnedAsset>,
        current: CurrentSource<'_>,
        prepared_path: &Path,
        paths: ActivationPaths<'_>,
        limits: PreparedLimits,
        now: i64,
    ) -> Result<Self> {
        use super::{
            candidate::{PriorInputs, RetainedCandidate},
            prepared::{self, PreparedInputs, PreparedRollbackMetadata},
        };
        use crate::commit::{
            IndexCommitCoordinator,
            transition::{
                self,
                runtime_journal::{self, ExistingJournal},
            },
        };
        let metadata = PreparedRollbackMetadata::read(prepared_path)?;
        let bytes = metadata.canonical_bytes()?;
        let prepared_sha256 = format!("{:x}", Sha256::digest(&bytes));
        let previous = match runtime_journal::existing(current.state).map_err(err)? {
            ExistingJournal::Runtime(record) => {
                if record.prepared_sha256 != prepared_sha256
                    || record.archive_path != paths.archive
                    || record.activation_backup_path != paths.backup
                {
                    return Err(RuntimeError);
                }
                return Self::open(runtime, asset, current, &record, limits, now);
            }
            ExistingJournal::Legacy(record)
                if record.phase == transition::TransitionPhase::Complete =>
            {
                Some(record)
            }
            ExistingJournal::Legacy(_) => return Err(RuntimeError),
            ExistingJournal::Missing => None,
        };
        if paths.archive.parent() != current.index.parent()
            || paths.archive.try_exists()?
            || [
                &metadata.current.path,
                &metadata.candidate.candidate.path,
                &metadata.prior_archive.path,
            ]
            .iter()
            .any(|p| paths.backup.starts_with(p))
            || paths.backup == current.state
            || paths.backup == prepared_path
        {
            return Err(RuntimeError);
        }
        // Both paths must have canonical private parents before any output.
        super::evidence::parent(paths.archive)?;
        super::evidence::parent(paths.backup)?;
        let target =
            CommitBinding::parse_retained(metadata.candidate.target_authority_json.as_bytes())
                .map_err(err)?;
        let coordinator = IndexCommitCoordinator::open_existing(
            current.index,
            current.state,
            current.binding.clone(),
        )
        .map_err(err)?;
        let record = coordinator
            .with_owned_offline_inspection(now, |owner| {
                let proof = prepared::reopen(
                    owner,
                    PreparedInputs {
                        prior: PriorInputs {
                            runtime,
                            asset,
                            evidence: &metadata.prior_evidence_path,
                            lineage: &metadata.lineage_paths,
                            archive: &metadata.prior_archive.path,
                        },
                        retained: RetainedCandidate {
                            transfer: &metadata.transfer_path,
                            metadata: &metadata.candidate,
                        },
                        target: &target,
                        quarantine_generation: &metadata.quarantine_generation,
                    },
                    current.embedding,
                    prepared_path,
                    limits,
                )
                .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                if proof.metadata() != &metadata {
                    return Err(crate::commit::CommitError::BindingMismatch);
                }
                let (state_device, state_inode, state_token) = owner.runtime_state_identity()?;
                let backup = owner.backup_current(paths.backup, limits.transfer.sql_bytes, true)?;
                let active = transition::journal_path(current.state)?;
                let mut lineage_paths = metadata.lineage_paths.clone();
                if let Some(previous) = &previous {
                    let history = runtime_journal::completed_path(current.state, previous)?;
                    for path in &mut lineage_paths {
                        if *path == active {
                            *path = history.clone();
                        }
                    }
                }
                let record = RuntimeJournalRecord {
                    version: 2,
                    protocol: "selected-runtime-publication-v2".into(),
                    phase: RuntimePhase::Prepared,
                    state_device,
                    state_inode,
                    state_token,
                    prepared: metadata.clone(),
                    prepared_sha256: prepared_sha256.clone(),
                    lineage_paths,
                    archive_path: paths.archive.into(),
                    activation_backup_path: paths.backup.into(),
                    activation_backup: backup,
                    adoption_sha256: metadata.adoption.sha256.clone(),
                    refenced: None,
                };
                record.canonical_bytes()?;
                Ok(record)
            })
            .map_err(err)?;
        let mut operator = Self::acquire(
            runtime,
            asset,
            current,
            &record,
            limits,
            now,
            JournalMode::Prepared,
        )?;
        operator
            .owner
            .with_exclusive_sql(now, |transaction| {
                operator
                    .verify_all(transaction)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                operator
                    .owner
                    .begin_runtime(transaction, &record, previous.as_deref())
            })
            .map_err(err)?;
        operator.active = true;
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
}

#[cfg(test)]
mod tests;

impl RuntimeTransitionOperator<'_> {
    /// Refence only the retained target under the exact selected implementation.
    /// Current source/SQL and all canonical rows/vectors remain unchanged.
    pub fn refence_target(&self, now: i64) -> Result<RuntimeJournalRecord> {
        use crate::chunks::runtime_refence::RefenceRequest;
        use std::{io::Write, os::unix::fs::PermissionsExt};
        self.owner
            .with_exclusive_sql(now, |transaction| {
                let result = (|| -> Result<RuntimeJournalRecord> {
                    let version = self.verify_all(transaction)?;
                    let record = self.owner.runtime_record().map_err(err)?;
                    if record.phase != RuntimePhase::SourceRetained {
                        return Err(RuntimeError);
                    }
                    let (source, target) = self.owner.physical_paths().map_err(err)?;
                    let request = RefenceRequest {
                        version: 1,
                        source: DirectoryIdentity::capture(&source).map_err(err)?,
                        target: DirectoryIdentity::capture(&target).map_err(err)?,
                        current_authority_json: record
                            .prepared
                            .candidate
                            .current
                            .metadata
                            .authority_json
                            .clone(),
                        old_authority_json: record.prepared.observed_target_authority_json.clone(),
                        desired_authority_json: record
                            .prepared
                            .candidate
                            .target_authority_json
                            .clone(),
                        embedding: self.embedding.clone(),
                        runtime: record.prepared.candidate.prior.descriptor.clone(),
                        source_version: version,
                        rows: record.prepared.candidate.current.metadata.source.rows,
                        snapshot_sha256: record
                            .prepared
                            .candidate
                            .current
                            .metadata
                            .source
                            .snapshot_sha256
                            .clone(),
                        limits: self.limits.transfer.native,
                    };
                    let root = tempfile::Builder::new()
                        .permissions(std::fs::Permissions::from_mode(0o700))
                        .tempdir()?;
                    let path = root.path().join("request.json");
                    let mut file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(&path)?;
                    file.write_all(&request.canonical_bytes().map_err(err)?)?;
                    drop(file);
                    let mode = self
                        .asset
                        .map(PinnedAsset::mode)
                        .unwrap_or_else(|| vec!["legacy".into()]);
                    let bytes = self.runtime.run(
                        &[
                            vec!["refence-owned".into(), path.as_os_str().to_owned()],
                            mode,
                        ]
                        .concat(),
                        self.limits.process_budget,
                        131072,
                    )?;
                    let report = super::source::SourceReport::parse_metadata(&bytes)?;
                    if report.descriptor != record.prepared.candidate.prior.descriptor
                        || report.source.source_version != version
                        || report.source.rows != request.rows
                        || report.source.snapshot_sha256 != request.snapshot_sha256
                    {
                        return Err(RuntimeError);
                    }
                    // Re-run actual checks before accepting the journaled result;
                    // SourceRetained explicitly permits the just-completed fence.
                    self.verify_all(transaction)?;
                    let result = self
                        .owner
                        .record_refenced(transaction, report.source)
                        .map_err(err)?;
                    self.verify_all(transaction)?;
                    Ok(result)
                })();
                result.map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .map_err(err)
    }
}

impl RuntimeTransitionOperator<'_> {
    pub fn install_target(&self, now: i64) -> Result<RuntimeJournalRecord> {
        self.owner
            .install_candidate_checked(now, |transaction| {
                self.verify_all(transaction)
                    .map(|_| ())
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)
            })
            .map_err(err)?;
        self.owner.runtime_record().map_err(err)
    }
    /// Actual native checks bracket the entire SQL adoption/CAS transaction.
    pub fn publish_derived(&self, now: i64) -> Result<()> {
        use crate::commit::transition::publication::{SqlPublication, publish_derived_owned};
        self.owner
            .with_exclusive_sql(now, |transaction| {
                let generation: String = transaction
                    .query_row(
                        "SELECT generation FROM index_commit_binding WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                if generation == self.target.generation() {
                    self.verify_all(transaction)
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                    return Ok(());
                }
                let version = self
                    .verify_all(transaction)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                publish_derived_owned(
                    &self.owner,
                    transaction,
                    SqlPublication {
                        source: &self.source,
                        target: &self.target,
                        target_version: version,
                        adoption: self.limits.adoption,
                        manifest: self.limits.sql,
                        backup_bytes: self.limits.transfer.sql_bytes,
                    },
                )?;
                self.verify_all(transaction)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                #[cfg(test)]
                crate::commit::transition::sql_checkpoint(
                    &self.owner,
                    transaction,
                    "runtime-adoption-before-commit",
                )?;
                Ok(())
            })
            .map_err(err)?;
        #[cfg(test)]
        crate::commit::transition::transition_checkpoint(&self.owner, "runtime-adoption-committed");
        Ok(())
    }
    pub fn complete_publication(&self, now: i64) -> Result<RuntimeJournalRecord> {
        self.owner
            .with_exclusive_sql(now, |transaction| {
                self.verify_all(transaction)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                let generation: String = transaction
                    .query_row(
                        "SELECT generation FROM index_commit_binding WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                if generation != self.target.generation() {
                    return Err(crate::commit::CommitError::Conflict);
                }
                let mut current = self.owner.runtime_record()?;
                if current.phase == RuntimePhase::TargetInstalled {
                    crate::commit::transition::publication::install_anchor(&self.owner)?;
                    self.verify_all(transaction)
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                    current = self
                        .owner
                        .advance_runtime_phase(transaction, RuntimePhase::Published)?;
                }
                if current.phase == RuntimePhase::Published {
                    self.verify_all(transaction)
                        .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                    current = self
                        .owner
                        .advance_runtime_phase(transaction, RuntimePhase::Complete)?;
                }
                if current.phase != RuntimePhase::Complete {
                    return Err(crate::commit::CommitError::Conflict);
                }
                self.verify_all(transaction)
                    .map_err(|_| crate::commit::CommitError::BindingMismatch)?;
                Ok(current)
            })
            .map_err(err)
    }
}

impl RuntimeTransitionOperator<'_> {
    pub fn retire_complete(&self, now: i64) -> Result<std::path::PathBuf> {
        self.owner
            .with_exclusive_sql(now, |transaction| {
                self.owner
                    .retire_runtime_complete(transaction, |transaction| {
                        self.verify_all(transaction)
                            .map(|_| ())
                            .map_err(|_| crate::commit::CommitError::BindingMismatch)
                    })
            })
            .map_err(err)
    }
    /// Inspect each actual phase and SQL tuple rather than replaying effects.
    pub fn reconcile(&self, now: i64) -> Result<std::path::PathBuf> {
        for _ in 0..10 {
            match self.owner.runtime_record().map_err(err)?.phase {
                RuntimePhase::Prepared => {
                    self.quarantine(now)?;
                }
                RuntimePhase::Quarantined => {
                    self.retain_current_source(now)?;
                }
                RuntimePhase::SourceRetained => {
                    self.refence_target(now)?;
                }
                RuntimePhase::TargetRefenced => {
                    self.install_target(now)?;
                }
                RuntimePhase::TargetInstalled => {
                    self.publish_derived(now)?;
                    self.complete_publication(now)?;
                }
                RuntimePhase::Published => {
                    self.complete_publication(now)?;
                }
                RuntimePhase::Complete => return self.retire_complete(now),
            }
        }
        Err(RuntimeError)
    }
}

/// Publication result is descriptive. The observed serving snapshot came from
/// a fresh read-only selected-runtime process after all publication locks ended.
pub struct PublicationOutcome {
    pub history: std::path::PathBuf,
    pub observation: super::source::SourceReport,
}
impl<'pair> RuntimeTransitionOperator<'pair> {
    pub fn finish(self, now: i64) -> Result<PublicationOutcome> {
        let history = self.reconcile(now)?;
        self.observe_completed(history)
    }
    /// Retry after the durable active-marker unlink using explicit immutable
    /// history, original trusted configuration and freshly held native/SQL locks.
    pub fn verify_completed(
        runtime: &'pair PinnedRuntime,
        asset: Option<&'pair PinnedAsset>,
        current: CurrentSource<'_>,
        history: &Path,
        limits: PreparedLimits,
        now: i64,
    ) -> Result<PublicationOutcome> {
        let record = RuntimeJournalRecord::read_history(history).map_err(err)?;
        let operator = Self::acquire(
            runtime,
            asset,
            current,
            &record,
            limits,
            now,
            JournalMode::Completed(history),
        )?;
        operator.observe_completed(history.to_owned())
    }
    fn observe_completed(self, history: std::path::PathBuf) -> Result<PublicationOutcome> {
        use std::{io::Write, os::unix::fs::PermissionsExt};
        let record = RuntimeJournalRecord::read_history(&history).map_err(err)?;
        let runtime = self.runtime;
        let asset = self.asset;
        let limits = self.limits;
        let embedding = self.embedding.clone();
        drop(self);
        if let Some(asset) = asset {
            asset.revalidate()?;
        }
        let request = super::admission::AdmissionRequest {
            version: 1,
            directory: record.prepared.current.path.clone(),
            state: record.prepared.state_path.clone(),
            authority_json: record.prepared.candidate.target_authority_json.clone(),
            embedding,
            limits: limits.transfer.native,
        };
        let root = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let path = root.path().join("admission.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&request.canonical_bytes()?)?;
        drop(file);
        let mode = asset
            .map(PinnedAsset::mode)
            .unwrap_or_else(|| vec!["legacy".into()]);
        let bytes = runtime.run(
            &[
                vec!["inspect-admission".into(), path.as_os_str().to_owned()],
                mode,
            ]
            .concat(),
            limits.process_budget,
            65536,
        )?;
        let observation = super::source::SourceReport::parse_metadata(&bytes)?;
        if observation.descriptor != record.prepared.candidate.prior.descriptor {
            return Err(RuntimeError);
        }
        if let Some(asset) = asset {
            asset.revalidate()?;
        }
        Ok(PublicationOutcome {
            history,
            observation,
        })
    }
}
