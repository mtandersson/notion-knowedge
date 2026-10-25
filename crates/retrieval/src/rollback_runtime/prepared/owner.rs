//! Prepare without releasing or reacquiring original SQL/index ownership.
use super::*;
use crate::{
    chunks::canonical_transfer::TransferLimits,
    commit::{
        OwnedOfflineInspection,
        transition::{
            adoption::{self, AdoptionLimits},
            manifest::{self, ManifestLimits, comparison::preparation_limits},
        },
    },
    rollback_runtime::candidate::{self, PriorInputs, RetainedCandidate, VerifiedPriorCandidate},
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt, time::Duration};

pub struct PreparedInputs<'a> {
    pub prior: PriorInputs<'a>,
    pub retained: RetainedCandidate<'a>,
    pub target: &'a CommitBinding,
    pub quarantine_generation: &'a str,
}
#[derive(Clone, Copy)]
pub struct PreparedLimits {
    pub transfer: TransferLimits,
    pub adoption: AdoptionLimits,
    pub sql: ManifestLimits,
    pub process_budget: Duration,
}
impl Default for PreparedLimits {
    fn default() -> Self {
        Self {
            transfer: TransferLimits::default(),
            adoption: AdoptionLimits::default(),
            sql: ManifestLimits::default(),
            process_budget: Duration::from_secs(300),
        }
    }
}
/// Only actual owned verification constructs this value. Retained metadata must
/// be independently reverified and cannot construct it by deserialization.
pub struct VerifiedPreparedRollback<'owner> {
    pub(super) owner: &'owner OwnedOfflineInspection<'owner>,
    pub(super) candidate: VerifiedPriorCandidate<'owner>,
    pub(super) metadata: PreparedRollbackMetadata,
    pub(super) limits: PreparedLimits,
}
impl VerifiedPreparedRollback<'_> {
    pub fn metadata(&self) -> &PreparedRollbackMetadata {
        &self.metadata
    }
    pub fn revalidate(&self) -> Result<()> {
        self.candidate.revalidate()?;
        self.revalidate_state()
    }
    pub(super) fn revalidate_state(&self) -> Result<()> {
        let provenance = self
            .owner
            .prior_archive(&self.metadata.lineage_paths)
            .map_err(err)?;
        if DirectoryIdentity::capture(self.owner.index_directory()).map_err(err)?
            != self.metadata.current
            || self.owner.state_path() != self.metadata.state_path
            || DirectoryIdentity::capture(&self.metadata.prior_archive.path).map_err(err)?
                != self.metadata.prior_archive
            || provenance.path != self.metadata.prior_archive.path
            || provenance.device != self.metadata.prior_archive.device
            || provenance.inode != self.metadata.prior_archive.inode
            || provenance.lineage_sha256 != self.metadata.candidate.lineage_sha256
        {
            return Err(RuntimeError);
        }
        self.metadata.canonical_bytes()?;
        self.metadata
            .backup
            .verify(&self.metadata.backup_path)
            .map_err(err)?;
        let limits = preparation_limits(
            self.owner.binding(),
            self.candidate.target(),
            &self.metadata.quarantine_generation,
            self.limits.sql,
        )
        .map_err(err)?;
        let actual =
            manifest::capture_sql_manifest(self.owner.owned_transaction().map_err(err)?, limits)
                .map_err(err)?;
        if actual != self.metadata.adoption
            || backup_manifest(&self.metadata.backup_path, &self.metadata.backup, limits)? != actual
            || self.candidate.metadata() != &self.metadata.candidate
        {
            return Err(RuntimeError);
        }
        self.owner.revalidate().map_err(err)
    }
}
fn backup_manifest(
    path: &Path,
    backup: &OfflineBackup,
    limits: ManifestLimits,
) -> Result<SqlManifest> {
    backup.verify(path).map_err(err)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let mut db = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(err)?;
    db.busy_timeout(Duration::ZERO).map_err(err)?;
    let transaction = db.transaction().map_err(err)?;
    crate::commit::transition::validate_connection_file(&transaction, &file, path).map_err(err)?;
    let manifest = manifest::capture_sql_manifest(&transaction, limits).map_err(err)?;
    backup.verify(path).map_err(err)?;
    crate::commit::transition::validate_connection_file(&transaction, &file, path).map_err(err)?;
    Ok(manifest)
}
/// Build the runtime-specific Prepared capability under the caller's original
/// owner. This does not quarantine, refence, install, publish SQL or serve.
pub fn prepare<'owner>(
    owner: &'owner OwnedOfflineInspection<'owner>,
    inputs: PreparedInputs<'owner>,
    embedding: &EmbeddingMetadata,
    backup_path: &Path,
    limits: PreparedLimits,
) -> Result<VerifiedPreparedRollback<'owner>> {
    prepare_inner(owner, inputs, embedding, backup_path, limits, None)
}
/// Freshly verify an explicitly retained Prepared record. Missing backups or
/// metadata are errors; reopening never recreates them.
pub fn reopen<'owner>(
    owner: &'owner OwnedOfflineInspection<'owner>,
    inputs: PreparedInputs<'owner>,
    embedding: &EmbeddingMetadata,
    path: &Path,
    limits: PreparedLimits,
) -> Result<VerifiedPreparedRollback<'owner>> {
    let retained = PreparedRollbackMetadata::read(path)?;
    let proof = prepare_inner(
        owner,
        inputs,
        embedding,
        &retained.backup_path,
        limits,
        Some(&retained),
    )?;
    if PreparedRollbackMetadata::read(path)? != retained {
        return Err(RuntimeError);
    }
    Ok(proof)
}
fn prepare_inner<'owner>(
    owner: &'owner OwnedOfflineInspection<'owner>,
    inputs: PreparedInputs<'owner>,
    embedding: &EmbeddingMetadata,
    backup_path: &Path,
    limits: PreparedLimits,
    retained: Option<&PreparedRollbackMetadata>,
) -> Result<VerifiedPreparedRollback<'owner>> {
    let prior_archive = DirectoryIdentity::capture(inputs.prior.archive).map_err(err)?;
    let prior_evidence_path = inputs.prior.evidence.to_owned();
    let lineage_paths = inputs.prior.lineage.to_vec();
    let transfer_path = inputs.retained.transfer.to_owned();
    let target = inputs.target;
    let quarantine_generation = inputs.quarantine_generation;
    if [
        &prior_archive.path,
        &inputs.retained.metadata.candidate.path,
        owner.index_directory(),
    ]
    .iter()
    .any(|path| backup_path.starts_with(path))
    {
        return Err(RuntimeError);
    }
    let candidate = candidate::verify_retained(
        owner,
        inputs.prior,
        embedding,
        target,
        inputs.retained,
        limits.transfer,
        limits.process_budget,
    )?;
    let provenance = owner.prior_archive(&lineage_paths).map_err(err)?;
    if provenance.path != prior_archive.path
        || provenance.device != prior_archive.device
        || provenance.inode != prior_archive.inode
    {
        return Err(RuntimeError);
    }
    let metadata = candidate.with_locked_candidate(|report| {
        let connection = owner.owned_transaction().map_err(err)?;
        let reused: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 OR generation=?2)",
            rusqlite::params![quarantine_generation, target.generation()], |r| r.get(0)).map_err(err)?;
        if reused { return Err(RuntimeError); }
        let sql_limits = preparation_limits(owner.binding(), target, quarantine_generation, limits.sql).map_err(err)?;
        let before = manifest::capture_sql_manifest(connection, sql_limits).map_err(err)?;
        let backup = owner.backup_current(backup_path, limits.transfer.sql_bytes, retained.is_none()).map_err(err)?;
        if backup_manifest(backup_path, &backup, sql_limits)? != before { return Err(RuntimeError); }
        adoption::preview_current(owner, target, report.source.source_version, limits.adoption).map_err(err)?;
        if manifest::capture_sql_manifest(connection, sql_limits).map_err(err)? != before { return Err(RuntimeError); }
        let metadata = PreparedRollbackMetadata {
            version: 1, protocol: PROTOCOL.into(), state_path: owner.state_path().into(),
            transfer_path, prior_evidence_path, lineage_paths, prior_archive,
            current: DirectoryIdentity::capture(owner.index_directory()).map_err(err)?,
            candidate: candidate.metadata().clone(),
            observed_target_authority_json: String::from_utf8(source::canonical(
                if candidate.metadata().kind == CandidateKind::ExactArchive { &provenance.binding } else { target }, 32768)?).map_err(err)?,
            quarantine_generation: quarantine_generation.into(),
            quarantine_epoch: owner.binding().epoch().checked_add(1).ok_or(RuntimeError)?,
            backup_path: backup_path.into(), backup, adoption: before,
        };
        metadata.canonical_bytes()?;
        if retained.is_some_and(|expected| expected != &metadata) { return Err(RuntimeError); }
        Ok(metadata)
    })?;
    let verified = VerifiedPreparedRollback {
        owner,
        candidate,
        metadata,
        limits,
    };
    verified.revalidate()?;
    Ok(verified)
}
