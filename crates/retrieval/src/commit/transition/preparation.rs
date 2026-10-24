//! Build a Prepared activation journal from verified local artifacts. This does
//! not quarantine or activate; all source processes must remain stopped.
use super::*;
use crate::chunks::{OfflineLexicalSource, TransitionCandidateInputs};
use adoption::AdoptionLimits;
use manifest::{
    ManifestLimits,
    comparison::{ComparisonInputs, capture_backup_manifest},
};
use sha2::{Digest, Sha256};

pub struct ActivationPaths<'a> {
    pub candidate: &'a Path,
    pub archive: &'a Path,
    pub backup: &'a Path,
    pub quarantine_generation: &'a str,
}

/// Create a fresh activation-time backup and seal a Prepared record. Existing
/// unresolved journals are never overwritten. On failure retain all artifacts;
/// Prepared alone does not revoke admission to the unchanged source.
pub fn prepare_activation(
    coordinator: &Arc<IndexCommitCoordinator>,
    now: i64,
    candidate: TransitionCandidateInputs<'_>,
    paths: ActivationPaths<'_>,
    adoption: AdoptionLimits,
    limits: ManifestLimits,
    backup_bytes: u64,
) -> Result<TransitionRecord, CommitError> {
    if candidate.source != coordinator.binding() {
        return Err(CommitError::BindingMismatch);
    }
    for path in [paths.candidate, paths.archive, paths.backup] {
        super::super::offline::private_parent(path)?;
    }
    validate_artifact_locations(
        &coordinator.state_path,
        [&coordinator.index_path, paths.candidate, paths.archive],
        paths.backup,
    )?;
    match fs::symlink_metadata(paths.archive) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io(error)),
        Ok(_) => return Err(CommitError::Conflict),
    }
    let previous = TransitionRecord::read(&coordinator.state_path)?;
    if previous
        .as_ref()
        .is_some_and(|record| record.phase != TransitionPhase::Complete)
    {
        return Err(CommitError::Conflict);
    }
    let source = OfflineLexicalSource::bind(
        &coordinator.index_path,
        candidate.source.table(),
        candidate.embedding.clone(),
        coordinator.clone(),
    )
    .map_err(|_| CommitError::BindingMismatch)?;
    let prepared = source.verify(
        paths.candidate,
        candidate.preparation_backup,
        candidate.target,
        candidate.dictionary,
        now,
        candidate.limits,
    )?;
    let backup =
        coordinator.with_offline_backup_bounded(paths.backup, now, backup_bytes, |backup| {
            if backup.bytes > backup_bytes {
                return Err(CommitError::InvalidInput);
            }
            Ok(backup.clone())
        })?;
    let target_identity = Identity::file(&owned_directory(paths.candidate)?)?;
    let mut record = TransitionRecord {
        version: 1,
        phase: TransitionPhase::Prepared,
        state_device: coordinator.state_identity.device,
        state_inode: coordinator.state_identity.inode,
        state_token: coordinator.state_token.clone(),
        source: TransitionEndpoint {
            path: coordinator.index_path.clone(),
            device: coordinator.index_identity.device,
            inode: coordinator.index_identity.inode,
            binding: serde_json::to_string(candidate.source)
                .map_err(|_| CommitError::InvalidInput)?,
        },
        target: TransitionEndpoint {
            path: coordinator.index_path.clone(),
            device: target_identity.device,
            inode: target_identity.inode,
            binding: serde_json::to_string(candidate.target)
                .map_err(|_| CommitError::InvalidInput)?,
        },
        candidate_path: paths.candidate.to_owned(),
        archive_path: paths.archive.to_owned(),
        quarantine_generation: paths.quarantine_generation.into(),
        quarantine_epoch: candidate
            .source
            .epoch()
            .checked_add(1)
            .ok_or(CommitError::InvalidInput)?,
        snapshot_sha256: prepared.snapshot_sha256.clone(),
        asset_sha256: candidate.dictionary.manifest_digest().into(),
        ready_sha256: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&prepared).map_err(|_| CommitError::InvalidInput)?)
        ),
        adoption_sha256: "0".repeat(64),
        backup_path: paths.backup.to_owned(),
        backup,
    };
    // This capture hashes original SQL bytes only. Its nonzero version checks
    // plan translatability; owned preview resolves the actual target version.
    record.adoption_sha256 = capture_backup_manifest(
        &record,
        &ComparisonInputs {
            source: candidate.source,
            target: candidate.target,
            target_version: prepared.source_version,
            plan_bytes: adoption.plan_bytes,
            backup_bytes,
            limits,
        },
    )?
    .sha256;
    let owner = TransitionOwnership::acquire(&coordinator.state_path, &record)?;
    if let Some(previous) = previous {
        owner.retire_completed(now, &previous)?;
    }
    record.persist(&owner, None)?;
    publication::preview_publication(&owner, now, candidate, adoption, limits, backup_bytes)?;
    Ok(record)
}
