//! Runtime-specific Prepared records use a separate protocol from the original
//! current-host-dictionary activation journal. Metadata alone grants no rights.
use super::{
    Result, RuntimeError,
    candidate::{CandidateEvidence, CandidateKind},
    source::{self, SourceReport},
};
use crate::{
    chunks::runtime_builder::{CandidateReport, DirectoryIdentity},
    commit::{CommitBinding, OfflineBackup, transition::manifest::SqlManifest},
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
mod owner;
mod store;
pub use owner::{PreparedInputs, PreparedLimits, VerifiedPreparedRollback, prepare, reopen};
const MAX_BYTES: usize = 262144;
const PROTOCOL: &str = "selected-runtime-rollback-prepared-v1";
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && path.file_name().is_some()
        && !path.components().any(|p| {
            matches!(
                p,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
}
fn logical(a: &CommitBinding, b: &CommitBinding) -> bool {
    a.table() == b.table()
        && a.workspace() == b.workspace()
        && a.roots() == b.roots()
        && a.scope() == b.scope()
}
/// Bounded descriptive metadata, not a verified Prepared capability. The
/// candidate's ownership digest commits to the original SQL path/inode/token
/// and current source identity; fresh retained ownership must establish it.
///
/// In ExactArchive mode observed_target_authority_json is the OLD binding.
/// Publication must durably refence under the selected implementation. Parsing
/// this document never reserves a generation, mutates SQLite or permits serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedRollbackMetadata {
    pub version: u32,
    pub protocol: String,
    pub state_path: PathBuf,
    pub transfer_path: PathBuf,
    pub prior_evidence_path: PathBuf,
    pub lineage_paths: Vec<PathBuf>,
    pub prior_archive: DirectoryIdentity,
    pub current: DirectoryIdentity,
    pub candidate: CandidateEvidence,
    pub observed_target_authority_json: String,
    pub quarantine_generation: String,
    pub quarantine_epoch: u64,
    pub backup_path: PathBuf,
    pub backup: OfflineBackup,
    pub adoption: SqlManifest,
}
impl PreparedRollbackMetadata {
    /// Canonical bounded representation. This validates descriptive relations
    /// only; actual native/SQL/runtime verification is a separate operation.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate_metadata()?;
        source::canonical(self, MAX_BYTES)
    }
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(err)?;
        if value.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(value)
    }
    fn validate_metadata(&self) -> Result<()> {
        let candidate = &self.candidate;
        let current =
            CommitBinding::parse_retained(candidate.current.metadata.authority_json.as_bytes())
                .map_err(err)?;
        let target = CommitBinding::parse_retained(candidate.target_authority_json.as_bytes())
            .map_err(err)?;
        let observed =
            CommitBinding::parse_retained(self.observed_target_authority_json.as_bytes())
                .map_err(err)?;
        let original = serde_json::to_string(&current).map_err(err)?;
        let quarantine = original
            .replace(
                &format!(
                    "\"generation\":{}",
                    serde_json::to_string(current.generation()).map_err(err)?
                ),
                &format!(
                    "\"generation\":{}",
                    serde_json::to_string(&self.quarantine_generation).map_err(err)?
                ),
            )
            .replace(
                &format!("\"epoch\":{}", current.epoch()),
                &format!("\"epoch\":{}", self.quarantine_epoch),
            );
        CommitBinding::parse_retained(quarantine.as_bytes()).map_err(err)?;
        if self.version != 1
            || self.protocol != PROTOCOL
            || candidate.version != 1
            || !absolute(&self.state_path)
            || !absolute(&self.backup_path)
            || !absolute(&self.transfer_path)
            || !absolute(&self.prior_evidence_path)
            || !absolute(&self.prior_archive.path)
            || self.lineage_paths.is_empty()
            || self.lineage_paths.len() > 64
            || self.lineage_paths.iter().any(|p| !absolute(p))
            || !absolute(&self.current.path)
            || !absolute(&candidate.candidate.path)
            || self.state_path == self.backup_path
            || self.backup_path
                == crate::commit::transition::journal_path(&self.state_path).map_err(err)?
            || [&self.current.path, &candidate.candidate.path]
                .iter()
                .any(|p| self.state_path.starts_with(p) || self.backup_path.starts_with(p))
            || (self.current.device, self.current.inode)
                == (candidate.candidate.device, candidate.candidate.inode)
            || !logical(&current, &target)
            || !logical(&current, &observed)
            || current.generation() == target.generation()
            || self.quarantine_generation.is_empty()
            || self.quarantine_generation.len() > 4096
            || self.quarantine_generation == current.generation()
            || self.quarantine_generation == target.generation()
            || current.epoch().checked_add(1) != Some(self.quarantine_epoch)
            || target.epoch() <= self.quarantine_epoch
            || self.backup != candidate.current.metadata.operational
            || candidate.prior.descriptor.is_legacy() != candidate.asset_sha256.is_none()
        {
            return Err(RuntimeError);
        }
        for digest in [
            &candidate.runtime_sha256,
            &candidate.lineage_sha256,
            &candidate.current.sha256,
            &candidate.current.metadata.ownership_sha256,
            &candidate.current.metadata.operational.sha256,
            &candidate.current.metadata.source.authority_sha256,
            &candidate.current.metadata.source.snapshot_sha256,
            &candidate.prior.source.authority_sha256,
            &candidate.prior.source.snapshot_sha256,
            &self.backup.sha256,
            &self.adoption.sha256,
        ]
        .into_iter()
        .chain(candidate.asset_sha256.iter())
        {
            if !hash(digest) {
                return Err(RuntimeError);
            }
        }
        SourceReport::parse_metadata(&source::canonical(&candidate.prior, 65536)?)?;
        match (candidate.kind, &candidate.built) {
            (CandidateKind::ExactArchive, None)
                if candidate.current.metadata.source.rows == candidate.prior.source.rows
                    && candidate.current.metadata.source.snapshot_sha256
                        == candidate.prior.source.snapshot_sha256
                    && observed.epoch() < current.epoch()
                    && observed.generation() != current.generation()
                    && observed.generation() != target.generation() => {}
            (CandidateKind::Rebuilt, Some(report))
                if report.candidate == candidate.candidate
                    && report.runtime == candidate.prior.descriptor
                    && report.target_authority_json == candidate.target_authority_json
                    && report.transfer_sha256 == candidate.current.sha256
                    && report.snapshot.rows == candidate.current.metadata.source.rows
                    && report.snapshot.snapshot_sha256
                        == candidate.current.metadata.source.snapshot_sha256
                    && self.observed_target_authority_json == candidate.target_authority_json =>
            {
                CandidateReport::parse_metadata(&source::canonical(report, 131072)?)
                    .map_err(err)?;
            }
            _ => return Err(RuntimeError),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
