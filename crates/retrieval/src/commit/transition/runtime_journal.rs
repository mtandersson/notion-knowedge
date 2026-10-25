//! Version-two active publication journal. Version-one readers must refuse this
//! record while it occupies the existing admission journal path. Completion
//! retains history durably before removing that admission blocker.
use super::*;
use crate::{
    chunks::RuntimeSourceSnapshot,
    rollback_runtime::{prepared::PreparedRollbackMetadata, source::canonical as bounded_json},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
const MAX_BYTES: usize = 524288;
const PROTOCOL: &str = "selected-runtime-publication-v2";
fn invalid<T>(_: T) -> CommitError {
    CommitError::BindingMismatch
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimePhase {
    Prepared,
    Quarantined,
    SourceRetained,
    TargetRefenced,
    TargetInstalled,
    Published,
    Complete,
}
impl RuntimePhase {
    fn refenced(self) -> bool {
        matches!(
            self,
            Self::TargetRefenced | Self::TargetInstalled | Self::Published | Self::Complete
        )
    }
}
/// Descriptive private journal data. Parsing cannot create a publication owner,
/// validate a native pair or authorize SQL/rename effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeJournalRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: RuntimePhase,
    pub state_device: u64,
    pub state_inode: u64,
    pub state_token: String,
    pub prepared: PreparedRollbackMetadata,
    pub prepared_sha256: String,
    /// Explicit immutable history names after retiring the previous active
    /// completed journal. Their contents must retain the verified lineage hash.
    pub lineage_paths: Vec<PathBuf>,
    /// Fresh unused sibling where the current physical source will be retained.
    pub archive_path: PathBuf,
    pub activation_backup_path: PathBuf,
    pub activation_backup: OfflineBackup,
    pub adoption_sha256: String,
    /// Actual selected-runtime snapshot after fresh physical refencing. Rename
    /// changes later physical inventories; it never permits row/plan changes.
    pub refenced: Option<RuntimeSourceSnapshot>,
}
impl RuntimeJournalRecord {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CommitError> {
        let prepared = self.prepared.canonical_bytes().map_err(invalid)?;
        let candidate = &self.prepared.candidate;
        let absolute = |path: &Path| {
            path.is_absolute()
                && path.file_name().is_some()
                && !path.components().any(|p| {
                    matches!(
                        p,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
        };
        if self.version != 2
            || self.protocol != PROTOCOL
            || self.state_token.len() != 32
            || !self.state_token.bytes().all(|b| b.is_ascii_hexdigit())
            || self.prepared_sha256 != format!("{:x}", Sha256::digest(prepared))
            || self.lineage_paths.is_empty()
            || self.lineage_paths.len() > 64
            || self.lineage_paths.iter().any(|p| !absolute(p))
            || !absolute(&self.archive_path)
            || !absolute(&self.activation_backup_path)
            || self.archive_path.parent() != self.prepared.current.path.parent()
            || [
                &self.prepared.current.path,
                &candidate.candidate.path,
                &self.prepared.prior_archive.path,
            ]
            .contains(&&self.archive_path)
            || [
                &self.prepared.current.path,
                &candidate.candidate.path,
                &self.prepared.prior_archive.path,
                &self.archive_path,
            ]
            .iter()
            .any(|p| {
                self.activation_backup_path.starts_with(p)
                    || self.prepared.state_path.starts_with(p)
            })
            || self.activation_backup_path == self.prepared.state_path
            || self.activation_backup != candidate.current.metadata.operational
            || !digest(&self.activation_backup.sha256)
            || !digest(&self.adoption_sha256)
            || self.phase.refenced() != self.refenced.is_some()
        {
            return Err(CommitError::BindingMismatch);
        }
        if let Some(snapshot) = &self.refenced {
            if snapshot.version != 2
                || snapshot.source_version == 0
                || snapshot.rows != candidate.current.metadata.source.rows
                || snapshot.snapshot_sha256 != candidate.current.metadata.source.snapshot_sha256
                || !digest(&snapshot.authority_sha256)
            {
                return Err(CommitError::BindingMismatch);
            }
            let target = CommitBinding::parse_retained(candidate.target_authority_json.as_bytes())?;
            match &snapshot.lexical {
                Some(binding) if candidate.prior.descriptor.describes_bound_source(binding) => {
                    binding
                        .validate_transfer_metadata(&candidate.current.metadata.embedding, &target)
                        .map_err(invalid)?;
                }
                None if candidate.prior.descriptor.is_legacy() => (),
                _ => return Err(CommitError::BindingMismatch),
            }
        }
        bounded_json(self, MAX_BYTES).map_err(invalid)
    }
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self, CommitError> {
        if bytes.len() > MAX_BYTES {
            return Err(CommitError::InvalidInput);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(invalid)?;
        if value.canonical_bytes()? != bytes {
            return Err(CommitError::BindingMismatch);
        }
        Ok(value)
    }
}
