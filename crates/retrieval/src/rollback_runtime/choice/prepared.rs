//! Candidate and immutable SQL preview while the original quarantine is held.
use super::*;
use crate::{
    chunks::{
        canonical_transfer::{self, TransferRecord},
        runtime_builder::{CandidateReport, CandidateRequest},
    },
    commit::transition::manifest::SqlManifest,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};
pub(super) const MAX: usize = 1 << 20;
const PROTOCOL: &str = "quarantined-runtime-choice-prepared-v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedChoiceMetadata {
    pub version: u32,
    pub protocol: String,
    pub selection: SelectionMetadata,
    pub source_evidence_path: Option<PathBuf>,
    pub transfer_path: PathBuf,
    pub transfer: TransferRecord,
    pub candidate: DirectoryIdentity,
    pub observed_authority_json: String,
    pub candidate_report: SourceReport,
    pub built: Option<CandidateReport>,
    pub backup_path: PathBuf,
    pub backup: OfflineBackup,
    pub adoption: SqlManifest,
}
/// Source selection needs a distinct candidate so the retained original source
/// remains immutable. A target can reuse its already verified physical pair.
pub struct PreparationPaths<'a> {
    pub transfer: &'a Path,
    pub candidate: Option<&'a Path>,
    pub backup: &'a Path,
}
pub struct VerifiedPreparedChoice<'a, 'owner> {
    pub(super) selection: VerifiedSelection<'a, 'owner>,
    pub(super) metadata: PreparedChoiceMetadata,
    pub(super) guard: Option<File>,
}
impl PreparedChoiceMetadata {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.selection.origin.canonical_bytes()?;
        let desired =
            CommitBinding::parse_retained(self.selection.desired_authority_json.as_bytes())
                .map_err(|_| RuntimeError)?;
        let origin = match &self.selection.origin {
            Origin::Activation(record) => *record.clone(),
            Origin::Runtime(record) => record.layout().map_err(|_| RuntimeError)?,
        };
        let original = CommitBinding::parse_retained(origin.source.binding.as_bytes())
            .map_err(|_| RuntimeError)?;
        let origin_bytes = match &self.selection.origin {
            Origin::Activation(record) => source::canonical(record, 65536)?,
            Origin::Runtime(record) => record.canonical_bytes().map_err(|_| RuntimeError)?,
        };
        if format!("{:x}", Sha256::digest(origin_bytes)) != self.selection.operation_sha256
            || self.transfer.metadata.authority_json != origin.source.binding
            || desired.table() != original.table()
            || desired.workspace() != original.workspace()
            || desired.roots() != original.roots()
            || desired.scope() != original.scope()
            || self.selection.current_native.version != 2
            || self.selection.current_native.source_version == 0
            || self.selection.observation.source.rows != self.selection.current_native.rows
            || self.selection.observation.source.snapshot_sha256
                != self.selection.current_native.snapshot_sha256
            || (self.selection.choice == Choice::OriginalSource)
                != self.source_evidence_path.is_some()
            || self.transfer_path == self.backup_path
            || self.backup.bytes == 0
            || self.transfer_path.starts_with(&self.candidate.path)
            || self.backup_path.starts_with(&self.candidate.path)
        {
            return Err(RuntimeError);
        }
        for hash in [
            &self.selection.operation_sha256,
            &self.selection.source_authority_sha256,
            &self.selection.original_source_authority_sha256,
            &self.selection.runtime_sha256,
            &self.transfer.sha256,
            &self.transfer.metadata.source.snapshot_sha256,
            &self.transfer.metadata.source.authority_sha256,
            &self.backup.sha256,
            &self.adoption.sha256,
        ]
        .into_iter()
        .chain(self.selection.asset_sha256.iter())
        {
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(RuntimeError);
            }
        }
        if self.version != 1
            || self.protocol != PROTOCOL
            || self.selection.version != 1
            || self.selection.protocol != "quarantined-runtime-selection-v1"
            || desired.epoch() <= self.selection.highest_reserved_epoch
            || self.transfer.metadata.source != self.selection.current_native
            || self.transfer.metadata.operational != self.selection.operational
            || self.transfer.metadata.ownership_sha256 != self.selection.source_authority_sha256
            || self.backup != self.selection.operational
            || self.candidate_report.descriptor != self.selection.observation.descriptor
            || self.candidate_report.source.rows != self.transfer.metadata.source.rows
            || self.candidate_report.source.snapshot_sha256
                != self.transfer.metadata.source.snapshot_sha256
            || (self.candidate.device, self.candidate.inode)
                == (self.selection.current.device, self.selection.current.inode)
            || (self.selection.choice == Choice::OriginalSource && self.built.is_none())
        {
            return Err(RuntimeError);
        }
        for path in [
            &self.transfer_path,
            &self.backup_path,
            &self.candidate.path,
            &self.selection.state_path,
        ]
        .into_iter()
        .chain(self.source_evidence_path.iter())
        {
            if !path.is_absolute()
                || path.file_name().is_none()
                || path.components().any(|p| {
                    matches!(
                        p,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
            {
                return Err(RuntimeError);
            }
        }
        if let Some(built) = &self.built {
            CandidateReport::parse_metadata(&source::canonical(built, 131072)?)
                .map_err(|_| RuntimeError)?;
            if self.candidate.path.parent() != self.selection.current.path.parent() {
                return Err(RuntimeError);
            }
            if built.candidate != self.candidate
                || built.runtime != self.candidate_report.descriptor
                || built.snapshot != self.candidate_report.source
                || built.target_authority_json != self.selection.desired_authority_json
                || built.transfer_sha256 != self.transfer.sha256
                || self.observed_authority_json != self.selection.desired_authority_json
            {
                return Err(RuntimeError);
            }
        } else if self.candidate != self.selection.selected
            || self.observed_authority_json != self.selection.observed_authority_json
            || self.candidate_report != self.selection.observation
        {
            return Err(RuntimeError);
        }
        SourceReport::parse_metadata(&source::canonical(&self.candidate_report, 65536)?)?;
        source::canonical(self, MAX)
    }
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if value.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(value)
    }
}
impl<'a, 'owner> VerifiedSelection<'a, 'owner> {
    pub fn prepare(
        self,
        paths: PreparationPaths<'_>,
    ) -> Result<VerifiedPreparedChoice<'a, 'owner>> {
        self.revalidate()?;
        for path in [paths.transfer, paths.backup]
            .into_iter()
            .chain(paths.candidate)
        {
            self.inspection
                .choice_output(path)
                .map_err(|_| RuntimeError)?;
        }
        if paths.transfer == paths.backup
            || paths
                .candidate
                .is_some_and(|p| paths.transfer.starts_with(p) || paths.backup.starts_with(p))
        {
            return Err(RuntimeError);
        }
        let transfer = canonical_transfer::export_quarantined(
            self.inspection,
            paths.transfer,
            self.inspection.embedding(),
            self.limits.transfer,
        )
        .map_err(|_| RuntimeError)?;
        if transfer.metadata.source != self.metadata.current_native
            || transfer.metadata.operational != self.metadata.operational
            || transfer.metadata.ownership_sha256 != self.metadata.source_authority_sha256
        {
            return Err(RuntimeError);
        }
        let (candidate, observed, candidate_report, built, guard) =
            if let Some(path) = paths.candidate {
                let request = CandidateRequest {
                    version: 1,
                    source: self.metadata.current.clone(),
                    transfer_path: paths.transfer.into(),
                    transfer: transfer.clone(),
                    target_authority_json: self.metadata.desired_authority_json.clone(),
                    candidate: path.into(),
                    runtime: self.metadata.observation.descriptor.clone(),
                    limits: self.limits.transfer,
                };
                let built = builder::build(
                    self.inputs.runtime,
                    self.inputs.asset,
                    &request,
                    self.limits.process_budget,
                )?;
                let guard = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(path)
                    .map_err(|_| RuntimeError)?;
                guard.try_lock().map_err(|_| RuntimeError)?;
                let report = observe(
                    self.inputs.runtime,
                    self.inputs.asset,
                    path,
                    self.inputs.desired,
                    self.inspection.embedding(),
                    self.limits.transfer,
                    self.limits.process_budget,
                )?;
                if report.source != built.snapshot || report.descriptor != built.runtime {
                    return Err(RuntimeError);
                }
                (
                    built.candidate.clone(),
                    self.metadata.desired_authority_json.clone(),
                    report,
                    Some(built),
                    Some(guard),
                )
            } else {
                if self.metadata.choice == Choice::OriginalSource {
                    return Err(RuntimeError);
                }
                (
                    self.metadata.selected.clone(),
                    self.metadata.observed_authority_json.clone(),
                    self.metadata.observation.clone(),
                    None,
                    None,
                )
            };
        let adoption = self
            .inspection
            .choice_preview(
                self.inputs.desired,
                candidate_report.source.source_version,
                self.limits,
            )
            .map_err(|_| RuntimeError)?;
        let backup = self
            .inspection
            .choice_backup(paths.backup, self.limits.transfer.sql_bytes, true)
            .map_err(|_| RuntimeError)?;
        let metadata = PreparedChoiceMetadata {
            version: 1,
            protocol: PROTOCOL.into(),
            selection: self.metadata.clone(),
            source_evidence_path: self.inputs.source_evidence.map(Into::into),
            transfer_path: paths.transfer.into(),
            transfer,
            candidate,
            observed_authority_json: observed,
            candidate_report,
            built,
            backup_path: paths.backup.into(),
            backup,
            adoption,
        };
        let prepared = VerifiedPreparedChoice {
            selection: self,
            metadata,
            guard,
        };
        prepared.revalidate()?;
        Ok(prepared)
    }
}
impl VerifiedPreparedChoice<'_, '_> {
    pub fn metadata(&self) -> &PreparedChoiceMetadata {
        &self.metadata
    }
    pub fn revalidate(&self) -> Result<()> {
        self.metadata.canonical_bytes()?;
        self.selection.revalidate()?;
        let inspection = self.selection.inspection;
        let limits = self.selection.limits;
        if self.metadata.transfer.metadata.embedding != *inspection.embedding()
            || self.metadata.transfer.metadata.authority_json
                != serde_json::to_string(inspection.binding()).map_err(|_| RuntimeError)?
        {
            return Err(RuntimeError);
        }
        for path in [&self.metadata.transfer_path, &self.metadata.backup_path] {
            inspection.choice_output(path).map_err(|_| RuntimeError)?;
            if path.starts_with(&self.metadata.candidate.path) {
                return Err(RuntimeError);
            }
        }
        if self.metadata.built.is_some() {
            inspection
                .choice_output(&self.metadata.candidate.path)
                .map_err(|_| RuntimeError)?;
        }
        if self.metadata.selection != self.selection.metadata
            || self.metadata.source_evidence_path.as_deref()
                != self.selection.inputs.source_evidence
            || self.metadata.backup
                != inspection
                    .choice_backup(&self.metadata.backup_path, limits.transfer.sql_bytes, false)
                    .map_err(|_| RuntimeError)?
            || self.metadata.adoption
                != inspection
                    .choice_preview(
                        self.selection.inputs.desired,
                        self.metadata.candidate_report.source.source_version,
                        limits,
                    )
                    .map_err(|_| RuntimeError)?
            || DirectoryIdentity::capture(&self.metadata.candidate.path)
                .map_err(|_| RuntimeError)?
                != self.metadata.candidate
        {
            return Err(RuntimeError);
        }
        if let Some(guard) = &self.guard {
            let m = guard.metadata().map_err(|_| RuntimeError)?;
            if m.dev() != self.metadata.candidate.device || m.ino() != self.metadata.candidate.inode
            {
                return Err(RuntimeError);
            }
        }
        let binding =
            CommitBinding::parse_retained(self.metadata.observed_authority_json.as_bytes())
                .map_err(|_| RuntimeError)?;
        if observe(
            self.selection.inputs.runtime,
            self.selection.inputs.asset,
            &self.metadata.candidate.path,
            &binding,
            inspection.embedding(),
            limits.transfer,
            limits.process_budget,
        )? != self.metadata.candidate_report
        {
            return Err(RuntimeError);
        }
        canonical_transfer::read(
            &self.metadata.transfer_path,
            &self.metadata.transfer,
            limits.transfer,
            |_| Ok(()),
        )
        .map_err(|_| RuntimeError)?;
        self.selection.revalidate()
    }
}
