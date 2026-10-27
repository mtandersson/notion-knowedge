//! Descriptive native-phase journal. A parsed record never grants ownership.
//!
//! The v4 journal will replace the active v3 handoff only after its exact
//! contents and the reservation intent have been durably retained. Native
//! effects require fresh physical, SQL and selected-runtime verification.
use super::*;
use crate::rollback_runtime::source::SourceReport;
use sha2::{Digest, Sha256};

const MAX_BYTES: usize = 4 << 20;
const PROTOCOL: &str = "explicit-choice-native-v4";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChoiceNativePhase {
    Ready,
    OtherRetained,
    SourceRetained,
    TargetRefenced,
    TargetInstalled,
}

/// A bounded description of native work under the original quarantined owner.
/// File identities are descriptive until rechecked against retained descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceNativeRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: ChoiceNativePhase,
    pub handoff: ChoiceHandoffRecord,
    pub reservation: PublicationStartMetadata,
    pub handoff_history_path: PathBuf,
    pub handoff_history_identity: RetainedFileIdentity,
    pub reservation_identity: RetainedFileIdentity,
    pub refenced: Option<SourceReport>,
}

impl ChoiceNativeRecord {
    /// Derive one immutable history name from the exact v3 handoff bytes.
    pub fn handoff_history_path(handoff: &ChoiceHandoffRecord) -> Result<PathBuf> {
        let digest = format!("{:x}", Sha256::digest(handoff.canonical_bytes()?));
        let active =
            crate::commit::transition::journal_path(&handoff.prepared.selection.state_path)
                .map_err(|_| RuntimeError)?;
        let mut name = active.file_name().ok_or(RuntimeError)?.to_os_string();
        name.push(format!(".{digest}.choice-native-origin.history"));
        Ok(active.with_file_name(name))
    }

    /// Validate canonical bounded description only. The caller must acquire
    /// the original physical owner and verify actual SQL, files and runtime.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.handoff.canonical_bytes()?;
        self.reservation.canonical_bytes(&self.handoff)?;
        let origin = match &self.handoff.prepared.selection.origin {
            Origin::Activation(record) => *record.clone(),
            Origin::Runtime(record) => record.layout().map_err(|_| RuntimeError)?,
        };
        let history = Self::handoff_history_path(&self.handoff)?;
        let protected = [
            &origin.source.path,
            &origin.target.path,
            &origin.candidate_path,
            &origin.archive_path,
            &self.handoff.prepared.candidate.path,
        ];
        let retained = [
            &self.handoff.prepared_path,
            &self.handoff.prepared.transfer_path,
            &self.handoff.prepared.backup_path,
            &self.handoff.origin_history_path,
            &self.handoff.prepared.selection.state_path,
            &origin.backup_path,
        ];
        let key = |identity: &RetainedFileIdentity| (identity.device, identity.inode);
        let existing = [
            &self.handoff.prepared_identity,
            &self.handoff.transfer_identity,
            &self.handoff.backup_identity,
            &self.handoff.origin_identity,
        ];
        if self.version != 4
            || self.protocol != PROTOCOL
            || self.handoff_history_path != history
            || protected.iter().any(|path| history.starts_with(path))
            || retained.contains(&&history)
            || self.handoff.prepared.source_evidence_path.as_ref() == Some(&history)
            || matches!(&self.handoff.prepared.selection.origin, Origin::Runtime(record)
                if history.starts_with(&record.prepared.prior_archive.path)
                    || record.lineage_paths.contains(&history))
            || self.handoff_history_identity.inode == 0
            || self.reservation_identity.inode == 0
            || key(&self.handoff_history_identity) == key(&self.reservation_identity)
            || existing.iter().any(|identity| {
                key(identity) == key(&self.handoff_history_identity)
                    || key(identity) == key(&self.reservation_identity)
            })
            || self
                .handoff
                .source_evidence_identity
                .as_ref()
                .is_some_and(|identity| {
                    key(identity) == key(&self.handoff_history_identity)
                        || key(identity) == key(&self.reservation_identity)
                })
            || [
                self.handoff_history_identity.mtime_nsec,
                self.handoff_history_identity.ctime_nsec,
                self.reservation_identity.mtime_nsec,
                self.reservation_identity.ctime_nsec,
            ]
            .iter()
            .any(|nanos| !(0..1_000_000_000).contains(nanos))
        {
            return Err(RuntimeError);
        }
        let has_refenced = matches!(
            self.phase,
            ChoiceNativePhase::TargetRefenced | ChoiceNativePhase::TargetInstalled
        );
        if self.refenced.is_some() != has_refenced {
            return Err(RuntimeError);
        }
        if let Some(report) = &self.refenced {
            let bytes = source::canonical(report, 65536)?;
            SourceReport::parse_metadata(&bytes)?;
            let selected = &self.handoff.prepared.selection;
            if report.descriptor != selected.observation.descriptor
                || report.source.source_version != selected.current_native.source_version
                || report.source.rows != selected.current_native.rows
                || report.source.snapshot_sha256 != selected.current_native.snapshot_sha256
                || report.source.lexical.is_some()
                    != self
                        .handoff
                        .prepared
                        .candidate_report
                        .source
                        .lexical
                        .is_some()
            {
                return Err(RuntimeError);
            }
            if let Some(binding) = &report.source.lexical {
                let desired = CommitBinding::parse_retained(
                    self.reservation.desired_authority_json.as_bytes(),
                )
                .map_err(|_| RuntimeError)?;
                binding
                    .validate_selected_metadata(
                        &self.handoff.prepared.transfer.metadata.embedding,
                        &desired,
                        &self.handoff.prepared.candidate,
                        &selected.current_native.snapshot_sha256,
                    )
                    .map_err(|_| RuntimeError)?;
                if !report.descriptor.describes_bound_source(binding) {
                    return Err(RuntimeError);
                }
            }
        }
        source::canonical(self, MAX_BYTES)
    }

    /// Parse a canonical description without opening a database or index.
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let record: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if record.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(record)
    }
}
