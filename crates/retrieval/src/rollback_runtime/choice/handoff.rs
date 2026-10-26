//! Descriptive version-three handoff metadata. It never grants ownership.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_BYTES: usize = 2 << 20;
const PROTOCOL: &str = "explicit-choice-handoff-v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandoffPhase {
    Handoff,
}

/// Strict metadata for a future active journal. Parsing or constructing this
/// value does not transfer the original owner or authorize any effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceHandoffRecord {
    pub version: u32,
    pub protocol: String,
    pub phase: HandoffPhase,
    pub prepared_path: PathBuf,
    pub prepared: PreparedChoiceMetadata,
    pub prepared_sha256: String,
    pub origin_history_path: PathBuf,
    pub origin_sha256: String,
}

fn history_path(state: &Path, origin_sha256: &str) -> Result<PathBuf> {
    let active = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
    let mut name = active.file_name().ok_or(RuntimeError)?.to_os_string();
    name.push(format!(".{origin_sha256}.choice-origin.history"));
    Ok(active.with_file_name(name))
}

impl ChoiceHandoffRecord {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let prepared = self.prepared.canonical_bytes()?;
        let state = &self.prepared.selection.state_path;
        let journal = crate::commit::transition::journal_path(state).map_err(|_| RuntimeError)?;
        let origin = &self.prepared.selection.origin;
        let layout = match origin {
            Origin::Activation(record) => *record.clone(),
            Origin::Runtime(record) => record.layout().map_err(|_| RuntimeError)?,
        };
        let mut protected = vec![
            &layout.source.path,
            &layout.candidate_path,
            &layout.archive_path,
            &self.prepared.selection.current.path,
            &self.prepared.selection.selected.path,
            &self.prepared.candidate.path,
        ];
        if let Origin::Runtime(record) = origin {
            protected.push(&record.prepared.prior_archive.path);
        }
        let valid_path = |path: &Path| {
            path.is_absolute()
                && path.file_name().is_some()
                && !path.components().any(|p| {
                    matches!(
                        p,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
        };
        if self.version != 3
            || self.protocol != PROTOCOL
            || self.phase != HandoffPhase::Handoff
            || !valid_path(&self.prepared_path)
            || self.prepared_sha256 != format!("{:x}", Sha256::digest(prepared))
            || self.origin_sha256 != self.prepared.selection.operation_sha256
            || self.origin_history_path != history_path(state, &self.origin_sha256)?
            || self.prepared_path == journal
            || self.prepared_path == self.origin_history_path
            || self.prepared_path == *state
            || self.prepared_path == self.prepared.transfer_path
            || self.prepared_path == self.prepared.backup_path
            || self.prepared.source_evidence_path.as_ref() == Some(&self.prepared_path)
            || protected.iter().any(|p| self.prepared_path.starts_with(p))
        {
            return Err(RuntimeError);
        }
        source::canonical(self, MAX_BYTES)
    }

    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if value.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(value)
    }
}

impl VerifiedPreparedChoice<'_, '_> {
    /// Describe a choice only after checking its original owner and the actual
    /// private Prepared artifact. This returns data, not a handoff capability.
    pub fn describe_handoff(&self, prepared_path: &Path) -> Result<ChoiceHandoffRecord> {
        self.revalidate()?;
        self.selection
            .inspection
            .choice_output(prepared_path)
            .map_err(|_| RuntimeError)?;
        if PreparedChoiceMetadata::read(prepared_path)? != *self.metadata() {
            return Err(RuntimeError);
        }
        let prepared = self.metadata().clone();
        let origin_sha256 = prepared.selection.operation_sha256.clone();
        let record = ChoiceHandoffRecord {
            version: 3,
            protocol: PROTOCOL.into(),
            phase: HandoffPhase::Handoff,
            prepared_path: prepared_path.into(),
            prepared_sha256: format!("{:x}", Sha256::digest(prepared.canonical_bytes()?)),
            origin_history_path: history_path(&prepared.selection.state_path, &origin_sha256)?,
            origin_sha256,
            prepared,
        };
        record.canonical_bytes()?;
        self.revalidate()?;
        if PreparedChoiceMetadata::read(prepared_path)? != record.prepared {
            return Err(RuntimeError);
        }
        Ok(record)
    }
}
