//! Read-only provenance for an explicitly selected retained physical generation.
use super::*;
use crate::commit::transition::TransitionRecord;

/// Internal live ownership result; never constructible from retained metadata.
pub(crate) struct PriorArchive {
    pub binding: CommitBinding,
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub ownership_sha256: String,
    pub snapshot_sha256: String,
    pub lineage_sha256: String,
}
impl OwnedOfflineInspection<'_> {
    /// Follow explicitly named completed records newest first. The first must
    /// end at this actual current owner; every older physical endpoint must
    /// connect exactly. No directory scan or operational backup is used.
    pub(crate) fn prior_archive(&self, paths: &[PathBuf]) -> Result<PriorArchive, CommitError> {
        self.revalidate()?;
        if paths.is_empty() || paths.len() > 64 {
            return Err(CommitError::InvalidInput);
        }
        let c = self.coordinator;
        let mut binding = c.binding.clone();
        let mut device = c.index_identity.device;
        let mut inode = c.index_identity.inode;
        let mut records = Vec::new();
        for path in paths {
            let record = TransitionRecord::read_completed(&c.state_path, path)?;
            if record.state_device != c.state_identity.device
                || record.state_inode != c.state_identity.inode
                || record.state_token != c.state_token
                || record.target.path != c.index_path
                || record.target.device != device
                || record.target.inode != inode
                || record.target.binding
                    != serde_json::to_string(&binding).map_err(|_| CommitError::InvalidInput)?
            {
                return Err(CommitError::BindingMismatch);
            }
            binding = CommitBinding::parse_retained(record.source.binding.as_bytes())?;
            device = record.source.device;
            inode = record.source.inode;
            records.push(record);
        }
        let prior = records.last().ok_or(CommitError::InvalidInput)?;
        let bytes = serde_json::to_vec(&(
            &binding,
            &prior.source.path,
            &c.state_path,
            device,
            inode,
            c.state_identity.device,
            c.state_identity.inode,
            &c.state_token,
        ))
        .map_err(|_| CommitError::InvalidInput)?;
        let lineage = serde_json::to_vec(&records).map_err(|_| CommitError::InvalidInput)?;
        self.revalidate()?;
        Ok(PriorArchive {
            binding,
            path: prior.archive_path.clone(),
            device,
            inode,
            ownership_sha256: format!("{:x}", Sha256::digest(bytes)),
            snapshot_sha256: prior.snapshot_sha256.clone(),
            lineage_sha256: format!("{:x}", Sha256::digest(lineage)),
        })
    }
}
