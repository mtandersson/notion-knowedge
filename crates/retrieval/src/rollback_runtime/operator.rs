//! Explicit offline verification only; never activation or source repair.
use super::{
    PinnedRuntime, Result, RuntimeError,
    evidence::{self, RetainedEvidence},
    source::canonical,
    verification::{self, PinnedAsset, VerificationLimits},
};
use crate::commit::{CommitBinding, CommitError, IndexCommitCoordinator};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationConfig {
    pub version: u32,
    pub index_directory: PathBuf,
    pub state_file: PathBuf,
    /// Exact canonical CommitBinding JSON from trusted operator configuration.
    /// SQL and physical source authority must independently match it.
    pub authority_json: String,
    pub embedding: EmbeddingMetadata,
    pub runtime: PathBuf,
    pub runtime_sha256: String,
    pub asset: Option<AssetConfig>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetConfig {
    pub path: PathBuf,
    pub sha256: String,
}
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
fn configuration(path: &Path) -> Result<VerificationConfig> {
    if !path.is_absolute() || std::fs::canonicalize(path)? != path {
        return Err(RuntimeError);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let m = file.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != std::fs::metadata("/proc/self")?.uid()
        || m.mode() & 0o077 != 0
        || m.len() > 65536
    {
        return Err(RuntimeError);
    }
    let mut bytes = Vec::new();
    (&mut file).take(65537).read_to_end(&mut bytes)?;
    let config: VerificationConfig = serde_json::from_slice(&bytes).map_err(err)?;
    let current = std::fs::symlink_metadata(path)?;
    if config.version != 1
        || canonical(&config, 65536)? != bytes
        || current.dev() != m.dev()
        || current.ino() != m.ino()
        || current.len() != m.len()
        || current.mode() != m.mode()
        || current.uid() != m.uid()
        || current.nlink() != 1
        || current.ctime() != m.ctime()
        || current.ctime_nsec() != m.ctime_nsec()
    {
        return Err(RuntimeError);
    }
    for p in [&config.index_directory, &config.state_file, &config.runtime] {
        if !p.is_absolute() || std::fs::canonicalize(p)? != *p {
            return Err(RuntimeError);
        }
    }
    Ok(config)
}
/// Run with explicit shutdown acknowledgement. `retained` requires the named
/// file to exist and remain identical; it never means discovering a record.
/// All original source processes must remain stopped until this returns.
pub fn verify(
    configuration_path: &Path,
    evidence_path: &Path,
    retained: bool,
    all_source_processes_stopped: bool,
) -> Result<()> {
    if !all_source_processes_stopped {
        return Err(RuntimeError);
    }
    let config = configuration(configuration_path)?;
    let previous = if retained {
        Some(RetainedEvidence::read(evidence_path)?)
    } else {
        match std::fs::symlink_metadata(evidence_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            _ => return Err(RuntimeError),
        }
    };
    let binding = CommitBinding::parse_retained(config.authority_json.as_bytes()).map_err(err)?;
    let runtime = PinnedRuntime::pin(&config.runtime, &config.runtime_sha256, 1024 * 1024 * 1024)?;
    let asset = config
        .asset
        .as_ref()
        .map(|a| PinnedAsset::pin(&a.path, &a.sha256))
        .transpose()?;
    let coordinator =
        IndexCommitCoordinator::open_existing(&config.index_directory, &config.state_file, binding)
            .map_err(err)?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(err)?
            .as_secs(),
    )
    .map_err(err)?;
    coordinator
        .with_owned_offline_inspection(now, |owner| {
            let proof = verification::verify(
                owner,
                &runtime,
                asset.as_ref(),
                &config.embedding,
                VerificationLimits::default(),
            )
            .map_err(|_| CommitError::BindingMismatch)?;
            let result = if retained {
                evidence::reuse(evidence_path, owner, &proof)
            } else {
                evidence::publish(evidence_path, owner, &proof)
            }
            .map_err(|_| CommitError::BindingMismatch)?;
            if previous.as_ref().is_some_and(|p| *p != result) {
                return Err(CommitError::BindingMismatch);
            }
            Ok(())
        })
        .map_err(err)
}
#[cfg(test)]
mod tests;
