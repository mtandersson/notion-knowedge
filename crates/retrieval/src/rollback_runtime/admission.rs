//! Explicit read-only ordinary admission under the selected implementation.
//! This action never starts workers/listeners, migrates SQL or performs effects.
use super::{Result, RuntimeError, source};
use crate::{
    chunks::CandidateLimits,
    commit::{CommitBinding, IndexCommitCoordinator},
    lexical_dictionary::Dictionary,
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
const MAX: usize = 65536;
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRequest {
    pub version: u32,
    pub directory: PathBuf,
    pub state: PathBuf,
    pub authority_json: String,
    pub embedding: EmbeddingMetadata,
    pub limits: CandidateLimits,
}
impl AdmissionRequest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        source::canonical(self, MAX)
    }
}
pub fn inspect(path: &Path, dictionary: Option<&Dictionary>) -> Result<Vec<u8>> {
    let parent = super::evidence::parent(path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.mode() & 0o077 != 0
        || before.uid() != std::fs::metadata("/proc/self")?.uid()
        || before.len() > MAX as u64
    {
        return Err(RuntimeError);
    }
    let mut bytes = Vec::new();
    (&mut file).take(MAX as u64 + 1).read_to_end(&mut bytes)?;
    let request: AdmissionRequest = serde_json::from_slice(&bytes).map_err(err)?;
    let after = std::fs::symlink_metadata(path)?;
    if request.version != 1
        || request.canonical_bytes()? != bytes
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.len() != before.len()
        || after.nlink() != 1
        || after.mode() != before.mode()
        || after.uid() != before.uid()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
        || !request.directory.is_absolute()
        || !request.state.is_absolute()
        || std::fs::canonicalize(&request.directory)? != request.directory
        || std::fs::canonicalize(&request.state)? != request.state
    {
        return Err(RuntimeError);
    }
    super::evidence::check_parent(&parent, path)?;
    let authority =
        CommitBinding::parse_retained(request.authority_json.as_bytes()).map_err(err)?;
    let coordinator = IndexCommitCoordinator::open_read_only(
        &request.directory,
        &request.state,
        authority.clone(),
    )
    .map_err(err)?;
    let _guard = coordinator.search_guard().map_err(err)?;
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let native = root.path().join("native.json");
    let mut config = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&native)?;
    config.write_all(&source::archived_request_bytes(
        &request.directory,
        &authority,
        &request.embedding,
        request.limits,
    )?)?;
    drop(config);
    let report = source::inspect_request(&native, dictionary)?;
    coordinator.identities().map_err(err)?;
    super::evidence::check_parent(&parent, path)?;
    Ok(report)
}
