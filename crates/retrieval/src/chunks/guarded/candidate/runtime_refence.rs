//! Internal selected-runtime refencing under the caller's retained SQL/physical
//! owner. No original SQL is opened and the current source is never written.
use super::*;
use crate::chunks::{
    lexical_binding::runtime::RuntimeDescriptor, runtime_builder::DirectoryIdentity,
};
use crate::rollback_runtime::{
    evidence::{check_parent, parent},
    source::canonical,
};
use serde::{Deserialize, Serialize};
use std::{io::Read, os::fd::AsRawFd};
const MAX_BYTES: usize = 131072;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefenceRequest {
    pub version: u32,
    pub source: DirectoryIdentity,
    pub target: DirectoryIdentity,
    pub current_authority_json: String,
    pub old_authority_json: String,
    pub desired_authority_json: String,
    pub embedding: EmbeddingMetadata,
    pub runtime: RuntimeDescriptor,
    pub source_version: u64,
    pub rows: u64,
    pub snapshot_sha256: String,
    pub limits: CandidateLimits,
}
impl RefenceRequest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChunkTableError> {
        canonical(self, MAX_BYTES).map_err(|_| bad())
    }
    fn read(path: &Path) -> Result<Self, ChunkTableError> {
        let directory = parent(path).map_err(|_| bad())?;
        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let before = file.metadata()?;
        if !before.is_file()
            || before.nlink() != 1
            || before.mode() & 0o077 != 0
            || before.uid() != fs::metadata("/proc/self")?.uid()
            || before.len() > MAX_BYTES as u64
        {
            return Err(bad());
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        let request: Self = serde_json::from_slice(&bytes)?;
        let after = fs::symlink_metadata(path)?;
        if request.canonical_bytes()? != bytes
            || after.dev() != before.dev()
            || after.ino() != before.ino()
            || after.len() != before.len()
            || after.nlink() != 1
            || after.mode() != before.mode()
            || after.uid() != before.uid()
            || after.ctime() != before.ctime()
            || after.ctime_nsec() != before.ctime_nsec()
        {
            return Err(bad());
        }
        check_parent(&directory, path).map_err(|_| bad())?;
        Ok(request)
    }
}
fn logical(a: &crate::commit::CommitBinding, b: &crate::commit::CommitBinding) -> bool {
    a.table() == b.table()
        && a.workspace() == b.workspace()
        && a.roots() == b.roots()
        && a.scope() == b.scope()
}
/// The parent already holds the target lock; reacquiring it here would reject
/// its own operation. The private request carries exact directory identities,
/// and every write is anchored to the captured target descriptor.
pub fn execute(
    path: &Path,
    dictionary: Option<&crate::lexical_dictionary::Dictionary>,
) -> Result<Vec<u8>, ChunkTableError> {
    let request = RefenceRequest::read(path)?;
    let current =
        crate::commit::CommitBinding::parse_retained(request.current_authority_json.as_bytes())
            .map_err(|_| bad())?;
    let old = crate::commit::CommitBinding::parse_retained(request.old_authority_json.as_bytes())
        .map_err(|_| bad())?;
    let desired =
        crate::commit::CommitBinding::parse_retained(request.desired_authority_json.as_bytes())
            .map_err(|_| bad())?;
    if request.version != 1
        || request.runtime != RuntimeDescriptor::current(dictionary)?
        || !logical(&current, &old)
        || !logical(&current, &desired)
        || current.generation() == desired.generation()
        || desired.epoch() <= current.epoch().checked_add(1).ok_or_else(bad)?
        || (request.source.device, request.source.inode)
            == (request.target.device, request.target.inode)
        || DirectoryIdentity::capture(&request.source.path)? != request.source
        || DirectoryIdentity::capture(&request.target.path)? != request.target
        || request.source_version == 0
    {
        return Err(bad());
    }
    let source = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&request.source.path)?;
    let target = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&request.target.path)?;
    let metadata = target.metadata()?;
    if metadata.dev() != request.target.device || metadata.ino() != request.target.inode {
        return Err(bad());
    }
    let anchored = PathBuf::from(format!("/proc/self/fd/{}", target.as_raw_fd()));
    let lexical = lexical_binding::read(&anchored)?;
    let expected = dictionary
        .map(|dictionary| {
            LexicalIndexBinding::for_candidate(
                &anchored,
                request.embedding.clone(),
                &desired,
                LexicalPlanIdentity::verified(dictionary)?,
                &request.snapshot_sha256,
            )
        })
        .transpose()?;
    let bound = match (&lexical, dictionary) {
        (Some(binding), Some(dictionary)) if request.runtime.describes_bound_source(binding) => {
            let plan = LexicalPlanIdentity::verified(dictionary)?;
            if binding
                .validate(&anchored, &request.embedding, &old, &plan)
                .is_ok()
            {
                &old
            } else {
                binding.validate(&anchored, &request.embedding, &desired, &plan)?;
                &desired
            }
        }
        (None, None) => &desired,
        _ => return Err(bad()),
    };
    let snapshot = inspect_runtime_source(
        &request.target.path,
        &request.embedding,
        bound,
        request.limits,
    )?;
    if snapshot.rows != request.rows
        || snapshot.source_version != request.source_version
        || snapshot.snapshot_sha256 != request.snapshot_sha256
    {
        return Err(bad());
    }
    if lexical != expected {
        let expected = expected.as_ref().ok_or_else(bad)?;
        // A fresh-generation binding must already be exactly the one this
        // selected implementation derives; never normalize an unknown artifact.
        if bound == &desired {
            return Err(bad());
        }
        let mut preflight = request.limits;
        preflight.files = preflight.files.checked_sub(1).ok_or_else(bad)?;
        inspect_runtime_source(&request.target.path, &request.embedding, &old, preflight)?;
        let stage = tempfile::Builder::new()
            .prefix(".nk-runtime-refence-")
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(&anchored)?;
        let (mut file, stage) = stage.keep().map_err(|e| e.error)?;
        file.write_all(&expected.canonical_bytes()?)?;
        file.sync_all()?;
        if lexical_binding::read(&anchored)? != lexical {
            return Err(bad());
        }
        fs::rename(&stage, anchored.join(lexical_binding::FILE))?;
        target.sync_all()?;
    }
    let actual = inspect_runtime_source(
        &request.target.path,
        &request.embedding,
        &desired,
        request.limits,
    )?;
    if actual.lexical != expected
        || actual.rows != request.rows
        || actual.source_version != request.source_version
        || actual.snapshot_sha256 != request.snapshot_sha256
        || DirectoryIdentity::capture(&request.source.path)? != request.source
        || DirectoryIdentity::capture(&request.target.path)? != request.target
        || source.metadata()?.dev() != request.source.device
        || source.metadata()?.ino() != request.source.inode
    {
        return Err(bad());
    }
    canonical(
        &crate::rollback_runtime::source::SourceReport {
            version: 1,
            descriptor: request.runtime,
            source: actual,
        },
        MAX_BYTES,
    )
    .map_err(|_| bad())
}
