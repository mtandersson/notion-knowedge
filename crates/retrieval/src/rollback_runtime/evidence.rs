//! Retained metadata is descriptive. Publication and comparison require fresh
//! executable/source verification under the original offline ownership.
use super::{
    Result, RuntimeError,
    source::{SourceReport, canonical},
    verification::VerifiedPriorRuntime,
};
use crate::commit::OwnedOfflineInspection;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::Path,
};
const MAX_BYTES: usize = 65536;
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}

/// No constructor or deserializer for VerifiedPriorRuntime is provided by this
/// metadata. Only comparison with a freshly verified owned proof permits reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedEvidence {
    version: u32,
    authority_sha256: String,
    operational_sha256: String,
    operational_bytes: u64,
    runtime_sha256: String,
    asset_sha256: Option<String>,
    observation: SourceReport,
}
impl RetainedEvidence {
    fn from_proof(proof: &VerifiedPriorRuntime<'_>) -> Self {
        Self {
            version: 1,
            authority_sha256: proof.authority_sha256().into(),
            operational_sha256: proof.operational_snapshot().sha256.clone(),
            operational_bytes: proof.operational_snapshot().bytes,
            runtime_sha256: proof.runtime_sha256().into(),
            asset_sha256: proof.asset_sha256().map(Into::into),
            observation: SourceReport {
                version: 1,
                descriptor: proof.descriptor().clone(),
                source: proof.source().clone(),
            },
        }
    }
    fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(err)?;
        if value.version != 1 || canonical(&value, MAX_BYTES)? != bytes {
            return Err(RuntimeError);
        }
        SourceReport::parse_metadata(&canonical(&value.observation, MAX_BYTES)?)?;
        if let Some(binding) = &value.observation.source.lexical {
            binding.validate_metadata().map_err(err)?;
        }
        for hash in [
            &value.authority_sha256,
            &value.operational_sha256,
            &value.runtime_sha256,
            &value.observation.source.authority_sha256,
            &value.observation.source.snapshot_sha256,
        ]
        .into_iter()
        .chain(value.asset_sha256.iter())
        {
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(RuntimeError);
            }
        }
        Ok(value)
    }
    /// Read bounded private canonical metadata, without granting authority.
    pub fn read(path: &Path) -> Result<Self> {
        let parent = parent(path)?;
        let (value, _) = read_at(&parent, path.file_name().ok_or(RuntimeError)?)?;
        check_parent(&parent, path)?;
        Ok(value)
    }
}
fn parent(path: &Path) -> Result<File> {
    let directory = path.parent().ok_or(RuntimeError)?;
    if !path.is_absolute()
        || path.file_name().is_none()
        || std::fs::canonicalize(directory)? != directory
    {
        return Err(RuntimeError);
    }
    for p in directory.ancestors() {
        let m = std::fs::symlink_metadata(p)?;
        let uid = std::fs::metadata("/proc/self")?.uid();
        if !m.is_dir()
            || ![0, uid].contains(&m.uid())
            || (m.mode() & 0o022 != 0 && m.mode() & 0o1000 == 0)
        {
            return Err(RuntimeError);
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(directory)?;
    let m = file.metadata()?;
    if m.uid() != std::fs::metadata("/proc/self")?.uid() || m.mode() & 0o077 != 0 {
        return Err(RuntimeError);
    }
    check_parent(&file, path)?;
    Ok(file)
}
fn check_parent(file: &File, path: &Path) -> Result<()> {
    let retained = file.metadata()?;
    let current = std::fs::symlink_metadata(path.parent().ok_or(RuntimeError)?)?;
    if !current.is_dir()
        || current.dev() != retained.dev()
        || current.ino() != retained.ino()
        || current.uid() != retained.uid()
        || current.mode() & 0o077 != 0
    {
        return Err(RuntimeError);
    }
    Ok(())
}
fn name(name: &std::ffi::OsStr) -> Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(name.as_bytes()).map_err(err)
}
fn read_at(parent: &File, filename: &std::ffi::OsStr) -> Result<(RetainedEvidence, File)> {
    use std::os::fd::FromRawFd;
    let name = name(filename)?;
    // SAFETY: valid retained directory fd and terminated single filename.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(RuntimeError);
    }
    // SAFETY: newly allocated owned descriptor.
    let mut file = unsafe { File::from_raw_fd(fd) };
    let m = file.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != std::fs::metadata("/proc/self")?.uid()
        || m.mode() & 0o077 != 0
        || m.len() > MAX_BYTES as u64
    {
        return Err(RuntimeError);
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    let value = RetainedEvidence::parse(&bytes)?;
    // Reopen the explicit name to detect replacement, including unlink while
    // reading. Never infer authority from another directory entry.
    let current = std::fs::symlink_metadata(format!(
        "/proc/self/fd/{}/{}",
        parent.as_raw_fd(),
        filename.to_str().ok_or(RuntimeError)?
    ))?;
    if current.dev() != m.dev()
        || current.ino() != m.ino()
        || current.len() != m.len()
        || current.nlink() != 1
        || current.mode() != m.mode()
        || current.uid() != m.uid()
        || current.mtime() != m.mtime()
        || current.mtime_nsec() != m.mtime_nsec()
        || current.ctime() != m.ctime()
        || current.ctime_nsec() != m.ctime_nsec()
    {
        return Err(RuntimeError);
    }
    Ok((value, file))
}

/// Publish without replacing existing data. Even exact retries run the real
/// source/runtime verifier again. Diagnostic stages survive all refusal paths.
/// All source processes must remain stopped throughout this operation.
pub fn publish(
    path: &Path,
    owner: &OwnedOfflineInspection<'_>,
    proof: &VerifiedPriorRuntime<'_>,
) -> Result<RetainedEvidence> {
    let directory = parent(path)?;
    if path
        .parent()
        .ok_or(RuntimeError)?
        .starts_with(owner.index_directory())
    {
        return Err(RuntimeError);
    }
    let fresh = proof.refresh(owner)?;
    let evidence = RetainedEvidence::from_proof(&fresh);
    let filename = path.file_name().ok_or(RuntimeError)?;
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let (existing, file) = read_at(&directory, filename)?;
            if existing != evidence {
                return Err(RuntimeError);
            }
            file.sync_all()?;
            directory.sync_all()?;
            check_parent(&directory, path)?;
            owner.revalidate().map_err(err)?;
            return Ok(existing);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return Err(RuntimeError),
    }
    // Anchor the stage in the retained directory, rather than a re-resolved
    // caller path. keep() deliberately retains partial writes as diagnostics.
    let anchored = format!("/proc/self/fd/{}", directory.as_raw_fd());
    let stage = tempfile::Builder::new()
        .prefix(".runtime-evidence-")
        .permissions(std::fs::Permissions::from_mode(0o600))
        .tempfile_in(anchored)?;
    let (mut file, stage_path) = stage.keep().map_err(err)?;
    file.write_all(&canonical(&evidence, MAX_BYTES)?)?;
    file.sync_all()?;
    directory.sync_all()?;
    check_parent(&directory, path)?;
    owner.revalidate().map_err(err)?;
    #[cfg(test)]
    checkpoint("stage-synced");
    let from = name(stage_path.file_name().ok_or(RuntimeError)?)?;
    let to = name(filename)?;
    // SAFETY: both names are single components relative to the retained private
    // directory. NOREPLACE prevents any data overwrite, including a race.
    if unsafe {
        libc::renameat2(
            directory.as_raw_fd(),
            from.as_ptr(),
            directory.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    } != 0
    {
        return Err(RuntimeError);
    }
    #[cfg(test)]
    checkpoint("renamed");
    directory.sync_all()?;
    let (actual, _) = read_at(&directory, filename)?;
    if actual != evidence {
        return Err(RuntimeError);
    }
    owner.revalidate().map_err(err)?;
    check_parent(&directory, path)?;
    #[cfg(test)]
    checkpoint("published");
    Ok(evidence)
}

#[cfg(test)]
type CheckpointHook = std::cell::RefCell<Option<Box<dyn Fn(&str)>>>;
#[cfg(test)]
thread_local! {
    pub(super) static CHECKPOINT: CheckpointHook = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn checkpoint(phase: &str) {
    CHECKPOINT.with(|hook| {
        if let Some(hook) = hook.borrow().as_ref() {
            hook(phase);
        }
    });
}
