//! Private no-clobber durable Prepared publication. Every retry revalidates the
//! actual owned inputs; incomplete stages are retained as diagnostics.
use super::*;
use crate::rollback_runtime::evidence::{check_parent, parent};
use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
};
fn anchored(parent: &File, path: &Path) -> Result<PathBuf> {
    Ok(
        PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()))
            .join(path.file_name().ok_or(RuntimeError)?),
    )
}
fn read(parent: &File, path: &Path) -> Result<(PreparedRollbackMetadata, File)> {
    let anchored = anchored(parent, path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&anchored)?;
    let original = file.metadata()?;
    if !original.is_file()
        || original.nlink() != 1
        || original.mode() & 0o077 != 0
        || original.uid() != fs::metadata("/proc/self")?.uid()
        || original.len() > MAX_BYTES as u64
    {
        return Err(RuntimeError);
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    let metadata = PreparedRollbackMetadata::parse_metadata(&bytes)?;
    let current = fs::symlink_metadata(&anchored)?;
    if !current.is_file()
        || current.dev() != original.dev()
        || current.ino() != original.ino()
        || current.nlink() != 1
        || current.len() != original.len()
        || current.uid() != original.uid()
        || current.mode() != original.mode()
        || current.mtime() != original.mtime()
        || current.mtime_nsec() != original.mtime_nsec()
        || current.ctime() != original.ctime()
        || current.ctime_nsec() != original.ctime_nsec()
    {
        return Err(RuntimeError);
    }
    check_parent(parent, path)?;
    Ok((metadata, file))
}
impl PreparedRollbackMetadata {
    /// Bounded private metadata read, without constructing a verified value.
    pub fn read(path: &Path) -> Result<Self> {
        let directory = parent(path)?;
        Ok(read(&directory, path)?.0)
    }
}
impl VerifiedPreparedRollback<'_> {
    /// Publish one explicit record without replacing any existing artifact.
    /// Exact retries require fresh actual native/SQL/runtime verification.
    pub fn publish(&self, path: &Path) -> Result<()> {
        let directory = parent(path)?;
        if [
            &self.metadata.current.path,
            &self.metadata.candidate.candidate.path,
            &self.metadata.prior_archive.path,
        ]
        .iter()
        .any(|p| path.starts_with(p))
            || path == self.metadata.state_path
            || path
                == crate::commit::transition::journal_path(&self.metadata.state_path)
                    .map_err(err)?
            || path == self.metadata.backup_path
            || path == self.metadata.transfer_path
            || path == self.metadata.prior_evidence_path
            || self.metadata.lineage_paths.iter().any(|p| p == path)
        {
            return Err(RuntimeError);
        }
        self.revalidate()?;
        self.candidate.with_locked_candidate(|_| {
            self.revalidate_state()?;
            check_parent(&directory, path)?;
            let destination = anchored(&directory, path)?;
            let published = match fs::symlink_metadata(&destination) {
                Ok(_) => {
                    let (existing, file) = read(&directory, path)?;
                    if existing != self.metadata {
                        return Err(RuntimeError);
                    }
                    file
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let stage = tempfile::Builder::new()
                        .prefix(".nk-runtime-prepared-")
                        .permissions(fs::Permissions::from_mode(0o600))
                        .tempfile_in(PathBuf::from(format!(
                            "/proc/self/fd/{}",
                            directory.as_raw_fd()
                        )))?;
                    // Keep before writing: failures and killed processes must
                    // leave nonauthoritative diagnostics, never clean evidence.
                    let (mut file, stage) = stage.keep().map_err(|e| err(e.error))?;
                    file.write_all(&self.metadata.canonical_bytes()?)?;
                    file.sync_all()?;
                    #[cfg(test)]
                    checkpoint(path, "stage-synced");
                    check_parent(&directory, path)?;
                    let from = CString::new(stage.file_name().ok_or(RuntimeError)?.as_bytes())
                        .map_err(err)?;
                    let to = CString::new(path.file_name().ok_or(RuntimeError)?.as_bytes())
                        .map_err(err)?;
                    // SAFETY: retained directory fd, owned terminated single
                    // filenames and Linux no-replace rename. No path traversal.
                    let result = unsafe {
                        libc::renameat2(
                            directory.as_raw_fd(),
                            from.as_ptr(),
                            directory.as_raw_fd(),
                            to.as_ptr(),
                            libc::RENAME_NOREPLACE,
                        )
                    };
                    if result != 0 {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    #[cfg(test)]
                    checkpoint(path, "record-renamed");
                    file
                }
                Err(error) => return Err(error.into()),
            };
            published.sync_all()?;
            directory.sync_all()?;
            #[cfg(test)]
            checkpoint(path, "record-published");
            let (metadata, current) = read(&directory, path)?;
            let original = published.metadata()?;
            let current = current.metadata()?;
            if metadata != self.metadata
                || original.dev() != current.dev()
                || original.ino() != current.ino()
            {
                return Err(RuntimeError);
            }
            self.revalidate_state()?;
            check_parent(&directory, path)
        })
    }
}
#[cfg(test)]
fn checkpoint(path: &Path, phase: &str) {
    if std::env::var_os("NK_RUNTIME_PREPARED_TEST_OUTPUT").as_deref() != Some(path.as_os_str())
        || std::env::var("NK_RUNTIME_PREPARED_TEST_PHASE")
            .ok()
            .as_deref()
            != Some(phase)
    {
        return;
    }
    fs::write(
        path.parent().unwrap().join("observed-runtime-prepared"),
        phase,
    )
    .unwrap();
    loop {
        std::thread::park();
    }
}
