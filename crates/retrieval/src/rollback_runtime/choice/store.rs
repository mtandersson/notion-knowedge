//! Immutable Prepared-choice records; metadata parsing never creates a proof.
use super::prepared::MAX;
use super::*;
use crate::rollback_runtime::evidence::{check_parent, parent};
use std::{
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
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
fn anchored(directory: &File, path: &Path) -> Result<PathBuf> {
    Ok(
        PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .join(path.file_name().ok_or(RuntimeError)?),
    )
}
fn read(directory: &File, path: &Path) -> Result<PreparedChoiceMetadata> {
    let at = anchored(directory, path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&at)?;
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.uid() != fs::metadata("/proc/self")?.uid()
        || before.mode() & 0o077 != 0
        || before.len() > MAX as u64
    {
        return Err(RuntimeError);
    }
    let mut bytes = Vec::new();
    (&mut file).take(MAX as u64 + 1).read_to_end(&mut bytes)?;
    let value = PreparedChoiceMetadata::parse_metadata(&bytes)?;
    let after = fs::symlink_metadata(&at)?;
    if !after.is_file()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.nlink() != 1
        || after.len() != before.len()
        || after.mode() != before.mode()
        || after.uid() != before.uid()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
    {
        return Err(RuntimeError);
    }
    check_parent(directory, path)?;
    Ok(value)
}
impl PreparedChoiceMetadata {
    pub fn read(path: &Path) -> Result<Self> {
        let directory = parent(path)?;
        read(&directory, path)
    }
}
#[cfg(test)]
fn checkpoint(metadata: &PreparedChoiceMetadata, phase: &str) {
    let Some(root) = std::env::var_os("NK_CHOICE_TEST_ROOT").map(PathBuf::from) else {
        return;
    };
    if metadata.selection.state_path.parent() != Some(root.as_path())
        || std::env::var("NK_CHOICE_TEST_PHASE").ok().as_deref() != Some(phase)
    {
        return;
    }
    fs::write(root.join("observed-choice-prepared"), phase).unwrap();
    loop {
        std::thread::park();
    }
}
impl VerifiedPreparedChoice<'_, '_> {
    pub fn publish(&self, path: &Path) -> Result<()> {
        self.revalidate()?;
        self.selection.inspection.choice_output(path).map_err(err)?;
        if path.starts_with(&self.metadata.candidate.path)
            || path == self.metadata.transfer_path
            || path == self.metadata.backup_path
        {
            return Err(RuntimeError);
        }
        let directory = parent(path)?;
        match fs::symlink_metadata(anchored(&directory, path)?) {
            Ok(_) => {
                if read(&directory, path)? != self.metadata {
                    return Err(RuntimeError);
                }
                self.revalidate()?;
                // A predecessor may have died after rename but before the
                // parent durability barrier. Exact-name retry must finish it.
                check_parent(&directory, path)?;
                directory.sync_all()?;
                #[cfg(test)]
                checkpoint(&self.metadata, "record-published");
                if read(&directory, path)? != self.metadata {
                    return Err(RuntimeError);
                }
                self.revalidate()?;
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let stage = tempfile::Builder::new()
            .prefix(".nk-choice-prepared-")
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(PathBuf::from(format!(
                "/proc/self/fd/{}",
                directory.as_raw_fd()
            )))?;
        let (mut file, stage) = stage.keep().map_err(|e| e.error)?;
        file.write_all(&self.metadata.canonical_bytes()?)?;
        file.sync_all()?;
        #[cfg(test)]
        checkpoint(&self.metadata, "stage-synced");
        self.revalidate()?;
        check_parent(&directory, path)?;
        let from = std::ffi::CString::new(stage.file_name().ok_or(RuntimeError)?.as_bytes())
            .map_err(err)?;
        let to = std::ffi::CString::new(path.file_name().ok_or(RuntimeError)?.as_bytes())
            .map_err(err)?;
        // SAFETY: one retained private parent fd and terminated single names.
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
            return Err(std::io::Error::last_os_error().into());
        }
        #[cfg(test)]
        checkpoint(&self.metadata, "record-renamed");
        directory.sync_all()?;
        #[cfg(test)]
        checkpoint(&self.metadata, "record-published");
        if read(&directory, path)? != self.metadata {
            return Err(RuntimeError);
        }
        self.revalidate()
    }
}
/// Exact retry requires the same trusted choice/runtime/configuration and a
/// freshly verified current owner. Missing artifacts are never recreated.
pub fn reopen<'a, 'owner>(
    inspection: &'a QuarantinedSourceInspection<'owner>,
    inputs: SelectionInputs<'a>,
    path: &Path,
    limits: PreparedLimits,
) -> Result<VerifiedPreparedChoice<'a, 'owner>> {
    inspection.choice_output(path).map_err(err)?;
    let directory = parent(path)?;
    let metadata = read(&directory, path)?;
    let selection = select(inspection, inputs, limits)?;
    if selection.metadata() != &metadata.selection {
        return Err(RuntimeError);
    }
    let guard = if metadata.built.is_some() {
        if crate::chunks::runtime_builder::DirectoryIdentity::capture(&metadata.candidate.path)
            .map_err(err)?
            != metadata.candidate
        {
            return Err(RuntimeError);
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&metadata.candidate.path)?;
        file.try_lock().map_err(err)?;
        Some(file)
    } else {
        None
    };
    let proof = VerifiedPreparedChoice {
        selection,
        metadata,
        guard,
    };
    proof.revalidate()?;
    if read(&directory, path)? != *proof.metadata() {
        return Err(RuntimeError);
    }
    Ok(proof)
}
