//! Reuse retained original ownership for backup and SQL preview, never nesting
//! the coordinator's index lock or SQL mutex.
use super::*;
use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
impl OwnedOfflineInspection<'_> {
    pub(crate) fn owned_connection(&self) -> Result<&Connection, CommitError> {
        self.revalidate()?;
        Ok(self.connection)
    }
    pub(crate) fn owned_transaction(&self) -> Result<&rusqlite::Transaction<'_>, CommitError> {
        self.revalidate()?;
        Ok(self.connection)
    }
    pub(crate) fn state_path(&self) -> &Path {
        &self.coordinator.state_path
    }
    /// Fresh immutable diagnostic backup; never opens the original SQL writable
    /// or restores it. The original exclusive transaction remains retained.
    pub(crate) fn backup_current(
        &self,
        path: &Path,
        max_bytes: u64,
        create: bool,
    ) -> Result<OfflineBackup, CommitError> {
        let expected = self.state_snapshot(max_bytes)?;
        private_parent(path)?;
        let parent_path = path.parent().ok_or(CommitError::InvalidInput)?;
        if parent_path.starts_with(self.index_directory())
            || path == self.state_path()
            || fs::canonicalize(parent_path).map_err(io)? != parent_path
        {
            return Err(CommitError::InvalidInput);
        }
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(parent_path)
            .map_err(io)?;
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()))
            .join(path.file_name().ok_or(CommitError::InvalidInput)?);
        match fs::symlink_metadata(path) {
            Ok(_) => {
                expected.verify(path)?;
                self.revalidate()?;
                return Ok(expected);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => (),
            Err(error) => return Err(io(error)),
        }
        let mut output = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&anchored)
            .map_err(io)?;
        let mut original = self.state.try_clone().map_err(io)?;
        original.rewind().map_err(io)?;
        let copied = std::io::copy(
            &mut (&mut original).take(max_bytes.saturating_add(1)),
            &mut output,
        )
        .map_err(io)?;
        if copied != expected.bytes
            || hash_file_bounded(&mut output, max_bytes)? != expected
            || self.state_snapshot(max_bytes)? != expected
        {
            return Err(CommitError::BindingMismatch);
        }
        output.sync_all().map_err(io)?;
        parent.sync_all().map_err(io)?;
        let retained = parent.metadata().map_err(io)?;
        let current = fs::symlink_metadata(parent_path).map_err(io)?;
        if !current.is_dir()
            || retained.dev() != current.dev()
            || retained.ino() != current.ino()
            || current.mode() & 0o077 != 0
            || current.uid() != retained.uid()
            || Identity::read(path)? != Identity::file(&output)?
        {
            return Err(CommitError::BindingMismatch);
        }
        expected.verify(path)?;
        self.revalidate()?;
        Ok(expected)
    }
}
