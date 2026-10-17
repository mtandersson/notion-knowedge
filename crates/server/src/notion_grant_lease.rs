//! Single local process ownership for the sealed grant. The persistent empty
//! sidecar is never removed: unlinking a locked inode could create two owners.
use crate::notion_grant_store::StoreError;
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(crate) struct GrantLease {
    file: File,
    path: PathBuf,
}
impl GrantLease {
    pub fn path(state: &Path) -> PathBuf {
        let mut path = state.as_os_str().to_owned();
        path.push(".lock");
        PathBuf::from(path)
    }
    pub fn acquire(state: &Path) -> Result<Self, StoreError> {
        let path = Self::path(state);
        let uid = rustix::process::geteuid().as_raw();
        for parent in path.parent().ok_or(StoreError::Configuration)?.ancestors() {
            let meta = fs::symlink_metadata(parent).map_err(|_| StoreError::Configuration)?;
            if !meta.is_dir()
                || (meta.uid() != uid && meta.uid() != 0)
                || (meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0)
            {
                return Err(StoreError::Configuration);
            }
        }
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => {
                file.sync_all().map_err(|_| StoreError::Unavailable)?;
                File::open(path.parent().ok_or(StoreError::Configuration)?)
                    .and_then(|dir| dir.sync_all())
                    .map_err(|_| StoreError::Unavailable)?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata =
                    fs::symlink_metadata(&path).map_err(|_| StoreError::Configuration)?;
                validate(&metadata, uid)?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|_| StoreError::Unavailable)?
            }
            Err(_) => return Err(StoreError::Unavailable),
        };
        let lease = Self { file, path };
        lease.validate()?;
        lease.file.try_lock().map_err(|_| StoreError::InUse)?;
        lease.validate()?;
        Ok(lease)
    }
    pub fn validate(&self) -> Result<(), StoreError> {
        let open = self.file.metadata().map_err(|_| StoreError::Unavailable)?;
        let path = fs::symlink_metadata(&self.path).map_err(|_| StoreError::Unavailable)?;
        let uid = rustix::process::geteuid().as_raw();
        validate(&open, uid)?;
        validate(&path, uid)?;
        if open.dev() != path.dev() || open.ino() != path.ino() {
            return Err(StoreError::Unavailable);
        }
        Ok(())
    }
}
fn validate(metadata: &fs::Metadata, uid: u32) -> Result<(), StoreError> {
    if !metadata.file_type().is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
        || metadata.len() != 0
    {
        return Err(StoreError::Configuration);
    }
    Ok(())
}
