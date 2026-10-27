//! Durable intent precedes SQL reservation. These bytes never grant authority.
use super::*;
use crate::{
    commit::transition::manifest::SqlManifest,
    rollback_runtime::evidence::{check_parent, parent},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
};

const MAX_BYTES: usize = 131072;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationStartMetadata {
    pub version: u32,
    pub protocol: String,
    pub handoff_sha256: String,
    pub desired_authority_json: String,
    pub before: SqlManifest,
}

fn path(handoff: &ChoiceHandoffRecord) -> Result<PathBuf> {
    let active = crate::commit::transition::journal_path(&handoff.prepared.selection.state_path)
        .map_err(|_| RuntimeError)?;
    let mut name = active.file_name().ok_or(RuntimeError)?.to_os_string();
    name.push(".choice-publication-start");
    let path = active.with_file_name(name);
    if [
        &handoff.prepared_path,
        &handoff.origin_history_path,
        &handoff.prepared.transfer_path,
        &handoff.prepared.backup_path,
    ]
    .into_iter()
    .chain(handoff.prepared.source_evidence_path.iter())
    .any(|retained| retained == &path)
    {
        return Err(RuntimeError);
    }
    Ok(path)
}

impl PublicationStartMetadata {
    pub fn canonical_bytes(&self, handoff: &ChoiceHandoffRecord) -> Result<Vec<u8>> {
        if self.version != 1
            || self.protocol != "explicit-choice-publication-start-v1"
            || self.handoff_sha256 != format!("{:x}", Sha256::digest(handoff.canonical_bytes()?))
            || self.desired_authority_json != handoff.prepared.selection.desired_authority_json
            || self.before != handoff.prepared.adoption
        {
            return Err(RuntimeError);
        }
        crate::rollback_runtime::source::canonical(self, MAX_BYTES)
    }

    /// Strict descriptive read. SQL absence/commit and native state still need
    /// fresh verification under the original owner before any effect.
    pub fn read(handoff: &ChoiceHandoffRecord) -> Result<Self> {
        handoff.verify_active()?;
        handoff.verify_retained_files()?;
        let path = path(handoff)?;
        let directory = parent(&path)?;
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .join(path.file_name().ok_or(RuntimeError)?);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&anchored)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o777 != 0o600
            || metadata.uid() != fs::metadata("/proc/self")?.uid()
            || metadata.len() > MAX_BYTES as u64
        {
            return Err(RuntimeError);
        }
        let mut bytes = Vec::new();
        (&file)
            .take((MAX_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        let result: Self = serde_json::from_slice(&bytes).map_err(|_| RuntimeError)?;
        if bytes != result.canonical_bytes(handoff)? {
            return Err(RuntimeError);
        }
        let identity = |m: &fs::Metadata| {
            (
                m.dev(),
                m.ino(),
                m.len(),
                m.mtime(),
                m.mtime_nsec(),
                m.ctime(),
                m.ctime_nsec(),
                m.nlink(),
                m.mode(),
                m.uid(),
            )
        };
        if identity(&metadata) != identity(&file.metadata()?)
            || identity(&metadata) != identity(&fs::symlink_metadata(&anchored)?)
        {
            return Err(RuntimeError);
        }
        check_parent(&directory, &path)?;
        handoff.verify_active()?;
        handoff.verify_retained_files()?;
        Ok(result)
    }
}

impl VerifiedPreparedChoice<'_, '_> {
    /// Retain an immutable exact intent under the freshly verified original
    /// owner. This does not reserve SQL, change native data, or enable serving.
    pub fn retain_publication_start(
        &self,
        handoff: &ChoiceHandoffRecord,
    ) -> Result<PublicationStartMetadata> {
        self.revalidate()?;
        handoff.verify_active()?;
        handoff.verify_retained_files()?;
        if self.metadata != handoff.prepared {
            return Err(RuntimeError);
        }
        let path = path(handoff)?;
        self.selection
            .inspection
            .choice_output(&path)
            .map_err(|_| RuntimeError)?;
        let intent = PublicationStartMetadata {
            version: 1,
            protocol: "explicit-choice-publication-start-v1".into(),
            handoff_sha256: format!("{:x}", Sha256::digest(handoff.canonical_bytes()?)),
            desired_authority_json: self.metadata.selection.desired_authority_json.clone(),
            before: self.metadata.adoption.clone(),
        };
        let directory = parent(&path)?;
        let root = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let anchored = root.join(path.file_name().ok_or(RuntimeError)?);
        match fs::symlink_metadata(&anchored) {
            Ok(_) => {
                if PublicationStartMetadata::read(handoff)? != intent {
                    return Err(RuntimeError);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut stage = tempfile::Builder::new()
                    .prefix(".nk-choice-publication-start-")
                    .permissions(fs::Permissions::from_mode(0o600))
                    .tempfile_in(root)?;
                stage.write_all(&intent.canonical_bytes(handoff)?)?;
                stage.as_file().sync_all()?;
                self.revalidate()?;
                handoff.verify_active()?;
                handoff.verify_retained_files()?;
                check_parent(&directory, &path)?;
                let from = std::ffi::CString::new(
                    stage.path().file_name().ok_or(RuntimeError)?.as_bytes(),
                )
                .map_err(|_| RuntimeError)?;
                let to = std::ffi::CString::new(path.file_name().ok_or(RuntimeError)?.as_bytes())
                    .map_err(|_| RuntimeError)?;
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
            }
            Err(error) => return Err(error.into()),
        }
        check_parent(&directory, &path)?;
        directory.sync_all()?;
        self.revalidate()?;
        if PublicationStartMetadata::read(handoff)? != intent {
            return Err(RuntimeError);
        }
        Ok(intent)
    }
}
