//! Source compatibility exists only while original offline ownership is held.
use super::{PinnedRuntime, Result, RuntimeError, probe, source};
use crate::{
    chunks::{
        CandidateLimits, RuntimeSourceSnapshot, inspect_runtime_source,
        lexical_binding::runtime::RuntimeDescriptor,
    },
    commit::{OfflineBackup, OwnedOfflineInspection},
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}

/// An explicitly selected opaque asset. The host never executes a bundle using
/// a different importer; the selected runtime must load it under its own code.
pub struct PinnedAsset {
    path: PathBuf,
    file: File,
    sha256: String,
    device: u64,
    inode: u64,
    bytes: u64,
    uid: u32,
}
impl PinnedAsset {
    pub fn pin(path: &Path, expected_sha256: &str) -> Result<Self> {
        if !path.is_absolute()
            || expected_sha256.len() != 64
            || !expected_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RuntimeError);
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let m = file.metadata()?;
        let uid = std::fs::metadata("/proc/self")?.uid();
        if !m.is_file()
            || m.nlink() != 1
            || m.mode() & 0o022 != 0
            || ![0, uid].contains(&m.uid())
            || m.len() > 128 * 1024 * 1024
        {
            return Err(RuntimeError);
        }
        let asset = Self {
            path: path.into(),
            file,
            sha256: expected_sha256.into(),
            device: m.dev(),
            inode: m.ino(),
            bytes: m.len(),
            uid: m.uid(),
        };
        asset.revalidate()?;
        Ok(asset)
    }
    fn revalidate(&self) -> Result<()> {
        let mut hash = Sha256::new();
        let mut count = 0u64;
        let mut buffer = [0; 65536];
        loop {
            let n = self.file.read_at(&mut buffer, count)?;
            if n == 0 {
                break;
            }
            count = count.checked_add(n as u64).ok_or(RuntimeError)?;
            if count > self.bytes {
                return Err(RuntimeError);
            }
            hash.update(&buffer[..n]);
        }
        let m = std::fs::symlink_metadata(&self.path)?;
        if !m.is_file()
            || m.nlink() != 1
            || m.mode() & 0o022 != 0
            || m.uid() != self.uid
            || m.dev() != self.device
            || m.ino() != self.inode
            || m.len() != self.bytes
            || count != self.bytes
            || format!("{:x}", hash.finalize()) != self.sha256
        {
            return Err(RuntimeError);
        }
        Ok(())
    }
    fn mode(&self) -> Vec<OsString> {
        vec![
            "expansion".into(),
            self.path.clone().into_os_string(),
            self.sha256.clone().into(),
        ]
    }
}
#[derive(Debug, Clone, Copy)]
pub struct VerificationLimits {
    pub native: CandidateLimits,
    pub sql_bytes: u64,
    pub process_budget: Duration,
}
impl Default for VerificationLimits {
    fn default() -> Self {
        Self {
            native: CandidateLimits::default(),
            sql_bytes: 64 * 1024 * 1024,
            process_budget: Duration::from_secs(300),
        }
    }
}

/// Native and executable verification tied to retained source ownership. This
/// type has no deserializer or public constructor. Its metadata may be retained
/// descriptively, but cannot manufacture fresh authority after the owner ends.
///
/// ```compile_fail
/// use notion_knowledge_retrieval::{commit::{CommitError, IndexCommitCoordinator},
///     rollback_runtime::{PinnedRuntime, verification::{self, VerifiedPriorRuntime, VerificationLimits}}};
/// use notion_knowledge_core::embedding::EmbeddingMetadata;
/// fn escape<'a>(c: &'a IndexCommitCoordinator, runtime: &'a PinnedRuntime,
///     embedding: &EmbeddingMetadata) -> VerifiedPriorRuntime<'a> {
///     c.with_owned_offline_inspection(10, |owner| {
///         verification::verify(owner, runtime, None, embedding, VerificationLimits::default())
///             .map_err(|_| CommitError::BindingMismatch)
///     }).unwrap()
/// }
/// ```
pub struct VerifiedPriorRuntime<'owner> {
    runtime: &'owner PinnedRuntime,
    asset: Option<&'owner PinnedAsset>,
    descriptor: RuntimeDescriptor,
    source: RuntimeSourceSnapshot,
    authority_sha256: String,
    operational: OfflineBackup,
    _ownership: PhantomData<&'owner ()>,
}
impl VerifiedPriorRuntime<'_> {
    pub fn runtime_sha256(&self) -> &str {
        self.runtime.sha256()
    }
    pub fn asset_sha256(&self) -> Option<&str> {
        self.asset.map(|a| a.sha256.as_str())
    }
    pub fn descriptor(&self) -> &RuntimeDescriptor {
        &self.descriptor
    }
    pub fn source(&self) -> &RuntimeSourceSnapshot {
        &self.source
    }
    pub fn authority_sha256(&self) -> &str {
        &self.authority_sha256
    }
    pub fn operational_snapshot(&self) -> &OfflineBackup {
        &self.operational
    }
}

/// Verify the actual source and real fencing/reopen probes with one immutable
/// trusted executable. The returned value borrows the owning context; it cannot
/// escape with_owned_offline_inspection or authorize cross-restart reuse.
/// All source processes must already be stopped. This acquires no SQL/index
/// lock itself and sends neither original SQL paths nor tokens to subprocesses.
pub fn verify<'owner>(
    owner: &'owner OwnedOfflineInspection<'_>,
    runtime: &'owner PinnedRuntime,
    asset: Option<&'owner PinnedAsset>,
    embedding: &EmbeddingMetadata,
    limits: VerificationLimits,
) -> Result<VerifiedPriorRuntime<'owner>> {
    if limits.process_budget.is_zero()
        || limits.process_budget > Duration::from_secs(300)
        || limits.sql_bytes == 0
    {
        return Err(RuntimeError);
    }
    let started = Instant::now();
    let remaining = || {
        limits
            .process_budget
            .checked_sub(started.elapsed())
            .filter(|d| !d.is_zero())
            .ok_or(RuntimeError)
    };
    owner.revalidate().map_err(err)?;
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    let authority_sha256 = owner.authority_sha256().map_err(err)?;
    let operational = owner.state_snapshot(limits.sql_bytes).map_err(err)?;
    let source = inspect_runtime_source(
        owner.index_directory(),
        embedding,
        owner.binding(),
        limits.native,
    )
    .map_err(err)?;
    if source.lexical.is_none() != asset.is_none() {
        return Err(RuntimeError);
    }
    let scratch = tempfile::Builder::new()
        .prefix("nk-prior-runtime-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let request_path = scratch.path().join("source.json");
    let mut request = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&request_path)?;
    request.write_all(&source::request_bytes(owner, embedding, limits.native)?)?;
    drop(request);
    let mode = asset
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    let run = |action: &str, path: &Path| {
        let args = [
            vec![action.into(), path.as_os_str().to_owned()],
            mode.clone(),
        ]
        .concat();
        runtime.run(&args, remaining()?, 65536)
    };
    let report = source::SourceReport::parse_metadata(&run("inspect-source", &request_path)?)?;
    if report.source != source
        || report.descriptor.is_legacy() != asset.is_none()
        || source
            .lexical
            .as_ref()
            .is_some_and(|b| !report.descriptor.describes_bound_source(b))
    {
        return Err(RuntimeError);
    }
    let root = scratch.path().join("probe");
    let created = probe::verify_report(
        &run("probe-create", &root)?,
        &report.descriptor,
        &root,
        limits.native,
    )?;
    let reopened = probe::verify_report(
        &run("probe-reopen", &root)?,
        &report.descriptor,
        &root,
        limits.native,
    )?;
    if created != reopened {
        return Err(RuntimeError);
    }
    #[cfg(test)]
    AFTER_PROBES.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    owner.revalidate().map_err(err)?;
    if owner.authority_sha256().map_err(err)? != authority_sha256
        || owner.state_snapshot(limits.sql_bytes).map_err(err)? != operational
        || inspect_runtime_source(
            owner.index_directory(),
            embedding,
            owner.binding(),
            limits.native,
        )
        .map_err(err)?
            != source
    {
        return Err(RuntimeError);
    }
    // The final native scan can be long. Recheck retained artifacts and SQL
    // identity afterward, immediately before making the scoped proof usable.
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    owner.revalidate().map_err(err)?;
    remaining()?;
    Ok(VerifiedPriorRuntime {
        runtime,
        asset,
        descriptor: report.descriptor,
        source,
        authority_sha256,
        operational,
        _ownership: PhantomData,
    })
}

#[cfg(test)]
thread_local! {
    static AFTER_PROBES: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests;
