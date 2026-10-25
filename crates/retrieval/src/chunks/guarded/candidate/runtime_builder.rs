//! Selected-runtime construction from canonical rows; never operational SQL.
use super::transfer::{self, TransferLimits, TransferRecord};
use super::*;
use crate::{
    chunks::lexical_binding::runtime::RuntimeDescriptor,
    rollback_runtime::{
        evidence::{check_parent, parent},
        source::canonical,
    },
};
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::Read, os::unix::fs::DirBuilderExt};
const MAX_METADATA: usize = 131072;
const PROTOCOL: &str = "canonical-runtime-candidate-v1;policy-independent-physical-fencing";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryIdentity {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
}
impl DirectoryIdentity {
    pub fn capture(path: &Path) -> Result<Self, ChunkTableError> {
        if !path.is_absolute() || fs::canonicalize(path)? != path {
            return Err(bad());
        }
        let m = fs::symlink_metadata(path)?;
        if !m.is_dir() || m.uid() != fs::metadata("/proc/self")?.uid() || m.mode() & 0o077 != 0 {
            return Err(bad());
        }
        Ok(Self {
            path: path.into(),
            device: m.dev(),
            inode: m.ino(),
        })
    }
    fn verify(&self) -> Result<(), ChunkTableError> {
        if Self::capture(&self.path)? != *self {
            return Err(bad());
        }
        Ok(())
    }
}
/// Bounded descriptive inputs. The integrated owner must establish original
/// provenance and freshness; supplying this document never creates authority.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRequest {
    pub version: u32,
    pub source: DirectoryIdentity,
    pub transfer_path: PathBuf,
    pub transfer: TransferRecord,
    pub target_authority_json: String,
    pub candidate: PathBuf,
    pub runtime: RuntimeDescriptor,
    pub limits: TransferLimits,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateReport {
    pub version: u32,
    pub protocol: String,
    pub runtime: RuntimeDescriptor,
    pub candidate: DirectoryIdentity,
    pub transfer_sha256: String,
    pub target_authority_json: String,
    pub snapshot: RuntimeSourceSnapshot,
}
fn json(value: &impl Serialize) -> Result<Vec<u8>, ChunkTableError> {
    canonical(value, MAX_METADATA).map_err(|_| bad())
}
impl CandidateReport {
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self, ChunkTableError> {
        if bytes.len() > MAX_METADATA {
            return Err(bad());
        }
        let report: Self = serde_json::from_slice(bytes)?;
        if report.version != 1
            || report.protocol != PROTOCOL
            || report.snapshot.version != 2
            || json(&report)? != bytes
        {
            return Err(bad());
        }
        RuntimeDescriptor::parse_metadata(&report.runtime.canonical_bytes()?)?;
        Ok(report)
    }
}
impl CandidateRequest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChunkTableError> {
        json(self)
    }
    fn read(path: &Path) -> Result<Self, ChunkTableError> {
        let directory = parent(path).map_err(|_| bad())?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let m = file.metadata()?;
        if !m.is_file()
            || m.nlink() != 1
            || m.mode() & 0o077 != 0
            || m.uid() != fs::metadata("/proc/self")?.uid()
            || m.len() > MAX_METADATA as u64
        {
            return Err(bad());
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_METADATA as u64 + 1)
            .read_to_end(&mut bytes)?;
        let request: Self = serde_json::from_slice(&bytes)?;
        if json(&request)? != bytes {
            return Err(bad());
        }
        let current = fs::symlink_metadata(path)?;
        if current.dev() != m.dev()
            || current.ino() != m.ino()
            || current.len() != m.len()
            || current.nlink() != 1
            || current.uid() != m.uid()
            || current.mode() != m.mode()
            || current.ctime() != m.ctime()
            || current.ctime_nsec() != m.ctime_nsec()
        {
            return Err(bad());
        }
        check_parent(&directory, path).map_err(|_| bad())?;
        Ok(request)
    }
    fn validate(
        &self,
        dictionary: Option<&crate::lexical_dictionary::Dictionary>,
    ) -> Result<crate::commit::CommitBinding, ChunkTableError> {
        if self.version != 1 || RuntimeDescriptor::current(dictionary)? != self.runtime {
            return Err(bad());
        }
        self.source.verify()?;
        let current = crate::commit::CommitBinding::parse_retained(
            self.transfer.metadata.authority_json.as_bytes(),
        )
        .map_err(|_| bad())?;
        let target =
            crate::commit::CommitBinding::parse_retained(self.target_authority_json.as_bytes())
                .map_err(|_| bad())?;
        if target.table() != current.table()
            || target.workspace() != current.workspace()
            || target.roots() != current.roots()
            || target.scope() != current.scope()
            || target.generation() == current.generation()
            || target.epoch() <= current.epoch()
            || self.candidate.parent() != self.source.path.parent()
            || self.candidate.starts_with(&self.source.path)
        {
            return Err(bad());
        }
        parent(&self.candidate).map_err(|_| bad())?;
        // Full validation before any candidate writes, without reading the
        // current source's native backend or reopening original operational SQL.
        transfer::read(&self.transfer_path, &self.transfer, self.limits, |_| Ok(()))?;
        Ok(target)
    }
}
/// Internal protocol entrypoint for the exact selected trusted executable.
/// Failed directories remain diagnostics, with no existing READY/transition
/// marker or operational anchor. Every report is descriptive only.
pub fn execute(
    path: &Path,
    dictionary: Option<&crate::lexical_dictionary::Dictionary>,
    create: bool,
) -> Result<Vec<u8>, ChunkTableError> {
    let request = CandidateRequest::read(path)?;
    let target = request.validate(dictionary)?;
    let parent = parent(&request.candidate).map_err(|_| bad())?;
    let origin = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&request.source.path)?;
    if create {
        // No replacing an existing candidate, source, SQL, asset or artifact.
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&request.candidate)?;
    }
    let identity = DirectoryIdentity::capture(&request.candidate)?;
    let guard = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&request.candidate)?;
    let initial = guard.metadata()?;
    if initial.dev() != identity.device || initial.ino() != identity.inode {
        return Err(bad());
    }
    guard.try_lock().map_err(|_| bad())?;
    if create {
        let reader =
            transfer::Reader::open(&request.transfer_path, &request.transfer, request.limits)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            use std::os::fd::AsRawFd;
            let anchored = PathBuf::from(format!("/proc/self/fd/{}", guard.as_raw_fd()));
            let db = lancedb::connect(&local_database_uri(&anchored)?)
                .execute()
                .await?;
            let table = db
                .create_table(
                    target.table(),
                    Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>,
                )
                .execute()
                .await?;
            validate_table_schema(&table, &request.transfer.metadata.embedding).await?;
            let table = LanceChunkTable {
                table,
                embedding: request.transfer.metadata.embedding.clone(),
            };
            let config = if dictionary.is_some() {
                FtsIndexConfig::lexical_expansion_v1()
            } else {
                FtsIndexConfig::default()
            };
            table.rebuild_fts_index(&config).await?;
            verify_profile_config(&table, &config).await?;
            Ok::<_, ChunkTableError>(())
        })?;
        if DirectoryIdentity::capture(&request.candidate)? != identity {
            return Err(bad());
        }
        if let Some(dictionary) = dictionary {
            let binding = LexicalIndexBinding::for_candidate(
                &request.candidate,
                request.transfer.metadata.embedding.clone(),
                &target,
                LexicalPlanIdentity::verified(dictionary)?,
                &request.transfer.metadata.source.snapshot_sha256,
            )?;
            write_new(
                &request.candidate.join(lexical_binding::FILE),
                &binding.canonical_bytes()?,
            )?;
        }
        sync_tree(&request.candidate, request.limits.native, &mut 0, &mut 0, 0)?;
        parent.sync_all()?;
    }
    if request.candidate.join(READY).exists()
        || request.candidate.join(".nk-operational-binding").exists()
    {
        return Err(bad());
    }
    let lexical = lexical_binding::read(&request.candidate)?;
    match (&lexical, dictionary) {
        (Some(binding), Some(dictionary)) if request.runtime.describes_bound_source(binding) => {
            binding.validate(
                &request.candidate,
                &request.transfer.metadata.embedding,
                &target,
                &LexicalPlanIdentity::verified(dictionary)?,
            )?
        }
        (None, None) => (),
        _ => return Err(bad()),
    }
    let snapshot = inspect_runtime_source(
        &request.candidate,
        &request.transfer.metadata.embedding,
        &target,
        request.limits.native,
    )?;
    if snapshot.rows != request.transfer.metadata.source.rows
        || snapshot.snapshot_sha256 != request.transfer.metadata.source.snapshot_sha256
    {
        return Err(bad());
    }
    request.source.verify()?;
    let m = origin.metadata()?;
    if m.dev() != request.source.device
        || m.ino() != request.source.inode
        || DirectoryIdentity::capture(&request.candidate)? != identity
    {
        return Err(bad());
    }
    check_parent(&parent, &request.candidate).map_err(|_| bad())?;
    // Re-read the interchange after the native writer/probes; output is not
    // accepted if its input was changed/replaced while derived files were made.
    transfer::read(
        &request.transfer_path,
        &request.transfer,
        request.limits,
        |_| Ok(()),
    )?;
    json(&CandidateReport {
        version: 1,
        protocol: PROTOCOL.into(),
        runtime: request.runtime,
        candidate: identity,
        transfer_sha256: request.transfer.sha256,
        target_authority_json: request.target_authority_json,
        snapshot,
    })
}
