//! Owned latest-row preparation with separately verified prior-pair provenance.
use super::{
    PinnedRuntime, Result, RuntimeError, builder,
    evidence::RetainedEvidence,
    source::{self, SourceReport},
    verification::PinnedAsset,
};
use crate::{
    chunks::{
        canonical_transfer::{self, TransferLimits, TransferRecord},
        inspect_runtime_source,
        runtime_builder::{CandidateReport, CandidateRequest, DirectoryIdentity},
    },
    commit::{CommitBinding, OwnedOfflineInspection},
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}

/// Explicit local retained inputs, never discovered by scanning directories.
pub struct PriorInputs<'a> {
    pub runtime: &'a PinnedRuntime,
    pub asset: Option<&'a PinnedAsset>,
    pub evidence: &'a Path,
    /// Newest completed transition first, ending at the selected prior source.
    pub lineage: &'a [PathBuf],
    pub archive: &'a Path,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateKind {
    ExactArchive,
    Rebuilt,
}
/// Descriptive retained data only; deserializing it never produces a proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvidence {
    pub version: u32,
    pub kind: CandidateKind,
    pub current: TransferRecord,
    pub prior: SourceReport,
    pub lineage_sha256: String,
    pub runtime_sha256: String,
    pub asset_sha256: Option<String>,
    pub candidate: DirectoryIdentity,
    pub target_authority_json: String,
    pub built: Option<CandidateReport>,
}
/// No public constructor or deserializer. This value borrows retained original
/// SQL/index ownership. Publication must reverify under its actual transaction
/// and reserve fresh fences; an exact archive still has its OLD physical binding.
pub struct VerifiedPriorCandidate<'owner> {
    owner: &'owner OwnedOfflineInspection<'owner>,
    runtime: &'owner PinnedRuntime,
    asset: Option<&'owner PinnedAsset>,
    evidence_path: PathBuf,
    retained: RetainedEvidence,
    lineage: Vec<PathBuf>,
    archive: DirectoryIdentity,
    transfer_path: PathBuf,
    embedding: EmbeddingMetadata,
    target: CommitBinding,
    limits: TransferLimits,
    budget: Duration,
    observation: CandidateEvidence,
}
impl VerifiedPriorCandidate<'_> {
    pub fn metadata(&self) -> &CandidateEvidence {
        &self.observation
    }
    pub fn target(&self) -> &CommitBinding {
        &self.target
    }
    /// Recheck actual original SQL/source, lineage, selected artifacts, archive
    /// and candidate. This is not authority to publish after ownership ends.
    pub fn revalidate(&self) -> Result<()> {
        let started = Instant::now();
        let remaining = || {
            self.budget
                .checked_sub(started.elapsed())
                .filter(|d| !d.is_zero())
                .ok_or(RuntimeError)
        };
        self.current_unchanged()?;
        let provenance = self.owner.prior_archive(&self.lineage).map_err(err)?;
        if provenance.lineage_sha256 != self.observation.lineage_sha256
            || provenance.ownership_sha256 != self.retained.prior_authority_sha256()
            || RetainedEvidence::read(&self.evidence_path)? != self.retained
            || DirectoryIdentity::capture(&provenance.path).map_err(err)? != self.archive
        {
            return Err(RuntimeError);
        }
        let _guard = archive_guard(&self.archive)?;
        let prior = observe(
            self.runtime,
            self.asset,
            &self.archive.path,
            &provenance.binding,
            &self.embedding,
            self.limits,
            remaining()?,
        )?;
        if prior != self.observation.prior {
            return Err(RuntimeError);
        }
        if let Some(report) = &self.observation.built {
            let request = self.request(report.candidate.path.clone())?;
            let fresh = reopen(self.runtime, self.asset, &request, remaining()?)?;
            if &fresh != report {
                return Err(RuntimeError);
            }
        } else if self.observation.candidate != self.archive {
            return Err(RuntimeError);
        }
        if DirectoryIdentity::capture(&self.observation.candidate.path).map_err(err)?
            != self.observation.candidate
            || DirectoryIdentity::capture(&self.archive.path).map_err(err)? != self.archive
        {
            return Err(RuntimeError);
        }
        self.current_unchanged()?;
        if let Some(asset) = self.asset {
            asset.revalidate()?;
        }
        if DirectoryIdentity::capture(&self.archive.path).map_err(err)? != self.archive
            || DirectoryIdentity::capture(&self.observation.candidate.path).map_err(err)?
                != self.observation.candidate
            || self
                .owner
                .prior_archive(&self.lineage)
                .map_err(err)?
                .lineage_sha256
                != self.observation.lineage_sha256
            || RetainedEvidence::read(&self.evidence_path)? != self.retained
        {
            return Err(RuntimeError);
        }
        remaining()?;
        Ok(())
    }
    fn request(&self, candidate: PathBuf) -> Result<CandidateRequest> {
        Ok(CandidateRequest {
            version: 1,
            source: DirectoryIdentity::capture(self.owner.index_directory()).map_err(err)?,
            transfer_path: self.transfer_path.clone(),
            transfer: self.observation.current.clone(),
            target_authority_json: self.observation.target_authority_json.clone(),
            candidate,
            runtime: self.observation.prior.descriptor.clone(),
            limits: self.limits,
        })
    }
    fn current_unchanged(&self) -> Result<()> {
        self.owner.revalidate().map_err(err)?;
        let current = &self.observation.current.metadata;
        if self.owner.authority_sha256().map_err(err)? != current.ownership_sha256
            || self
                .owner
                .state_snapshot(self.limits.sql_bytes)
                .map_err(err)?
                != current.operational
            || inspect_runtime_source(
                self.owner.index_directory(),
                &self.embedding,
                self.owner.binding(),
                self.limits.native,
            )
            .map_err(err)?
                != current.source
        {
            return Err(RuntimeError);
        }
        canonical_transfer::read(
            &self.transfer_path,
            &self.observation.current,
            self.limits,
            |_| Ok(()),
        )
        .map_err(err)?;
        self.owner.revalidate().map_err(err)
    }
}
fn archive_guard(identity: &DirectoryIdentity) -> Result<File> {
    use std::os::unix::fs::MetadataExt;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&identity.path)?;
    let m = file.metadata()?;
    if m.dev() != identity.device || m.ino() != identity.inode {
        return Err(RuntimeError);
    }
    file.try_lock().map_err(err)?;
    Ok(file)
}
fn mode(asset: Option<&PinnedAsset>) -> Vec<std::ffi::OsString> {
    asset
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()])
}
fn with_request<T>(bytes: &[u8], run: impl FnOnce(&Path) -> Result<T>) -> Result<T> {
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let path = root.path().join("request");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(bytes)?;
    drop(file);
    run(&path)
}
fn observe(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    path: &Path,
    binding: &CommitBinding,
    embedding: &EmbeddingMetadata,
    limits: TransferLimits,
    budget: Duration,
) -> Result<SourceReport> {
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    let bytes = source::archived_request_bytes(path, binding, embedding, limits.native)?;
    let report = with_request(&bytes, |request| {
        SourceReport::parse_metadata(
            &runtime.run(
                &[
                    vec!["inspect-source".into(), request.as_os_str().to_owned()],
                    mode(asset),
                ]
                .concat(),
                budget,
                65536,
            )?,
        )
    })?;
    if report.descriptor.is_legacy() != asset.is_none()
        || report
            .source
            .lexical
            .as_ref()
            .is_some_and(|b| !report.descriptor.describes_bound_source(b))
    {
        return Err(RuntimeError);
    }
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    Ok(report)
}
fn reopen(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    request: &CandidateRequest,
    budget: Duration,
) -> Result<CandidateReport> {
    with_request(&request.canonical_bytes().map_err(err)?, |path| {
        CandidateReport::parse_metadata(
            &runtime.run(
                &[
                    vec!["candidate-reopen".into(), path.as_os_str().to_owned()],
                    mode(asset),
                ]
                .concat(),
                budget,
                131072,
            )?,
        )
        .map_err(err)
    })
}

/// Capture the latest owned source once. Reuse only an exact canonical archive;
/// newer acknowledged rows require a fresh selected-prior-runtime candidate.
/// Original operational SQL is never restored or passed to a subprocess.
pub fn prepare<'owner>(
    owner: &'owner OwnedOfflineInspection<'owner>,
    prior: PriorInputs<'owner>,
    embedding: &EmbeddingMetadata,
    target: &CommitBinding,
    transfer_path: &Path,
    candidate: &Path,
    limits: TransferLimits,
    budget: Duration,
) -> Result<VerifiedPriorCandidate<'owner>> {
    if budget.is_zero()
        || budget > Duration::from_secs(300)
        || target.table() != owner.binding().table()
        || target.workspace() != owner.binding().workspace()
        || target.roots() != owner.binding().roots()
        || target.scope() != owner.binding().scope()
        || target.generation() == owner.binding().generation()
        || target.epoch() < owner.binding().epoch().checked_add(2).ok_or(RuntimeError)?
    {
        return Err(RuntimeError);
    }
    let started = Instant::now();
    let remaining = || {
        budget
            .checked_sub(started.elapsed())
            .filter(|d| !d.is_zero())
            .ok_or(RuntimeError)
    };
    let provenance = owner.prior_archive(prior.lineage).map_err(err)?;
    let retained = RetainedEvidence::read(prior.evidence)?;
    let archive = DirectoryIdentity::capture(prior.archive).map_err(err)?;
    if archive.path != provenance.path
        || archive.device != provenance.device
        || archive.inode != provenance.inode
        || retained.prior_authority_sha256() != provenance.ownership_sha256
    {
        return Err(RuntimeError);
    }
    if transfer_path.starts_with(&archive.path) || candidate.starts_with(&archive.path) {
        return Err(RuntimeError);
    }
    super::evidence::parent(transfer_path)?;
    let guard = archive_guard(&archive)?;
    let observation = observe(
        prior.runtime,
        prior.asset,
        prior.archive,
        &provenance.binding,
        embedding,
        limits,
        remaining()?,
    )?;
    if !retained.matches_prior_observation(
        prior.runtime.sha256(),
        prior.asset.map(PinnedAsset::sha256),
        &observation,
    ) || observation.source.snapshot_sha256 != provenance.snapshot_sha256
    {
        return Err(RuntimeError);
    }
    builder::verify_fencing(
        prior.runtime,
        prior.asset,
        &observation.descriptor,
        limits.native,
        remaining()?,
    )?;
    let transfer =
        canonical_transfer::export(owner, transfer_path, embedding, limits).map_err(err)?;
    let exact = transfer.metadata.source.rows == observation.source.rows
        && transfer.metadata.source.snapshot_sha256 == observation.source.snapshot_sha256;
    let target_authority_json =
        String::from_utf8(source::canonical(target, 32768)?).map_err(err)?;
    let built = if exact {
        None
    } else {
        let request = CandidateRequest {
            version: 1,
            source: DirectoryIdentity::capture(owner.index_directory()).map_err(err)?,
            transfer_path: transfer_path.into(),
            transfer: transfer.clone(),
            target_authority_json: target_authority_json.clone(),
            candidate: candidate.into(),
            runtime: observation.descriptor.clone(),
            limits,
        };
        Some(builder::build(
            prior.runtime,
            prior.asset,
            &request,
            remaining()?,
        )?)
    };
    let evidence = CandidateEvidence {
        version: 1,
        kind: if exact {
            CandidateKind::ExactArchive
        } else {
            CandidateKind::Rebuilt
        },
        current: transfer,
        prior: observation,
        lineage_sha256: provenance.lineage_sha256,
        runtime_sha256: prior.runtime.sha256().into(),
        asset_sha256: prior.asset.map(|a| a.sha256().into()),
        candidate: built
            .as_ref()
            .map(|b| b.candidate.clone())
            .unwrap_or_else(|| archive.clone()),
        target_authority_json,
        built,
    };
    let proof = VerifiedPriorCandidate {
        owner,
        runtime: prior.runtime,
        asset: prior.asset,
        evidence_path: prior.evidence.into(),
        retained,
        lineage: prior.lineage.to_vec(),
        archive,
        transfer_path: transfer_path.into(),
        embedding: embedding.clone(),
        target: target.clone(),
        limits,
        budget,
        observation: evidence,
    };
    drop(guard);
    proof.revalidate()?;
    remaining()?;
    Ok(proof)
}
