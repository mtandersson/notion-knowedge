//! Exact sealed execution and real fencing probes; results remain descriptive.
use super::{PinnedRuntime, Result, RuntimeError, probe, verification::PinnedAsset};
use crate::chunks::runtime_builder::{CandidateReport, CandidateRequest};
use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    time::{Duration, Instant},
};
/// Build and reopen using one explicitly trusted immutable runtime/asset pair.
/// No current-source SQL is opened here. The caller must retain current source
/// ownership and freshly verify prior provenance; #540 supplies that boundary.
/// Successful execution is not a transferable publication capability.
pub fn build(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    request: &CandidateRequest,
    budget: Duration,
) -> Result<CandidateReport> {
    if budget.is_zero()
        || budget > Duration::from_secs(300)
        || request.runtime.is_legacy() != asset.is_none()
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
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    let scratch = tempfile::Builder::new()
        .prefix("nk-runtime-builder-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let path = scratch.path().join("request.json");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(&request.canonical_bytes().map_err(|_| RuntimeError)?)?;
    drop(file);
    let mode = asset
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    let run = |action: &str, path: &std::path::Path| {
        runtime.run(
            &[
                vec![action.into(), path.as_os_str().to_owned()],
                mode.clone(),
            ]
            .concat(),
            remaining()?,
            131072,
        )
    };
    // A capability string cannot prove that a coupled-generation runtime can
    // admit revoked/current policy under distinct physical fences.
    verify_fencing(
        runtime,
        asset,
        &request.runtime,
        request.limits.native,
        remaining()?,
    )?;
    let create = CandidateReport::parse_metadata(&run("candidate-create", &path)?)
        .map_err(|_| RuntimeError)?;
    let reopen = CandidateReport::parse_metadata(&run("candidate-reopen", &path)?)
        .map_err(|_| RuntimeError)?;
    if create != reopen
        || create.runtime != request.runtime
        || create.target_authority_json != request.target_authority_json
        || create.transfer_sha256 != request.transfer.sha256
        || create.candidate.path != request.candidate
        || create.snapshot.rows != request.transfer.metadata.source.rows
        || create.snapshot.snapshot_sha256 != request.transfer.metadata.source.snapshot_sha256
    {
        return Err(RuntimeError);
    }
    if crate::chunks::runtime_builder::DirectoryIdentity::capture(&request.candidate)
        .map_err(|_| RuntimeError)?
        != reopen.candidate
    {
        return Err(RuntimeError);
    }
    if let Some(asset) = asset {
        asset.revalidate()?;
    }
    remaining()?;
    Ok(reopen)
}
pub(super) fn verify_fencing(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    descriptor: &crate::chunks::lexical_binding::runtime::RuntimeDescriptor,
    limits: crate::chunks::CandidateLimits,
    budget: Duration,
) -> Result<()> {
    probe_runtime(runtime, asset, descriptor, limits, budget, false)
}
pub(super) fn verify_publication_runtime(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    descriptor: &crate::chunks::lexical_binding::runtime::RuntimeDescriptor,
    limits: crate::chunks::CandidateLimits,
    budget: Duration,
) -> Result<()> {
    probe_runtime(runtime, asset, descriptor, limits, budget, true)
}
fn probe_runtime(
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    descriptor: &crate::chunks::lexical_binding::runtime::RuntimeDescriptor,
    limits: crate::chunks::CandidateLimits,
    budget: Duration,
    admission: bool,
) -> Result<()> {
    let started = Instant::now();
    let scratch = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let root = scratch.path().join("probe");
    let mode = asset
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    let run = |action: &str| {
        runtime.run(
            &[
                vec![action.into(), root.as_os_str().to_owned()],
                mode.clone(),
            ]
            .concat(),
            budget
                .checked_sub(started.elapsed())
                .filter(|d| !d.is_zero())
                .ok_or(RuntimeError)?,
            65536,
        )
    };
    let created = probe::verify_report(&run("probe-create")?, descriptor, &root, limits)?;
    let reopened = probe::verify_report(&run("probe-reopen")?, descriptor, &root, limits)?;
    if created != reopened {
        return Err(RuntimeError);
    }
    if admission {
        let path = scratch.path().join("admission.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&probe::admission_request(
            &root,
            descriptor.is_legacy(),
            limits,
        )?)?;
        drop(file);
        let bytes = runtime.run(
            &[
                vec!["inspect-admission".into(), path.as_os_str().to_owned()],
                mode,
            ]
            .concat(),
            budget
                .checked_sub(started.elapsed())
                .filter(|d| !d.is_zero())
                .ok_or(RuntimeError)?,
            65536,
        )?;
        probe::verify_admission_report(&bytes, &created)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests;
