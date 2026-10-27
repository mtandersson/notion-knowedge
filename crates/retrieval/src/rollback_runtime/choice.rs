//! Explicit quarantined recovery choices. Retained metadata is not authority.
use super::{Result, RuntimeError, source};
use crate::commit::transition::{TransitionRecord, runtime_journal::RuntimeJournalRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    OriginalSource,
    PreparedTarget,
}
/// Exact originating dialect, never a version-one disguise for a later protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Origin {
    Activation(Box<TransitionRecord>),
    Runtime(Box<RuntimeJournalRecord>),
}
impl Origin {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::Activation(record) => {
                crate::commit::transition::source::validate_origin_activation(record)
                    .map_err(|_| RuntimeError)?;
            }
            Self::Runtime(record) => {
                record.canonical_bytes().map_err(|_| RuntimeError)?;
            }
        }
        source::canonical(self, 1 << 20)
    }
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 1 << 20 {
            return Err(RuntimeError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RuntimeError)?;
        if value.canonical_bytes()? != bytes {
            return Err(RuntimeError);
        }
        Ok(value)
    }
}

use super::{
    PinnedRuntime, builder, candidate::observe, evidence::RetainedEvidence,
    prepared::PreparedLimits, source::SourceReport, verification::PinnedAsset,
};
use crate::{
    chunks::{RuntimeSourceSnapshot, inspect_runtime_source, runtime_builder::DirectoryIdentity},
    commit::{CommitBinding, OfflineBackup, transition::source::QuarantinedSourceInspection},
};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionMetadata {
    pub version: u32,
    pub protocol: String,
    pub choice: Choice,
    pub origin: Origin,
    pub operation_sha256: String,
    pub source_authority_sha256: String,
    pub original_source_authority_sha256: String,
    pub state_path: PathBuf,
    pub operational: OfflineBackup,
    pub current: DirectoryIdentity,
    pub current_native: RuntimeSourceSnapshot,
    pub selected: DirectoryIdentity,
    pub observed_authority_json: String,
    pub observation: SourceReport,
    pub runtime_sha256: String,
    pub asset_sha256: Option<String>,
    pub desired_authority_json: String,
    pub highest_reserved_epoch: u64,
}
/// Explicit trusted artifact selection. Original-source choices additionally
/// require retained evidence from that original physical/SQL source identity.
pub struct SelectionInputs<'a> {
    pub choice: Choice,
    pub runtime: &'a PinnedRuntime,
    pub asset: Option<&'a PinnedAsset>,
    pub source_evidence: Option<&'a Path>,
    pub desired: &'a CommitBinding,
}
/// Fresh read-only selection, not a Prepared or publication capability. It
/// borrows the actual quarantined owner; metadata has no inverse constructor.
pub struct VerifiedSelection<'a, 'owner> {
    inspection: &'a QuarantinedSourceInspection<'owner>,
    inputs: SelectionInputs<'a>,
    limits: PreparedLimits,
    metadata: SelectionMetadata,
}
impl VerifiedSelection<'_, '_> {
    pub fn metadata(&self) -> &SelectionMetadata {
        &self.metadata
    }
    pub fn revalidate(&self) -> Result<()> {
        if inspect_selection(self.inspection, &self.inputs, self.limits)? != self.metadata {
            return Err(RuntimeError);
        }
        Ok(())
    }
}
pub fn select<'a, 'owner>(
    inspection: &'a QuarantinedSourceInspection<'owner>,
    inputs: SelectionInputs<'a>,
    limits: PreparedLimits,
) -> Result<VerifiedSelection<'a, 'owner>> {
    let started = Instant::now();
    let metadata = inspect_selection(inspection, &inputs, limits)?;
    let remaining = || {
        limits
            .process_budget
            .checked_sub(started.elapsed())
            .filter(|budget| !budget.is_zero())
            .ok_or(RuntimeError)
    };
    // The pair is immutable for this borrowed proof. Fresh-process reopening
    // repeats these real probes; each revalidation still reopens actual native
    // data and checks the live SQL/physical source and artifact identities.
    builder::verify_publication_runtime(
        inputs.runtime,
        inputs.asset,
        &metadata.observation.descriptor,
        limits.transfer.native,
        remaining()?,
    )?;
    let mut recheck = limits;
    recheck.process_budget = remaining()?;
    if inspect_selection(inspection, &inputs, recheck)? != metadata {
        return Err(RuntimeError);
    }
    Ok(VerifiedSelection {
        inspection,
        inputs,
        limits,
        metadata,
    })
}
fn inspect_selection(
    inspection: &QuarantinedSourceInspection<'_>,
    inputs: &SelectionInputs<'_>,
    limits: PreparedLimits,
) -> Result<SelectionMetadata> {
    let started = Instant::now();
    let remaining = || {
        limits
            .process_budget
            .checked_sub(started.elapsed())
            .filter(|d| !d.is_zero())
            .ok_or(RuntimeError)
    };
    inspection.revalidate().map_err(|_| RuntimeError)?;
    inspection
        .validate_choice_fence(inputs.desired)
        .map_err(|_| RuntimeError)?;
    let context = inspection
        .choice_context(inputs.choice)
        .map_err(|_| RuntimeError)?;
    let operational = inspection
        .choice_operational_preimage(limits.transfer.sql_bytes)
        .map_err(|_| RuntimeError)?;
    let authority = inspection.authority_sha256().map_err(|_| RuntimeError)?;
    let current_native = inspect_runtime_source(
        inspection.index_directory(),
        inspection.embedding(),
        inspection.binding(),
        limits.transfer.native,
    )
    .map_err(|_| RuntimeError)?;
    let evidence = match inputs.choice {
        Choice::OriginalSource => Some(RetainedEvidence::read(
            inputs.source_evidence.ok_or(RuntimeError)?,
        )?),
        Choice::PreparedTarget if inputs.source_evidence.is_none() => None,
        _ => return Err(RuntimeError),
    };
    if let Some((sha, asset)) = &context.target_native.pinned
        && inputs.choice == Choice::PreparedTarget
        && (sha != inputs.runtime.sha256()
            || asset.as_deref() != inputs.asset.map(PinnedAsset::sha256))
    {
        return Err(RuntimeError);
    }
    let mut observed = None;
    for binding in &context.authorities {
        if let Ok(report) = observe(
            inputs.runtime,
            inputs.asset,
            &context.selected.path,
            binding,
            inspection.embedding(),
            limits.transfer,
            remaining()?,
        ) {
            observed = Some((binding, report));
            break;
        }
    }
    let (binding, report) = observed.ok_or(RuntimeError)?;
    match inputs.choice {
        Choice::OriginalSource => {
            let evidence = evidence.as_ref().ok_or(RuntimeError)?;
            if evidence.prior_authority_sha256() != context.original_source_authority_sha256
                || !evidence.matches_current_source_pair(
                    inputs.runtime.sha256(),
                    inputs.asset.map(PinnedAsset::sha256),
                    &report,
                )
            {
                return Err(RuntimeError);
            }
        }
        Choice::PreparedTarget if report.descriptor != context.target_native.descriptor => {
            return Err(RuntimeError);
        }
        Choice::PreparedTarget => (),
    }
    if report.source.rows != current_native.rows
        || report.source.snapshot_sha256 != current_native.snapshot_sha256
    {
        return Err(RuntimeError);
    }
    if observe(
        inputs.runtime,
        inputs.asset,
        &context.selected.path,
        binding,
        inspection.embedding(),
        limits.transfer,
        remaining()?,
    )? != report
        || inspect_runtime_source(
            inspection.index_directory(),
            inspection.embedding(),
            inspection.binding(),
            limits.transfer.native,
        )
        .map_err(|_| RuntimeError)?
            != current_native
        || inspection
            .choice_operational_preimage(limits.transfer.sql_bytes)
            .map_err(|_| RuntimeError)?
            != operational
        || inspection.authority_sha256().map_err(|_| RuntimeError)? != authority
        || DirectoryIdentity::capture(&context.selected.path).map_err(|_| RuntimeError)?
            != context.selected
    {
        return Err(RuntimeError);
    }
    if let Some(retained) = evidence
        && RetainedEvidence::read(inputs.source_evidence.ok_or(RuntimeError)?)? != retained
    {
        return Err(RuntimeError);
    }
    if let Some(asset) = inputs.asset {
        asset.revalidate()?;
    }
    inspection.revalidate().map_err(|_| RuntimeError)?;
    remaining()?;
    Ok(SelectionMetadata {
        version: 1,
        protocol: "quarantined-runtime-selection-v1".into(),
        choice: inputs.choice,
        origin: context.origin,
        operation_sha256: context.operation_sha256,
        source_authority_sha256: authority,
        original_source_authority_sha256: context.original_source_authority_sha256,
        state_path: context.state_path,
        operational,
        current: context.source,
        current_native,
        selected: context.selected,
        observed_authority_json: String::from_utf8(source::canonical(binding, 32768)?)
            .map_err(|_| RuntimeError)?,
        observation: report,
        runtime_sha256: inputs.runtime.sha256().into(),
        asset_sha256: inputs.asset.map(|a| a.sha256().into()),
        desired_authority_json: String::from_utf8(source::canonical(inputs.desired, 32768)?)
            .map_err(|_| RuntimeError)?,
        highest_reserved_epoch: context.highest_reserved_epoch,
    })
}

#[cfg(test)]
pub(crate) mod tests;

mod prepared;
pub use prepared::{PreparationPaths, PreparedChoiceMetadata, VerifiedPreparedChoice};

mod store;
pub use store::reopen;

mod handoff;
pub use handoff::{
    ChoiceHandoffRecord, HandoffPhase, RetainedFileIdentity, recover_activation_handoff,
};

mod reservation;
pub use reservation::{PublicationStartMetadata, reserve_activation_publication};

mod native_journal;
pub use native_journal::{ChoiceNativePhase, ChoiceNativeRecord};
