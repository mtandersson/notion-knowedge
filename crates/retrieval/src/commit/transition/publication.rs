//! Atomic derived-state publication. Journal completion and serving admission
//! remain separate and require durable anchor installation and fresh validation.
use super::*;
use crate::chunks::{TransitionCandidateInputs, verify_transition_candidate};
use adoption::{AdoptionLimits, AdoptionReport, adopt_derived_work};
use manifest::{
    ManifestLimits,
    comparison::{ComparisonInputs, ComparisonStage, verify_backup_comparison},
};

/// Reverify the native pair inside the actual exclusive transaction, then
/// transfer authority and compare the entire resulting SQL state. This owns
/// the transaction and rolls it back on any error. It leaves the journal at
/// TargetInstalled, denying admission until explicit completion.
pub fn publish_derived_state(
    owner: &TransitionOwnership,
    now: i64,
    candidate: TransitionCandidateInputs<'_>,
    adoption: AdoptionLimits,
    manifest: ManifestLimits,
    backup_bytes: u64,
) -> Result<AdoptionReport, CommitError> {
    const PUBLISH: &str = "UPDATE main.index_commit_binding SET index_device=?1,index_inode=?2,generation=?3,epoch=?4 WHERE singleton=1 AND generation=?5 AND epoch=?6";
    owner.with_exclusive_sql(now, |transaction| {
        let source = candidate.source;
        let target = candidate.target;
        let verified = verify_transition_candidate(owner, transaction, candidate)?;
        if verified.record().phase != TransitionPhase::TargetInstalled {
            return Err(CommitError::Conflict);
        }
        let comparison = ComparisonInputs {
            source,
            target,
            target_version: verified.target_version(),
            plan_bytes: adoption.plan_bytes,
            backup_bytes,
            limits: manifest,
        };
        verify_backup_comparison(
            owner,
            transaction,
            &comparison,
            ComparisonStage::BeforePublication,
        )?;
        let report = adopt_derived_work(
            owner,
            transaction,
            source,
            target,
            verified.target_version(),
            adoption,
        )?;
        let record = verified.record();
        let changed = transaction
            .execute(
                PUBLISH,
                params![
                    record.target.device.to_string(),
                    record.target.inode.to_string(),
                    target.generation(),
                    i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?,
                    record.quarantine_generation,
                    i64::try_from(record.quarantine_epoch)
                        .map_err(|_| CommitError::InvalidInput)?,
                ],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(CommitError::BindingMismatch);
        }
        verify_backup_comparison(
            owner,
            transaction,
            &comparison,
            ComparisonStage::AfterPublication,
        )?;
        Ok(report)
    })
}
