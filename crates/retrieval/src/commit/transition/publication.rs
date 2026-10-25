//! Atomic derived-state publication. Journal completion and serving admission
//! remain separate and require durable anchor installation and fresh validation.
use super::*;
use crate::chunks::{TransitionCandidateInputs, verify_transition_candidate};
use adoption::{AdoptionLimits, AdoptionReport, adopt_derived_work};
use manifest::{
    ManifestLimits,
    comparison::{ComparisonInputs, ComparisonStage, verify_backup_comparison},
};

#[derive(Clone, Copy)]
enum Authority {
    Source,
    Quarantine,
    Target,
}

/// Explicit offline forward activation/reconciliation. The caller must stop all
/// source processes before acquiring the owner. Every step inspects actual SQL
/// authority and freshly verifies the retained native pair; journal phase alone
/// cannot authorize adoption. Completed generations use ordinary runtime
/// admission, because later acknowledged work need not match the old snapshot.
pub fn reconcile_forward(
    owner: &TransitionOwnership,
    now: i64,
    candidate: TransitionCandidateInputs<'_>,
    adoption: AdoptionLimits,
    limits: ManifestLimits,
    backup_bytes: u64,
) -> Result<TransitionRecord, CommitError> {
    for _ in 0..6 {
        let (phase, authority) = owner.with_exclusive_sql(now, |transaction| {
            let verified = verify_transition_candidate(owner, transaction, candidate)?;
            let generation: String = transaction
                .query_row(
                    "SELECT generation FROM main.index_commit_binding WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            // validate_owned_transaction (inside native verification) already
            // checks the exact physical identity, scope, token and epoch tuple.
            let authority = if generation == candidate.source.generation() {
                Authority::Source
            } else if generation == owner.record.quarantine_generation {
                Authority::Quarantine
            } else if generation == candidate.target.generation() {
                Authority::Target
            } else {
                return Err(CommitError::BindingMismatch);
            };
            Ok((verified.record().phase, authority))
        })?;
        match (phase, authority) {
            (TransitionPhase::Prepared, Authority::Source) => {
                preview_publication(owner, now, candidate, adoption, limits, backup_bytes)?;
                owner.quarantine(now)?;
            }
            (TransitionPhase::Prepared, Authority::Quarantine) => {
                owner.quarantine(now)?;
            }
            (
                TransitionPhase::Quarantined | TransitionPhase::SourceRetained,
                Authority::Quarantine,
            ) => {
                owner.install_candidate(now)?;
            }
            (TransitionPhase::TargetInstalled, Authority::Quarantine) => {
                publish_derived_state(owner, now, candidate, adoption, limits, backup_bytes)?;
            }
            (TransitionPhase::TargetInstalled | TransitionPhase::Published, Authority::Target) => {
                return complete_publication(
                    owner,
                    now,
                    candidate,
                    adoption.plan_bytes,
                    limits,
                    backup_bytes,
                );
            }
            _ => return Err(CommitError::BindingMismatch),
        }
    }
    Err(CommitError::Conflict)
}

/// Validate all retained derived work before quarantine, without changing
/// operational rows. The temporary ownership set is rolled back on success.
pub fn preview_publication(
    owner: &TransitionOwnership,
    now: i64,
    candidate: TransitionCandidateInputs<'_>,
    adoption: AdoptionLimits,
    limits: ManifestLimits,
    backup_bytes: u64,
) -> Result<(), CommitError> {
    owner.with_exclusive_sql(now, |transaction| {
        let verified = verify_transition_candidate(owner, transaction, candidate)?;
        if verified.record().phase != TransitionPhase::Prepared {
            return Err(CommitError::Conflict);
        }
        let comparison = ComparisonInputs {
            source: candidate.source,
            target: candidate.target,
            target_version: verified.target_version(),
            plan_bytes: adoption.plan_bytes,
            backup_bytes,
            limits,
        };
        verify_backup_comparison(owner, transaction, &comparison, ComparisonStage::Prepared)?;
        adoption::preview_derived_work(
            owner,
            transaction,
            candidate.source,
            candidate.target,
            verified.target_version(),
            adoption,
        )?;
        verify_backup_comparison(owner, transaction, &comparison, ComparisonStage::Prepared)?;
        Ok(())
    })
}

pub(crate) fn install_anchor(owner: &TransitionOwnership) -> Result<(), CommitError> {
    owner.revalidate()?;
    let directory = &owner.record.target.path;
    let destination = directory.join(ANCHOR);
    if !destination.try_exists().map_err(io)? {
        let temporary = tempfile::Builder::new()
            .prefix(".nk-binding-")
            .tempfile_in(directory)
            .map_err(io)?;
        let expected = format!(
            "nk-index-binding-v1\n{}\n{}\n{}\n",
            owner.record.state_device, owner.record.state_inode, owner.record.state_token
        );
        temporary
            .as_file()
            .write_all(expected.as_bytes())
            .map_err(io)?;
        temporary.as_file().sync_all().map_err(io)?;
        owner.revalidate()?;
        temporary
            .persist_noclobber(&destination)
            .map_err(|error| io(error.error))?;
    }
    owner.verify_anchor(&owner.target)?;
    owner.target.sync_all().map_err(io)?;
    #[cfg(test)]
    transition_checkpoint(owner, "anchor-installed");
    owner.revalidate()
}

/// Finish a committed SQL publication, including recovery when the journal
/// still says TargetInstalled. Never re-adopt work or restore the backup.
/// A fresh complete native/SQL comparison precedes each journal advance.
pub fn complete_publication(
    owner: &TransitionOwnership,
    now: i64,
    candidate: TransitionCandidateInputs<'_>,
    plan_bytes: u64,
    limits: ManifestLimits,
    backup_bytes: u64,
) -> Result<TransitionRecord, CommitError> {
    owner.with_exclusive_sql(now, |transaction| {
        let verify = || {
            let verified = verify_transition_candidate(owner, transaction, candidate)?;
            let comparison = ComparisonInputs {
                source: candidate.source,
                target: candidate.target,
                target_version: verified.target_version(),
                plan_bytes,
                backup_bytes,
                limits,
            };
            verify_backup_comparison(
                owner,
                transaction,
                &comparison,
                ComparisonStage::AfterPublication,
            )?;
            Ok::<_, CommitError>(verified.record().clone())
        };
        let mut current = verify()?;
        install_anchor(owner)?;
        if current.phase == TransitionPhase::TargetInstalled {
            let previous = current.clone();
            current.phase = TransitionPhase::Published;
            current.persist(owner, Some(&previous))?;
        }
        if verify()? != current || current.phase != TransitionPhase::Published {
            return Err(CommitError::Conflict);
        }
        owner.verify_anchor(&owner.target)?;
        let previous = current.clone();
        current.phase = TransitionPhase::Complete;
        current.persist(owner, Some(&previous))?;
        Ok(current)
    })
}

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
    let report = owner.with_exclusive_sql(now, |transaction| {
        let source = candidate.source;
        let target = candidate.target;
        let verified = verify_transition_candidate(owner, transaction, candidate)?;
        if verified.record().phase != TransitionPhase::TargetInstalled {
            return Err(CommitError::Conflict);
        }
        let report = publish_derived_owned(
            owner,
            transaction,
            SqlPublication {
                source,
                target,
                target_version: verified.target_version(),
                adoption,
                manifest,
                backup_bytes,
            },
        )?;
        #[cfg(test)]
        {
            if std::env::var("NK_TRANSITION_TEST_PHASE").ok().as_deref()
                == Some("sql-before-commit")
            {
                // Force dirty pages into the original file, producing a hot
                // rollback journal when the subprocess is killed below.
                let flushed = unsafe { rusqlite::ffi::sqlite3_db_cacheflush(transaction.handle()) };
                if flushed != rusqlite::ffi::SQLITE_OK {
                    return Err(CommitError::Unavailable);
                }
            }
            transition_checkpoint(owner, "sql-before-commit");
        }
        Ok(report)
    })?;
    #[cfg(test)]
    transition_checkpoint(owner, "sql-published");
    Ok(report)
}

pub(crate) struct SqlPublication<'a> {
    pub source: &'a CommitBinding,
    pub target: &'a CommitBinding,
    pub target_version: u64,
    pub adoption: AdoptionLimits,
    pub manifest: ManifestLimits,
    pub backup_bytes: u64,
}
/// Called only after actual native verification in this same transaction.
pub(crate) fn publish_derived_owned(
    owner: &TransitionOwnership,
    transaction: &mut rusqlite::Transaction<'_>,
    input: SqlPublication<'_>,
) -> Result<AdoptionReport, CommitError> {
    const PUBLISH: &str = "UPDATE main.index_commit_binding SET index_device=?1,index_inode=?2,generation=?3,epoch=?4 WHERE singleton=1 AND generation=?5 AND epoch=?6";
    owner.validate_owned_transaction(transaction)?;
    let record = owner
        .current_record()?
        .ok_or(CommitError::BindingMismatch)?;
    let source = input.source;
    let target = input.target;
    if record.phase != TransitionPhase::TargetInstalled
        || serde_json::to_string(source).map_err(|_| CommitError::InvalidInput)?
            != record.source.binding
        || serde_json::to_string(target).map_err(|_| CommitError::InvalidInput)?
            != record.target.binding
    {
        return Err(CommitError::BindingMismatch);
    }
    let comparison = ComparisonInputs {
        source,
        target,
        target_version: input.target_version,
        plan_bytes: input.adoption.plan_bytes,
        backup_bytes: input.backup_bytes,
        limits: input.manifest,
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
        input.target_version,
        input.adoption,
    )?;
    let record = &record;
    let changed = transaction
        .execute(
            PUBLISH,
            params![
                record.target.device.to_string(),
                record.target.inode.to_string(),
                target.generation(),
                i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?,
                record.quarantine_generation,
                i64::try_from(record.quarantine_epoch).map_err(|_| CommitError::InvalidInput)?,
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
}
