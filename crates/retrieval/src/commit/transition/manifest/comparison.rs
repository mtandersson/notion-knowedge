//! Exact allowed-delta comparison with an immutable activation backup.
use super::*;
use crate::durable_plan::DurablePlan;
use std::{cell::Cell, os::unix::fs::OpenOptionsExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonStage {
    Prepared,
    BeforePublication,
    AfterPublication,
}

pub struct ComparisonInputs<'a> {
    pub source: &'a CommitBinding,
    pub target: &'a CommitBinding,
    pub target_version: u64,
    pub plan_bytes: u64,
    pub backup_bytes: u64,
    pub limits: ManifestLimits,
}

struct ActivePlan {
    run: String,
    original: String,
    adopted: String,
}
struct Context<'a> {
    record: &'a TransitionRecord,
    input: &'a ComparisonInputs<'a>,
    stage: ComparisonStage,
    source_device: String,
    source_inode: String,
    target_device: String,
    target_inode: String,
    source_epoch: i64,
    target_epoch: i64,
    state_device: String,
    state_inode: String,
    active: Option<ActivePlan>,
    reserved: Cell<u8>,
}

fn index(columns: &[String], name: &str) -> Result<usize, CommitError> {
    columns
        .iter()
        .position(|column| column == name)
        .ok_or(CommitError::InvalidInput)
}
fn text<'a>(row: &'a Row<'_>, columns: &[String], name: &str) -> Result<&'a str, CommitError> {
    row.get_ref(index(columns, name)?)
        .map_err(sql)?
        .as_str()
        .map_err(|_| CommitError::InvalidInput)
}
fn integer(row: &Row<'_>, columns: &[String], name: &str) -> Result<i64, CommitError> {
    row.get_ref(index(columns, name)?)
        .map_err(sql)?
        .as_i64()
        .map_err(|_| CommitError::InvalidInput)
}

fn source_limits(
    record: &TransitionRecord,
    input: &ComparisonInputs<'_>,
) -> Result<ManifestLimits, CommitError> {
    preparation_limits(
        input.source,
        input.target,
        &record.quarantine_generation,
        input.limits,
    )
}
/// Leave room for both future physical fences before any Prepared publication.
pub(crate) fn preparation_limits(
    source: &CommitBinding,
    target: &CommitBinding,
    quarantine_generation: &str,
    limits: ManifestLimits,
) -> Result<ManifestLimits, CommitError> {
    let mut limits = limits;
    // Reserve capacity for the only two new logical rows before quarantine.
    // The estimate matches the reader's byte preflight, including ROWID/epoch.
    let mut reserve = 0u64;
    for generation in [quarantine_generation, target.generation()] {
        let bytes = 4 * (generation.len() as u64 + source.scope().len() as u64 + 40) + 128;
        if bytes > limits.row_bytes {
            return Err(CommitError::InvalidInput);
        }
        reserve = reserve
            .checked_add(bytes)
            .ok_or(CommitError::InvalidInput)?;
    }
    limits.rows = limits
        .rows
        .checked_sub(2)
        .ok_or(CommitError::InvalidInput)?;
    limits.total_bytes = limits
        .total_bytes
        .checked_sub(reserve)
        .ok_or(CommitError::InvalidInput)?;
    limits.digest_bytes = limits
        .digest_bytes
        .checked_sub(64)
        .ok_or(CommitError::InvalidInput)?;
    Ok(limits)
}

fn check_input(record: &TransitionRecord, input: &ComparisonInputs<'_>) -> Result<(), CommitError> {
    if input.target_version == 0
        || input.plan_bytes == 0
        || input.backup_bytes == 0
        || record.backup.bytes > input.backup_bytes
        || serde_json::to_string(input.source).map_err(|_| CommitError::InvalidInput)?
            != record.source.binding
        || serde_json::to_string(input.target).map_err(|_| CommitError::InvalidInput)?
            != record.target.binding
    {
        return Err(CommitError::BindingMismatch);
    }
    canonical(record)?;
    Ok(())
}

fn backup_guard(
    record: &TransitionRecord,
    file: &File,
    db: &Connection,
) -> Result<(), CommitError> {
    record.backup.verify(&record.backup_path)?;
    validate_connection_file(db, file, &record.backup_path)
}

fn with_backup<T>(
    record: &TransitionRecord,
    operation: impl FnOnce(&rusqlite::Transaction<'_>, &File) -> Result<T, CommitError>,
) -> Result<T, CommitError> {
    record.backup.verify(&record.backup_path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&record.backup_path)
        .map_err(io)?;
    if Identity::file(&file)?
        == (Identity {
            device: record.state_device,
            inode: record.state_inode,
        })
    {
        return Err(CommitError::InvalidInput);
    }
    let connection = Connection::open_with_flags(
        &record.backup_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(sql)?;
    let transaction = connection.unchecked_transaction().map_err(sql)?;
    // Force opening the main pager before querying its descriptor identity.
    transaction
        .query_row("SELECT 1 FROM main.sqlite_schema LIMIT 1", [], |r| {
            r.get::<_, i64>(0)
        })
        .map_err(sql)?;
    backup_guard(record, &file, &transaction)?;
    let result = operation(&transaction, &file)?;
    backup_guard(record, &file, &transaction)?;
    transaction.commit().map_err(sql)?;
    Ok(result)
}

impl<'a> Context<'a> {
    fn load(
        record: &'a TransitionRecord,
        input: &'a ComparisonInputs<'a>,
        backup: &rusqlite::Transaction<'_>,
        stage: ComparisonStage,
    ) -> Result<Self, CommitError> {
        let mut active = None;
        let mut statement = backup.prepare("SELECT CASE WHEN octet_length(r.run_id) BETWEEN 1 AND 4096 THEN r.run_id ELSE NULL END,CASE WHEN p.document IS NULL OR octet_length(p.document)<=?1 THEN p.document ELSE NULL END,p.document IS NOT NULL FROM main.reconciliation_runs r LEFT JOIN main.reconciliation_plans p USING(run_id) WHERE r.phase<>2 AND r.abandoned=0 LIMIT 2").map_err(sql)?;
        let mut rows = statement
            .query([i64::try_from(input.plan_bytes).map_err(|_| CommitError::InvalidInput)?])
            .map_err(sql)?;
        let mut count = 0;
        while let Some(row) = rows.next().map_err(sql)? {
            count += 1;
            if count > 1 {
                return Err(CommitError::InvalidInput);
            }
            let original: Option<String> = row.get(1).map_err(sql)?;
            if original.is_none() && row.get::<_, bool>(2).map_err(sql)? {
                return Err(CommitError::InvalidInput);
            }
            if let Some(original) = original {
                if original.len() as u64 > input.plan_bytes {
                    return Err(CommitError::InvalidInput);
                }
                let run: String = row.get(0).map_err(sql)?;
                let plan: DurablePlan =
                    serde_json::from_str(&original).map_err(|_| CommitError::InvalidInput)?;
                if plan.run_id() != run {
                    return Err(CommitError::BindingMismatch);
                }
                let adopted = plan
                    .adopt_physical_generation(input.source, input.target, input.target_version)
                    .map_err(|_| CommitError::BindingMismatch)?;
                let adopted =
                    serde_json::to_string(&adopted).map_err(|_| CommitError::InvalidInput)?;
                if original.len() as u64 > input.plan_bytes
                    || adopted.len() as u64 > input.plan_bytes
                {
                    return Err(CommitError::InvalidInput);
                }
                active = Some(ActivePlan {
                    run,
                    original,
                    adopted,
                });
            }
        }
        Ok(Self {
            record,
            input,
            stage,
            source_device: record.source.device.to_string(),
            source_inode: record.source.inode.to_string(),
            target_device: record.target.device.to_string(),
            target_inode: record.target.inode.to_string(),
            source_epoch: i64::try_from(input.source.epoch())
                .map_err(|_| CommitError::InvalidInput)?,
            target_epoch: i64::try_from(input.target.epoch())
                .map_err(|_| CommitError::InvalidInput)?,
            state_device: record.state_device.to_string(),
            state_inode: record.state_inode.to_string(),
            active,
            reserved: Cell::new(0),
        })
    }

    fn generation<'r>(
        &self,
        row: &'r Row<'_>,
        columns: &[String],
        table: &str,
    ) -> Result<&'r str, CommitError> {
        let generation = text(row, columns, "generation")?;
        if table == "index_commit_receipts" && integer(row, columns, "status")? == 3 {
            return Ok(generation);
        }
        if generation == self.record.quarantine_generation {
            return Err(CommitError::BindingMismatch);
        }
        match self.stage {
            ComparisonStage::Prepared | ComparisonStage::BeforePublication
                if generation == self.input.target.generation() =>
            {
                return Err(CommitError::BindingMismatch);
            }
            ComparisonStage::AfterPublication if generation == self.input.source.generation() => {
                return Err(CommitError::BindingMismatch);
            }
            _ => {}
        }
        Ok(generation)
    }
}

impl Normalizer for Context<'_> {
    fn keep_row(
        &self,
        table: &str,
        columns: &[String],
        row: &Row<'_>,
    ) -> Result<bool, CommitError> {
        if table == "index_commit_generations" {
            if columns != ["generation", "scope", "epoch", "rowid"] {
                return Err(CommitError::InvalidInput);
            }
            let generation = text(row, columns, "generation")?;
            let epoch = integer(row, columns, "epoch")?;
            let q_epoch = i64::try_from(self.record.quarantine_epoch)
                .map_err(|_| CommitError::InvalidInput)?;
            if generation == self.record.quarantine_generation
                || generation == self.input.target.generation()
                || epoch == q_epoch
                || epoch == self.target_epoch
            {
                if self.stage == ComparisonStage::Prepared {
                    return Err(CommitError::Conflict);
                }
                let bit = if generation == self.record.quarantine_generation && epoch == q_epoch {
                    1
                } else if generation == self.input.target.generation() && epoch == self.target_epoch
                {
                    2
                } else {
                    return Err(CommitError::BindingMismatch);
                };
                if text(row, columns, "scope")? != self.input.source.scope()
                    || self.reserved.get() & bit != 0
                {
                    return Err(CommitError::BindingMismatch);
                }
                self.reserved.set(self.reserved.get() | bit);
                return Ok(false);
            }
        } else if table == "index_commit_binding" {
            let (generation, epoch, device, inode) = match self.stage {
                ComparisonStage::Prepared => (
                    self.input.source.generation(),
                    self.source_epoch,
                    self.source_device.as_str(),
                    self.source_inode.as_str(),
                ),
                ComparisonStage::BeforePublication => (
                    self.record.quarantine_generation.as_str(),
                    i64::try_from(self.record.quarantine_epoch)
                        .map_err(|_| CommitError::InvalidInput)?,
                    self.source_device.as_str(),
                    self.source_inode.as_str(),
                ),
                ComparisonStage::AfterPublication => (
                    self.input.target.generation(),
                    self.target_epoch,
                    self.target_device.as_str(),
                    self.target_inode.as_str(),
                ),
            };
            for (name, expected) in [
                ("generation", generation),
                ("index_device", device),
                ("index_inode", inode),
                ("state_device", self.state_device.as_str()),
                ("state_inode", self.state_inode.as_str()),
                ("table_name", self.input.source.table()),
                ("workspace", self.input.source.workspace()),
                ("scope", self.input.source.scope()),
            ] {
                if text(row, columns, name)? != expected {
                    return Err(CommitError::BindingMismatch);
                }
            }
            if integer(row, columns, "singleton")? != 1 || integer(row, columns, "epoch")? != epoch
            {
                return Err(CommitError::BindingMismatch);
            }
        } else if table == "index_commit_state_identity"
            && (integer(row, columns, "singleton")? != 1
                || text(row, columns, "identity")? != self.record.state_token)
        {
            return Err(CommitError::BindingMismatch);
        }
        Ok(true)
    }
    fn value<'a>(
        &'a self,
        table: &str,
        columns: &[String],
        row: &'a Row<'_>,
        column: usize,
        value: ValueRef<'a>,
    ) -> Result<ValueRef<'a>, CommitError> {
        let name = &columns[column];
        if table == "index_commit_binding" {
            return Ok(match name.as_str() {
                "generation" => ValueRef::Text(self.input.source.generation().as_bytes()),
                "epoch" => ValueRef::Integer(self.source_epoch),
                "index_device" => ValueRef::Text(self.source_device.as_bytes()),
                "index_inode" => ValueRef::Text(self.source_inode.as_bytes()),
                _ => value,
            });
        }
        if matches!(
            table,
            "index_commit_receipts"
                | "page_metadata_identity"
                | "page_relation_projections"
                | "page_alias_projections"
                | "reconciliation_schedules"
        ) {
            let generation = self.generation(row, columns, table)?;
            if table == "reconciliation_schedules"
                && (generation == self.input.source.generation()
                    || generation == self.input.target.generation())
            {
                let expected = if self.stage == ComparisonStage::AfterPublication {
                    self.target_epoch
                } else {
                    self.source_epoch
                };
                if integer(row, columns, "epoch")? != expected {
                    return Err(CommitError::BindingMismatch);
                }
                if name == "epoch" {
                    return Ok(ValueRef::Integer(self.source_epoch));
                }
            }
            if self.stage == ComparisonStage::AfterPublication
                && generation == self.input.target.generation()
                && name == "generation"
            {
                if table == "index_commit_receipts" && integer(row, columns, "status")? == 3 {
                    return Ok(value);
                }
                return Ok(ValueRef::Text(self.input.source.generation().as_bytes()));
            }
        }
        if table == "reconciliation_plans"
            && name == "document"
            && let Some(plan) = &self.active
            && text(row, columns, "run_id")? == plan.run
        {
            let expected = if self.stage == ComparisonStage::AfterPublication {
                &plan.adopted
            } else {
                &plan.original
            };
            if value.as_str().map_err(|_| CommitError::InvalidInput)? != expected {
                return Err(CommitError::BindingMismatch);
            }
            return Ok(ValueRef::Text(plan.original.as_bytes()));
        }
        Ok(value)
    }
    fn finish_table(&self, table: &str, physical: u64, logical: u64) -> Result<(), CommitError> {
        if matches!(
            table,
            "index_commit_binding" | "index_commit_state_identity"
        ) && (physical != 1 || logical != 1)
        {
            return Err(CommitError::BindingMismatch);
        }
        if table == "index_commit_generations" {
            let (count, mask) = if self.stage == ComparisonStage::Prepared {
                (0, 0)
            } else {
                (2, 3)
            };
            if physical - logical != count || self.reserved.get() != mask {
                return Err(CommitError::BindingMismatch);
            }
        }
        Ok(())
    }
}

/// Seal the source preimage before the record is persisted. This does not touch
/// operational state. The source hash must become record.adoption_sha256.
pub fn capture_backup_manifest(
    record: &TransitionRecord,
    input: &ComparisonInputs<'_>,
) -> Result<SqlManifest, CommitError> {
    check_input(record, input)?;
    with_backup(record, |backup, _file| {
        let raw = capture_sql_manifest(backup, source_limits(record, input)?)?;
        let context = Context::load(record, input, backup, ComparisonStage::Prepared)?;
        let checked = capture_normalized(backup, source_limits(record, input)?, &context)?;
        if checked != raw {
            return Err(CommitError::BindingMismatch);
        }
        Ok(raw)
    })
}

/// Compare every preserved main-schema value against the verified backup.
/// Native verification and publication must occur in this same transaction.
pub fn verify_backup_comparison(
    owner: &TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    input: &ComparisonInputs<'_>,
    stage: ComparisonStage,
) -> Result<SqlManifest, CommitError> {
    owner.validate_owned_transaction(transaction)?;
    let record = TransitionRecord::read(&owner.state_path)?.ok_or(CommitError::BindingMismatch)?;
    let mut expected = owner.record().clone();
    expected.phase = record.phase;
    check_input(&record, input)?;
    let valid_phase = match stage {
        ComparisonStage::Prepared => record.phase == TransitionPhase::Prepared,
        ComparisonStage::BeforePublication => record.phase == TransitionPhase::TargetInstalled,
        ComparisonStage::AfterPublication => matches!(
            record.phase,
            TransitionPhase::TargetInstalled | TransitionPhase::Published
        ),
    };
    if expected != record || !valid_phase {
        return Err(CommitError::BindingMismatch);
    }
    let (source_path, target_path) = owner.physical_paths()?;
    let valid_paths = if stage == ComparisonStage::Prepared {
        source_path == record.source.path && target_path == record.candidate_path
    } else {
        source_path == record.archive_path && target_path == record.target.path
    };
    if !valid_paths {
        return Err(CommitError::BindingMismatch);
    }
    with_backup(&record, |backup, file| {
        let original = capture_sql_manifest(backup, source_limits(&record, input)?)?;
        if original.sha256 != record.adoption_sha256 {
            return Err(CommitError::BindingMismatch);
        }
        let mut context = Context::load(&record, input, backup, ComparisonStage::Prepared)?;
        if capture_normalized(backup, source_limits(&record, input)?, &context)? != original {
            return Err(CommitError::BindingMismatch);
        }
        context.stage = stage;
        context.reserved.set(0);
        let actual = capture_normalized(transaction, input.limits, &context)?;
        backup_guard(&record, file, backup)?;
        owner.validate_owned_transaction(transaction)?;
        if actual != original
            || TransitionRecord::read(&owner.state_path)?.as_ref() != Some(&record)
        {
            return Err(CommitError::BindingMismatch);
        }
        Ok(original)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sqlite_descriptor_cannot_point_to_a_transient_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite");
        let original = Connection::open(&path).unwrap();
        original
            .execute_batch("CREATE TABLE data(value); INSERT INTO data VALUES('retained')")
            .unwrap();
        drop(original);
        let retained = File::open(&path).unwrap();
        let aside = directory.path().join("retained.sqlite");
        fs::rename(&path, &aside).unwrap();
        fs::copy(&aside, &path).unwrap();
        let transient =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        transient
            .query_row("SELECT value FROM main.data", [], |r| r.get::<_, String>(0))
            .unwrap();
        fs::remove_file(&path).unwrap();
        fs::rename(&aside, &path).unwrap();
        assert_eq!(
            validate_connection_file(&transient, &retained, &path),
            Err(CommitError::BindingMismatch)
        );
        let correct =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        correct
            .query_row("SELECT value FROM main.data", [], |r| r.get::<_, String>(0))
            .unwrap();
        validate_connection_file(&correct, &retained, &path).unwrap();
    }
}
