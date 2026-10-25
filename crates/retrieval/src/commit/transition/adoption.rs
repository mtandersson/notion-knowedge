//! Compatible derived SQL authority transfer. No Notion effects or physical
//! publication occur here. The transition owner must verify the native pair,
//! retained backup and adoption manifest while holding exclusive ownership.
use super::*;
use crate::durable_plan::{ActualOutcome, DurablePlan, PreparedOperation};

#[derive(Debug, Clone, Copy)]
pub struct AdoptionLimits {
    pub rows: u64,
    pub plan_bytes: u64,
    pub effect_bytes: u64,
    pub total_effect_bytes: u64,
    pub identity_bytes: u64,
}
impl Default for AdoptionLimits {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            plan_bytes: 16 * 1024 * 1024,
            effect_bytes: 1024 * 1024,
            total_effect_bytes: 64 * 1024 * 1024,
            identity_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AdoptionReport {
    pub receipts: usize,
    pub metadata: usize,
    pub relations: usize,
    pub aliases: usize,
    pub schedules: usize,
    pub plans: usize,
}

/// Transfer compatible derived authority inside the owner's exclusive SQL
/// transaction. A nested savepoint prevents partial adoption even if a caller
/// accidentally commits its outer transaction after this function returns Err.
/// This neither publishes index_commit_binding nor acknowledges any pending work.
pub fn adopt_derived_work(
    owner: &TransitionOwnership,
    transaction: &mut rusqlite::Transaction<'_>,
    source: &CommitBinding,
    target: &CommitBinding,
    target_index_version: u64,
    limits: AdoptionLimits,
) -> Result<AdoptionReport, CommitError> {
    owner.validate_owned_transaction(transaction)?;
    let mut expected = owner.record.clone();
    expected.phase = TransitionPhase::TargetInstalled;
    if TransitionRecord::read(&owner.state_path)?.as_ref() != Some(&expected)
        || serde_json::to_string(source).map_err(|_| CommitError::InvalidInput)?
            != expected.source.binding
        || serde_json::to_string(target).map_err(|_| CommitError::InvalidInput)?
            != expected.target.binding
        || Identity::file(&owned_directory(&expected.target.path)?)?
            != Identity::file(&owner.target)?
    {
        return Err(CommitError::BindingMismatch);
    }
    owner.validate_sql(transaction)?;
    let quarantine: bool = transaction
        .query_row(
            "SELECT generation=?1 AND epoch=?2 FROM index_commit_binding WHERE singleton=1",
            params![
                expected.quarantine_generation,
                i64::try_from(expected.quarantine_epoch).map_err(|_| CommitError::InvalidInput)?
            ],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if !quarantine {
        return Err(CommitError::BindingMismatch);
    }
    transfer_sql(
        owner,
        transaction,
        source,
        target,
        target_index_version,
        limits,
        false,
    )?
    .ok_or(CommitError::InvalidInput)
}

pub(super) fn preview_derived_work(
    owner: &TransitionOwnership,
    transaction: &mut rusqlite::Transaction<'_>,
    source: &CommitBinding,
    target: &CommitBinding,
    target_index_version: u64,
    limits: AdoptionLimits,
) -> Result<(), CommitError> {
    transfer_sql(
        owner,
        transaction,
        source,
        target,
        target_index_version,
        limits,
        true,
    )?;
    Ok(())
}

fn transfer_sql(
    owner: &TransitionOwnership,
    transaction: &mut rusqlite::Transaction<'_>,
    source: &CommitBinding,
    target: &CommitBinding,
    target_index_version: u64,
    limits: AdoptionLimits,
    preview: bool,
) -> Result<Option<AdoptionReport>, CommitError> {
    owner.validate_owned_transaction(transaction)?;
    let savepoint = transaction.savepoint().map_err(sql)?;
    let plans = inspect_derived_work(
        &savepoint,
        source,
        target,
        target_index_version,
        limits,
        preview,
    )?;
    if preview {
        // Dropping the savepoint rolls back the temporary ownership table.
        // No operational table or receipt has been changed.
        owner.validate_owned_transaction(&savepoint)?;
        return Ok(None);
    }
    let update = |table: &str| {
        savepoint
            .execute(
                &format!("UPDATE {table} SET generation=?2 WHERE generation=?1"),
                params![source.generation(), target.generation()],
            )
            .map_err(sql)
    };
    let receipts = savepoint.execute("UPDATE index_commit_receipts SET generation=?2 WHERE generation=?1 AND status IN(0,1,2)",params![source.generation(),target.generation()]).map_err(sql)?;
    let metadata = update("page_metadata_identity")?;
    let relations = update("page_relation_projections")?;
    let aliases = update("page_alias_projections")?;
    let schedules = savepoint
        .execute(
            "UPDATE reconciliation_schedules SET generation=?2,epoch=?3 WHERE generation=?1",
            params![
                source.generation(),
                target.generation(),
                i64::try_from(target.epoch()).map_err(|_| CommitError::InvalidInput)?
            ],
        )
        .map_err(sql)?;
    for (run, document) in &plans {
        savepoint
            .execute(
                "UPDATE reconciliation_plans SET document=?2 WHERE run_id=?1",
                params![run, document],
            )
            .map_err(sql)?;
    }
    let report = AdoptionReport {
        receipts,
        metadata,
        relations,
        aliases,
        schedules,
        plans: plans.len(),
    };
    savepoint
        .execute_batch("DROP TABLE nk_adoption_owned_pages;")
        .map_err(sql)?;
    owner.revalidate()?;
    owner.validate_sql(&savepoint)?;
    savepoint.commit().map_err(sql)?;
    Ok(Some(report))
}

// Shared validation for publication and an already-owned read-only preview.
fn inspect_derived_work(
    connection: &Connection,
    source: &CommitBinding,
    target: &CommitBinding,
    target_index_version: u64,
    limits: AdoptionLimits,
    preview: bool,
) -> Result<Vec<(String, String)>, CommitError> {
    if source.table() != target.table()
        || source.workspace() != target.workspace()
        || source.roots() != target.roots()
        || source.scope() != target.scope()
        || source.generation() == target.generation()
        || source.epoch() >= target.epoch()
        || target_index_version == 0
        || limits.rows == 0
        || limits.plan_bytes == 0
        || limits.effect_bytes == 0
        || limits.total_effect_bytes == 0
        || limits.identity_bytes == 0
    {
        return Err(CommitError::InvalidInput);
    }
    let sentinel = i64::try_from(limits.rows)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or(CommitError::InvalidInput)?;
    let plan_bytes = i64::try_from(limits.plan_bytes).map_err(|_| CommitError::InvalidInput)?;
    let effect_bytes = i64::try_from(limits.effect_bytes).map_err(|_| CommitError::InvalidInput)?;
    // Bound complete table scans, including unrelated historical generations,
    // before generation-filtered queries or updates can inspect them.
    let mut charged = 0u64;
    for table in [
        "index_commit_receipts",
        "page_metadata_identity",
        "page_relation_projections",
        "page_alias_projections",
        "relation_operation_proposals",
        "alias_operation_proposals",
        "reconciliation_runs",
        "reconciliation_plans",
        "reconciliation_plan_progress",
        "reconciliation_schedules",
    ] {
        let count: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM (SELECT 1 FROM {table} LIMIT ?1)"),
                [sentinel],
                |r| r.get(0),
            )
            .map_err(sql)?;
        charged = charged
            .checked_add(count as u64)
            .ok_or(CommitError::InvalidInput)?;
        if charged > limits.rows {
            return Err(CommitError::InvalidInput);
        }
    }
    let invalid_page: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE generation=?1 AND status IN(0,1,2) AND length(CAST(page_id AS BLOB)) NOT BETWEEN 1 AND 4096)",[source.generation()],|r|r.get(0)).map_err(sql)?;
    let page_bytes: i64 = connection.query_row("SELECT coalesce(sum(length(CAST(page_id AS BLOB))),0) FROM index_commit_receipts WHERE generation=?1 AND status IN(0,1,2)",[source.generation()],|r|r.get(0)).map_err(sql)?;
    if invalid_page || page_bytes as u64 > limits.identity_bytes {
        return Err(CommitError::InvalidInput);
    }
    connection
        .execute_batch(
            "CREATE TEMP TABLE nk_adoption_owned_pages(page_id TEXT PRIMARY KEY) WITHOUT ROWID;",
        )
        .map_err(sql)?;
    connection.execute("INSERT INTO nk_adoption_owned_pages SELECT DISTINCT page_id FROM index_commit_receipts WHERE generation=?1 AND status IN(0,1,2)",[source.generation()]).map_err(sql)?;
    let reserved: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 AND scope=?2 AND epoch=?3)",params![target.generation(),target.scope(),i64::try_from(target.epoch()).map_err(|_|CommitError::InvalidInput)?],|r|r.get(0)).map_err(sql)?;
    if reserved == preview {
        return Err(CommitError::BindingMismatch);
    }
    let source_reserved: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1 AND scope=?2 AND epoch=?3)",params![source.generation(),source.scope(),i64::try_from(source.epoch()).map_err(|_|CommitError::InvalidInput)?],|r|r.get(0)).map_err(sql)?;
    let policy: bool = connection.query_row("SELECT table_name=?1 AND workspace=?2 AND scope=?3 FROM index_commit_binding WHERE singleton=1",params![source.table(),source.workspace(),source.scope()],|r|r.get(0)).optional().map_err(sql)?.unwrap_or(false);
    if !source_reserved || !policy {
        return Err(CommitError::BindingMismatch);
    }
    for table in [
        "page_metadata_identity",
        "page_relation_projections",
        "page_alias_projections",
    ] {
        let orphan: bool = connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} p WHERE p.generation=?1 AND NOT EXISTS(SELECT 1 FROM nk_adoption_owned_pages r WHERE r.page_id=p.page_id))"),[source.generation()],|r|r.get(0)).map_err(sql)?;
        if orphan {
            return Err(CommitError::BindingMismatch);
        }
    }
    for table in ["page_relation_projections", "page_alias_projections"] {
        let invalid: bool = connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE generation=?1 AND (version<>1 OR length(CAST(effect AS BLOB))>?2))"),params![source.generation(),effect_bytes],|r|r.get(0)).map_err(sql)?;
        if invalid {
            return Err(CommitError::InvalidInput);
        }
    }
    for table in ["relation_operation_proposals", "alias_operation_proposals"] {
        let oversized: bool = connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} p JOIN index_commit_receipts r USING(operation_id) WHERE r.generation=?1 AND r.status IN(0,1,2) AND length(CAST(p.effect AS BLOB))>?2)"),params![source.generation(),effect_bytes],|r|r.get(0)).map_err(sql)?;
        if oversized {
            return Err(CommitError::InvalidInput);
        }
    }
    let mut total_effects = 0u64;
    for query in [
        "SELECT coalesce(sum(length(CAST(effect AS BLOB))),0) FROM page_relation_projections WHERE generation=?1",
        "SELECT coalesce(sum(length(CAST(effect AS BLOB))),0) FROM page_alias_projections WHERE generation=?1",
        "SELECT coalesce(sum(length(CAST(p.effect AS BLOB))),0) FROM relation_operation_proposals p JOIN index_commit_receipts r USING(operation_id) WHERE r.generation=?1 AND r.status IN(0,1,2)",
        "SELECT coalesce(sum(length(CAST(p.effect AS BLOB))),0) FROM alias_operation_proposals p JOIN index_commit_receipts r USING(operation_id) WHERE r.generation=?1 AND r.status IN(0,1,2)",
    ] {
        let bytes: i64 = connection
            .query_row(query, [source.generation()], |r| r.get(0))
            .map_err(sql)?;
        total_effects = total_effects
            .checked_add(bytes as u64)
            .ok_or(CommitError::InvalidInput)?;
        if total_effects > limits.total_effect_bytes {
            return Err(CommitError::InvalidInput);
        }
    }
    for (markers, proposals, format) in [
        (
            "page_alias_projections",
            "alias_operation_proposals",
            "(length(CAST(effect AS BLOB))<>73 OR substr(effect,1,9)<>'alias-v1:' OR substr(effect,10) GLOB '*[^0-9a-f]*')",
        ),
        (
            "page_relation_projections",
            "relation_operation_proposals",
            "(substr(effect,1,12)<>'relation-v1:' AND substr(effect,1,9)<>'graph-v2:')",
        ),
    ] {
        let invalid: bool = connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {markers} WHERE generation=?1 AND {format}) OR EXISTS(SELECT 1 FROM {proposals} JOIN index_commit_receipts USING(operation_id) WHERE generation=?1 AND status IN(0,1,2) AND {format})"),[source.generation()],|r|r.get(0)).map_err(sql)?;
        if invalid {
            return Err(CommitError::InvalidInput);
        }
    }
    {
        let mut statement = connection.prepare("SELECT effect FROM page_relation_projections WHERE generation=?1 UNION ALL SELECT p.effect FROM relation_operation_proposals p JOIN index_commit_receipts r USING(operation_id) WHERE r.generation=?1 AND r.status IN(0,1,2)").map_err(sql)?;
        for effect in statement
            .query_map([source.generation()], |r| r.get::<_, String>(0))
            .map_err(sql)?
        {
            if !super::super::projection::valid_effect_encoding(&effect.map_err(sql)?) {
                return Err(CommitError::InvalidInput);
            }
        }
    }
    let invalid_schedule: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_schedules WHERE generation=?1 AND (table_name<>?2 OR workspace<>?3 OR scope<>?4 OR epoch<>?5))",params![source.generation(),source.table(),source.workspace(),source.scope(),i64::try_from(source.epoch()).map_err(|_|CommitError::InvalidInput)?],|r|r.get(0)).map_err(sql)?;
    if invalid_schedule {
        return Err(CommitError::BindingMismatch);
    }
    let mut plans = Vec::new();
    {
        let mut statement = connection.prepare("SELECT CASE WHEN length(CAST(r.run_id AS BLOB)) BETWEEN 1 AND 4096 THEN r.run_id ELSE NULL END,CASE WHEN length(CAST(r.scope AS BLOB))=64 THEN r.scope ELSE NULL END,p.version,CASE WHEN length(CAST(p.document AS BLOB))<=?1 THEN p.document ELSE NULL END FROM reconciliation_runs r LEFT JOIN reconciliation_plans p USING(run_id) WHERE r.phase<>2 AND r.abandoned=0 LIMIT 2").map_err(sql)?;
        let mut rows = statement.query([plan_bytes]).map_err(sql)?;
        let mut active = 0;
        while let Some(row) = rows.next().map_err(sql)? {
            active += 1;
            if active > 1 || row.get::<_, String>(1).map_err(sql)? != source.scope() {
                return Err(CommitError::BindingMismatch);
            }
            let version: Option<i64> = row.get(2).map_err(sql)?;
            if version.is_none() {
                continue;
            } // A run not yet planned retains its ID and queue.
            if version != Some(1) {
                return Err(CommitError::InvalidInput);
            }
            let run: String = row.get(0).map_err(sql)?;
            let document: String = row.get(3).map_err(sql)?;
            let plan: DurablePlan =
                serde_json::from_str(&document).map_err(|_| CommitError::InvalidInput)?;
            if plan.run_id() != run {
                return Err(CommitError::BindingMismatch);
            }
            let prepared_bytes:i64 = connection.query_row("SELECT coalesce(sum(length(CAST(prepared AS BLOB))),0) FROM reconciliation_plan_progress WHERE run_id=?1",[&run],|r|r.get(0)).map_err(sql)?;
            if (document.len() as u64)
                .checked_add(prepared_bytes as u64)
                .is_none_or(|bytes| bytes > limits.plan_bytes)
            {
                return Err(CommitError::InvalidInput);
            }
            let mut progress = connection.prepare("SELECT CASE WHEN length(CAST(page_id AS BLOB)) BETWEEN 1 AND 4096 THEN page_id ELSE NULL END,CASE WHEN length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 4096 THEN operation_id ELSE NULL END,status,prepared FROM reconciliation_plan_progress WHERE run_id=?1 ORDER BY page_id").map_err(sql)?;
            let mut progress_rows = progress.query([&run]).map_err(sql)?;
            let mut count = 0usize;
            while let Some(row) = progress_rows.next().map_err(sql)? {
                count += 1;
                let page: String = row.get(0).map_err(sql)?;
                let operation: String = row.get(1).map_err(sql)?;
                if plan
                    .pages
                    .get(&page)
                    .is_none_or(|item| item.page_id != page || item.operation_id != operation)
                {
                    return Err(CommitError::BindingMismatch);
                }
                let prepared: Option<String> = row.get(3).map_err(sql)?;
                if row.get::<_, i64>(2).map_err(sql)? == 1 && prepared.is_none() {
                    return Err(CommitError::InvalidInput);
                }
                if let Some(prepared) = prepared {
                    let prepared: PreparedOperation =
                        serde_json::from_str(&prepared).map_err(|_| CommitError::InvalidInput)?;
                    let (action, checkpoint) =
                        match prepared.action().map_err(|_| CommitError::InvalidInput)? {
                            ActualOutcome::Deleted => {
                                if prepared.content_hash().is_some() {
                                    return Err(CommitError::InvalidInput);
                                }
                                (
                                    PageAction::Delete,
                                    PageSyncState::tombstone(
                                        page,
                                        prepared.last_edited_time().map(str::to_owned),
                                    ),
                                )
                            }
                            outcome => (
                                if outcome == ActualOutcome::Refreshed {
                                    PageAction::Refresh
                                } else {
                                    PageAction::Unchanged
                                },
                                PageSyncState::present(
                                    page,
                                    prepared
                                        .content_hash()
                                        .ok_or(CommitError::InvalidInput)?
                                        .into(),
                                    prepared.last_edited_time().map(str::to_owned),
                                ),
                            ),
                        };
                    PageOperation::new(
                        &operation,
                        prepared.revision(),
                        action,
                        checkpoint.map_err(|_| CommitError::InvalidInput)?,
                    )?;
                }
            }
            if count != plan.pages.len() {
                return Err(CommitError::BindingMismatch);
            }
            let adopted = plan
                .adopt_physical_generation(source, target, target_index_version)
                .map_err(|_| CommitError::BindingMismatch)?;
            let encoded = serde_json::to_string(&adopted).map_err(|_| CommitError::InvalidInput)?;
            if (encoded.len() as u64)
                .checked_add(prepared_bytes as u64)
                .is_none_or(|bytes| bytes > limits.plan_bytes)
            {
                return Err(CommitError::InvalidInput);
            }
            plans.push((run, encoded));
        }
    }
    Ok(plans)
}

/// Validate compatible unfinished work in the caller's existing exclusive SQL
/// context. Only a temporary ownership table is created, then rolled back.
#[cfg(target_os = "linux")]
pub(crate) fn preview_current(
    owner: &super::super::offline::OwnedOfflineInspection<'_>,
    target: &CommitBinding,
    target_version: u64,
    limits: AdoptionLimits,
) -> Result<(), CommitError> {
    struct Preview<'a> {
        connection: &'a Connection,
        active: bool,
    }
    impl Drop for Preview<'_> {
        fn drop(&mut self) {
            if self.active {
                let _ = self
                    .connection
                    .execute_batch("ROLLBACK TO nk_runtime_preview; RELEASE nk_runtime_preview");
            }
        }
    }
    let connection = owner.owned_connection()?;
    connection
        .execute_batch("SAVEPOINT nk_runtime_preview")
        .map_err(sql)?;
    let mut guard = Preview {
        connection,
        active: true,
    };
    let result = inspect_derived_work(
        connection,
        owner.binding(),
        target,
        target_version,
        limits,
        true,
    );
    connection
        .execute_batch("ROLLBACK TO nk_runtime_preview; RELEASE nk_runtime_preview")
        .map_err(sql)?;
    guard.active = false;
    result?;
    owner.revalidate()
}
