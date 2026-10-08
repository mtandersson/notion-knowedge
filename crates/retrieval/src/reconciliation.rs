//! SQLite journal. Each command uses an IMMEDIATE transaction across connections.
use crate::sync_state::SqliteSyncStateStore;
use notion_knowledge_core::reconciliation::*;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
fn sql(_: rusqlite::Error) -> JournalError {
    JournalError::Unavailable
}
fn expiry(now: i64, duration: i64) -> Result<i64, JournalError> {
    if now < 0 || !(1..=3600).contains(&duration) {
        return Err(JournalError::InvalidInput);
    }
    now.checked_add(duration).ok_or(JournalError::InvalidInput)
}
fn guard(db: &Connection, lease: &Lease, now: i64) -> Result<(), JournalError> {
    if now < 0 {
        return Err(JournalError::InvalidInput);
    }
    let valid: bool = db
        .query_row(
            "SELECT fence=?1 AND expires_at>?2 FROM reconciliation_lease WHERE singleton=1",
            params![lease.fence, now],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if valid {
        Ok(())
    } else {
        Err(JournalError::LeaseLost)
    }
}
fn phase(db: &Connection, lease: &Lease, run: &str, expected: i64) -> Result<(), JournalError> {
    let valid: bool = db
        .query_row(
            "SELECT fence=?1 AND phase=?2 FROM reconciliation_runs WHERE run_id=?3",
            params![lease.fence, expected, run],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?
        .unwrap_or(false);
    if valid {
        Ok(())
    } else {
        Err(JournalError::InvalidTransition)
    }
}
fn failure_number(value: FailureClass) -> i64 {
    match value {
        FailureClass::Source => 0,
        FailureClass::Index => 1,
        FailureClass::Conflict => 2,
        FailureClass::Unavailable => 3,
    }
}
fn failure_value(value: Option<i64>) -> Result<Option<FailureClass>, JournalError> {
    value
        .map(|value| match value {
            0 => Ok(FailureClass::Source),
            1 => Ok(FailureClass::Index),
            2 => Ok(FailureClass::Conflict),
            3 => Ok(FailureClass::Unavailable),
            _ => Err(JournalError::Unavailable),
        })
        .transpose()
}
fn action_number(action: WorkAction) -> i64 {
    match action {
        WorkAction::Refresh => 0,
        WorkAction::Delete => 1,
        WorkAction::Unchanged => 2,
    }
}
fn snapshot(
    db: &Connection,
    run: &str,
    scope: &ReconciliationScope,
) -> Result<ReconciliationRun, JournalError> {
    let (identity, phase, checkpoint, next_deadline): (String, i64, Option<String>, Option<i64>) =
        db.query_row(
            "SELECT scope,phase,checkpoint,next_deadline FROM reconciliation_runs WHERE run_id=?1",
            [run],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(sql)?
        .ok_or(JournalError::InvalidTransition)?;
    if identity != scope.identity() {
        return Err(JournalError::ScopeMismatch);
    }
    let (inventory_count, pending_count, applied_count, failed_count) = db.query_row("SELECT COUNT(*),COALESCE(SUM(status=0),0),COALESCE(SUM(status=1),0),COALESCE(SUM(status=2),0) FROM reconciliation_inventory WHERE run_id=?1", [run], |r| Ok((r.get::<_,i64>(0)? as u64,r.get::<_,i64>(1)? as u64,r.get::<_,i64>(2)? as u64,r.get::<_,i64>(3)? as u64))).map_err(sql)?;
    let (refreshed_count,deleted_count,unchanged_count)=db.query_row("SELECT COALESCE(SUM(status=1 AND action=0),0),COALESCE(SUM(status=1 AND action=1),0),COALESCE(SUM(status=1 AND action=2),0) FROM reconciliation_inventory WHERE run_id=?1",[run],|r|Ok((r.get::<_,i64>(0)? as u64,r.get::<_,i64>(1)? as u64,r.get::<_,i64>(2)? as u64))).map_err(sql)?;
    Ok(ReconciliationRun {
        run_id: run.into(),
        phase: match phase {
            0 => RunPhase::Inventory,
            1 => RunPhase::Applying,
            2 => RunPhase::Completed,
            _ => return Err(JournalError::Unavailable),
        },
        checkpoint,
        inventory_count,
        pending_count,
        applied_count,
        failed_count,
        refreshed_count,
        deleted_count,
        unchanged_count,
        next_deadline,
        failure: failure_value(
            db.query_row(
                "SELECT failure FROM reconciliation_runs WHERE run_id=?1",
                [run],
                |r| r.get(0),
            )
            .map_err(sql)?,
        )?,
    })
}
impl SqliteSyncStateStore {
    fn journal_transaction<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> Result<T, JournalError>,
    ) -> Result<T, JournalError> {
        let mut db = self
            .lock_connection()
            .map_err(|_| JournalError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let result = action(&tx)?;
        tx.commit().map_err(sql)?;
        Ok(result)
    }
}
impl ReconciliationJournal for SqliteSyncStateStore {
    fn acquire_lease(&self, now: i64, duration: i64) -> Result<Lease, JournalError> {
        let expires_at = expiry(now, duration)?;
        self.journal_transaction(|db| {
            let (fence, deadline): (i64, i64) = db
                .query_row(
                    "SELECT fence,expires_at FROM reconciliation_lease WHERE singleton=1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(sql)?;
            if deadline > now {
                return Err(JournalError::Busy);
            }
            let fence = fence.checked_add(1).ok_or(JournalError::Unavailable)?;
            db.execute(
                "UPDATE reconciliation_lease SET fence=?1,expires_at=?2 WHERE singleton=1",
                params![fence, expires_at],
            )
            .map_err(sql)?;
            Ok(Lease { fence, expires_at })
        })
    }
    fn renew_lease(&self, lease: &Lease, now: i64, duration: i64) -> Result<Lease, JournalError> {
        let expires_at = expiry(now, duration)?;
        self.journal_transaction(|db| {
            guard(db, lease, now)?;
            db.execute(
                "UPDATE reconciliation_lease SET expires_at=?1 WHERE singleton=1",
                [expires_at],
            )
            .map_err(sql)?;
            Ok(Lease {
                fence: lease.fence,
                expires_at,
            })
        })
    }
    fn release_lease(&self, lease: &Lease, now: i64) -> Result<(), JournalError> {
        self.journal_transaction(|db| {
            guard(db, lease, now)?;
            db.execute(
                "UPDATE reconciliation_lease SET expires_at=0 WHERE singleton=1",
                [],
            )
            .map_err(sql)?;
            Ok(())
        })
    }
    fn start_or_resume(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        scope: &ReconciliationScope,
    ) -> Result<ReconciliationRun, JournalError> {
        validate(run)?;
        self.journal_transaction(|db| {
            guard(db,lease,now)?;
            let existing:Option<String>=db.query_row("SELECT scope FROM reconciliation_runs WHERE run_id=?1",[run],|r|r.get(0)).optional().map_err(sql)?;
            if existing.as_deref().is_some_and(|s|s!=scope.identity()) {return Err(JournalError::ScopeMismatch);}
            let active:Option<String>=db.query_row("SELECT run_id FROM reconciliation_runs WHERE phase<>2",[],|r|r.get(0)).optional().map_err(sql)?;
            if active.as_deref().is_some_and(|id|id!=run) {return Err(JournalError::Busy);}
            db.execute("INSERT INTO reconciliation_runs(run_id,scope,fence,phase) VALUES(?1,?2,?3,0) ON CONFLICT(run_id) DO UPDATE SET fence=excluded.fence",params![run,scope.identity(),lease.fence]).map_err(sql)?;
            snapshot(db,run,scope)
        })
    }
    fn run(
        &self,
        run: &str,
        scope: &ReconciliationScope,
    ) -> Result<ReconciliationRun, JournalError> {
        self.journal_transaction(|db| snapshot(db, run, scope))
    }
    fn inventory(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        pages: &[InventoryPage],
        checkpoint: Option<&str>,
    ) -> Result<(), JournalError> {
        if pages.len() > 1000 {
            return Err(JournalError::InvalidInput);
        }
        for page in pages {
            validate(&page.page_id)?;
            validate(&page.revision)?;
        }
        if let Some(value) = checkpoint {
            validate(value)?;
        }
        self.journal_transaction(|db| {guard(db,lease,now)?;phase(db,lease,run,0)?;
            for page in pages {
                let old:Option<(String,i64)>=db.query_row("SELECT revision,action FROM reconciliation_inventory WHERE run_id=?1 AND page_id=?2",params![run,page.page_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql)?;
                if old.as_ref().is_some_and(|(revision,action)|revision!=&page.revision || *action!=action_number(page.action)) {return Err(JournalError::InvalidTransition);}
                db.execute("INSERT OR IGNORE INTO reconciliation_inventory VALUES(?1,?2,?3,?4,0,NULL)",params![run,page.page_id,page.revision,action_number(page.action)]).map_err(sql)?;
            }
            db.execute("UPDATE reconciliation_runs SET checkpoint=?1 WHERE run_id=?2",params![checkpoint,run]).map_err(sql)?;Ok(())
        })
    }
    fn seal_inventory(&self, lease: &Lease, now: i64, run: &str) -> Result<(), JournalError> {
        self.journal_transaction(|db| {
            guard(db, lease, now)?;
            phase(db, lease, run, 0)?;
            db.execute(
                "UPDATE reconciliation_runs SET phase=1 WHERE run_id=?1",
                [run],
            )
            .map_err(sql)?;
            Ok(())
        })
    }
    fn work(&self, run: &str) -> Result<Vec<JournalWork>, JournalError> {
        self.journal_transaction(|db| {
            let mut statement=db.prepare("SELECT page_id,revision,status,action,failure FROM reconciliation_inventory WHERE run_id=?1 ORDER BY page_id").map_err(sql)?;
            statement.query_map([run],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,Option<i64>>(4)?))).map_err(sql)?.map(|r| {
                let (page_id,revision,status,action,failure)=r.map_err(sql)?;
                Ok(JournalWork {
                    page: InventoryPage { page_id,revision,action:match action {0=>WorkAction::Refresh,1=>WorkAction::Delete,2=>WorkAction::Unchanged,_=>return Err(JournalError::Unavailable)} },
                    failure: failure_value(failure)?,
                    status:match status {0=>WorkStatus::Pending,1=>WorkStatus::Applied,2=>WorkStatus::Failed,_=>return Err(JournalError::Unavailable)}
                })
            }).collect()
        })
    }
    fn acknowledge(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        page: &str,
        status: WorkStatus,
    ) -> Result<(), JournalError> {
        let target = match status {
            WorkStatus::Applied => 1,
            WorkStatus::Failed => 2,
            WorkStatus::Pending => return Err(JournalError::InvalidTransition),
        };
        self.journal_transaction(|db| {
            guard(db, lease, now)?;
            phase(db, lease, run, 1)?;
            let current: Option<i64> = db
                .query_row(
                    "SELECT status FROM reconciliation_inventory WHERE run_id=?1 AND page_id=?2",
                    params![run, page],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql)?;
            if current != Some(0) && current != Some(target) {
                return Err(JournalError::InvalidTransition);
            }
            db.execute(
                "UPDATE reconciliation_inventory SET status=?1,failure=CASE WHEN ?1=2 THEN 1 ELSE NULL END WHERE run_id=?2 AND page_id=?3",
                params![target, run, page],
            )
            .map_err(sql)?;
            Ok(())
        })
    }
    fn retry_failed(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        page: &str,
    ) -> Result<(), JournalError> {
        self.journal_transaction(|db| {guard(db,lease,now)?;phase(db,lease,run,1)?;let count=db.execute("UPDATE reconciliation_inventory SET status=0,failure=NULL WHERE run_id=?1 AND page_id=?2 AND status=2",params![run,page]).map_err(sql)?;if count==1{Ok(())}else{Err(JournalError::InvalidTransition)}})
    }
    fn record_failure(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        failure: FailureClass,
        deadline: Option<i64>,
    ) -> Result<(), JournalError> {
        if deadline.is_some_and(|d| d < now) {
            return Err(JournalError::InvalidInput);
        }
        self.journal_transaction(|db| {guard(db,lease,now)?;
            let count=db.execute("UPDATE reconciliation_runs SET failure=?1,next_deadline=?2 WHERE run_id=?3 AND fence=?4 AND phase<>2",params![failure_number(failure),deadline,run,lease.fence]).map_err(sql)?;
            if count==1 {Ok(())}else{Err(JournalError::InvalidTransition)}
        })
    }
    fn complete(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        deadline: Option<i64>,
    ) -> Result<(), JournalError> {
        if deadline.is_some_and(|d| d < now) {
            return Err(JournalError::InvalidInput);
        }
        self.journal_transaction(|db| {
            guard(db, lease, now)?;
            phase(db, lease, run, 1)?;
            let remaining: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM reconciliation_inventory WHERE run_id=?1 AND status<>1",
                    [run],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            if remaining != 0 {
                return Err(JournalError::InvalidTransition);
            }
            db.execute(
                "UPDATE reconciliation_runs SET phase=2,failure=NULL,next_deadline=?1 WHERE run_id=?2",
                params![deadline, run],
            )
            .map_err(sql)?;
            Ok(())
        })
    }
}
