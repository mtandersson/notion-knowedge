//! Durable scheduling through the same trusted coordinator binding and global lease.
//! This module does not dispatch source requests or start a background runtime.
use crate::{
    commit::{CommitBinding, CommitError, IndexCommitCoordinator},
    reconciliation::guard,
};
use notion_knowledge_core::reconciliation::{FailureClass, JournalError, Lease, validate};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulePolicy {
    interval_seconds: i64,
    max_attempts: i64,
    backoff_seconds: i64,
    max_backoff_seconds: i64,
}
impl SchedulePolicy {
    /// Seconds. Intervals: 60 seconds–30 days; 1–16 failure attempts;
    /// initial backoff: 1 second–1 day; maximum backoff: initial–7 days.
    pub fn new(
        interval: i64,
        attempts: i64,
        backoff: i64,
        maximum: i64,
    ) -> Result<Self, JournalError> {
        if !(60..=2_592_000).contains(&interval)
            || !(1..=16).contains(&attempts)
            || !(1..=86_400).contains(&backoff)
            || !(backoff..=604_800).contains(&maximum)
        {
            return Err(JournalError::InvalidInput);
        }
        Ok(Self {
            interval_seconds: interval,
            max_attempts: attempts,
            backoff_seconds: backoff,
            max_backoff_seconds: maximum,
        })
    }
    pub fn interval_seconds(self) -> i64 {
        self.interval_seconds
    }
    pub fn max_attempts(self) -> i64 {
        self.max_attempts
    }
    pub fn backoff_seconds(self) -> i64 {
        self.backoff_seconds
    }
    pub fn max_backoff_seconds(self) -> i64 {
        self.max_backoff_seconds
    }
}
/// Explicit replacement of authority/policy after recovery of the old cycle.
#[derive(Debug, Clone)]
pub struct ScheduleReconfiguration {
    pub previous_binding: CommitBinding,
    pub policy: SchedulePolicy,
    pub next_deadline: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleStatus {
    Ready,
    RetryWait,
    HeldPages,
    Exhausted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub schedule_id: String,
    pub policy: SchedulePolicy,
    pub next_deadline: i64,
    pub active_run: Option<String>,
    pub status: ScheduleStatus,
    pub attempts: i64,
    pub cycle: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleEventKind {
    Registered,
    RunFailure,
    HeldPages,
    Completed,
    Retry,
    Abandoned,
    Reconfigured,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleEvent {
    pub sequence: i64,
    pub cycle: i64,
    pub run_id: Option<String>,
    pub at_seconds: i64,
    pub kind: ScheduleEventKind,
    pub failure: Option<FailureClass>,
    pub attempt: i64,
    pub deadline: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureEvidence {
    pub sequence: i64,
    pub run_id: String,
    pub page_id: Option<String>,
    /// 0 legacy page, 1 legacy run, 2 authoritative page, 3 authoritative run.
    pub kind: i64,
    /// Sanitized enum encoding, never an upstream error or source body.
    pub class: String,
}
fn external(coordinator: &IndexCommitCoordinator) -> Result<std::fs::File, JournalError> {
    coordinator.try_acquire().map_err(|e| match e {
        CommitError::Conflict => JournalError::Busy,
        CommitError::BindingMismatch => JournalError::ScopeMismatch,
        _ => JournalError::Unavailable,
    })
}
fn sql(_: rusqlite::Error) -> JournalError {
    JournalError::Unavailable
}
fn time(now: i64) -> Result<(), JournalError> {
    if now < 0 {
        Err(JournalError::InvalidInput)
    } else {
        Ok(())
    }
}
fn add(now: i64, delay: i64) -> Result<i64, JournalError> {
    time(now)?;
    now.checked_add(delay).ok_or(JournalError::InvalidInput)
}
fn number(failure: &FailureClass) -> i64 {
    match failure {
        FailureClass::Source => 0,
        FailureClass::Index => 1,
        FailureClass::Conflict => 2,
        FailureClass::Unavailable => 3,
    }
}
fn failure(value: Option<i64>) -> Result<Option<FailureClass>, JournalError> {
    value
        .map(|v| match v {
            0 => Ok(FailureClass::Source),
            1 => Ok(FailureClass::Index),
            2 => Ok(FailureClass::Conflict),
            3 => Ok(FailureClass::Unavailable),
            _ => Err(JournalError::Unavailable),
        })
        .transpose()
}
fn load(
    db: &Connection,
    coordinator: &IndexCommitCoordinator,
    id: &str,
) -> Result<Schedule, JournalError> {
    let binding = coordinator.binding();
    let row=db.query_row("SELECT table_name,workspace,scope,generation,epoch,interval_seconds,max_attempts,backoff_seconds,max_backoff_seconds,next_deadline,active_run,status,attempts,cycle FROM reconciliation_schedules WHERE schedule_id=?1",[id],|r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?,r.get::<_,i64>(7)?,r.get::<_,i64>(8)?,r.get::<_,i64>(9)?,r.get::<_,Option<String>>(10)?,r.get::<_,i64>(11)?,r.get::<_,i64>(12)?,r.get::<_,i64>(13)?))).optional().map_err(sql)?.ok_or(JournalError::InvalidTransition)?;
    if row.0 != binding.table()
        || row.1 != binding.workspace()
        || row.2 != binding.scope()
        || row.3 != binding.generation()
        || row.4 != binding.epoch() as i64
    {
        return Err(JournalError::ScopeMismatch);
    }
    Ok(Schedule {
        schedule_id: id.into(),
        policy: SchedulePolicy::new(row.5, row.6, row.7, row.8)?,
        next_deadline: row.9,
        active_run: row.10,
        status: match row.11 {
            0 => ScheduleStatus::Ready,
            1 => ScheduleStatus::RetryWait,
            2 => ScheduleStatus::HeldPages,
            3 => ScheduleStatus::Exhausted,
            _ => return Err(JournalError::Unavailable),
        },
        attempts: row.12,
        cycle: row.13,
    })
}
fn event(
    db: &Connection,
    s: &Schedule,
    now: i64,
    kind: i64,
    class: Option<i64>,
    id: Option<&str>,
) -> Result<(), JournalError> {
    db.execute("INSERT INTO reconciliation_schedule_events(schedule_id,cycle,run_id,at_seconds,kind,failure,attempt,deadline,event_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![s.schedule_id,s.cycle,s.active_run,now,kind,class,s.attempts,s.next_deadline,id]).map_err(sql)?;
    Ok(())
}
fn page_failures(db: &Connection, run: &str) -> Result<bool, JournalError> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_inventory WHERE run_id=?1 AND status=2) OR EXISTS(SELECT 1 FROM reconciliation_plan_progress WHERE run_id=?1 AND status=2) OR EXISTS(SELECT 1 FROM reconciliation_plan_run_failures WHERE run_id=?1) OR EXISTS(SELECT 1 FROM reconciliation_plans p, json_each(p.document,'$.planning_failures') WHERE p.run_id=?1) OR EXISTS(SELECT 1 FROM reconciliation_plans p, json_each(p.document,'$.run_failures') WHERE p.run_id=?1)",[run],|r|r.get(0)).map_err(sql)
}

impl IndexCommitCoordinator {
    fn schedule_write<T>(
        &self,
        lease: &Lease,
        now: i64,
        action: impl FnOnce(&Transaction<'_>) -> Result<T, JournalError>,
    ) -> Result<T, JournalError> {
        time(now)?;
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_transaction(|db| {
            self.validate_binding(db)
                .map_err(|_| JournalError::ScopeMismatch)?;
            guard(db, lease, now)?;
            action(db)
        })
    }
    /// Gate event admission under the common lease before consuming an inbox
    /// attempt. A checkpointed reconciliation remains its logical owner until
    /// completion or explicit recovery, even between service processes. Held
    /// or exhausted failures reserve no live admission and must not starve events.
    pub fn ensure_no_active_reconciliation(
        &self,
        lease: &Lease,
        now: i64,
    ) -> Result<(), JournalError> {
        self.schedule_write(lease, now, |db| {
            let mut statement = db.prepare("SELECT run_id FROM reconciliation_runs r WHERE phase<>2 AND abandoned=0 AND failure IS NULL AND NOT EXISTS(SELECT 1 FROM reconciliation_schedules s WHERE s.active_run=r.run_id AND s.status IN(2,3))").map_err(sql)?;
            let runs = statement.query_map([], |r|r.get::<_,String>(0)).map_err(sql)?.collect::<Result<Vec<_>,_>>().map_err(sql)?;
            for run in runs { if !page_failures(db,&run)? { return Err(JournalError::Busy); } }
            Ok(())
        })
    }
    /// Register once. Exact replay preserves deadlines, active identity and budgets.
    /// Different policy/authority is rejected; use explicit recovery/reconfiguration.
    pub fn register_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        policy: SchedulePolicy,
        first_deadline: i64,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        time(first_deadline)?;
        add(first_deadline, policy.interval_seconds)?;
        self.schedule_write(lease,now,|db| {
            let exists:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_schedules WHERE schedule_id=?1)",[id],|r|r.get(0)).map_err(sql)?;
            if exists { let s=load(db,self,id)?; if s.policy!=policy {return Err(JournalError::ScopeMismatch);} return Ok(s); }
            let b=self.binding();
            db.execute("INSERT INTO reconciliation_schedules(schedule_id,table_name,workspace,scope,generation,epoch,interval_seconds,max_attempts,backoff_seconds,max_backoff_seconds,next_deadline) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",params![id,b.table(),b.workspace(),b.scope(),b.generation(),b.epoch() as i64,policy.interval_seconds,policy.max_attempts,policy.backoff_seconds,policy.max_backoff_seconds,first_deadline]).map_err(sql)?;
            let s=load(db,self,id)?;event(db,&s,now,0,None,None)?;Ok(s)
        })
    }
    pub fn schedule(&self, id: &str) -> Result<Schedule, JournalError> {
        validate(id)?;
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db)
                .map_err(|_| JournalError::ScopeMismatch)?;
            load(db, self, id)
        })
    }
    /// Inspect due schedules for this EXACT trusted scope/generation/epoch.
    /// Selection is advisory; claim rechecks deadline/exclusivity in one transaction.
    pub fn due_schedules(&self, now: i64, limit: usize) -> Result<Vec<Schedule>, JournalError> {
        time(now)?;
        if !(1..=1000).contains(&limit) {
            return Err(JournalError::InvalidInput);
        }
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let b=self.binding();
            let mut stmt=db.prepare("SELECT schedule_id FROM reconciliation_schedules WHERE table_name=?1 AND workspace=?2 AND scope=?3 AND generation=?4 AND epoch=?5 AND status IN(0,1) AND next_deadline<=?6 ORDER BY next_deadline,schedule_id LIMIT ?7").map_err(sql)?;
            let ids=stmt.query_map(params![b.table(),b.workspace(),b.scope(),b.generation(),b.epoch() as i64,now,limit as i64],|r|r.get::<_,String>(0)).map_err(sql)?.collect::<Result<Vec<_>,_>>().map_err(sql)?;
            ids.iter().map(|id|load(db,self,id)).collect()
        })
    }
    /// `new_run` is used only for a new cycle. Restart always rebinds the stored
    /// active run to the new fence; it never creates a second inventory.
    /// Claim does not consume attempts. Caller holds the common external guard
    /// before source/index effects, exactly as manual/webhook execution does.
    pub fn claim_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        new_run: &str,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        validate(new_run)?;
        time(now)?;
        let _guard = external(self)?;
        self.schedule_write(lease,now,|db| {
            let s=load(db,self,id)?;
            if matches!(s.status,ScheduleStatus::HeldPages|ScheduleStatus::Exhausted) {return Err(JournalError::InvalidTransition);}
            if s.next_deadline>now {return Err(JournalError::InvalidTransition);}
            let active:Option<String>=db.query_row("SELECT run_id FROM reconciliation_runs WHERE phase<>2 AND abandoned=0",[],|r|r.get(0)).optional().map_err(sql)?;
            let run=s.active_run.as_deref().unwrap_or(new_run);
            if active.as_deref().is_some_and(|other|other!=run) {return Err(JournalError::Busy);}
            if s.active_run.is_some() {
                let changed=db.execute("UPDATE reconciliation_runs SET fence=?1 WHERE run_id=?2 AND scope=?3 AND phase<>2 AND abandoned=0",params![lease.fence,run,self.binding().scope()]).map_err(sql)?;
                if changed!=1 {return Err(JournalError::InvalidTransition);}
            } else {
                // No ON CONFLICT: an existing receipt/run can never be reused.
                let used:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_runs WHERE run_id=?1)",[run],|r|r.get(0)).map_err(sql)?;
                if used {return Err(JournalError::InvalidTransition);}
                db.execute("INSERT INTO reconciliation_runs(run_id,scope,fence,phase) VALUES(?1,?2,?3,0)",params![run,self.binding().scope(),lease.fence]).map_err(sql)?;
                db.execute("UPDATE reconciliation_schedules SET active_run=?1,cycle=cycle+1 WHERE schedule_id=?2",params![run,id]).map_err(sql)?;
            }
            load(db,self,id)
        })
    }
    /// A retryable run-level failure consumes ONE budget entry. Stable `failure_id`
    /// makes duplicate reporting replay-safe; changing its class is rejected.
    /// Failed page/planning work holds the cycle and is never implicitly retried.
    pub fn fail_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        failure_id: &str,
        class: FailureClass,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        validate(failure_id)?;
        self.schedule_write(lease,now,|db| {
            let s=load(db,self,id)?;
            let run=s.active_run.as_deref().ok_or(JournalError::InvalidTransition)?;
            let old:Option<i64>=db.query_row("SELECT failure FROM reconciliation_schedule_events WHERE schedule_id=?1 AND cycle=?2 AND event_id=?3",params![id,s.cycle,failure_id],|r|r.get(0)).optional().map_err(sql)?;
            if let Some(old)=old {if old!=number(&class){return Err(JournalError::InvalidTransition);}return Ok(s);}
            if matches!(s.status,ScheduleStatus::HeldPages|ScheduleStatus::Exhausted) {return Err(JournalError::InvalidTransition);}
            let owned:bool=db.query_row("SELECT fence=?1 AND abandoned=0 AND phase<>2 FROM reconciliation_runs WHERE run_id=?2",params![lease.fence,run],|r|r.get(0)).map_err(sql)?;
            if !owned {return Err(JournalError::InvalidTransition);}
            if s.next_deadline>now {return Err(JournalError::InvalidTransition);}
            let held=page_failures(db,run)?;
            let attempts=if held{s.attempts}else{s.attempts+1};
            let status=if held{2}else if attempts>=s.policy.max_attempts{3}else{1};
            let delay=s.policy.backoff_seconds.saturating_mul(1_i64 << (attempts.max(1)-1)).min(s.policy.max_backoff_seconds);
            let deadline=if held{s.next_deadline}else{add(now,delay)?};
            db.execute("UPDATE reconciliation_schedules SET status=?1,attempts=?2,next_deadline=?3 WHERE schedule_id=?4",params![status,attempts,deadline,id]).map_err(sql)?;
            db.execute("UPDATE reconciliation_runs SET next_deadline=?1 WHERE run_id=?2",params![deadline,run]).map_err(sql)?;
            let s=load(db,self,id)?;event(db,&s,now,if held{2}else{1},Some(number(&class)),Some(failure_id))?;Ok(s)
        })
    }
    /// Explicitly hold detected failed pages WITHOUT consuming run-failure budget.
    pub fn hold_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        self.schedule_write(lease, now, |db| {
            let s = load(db, self, id)?;
            let run = s
                .active_run
                .as_deref()
                .ok_or(JournalError::InvalidTransition)?;
            if !page_failures(db, run)? {
                return Err(JournalError::InvalidTransition);
            }
            if s.status == ScheduleStatus::HeldPages {
                return Ok(s);
            }
            db.execute(
                "UPDATE reconciliation_schedules SET status=2 WHERE schedule_id=?1",
                [id],
            )
            .map_err(sql)?;
            let s = load(db, self, id)?;
            event(db, &s, now, 2, None, None)?;
            Ok(s)
        })
    }
    /// Cancellation checkpoints the same run/deadline without consuming a budget.
    /// Inventory/page checkpoints remain owned by the real runner transactions.
    pub fn checkpoint_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        self.schedule_write(lease, now, |db| {
            let s = load(db, self, id)?;
            if s.active_run.is_none() {
                return Err(JournalError::InvalidTransition);
            }
            event(db, &s, now, 7, None, None)?;
            Ok(s)
        })
    }
    /// Explicit operator recovery AFTER failed-page retry. Never resets failed work.
    /// A completed/abandoned run requires `abandon_schedule` + a fresh cycle.
    pub fn retry_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        deadline: i64,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        time(deadline)?;
        self.schedule_write(lease,now,|db| {
            let s=load(db,self,id)?;let run=s.active_run.as_deref().ok_or(JournalError::InvalidTransition)?;
            if page_failures(db,run)? {return Err(JournalError::InvalidTransition);}
            let live:bool=db.query_row("SELECT phase<>2 AND abandoned=0 FROM reconciliation_runs WHERE run_id=?1",[run],|r|r.get(0)).map_err(sql)?;
            if !live {return Err(JournalError::InvalidTransition);}
            db.execute("UPDATE reconciliation_schedules SET status=0,attempts=0,next_deadline=?1 WHERE schedule_id=?2",params![deadline,id]).map_err(sql)?;
            db.execute("UPDATE reconciliation_runs SET next_deadline=?1 WHERE run_id=?2",params![deadline,run]).map_err(sql)?;
            let s=load(db,self,id)?;event(db,&s,now,4,None,None)?;Ok(s)
        })
    }
    /// Stop a failed/incomplete cycle explicitly. Preserve every inventory row,
    /// preparation, failure history and completed receipt. A pending submitted
    /// receipt must first be recovered; abandoning it returns Busy. This consumes
    /// the lease: acquire a new fence before starting work or changing authority.
    pub fn abandon_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        next_deadline: i64,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        time(next_deadline)?;
        time(now)?;
        let _guard = external(self)?;
        self.schedule_write(lease,now,|db| {
            let s=load(db,self,id)?;let run=s.active_run.as_deref().ok_or(JournalError::InvalidTransition)?;
            let pending:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_plan_progress p JOIN index_commit_receipts r ON r.operation_id=p.operation_id WHERE p.run_id=?1 AND r.status=0)",[run],|r|r.get(0)).map_err(sql)?;
            if pending {return Err(JournalError::Busy);}
            let owner:Option<String>=db.query_row("SELECT run_id FROM reconciliation_runs WHERE phase<>2 AND abandoned=0",[],|r|r.get(0)).optional().map_err(sql)?;
            if owner.as_deref().is_some_and(|v|v!=run){return Err(JournalError::Busy);}
            event(db,&s,now,5,None,None)?;
            db.execute("UPDATE reconciliation_runs SET abandoned=1 WHERE run_id=?1 AND phase<>2",[run]).map_err(sql)?;
            db.execute("UPDATE reconciliation_schedules SET active_run=NULL,status=0,attempts=0,next_deadline=?1 WHERE schedule_id=?2",params![next_deadline,id]).map_err(sql)?;
            // Revoke queued callers still holding the old token. The external
            // lock proves no owned effect is currently running; reacquisition
            // must advance the fence before any new cycle or binding change.
            db.execute("UPDATE reconciliation_lease SET expires_at=0 WHERE singleton=1",[]).map_err(sql)?;
            load(db,self,id)
        })
    }
    /// Change interval/retry policy explicitly between cycles. Authority changes
    /// require recovery under the OLD binding first, then explicit replacement
    /// through the newly trusted binding with a matching previous scope/epoch.
    pub fn reconfigure_schedule(
        &self,
        lease: &Lease,
        now: i64,
        id: &str,
        change: &ScheduleReconfiguration,
    ) -> Result<Schedule, JournalError> {
        validate(id)?;
        let previous = &change.previous_binding;
        let policy = change.policy;
        let deadline = change.next_deadline;
        time(deadline)?;
        add(deadline, policy.interval_seconds)?;
        time(now)?;
        let _guard = external(self)?;
        self.schedule_write(lease,now,|db| {
            let old:(String,String,String,String,i64,Option<String>)=db.query_row("SELECT table_name,workspace,scope,generation,epoch,active_run FROM reconciliation_schedules WHERE schedule_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional().map_err(sql)?.ok_or(JournalError::InvalidTransition)?;
            if old.0!=previous.table() || old.1!=previous.workspace() || old.2!=previous.scope() || old.3!=previous.generation() || old.4!=previous.epoch() as i64 {return Err(JournalError::ScopeMismatch);}
            let active:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM reconciliation_runs WHERE phase<>2 AND abandoned=0)",[],|r|r.get(0)).map_err(sql)?;
            if old.5.is_some() || active {return Err(JournalError::Busy);}
            let b=self.binding();
            db.execute("UPDATE reconciliation_schedules SET table_name=?1,workspace=?2,scope=?3,generation=?4,epoch=?5,interval_seconds=?6,max_attempts=?7,backoff_seconds=?8,max_backoff_seconds=?9,next_deadline=?10,status=0,attempts=0 WHERE schedule_id=?11",params![b.table(),b.workspace(),b.scope(),b.generation(),b.epoch() as i64,policy.interval_seconds,policy.max_attempts,policy.backoff_seconds,policy.max_backoff_seconds,deadline,id]).map_err(sql)?;
            let s=load(db,self,id)?;event(db,&s,now,6,None,None)?;Ok(s)
        })
    }
    /// Bounded paginated immutable scheduling evidence for operator inspection.
    pub fn schedule_events(
        &self,
        id: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<ScheduleEvent>, JournalError> {
        validate(id)?;
        time(after)?;
        if !(1..=1000).contains(&limit) {
            return Err(JournalError::InvalidInput);
        }
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;load(db,self,id)?;
            let mut stmt=db.prepare("SELECT sequence,cycle,run_id,at_seconds,kind,failure,attempt,deadline FROM reconciliation_schedule_events WHERE schedule_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3").map_err(sql)?;
            stmt.query_map(params![id,after,limit as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get::<_,i64>(4)?,r.get(5)?,r.get(6)?,r.get(7)?))).map_err(sql)?.map(|row|{let(a,b,c,d,e,f,g,h)=row.map_err(sql)?;Ok(ScheduleEvent{sequence:a,cycle:b,run_id:c,at_seconds:d,kind:match e{0=>ScheduleEventKind::Registered,1=>ScheduleEventKind::RunFailure,2=>ScheduleEventKind::HeldPages,3=>ScheduleEventKind::Completed,4=>ScheduleEventKind::Retry,5=>ScheduleEventKind::Abandoned,6=>ScheduleEventKind::Reconfigured,7=>ScheduleEventKind::Cancelled,_=>return Err(JournalError::Unavailable)},failure:failure(f)?,attempt:g,deadline:h})}).collect()
        })
    }
    pub fn reconciliation_failure_evidence(
        &self,
        run: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<FailureEvidence>, JournalError> {
        validate(run)?;
        time(after)?;
        if !(1..=1000).contains(&limit) {
            return Err(JournalError::InvalidInput);
        }
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let scope:Option<String>=db.query_row("SELECT scope FROM reconciliation_runs WHERE run_id=?1",[run],|r|r.get(0)).optional().map_err(sql)?;
            if scope.as_deref()!=Some(self.binding().scope()){return Err(JournalError::ScopeMismatch);}
            let mut stmt=db.prepare("SELECT sequence,run_id,page_id,kind,class FROM reconciliation_failure_history WHERE run_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3").map_err(sql)?;
            stmt.query_map(params![run,after,limit as i64],|r|Ok(FailureEvidence{sequence:r.get(0)?,run_id:r.get(1)?,page_id:r.get(2)?,kind:r.get(3)?,class:r.get(4)?})).map_err(sql)?.collect::<Result<Vec<_>,_>>().map_err(sql)
        })
    }
}
/// Called INSIDE the runner/manual completion transaction. Crash cannot separate
/// terminal run acknowledgment from deadline progression. Failed plans are held.
pub(crate) fn completed(db: &Connection, now: i64, run: &str) -> Result<(), JournalError> {
    let schedule:Option<(String,i64,i64,i64,i64)>=db.query_row("SELECT schedule_id,interval_seconds,cycle,attempts,next_deadline FROM reconciliation_schedules WHERE active_run=?1",[run],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(sql)?;
    let Some((id, interval, cycle, attempts, old_deadline)) = schedule else {
        return Ok(());
    };
    let held = page_failures(db, run)?;
    let deadline = if held {
        old_deadline
    } else {
        add(now, interval)?
    };
    db.execute("INSERT INTO reconciliation_schedule_events(schedule_id,cycle,run_id,at_seconds,kind,attempt,deadline) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![id,cycle,run,now,if held{2}else{3},attempts,deadline]).map_err(sql)?;
    db.execute("UPDATE reconciliation_schedules SET active_run=CASE WHEN ?1 THEN active_run ELSE NULL END,status=CASE WHEN ?1 THEN 2 ELSE 0 END,attempts=CASE WHEN ?1 THEN attempts ELSE 0 END,next_deadline=?2 WHERE schedule_id=?3",params![held,deadline,id]).map_err(sql)?;
    db.execute(
        "UPDATE reconciliation_runs SET next_deadline=?1 WHERE run_id=?2",
        params![deadline, run],
    )
    .map_err(sql)?;
    Ok(())
}
