//! Versioned, payload-free reconciliation plans and actual receipt accounting.
//! A loaded digest is evidence to compare with fresh reads, never source authority.
use crate::{
    commit::{CommitBinding, IndexCommitCoordinator, PageAction, PageOperation},
    planner::{Difference, PlanError, ReconciliationPlan},
    reconciliation::{guard, phase},
};
use notion_knowledge_core::reconciliation::{JournalError, Lease};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

fn sql(_: rusqlite::Error) -> JournalError {
    JournalError::Unavailable
}
fn digest(value: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(value))
}
fn encode<T: Serialize>(value: &T) -> Result<String, JournalError> {
    serde_json::to_string(value).map_err(|_| JournalError::InvalidInput)
}
fn decode<T: for<'a> Deserialize<'a>>(value: &str) -> Result<T, JournalError> {
    serde_json::from_str(value).map_err(|_| JournalError::Unavailable)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableFailure {
    Ownership,
    Index,
    Source,
    Permission,
    MissingSource,
    Unauthenticated,
    UnsupportedSource,
    Conflict,
    IncompleteTraversal,
    TraversalSource,
    TraversalPermission,
    TraversalMissingSource,
    TraversalUnauthenticated,
    TraversalUnsupportedSource,
    TraversalConflict,
}
impl From<PlanError> for DurableFailure {
    fn from(value: PlanError) -> Self {
        use notion_knowledge_core::backend::BackendErrorKind as B;
        match value {
            PlanError::Ownership => Self::Ownership,
            PlanError::Index => Self::Index,
            PlanError::Source => Self::Source,
            PlanError::Permission => Self::Permission,
            PlanError::MissingSource => Self::MissingSource,
            PlanError::Unauthenticated => Self::Unauthenticated,
            PlanError::UnsupportedSource => Self::UnsupportedSource,
            PlanError::Conflict => Self::Conflict,
            PlanError::IncompleteTraversal => Self::IncompleteTraversal,
            PlanError::Traversal(kind) => match kind {
                B::PermissionDenied => Self::TraversalPermission,
                B::NotFound => Self::TraversalMissingSource,
                B::Unauthenticated => Self::TraversalUnauthenticated,
                B::UnsupportedContent | B::InvalidInput => Self::TraversalUnsupportedSource,
                B::Conflict => Self::TraversalConflict,
                _ => Self::TraversalSource,
            },
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposedDifference {
    Missing,
    Stale,
    Restored,
    Unchanged,
    Orphan,
}
impl From<Difference> for ProposedDifference {
    fn from(value: Difference) -> Self {
        match value {
            Difference::Missing => Self::Missing,
            Difference::Stale => Self::Stale,
            Difference::Restored => Self::Restored,
            Difference::Unchanged => Self::Unchanged,
            Difference::Orphan => Self::Orphan,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePage {
    pub page_id: String,
    pub operation_id: String,
    pub difference: ProposedDifference,
    /// Hash of complete lifecycle evidence; compare to a fresh authoritative read.
    pub lifecycle_digest: String,
    pub metadata_identity: Option<String>,
    /// Hash of the planner's complete local checkpoint/chunk snapshot.
    pub local_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Authority {
    table: String,
    workspace: String,
    scope: String,
    generation: String,
    epoch: u64,
}
impl From<&CommitBinding> for Authority {
    fn from(b: &CommitBinding) -> Self {
        Self {
            table: b.table().into(),
            workspace: b.workspace().into(),
            scope: b.scope().into(),
            generation: b.generation().into(),
            epoch: b.epoch(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePlan {
    version: u32,
    run_id: String,
    authority: Authority,
    pub index_version: u64,
    pub traversal_complete: bool,
    pub inspected_ids: BTreeSet<String>,
    pub pages: BTreeMap<String, DurablePage>,
    pub planning_failures: BTreeMap<String, DurableFailure>,
    pub run_failures: Vec<DurableFailure>,
}
impl DurablePlan {
    pub fn from_planner(run_id: &str, plan: &ReconciliationPlan) -> Result<Self, JournalError> {
        notion_knowledge_core::reconciliation::validate(run_id)?;
        if plan.scanned != plan.inspected_ids.len() {
            return Err(JournalError::InvalidInput);
        }
        let authority = Authority::from(&plan.binding);
        let mut pages = BTreeMap::new();
        for p in &plan.pages {
            if !plan.inspected_ids.contains(&p.page_id)
                || pages.contains_key(&p.page_id)
                || (p.difference == Difference::Orphan && !plan.traversal_complete)
            {
                return Err(JournalError::InvalidInput);
            }
            let lifecycle_digest = Self::lifecycle_digest(&p.evidence);
            let local_digest = Self::local_digest(plan.local.get(&p.page_id));
            let difference = p.difference.into();
            // Length-delimited JSON encodes every operation identity input.
            let operation_id = format!(
                "reconcile-v1-{}",
                digest(encode(&(
                    run_id,
                    &authority,
                    &p.page_id,
                    difference,
                    &lifecycle_digest,
                    &p.metadata_identity,
                    &local_digest
                ))?)
            );
            pages.insert(
                p.page_id.clone(),
                DurablePage {
                    page_id: p.page_id.clone(),
                    operation_id,
                    difference,
                    lifecycle_digest,
                    metadata_identity: p.metadata_identity.clone(),
                    local_digest,
                },
            );
        }
        Ok(Self {
            version: 1,
            run_id: run_id.into(),
            authority,
            index_version: plan.index_version,
            traversal_complete: plan.traversal_complete,
            inspected_ids: plan.inspected_ids.clone(),
            pages,
            planning_failures: plan
                .errors
                .iter()
                .map(|(id, e)| (id.clone(), (*e).into()))
                .collect(),
            run_failures: plan.run_errors.iter().copied().map(Into::into).collect(),
        })
    }
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn lifecycle_digest(
        evidence: &notion_knowledge_core::lifecycle::LifecycleEvidence,
    ) -> String {
        digest(format!("lifecycle-v1:{evidence:?}"))
    }
    pub fn local_digest(local: Option<&crate::planner::LocalPage>) -> String {
        digest(format!("local-v1:{local:?}"))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActualOutcome {
    Refreshed,
    Deleted,
    Unchanged,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableProgress {
    pub scanned: u64,
    pub planned_refresh: u64,
    pub planned_delete: u64,
    pub changed: u64,
    pub deleted: u64,
    pub unchanged: u64,
    pub pending: BTreeSet<String>,
    pub successful: BTreeMap<String, ActualOutcome>,
    pub failed: BTreeMap<String, DurableFailure>,
    pub planning_failures: BTreeMap<String, DurableFailure>,
    pub run_failures: BTreeMap<String, DurableFailure>,
}
impl DurableProgress {
    /// Distinct page failures; a planning and application failure never doubles a page.
    pub fn failed_pages(&self) -> usize {
        self.failed
            .keys()
            .chain(self.planning_failures.keys())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedOperation {
    revision: String,
    action: i64,
    content_hash: Option<String>,
    edited: Option<String>,
}
type ReceiptRow = (String, i64, Option<String>, Option<String>);
type StatusReceiptRow = (i64, String, i64, Option<String>, Option<String>);
impl PreparedOperation {
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn action(&self) -> Result<ActualOutcome, JournalError> {
        outcome(self.action)
    }
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
    pub fn last_edited_time(&self) -> Option<&str> {
        self.edited.as_deref()
    }
    fn from(operation: &PageOperation) -> Self {
        Self {
            revision: operation.revision().into(),
            action: match operation.action() {
                PageAction::Refresh => 0,
                PageAction::Delete => 1,
                PageAction::Unchanged => 2,
            },
            content_hash: operation.checkpoint().content_hash().map(Into::into),
            edited: operation.checkpoint().last_edited_time().map(Into::into),
        }
    }
}

impl IndexCommitCoordinator {
    /// Record the complete planner contract and seal inventory in ONE transaction.
    /// Exact duplicate recording is harmless in any phase; conflicting replans
    /// require a new run, preserving all old successes and failures.
    pub fn record_reconciliation_plan(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        source: &ReconciliationPlan,
    ) -> Result<(), JournalError> {
        let plan = DurablePlan::from_planner(run, source)?;
        if plan.version != 1 || plan.authority != Authority::from(self.binding()) {
            return Err(JournalError::ScopeMismatch);
        }
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        let document = encode(&plan)?;
        self.state().journal_transaction(|db| {
            guard(db,lease,now)?;
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let old:Option<String> = db.query_row("SELECT document FROM reconciliation_plans WHERE run_id=?1",[&plan.run_id],|r|r.get(0)).optional().map_err(sql)?;
            if let Some(old)=old { return if old==document { phase_any(db,lease,&plan.run_id) } else { Err(JournalError::InvalidTransition) }; }
            phase(db,lease,&plan.run_id,0)?;
            let scope:String=db.query_row("SELECT scope FROM reconciliation_runs WHERE run_id=?1",[&plan.run_id],|r|r.get(0)).map_err(sql)?;
            if scope != plan.authority.scope {return Err(JournalError::ScopeMismatch);}
            let legacy:i64=db.query_row("SELECT COUNT(*) FROM reconciliation_inventory WHERE run_id=?1",[&plan.run_id],|r|r.get(0)).map_err(sql)?;
            if legacy!=0 {return Err(JournalError::InvalidTransition);}
            db.execute("INSERT INTO reconciliation_plans VALUES(?1,1,?2)",params![plan.run_id,document]).map_err(sql)?;
            for p in plan.pages.values() {
                db.execute("INSERT INTO reconciliation_plan_progress(run_id,page_id,operation_id) VALUES(?1,?2,?3)",params![plan.run_id,p.page_id,p.operation_id]).map_err(sql)?;
            }
            for (i,failure) in plan.run_failures.iter().enumerate() {
                db.execute("INSERT INTO reconciliation_plan_run_failures VALUES(?1,?2,?3)",params![plan.run_id,format!("planning-{i}"),encode(failure)?]).map_err(sql)?;
            }
            db.execute("UPDATE reconciliation_runs SET phase=1 WHERE run_id=?1",[&plan.run_id]).map_err(sql)?;
            Ok(())
        })
    }
    /// Reopen with current trusted local authority, never reconstruct authority
    /// from persisted plan bytes. Loaded evidence must be authoritatively revalidated.
    pub fn reconciliation_plan(&self, run: &str) -> Result<DurablePlan, JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db)
                .map_err(|_| JournalError::ScopeMismatch)?;
            load(db, run, self.binding())
        })
    }
    /// The exact persisted receipt target survives process restart. This is
    /// operational provenance, not a fresh source authorization.
    pub fn prepared_reconciliation_operation(
        &self,
        run: &str,
        page: &str,
    ) -> Result<Option<PreparedOperation>, JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            load(db,run,self.binding())?;
            let prepared:Option<String>=db.query_row("SELECT prepared FROM reconciliation_plan_progress WHERE run_id=?1 AND page_id=?2",params![run,page],|r|r.get(0)).optional().map_err(sql)?.ok_or(JournalError::InvalidInput)?;
            prepared.map(|value|decode(&value)).transpose()
        })
    }
    /// Bind the runner's freshly authorized operation BEFORE submitting it.
    /// Fresh source reads and local conflict checks must happen inside the owned
    /// commit boundary. Changed lifecycle/action requires an explicit replan;
    /// recording a decision is not authorization to apply it outside that guard.
    pub fn prepare_reconciliation_operation(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        operation: &PageOperation,
        evidence: &notion_knowledge_core::lifecycle::LifecycleEvidence,
        replan: bool,
    ) -> Result<(), JournalError> {
        use notion_knowledge_core::lifecycle::LifecycleStatus;
        self.progress_transition(lease,now,run,|db| {
            let plan=load(db,run,self.binding())?;
            let p=plan.pages.get(operation.checkpoint().page_id()).ok_or(JournalError::InvalidInput)?;
            if p.operation_id!=operation.id()
                || !self.binding().matches_policy(&evidence.scope.workspace_id,
                    &crate::planner::production_scope(&evidence.scope).map_err(|_|JournalError::ScopeMismatch)?)
                {return Err(JournalError::ScopeMismatch);}
            let allowed=matches!(evidence.status,LifecycleStatus::Allowed{..});
            if (operation.action()==PageAction::Delete)==allowed
                || (operation.action()==PageAction::Delete && !plan.traversal_complete) {
                return Err(JournalError::InvalidTransition);
            }
            let expected=match p.difference { ProposedDifference::Orphan=>PageAction::Delete,ProposedDifference::Unchanged=>PageAction::Unchanged,_=>PageAction::Refresh };
            if !replan && (operation.action()!=expected || DurablePlan::lifecycle_digest(evidence)!=p.lifecycle_digest) {return Err(JournalError::InvalidTransition);}
            let (status,original):(i64,Option<String>)=db.query_row("SELECT status,prepared FROM reconciliation_plan_progress WHERE run_id=?1 AND page_id=?2",params![run,p.page_id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
            if status!=0 {return Err(JournalError::InvalidTransition);}
            let prepared=PreparedOperation::from(operation);
            // Once a real commit exists, preparation cannot be rewritten to
            // reinterpret that already-applied effect after a crash.
            let receipt:Option<StatusReceiptRow>=db.query_row("SELECT status,revision,action,content_hash,last_edited_time FROM index_commit_receipts WHERE operation_id=?1",[operation.id()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(sql)?;
            if let Some((status,revision,action,content_hash,edited))=receipt
                && status==1 && (prepared!= (PreparedOperation{revision,action,content_hash,edited}) || original.as_deref()!=Some(&encode(&prepared)?)) {return Err(JournalError::InvalidTransition);}
            db.execute("UPDATE reconciliation_plan_progress SET prepared=?1 WHERE run_id=?2 AND page_id=?3",params![encode(&prepared)?,run,p.page_id]).map_err(sql)?;
            Ok(())
        })
    }
    /// Read the real Applied commit receipt in the same transaction as acknowledgment.
    /// Call only AFTER the owned effect/receipt completes. Pending, failed,
    /// superseded, wrong-page or wrong-generation receipts cannot acknowledge work.
    /// The actual action may differ from the proposal after guarded revalidation.
    pub fn acknowledge_reconciliation_receipt(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        page: &str,
    ) -> Result<ActualOutcome, JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_transaction(|db| {
            guard(db,lease,now)?; phase(db,lease,run,1)?;
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let plan=load(db,run,self.binding())?;
            let p=plan.pages.get(page).ok_or(JournalError::InvalidInput)?;
            let (status,old):(i64,Option<i64>)=db.query_row("SELECT status,outcome FROM reconciliation_plan_progress WHERE run_id=?1 AND page_id=?2",params![run,page],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
            if status==2 {return Err(JournalError::InvalidTransition);}
            let prepared:Option<String>=db.query_row("SELECT prepared FROM reconciliation_plan_progress WHERE run_id=?1 AND page_id=?2",params![run,page],|r|r.get(0)).map_err(sql)?;
            let prepared:PreparedOperation=decode(&prepared.ok_or(JournalError::InvalidTransition)?)?;
            let receipt:Option<ReceiptRow>=db.query_row("SELECT revision,action,content_hash,last_edited_time FROM index_commit_receipts WHERE operation_id=?1 AND page_id=?2 AND generation=?3 AND status=1",params![p.operation_id,page,self.binding().generation()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(sql)?;
            let (revision,action,content_hash,edited)=receipt.ok_or(JournalError::InvalidTransition)?;
            if prepared!= (PreparedOperation{revision,action,content_hash,edited}) {return Err(JournalError::InvalidTransition);}
            let projected:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM relation_operation_proposals WHERE operation_id=?1)",[&p.operation_id],|r|r.get(0)).map_err(sql)?;
            if projected && !crate::commit::projection::current_complete(db,self.binding(),page).map_err(|_|JournalError::Unavailable)? {return Err(JournalError::InvalidTransition);}
            let outcome=outcome(action)?;
            if old.is_some_and(|old|old!=action) {return Err(JournalError::InvalidTransition);}
            db.execute("UPDATE reconciliation_plan_progress SET status=1,outcome=?1,failure=NULL WHERE run_id=?2 AND page_id=?3",params![action,run,page]).map_err(sql)?;
            Ok(outcome)
        })
    }
    pub fn fail_reconciliation_page(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        page: &str,
        failure: DurableFailure,
    ) -> Result<(), JournalError> {
        self.progress_transition(lease,now,run,|db| {
            let old:(i64,Option<String>)=db.query_row("SELECT status,failure FROM reconciliation_plan_progress WHERE run_id=?1 AND page_id=?2",params![run,page],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql)?.ok_or(JournalError::InvalidInput)?;
            let class=encode(&failure)?;
            if old.0==1 || (old.0==2 && old.1.as_deref()!=Some(&class)) {return Err(JournalError::InvalidTransition);}
            db.execute("UPDATE reconciliation_plan_progress SET status=2,failure=?1 WHERE run_id=?2 AND page_id=?3",params![class,run,page]).map_err(sql)?;
            Ok(())
        })
    }
    pub fn retry_reconciliation_page(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        page: &str,
    ) -> Result<(), JournalError> {
        self.progress_transition(lease,now,run,|db| {
            if db.execute("UPDATE reconciliation_plan_progress SET status=0,failure=NULL WHERE run_id=?1 AND page_id=?2 AND status=2",params![run,page]).map_err(sql)? !=1 {return Err(JournalError::InvalidTransition);}
            Ok(())
        })
    }
    /// Persist the explicit fresh retry proposal and clear Failed atomically.
    /// These bytes never authorize effects: the runner rechecks all evidence
    /// under the same owned source/lease/index boundary after every restart.
    pub fn retry_reconciliation_proposal(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        proposal: &DurablePage,
    ) -> Result<(), JournalError> {
        self.progress_transition(lease, now, run, |db| {
            let plan = load(db, run, self.binding())?;
            let original = plan.pages.get(&proposal.page_id).ok_or(JournalError::InvalidInput)?;
            if original.operation_id != proposal.operation_id {
                return Err(JournalError::InvalidInput);
            }
            if db.execute("UPDATE reconciliation_plan_progress SET status=0,failure=NULL WHERE run_id=?1 AND page_id=?2 AND status=2",params![run,proposal.page_id]).map_err(sql)? != 1 {
                return Err(JournalError::InvalidTransition);
            }
            db.execute("INSERT INTO reconciliation_retry_proposals VALUES(?1,?2,?3) ON CONFLICT(run_id,page_id) DO UPDATE SET proposal=excluded.proposal",params![run,proposal.page_id,encode(proposal)?]).map_err(sql)?;
            Ok(())
        })
    }
    pub fn reconciliation_page_proposal(
        &self,
        run: &str,
        page: &str,
    ) -> Result<(DurablePage, bool), JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let plan=load(db,run,self.binding())?;
            let original=plan.pages.get(page).ok_or(JournalError::InvalidInput)?;
            let stored: Option<String>=db.query_row("SELECT proposal FROM reconciliation_retry_proposals WHERE run_id=?1 AND page_id=?2",params![run,page],|r|r.get(0)).optional().map_err(sql)?;
            match stored {
                None => Ok((original.clone(),false)),
                Some(stored) => {
                    let proposal:DurablePage=decode(&stored)?;
                    if proposal.page_id!=page || proposal.operation_id!=original.operation_id {return Err(JournalError::InvalidInput);}
                    Ok((proposal,true))
                }
            }
        })
    }
    pub fn fail_reconciliation_run(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        failure_id: &str,
        failure: DurableFailure,
    ) -> Result<(), JournalError> {
        notion_knowledge_core::reconciliation::validate(failure_id)?;
        self.progress_transition(lease,now,run,|db| {
            let class=encode(&failure)?;
            let old:Option<String>=db.query_row("SELECT class FROM reconciliation_plan_run_failures WHERE run_id=?1 AND failure_id=?2",params![run,failure_id],|r|r.get(0)).optional().map_err(sql)?;
            if old.is_some_and(|old|old!=class) {return Err(JournalError::InvalidTransition);}
            db.execute("INSERT OR IGNORE INTO reconciliation_plan_run_failures VALUES(?1,?2,?3)",params![run,failure_id,class]).map_err(sql)?;
            Ok(())
        })
    }
    fn progress_transition<T>(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
        action: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, JournalError>,
    ) -> Result<T, JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_transaction(|db| {
            guard(db, lease, now)?;
            phase(db, lease, run, 1)?;
            self.validate_binding(db)
                .map_err(|_| JournalError::ScopeMismatch)?;
            load(db, run, self.binding())?;
            action(db)
        })
    }
    /// Finish with failed items retained. They require explicit retry BEFORE
    /// completion; after completion a new run replans against fresh authority.
    pub fn complete_reconciliation_plan(
        &self,
        lease: &Lease,
        now: i64,
        run: &str,
    ) -> Result<(), JournalError> {
        self.progress_transition(lease,now,run,|db| {
            let pending:i64=db.query_row("SELECT COUNT(*) FROM reconciliation_plan_progress WHERE run_id=?1 AND status=0",[run],|r|r.get(0)).map_err(sql)?;
            if pending!=0 {return Err(JournalError::InvalidTransition);}
            let plan=load(db,run,self.binding())?;
            let failed:i64=db.query_row("SELECT (SELECT COUNT(*) FROM reconciliation_plan_progress WHERE run_id=?1 AND status=2)+(SELECT COUNT(*) FROM reconciliation_plan_run_failures WHERE run_id=?1)",[run],|r|r.get(0)).map_err(sql)?;
            if plan.traversal_complete && plan.planning_failures.is_empty() && plan.run_failures.is_empty() && failed==0 {
                let b=self.binding();
                db.execute("INSERT INTO reconciliation_success(run_id,table_name,workspace,scope,generation,epoch,completed_at_seconds) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![run,b.table(),b.workspace(),b.scope(),b.generation(),b.epoch() as i64,now]).map_err(sql)?;
            }
            db.execute("UPDATE reconciliation_runs SET phase=2 WHERE run_id=?1",[run]).map_err(sql)?;crate::schedule::completed(db,now,run)?;Ok(())
        })
    }
    pub fn reconciliation_progress(&self, run: &str) -> Result<DurableProgress, JournalError> {
        self.identities().map_err(|_| JournalError::ScopeMismatch)?;
        self.state().journal_read(|db| {
            self.validate_binding(db).map_err(|_|JournalError::ScopeMismatch)?;
            let p=load(db,run,self.binding())?;
            let mut result=DurableProgress {scanned:p.inspected_ids.len() as u64,planned_refresh:p.pages.values().filter(|p|matches!(p.difference,ProposedDifference::Missing|ProposedDifference::Stale|ProposedDifference::Restored)).count() as u64,planned_delete:p.pages.values().filter(|p|p.difference==ProposedDifference::Orphan).count() as u64,changed:0,deleted:0,unchanged:0,pending:BTreeSet::new(),successful:BTreeMap::new(),failed:BTreeMap::new(),planning_failures:p.planning_failures,run_failures:BTreeMap::new()};
            let mut stmt=db.prepare("SELECT page_id,status,outcome,failure FROM reconciliation_plan_progress WHERE run_id=?1 ORDER BY page_id").map_err(sql)?;
            let rows=stmt.query_map([run],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,Option<String>>(3)?))).map_err(sql)?;
            for row in rows { let (id,status,actual,failure)=row.map_err(sql)?; match status {0=>{result.pending.insert(id);},1=>{let actual=outcome(actual.ok_or(JournalError::Unavailable)?)?;match actual {ActualOutcome::Refreshed=>result.changed+=1,ActualOutcome::Deleted=>result.deleted+=1,ActualOutcome::Unchanged=>result.unchanged+=1};result.successful.insert(id,actual);},2=>{result.failed.insert(id,decode(&failure.ok_or(JournalError::Unavailable)?)?);},_=>return Err(JournalError::Unavailable)} }
            let mut stmt=db.prepare("SELECT failure_id,class FROM reconciliation_plan_run_failures WHERE run_id=?1 ORDER BY failure_id").map_err(sql)?;
            for row in stmt.query_map([run],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(sql)? { let (id,class)=row.map_err(sql)?;result.run_failures.insert(id,decode(&class)?); }
            Ok(result)
        })
    }
}
fn load(
    db: &rusqlite::Connection,
    run: &str,
    binding: &CommitBinding,
) -> Result<DurablePlan, JournalError> {
    let document: String = db
        .query_row(
            "SELECT document FROM reconciliation_plans WHERE run_id=?1",
            [run],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?
        .ok_or(JournalError::InvalidTransition)?;
    let plan: DurablePlan = decode(&document)?;
    if plan.version != 1 || plan.authority != Authority::from(binding) || plan.run_id != run {
        return Err(JournalError::ScopeMismatch);
    }
    Ok(plan)
}
fn phase_any(db: &rusqlite::Connection, lease: &Lease, run: &str) -> Result<(), JournalError> {
    let fence: i64 = db
        .query_row(
            "SELECT fence FROM reconciliation_runs WHERE run_id=?1",
            [run],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if fence == lease.fence {
        Ok(())
    } else {
        Err(JournalError::InvalidTransition)
    }
}
fn outcome(value: i64) -> Result<ActualOutcome, JournalError> {
    match value {
        0 => Ok(ActualOutcome::Refreshed),
        1 => Ok(ActualOutcome::Deleted),
        2 => Ok(ActualOutcome::Unchanged),
        _ => Err(JournalError::Unavailable),
    }
}
