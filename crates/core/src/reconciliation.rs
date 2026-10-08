//! Durable reconciliation orchestration, independent of database and source adapters.
use crate::sync_state::validate_identifier;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalError {
    InvalidInput,
    Unavailable,
    ScopeMismatch,
    Busy,
    LeaseLost,
    InvalidTransition,
}
impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reconciliation journal failed: {self:?}")
    }
}
impl std::error::Error for JournalError {}

/// Roots and exclusions are sets of canonical source IDs. Policy and generation
/// are opaque, caller-owned version identities, not source bodies or credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationScope {
    identity: String,
}
impl ReconciliationScope {
    pub fn new(
        mut roots: Vec<String>,
        mut exclusions: Vec<String>,
        policy: &str,
        generation: &str,
    ) -> Result<Self, JournalError> {
        if roots.is_empty() {
            return Err(JournalError::InvalidInput);
        }
        for value in roots.iter_mut().chain(exclusions.iter_mut()) {
            *value = value.trim().to_owned();
        }
        for value in roots
            .iter()
            .chain(exclusions.iter())
            .map(String::as_str)
            .chain([policy, generation])
        {
            validate(value)?;
        }
        roots.sort();
        roots.dedup();
        exclusions.sort();
        exclusions.dedup();
        let mut hash = Sha256::new();
        for values in [&roots, &exclusions] {
            hash.update((values.len() as u64).to_le_bytes());
            for value in values {
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value);
            }
        }
        for value in [policy, generation] {
            hash.update((value.len() as u64).to_le_bytes());
            hash.update(value);
        }
        Ok(Self {
            identity: format!("{:x}", hash.finalize()),
        })
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
}
pub fn validate(value: &str) -> Result<(), JournalError> {
    if value.len() > 4096 || validate_identifier(value).is_err() {
        Err(JournalError::InvalidInput)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub fence: i64,
    pub expires_at: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPhase {
    Inventory,
    Applying,
    Completed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkStatus {
    Pending,
    Applied,
    Failed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Delete is a scoped local candidate confirmed absent by a completed inventory;
/// Refresh and Unchanged represent authoritative inventory pages.
pub enum WorkAction {
    Refresh,
    Delete,
    Unchanged,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryPage {
    pub page_id: String,
    pub revision: String,
    pub action: WorkAction,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    Source,
    Index,
    Conflict,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalWork {
    pub page: InventoryPage,
    pub status: WorkStatus,
    pub failure: Option<FailureClass>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationRun {
    pub run_id: String,
    pub phase: RunPhase,
    pub checkpoint: Option<String>,
    pub inventory_count: u64,
    pub pending_count: u64,
    pub applied_count: u64,
    pub failed_count: u64,
    pub next_deadline: Option<i64>,
    pub refreshed_count: u64,
    pub deleted_count: u64,
    pub unchanged_count: u64,
    pub failure: Option<FailureClass>,
}

/// All mutations require a current database-wide fence. An index writer must
/// acquire this SAME lease and hold an external commit fence through its write;
/// the journal alone cannot fence an independent database's in-flight writes.
/// The clock is supplied by the scheduler and must be monotonic across holders.
pub trait ReconciliationJournal: Send + Sync {
    fn acquire_lease(&self, now: i64, duration: i64) -> Result<Lease, JournalError>;
    fn renew_lease(&self, lease: &Lease, now: i64, duration: i64) -> Result<Lease, JournalError>;
    fn release_lease(&self, lease: &Lease, now: i64) -> Result<(), JournalError>;
    fn start_or_resume(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        scope: &ReconciliationScope,
    ) -> Result<ReconciliationRun, JournalError>;
    fn run(
        &self,
        run_id: &str,
        scope: &ReconciliationScope,
    ) -> Result<ReconciliationRun, JournalError>;
    fn inventory(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        pages: &[InventoryPage],
        checkpoint: Option<&str>,
    ) -> Result<(), JournalError>;
    fn seal_inventory(&self, lease: &Lease, now: i64, run_id: &str) -> Result<(), JournalError>;
    fn work(&self, run_id: &str) -> Result<Vec<JournalWork>, JournalError>;
    fn acknowledge(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        page_id: &str,
        status: WorkStatus,
    ) -> Result<(), JournalError>;
    fn retry_failed(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        page_id: &str,
    ) -> Result<(), JournalError>;
    /// Retains work and records sanitized run failure/deadline, even during inventory.
    fn record_failure(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        failure: FailureClass,
        next_deadline: Option<i64>,
    ) -> Result<(), JournalError>;
    fn complete(
        &self,
        lease: &Lease,
        now: i64,
        run_id: &str,
        next_deadline: Option<i64>,
    ) -> Result<(), JournalError>;
}
