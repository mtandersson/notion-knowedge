//! Cooperative, owned commit operations on a trusted local Unix index.
//!
//! The directory inode is the advisory serialization anchor, not a replaceable
//! lock file. Only participating writers are protected; see docs/index-commits.md.
use std::{
    fs::{self, File, OpenOptions},
    future::Future,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use notion_knowledge_core::{
    logging::{self, EventGuard, Operation, Outcome},
    reconciliation::{FailureClass, Lease, ReconciliationScope, validate},
    sync_state::PageSyncState,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::oneshot;

use crate::sync_state::SqliteSyncStateStore;
mod alias_projection;
mod cache_reset;
mod offline;
pub mod transition;
pub use alias_projection::AliasProjection;
#[cfg(all(feature = "local-lancedb", target_os = "linux"))]
pub(crate) use offline::{LineageContext, resolve_archive_lineage};
pub use offline::{OfflineBackup, OwnedOfflineInspection};
pub(crate) mod projection;
pub use projection::RelationProjection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    InvalidInput,
    ProjectionRepairRequired,
    BindingMismatch,
    LeaseLost,
    Conflict,
    Unavailable,
    Operation(FailureClass),
}
impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index commit failed: {self:?}")
    }
}
impl std::error::Error for CommitError {}
fn sql(_: rusqlite::Error) -> CommitError {
    CommitError::Unavailable
}
fn io(_: std::io::Error) -> CommitError {
    CommitError::Unavailable
}

/// Trusted operator configuration, never inferred from an incoming webhook.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CommitBinding {
    table: String,
    workspace: String,
    roots: Vec<String>,
    scope: String,
    generation: String,
    epoch: i64,
}
impl CommitBinding {
    pub fn new(
        table: &str,
        workspace: &str,
        scope: &ReconciliationScope,
        generation: &str,
        epoch: u64,
    ) -> Result<Self, CommitError> {
        for value in [table, workspace, generation] {
            validate(value).map_err(|_| CommitError::InvalidInput)?;
        }
        let epoch = i64::try_from(epoch).map_err(|_| CommitError::InvalidInput)?;
        // A table name is one logical local table, never a path or URI.
        if !table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(CommitError::InvalidInput);
        }
        Ok(Self {
            table: table.into(),
            workspace: workspace.into(),
            roots: scope.roots().to_vec(),
            scope: scope.identity().into(),
            generation: generation.into(),
            epoch,
        })
    }
    /// Parse canonical descriptive authority sent by an already-owned offline
    /// verifier. This does not open SQL, initialize a coordinator or grant use.
    #[cfg(all(target_os = "linux", feature = "local-lancedb"))]
    pub(crate) fn parse_retained(bytes: &[u8]) -> Result<Self, CommitError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            table: String,
            workspace: String,
            roots: Vec<String>,
            scope: String,
            generation: String,
            epoch: u64,
        }
        if bytes.len() > 32768 {
            return Err(CommitError::InvalidInput);
        }
        let wire: Wire = serde_json::from_slice(bytes).map_err(|_| CommitError::InvalidInput)?;
        if wire.scope.len() != 64
            || !wire
                .scope
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(CommitError::InvalidInput);
        }
        // Validate canonical roots through the original constructor. Retain the
        // exact trusted source scope digest; never infer a new policy identity.
        let roots = ReconciliationScope::new(
            wire.roots.clone(),
            vec![],
            "retained-metadata",
            "retained-metadata",
        )
        .map_err(|_| CommitError::InvalidInput)?;
        if roots.roots() != wire.roots {
            return Err(CommitError::InvalidInput);
        }
        let mut result = Self::new(
            &wire.table,
            &wire.workspace,
            &roots,
            &wire.generation,
            wire.epoch,
        )?;
        result.scope = wire.scope;
        if serde_json::to_vec(&result).map_err(|_| CommitError::InvalidInput)? != bytes {
            return Err(CommitError::InvalidInput);
        }
        Ok(result)
    }
    pub fn table(&self) -> &str {
        &self.table
    }
    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn roots(&self) -> &[String] {
        &self.roots
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    /// Source policy is independent of the physical index fencing generation.
    /// The scope digest already commits to roots, exclusions and policy version.
    pub fn matches_policy(&self, workspace: &str, scope: &ReconciliationScope) -> bool {
        self.workspace == workspace && self.scope == scope.identity() && self.roots == scope.roots()
    }
    pub fn generation(&self) -> &str {
        &self.generation
    }
    pub fn epoch(&self) -> u64 {
        self.epoch as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageAction {
    Refresh,
    Delete,
    Unchanged,
}
impl PageAction {
    fn number(self) -> i64 {
        match self {
            Self::Refresh => 0,
            Self::Delete => 1,
            Self::Unchanged => 2,
        }
    }
}

/// Immutable replay identity plus a payload-free page checkpoint. The revision
/// and content hash must identify the prepared effect, not contain source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageOperation {
    id: String,
    revision: String,
    action: PageAction,
    checkpoint: PageSyncState,
    metadata_identity: Option<String>,
    relation_projection: Option<RelationProjection>,
    alias_projection: Option<AliasProjection>,
    retry_latest_refresh: bool,
    retry_latest_lifecycle: bool,
}
impl PageOperation {
    pub fn new(
        id: &str,
        revision: &str,
        action: PageAction,
        checkpoint: PageSyncState,
    ) -> Result<Self, CommitError> {
        for value in [id, revision, checkpoint.page_id()] {
            validate(value).map_err(|_| CommitError::InvalidInput)?;
        }
        if let Some(hash) = checkpoint.content_hash() {
            validate(hash).map_err(|_| CommitError::InvalidInput)?;
        }
        if checkpoint
            .last_edited_time()
            .is_some_and(|s| s.len() > 4096)
            || (action == PageAction::Delete) != checkpoint.is_tombstone()
        {
            return Err(CommitError::InvalidInput);
        }
        Ok(Self {
            id: id.into(),
            revision: revision.into(),
            action,
            checkpoint,
            metadata_identity: None,
            relation_projection: None,
            alias_projection: None,
            retry_latest_refresh: false,
            retry_latest_lifecycle: false,
        })
    }
    /// Retry durable page-refresh work using freshly authorized current content.
    /// Only unacknowledged receipts for the same page/action/generation may
    /// replace their proposal, under the external commit guard. An Applied or
    /// Superseded receipt remains immutable. Callers must supply a complete
    /// idempotent page replacement and revalidate source before its effects.
    pub fn retryable_refresh(
        id: &str,
        revision: &str,
        checkpoint: PageSyncState,
    ) -> Result<Self, CommitError> {
        let mut operation = Self::new(id, revision, PageAction::Refresh, checkpoint)?;
        operation.retry_latest_refresh = true;
        Ok(operation)
    }
    /// A lifecycle hint may change action while unacknowledged. Recompute
    /// current authority under the same guard; acknowledged hints stay immutable.
    pub fn retryable_lifecycle(
        id: &str,
        revision: &str,
        action: PageAction,
        checkpoint: PageSyncState,
    ) -> Result<Self, CommitError> {
        let mut operation = Self::new(id, revision, action, checkpoint)?;
        operation.retry_latest_lifecycle = true;
        Ok(operation)
    }
    pub fn with_metadata_identity(mut self, identity: String) -> Result<Self, CommitError> {
        if self.action == PageAction::Delete
            || identity.len() != 64
            || !identity.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(CommitError::InvalidInput);
        }
        self.metadata_identity = Some(identity);
        Ok(self)
    }
    /// Attach a complete relation effect. Replaying an immutable Applied receipt
    /// whose effect is absent returns ProjectionRepairRequired; use a fresh
    /// operation ID and freshly authorized complete page proposal for repair.
    pub fn with_relation_projection(
        mut self,
        projection: RelationProjection,
    ) -> Result<Self, CommitError> {
        if projection.page_id() != self.checkpoint.page_id() {
            return Err(CommitError::InvalidInput);
        }
        self.relation_projection = Some(projection);
        Ok(self)
    }
    /// Attach exact aliases extracted from trusted normalized source metadata.
    pub fn with_alias_projection(
        mut self,
        projection: AliasProjection,
    ) -> Result<Self, CommitError> {
        if projection.page_id() != self.checkpoint.page_id() {
            return Err(CommitError::InvalidInput);
        }
        self.alias_projection = Some(projection);
        Ok(self)
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn action(&self) -> PageAction {
        self.action
    }
    pub fn checkpoint(&self) -> &PageSyncState {
        &self.checkpoint
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptStatus {
    Pending,
    Applied,
    Failed,
    Superseded,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationReceipt {
    pub status: ReceiptStatus,
    pub attempts: u64,
    pub failure: Option<FailureClass>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
}
impl Identity {
    fn read(path: &Path) -> Result<Self, CommitError> {
        let m = fs::metadata(path).map_err(io)?;
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
        })
    }
    fn file(file: &File) -> Result<Self, CommitError> {
        let m = file.metadata().map_err(io)?;
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
        })
    }
}

/// One state database and one index directory/table/workspace per coordinator.
/// Directly cloned `Arc`s share configuration, not a process-local lock.
pub struct IndexCommitCoordinator {
    state: Arc<SqliteSyncStateStore>,
    index_path: PathBuf,
    state_path: PathBuf,
    index_identity: Identity,
    state_identity: Identity,
    state_token: String,
    binding: CommitBinding,
    owned_threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}
const ANCHOR: &str = ".nk-operational-binding";
impl IndexCommitCoordinator {
    /// Initialize an explicit trusted binding, or verify an identical existing
    /// binding. The index directory must already exist. No index is rebuilt.
    pub fn initialize(
        index: impl AsRef<Path>,
        state: impl AsRef<Path>,
        binding: CommitBinding,
    ) -> Result<Arc<Self>, CommitError> {
        Self::connect(index.as_ref(), state.as_ref(), binding, true)
    }
    /// Open an existing binding. Does not silently create or change authority.
    pub fn open(
        index: impl AsRef<Path>,
        state: impl AsRef<Path>,
        binding: CommitBinding,
    ) -> Result<Arc<Self>, CommitError> {
        Self::connect(index.as_ref(), state.as_ref(), binding, false)
    }
    /// Validate existing authority using only reads; no migrations, writer
    /// transaction, directory guard, anchor creation or operational metadata.
    pub fn open_read_only(
        index: impl AsRef<Path>,
        state: impl AsRef<Path>,
        binding: CommitBinding,
    ) -> Result<Arc<Self>, CommitError> {
        Self::inspect_existing(index.as_ref(), state.as_ref(), binding, true)
    }
    /// Existing current-schema writer; validation itself does not lock the
    /// index or start a writer transaction. Effects still require the shared
    /// lease and operation-owned guard, with fresh authority checks.
    pub fn open_existing(
        index: impl AsRef<Path>,
        state: impl AsRef<Path>,
        binding: CommitBinding,
    ) -> Result<Arc<Self>, CommitError> {
        Self::inspect_existing(index.as_ref(), state.as_ref(), binding, false)
    }
    fn inspect_existing(
        index: &Path,
        state: &Path,
        binding: CommitBinding,
        read_only: bool,
    ) -> Result<Arc<Self>, CommitError> {
        let index_path = fs::canonicalize(index).map_err(io)?;
        if !index_path.is_dir() || fs::metadata(state).map_err(io)?.nlink() != 1 {
            return Err(CommitError::BindingMismatch);
        }
        let state_path = fs::canonicalize(state).map_err(io)?;
        let index_identity = Identity::read(&index_path)?;
        let state_identity = Identity::read(&state_path)?;
        let store = Arc::new(
            if read_only {
                SqliteSyncStateStore::open_read_only(&state_path)
            } else {
                SqliteSyncStateStore::open_existing(&state_path)
            }
            .map_err(|_| CommitError::Unavailable)?,
        );
        let state_token = store
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?
            .query_row(
                "SELECT identity FROM index_commit_state_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let coordinator = Arc::new(Self {
            state: store,
            index_path,
            state_path,
            index_identity,
            state_identity,
            state_token,
            binding,
            owned_threads: Mutex::new(Vec::new()),
        });
        let db = coordinator
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        coordinator.validate_binding(&db)?;
        coordinator.anchor(false)?;
        drop(db);
        Ok(coordinator)
    }
    fn connect(
        index: &Path,
        state: &Path,
        binding: CommitBinding,
        initialize: bool,
    ) -> Result<Arc<Self>, CommitError> {
        if !initialize && !state.exists() {
            return Err(CommitError::BindingMismatch);
        }
        let index_path = fs::canonicalize(index).map_err(io)?;
        if !index_path.is_dir() {
            return Err(CommitError::InvalidInput);
        }
        let guard = File::open(&index_path).map_err(io)?;
        guard.lock().map_err(io)?;
        let index_identity = Identity::file(&guard)?;
        if Identity::read(&index_path)? != index_identity {
            return Err(CommitError::BindingMismatch);
        }
        // Refuse SQLite aliases through hard links: SQLite's journal pathname
        // cannot safely coordinate multiple names for one database file.
        if state.exists() && fs::metadata(state).map_err(io)?.nlink() != 1 {
            return Err(CommitError::BindingMismatch);
        }
        // The coordinator supports trusted aliases, but the storage boundary
        // deliberately refuses to follow a final-component symlink. Resolve an
        // existing alias here, then open its target with that strict boundary.
        let state_target = if state.exists() {
            fs::canonicalize(state).map_err(io)?
        } else {
            state.to_path_buf()
        };
        let store = Arc::new(
            SqliteSyncStateStore::open(&state_target).map_err(|_| CommitError::Unavailable)?,
        );
        let state_path = fs::canonicalize(&state_target).map_err(io)?;
        let state_identity = Identity::read(&state_path)?;
        let state_token = store
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?
            .query_row(
                "SELECT identity FROM index_commit_state_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let coordinator = Arc::new(Self {
            state: store,
            index_path,
            state_path,
            index_identity,
            state_identity,
            state_token,
            binding,
            owned_threads: Mutex::new(Vec::new()),
        });
        let mut db = coordinator
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        if initialize {
            tx.execute(
                "INSERT OR IGNORE INTO index_commit_binding VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    index_identity.device.to_string(),
                    index_identity.inode.to_string(),
                    state_identity.device.to_string(),
                    state_identity.inode.to_string(),
                    coordinator.binding.table,
                    coordinator.binding.workspace,
                    coordinator.binding.scope,
                    coordinator.binding.generation,
                    coordinator.binding.epoch
                ],
            )
            .map_err(sql)?;
            coordinator.validate_binding(&tx)?;
            coordinator.anchor(true)?;
            tx.execute(
                "INSERT OR IGNORE INTO index_commit_generations VALUES(?1,?2,?3)",
                params![
                    coordinator.binding.generation,
                    coordinator.binding.scope,
                    coordinator.binding.epoch
                ],
            )
            .map_err(sql)?;
        } else {
            coordinator.validate_binding(&tx)?;
            coordinator.anchor(false)?;
        }
        tx.commit().map_err(sql)?;
        drop(db);
        drop(guard);
        Ok(coordinator)
    }
    /// Wait for an already submitted owned effect before a graceful operator
    /// exit. Cancellation invalidates its clock, but never releases a live guard.
    pub async fn drain_owned(self: &Arc<Self>) -> Result<(), CommitError> {
        let coordinator = self.clone();
        tokio::task::spawn_blocking(move || {
            let handles: Vec<_> = coordinator
                .owned_threads
                .lock()
                .map_err(|_| CommitError::Unavailable)?
                .drain(..)
                .collect();
            for handle in handles {
                handle.join().map_err(|_| CommitError::Unavailable)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| CommitError::Unavailable)?
    }
    fn track_owned(&self, handle: std::thread::JoinHandle<()>) -> Result<(), CommitError> {
        let mut handles = self
            .owned_threads
            .lock()
            .map_err(|_| CommitError::Unavailable)?;
        handles.retain(|h| !h.is_finished());
        handles.push(handle);
        Ok(())
    }
    pub fn state(&self) -> &Arc<SqliteSyncStateStore> {
        &self.state
    }
    pub fn binding(&self) -> &CommitBinding {
        &self.binding
    }
    /// Canonical local directory owning the advisory guard and LanceDB tables.
    pub fn index_directory(&self) -> &Path {
        &self.index_path
    }
    fn anchor(&self, initialize: bool) -> Result<(), CommitError> {
        let path = self.index_path.join(ANCHOR);
        let expected = format!(
            "nk-index-binding-v1\n{}\n{}\n{}\n",
            self.state_identity.device, self.state_identity.inode, self.state_token
        );
        if initialize && !path.exists() {
            // Publish complete bytes without replacing an existing anchor.
            // A crash before publication leaves only an inert temporary file.
            let temporary = self.index_path.join(format!(
                ".nk-binding-{}-{}",
                std::process::id(),
                self.state_identity.inode
            ));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(io)?;
            file.write_all(expected.as_bytes()).map_err(io)?;
            file.sync_all().map_err(io)?;
            fs::hard_link(&temporary, &path).map_err(io)?;
            fs::remove_file(&temporary).map_err(io)?;
            File::open(&self.index_path)
                .map_err(io)?
                .sync_all()
                .map_err(io)?;
        }
        let meta = fs::symlink_metadata(&path).map_err(io)?;
        if !meta.is_file() || meta.nlink() != 1 || meta.len() > 128 {
            return Err(CommitError::BindingMismatch);
        }
        let mut content = String::new();
        File::open(path)
            .map_err(io)?
            .take(129)
            .read_to_string(&mut content)
            .map_err(io)?;
        if content != expected {
            return Err(CommitError::BindingMismatch);
        }
        Ok(())
    }
    pub(crate) fn identities(&self) -> Result<(), CommitError> {
        if Identity::read(&self.index_path)? != self.index_identity
            || Identity::read(&self.state_path)? != self.state_identity
            || fs::metadata(&self.state_path).map_err(io)?.nlink() != 1
        {
            return Err(CommitError::BindingMismatch);
        }
        self.anchor(false)
    }
    pub(crate) fn try_acquire(&self) -> Result<File, CommitError> {
        let guard = File::open(&self.index_path).map_err(io)?;
        match guard.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(CommitError::Conflict),
            Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
        }
        if Identity::file(&guard)? != self.index_identity {
            return Err(CommitError::BindingMismatch);
        }
        self.identities()?;
        Ok(guard)
    }
    /// Fence a read snapshot against concurrent commits and unfinished effects.
    #[cfg(feature = "local-lancedb")]
    pub(crate) fn search_guard(&self) -> Result<File, CommitError> {
        // Readers may nest (graph + vector/FTS) while sharing one committed
        // revision. Writers still use exclusive try_acquire/acquire locks.
        let guard = File::open(&self.index_path).map_err(io)?;
        match guard.try_lock_shared() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(CommitError::Conflict),
            Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
        }
        if Identity::file(&guard)? != self.index_identity {
            return Err(CommitError::BindingMismatch);
        }
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        let unfinished: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE generation=?1 AND status IN(0,2))",
            [&self.binding.generation], |row| row.get(0)).map_err(sql)?;
        if unfinished {
            return Err(CommitError::Conflict);
        }
        Ok(guard)
    }
    fn acquire(&self) -> Result<File, CommitError> {
        let guard = File::open(&self.index_path).map_err(io)?;
        guard.lock().map_err(io)?;
        if Identity::file(&guard)? != self.index_identity {
            return Err(CommitError::BindingMismatch);
        }
        self.identities()?;
        Ok(guard)
    }
    /// Owned callers must not leave an unbounded detached thread waiting on a
    /// directory lock after their lease or cancellation clock loses authority.
    fn acquire_owned(
        &self,
        lease: &Lease,
        clock: &Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<File, CommitError> {
        let guard = File::open(&self.index_path).map_err(io)?;
        loop {
            if clock() >= lease.expires_at {
                return Err(CommitError::LeaseLost);
            }
            match guard.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(20))
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
            }
        }
        if Identity::file(&guard)? != self.index_identity {
            return Err(CommitError::BindingMismatch);
        }
        self.identities()?;
        Ok(guard)
    }
    pub(crate) fn validate_binding(&self, db: &Connection) -> Result<(), CommitError> {
        transition::check_admission(self)?;
        let token: String = db
            .query_row(
                "SELECT identity FROM index_commit_state_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if token != self.state_token {
            return Err(CommitError::BindingMismatch);
        }
        let valid: bool = db.query_row("SELECT index_device=?1 AND index_inode=?2 AND state_device=?3 AND state_inode=?4 AND table_name=?5 AND workspace=?6 AND scope=?7 AND generation=?8 AND epoch=?9 FROM index_commit_binding WHERE singleton=1",params![self.index_identity.device.to_string(),self.index_identity.inode.to_string(),self.state_identity.device.to_string(),self.state_identity.inode.to_string(),self.binding.table,self.binding.workspace,self.binding.scope,self.binding.generation,self.binding.epoch],|r|r.get(0)).optional().map_err(sql)?.unwrap_or(false);
        if valid {
            Ok(())
        } else {
            Err(CommitError::BindingMismatch)
        }
    }
    fn validate_fence(&self, db: &Connection, lease: &Lease, now: i64) -> Result<(), CommitError> {
        if now < 0 {
            return Err(CommitError::InvalidInput);
        }
        let valid: bool = db
            .query_row(
                "SELECT fence=?1 AND expires_at>?2 FROM reconciliation_lease WHERE singleton=1",
                params![lease.fence, now],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if !valid {
            return Err(CommitError::LeaseLost);
        }
        let active = crate::schedule::live_reconciliation_owners(db)
            .map_err(|_| CommitError::Unavailable)?;
        if active
            .iter()
            .any(|(scope, fence)| scope != &self.binding.scope || *fence != lease.fence)
        {
            return Err(CommitError::Conflict);
        }
        Ok(())
    }
    fn revalidate(&self, lease: &Lease, now: i64) -> Result<(), CommitError> {
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        self.validate_fence(&db, lease, now)
    }
    /// Explicit compare-and-change of current authority under the SAME guard.
    /// Table/workspace cannot change; a generation can never be reused/regressed.
    /// Finish or recover old receipts and runs before changing configuration.
    pub fn change_scope(
        self: &Arc<Self>,
        next: CommitBinding,
        lease: &Lease,
        now: i64,
    ) -> Result<Arc<Self>, CommitError> {
        self.change_scope_inner(next, lease, now, None)
    }
    /// Explicitly abandon exactly the listed unresolved receipts when changing
    /// authority. They remain durable Superseded receipts, NEVER Applied. A later
    /// source reconciliation uses new operation IDs in the new generation.
    pub fn supersede_scope(
        self: &Arc<Self>,
        next: CommitBinding,
        lease: &Lease,
        now: i64,
        unresolved: &[String],
    ) -> Result<Arc<Self>, CommitError> {
        for id in unresolved {
            validate(id).map_err(|_| CommitError::InvalidInput)?;
        }
        self.change_scope_inner(next, lease, now, Some(unresolved))
    }
    fn change_scope_inner(
        self: &Arc<Self>,
        next: CommitBinding,
        lease: &Lease,
        now: i64,
        supersede: Option<&[String]>,
    ) -> Result<Arc<Self>, CommitError> {
        if next.table != self.binding.table
            || next.workspace != self.binding.workspace
            || next.generation == self.binding.generation
            || next.epoch <= self.binding.epoch
        {
            return Err(CommitError::BindingMismatch);
        }
        let _guard = self.acquire()?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        self.validate_binding(&tx)?;
        self.validate_fence(&tx, lease, now)?;
        if let Some(expected) = supersede {
            let actual = {
                let mut statement = tx.prepare("SELECT operation_id FROM index_commit_receipts WHERE status IN(0,2) ORDER BY operation_id").map_err(sql)?;
                statement
                    .query_map([], |r| r.get::<_, String>(0))
                    .map_err(sql)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql)?
            };
            let mut expected = expected.to_vec();
            expected.sort();
            if expected != actual {
                return Err(CommitError::Conflict);
            }
            tx.execute(
                "UPDATE index_commit_receipts SET status=3,failure=2 WHERE status IN(0,2)",
                [],
            )
            .map_err(sql)?;
        }
        let unfinished: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE status IN(0,2)) OR EXISTS(SELECT 1 FROM reconciliation_runs WHERE phase<>2 AND abandoned=0) OR EXISTS(SELECT 1 FROM index_commit_generations WHERE generation=?1)",[&next.generation],|r|r.get(0)).map_err(sql)?;
        if unfinished {
            return Err(CommitError::Conflict);
        }
        tx.execute(
            "INSERT INTO index_commit_generations VALUES(?1,?2,?3)",
            params![next.generation, next.scope, next.epoch],
        )
        .map_err(sql)?;
        tx.execute(
            "UPDATE index_commit_binding SET scope=?1,generation=?2,epoch=?3 WHERE singleton=1",
            params![next.scope, next.generation, next.epoch],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Arc::new(Self {
            state: self.state.clone(),
            index_path: self.index_path.clone(),
            state_path: self.state_path.clone(),
            index_identity: self.index_identity,
            state_identity: self.state_identity,
            state_token: self.state_token.clone(),
            binding: next,
            owned_threads: Mutex::new(Vec::new()),
        }))
    }
    /// Durable evidence of page ownership even after external effects removed
    /// all chunks and the checkpoint transaction was interrupted. Scope is the
    /// current trusted generation; an unrelated event ID is never sufficient.
    /// Only receipts belonging to the current binding establish sync ownership.
    pub fn owned_page_ids(&self) -> Result<Vec<String>, CommitError> {
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        let mut statement = db.prepare("SELECT DISTINCT page_id FROM index_commit_receipts WHERE generation=?1 AND status IN(0,1,2) ORDER BY page_id").map_err(sql)?;
        statement
            .query_map([&self.binding.generation], |r| r.get(0))
            .map_err(sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql)
    }
    pub fn metadata_identity(&self, page_id: &str) -> Result<Option<String>, CommitError> {
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        db.query_row(
            "SELECT identity FROM page_metadata_identity WHERE page_id=?1 AND generation=?2",
            params![page_id, self.binding.generation],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)
    }
    pub fn owns_page_work(&self, page_id: &str) -> Result<bool, CommitError> {
        validate(page_id).map_err(|_| CommitError::InvalidInput)?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        db.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE generation=?1 AND page_id=?2 AND status IN(0,1,2))",
            params![self.binding.generation, page_id], |r| r.get(0)).map_err(sql)
    }
    /// Unfinished derived page effects keep search closed, but must not prevent
    /// an unrelated healthy page from syncing and acknowledging its own work.
    pub fn has_unfinished_commits(&self) -> Result<bool, CommitError> {
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        db.query_row(
            "SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE generation=?1 AND status IN(0,2))",
            [&self.binding.generation], |row| row.get(0),
        ).map_err(sql)
    }

    pub fn receipt(&self, operation_id: &str) -> Result<Option<OperationReceipt>, CommitError> {
        validate(operation_id).map_err(|_| CommitError::InvalidInput)?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        db.query_row(
            "SELECT status,attempts,failure FROM index_commit_receipts WHERE operation_id=?1",
            [operation_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql)?
        .map(|(status, attempts, failure)| {
            Ok(OperationReceipt {
                status: match status {
                    0 => ReceiptStatus::Pending,
                    1 => ReceiptStatus::Applied,
                    2 => ReceiptStatus::Failed,
                    3 => ReceiptStatus::Superseded,
                    _ => return Err(CommitError::Unavailable),
                },
                attempts: attempts as u64,
                failure: failure.map(failure_value).transpose()?,
            })
        })
        .transpose()
    }
    fn begin(
        &self,
        operation: &PageOperation,
        lease: &Lease,
        now: i64,
    ) -> Result<bool, CommitError> {
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        self.validate_binding(&tx)?;
        self.validate_fence(&tx, lease, now)?;
        let checkpoint = &operation.checkpoint;
        if let Some(projection) = &operation.relation_projection
            && !projection.matches_operation(&self.binding, operation)
        {
            return Err(CommitError::BindingMismatch);
        }
        if let Some(projection) = &operation.alias_projection
            && !projection.matches_operation(&self.binding, operation)
        {
            return Err(CommitError::BindingMismatch);
        }
        let existing: Option<(bool, i64, bool)> = tx.query_row("SELECT generation=?2 AND page_id=?3 AND revision=?4 AND action=?5 AND content_hash IS ?6 AND last_edited_time IS ?7,status,generation=?2 AND page_id=?3 AND (action=?5 OR ?8) FROM index_commit_receipts WHERE operation_id=?1",params![operation.id,self.binding.generation,checkpoint.page_id(),operation.revision,operation.action.number(),checkpoint.content_hash(),checkpoint.last_edited_time(),operation.retry_latest_lifecycle],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql)?;
        if let Some((matches, status, same_authority)) = existing {
            // An acknowledged hint stays acknowledged even if the source
            // changed before redelivery. A distinct event refreshes that edit.
            if (operation.retry_latest_refresh || operation.retry_latest_lifecycle)
                && same_authority
                && status == 1
            {
                if let Some(projection) = &operation.relation_projection {
                    let proposed: Option<String> = tx
                        .query_row(
                            "SELECT effect FROM relation_operation_proposals WHERE operation_id=?1",
                            [&operation.id],
                            |r| r.get(0),
                        )
                        .optional()
                        .map_err(sql)?;
                    if proposed.as_deref() != Some(&projection.effect)
                        || !projection.complete(&tx)?
                    {
                        return Err(CommitError::ProjectionRepairRequired);
                    }
                }
                alias_projection::validate_applied(&tx, operation)?;
                tx.commit().map_err(sql)?;
                return Ok(false);
            }
            if !matches
                && !((operation.retry_latest_refresh || operation.retry_latest_lifecycle)
                    && same_authority
                    && matches!(status, 0 | 2))
            {
                return Err(CommitError::Conflict);
            }
            if status == 3 {
                return Err(CommitError::Conflict);
            }
            if status == 1 {
                if let Some(projection) = &operation.relation_projection {
                    let proposed: Option<String> = tx
                        .query_row(
                            "SELECT effect FROM relation_operation_proposals WHERE operation_id=?1",
                            [&operation.id],
                            |r| r.get(0),
                        )
                        .optional()
                        .map_err(sql)?;
                    if proposed.as_deref() != Some(&projection.effect)
                        || !projection.complete(&tx)?
                    {
                        return Err(CommitError::ProjectionRepairRequired);
                    }
                }
                alias_projection::validate_applied(&tx, operation)?;
                tx.commit().map_err(sql)?;
                return Ok(false);
            }
            tx.execute("UPDATE index_commit_receipts SET status=0,failure=NULL,attempts=attempts+1,revision=?2,content_hash=?3,last_edited_time=?4,action=?5 WHERE operation_id=?1",params![operation.id,operation.revision,checkpoint.content_hash(),checkpoint.last_edited_time(),operation.action.number()]).map_err(sql)?;
        } else {
            tx.execute(
                "INSERT INTO index_commit_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,0,NULL,1)",
                params![
                    operation.id,
                    self.binding.generation,
                    checkpoint.page_id(),
                    operation.revision,
                    operation.action.number(),
                    checkpoint.content_hash(),
                    checkpoint.last_edited_time()
                ],
            )
            .map_err(sql)?;
        }
        let previous: Option<String> = tx
            .query_row(
                "SELECT effect FROM relation_operation_proposals WHERE operation_id=?1",
                [&operation.id],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        let proposed = operation
            .relation_projection
            .as_ref()
            .map(|p| p.effect.as_str());
        if previous.as_deref() != proposed
            && !(operation.retry_latest_refresh || operation.retry_latest_lifecycle)
            && existing.is_some()
        {
            return Err(CommitError::Conflict);
        }
        tx.execute(
            "DELETE FROM relation_operation_proposals WHERE operation_id=?1",
            [&operation.id],
        )
        .map_err(sql)?;
        if let Some(effect) = proposed {
            tx.execute(
                "INSERT INTO relation_operation_proposals VALUES(?1,?2)",
                params![operation.id, effect],
            )
            .map_err(sql)?;
        }
        alias_projection::persist_proposal(&tx, operation, existing.is_some())?;
        tx.commit().map_err(sql)?;
        Ok(true)
    }
    fn finish(
        &self,
        operation: &PageOperation,
        lease: &Lease,
        now: i64,
    ) -> Result<(), CommitError> {
        self.identities()?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        self.validate_binding(&tx)?;
        self.validate_fence(&tx, lease, now)?;
        if let Some(projection) = &operation.relation_projection {
            projection.install(&tx)?;
        } else {
            // A legacy write cannot attest that an earlier graph is current.
            tx.execute(
                "DELETE FROM page_relation_projections WHERE page_id=?1",
                [operation.checkpoint.page_id()],
            )
            .map_err(sql)?;
        }
        if let Some(projection) = &operation.alias_projection {
            projection.install(&tx)?;
        } else {
            tx.execute(
                "DELETE FROM page_alias_projections WHERE page_id=?1",
                [operation.checkpoint.page_id()],
            )
            .map_err(sql)?;
        }
        let checkpoint = &operation.checkpoint;
        tx.execute("INSERT INTO page_sync_state VALUES(?1,?2,?3,?4,unixepoch()) ON CONFLICT(page_id) DO UPDATE SET content_hash=excluded.content_hash,last_edited_time=excluded.last_edited_time,tombstoned=excluded.tombstoned,updated_at_unix=excluded.updated_at_unix",params![checkpoint.page_id(),checkpoint.content_hash(),checkpoint.last_edited_time(),i64::from(checkpoint.is_tombstone())]).map_err(sql)?;
        tx.execute(
            "DELETE FROM page_metadata_identity WHERE page_id=?1",
            [checkpoint.page_id()],
        )
        .map_err(sql)?;
        if let Some(identity) = &operation.metadata_identity {
            tx.execute(
                "INSERT INTO page_metadata_identity VALUES(?1,?2,?3)",
                params![checkpoint.page_id(), self.binding.generation, identity],
            )
            .map_err(sql)?;
        }
        let count=tx.execute("UPDATE index_commit_receipts SET status=1,failure=NULL WHERE operation_id=?1 AND status=0",[&operation.id]).map_err(sql)?;
        if count != 1 {
            return Err(CommitError::Conflict);
        }
        tx.commit().map_err(sql)
    }
    fn fail(&self, operation: &PageOperation, failure: FailureClass) -> Result<(), CommitError> {
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        db.execute("UPDATE index_commit_receipts SET status=2,failure=?1 WHERE operation_id=?2 AND status=0",params![failure_number(failure),operation.id]).map_err(sql)?;
        Ok(())
    }
    /// Eagerly transfer ownership before returning the observer future. Dropping
    /// that observer NEVER aborts the dedicated thread, operation or checkpoint.
    /// Callbacks must await all effects they start, and supply idempotent replay.
    pub fn submit<F, Fut>(
        self: &Arc<Self>,
        operation: PageOperation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        effect: F,
    ) -> impl Future<Output = Result<CommitOutcome, CommitError>> + Send + 'static
    where
        F: FnOnce(CommitContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), FailureClass>> + Send + 'static,
    {
        self.submit_with_authority(operation, lease, clock, effect)
    }

    /// Retain the callback's authority until final acknowledgment completes.
    pub fn submit_with_authority<F, Fut, Authority>(
        self: &Arc<Self>,
        operation: PageOperation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        effect: F,
    ) -> impl Future<Output = Result<CommitOutcome, CommitError>> + Send + 'static
    where
        F: FnOnce(CommitContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Authority, FailureClass>> + Send + 'static,
        Authority: Send + 'static,
    {
        let coordinator = self.clone();
        let correlation = logging::current_id();
        let (sender, receiver) = oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("nk-index-commit".into())
            .spawn(move || {
                let mut log = EventGuard::with_id(Operation::IndexCommit, correlation);
                let result = (|| {
                    let _guard = coordinator.acquire_owned(&lease, &clock)?;
                    coordinator.revalidate(&lease, clock())?;
                    if !coordinator.begin(&operation, &lease, clock())? {
                        return Ok(CommitOutcome::AlreadyApplied);
                    }
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(io)?;
                    let context = CommitContext {
                        coordinator: coordinator.clone(),
                        lease: lease.clone(),
                        clock: clock.clone(),
                    };
                    match runtime.block_on(effect(context)) {
                        Ok(authority) => {
                            let result = coordinator
                                .finish(&operation, &lease, clock())
                                .map(|()| CommitOutcome::Applied);
                            drop(authority);
                            result
                        }
                        Err(failure) => {
                            coordinator.fail(&operation, failure.clone())?;
                            Err(CommitError::Operation(failure))
                        }
                    }
                })();
                log.finish(if result.is_ok() {
                    Outcome::Success
                } else {
                    Outcome::Failed
                });
                let _ = sender.send(result);
            });
        let tracked = spawned
            .map_err(io)
            .and_then(|handle| self.track_owned(handle));
        async move {
            tracked?;
            receiver.await.map_err(|_| CommitError::Unavailable)?
        }
    }
    /// Own index bootstrap, startup FTS repair and other idempotent index-only
    /// maintenance. A page receipt is intentionally not written: this API MUST
    /// NOT be used to acknowledge a page update. The operation owns its runtime
    /// and directory lock even after observer cancellation.
    pub fn maintain<F, Fut>(
        self: &Arc<Self>,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        effect: F,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static
    where
        F: FnOnce(CommitContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), FailureClass>> + Send + 'static,
    {
        let coordinator = self.clone();
        let (sender, receiver) = oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("nk-index-maintenance".into())
            .spawn(move || {
                let result = (|| {
                    let _guard = coordinator.acquire_owned(&lease, &clock)?;
                    coordinator.revalidate(&lease, clock())?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(io)?;
                    let context = CommitContext {
                        coordinator,
                        lease,
                        clock,
                    };
                    runtime
                        .block_on(effect(context))
                        .map_err(CommitError::Operation)
                })();
                let _ = sender.send(result);
            });
        let tracked = spawned
            .map_err(io)
            .and_then(|handle| self.track_owned(handle));
        async move {
            tracked?;
            receiver.await.map_err(|_| CommitError::Unavailable)?
        }
    }
}

/// Available only inside the owned effect; does not hold a SQLite mutex.
/// Consumers should revalidate after source preparation and before each effect.
pub struct CommitContext {
    coordinator: Arc<IndexCommitCoordinator>,
    lease: Lease,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}
impl CommitContext {
    pub fn revalidate(&self) -> Result<(), CommitError> {
        self.coordinator.revalidate(&self.lease, (self.clock)())
    }
    pub fn binding(&self) -> &CommitBinding {
        self.coordinator.binding()
    }
    pub fn state(&self) -> &Arc<SqliteSyncStateStore> {
        self.coordinator.state()
    }
}
fn failure_number(failure: FailureClass) -> i64 {
    match failure {
        FailureClass::Source => 0,
        FailureClass::Index => 1,
        FailureClass::Conflict => 2,
        FailureClass::Unavailable => 3,
    }
}
fn failure_value(failure: i64) -> Result<FailureClass, CommitError> {
    match failure {
        0 => Ok(FailureClass::Source),
        1 => Ok(FailureClass::Index),
        2 => Ok(FailureClass::Conflict),
        3 => Ok(FailureClass::Unavailable),
        _ => Err(CommitError::Unavailable),
    }
}
