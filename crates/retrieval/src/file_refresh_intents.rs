//! Operational source-effect ownership. An expired owner is quarantined, never
//! evidence that a remote effect finished. These are trusted storage primitives;
//! server admission must additionally validate the live grant and physical root.
use crate::{
    mutation_refresh::{authority_matches, encoded},
    sync_state::SqliteSyncStateStore,
};
use notion_knowledge_core::write_refresh::{
    GrantRefreshAuthority, RefreshKey, RefreshQueueError as Error,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

pub const MAX_LEASE_SECONDS: i64 = 300;
pub const MAX_OWNER_SECONDS: i64 = 3600;
pub const MAX_HELD_PER_WORKSPACE: i64 = 64;
#[derive(Clone, Debug)]
pub struct FileRefreshIntent {
    key: RefreshKey,
    authority: GrantRefreshAuthority,
    owner: Vec<u8>,
}
impl FileRefreshIntent {
    pub fn key(&self) -> &RefreshKey {
        &self.key
    }
    pub fn authority(&self) -> &GrantRefreshAuthority {
        &self.authority
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileIntentStatus {
    Held,
    RecoveryRequired,
    Released,
    Discarded,
}
fn sql(_: rusqlite::Error) -> Error {
    Error::Unavailable
}
fn until(now: i64, seconds: i64) -> Result<i64, Error> {
    if now < 0 || !(1..=MAX_LEASE_SECONDS).contains(&seconds) {
        return Err(Error::InvalidInput);
    }
    now.checked_add(seconds).ok_or(Error::InvalidInput)
}
impl SqliteSyncStateStore {
    /// Both rows commit together. Existing source/semantic keys are never adopted.
    pub fn begin_file_refresh_intent(
        &self,
        key: &RefreshKey,
        authority: &GrantRefreshAuthority,
        now: i64,
        seconds: i64,
    ) -> Result<FileRefreshIntent, Error> {
        authority_matches(key, Some(authority))?;
        let lease = until(now, seconds)?;
        let deadline = now
            .checked_add(MAX_OWNER_SECONDS)
            .ok_or(Error::InvalidInput)?;
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let count: i64 = tx
            .query_row(
                "SELECT count(*) FROM file_refresh_intents WHERE workspace_id=?1 AND phase='held'",
                [&key.workspace_id],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if count >= MAX_HELD_PER_WORKSPACE {
            return Err(Error::Unavailable);
        }
        let existing:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2)",params![key.workspace_id,key.mutation_digest],|r|r.get(0)).map_err(sql)?;
        if existing {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id,authority) VALUES(?1,?2,?3,?4)",params![key.workspace_id,key.mutation_digest,key.page_id,encoded(Some(authority))?]).map_err(sql)?;
        tx.execute("INSERT INTO file_refresh_intents(workspace_id,mutation_digest,owner,lease_until,deadline,phase) VALUES(?1,?2,randomblob(32),?3,?4,'held')",params![key.workspace_id,key.mutation_digest,lease,deadline]).map_err(sql)?;
        let owner=tx.query_row("SELECT owner FROM file_refresh_intents WHERE workspace_id=?1 AND mutation_digest=?2",params![key.workspace_id,key.mutation_digest],|r|r.get(0)).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(FileRefreshIntent {
            key: key.clone(),
            authority: authority.clone(),
            owner,
        })
    }
    /// Reopen a record for inspection/reconciliation, not source ownership.
    pub fn file_refresh_intent_status(
        &self,
        key: &RefreshKey,
        authority: &GrantRefreshAuthority,
        now: i64,
    ) -> Result<Option<FileIntentStatus>, Error> {
        authority_matches(key, Some(authority))?;
        if now < 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let row:Option<(String,String,String,i64)>=db.query_row("SELECT m.page_id,m.authority,f.phase,f.lease_until FROM file_refresh_intents f JOIN mutation_refresh m USING(workspace_id,mutation_digest) WHERE f.workspace_id=?1 AND f.mutation_digest=?2",params![key.workspace_id,key.mutation_digest],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(sql)?;
        row.map(|(page, stored, phase, lease)| {
            if page != key.page_id || stored != encoded(Some(authority))? {
                return Err(Error::Conflict);
            }
            match phase.as_str() {
                "held" => Ok(if lease > now {
                    FileIntentStatus::Held
                } else {
                    FileIntentStatus::RecoveryRequired
                }),
                "released" => Ok(FileIntentStatus::Released),
                "discarded" => Ok(FileIntentStatus::Discarded),
                _ => Err(Error::Unavailable),
            }
        })
        .transpose()
    }
    pub fn file_source_owner_is_current(
        &self,
        intent: &FileRefreshIntent,
        now: i64,
    ) -> Result<bool, Error> {
        if now < 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        db.query_row("SELECT EXISTS(SELECT 1 FROM file_refresh_intents f JOIN mutation_refresh m USING(workspace_id,mutation_digest) WHERE f.workspace_id=?1 AND f.mutation_digest=?2 AND m.page_id=?3 AND m.authority=?4 AND f.owner=?5 AND f.phase='held' AND f.lease_until>?6 AND f.deadline>?6 AND m.state=0 AND m.generation=0)",params![intent.key.workspace_id,intent.key.mutation_digest,intent.key.page_id,encoded(Some(&intent.authority))?,intent.owner,now],|r|r.get(0)).map_err(sql)
    }
    pub fn renew_file_source_owner(
        &self,
        intent: &FileRefreshIntent,
        now: i64,
        seconds: i64,
    ) -> Result<(), Error> {
        let lease = until(now, seconds)?;
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed=db.execute("UPDATE file_refresh_intents SET lease_until=min(?6,deadline) WHERE workspace_id=?1 AND mutation_digest=?2 AND owner=?3 AND phase='held' AND lease_until>?4 AND deadline>?4 AND EXISTS(SELECT 1 FROM mutation_refresh m WHERE m.workspace_id=?1 AND m.mutation_digest=?2 AND m.page_id=?5 AND m.authority=?7 AND m.state=0 AND m.generation=0)",params![intent.key.workspace_id,intent.key.mutation_digest,intent.owner,now,intent.key.page_id,lease,encoded(Some(&intent.authority))?]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::ClaimLost)
        }
    }
    /// Only a live source owner may settle. Repeated identical settlement is
    /// idempotent; conflicting settlement and expired held owners are rejected.
    pub fn settle_file_refresh_intent(
        &self,
        intent: &FileRefreshIntent,
        now: i64,
        possible_effect: bool,
    ) -> Result<(), Error> {
        if now < 0 {
            return Err(Error::InvalidInput);
        }
        let phase = if possible_effect {
            "released"
        } else {
            "discarded"
        };
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let row:Option<(String,i64,String,String,i64,i64)>=tx.query_row("SELECT f.phase,f.lease_until,m.page_id,m.authority,m.state,m.generation FROM file_refresh_intents f JOIN mutation_refresh m USING(workspace_id,mutation_digest) WHERE f.workspace_id=?1 AND f.mutation_digest=?2 AND f.owner=?3",params![intent.key.workspace_id,intent.key.mutation_digest,intent.owner],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional().map_err(sql)?;
        let Some((stored, lease, page, authority, state, generation)) = row else {
            return Err(Error::ClaimLost);
        };
        if page != intent.key.page_id || authority != encoded(Some(&intent.authority))? {
            return Err(Error::Conflict);
        }
        if stored == phase {
            return Ok(());
        }
        if stored != "held" || lease <= now || state != 0 || generation != 0 {
            return Err(Error::ClaimLost);
        }
        tx.execute("UPDATE file_refresh_intents SET phase=?4 WHERE workspace_id=?1 AND mutation_digest=?2 AND owner=?3",params![intent.key.workspace_id,intent.key.mutation_digest,intent.owner,phase]).map_err(sql)?;
        if !possible_effect {
            tx.execute("UPDATE mutation_refresh SET state=3,generation=generation+1 WHERE workspace_id=?1 AND mutation_digest=?2",params![intent.key.workspace_id,intent.key.mutation_digest]).map_err(sql)?;
        }
        tx.commit().map_err(sql)
    }
    /// Trusted reconciliation explicitly asserts source quiescence. Expiry on
    /// its own never calls this method and never makes refresh eligible.
    pub fn reconcile_quiescent_file_intent(
        &self,
        key: &RefreshKey,
        authority: &GrantRefreshAuthority,
        now: i64,
    ) -> Result<(), Error> {
        authority_matches(key, Some(authority))?;
        if now < 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed=db.execute("UPDATE file_refresh_intents SET phase='released' WHERE workspace_id=?1 AND mutation_digest=?2 AND phase='held' AND lease_until<=?3 AND EXISTS(SELECT 1 FROM mutation_refresh m WHERE m.workspace_id=?1 AND m.mutation_digest=?2 AND m.page_id=?4 AND m.authority=?5 AND m.state=0 AND m.generation=0)",params![key.workspace_id,key.mutation_digest,now,key.page_id,encoded(Some(authority))?]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::ClaimLost)
        }
    }
}
