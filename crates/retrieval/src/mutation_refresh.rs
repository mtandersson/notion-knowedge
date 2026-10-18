//! Trusted mutation page queue in the operational database, never fake webhook
//! events. The existing authoritative refresh worker owns all index effects.
use crate::sync_state::SqliteSyncStateStore;
use notion_knowledge_core::write_refresh::{
    IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError as Error,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRefreshClaim {
    pub key: RefreshKey,
    pub generation: i64,
}

fn sql(_: rusqlite::Error) -> Error {
    Error::Unavailable
}
fn uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
fn valid(key: &RefreshKey) -> Result<(), Error> {
    if !uuid(&key.workspace_id)
        || !uuid(&key.page_id)
        || key.mutation_digest.len() != 64
        || !key
            .mutation_digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::InvalidInput);
    }
    Ok(())
}
fn state(s: i64) -> Result<IndexStatus, Error> {
    match s {
        0 => Ok(IndexStatus::Queued),
        1 => Ok(IndexStatus::Running),
        2 => Ok(IndexStatus::Completed),
        3 => Ok(IndexStatus::Failed),
        _ => Err(Error::Unavailable),
    }
}
impl MutationRefreshQueue for SqliteSyncStateStore {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, Error> {
        valid(key)?;
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        tx.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING", params![key.workspace_id,key.mutation_digest,key.page_id]).map_err(sql)?;
        let (page,s):(String,i64) = tx.query_row("SELECT page_id,state FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2",params![key.workspace_id,key.mutation_digest],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
        if page != key.page_id {
            return Err(Error::Conflict);
        }
        let status = state(s)?;
        tx.commit().map_err(sql)?;
        Ok(status)
    }
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, Error> {
        valid(key)?;
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let found:Option<(String,i64)> = db.query_row("SELECT page_id,state FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2",params![key.workspace_id,key.mutation_digest],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql)?;
        found
            .map(|(page, s)| {
                if page == key.page_id {
                    state(s)
                } else {
                    Err(Error::Conflict)
                }
            })
            .transpose()
    }
}
impl SqliteSyncStateStore {
    /// Explicit derived-index retry. This port has no Notion write capability;
    /// preserve the generation so an expired worker cannot acknowledge it.
    pub fn retry_mutation_refresh(&self, key: &RefreshKey) -> Result<(), Error> {
        valid(key)?;
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed = db.execute("UPDATE mutation_refresh SET state=0,attempts=0,lease_until=NULL WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND state=3",params![key.workspace_id,key.mutation_digest,key.page_id]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }
    pub fn claim_mutation_refresh(
        &self,
        workspace: &str,
        now: i64,
        lease_seconds: i64,
    ) -> Result<Option<MutationRefreshClaim>, Error> {
        if !uuid(workspace) || now < 0 || !(1..=3600).contains(&lease_seconds) {
            return Err(Error::InvalidInput);
        }
        let until = now.checked_add(lease_seconds).ok_or(Error::InvalidInput)?;
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        tx.execute("UPDATE mutation_refresh SET state=CASE WHEN attempts>=3 THEN 3 ELSE 0 END,lease_until=NULL WHERE workspace_id=?1 AND state=1 AND lease_until<=?2",params![workspace,now]).map_err(sql)?;
        let row:Option<(String,String,i64)> = tx.query_row("SELECT mutation_digest,page_id,generation+1 FROM mutation_refresh WHERE workspace_id=?1 AND state=0 AND attempts<3 AND generation<9223372036854775807 ORDER BY mutation_digest LIMIT 1",params![workspace],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql)?;
        let Some((digest, page, generation)) = row else {
            tx.commit().map_err(sql)?;
            return Ok(None);
        };
        tx.execute("UPDATE mutation_refresh SET state=1,generation=?3,attempts=attempts+1,lease_until=?4 WHERE workspace_id=?1 AND mutation_digest=?2",params![workspace,digest,generation,until]).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Some(MutationRefreshClaim {
            key: RefreshKey {
                workspace_id: workspace.to_owned(),
                mutation_digest: digest,
                page_id: page,
            },
            generation,
        }))
    }
    pub fn mutation_refresh_is_current(
        &self,
        claim: &MutationRefreshClaim,
        now: i64,
    ) -> Result<bool, Error> {
        valid(&claim.key)?;
        if now < 0 || claim.generation <= 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        db.query_row("SELECT EXISTS(SELECT 1 FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND generation=?4 AND state=1 AND lease_until>?5)",params![claim.key.workspace_id,claim.key.mutation_digest,claim.key.page_id,claim.generation,now],|r|r.get(0)).map_err(sql)
    }
    /// Success only after the shared guarded refresh commits. A failed derived
    /// refresh is terminal and observable; it never changes the mutation ledger.
    pub fn complete_mutation_refresh(
        &self,
        claim: &MutationRefreshClaim,
        now: i64,
        succeeded: bool,
    ) -> Result<(), Error> {
        valid(&claim.key)?;
        if now < 0 || claim.generation <= 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed = db.execute("UPDATE mutation_refresh SET state=?6,lease_until=NULL WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND generation=?4 AND state=1 AND lease_until>?5",params![claim.key.workspace_id,claim.key.mutation_digest,claim.key.page_id,claim.generation,now,if succeeded {2} else {3}]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::ClaimLost)
        }
    }
}
