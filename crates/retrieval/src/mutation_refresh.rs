//! Trusted mutation page queue in the operational database, never fake webhook
//! events. The existing authoritative refresh worker owns all index effects.
use crate::sync_state::SqliteSyncStateStore;
use notion_knowledge_core::write_refresh::{
    GrantRefreshAuthority, IndexStatus, MutationRefreshQueue, RefreshKey,
    RefreshQueueError as Error,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRefreshClaim {
    pub key: RefreshKey,
    pub generation: i64,
    pub authority: Option<GrantRefreshAuthority>,
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
pub(crate) fn encoded(authority: Option<&GrantRefreshAuthority>) -> Result<String, Error> {
    match authority {
        None => Ok("integration".into()),
        Some(value) => {
            value.validate()?;
            serde_json::to_string(value).map_err(|_| Error::InvalidInput)
        }
    }
}
pub(crate) fn authority_matches(
    key: &RefreshKey,
    authority: Option<&GrantRefreshAuthority>,
) -> Result<(), Error> {
    valid(key)?;
    if authority.is_some_and(|a| a.workspace_id() != key.workspace_id) {
        return Err(Error::Conflict);
    }
    Ok(())
}
/// Trusted per-request admission/status contract shared by semantic and file
/// completion. Only non-secret immutable authority is persisted.
pub struct GrantMutationRefreshQueue {
    state: std::sync::Arc<SqliteSyncStateStore>,
    authority: GrantRefreshAuthority,
}
impl GrantMutationRefreshQueue {
    pub fn new(
        state: std::sync::Arc<SqliteSyncStateStore>,
        authority: GrantRefreshAuthority,
    ) -> Result<Self, Error> {
        authority.validate()?;
        Ok(Self { state, authority })
    }
    pub fn authority(&self) -> &GrantRefreshAuthority {
        &self.authority
    }
    pub fn retry(&self, key: &RefreshKey) -> Result<(), Error> {
        self.state.retry_refresh(key, Some(&self.authority))
    }
    /// Explicit derived-work cancellation, independent of any verified source
    /// receipt. A new claim generation fences a worker already preparing data.
    pub fn cancel(&self, key: &RefreshKey) -> Result<(), Error> {
        authority_matches(key, Some(&self.authority))?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| Error::Unavailable)?;
        let changed = db.execute("UPDATE mutation_refresh SET state=3,generation=generation+1,lease_until=NULL WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND authority=?4 AND state IN (0,1) AND generation<9223372036854775807", params![key.workspace_id,key.mutation_digest,key.page_id,encoded(Some(&self.authority))?]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }
}
impl MutationRefreshQueue for GrantMutationRefreshQueue {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, Error> {
        self.state.admit_refresh(key, Some(&self.authority))
    }
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, Error> {
        self.state.refresh_status(key, Some(&self.authority))
    }
}
impl MutationRefreshQueue for SqliteSyncStateStore {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, Error> {
        self.admit_refresh(key, None)
    }
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, Error> {
        self.refresh_status(key, None)
    }
}
impl SqliteSyncStateStore {
    fn admit_refresh(
        &self,
        key: &RefreshKey,
        authority: Option<&GrantRefreshAuthority>,
    ) -> Result<IndexStatus, Error> {
        authority_matches(key, authority)?;
        let authority = encoded(authority)?;
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        tx.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id,authority) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING", params![key.workspace_id,key.mutation_digest,key.page_id,authority]).map_err(sql)?;
        let (page, stored, status): (String,String,i64) = tx.query_row("SELECT page_id,authority,state FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2", params![key.workspace_id,key.mutation_digest],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(sql)?;
        if page != key.page_id || stored != authority {
            return Err(Error::Conflict);
        }
        let status = state(status)?;
        tx.commit().map_err(sql)?;
        Ok(status)
    }
    pub(crate) fn refresh_status(
        &self,
        key: &RefreshKey,
        authority: Option<&GrantRefreshAuthority>,
    ) -> Result<Option<IndexStatus>, Error> {
        authority_matches(key, authority)?;
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let found: Option<(String,String,i64)> = db.query_row("SELECT page_id,authority,state FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2",params![key.workspace_id,key.mutation_digest],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql)?;
        found
            .map(|(page, stored, s)| {
                if page == key.page_id && stored == encoded(authority)? {
                    state(s)
                } else {
                    Err(Error::Conflict)
                }
            })
            .transpose()
    }
    pub fn retry_mutation_refresh(&self, key: &RefreshKey) -> Result<(), Error> {
        self.retry_refresh(key, None)
    }
    fn retry_refresh(
        &self,
        key: &RefreshKey,
        authority: Option<&GrantRefreshAuthority>,
    ) -> Result<(), Error> {
        authority_matches(key, authority)?;
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed = db.execute("UPDATE mutation_refresh SET state=0,attempts=0,lease_until=NULL WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND authority=?4 AND state=3 AND NOT EXISTS(SELECT 1 FROM file_refresh_intents f WHERE f.workspace_id=mutation_refresh.workspace_id AND f.mutation_digest=mutation_refresh.mutation_digest AND f.phase!='released')",params![key.workspace_id,key.mutation_digest,key.page_id,encoded(authority)?]).map_err(sql)?;
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
        self.claim_refresh(workspace, now, lease_seconds, false)
    }
    /// OAuth-only admission. Every returned immutable authority must still be
    /// validated against live sealed credentials and exact current root policy.
    pub fn claim_grant_mutation_refresh(
        &self,
        workspace: &str,
        now: i64,
        lease_seconds: i64,
    ) -> Result<Option<MutationRefreshClaim>, Error> {
        self.claim_refresh(workspace, now, lease_seconds, true)
    }
    fn claim_refresh(
        &self,
        workspace: &str,
        now: i64,
        lease_seconds: i64,
        oauth: bool,
    ) -> Result<Option<MutationRefreshClaim>, Error> {
        if !uuid(workspace) || now < 0 || !(1..=3600).contains(&lease_seconds) {
            return Err(Error::InvalidInput);
        }
        let until = now.checked_add(lease_seconds).ok_or(Error::InvalidInput)?;
        let mut db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        tx.execute("UPDATE mutation_refresh SET state=CASE WHEN attempts>=3 THEN 3 ELSE 0 END,lease_until=NULL WHERE workspace_id=?1 AND (authority!='integration')=?3 AND state=1 AND lease_until<=?2",params![workspace,now,oauth]).map_err(sql)?;
        let row: Option<(String,String,i64,String)> = tx.query_row("SELECT mutation_digest,page_id,generation+1,authority FROM mutation_refresh WHERE workspace_id=?1 AND (authority!='integration')=?2 AND state=0 AND attempts<3 AND generation<9223372036854775807 AND NOT EXISTS(SELECT 1 FROM file_refresh_intents f WHERE f.workspace_id=mutation_refresh.workspace_id AND f.mutation_digest=mutation_refresh.mutation_digest AND f.phase!='released') ORDER BY mutation_digest LIMIT 1",params![workspace,oauth],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(sql)?;
        let Some((digest, page, generation, stored)) = row else {
            tx.commit().map_err(sql)?;
            return Ok(None);
        };
        let authority = if oauth {
            let value: GrantRefreshAuthority =
                serde_json::from_str(&stored).map_err(|_| Error::Unavailable)?;
            if encoded(Some(&value))? != stored || value.workspace_id() != workspace {
                return Err(Error::Unavailable);
            }
            Some(value)
        } else {
            None
        };
        tx.execute("UPDATE mutation_refresh SET state=1,generation=?3,attempts=attempts+1,lease_until=?4 WHERE workspace_id=?1 AND mutation_digest=?2 AND authority=?5",params![workspace,digest,generation,until,stored]).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Some(MutationRefreshClaim {
            key: RefreshKey {
                workspace_id: workspace.into(),
                mutation_digest: digest,
                page_id: page,
            },
            generation,
            authority,
        }))
    }
    pub fn mutation_refresh_is_current(
        &self,
        claim: &MutationRefreshClaim,
        now: i64,
    ) -> Result<bool, Error> {
        authority_matches(&claim.key, claim.authority.as_ref())?;
        if now < 0 || claim.generation <= 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        db.query_row("SELECT EXISTS(SELECT 1 FROM mutation_refresh WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND generation=?4 AND state=1 AND lease_until>?5 AND authority=?6)",params![claim.key.workspace_id,claim.key.mutation_digest,claim.key.page_id,claim.generation,now,encoded(claim.authority.as_ref())?],|r|r.get(0)).map_err(sql)
    }
    pub fn complete_mutation_refresh(
        &self,
        claim: &MutationRefreshClaim,
        now: i64,
        succeeded: bool,
    ) -> Result<(), Error> {
        authority_matches(&claim.key, claim.authority.as_ref())?;
        if now < 0 || claim.generation <= 0 {
            return Err(Error::InvalidInput);
        }
        let db = self.lock_connection().map_err(|_| Error::Unavailable)?;
        let changed = db.execute("UPDATE mutation_refresh SET state=?6,lease_until=NULL WHERE workspace_id=?1 AND mutation_digest=?2 AND page_id=?3 AND generation=?4 AND state=1 AND lease_until>?5 AND authority=?7",params![claim.key.workspace_id,claim.key.mutation_digest,claim.key.page_id,claim.generation,now,if succeeded {2} else {3},encoded(claim.authority.as_ref())?]).map_err(sql)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::ClaimLost)
        }
    }
}
