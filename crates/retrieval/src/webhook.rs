//! Durable, minimal webhook hints. No raw bodies, credentials or signatures.
use crate::sync_state::SqliteSyncStateStore;
use notion_knowledge_core::webhook::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
fn sql(_: rusqlite::Error) -> InboxError {
    InboxError::Unavailable
}
fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}
fn key(key: &EventKey) -> Result<EventKey, InboxError> {
    if ![&key.workspace_id, &key.subscription_id, &key.event_id]
        .into_iter()
        .all(|s| valid_id(s))
    {
        return Err(InboxError::InvalidInput);
    }
    Ok(EventKey {
        workspace_id: key.workspace_id.to_ascii_lowercase(),
        subscription_id: key.subscription_id.to_ascii_lowercase(),
        event_id: key.event_id.to_ascii_lowercase(),
    })
}
fn normalized(event: &WebhookEvent) -> Result<WebhookEvent, InboxError> {
    key(&event.key())?;
    if !valid_id(&event.integration_id)
        || !valid_id(&event.entity_id)
        || event.attempt_number == 0
        || event.timestamp.len() > 128
        || !event.valid_timestamp()
        || event.event_type.is_empty()
        || event.event_type.len() > 128
        || !event
            .event_type
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b == b'.' || b == b'_')
        || !matches!(
            event.entity_type.as_str(),
            "page" | "block" | "database" | "data_source" | "comment"
        )
    {
        return Err(InboxError::InvalidInput);
    }
    let mut event = event.clone();
    event.id.make_ascii_lowercase();
    event.workspace_id.make_ascii_lowercase();
    event.subscription_id.make_ascii_lowercase();
    event.integration_id.make_ascii_lowercase();
    event.entity_id.make_ascii_lowercase();
    Ok(event)
}
fn snapshot(db: &Connection, k: &EventKey) -> Result<Option<InboxEvent>, InboxError> {
    db.query_row("SELECT integration_id,event_timestamp,event_type,entity_id,entity_type,attempt_number,state,generation,lease_until,failure FROM webhook_inbox WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![k.workspace_id,k.subscription_id,k.event_id],|r| {
        let state=match r.get::<_,i64>(6)? {0=>EventState::Pending,1=>EventState::Running,2=>EventState::Succeeded,3=>EventState::Failed,_=>return Err(rusqlite::Error::InvalidQuery)};
        let failure=match r.get::<_,Option<i64>>(9)? {None=>None,Some(0)=>Some(EventFailure::Source),Some(1)=>Some(EventFailure::Index),Some(2)=>Some(EventFailure::Conflict),Some(3)=>Some(EventFailure::Unavailable),_=>return Err(rusqlite::Error::InvalidQuery)};
        Ok(InboxEvent {event:WebhookEvent{id:k.event_id.clone(),workspace_id:k.workspace_id.clone(),subscription_id:k.subscription_id.clone(),integration_id:r.get(0)?,timestamp:r.get(1)?,event_type:r.get(2)?,entity_id:r.get(3)?,entity_type:r.get(4)?,attempt_number:r.get(5)?},state,generation:r.get(7)?,lease_until:r.get(8)?,failure})
    }).optional().map_err(sql)
}
impl WebhookInbox for SqliteSyncStateStore {
    fn receive(&self, event: &WebhookEvent) -> Result<Receipt, InboxError> {
        let event = normalized(event)?;
        let k = event.key();
        let mut db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        if let Some(old) = snapshot(&tx, &k)? {
            let mut expected = old.event;
            expected.attempt_number = event.attempt_number;
            if expected != event {
                return Err(InboxError::Conflict);
            }
            // Retain first-received hints and processing state across all redeliveries.
            tx.commit().map_err(sql)?;
            return Ok(Receipt::Duplicate);
        }
        // Identity-only legacy entries cannot prove matching payload or pending work.
        // Preserve them and fail closed rather than fabricate or acknowledge lost hints.
        let legacy: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM webhook_events WHERE lower(event_id)=?1)",
                [&event.id],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if legacy {
            return Err(InboxError::Conflict);
        }
        tx.execute("INSERT INTO webhook_inbox(workspace_id,subscription_id,event_id,integration_id,event_timestamp,event_type,entity_id,entity_type,attempt_number) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![event.workspace_id,event.subscription_id,event.id,event.integration_id,event.timestamp,event.event_type,event.entity_id,event.entity_type,event.attempt_number]).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Receipt::Inserted)
    }
    fn event(&self, k: &EventKey) -> Result<Option<InboxEvent>, InboxError> {
        let k = key(k)?;
        let db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        snapshot(&db, &k)
    }
    fn claim(
        &self,
        scope: &InboxScope,
        now: i64,
        lease_seconds: i64,
    ) -> Result<Option<(EventClaim, InboxEvent)>, InboxError> {
        if !valid_id(&scope.workspace_id)
            || !valid_id(&scope.subscription_id)
            || now < 0
            || !(1..=3600).contains(&lease_seconds)
        {
            return Err(InboxError::InvalidInput);
        }
        let until = now
            .checked_add(lease_seconds)
            .ok_or(InboxError::InvalidInput)?;
        let mut db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let k=tx.query_row("SELECT workspace_id,subscription_id,event_id FROM webhook_inbox WHERE workspace_id=?2 AND subscription_id=?3 AND (state=0 OR (state=1 AND lease_until<=?1)) ORDER BY received_at_unix,workspace_id,subscription_id,event_id LIMIT 1",params![now,scope.workspace_id.to_ascii_lowercase(),scope.subscription_id.to_ascii_lowercase()],|r|Ok(EventKey{workspace_id:r.get(0)?,subscription_id:r.get(1)?,event_id:r.get(2)?})).optional().map_err(sql)?;
        let Some(k) = k else {
            tx.commit().map_err(sql)?;
            return Ok(None);
        };
        let prior = snapshot(&tx, &k)?.ok_or(InboxError::Unavailable)?;
        let generation = prior
            .generation
            .checked_add(1)
            .ok_or(InboxError::Unavailable)?;
        tx.execute("UPDATE webhook_inbox SET state=1,generation=?1,lease_until=?2 WHERE workspace_id=?3 AND subscription_id=?4 AND event_id=?5",params![generation,until,k.workspace_id,k.subscription_id,k.event_id]).map_err(sql)?;
        let event = snapshot(&tx, &k)?.ok_or(InboxError::Unavailable)?;
        tx.commit().map_err(sql)?;
        Ok(Some((EventClaim { key: k, generation }, event)))
    }
    fn finish(
        &self,
        claim: &EventClaim,
        now: i64,
        failure: Option<EventFailure>,
    ) -> Result<(), InboxError> {
        let k = key(&claim.key)?;
        if now < 0 || claim.generation <= 0 {
            return Err(InboxError::InvalidInput);
        }
        let failure = failure.map(|f| match f {
            EventFailure::Source => 0,
            EventFailure::Index => 1,
            EventFailure::Conflict => 2,
            EventFailure::Unavailable => 3,
        });
        let db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        let n=db.execute("UPDATE webhook_inbox SET state=?1,failure=?2,lease_until=NULL WHERE workspace_id=?3 AND subscription_id=?4 AND event_id=?5 AND state=1 AND generation=?6 AND lease_until>?7",params![if failure.is_some(){3}else{2},failure,k.workspace_id,k.subscription_id,k.event_id,claim.generation,now]).map_err(sql)?;
        if n == 1 {
            Ok(())
        } else {
            Err(InboxError::ClaimLost)
        }
    }
}
