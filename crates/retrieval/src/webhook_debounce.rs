//! Durable per-page batches over the event inbox, with per-event retry budgets.
use crate::{
    sync_state::SqliteSyncStateStore,
    webhook::{complete_event, snapshot},
};
use notion_knowledge_core::webhook::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
fn sql(_: rusqlite::Error) -> InboxError {
    InboxError::Unavailable
}
fn scope(scope: &InboxScope) -> Result<InboxScope, InboxError> {
    fn id(s: &str) -> bool {
        s.len() == 36
            && s.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
    }
    if !id(&scope.workspace_id) || !id(&scope.subscription_id) {
        return Err(InboxError::InvalidInput);
    }
    Ok(InboxScope {
        workspace_id: scope.workspace_id.to_ascii_lowercase(),
        subscription_id: scope.subscription_id.to_ascii_lowercase(),
    })
}
fn later(a: &WebhookEvent, b: &WebhookEvent) -> Result<bool, InboxError> {
    let a_time = a.timestamp_order().ok_or(InboxError::Unavailable)?;
    let b_time = b.timestamp_order().ok_or(InboxError::Unavailable)?;
    Ok((a_time, a.id.as_str()) > (b_time, b.id.as_str()))
}
fn members(db: &Connection, k: &PageWorkKey, batch: i64) -> Result<Vec<InboxEvent>, InboxError> {
    let mut stmt=db.prepare("SELECT event_id FROM webhook_page_members WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3 AND batch=?4 ORDER BY event_id").map_err(sql)?;
    let ids = stmt
        .query_map(
            params![
                k.scope.workspace_id,
                k.scope.subscription_id,
                k.page_id,
                batch
            ],
            |r| r.get::<_, String>(0),
        )
        .map_err(sql)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql)?;
    ids.into_iter()
        .map(|id| {
            snapshot(
                db,
                &EventKey {
                    workspace_id: k.scope.workspace_id.clone(),
                    subscription_id: k.scope.subscription_id.clone(),
                    event_id: id,
                },
            )?
            .ok_or(InboxError::Unavailable)
        })
        .collect()
}
fn newest(events: &[InboxEvent]) -> Result<WebhookEvent, InboxError> {
    let mut latest = events.first().ok_or(InboxError::Unavailable)?.event.clone();
    for event in events.iter().skip(1) {
        if later(&event.event, &latest)? {
            latest = event.event.clone();
        }
    }
    Ok(latest)
}
pub(crate) fn enqueue(
    db: &Connection,
    event: &WebhookEvent,
    now: i64,
    window: DebounceWindow,
) -> Result<(), InboxError> {
    if event.entity_type != "page"
        || !matches!(
            event.event_type.as_str(),
            "page.content_updated" | "page.properties_updated"
        )
    {
        return Ok(());
    }
    let k = PageWorkKey {
        scope: InboxScope {
            workspace_id: event.workspace_id.clone(),
            subscription_id: event.subscription_id.clone(),
        },
        page_id: event.entity_id.clone(),
    };
    let quiet = now
        .checked_add(window.quiet_ms)
        .ok_or(InboxError::InvalidInput)?;
    let cap = now
        .checked_add(window.max_delay_ms)
        .ok_or(InboxError::InvalidInput)?;
    let prior = members(db, &k, 0)?;
    db.execute("INSERT INTO webhook_page_work(workspace_id,subscription_id,page_id,first_ms,due_ms,quiet_ms,max_delay_ms) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",params![k.scope.workspace_id,k.scope.subscription_id,k.page_id,now,quiet.min(cap),window.quiet_ms,window.max_delay_ms]).map_err(sql)?;
    if prior.is_empty() {
        db.execute("UPDATE webhook_page_work SET first_ms=?4,due_ms=?5,quiet_ms=?6,max_delay_ms=?7 WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![k.scope.workspace_id,k.scope.subscription_id,k.page_id,now,quiet.min(cap),window.quiet_ms,window.max_delay_ms]).map_err(sql)?;
    } else if later(event, &newest(&prior)?)? {
        let (first,quiet,cap):(i64,i64,i64)=db.query_row("SELECT first_ms,quiet_ms,max_delay_ms FROM webhook_page_work WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![k.scope.workspace_id,k.scope.subscription_id,k.page_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(sql)?;
        let due = now
            .checked_add(quiet)
            .ok_or(InboxError::InvalidInput)?
            .min(first.checked_add(cap).ok_or(InboxError::InvalidInput)?);
        db.execute("UPDATE webhook_page_work SET due_ms=max(due_ms,?4) WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![k.scope.workspace_id,k.scope.subscription_id,k.page_id,due]).map_err(sql)?;
    }
    db.execute("INSERT INTO webhook_page_members(workspace_id,subscription_id,event_id,page_id) VALUES(?1,?2,?3,?4)",params![event.workspace_id,event.subscription_id,event.id,event.entity_id]).map_err(sql)?;
    Ok(())
}
impl WebhookDebounce for SqliteSyncStateStore {
    fn receive_debounced(
        &self,
        event: &WebhookEvent,
        now_ms: i64,
        window: DebounceWindow,
    ) -> Result<Receipt, InboxError> {
        window.validate()?;
        if now_ms < 0 || now_ms.checked_add(window.max_delay_ms).is_none() {
            return Err(InboxError::InvalidInput);
        }
        self.receive_inner(event, Some((now_ms, window)))
    }
    fn claim_page(
        &self,
        input: &InboxScope,
        now_ms: i64,
        lease_seconds: i64,
    ) -> Result<Option<PageWorkClaim>, InboxError> {
        let s = scope(input)?;
        if now_ms < 0 || !(1..=3600).contains(&lease_seconds) {
            return Err(InboxError::InvalidInput);
        }
        let until = now_ms
            .checked_add(lease_seconds * 1000)
            .ok_or(InboxError::InvalidInput)?;
        let now = now_ms / 1000;
        let mut db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        // Recovery consumes the same bounded budget as ordinary event claims.
        let mut stmt=tx.prepare("SELECT page_id,generation FROM webhook_page_work WHERE workspace_id=?1 AND subscription_id=?2 AND lease_until_ms<=?3").map_err(sql)?;
        let expired = stmt
            .query_map(params![s.workspace_id, s.subscription_id, now_ms], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql)?;
        drop(stmt);
        for (page, generation) in expired {
            let k = PageWorkKey {
                scope: s.clone(),
                page_id: page,
            };
            for event in members(&tx, &k, generation)? {
                let key = event.event.key();
                if event.cycle_attempts >= event.policy.max_attempts {
                    tx.execute("UPDATE webhook_inbox SET state=3,failure=3,last_failure=3,lease_until=NULL,retry_at=NULL WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![key.workspace_id,key.subscription_id,key.event_id]).map_err(sql)?;
                    remove(&tx, &key)?;
                } else {
                    tx.execute("UPDATE webhook_inbox SET state=0,lease_until=NULL,retry_at=?4,last_failure=3 WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![key.workspace_id,key.subscription_id,key.event_id,now]).map_err(sql)?;
                    tx.execute("UPDATE webhook_page_members SET batch=0 WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![key.workspace_id,key.subscription_id,key.event_id]).map_err(sql)?;
                }
            }
            tx.execute("UPDATE webhook_page_work SET lease_until_ms=NULL,due_ms=coalesce(due_ms,?4),first_ms=coalesce(first_ms,?4) WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![s.workspace_id,s.subscription_id,k.page_id,now_ms]).map_err(sql)?;
        }
        let page=tx.query_row("SELECT p.page_id FROM webhook_page_work p WHERE p.workspace_id=?1 AND p.subscription_id=?2 AND p.lease_until_ms IS NULL AND p.due_ms<=?3 AND p.generation<9223372036854775807 AND EXISTS(SELECT 1 FROM webhook_page_members m JOIN webhook_inbox e USING(workspace_id,subscription_id,event_id) WHERE m.workspace_id=p.workspace_id AND m.subscription_id=p.subscription_id AND m.page_id=p.page_id AND m.batch=0 AND e.state=0 AND (e.retry_at IS NULL OR e.retry_at<=?4) AND e.cycle_attempts<e.max_attempts AND e.generation<9223372036854775807 AND e.lifetime_attempts<9223372036854775807) ORDER BY p.due_ms,p.page_id LIMIT 1",params![s.workspace_id,s.subscription_id,now_ms,now],|r|r.get::<_,String>(0)).optional().map_err(sql)?;
        let Some(page) = page else {
            tx.commit().map_err(sql)?;
            return Ok(None);
        };
        let k = PageWorkKey {
            scope: s.clone(),
            page_id: page,
        };
        let generation:i64=tx.query_row("SELECT generation+1 FROM webhook_page_work WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![s.workspace_id,s.subscription_id,k.page_id],|r|r.get(0)).map_err(sql)?;
        let events = members(&tx, &k, 0)?
            .into_iter()
            .filter(|e| {
                e.state == EventState::Pending
                    && e.retry_at.is_none_or(|t| t <= now)
                    && e.cycle_attempts < e.policy.max_attempts
                    && e.generation < i64::MAX
                    && e.lifetime_attempts < i64::MAX
            })
            .collect::<Vec<_>>();
        let latest = newest(&events)?;
        let mut claims = Vec::new();
        for event in events {
            let key = event.event.key();
            let gen_ = event.generation + 1;
            tx.execute("UPDATE webhook_inbox SET state=1,generation=?4,lease_until=?5,cycle_attempts=cycle_attempts+1,lifetime_attempts=lifetime_attempts+1,retry_at=NULL WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![key.workspace_id,key.subscription_id,key.event_id,gen_,until/1000+i64::from(until%1000!=0)]).map_err(sql)?;
            tx.execute("UPDATE webhook_page_members SET batch=?4 WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![key.workspace_id,key.subscription_id,key.event_id,generation]).map_err(sql)?;
            claims.push(EventClaim {
                key,
                generation: gen_,
            });
        }
        tx.execute("UPDATE webhook_page_work SET generation=?4,lease_until_ms=?5 WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![s.workspace_id,s.subscription_id,k.page_id,generation,until]).map_err(sql)?;
        reset_pending(&tx, &k)?;
        tx.commit().map_err(sql)?;
        Ok(Some(PageWorkClaim {
            key: k,
            generation,
            events: claims,
            newest: latest,
        }))
    }
    fn complete_page(
        &self,
        claim: &PageWorkClaim,
        now_ms: i64,
        outcome: ProcessingOutcome,
    ) -> Result<(), InboxError> {
        let s = scope(&claim.key.scope)?;
        if now_ms < 0 || claim.generation <= 0 || s != claim.key.scope {
            return Err(InboxError::InvalidInput);
        }
        let mut db = self
            .lock_connection()
            .map_err(|_| InboxError::Unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let live:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM webhook_page_work WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3 AND generation=?4 AND lease_until_ms>?5)",params![s.workspace_id,s.subscription_id,claim.key.page_id,claim.generation,now_ms],|r|r.get(0)).map_err(sql)?;
        if !live {
            return Err(InboxError::ClaimLost);
        }
        let events = members(&tx, &claim.key, claim.generation)?;
        let actual = events
            .iter()
            .map(|e| EventClaim {
                key: e.event.key(),
                generation: e.generation,
            })
            .collect::<Vec<_>>();
        if actual != claim.events || newest(&events)? != claim.newest {
            return Err(InboxError::ClaimLost);
        }
        for event in &actual {
            complete_event(&tx, event, now_ms / 1000, outcome)?;
            let after = snapshot(&tx, &event.key)?.ok_or(InboxError::Unavailable)?;
            if after.state == EventState::Pending {
                tx.execute("UPDATE webhook_page_members SET batch=0 WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![event.key.workspace_id,event.key.subscription_id,event.key.event_id]).map_err(sql)?;
            } else {
                remove(&tx, &event.key)?;
            }
        }
        tx.execute("UPDATE webhook_page_work SET lease_until_ms=NULL WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![s.workspace_id,s.subscription_id,claim.key.page_id]).map_err(sql)?;
        // A retry remains governed by its own retry_at even if a fresh event's
        // shorter quiet period makes the page eligible sooner.
        if !members(&tx, &claim.key, 0)?.is_empty() {
            tx.execute("UPDATE webhook_page_work SET first_ms=coalesce(first_ms,?4),due_ms=coalesce(due_ms,?4) WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![s.workspace_id,s.subscription_id,claim.key.page_id,now_ms]).map_err(sql)?;
        }
        tx.commit().map_err(sql)
    }
}
fn remove(db: &Connection, k: &EventKey) -> Result<(), InboxError> {
    db.execute("DELETE FROM webhook_page_members WHERE workspace_id=?1 AND subscription_id=?2 AND event_id=?3",params![k.workspace_id,k.subscription_id,k.event_id]).map_err(sql)?;
    Ok(())
}
fn reset_pending(db: &Connection, k: &PageWorkKey) -> Result<(), InboxError> {
    if members(db, k, 0)?.is_empty() {
        db.execute("UPDATE webhook_page_work SET first_ms=NULL,due_ms=NULL WHERE workspace_id=?1 AND subscription_id=?2 AND page_id=?3",params![k.scope.workspace_id,k.scope.subscription_id,k.page_id]).map_err(sql)?;
    }
    Ok(())
}
