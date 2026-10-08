//! Local operator commands. No source requests or transport startup.
use notion_knowledge_core::webhook::*;
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use serde_json::{Value, json};
fn report(e: InboxEvent) -> Value {
    json!({"event_id":e.event.id,"state":format!("{:?}",e.state),"generation":e.generation,"processing_attempts":e.cycle_attempts,"lifetime_attempts":e.lifetime_attempts,"notion_attempt_number":e.event.attempt_number,"retry_at":e.retry_at,"last_failure":e.last_failure.map(|f|format!("{f:?}")),"failure":e.failure.map(|f|format!("{f:?}")),"max_attempts":e.policy.max_attempts,"base_seconds":e.policy.base_seconds,"max_seconds":e.policy.max_seconds})
}
/// Exact positional forms deliberately reject extra arguments before opening state.
pub fn run(args: &[String]) -> Result<Value, &'static str> {
    if args.len() < 5 {
        return Err(
            "expected command, existing state file, workspace UUID, subscription UUID and limit or event UUID",
        );
    }
    let command = args[0].as_str();
    if !matches!(
        (command, args.len()),
        ("--webhook-failed", 5) | ("--webhook-inspect", 5) | ("--webhook-retry", 9)
    ) {
        return Err("invalid webhook operator arguments");
    }
    let valid_id = |id: &str| {
        id.len() == 36
            && id.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
    };
    if !valid_id(&args[2])
        || !valid_id(&args[3])
        || (command != "--webhook-failed" && !valid_id(&args[4]))
    {
        return Err("invalid identity");
    }
    let scope = InboxScope {
        workspace_id: args[2].clone(),
        subscription_id: args[3].clone(),
    };
    let k = EventKey {
        workspace_id: args[2].clone(),
        subscription_id: args[3].clone(),
        event_id: args[4].clone(),
    };
    let number = |s: &str| s.parse::<i64>().map_err(|_| "invalid integer argument");
    let limit = if command == "--webhook-failed" {
        Some(number(&args[4])?)
    } else {
        None
    };
    if limit.is_some_and(|l| !(1..=1000).contains(&l)) {
        return Err("invalid list limit");
    }
    let policy = if command == "--webhook-retry" {
        Some(
            RetryPolicy {
                max_attempts: number(&args[6])?,
                base_seconds: number(&args[7])?,
                max_seconds: number(&args[8])?,
            }
            .validate()
            .map_err(|_| "invalid retry policy")?,
        )
    } else {
        None
    };
    let generation = if command == "--webhook-retry" {
        Some(number(&args[5])?)
    } else {
        None
    };
    let path = std::path::Path::new(&args[1]);
    if !path.is_absolute() || !path.is_file() {
        return Err("state file must already exist at an absolute path");
    }
    let store = SqliteSyncStateStore::open(path).map_err(|_| "state unavailable")?;
    match command {
        "--webhook-failed"=>store.failed(&scope,limit.unwrap()).map(|events|json!({"events":events.into_iter().map(report).collect::<Vec<_>>(),"limit":limit})).map_err(|_|"invalid scope or state unavailable"),
        "--webhook-inspect"=>store.event(&k).map_err(|_|"invalid identity or state unavailable")?.map(report).ok_or("event not found"),
        _=>{
            let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|"clock unavailable")?.as_secs();
            let now=i64::try_from(now).map_err(|_|"clock unavailable")?;
            store.requeue(&k,generation.unwrap(),now,policy.unwrap()).map_err(|_|"retry rejected: invalid identity, stale generation, nonfailed event or unavailable state")?;
            Ok(json!({"requeued":true}))
        }
    }
}
