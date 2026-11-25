use std::sync::Arc;

use notion_knowledge_core::audit::AuditRetention;
use notion_knowledge_mcp::KnowledgeServer;
use notion_knowledge_retrieval::{
    audit::SqliteAuditStore, idempotency::SqliteIdempotencyStore, sync_state::SqliteSyncStateStore,
};
use serde_json::{Value, json};

use super::ingestion_tests::{http_call, notion};
use crate::{
    config::Config,
    root_scope::{self, Transport},
};

const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
const SOURCE: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const DATABASE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const ROW: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const RELATION_TARGET: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const KEY: &str = "journal-entry-unique-101";
const BODY: &str = "Journal entry";
const REVISION: &str = "2026-11-25T16:00:00Z";
const TOOL: &str = "knowledge_create_row";

fn config(path: &std::path::Path, read_only: bool) -> Config {
    let values = [
        ("NK_READ_ONLY", if read_only { "true" } else { "false" }),
        ("NK_NOTION_AUTH", "integration"),
        ("NOTION_TOKEN", "fixture-secret"),
        ("NK_NOTION_SCOPE_ROOTS", ROOT),
        ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
        ("NK_HTTP_AUTH", "bearer"),
        (
            "NK_HTTP_BEARER_TOKEN",
            "local-fixture-bearer-with-32-characters",
        ),
    ];
    let mut config = Config::from_lookup(|key| {
        values
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.into())
    })
    .unwrap();
    config.webhook_state_file = Some(path.join("state.sqlite"));
    config
}

fn schema() -> Vec<(u16, Value)> {
    vec![
        (
            200,
            json!({"object":"data_source","id":SOURCE,"parent":{"type":"database_id","database_id":DATABASE},
            "properties":{
                "Renamed entry title":{"id":"title","type":"title","title":{}},
                "Due date":{"id":"date-id","type":"date","date":{}},
                "State":{"id":"status-id","type":"status","status":{"options":[{"id":"done-id","name":"Done"}]}},
                "Points":{"id":"number-id","type":"number","number":{}},
                "Sent":{"id":"check-id","type":"checkbox","checkbox":{}},
                "Reference URL":{"id":"url-id","type":"url","url":{}},
                "Link":{"id":"related-id","type":"relation","relation":{"data_source_id":SOURCE,"limit":1}}
            }}),
        ),
        (
            200,
            json!({"object":"database","id":DATABASE,"in_trash":false,
            "parent":{"type":"page_id","page_id":ROOT}}),
        ),
        (
            200,
            json!({"object":"page","id":ROOT,"in_trash":false,"parent":{"type":"workspace"}}),
        ),
    ]
}

fn row() -> Value {
    json!({"object":"page","id":ROW,"in_trash":false,
        "parent":{"type":"data_source_id","data_source_id":SOURCE},
        "url":format!("https://www.notion.so/{ROW}"),"last_edited_time":REVISION,"properties":{
            "Renamed entry title":{"id":"title","type":"title","title":[{"type":"text","plain_text":"Fixture journal"}]},
            "Due date":{"id":"date-id","type":"date","date":{"start":"2026-12-02","end":null,"time_zone":null}},
            "State":{"id":"status-id","type":"status","status":{"id":"done-id","name":"Done"}},
            "Points":{"id":"number-id","type":"number","number":2.5},
            "Sent":{"id":"check-id","type":"checkbox","checkbox":true},
            "Reference URL":{"id":"url-id","type":"url","url":"https://example.org/receipt"}
        }
    })
}

fn call() -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":TOOL,"arguments":{
        "root_page_id":ROOT,"data_source_id":SOURCE,"idempotency_key":KEY,"title":"Fixture journal","markdown":BODY,
        "properties":{
            "date-id":{"type":"date","value":{"start":"2026-12-02"}},
            "status-id":{"type":"status","value":"done-id"},
            "number-id":{"type":"number","value":2.5},
            "check-id":{"type":"checkbox","value":true},
            "url-id":{"type":"url","value":"https://example.org/receipt"}
        }
    }}})
}

struct Stores {
    directory: tempfile::TempDir,
    ledger: Arc<SqliteIdempotencyStore>,
    queue: Arc<SqliteSyncStateStore>,
    audit: Arc<SqliteAuditStore>,
}

impl Stores {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        Self {
            ledger: Arc::new(
                SqliteIdempotencyStore::initialize_new(path.join("ledger.sqlite")).unwrap(),
            ),
            queue: Arc::new(SqliteSyncStateStore::open(path.join("state.sqlite")).unwrap()),
            audit: Arc::new(
                SqliteAuditStore::open(path.join("audit.sqlite"), AuditRetention::default())
                    .unwrap(),
            ),
            directory,
        }
    }

    async fn invoke(
        &self,
        script: Vec<(u16, Value)>,
        request: Value,
        read_only: bool,
    ) -> (Value, Vec<(String, Vec<u8>)>) {
        let config = config(self.directory.path(), read_only);
        let (source, log, fake) = notion(script).await;
        let handler = root_scope::attach_client(
            KnowledgeServer::default().with_read_only(read_only),
            &config,
            Transport::Http,
            source.clone(),
        )
        .unwrap();
        let handler = crate::write_stores::attach_runtime(
            handler,
            Arc::new(source),
            config.notion_scope.clone().unwrap(),
            self.ledger.clone(),
            self.queue.clone(),
            self.audit.clone(),
        )
        .unwrap();
        let response = http_call(config, handler, request).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), fake)
            .await
            .unwrap()
            .unwrap();
        let log = log.lock().unwrap().clone();
        (response, log)
    }
}

#[tokio::test]
async fn installed_row_create_writes_typed_properties_and_markdown_once_and_replays() {
    let stores = Stores::new();
    let (discovery, _) = stores
        .invoke(
            vec![],
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            false,
        )
        .await;
    assert!(
        discovery["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == TOOL)
    );
    let mut script = schema();
    script.extend([
        (200,row()), (200,row()),
        (200,json!({"object":"page_markdown","id":ROW,"markdown":BODY,"truncated":false,"unknown_block_ids":[]})),
        (200,row()),
    ]);
    script.extend(schema());
    let (first, log) = stores.invoke(script, call(), false).await;
    let output = &first["result"]["structuredContent"];
    assert_eq!(output["write"]["status"], "verified", "{first}");
    assert_eq!(output["write"]["page_id"], ROW);
    assert_eq!(output["write"]["replayed"], false);
    assert_eq!(output["index"]["status"], "queued");
    assert_eq!(output["audit"]["status"], "recorded");
    let post: Vec<_> = log
        .iter()
        .filter(|entry| entry.0.starts_with("POST /v1/pages "))
        .collect();
    assert_eq!(post.len(), 1);
    let sent: Value = serde_json::from_slice(&post[0].1).unwrap();
    assert_eq!(sent["parent"]["data_source_id"], SOURCE);
    assert!(sent["parent"].get("page_id").is_none());
    assert_eq!(
        sent["properties"]["title"]["title"][0]["text"]["content"],
        "Fixture journal"
    );
    assert_eq!(sent["properties"]["status-id"]["status"]["id"], "done-id");
    assert_eq!(sent["properties"]["number-id"]["number"], 2.5);
    assert_eq!(sent["properties"]["check-id"]["checkbox"], true);
    assert_eq!(sent["properties"]["date-id"]["date"]["start"], "2026-12-02");
    assert_eq!(
        sent["properties"]["url-id"]["url"],
        "https://example.org/receipt"
    );
    assert_eq!(sent["markdown"], BODY);

    let mut read = schema();
    read[0].1["properties"]["State"]["status"]["options"] = json!([]);
    read.push((200, row()));
    let (replay, log) = stores.invoke(read, call(), false).await;
    assert_eq!(
        replay["result"]["structuredContent"]["write"]["replayed"], true,
        "{replay}"
    );
    assert_eq!(
        replay["result"]["structuredContent"]["index"]["refresh_id"],
        output["index"]["refresh_id"]
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
}

#[tokio::test]
async fn installed_row_create_denies_scope_and_read_only_without_effect() {
    let stores = Stores::new();
    let (denied, log) = stores.invoke(vec![], call(), true).await;
    assert!(denied["error"].is_object());
    assert!(log.is_empty());
    let mut denied_source = schema();
    denied_source[1].1["parent"] = json!({"type":"workspace"});
    denied_source.truncate(2);
    let (denied, log) = stores.invoke(denied_source, call(), false).await;
    assert_eq!(
        denied["result"]["structuredContent"]["write"]["status"],
        "not_authorized"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let mut bad_relation = call();
    bad_relation["params"]["arguments"]["properties"]["related-id"] =
        json!({"type":"relation","value":[RELATION_TARGET]});
    let mut relation_script = schema();
    relation_script.extend(schema());
    relation_script.push((403, json!({"message":"fixture private"})));
    let (denied, log) = stores.invoke(relation_script, bad_relation, false).await;
    assert_eq!(
        denied["result"]["structuredContent"]["write"]["status"], "not_authorized",
        "{denied}"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let mut bad_input = call();
    bad_input["params"]["arguments"]["properties"]["status-id"]["value"] = json!("missing-option");
    let (invalid, log) = stores.invoke(schema(), bad_input, false).await;
    assert_eq!(
        invalid["result"]["structuredContent"]["write"]["status"],
        "invalid_input"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
}

#[tokio::test]
async fn installed_row_create_refuses_to_replay_a_partial_write() {
    let stores = Stores::new();
    let mut script = schema();
    let mut incomplete = row();
    incomplete["properties"]["State"]["status"] = Value::Null;
    script.extend([
        (200,row()), (200,incomplete.clone()),
        (200,json!({"object":"page_markdown","id":ROW,"markdown":BODY,"truncated":false,"unknown_block_ids":[]})),
        (200,incomplete),
    ]);
    let (first, log) = stores.invoke(script, call(), false).await;
    assert_eq!(
        first["result"]["structuredContent"]["write"]["status"], "outcome_unknown",
        "{first}"
    );
    assert_eq!(
        log.iter()
            .filter(|entry| entry.0.starts_with("POST /v1/pages "))
            .count(),
        1
    );
    let (retry, log) = stores.invoke(schema(), call(), false).await;
    assert_eq!(
        retry["result"]["structuredContent"]["write"]["status"],
        "outcome_unknown"
    );
    assert_eq!(
        retry["result"]["structuredContent"]["index"]["status"],
        "not_attempted"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
}

#[tokio::test]
async fn installed_row_create_rechecks_destination_after_relation_authorization() {
    let stores = Stores::new();
    let mut input = call();
    input["params"]["arguments"]["properties"]["related-id"] =
        json!({"type":"relation","value":[RELATION_TARGET]});
    let mut target = row();
    target["id"] = json!(RELATION_TARGET);
    let relation_authorization = || {
        let mut script = schema();
        script.extend(schema());
        script.push((200, target.clone()));
        script
    };

    let mut overflow = input.clone();
    overflow["params"]["arguments"]["properties"]["related-id"]["value"] =
        json!([RELATION_TARGET, ROW]);
    let (invalid, log) = stores.invoke(schema(), overflow, false).await;
    assert_eq!(
        invalid["result"]["structuredContent"]["write"]["status"],
        "invalid_input"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let mut moved = schema();
    moved[1].1["parent"] = json!({"type":"workspace"});
    moved.truncate(2);
    let mut script = relation_authorization();
    script.extend(moved);
    let (denied, log) = stores.invoke(script, input.clone(), false).await;
    assert_eq!(
        denied["result"]["structuredContent"]["write"]["status"], "not_authorized",
        "{denied}"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let mut script = relation_authorization();
    script.extend(schema());
    let mut linked = row();
    linked["properties"]["Link"] = json!({"id":"related-id","type":"relation","has_more":false,"relation":[{"id":RELATION_TARGET}]});
    script.extend([
        (200,linked.clone()),(200,linked.clone()),
        (200,json!({"object":"page_markdown","id":ROW,"markdown":BODY,"truncated":false,"unknown_block_ids":[]})),
        (200,linked)
    ]);
    script.extend(schema());
    let (created, log) = stores.invoke(script, input, false).await;
    assert_eq!(
        created["result"]["structuredContent"]["write"]["status"], "verified",
        "{created}"
    );
    let wire: Value = serde_json::from_slice(
        &log.iter()
            .find(|entry| entry.0.starts_with("POST /v1/pages "))
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(
        wire["properties"]["related-id"]["relation"],
        json!([{"id":RELATION_TARGET}])
    );
}

#[tokio::test]
async fn oversized_row_create_rejects_without_consuming_the_key() {
    let stores = Stores::new();
    let mut input = call();
    input["params"]["arguments"]["markdown"] = json!(char::from(92).to_string().repeat(200_000));
    let properties = input["params"]["arguments"]["properties"]
        .as_object_mut()
        .unwrap();
    properties.clear();
    let mut extended = schema();
    for index in 0..24 {
        let key = format!("text-{index}");
        extended[0].1["properties"][&key] = json!({"id":key,"type":"rich_text","rich_text":{}});
        properties.insert(key, json!({"type":"rich_text","value":"🟪".repeat(2000)}));
    }
    let (invalid, log) = stores.invoke(extended, input, false).await;
    assert_eq!(
        invalid["result"]["structuredContent"]["write"]["status"], "invalid_input",
        "{invalid}"
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
    let mut changed = call();
    changed["params"]["arguments"]["properties"]["status-id"]["value"] = json!("invalid-option");
    let (invalid, _) = stores.invoke(schema(), changed, false).await;
    assert_eq!(
        invalid["result"]["structuredContent"]["write"]["status"], "invalid_input",
        "{invalid}"
    );
}
