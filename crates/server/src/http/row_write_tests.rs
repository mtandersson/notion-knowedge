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
// Retired tool names; they remain capability selectors only.
const RETIRED_CREATE: &str = "knowledge_create_row";
const RETIRED_UPDATE: &str = "knowledge_update_row";
const VIEW: &str = "33333333-3333-3333-3333-333333333333";
const QUERY: &str = "55555555-5555-5555-5555-555555555555";

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
                "State":{"id":"status-id","type":"status","status":{"options":[{"id":"done-id","name":"Done"},{"id":"pending-id","name":"Pending"}]}},
                "Points":{"id":"number-id","type":"number","number":{}},
                "Sent":{"id":"check-id","type":"checkbox","checkbox":{}},
                "Reference URL":{"id":"url-id","type":"url","url":{}},
                "Link":{"id":"related-id","type":"relation","relation":{"data_source_id":SOURCE,"limit":1}},
                "Note":{"id":"note-id","type":"rich_text","rich_text":{}},
                "Kind":{"id":"select-id","type":"select","select":{"options":[{"id":"report-id","name":"Report"}]}}
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
            "Reference URL":{"id":"url-id","type":"url","url":"https://example.org/receipt"},
            "Note":{"id":"note-id","type":"rich_text","rich_text":[]},
            "Kind":{"id":"select-id","type":"select","select":null}
        }
    })
}

fn call() -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_create_page","arguments":{
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

    fn handler(
        &self,
        source: notion_knowledge_notion::NotionClient,
        config: &Config,
    ) -> KnowledgeServer {
        let handler = root_scope::attach_client(
            KnowledgeServer::default().with_read_only(config.read_only),
            config,
            Transport::Http,
            source.clone(),
        )
        .unwrap();
        crate::write_stores::attach_runtime(
            handler,
            Arc::new(source),
            config.notion_scope.clone().unwrap(),
            self.ledger.clone(),
            self.queue.clone(),
            self.audit.clone(),
        )
        .unwrap()
    }

    async fn invoke(
        &self,
        script: Vec<(u16, Value)>,
        request: Value,
        read_only: bool,
    ) -> (Value, Vec<(String, Vec<u8>)>) {
        self.invoke_with(script, request, read_only, None).await
    }

    async fn invoke_with(
        &self,
        script: Vec<(u16, Value)>,
        request: Value,
        read_only: bool,
        allowlist: Option<&str>,
    ) -> (Value, Vec<(String, Vec<u8>)>) {
        let mut config = config(self.directory.path(), read_only);
        if let Some(allowlist) = allowlist {
            // The HTTP transport installs the configured policy on every session.
            config.capability_policy =
                notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(
                    Some(allowlist),
                    None,
                )
                .unwrap();
        }
        let (source, log, fake) = notion(script).await;
        let handler = self.handler(source, &config);
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
    let listed = discovery["result"]["tools"].as_array().unwrap();
    let create = listed
        .iter()
        .find(|tool| tool["name"] == "knowledge_create_page")
        .unwrap();
    assert!(create["inputSchema"]["properties"]["data_source_id"].is_object());
    assert!(listed.iter().all(|tool| tool["name"] != RETIRED_CREATE));
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

fn view_steps() -> Vec<(u16, Value)> {
    let mut steps = vec![(
        200,
        json!({
            "object":"view","id":VIEW,"parent":{"type":"database_id","database_id":DATABASE},
            "type":"table","data_source_id":SOURCE,"last_edited_time":REVISION
        }),
    )];
    steps.extend(schema());
    steps
}

async fn view_row(stores: &Stores, page: Value) -> Value {
    let mut script = view_steps();
    script.extend([
        (
            200,
            json!({"object":"view_query","view_id":VIEW,"id":QUERY,
            "results":[{"object":"page","id":ROW}],"has_more":false,"next_cursor":null}),
        ),
        (200, page),
    ]);
    script.extend(view_steps());
    let request = json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
        "name":"knowledge_get","arguments":{"view_id":VIEW,"include_row_revision":true}
    }});
    let (snapshot, _) = stores.invoke(script, request, false).await;
    snapshot["result"]["structuredContent"]["rows"][0].clone()
}

async fn revision(stores: &Stores) -> String {
    let token = view_row(stores, row()).await["row_revision"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(token.len(), 64);
    token
}

fn update(revision: &str, key: &str) -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_update_page","arguments":{
        "operation":"patch_properties","root_page_id":ROOT,"page_id":ROW,"idempotency_key":key,"expected_row_revision":revision,
        "properties":{
            "status-id":{"type":"status","value":"pending-id"},
            "date-id":{"type":"date","value":null},
            "url-id":{"type":"url","value":"https://example.org/updated"},
            "note-id":{"type":"rich_text","value":"Followed up"},
            "select-id":{"type":"select","value":"report-id"}
        }
    }}})
}

fn updated_row() -> Value {
    let mut page = row();
    // Intentionally unchanged timestamp. The server must detect value changes too.
    page["properties"]["State"]["status"] = json!({"id":"pending-id","name":"Pending"});
    page["properties"]["Due date"]["date"] = Value::Null;
    page["properties"]["Reference URL"]["url"] = json!("https://example.org/updated");
    page["properties"]["Note"]["rich_text"] = json!([{"type":"text","plain_text":"Followed up"}]);
    page["properties"]["Kind"]["select"] = json!({"id":"report-id","name":"Report"});
    page
}

fn patch_steps(readback: Value) -> Vec<(u16, Value)> {
    let mut script = vec![(200, row())];
    script.extend(schema());
    script.extend([
        (200, row()),
        (200, json!({"object":"page","id":ROW})),
        (200, readback),
    ]);
    script
}

fn status(output: &Value) -> &str {
    output["result"]["structuredContent"]["write"]["status"]
        .as_str()
        .unwrap()
}

#[tokio::test]
async fn installed_row_update_patches_only_requested_fields_and_replays() {
    let stores = Stores::new();
    let revision = revision(&stores).await;
    let (discovery, upstream) = stores
        .invoke(
            vec![],
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            false,
        )
        .await;
    assert!(upstream.is_empty());
    let tool = discovery["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "knowledge_update_page")
        .unwrap();
    assert_eq!(tool["annotations"]["destructiveHint"], false);
    assert_eq!(
        tool["inputSchema"]["properties"]["operation"]["enum"],
        json!(["append_markdown", "patch_properties"])
    );
    assert!(
        discovery["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool["name"] != RETIRED_UPDATE && tool["name"] != "knowledge_append")
    );
    let mut script = patch_steps(updated_row());
    script.extend(schema());
    let (first, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&first), "verified", "{first}");
    assert_eq!(
        first["result"]["structuredContent"]["index"]["status"],
        "queued"
    );
    assert_eq!(
        first["result"]["structuredContent"]["audit"]["status"],
        "recorded"
    );
    let patches: Vec<_> = log
        .iter()
        .filter(|entry| entry.0.starts_with("PATCH "))
        .collect();
    assert_eq!(patches.len(), 1);
    assert!(patches[0].0.starts_with(&format!("PATCH /v1/pages/{ROW} ")));
    let body: Value = serde_json::from_slice(&patches[0].1).unwrap();
    assert_eq!(
        body,
        json!({"properties":{
            "status-id":{"status":{"id":"pending-id"}},
            "date-id":{"date":null},
            "url-id":{"url":"https://example.org/updated"},
            "note-id":{"rich_text":[{"type":"text","text":{"content":"Followed up"}}]},
            "select-id":{"select":{"id":"report-id"}}
        }})
    );

    let mut script = vec![(200, updated_row())];
    let mut changed_schema = schema();
    changed_schema[0].1["properties"]["State"]["status"]["options"] = json!([]);
    script.extend(changed_schema);
    let (replay, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&replay), "verified", "{replay}");
    assert_eq!(
        replay["result"]["structuredContent"]["write"]["replayed"],
        true
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
}

#[tokio::test]
async fn row_update_denies_scope_and_detects_edits_between_two_reads() {
    let stores = Stores::new();
    let revision = revision(&stores).await;
    let mut outside = schema();
    outside[1].1["parent"] = json!({"type":"workspace"});
    outside.truncate(2);
    let mut script = vec![(200, row())];
    script.extend(outside);
    let (denied, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&denied), "not_authorized");
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let mut script = vec![(200, row())];
    script.extend(schema());
    let mut changed = row();
    changed["properties"]["Note"]["rich_text"] =
        json!([{"type":"text","plain_text":"Concurrent edit"}]);
    script.push((200, changed));
    let (conflict, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&conflict), "revision_conflict");
    assert_eq!(
        conflict["result"]["structuredContent"]["write"]["current_last_edited_time"],
        REVISION
    );
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));

    let script = patch_steps(row());
    let (unknown, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&unknown), "outcome_unknown");
    assert_eq!(
        log.iter()
            .filter(|entry| entry.0.starts_with("PATCH "))
            .count(),
        1
    );
    let mut script = vec![(200, row())];
    script.extend(schema());
    let (retry, log) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&retry), "outcome_unknown");
    assert!(log.iter().all(|entry| entry.0.starts_with("GET ")));
}

#[tokio::test]
async fn concurrent_row_updates_with_distinct_keys_only_patch_once() {
    let stores = Stores::new();
    let revision = revision(&stores).await;
    let mut script = patch_steps(updated_row());
    script.extend(schema());
    script.push((200, updated_row()));
    script.extend(schema());
    let (source, log, fake) = notion(script).await;
    let first = config(stores.directory.path(), false);
    let second = config(stores.directory.path(), false);
    let handler = stores.handler(source, &first);
    let (one, two) = tokio::join!(
        http_call(first, handler.clone(), update(&revision, KEY)),
        http_call(
            second,
            handler,
            update(&revision, "another-journal-entry-102")
        ),
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), fake)
        .await
        .unwrap()
        .unwrap();
    let mut results = [status(&one), status(&two)];
    results.sort();
    assert_eq!(results, ["revision_conflict", "verified"]);
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.0.starts_with("PATCH "))
            .count(),
        1
    );
}

#[tokio::test]
async fn view_and_update_withhold_revision_when_notion_truncates_rich_text() {
    let stores = Stores::new();
    let known_revision = revision(&stores).await;
    let mut truncated = row();
    truncated["properties"]["Note"]["rich_text"] = json!(
        (0..25)
            .map(|_| json!({
                "type":"mention","mention":{"type":"page","page":{"id":RELATION_TARGET}},
                "plain_text":"Linked row"
            }))
            .collect::<Vec<_>>()
    );
    let row = view_row(&stores, truncated.clone()).await;
    assert_eq!(row["page_id"], ROW);
    assert!(row["row_revision"].is_null());
    let mut script = vec![(200, truncated)];
    script.extend(schema());
    let (denied, upstream) = stores
        .invoke(script, update(&known_revision, KEY), false)
        .await;
    assert_eq!(status(&denied), "unavailable");
    assert!(upstream.iter().all(|entry| entry.0.starts_with("GET ")));
}

/// Measured in the installed transport with every row, page and view tool on.
#[tokio::test]
async fn installed_write_catalog_stays_within_its_size_budget() {
    let stores = Stores::new();
    let (discovery, _) = stores
        .invoke(
            vec![],
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            false,
        )
        .await;
    let tools = discovery["result"]["tools"].as_array().unwrap();
    let total: usize = tools.iter().map(|tool| tool.to_string().len()).sum();
    let sizes: Vec<String> = tools
        .iter()
        .map(|tool| {
            format!(
                "{}={}",
                tool["name"].as_str().unwrap(),
                tool.to_string().len()
            )
        })
        .collect();
    eprintln!("MEASURE catalog total={total} {}", sizes.join(" "));
    assert!(total <= 25_000, "catalog is {total} B");
}

fn merged(mut request: Value, name: &str, operation: Option<&str>) -> Value {
    request["params"]["name"] = json!(name);
    if let Some(operation) = operation {
        request["params"]["arguments"]["operation"] = json!(operation);
    }
    request
}

#[tokio::test]
async fn merged_update_page_patches_rows_and_rejects_unknown_operations() {
    let stores = Stores::new();
    let revision = revision(&stores).await;
    let mut script = patch_steps(updated_row());
    script.extend(schema());
    let (patched, log) = stores
        .invoke(
            script,
            merged(
                update(&revision, KEY),
                "knowledge_update_page",
                Some("patch_properties"),
            ),
            false,
        )
        .await;
    assert_eq!(status(&patched), "verified", "{patched}");
    assert_eq!(
        log.iter()
            .filter(|entry| entry.0.starts_with("PATCH "))
            .count(),
        1
    );

    let (denied, _) = stores
        .invoke(
            vec![],
            merged(
                update(&revision, KEY),
                "knowledge_update_page",
                Some("unknown_operation"),
            ),
            false,
        )
        .await;
    assert!(
        denied["error"].is_object() || denied["result"]["isError"] == true,
        "{denied}"
    );
}

#[tokio::test]
async fn merged_create_page_creates_a_row_and_replays_by_key() {
    let stores = Stores::new();
    let mut script = schema();
    script.extend([
        (200, row()),
        (200, row()),
        (200, json!({"object":"page_markdown","id":ROW,"markdown":BODY,"truncated":false,"unknown_block_ids":[]})),
        (200, row()),
    ]);
    script.extend(schema());
    let (first, _) = stores
        .invoke(script, merged(call(), "knowledge_create_page", None), false)
        .await;
    assert_eq!(status(&first), "verified", "{first}");

    // The same key replays without a second write.
    let mut read = schema();
    read[0].1["properties"]["State"]["status"]["options"] = json!([]);
    read.push((200, row()));
    let (replay, log) = stores.invoke(read, call(), false).await;
    assert_eq!(
        replay["result"]["structuredContent"]["write"]["replayed"], true,
        "{replay}"
    );
    assert!(log.iter().all(|entry| !entry.0.starts_with("POST ")));
}

#[tokio::test]
async fn page_selectors_alone_never_grant_row_writes() {
    let stores = Stores::new();
    let allowlist = Some("read,knowledge_create_page,knowledge_update_page,knowledge_append");
    let (discovery, _) = stores
        .invoke_with(
            vec![],
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            false,
            allowlist,
        )
        .await;
    let tools = discovery["result"]["tools"].as_array().unwrap();
    let create = tools
        .iter()
        .find(|tool| tool["name"] == "knowledge_create_page")
        .unwrap();
    assert!(create["inputSchema"]["properties"]["data_source_id"].is_null());
    let listed_update = tools
        .iter()
        .find(|tool| tool["name"] == "knowledge_update_page")
        .unwrap();
    assert_eq!(
        listed_update["inputSchema"]["properties"]["operation"]["enum"],
        json!(["append_markdown"])
    );

    let (created, log) = stores.invoke_with(vec![], call(), false, allowlist).await;
    assert!(created["error"].is_object(), "{created}");
    assert!(log.is_empty());
    let patch = update(&"0".repeat(64), "row-patch-without-selector");
    let (patched, log) = stores.invoke_with(vec![], patch, false, allowlist).await;
    assert!(patched["error"].is_object(), "{patched}");
    assert!(log.is_empty());
}

#[tokio::test]
async fn retired_row_tool_names_are_not_callable() {
    let stores = Stores::new();
    for name in [RETIRED_CREATE, RETIRED_UPDATE] {
        let mut request = call();
        request["params"]["name"] = json!(name);
        let (response, log) = stores.invoke(vec![], request, false).await;
        assert_eq!(
            response["error"]["message"], "unknown tool",
            "{name}: {response}"
        );
        assert!(log.is_empty());
    }
}

fn refusal_body(code: &str) -> Value {
    json!({"object":"error","status":0,"code":code,"message":"Fixture refusal text"})
}

fn created_steps() -> Vec<(u16, Value)> {
    let mut script = schema();
    script.extend([
        (200,row()), (200,row()),
        (200,json!({"object":"page_markdown","id":ROW,"markdown":BODY,"truncated":false,"unknown_block_ids":[]})),
        (200,row()),
    ]);
    script.extend(schema());
    script
}

#[tokio::test]
async fn refused_row_create_reports_why_and_releases_the_key() {
    for (code, status_code, status_text, reason) in [
        (400, "validation_error", "invalid_input", "invalid_request"),
        (
            403,
            "restricted_resource",
            "not_authorized",
            "permission_denied",
        ),
        (404, "object_not_found", "not_authorized", "not_found"),
    ] {
        let stores = Stores::new();
        let mut script = schema();
        script.push((code, refusal_body(status_code)));
        let (refused, log) = stores.invoke(script, call(), false).await;
        let write = &refused["result"]["structuredContent"]["write"];
        assert_eq!(write["status"], status_text, "{refused}");
        assert_eq!(write["reason"], reason, "{refused}");
        let message = write["message"].as_str().unwrap();
        assert!(message.contains("Nothing was changed"), "{message}");
        assert!(message.ends_with("The same idempotency key can be retried."));
        assert!(!refused.to_string().contains("Fixture refusal text"));
        assert_eq!(
            log.iter()
                .filter(|e| e.0.starts_with("POST /v1/pages "))
                .count(),
            1
        );

        let (retried, _) = stores.invoke(created_steps(), call(), false).await;
        assert_eq!(status(&retried), "verified", "{retried}");
    }
}

#[tokio::test]
async fn row_create_server_errors_stay_outcome_unknown_and_keep_the_key() {
    let stores = Stores::new();
    let mut script = schema();
    script.push((500, refusal_body("internal_server_error")));
    let (unknown, _) = stores.invoke(script, call(), false).await;
    assert_eq!(status(&unknown), "outcome_unknown", "{unknown}");
    let (retry, log) = stores.invoke(schema(), call(), false).await;
    assert_eq!(status(&retry), "outcome_unknown", "{retry}");
    assert!(log.iter().all(|entry| !entry.0.starts_with("POST ")));
}

#[tokio::test]
async fn refused_row_update_reports_why_and_releases_the_key() {
    let stores = Stores::new();
    let revision = revision(&stores).await;
    let mut script = vec![(200, row())];
    script.extend(schema());
    script.extend([(200, row()), (400, refusal_body("validation_error"))]);
    let (refused, _) = stores.invoke(script, update(&revision, KEY), false).await;
    let write = &refused["result"]["structuredContent"]["write"];
    assert_eq!(write["status"], "invalid_input", "{refused}");
    assert_eq!(write["reason"], "invalid_request");
    assert!(write["message"].as_str().unwrap().contains("property ids"));

    let mut script = patch_steps(updated_row());
    script.extend(schema());
    let (retried, _) = stores.invoke(script, update(&revision, KEY), false).await;
    assert_eq!(status(&retried), "verified", "{retried}");
}
