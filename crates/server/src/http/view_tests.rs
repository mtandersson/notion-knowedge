use notion_knowledge_mcp::{KnowledgeServer, view::SELECTOR};
use serde_json::{Value, json};

use crate::{
    config::Config,
    root_scope::{self, Transport},
};

use super::ingestion_tests::{http_call, notion, stdio_call};

const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
const VIEW: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const SOURCE: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const DB: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const LINK: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const QUERY: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const FIRST: &str = "88888888-8888-8888-8888-888888888888";
const SECOND: &str = "55555555-5555-5555-5555-555555555555";
const REVISION: &str = "2026-11-24T15:00:00Z";
const CURSOR: &str = "cursor+with&opaque=value";

fn config(path: &std::path::Path, http: bool) -> Config {
    let values = [
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
            .find(|(name, _)| *name == key && (http || !name.starts_with("NK_HTTP_")))
            .map(|(_, value)| value.into())
    })
    .unwrap();
    config.webhook_state_file = Some(path.join("sync.sqlite"));
    config
}

fn scoped_view(linked: bool) -> Vec<(u16, Value)> {
    let parent = if linked { LINK } else { DB };
    let root = json!({"object":"page","id":ROOT,"in_trash":false,"parent":{"type":"workspace","workspace":true}});
    let database = json!({"object":"database","id":DB,"in_trash":false,"parent":{"type":"page_id","page_id":ROOT}});
    let mut route = vec![(
        200,
        json!({
            "object":"view","id":VIEW,"parent":{"type":"database_id","database_id":parent},
            "type":"table","data_source_id":SOURCE,"last_edited_time":REVISION,
            "filter":{"and":[{"property":"Status","status":{"equals":"Done"}},
                {"property":"Due","date":{"on_or_before":"2026-12-01"}}]},
            "sorts":[{"property":"Due","direction":"descending"}],
        "quick_filters":{"Category":{"select":{"equals":"Delivery"}}}
        }),
    )];
    route.extend([
        (200,json!({"object":"data_source","id":SOURCE,"parent":{"type":"database_id","database_id":DB},"properties":{
            "Name":{"id":"title","type":"title","title":{}},
            "Status":{"id":"status","type":"status","status":{}},
            "Due":{"id":"date","type":"date","date":{}},
            "Attachment":{"id":"file","type":"files","files":{}},
            "Private relation":{"id":"rel","type":"relation","relation":{}},
            "Related summary":{"id":"roll","type":"rollup","rollup":{}}}})),
        (200,database),
        (200,root.clone()),
    ]);
    if linked {
        route.extend([
            (404, json!({})),
            (
                200,
                json!({"object":"block","id":LINK,"type":"child_database","in_trash":false,
                "parent":{"type":"page_id","page_id":ROOT}}),
            ),
            (200, root),
        ]);
    }
    route
}

fn row(id: &str, due: &str) -> (u16, Value) {
    (
        200,
        json!({"object":"page","id":id,"in_trash":false,"last_edited_time":REVISION,
            "parent":{"type":"data_source_id","data_source_id":SOURCE},"properties":{
            "Name":{"id":"title","type":"title","title":[{"type":"text","plain_text":id}]},
            "Status":{"id":"status","type":"status","status":{"name":"Done","id":"option"}},
            "Due":{"id":"date","type":"date","date":{"start":due,"end":null,"time_zone":null}},
        "Attachment":{"id":"file","type":"files","files":[{"type":"file","file":{"url":"https://files.example/?token=secret"}}]},
        "Private relation":{"id":"rel","type":"relation","relation":[{"id":"33333333-3333-3333-3333-333333333333"}]},
        "Related summary":{"id":"roll","type":"rollup","rollup":{"type":"number","number":910019}}
        }}),
    )
}

async fn call(
    steps: Vec<(u16, Value)>,
    request: Value,
    http: bool,
) -> (Value, Vec<(String, Vec<u8>)>) {
    call_with(KnowledgeServer::default(), steps, request, http).await
}

async fn call_with(
    server: KnowledgeServer,
    steps: Vec<(u16, Value)>,
    request: Value,
    http: bool,
) -> (Value, Vec<(String, Vec<u8>)>) {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), http);
    let (source, log, fake) = notion(steps).await;
    let handler = root_scope::attach_client(
        server,
        &config,
        if http {
            Transport::Http
        } else {
            Transport::Stdio
        },
        source,
    )
    .unwrap();
    let result = if http {
        http_call(config, handler, request).await
    } else {
        stdio_call(handler, request).await
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), fake)
        .await
        .unwrap()
        .unwrap();
    let log = log.lock().unwrap().clone();
    (result, log)
}

fn query(arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_get","arguments":arguments}})
}

#[tokio::test]
async fn installed_view_discovery_on_read_only_transports() {
    for http in [false, true] {
        let (list, upstream) = call(
            vec![],
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            http,
        )
        .await;
        let tool = list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "knowledge_get")
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(
            tool["inputSchema"]["properties"]["page_size"]["maximum"],
            25
        );
        assert!(tool["inputSchema"]["properties"]["fields"].is_object());
        // 4 377 B when the view mode was added (it replaced knowledge_get at
        // 3 064 plus the view tool at 1 470); the data_source_id listing mode
        // brought it to about 4 750.
        assert!(tool.to_string().len() <= 4_800);
        assert!(
            list["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .all(|tool| tool["name"] != SELECTOR)
        );
        assert!(upstream.is_empty());
    }
}

#[tokio::test]
async fn linked_view_uses_notion_order_and_paginates_with_typed_rows() {
    let mut steps = scoped_view(true);
    steps.extend([
        (
            200,
            json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[
            {"object":"page","id":FIRST},{"object":"page","id":SECOND}
        ],"has_more":true,"next_cursor":CURSOR,"request_status":{"type":"complete"}}),
        ),
        row(FIRST, "2026-11-29"),
        row(SECOND, "2026-11-27"),
    ]);
    steps.extend(scoped_view(true));
    let (first, log) = call(
        steps,
        query(json!({"view_id":format!("view://{VIEW}"),"page_size":2})),
        true,
    )
    .await;
    let output = &first["result"]["structuredContent"];
    assert_eq!(output["status"], "more", "{first}");
    assert_eq!(output["filter"]["and"][0]["status"]["equals"], "Done");
    assert_eq!(
        output["filter"]["and"][1]["date"]["on_or_before"],
        "2026-12-01"
    );
    assert_eq!(output["sorts"][0]["direction"], "descending");
    assert_eq!(
        output["quick_filters"]["Category"]["select"]["equals"],
        "Delivery"
    );
    assert_eq!(output["rows"][0]["page_id"], FIRST);
    assert_eq!(output["rows"][1]["page_id"], SECOND);
    assert_eq!(
        output["omitted_columns"],
        json!([{"id":"file","name":"Attachment","type":"files"},
            {"id":"rel","name":"Private relation","type":"relation"},
            {"id":"roll","name":"Related summary","type":"rollup"}])
    );
    assert_eq!(
        output["columns"],
        json!([{"id":"title","name":"Name","type":"title"},
            {"id":"date","name":"Due","type":"date"},{"id":"status","name":"Status","type":"status"}])
    );
    assert!(output["rows"][0].get("row_revision").is_none());
    assert!(output["rows"][0].get("omitted").is_none());
    let wire = first.to_string();
    assert!(!wire.contains("token=secret"));
    assert!(!wire.contains("33333333-3333-3333-3333-333333333333"));
    assert!(!wire.contains("910019"));
    assert_eq!(
        output["rows"][0]["values"],
        json!([FIRST, {"start":"2026-11-29"}, {"id":"option","name":"Done"}])
    );
    assert!(log[4].0.starts_with(&format!("GET /v1/databases/{LINK} ")));
    assert!(log[5].0.starts_with(&format!("GET /v1/blocks/{LINK} ")));
    assert!(log[2].0.starts_with(&format!("GET /v1/databases/{DB} ")));
    assert!(
        log[7]
            .0
            .starts_with(&format!("POST /v1/views/{VIEW}/queries "))
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&log[7].1).unwrap(),
        json!({"page_size":2})
    );

    let mut continuation = output["continuation"].clone();
    continuation["cursor"] = json!("arbitrary-cursor");
    let (forged, log) = call(
        vec![],
        query(json!({"view_id":VIEW,"continuation":continuation})),
        true,
    )
    .await;
    assert!(
        forged["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("invalid_input")
    );
    assert!(log.is_empty());

    let mut changed = scoped_view(true);
    changed[0].1["quick_filters"] = json!({"Category":{"select":{"equals":"Task"}}});
    let (stale, _) = call(
        changed,
        query(json!({"view_id":VIEW,"continuation":output["continuation"]})),
        false,
    )
    .await;
    assert!(
        stale["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("view_query_expired_or_changed")
    );

    let mut steps = scoped_view(true);
    steps.extend([
        (200,json!({"object":"list","type":"page","results":[{"object":"page","id":ROOT}],
            "has_more":false,"next_cursor":null,"request_status":{"type":"incomplete","incomplete_reason":"query_result_limit_reached"}})),
        row(ROOT,"2026-11-26"),
    ]);
    steps.extend(scoped_view(true));
    let (last, log) = call(
        steps,
        query(json!({"view_id":VIEW,"continuation":output["continuation"]})),
        false,
    )
    .await;
    assert_eq!(
        last["result"]["structuredContent"]["status"], "incomplete",
        "{last}"
    );
    assert_eq!(
        last["result"]["structuredContent"]["rows"][0]["page_id"],
        ROOT
    );
    assert!(last["result"]["structuredContent"]["continuation"].is_null());
    assert!(log[7].0.starts_with(&format!("GET /v1/views/{VIEW}/queries/{QUERY}?page_size=10&start_cursor=cursor%2Bwith%26opaque%3Dvalue ")));
}

#[tokio::test]
async fn view_results_distinguish_empty_denied_unsupported_and_unavailable() {
    let mut empty = scoped_view(false);
    empty.push((200,json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[],"has_more":false,"next_cursor":null})));
    empty.extend(scoped_view(false));
    let (result, _) = call(empty, query(json!({"view_id":VIEW})), false).await;
    assert_eq!(
        result["result"]["structuredContent"]["status"], "empty",
        "{result}"
    );

    let mut denied = scoped_view(true);
    denied[2].1["parent"] = json!({"type":"workspace","workspace":true});
    denied.truncate(3);
    let mut outside_host = scoped_view(true);
    outside_host[5].1["parent"] = json!({"type":"workspace","workspace":true});
    outside_host.truncate(6);
    let mut unsupported = scoped_view(false);
    unsupported[0].1["type"] = json!("dashboard");
    unsupported.truncate(1);
    let mut unavailable = scoped_view(false);
    unavailable.push((503, json!({"message":"Never expose upstream detail"})));
    for (steps, code) in [
        (denied, "source_not_accessible"),
        (outside_host, "source_not_accessible"),
        (unsupported, "unsupported_view"),
        (unavailable, "notion_unavailable"),
    ] {
        // A bad field never reveals column names for a view that is refused first.
        // (The unavailable case fails after the columns resolve, so it only
        // takes the plain request.)
        let mut variants = vec![json!({"view_id":VIEW})];
        if code != "notion_unavailable" {
            variants.push(json!({"view_id":VIEW,"fields":["Missing"]}));
        }
        for arguments in variants {
            let (result, _) = call(steps.clone(), query(arguments), false).await;
            assert_eq!(result["result"]["isError"], true, "{result}");
            let text = result["result"]["content"][0]["text"].as_str().unwrap();
            assert!(text.starts_with(code), "{text}");
            assert!(!text.contains("unknown_field"), "{text}");
        }
    }
}

#[tokio::test]
async fn selected_columns_and_explicit_row_revision_shrink_the_response() {
    let mut steps = scoped_view(false);
    steps.extend([
        (
            200,
            json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[{"object":"page","id":FIRST}],
            "has_more":false,"next_cursor":null}),
        ),
        row(FIRST, "2026-11-29"),
    ]);
    steps.extend(scoped_view(false));
    let (result, _) = call(
        steps,
        query(json!({"view_id":VIEW,"fields":["status","Attachment"],"include_row_revision":true})),
        false,
    )
    .await;
    let output = &result["result"]["structuredContent"];
    assert_eq!(
        output["columns"],
        json!([{"id":"title","name":"Name","type":"title"},{"id":"status","name":"Status","type":"status"}]),
        "{result}"
    );
    assert_eq!(
        output["omitted_columns"],
        json!([{"id":"file","name":"Attachment","type":"files"}])
    );
    assert_eq!(
        output["rows"][0]["values"],
        json!([FIRST, {"id":"option","name":"Done"}])
    );
    assert_eq!(
        output["rows"][0]["row_revision"].as_str().unwrap().len(),
        64
    );
    assert!(!result.to_string().contains("token=secret"));

    let mut unknown = scoped_view(false);
    unknown.truncate(4);
    let (result, _) = call(
        unknown,
        query(json!({"view_id":VIEW,"fields":["Missing"]})),
        false,
    )
    .await;
    // The refusal names the problem and lists the view's real columns, so the
    // caller can correct the entry without guessing at ids or continuations.
    let text = result["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.starts_with("invalid_input: unknown_field"), "{result}");
    for column in ["Name", "Status", "Attachment"] {
        assert!(text.contains(column), "{text}");
    }
    assert!(
        !text.contains("UUID") && !text.contains("continuation"),
        "{text}"
    );
    assert_eq!(result["result"]["isError"], true, "{result}");
}

fn nth(index: usize) -> String {
    format!("00000000-0000-4000-8000-{index:012}")
}

async fn ten_rows(arguments: Value) -> Value {
    let ids: Vec<String> = (1..=10).map(nth).collect();
    let mut steps = scoped_view(false);
    steps.push((
        200,
        json!({"object":"view_query","view_id":VIEW,"id":QUERY,
        "results":ids.iter().map(|id| json!({"object":"page","id":id})).collect::<Vec<_>>(),
        "has_more":false,"next_cursor":null}),
    ));
    steps.extend(ids.iter().map(|id| row(id, "2026-11-29")));
    steps.extend(scoped_view(false));
    let (result, _) = call(steps, query(arguments), false).await;
    result
}

/// Catalog and response sizes are part of the agent-facing contract: the
/// synthetic ten-row response was 7 800 B before #810 (see handover.md).
#[tokio::test]
async fn ten_row_view_response_stays_compact() {
    let selected = ten_rows(json!({"view_id":VIEW,"page_size":10,"fields":["Status","Due"]})).await;
    let revisions = ten_rows(
        json!({"view_id":VIEW,"page_size":10,"fields":["Status","Due"],"include_row_revision":true}),
    )
    .await;
    let selected_bytes = selected["result"]["structuredContent"].to_string().len();
    let revision_bytes = revisions["result"]["structuredContent"].to_string().len();
    eprintln!("MEASURE view selected={selected_bytes} with_revisions={revision_bytes}");
    assert_eq!(
        selected["result"]["structuredContent"]["rows"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
    assert!(selected_bytes <= 2_700, "{selected_bytes} B");
    assert!(revision_bytes <= 3_500, "{revision_bytes} B");
}

#[tokio::test]
async fn knowledge_get_view_mode_returns_rows_and_rejects_mixed_arguments() {
    let steps = || {
        let mut steps = scoped_view(false);
        steps.extend([
            (
                200,
                json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[{"object":"page","id":FIRST}],
                "has_more":false,"next_cursor":null}),
            ),
            row(FIRST, "2026-11-29"),
        ]);
        steps.extend(scoped_view(false));
        steps
    };
    let as_get = |arguments: Value| {
        let mut request = query(arguments);
        request["params"]["name"] = json!("knowledge_get");
        request
    };
    let (new, _) = call(
        steps(),
        as_get(json!({"view_id":VIEW,"fields":["Status"]})),
        false,
    )
    .await;
    assert_eq!(
        new["result"]["structuredContent"]["rows"][0]["values"][1]["name"],
        "Done"
    );

    let (mixed, upstream) = call(
        vec![],
        as_get(json!({"view_id":VIEW,"refs":[{"kind":"page","id":FIRST}],"max_chars":100})),
        false,
    )
    .await;
    assert!(
        mixed["error"].is_object() || mixed["result"]["isError"] == true,
        "{mixed}"
    );
    assert!(upstream.is_empty());
}

fn get_view(arguments: Value) -> Value {
    let mut request = query(arguments);
    request["params"]["name"] = json!("knowledge_get");
    request
}

fn is_rejected(response: &Value) -> bool {
    response["error"].is_object() || response["result"]["isError"] == true
}

#[tokio::test]
async fn view_mode_follows_the_view_selector_and_validates_its_arguments() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let blocked = || {
        KnowledgeServer::default()
            .with_capability_policy(CapabilityPolicy::from_lists(None, Some(SELECTOR)).unwrap())
    };
    // An allowlist naming knowledge_get but not the view selector also hides view mode.
    let get_only = KnowledgeServer::default()
        .with_capability_policy(CapabilityPolicy::from_lists(Some("knowledge_get"), None).unwrap());
    let (denied, upstream) =
        call_with(get_only, vec![], get_view(json!({"view_id":VIEW})), false).await;
    assert!(is_rejected(&denied), "{denied}");
    assert!(upstream.is_empty());
    let (list, _) = call_with(
        blocked(),
        vec![],
        json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
        false,
    )
    .await;
    let get = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "knowledge_get")
        .unwrap();
    assert!(get["inputSchema"]["properties"]["view_id"].is_null());
    let (denied, upstream) =
        call_with(blocked(), vec![], get_view(json!({"view_id":VIEW})), false).await;
    assert!(is_rejected(&denied), "{denied}");
    assert!(upstream.is_empty());

    for arguments in [
        json!({"view_id":null}),
        json!({"view_id":""}),
        json!({"view_id":VIEW,"freshness":"live"}),
    ] {
        let (response, upstream) = call(vec![], get_view(arguments), false).await;
        assert!(is_rejected(&response), "{response}");
        assert!(upstream.is_empty());
    }
}

#[tokio::test]
async fn view_mode_works_at_the_smallest_tool_capacity_and_declares_its_output() {
    let mut steps = scoped_view(false);
    steps.push((
        200,
        json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[],"has_more":false,"next_cursor":null}),
    ));
    steps.extend(scoped_view(false));
    let (response, _) = call_with(
        KnowledgeServer::default().with_tool_capacity(8),
        steps,
        get_view(json!({"view_id":VIEW})),
        false,
    )
    .await;
    assert_eq!(
        response["result"]["structuredContent"]["status"], "empty",
        "{response}"
    );

    let (list, _) = call(
        vec![],
        json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
        false,
    )
    .await;
    let get = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "knowledge_get")
        .unwrap();
    assert!(get["outputSchema"]["required"].is_null());
    assert_eq!(get["outputSchema"]["properties"]["rows"]["type"], "array");
}

#[tokio::test]
async fn retired_view_tool_name_is_not_callable() {
    let mut request = query(json!({"view_id":VIEW}));
    request["params"]["name"] = json!(SELECTOR);
    let (response, upstream) = call(vec![], request, false).await;
    assert_eq!(response["error"]["message"], "unknown tool", "{response}");
    assert!(upstream.is_empty());
}

#[tokio::test]
async fn a_view_only_allowlist_reads_saved_views_but_not_sources() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let view_only = || {
        KnowledgeServer::default()
            .with_capability_policy(CapabilityPolicy::from_lists(Some(SELECTOR), None).unwrap())
    };
    let (list, _) = call_with(
        view_only(),
        vec![],
        json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
        false,
    )
    .await;
    let get = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "knowledge_get")
        .unwrap();
    assert!(get["inputSchema"]["properties"]["view_id"].is_object());

    let mut steps = scoped_view(false);
    steps.extend([
        (
            200,
            json!({"object":"view_query","view_id":VIEW,"id":QUERY,"results":[{"object":"page","id":FIRST}],
            "has_more":false,"next_cursor":null}),
        ),
        row(FIRST, "2026-11-29"),
    ]);
    steps.extend(scoped_view(false));
    let (rows, _) = call_with(view_only(), steps, get_view(json!({"view_id":VIEW})), false).await;
    assert!(!is_rejected(&rows), "{rows}");
    assert_eq!(
        rows["result"]["structuredContent"]["rows"][0]["page_id"], FIRST,
        "{rows}"
    );

    let (sources, upstream) = call_with(
        view_only(),
        vec![],
        get_view(json!({"refs":[{"kind":"page","id":FIRST}],"max_chars":100})),
        false,
    )
    .await;
    assert!(is_rejected(&sources), "{sources}");
    assert!(upstream.is_empty());
}

const OTHER_VIEW: &str = "99999999-9999-4999-8999-999999999999";
const OTHER_SOURCE: &str = "77777777-7777-4777-8777-777777777777";

fn view_object(id: &str, name: &str, kind: &str, source: &str) -> (u16, Value) {
    (
        200,
        json!({"object":"view","id":id,"name":name,"type":kind,
            "parent":{"type":"database_id","database_id":DB},"data_source_id":source}),
    )
}

#[tokio::test]
async fn data_source_lists_only_its_own_queryable_views() {
    // The three scope lookups for the data source, then the listing and one
    // fetch per listed view (the list endpoint returns bare ids).
    let mut steps = scoped_view(false).split_off(1);
    steps.extend([
        (
            200,
            json!({"object":"list","type":"view","has_more":false,"next_cursor":null,
                "results":[{"object":"view","id":VIEW},{"object":"view","id":OTHER_VIEW},
                    {"object":"view","id":FIRST}]}),
        ),
        view_object(VIEW, "Active deliveries", "table", SOURCE),
        view_object(OTHER_VIEW, "Other source", "table", OTHER_SOURCE),
        view_object(FIRST, "Dashboard", "dashboard", SOURCE),
    ]);
    let (result, log) = call(steps, query(json!({"data_source_id":SOURCE})), false).await;
    let output = &result["result"]["structuredContent"];
    assert_eq!(
        output["views"],
        json!([{"id":VIEW,"name":"Active deliveries","type":"table"}]),
        "{result}"
    );
    assert_eq!(output["status"], "ok");
    assert_eq!(output["truncated"], false);
    assert!(
        log.iter().any(
            |(line, _)| line.starts_with(&format!("GET /v1/views?database_id={DB}&page_size="))
        ),
        "{log:?}"
    );
}

#[tokio::test]
async fn view_listing_is_refused_outside_scope_and_never_mixes_modes() {
    let mut outside = scoped_view(false).split_off(1);
    outside[1].1["parent"] = json!({"type":"workspace","workspace":true});
    outside.truncate(2);
    let (result, _) = call(outside, query(json!({"data_source_id":SOURCE})), false).await;
    assert_eq!(result["result"]["isError"], true, "{result}");
    let text = result["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.starts_with("source_not_accessible"), "{text}");

    for arguments in [
        json!({"data_source_id":SOURCE,"view_id":VIEW}),
        json!({"data_source_id":SOURCE,"refs":[]}),
        json!({"data_source_id":"nope"}),
    ] {
        let (result, upstream) = call(vec![], query(arguments), false).await;
        assert!(
            result["error"].is_object() || result["result"]["isError"] == true,
            "{result}"
        );
        assert!(upstream.is_empty());
    }
}
