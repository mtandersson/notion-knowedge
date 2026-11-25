use notion_knowledge_mcp::{KnowledgeServer, view::TOOL};
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
            "sorts":[{"property":"Due","direction":"descending"}]
        }),
    )];
    route.extend([
        (200,json!({"object":"data_source","id":SOURCE,"parent":{"type":"database_id","database_id":DB}})),
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
            "Status":{"id":"status","type":"status","status":{"name":"Done","id":"option"}},
            "Due":{"id":"date","type":"date","date":{"start":due,"end":null,"time_zone":null}}
        }}),
    )
}

async fn call(
    steps: Vec<(u16, Value)>,
    request: Value,
    http: bool,
) -> (Value, Vec<(String, Vec<u8>)>) {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), http);
    let (source, log, fake) = notion(steps).await;
    let handler = root_scope::attach_client(
        KnowledgeServer::default(),
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
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":TOOL,"arguments":arguments}})
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
            .find(|tool| tool["name"] == TOOL)
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(
            tool["inputSchema"]["properties"]["page_size"]["maximum"],
            25
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
    assert_eq!(output["rows"][0]["page_id"], FIRST);
    assert_eq!(output["rows"][1]["page_id"], SECOND);
    assert_eq!(
        output["rows"][0]["properties"]["Due"]["date"]["start"],
        "2026-11-29"
    );
    assert_eq!(
        output["rows"][0]["properties"]["Status"]["status"]["name"],
        "Done"
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
    let mut unsupported = scoped_view(false);
    unsupported[0].1["type"] = json!("dashboard");
    unsupported.truncate(1);
    let mut unavailable = scoped_view(false);
    unavailable.push((503, json!({"message":"Never expose upstream detail"})));
    for (steps, code) in [
        (denied, "source_not_accessible"),
        (unsupported, "unsupported_view"),
        (unavailable, "notion_unavailable"),
    ] {
        let (result, _) = call(steps, query(json!({"view_id":VIEW})), false).await;
        assert_eq!(result["result"]["isError"], true, "{result}");
        let text = result["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.starts_with(code), "{text}");
    }
}
