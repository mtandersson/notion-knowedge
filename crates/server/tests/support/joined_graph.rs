//! Final joined graph/alias/vector/FTS smoke; all hits come from committed adapters.
use super::*;
use notion_knowledge_server::{
    index_runtime::{IndexRuntime, OpenMode},
    root_scope::Transport,
};

fn config(runtime: &TestRuntime, enabled: bool) -> notion_knowledge_server::config::Config {
    let mut config = retrieval_config(runtime);
    config.graph_index = Some(runtime.index.clone());
    config.graph_boost.enabled = enabled;
    config.graph_boost.candidate_limit = 3;
    config.retrieval.as_mut().unwrap().alias_sources =
        notion_knowledge_core::aliases::AliasSources::new(true, vec!["alias".into()]).unwrap();
    config
}
async fn fixture() -> TestRuntime {
    let runtime =
        TestRuntime::with_joined_sources("webhook-v1", ExclusionRules::default(), true).await;
    runtime.source.change(|m| {
        m.title = "Saffron fyr Lighthouse".into();
        m.markdown = format!("Saffron fyr Saffron fyr ANKARE. Home energy dashboard. Hushållets eldata. [CSV](https://www.notion.so/{OTHER}) [denied](https://www.notion.so/{FOREIGN})");
    });
    commit_search_fixture(&runtime).await;
    runtime
        .worker
        .state
        .receive(&event(THIRD, "page.created"))
        .unwrap();
    tick(&runtime.worker, false).await;
    runtime
}
fn query(id: u64) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"knowledge_search","arguments":{"query":"Saffron fyr","limit":3,"mode":"hybrid"}}})
}
fn hits(output: &Value) -> Vec<Value> {
    output["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap_or_else(|| panic!("{output}"))
        .clone()
}
fn identity(hit: &Value) -> String {
    hit["source"]["chunk_id"].as_str().unwrap().into()
}
fn assert_same_candidates_and_scores(before: &[Value], after: &[Value]) {
    let map = |rows: &[Value]| {
        rows.iter()
            .map(|h| {
                (
                    identity(h),
                    (h["score"].clone(), h["matched_paths"].clone()),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(map(before), map(after));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_committed_bilingual_aliases_navigation_and_off_on_ranking_survive_reopen_and_edits()
{
    let runtime = fixture().await;
    let off_config = config(&runtime, false);
    let on_config = config(&runtime, true);
    let read = IndexRuntime::compose(
        off_config.retrieval.as_ref().unwrap(),
        &runtime.db,
        off_config.notion_scope.clone().unwrap(),
        runtime.source.client(),
        runtime.worker.provider.clone(),
        OpenMode::ReadOnly,
    )
    .await
    .unwrap();
    assert_eq!(
        read.lookup_alias("saffron FYR lighthouse").unwrap(),
        vec![PAGE.to_string()]
    );
    assert_eq!(
        read.lookup_alias("FYREN / lighthouse").unwrap(),
        vec![PAGE.to_string(), OTHER.to_string()]
    );
    assert!(read.lookup_alias("CSV").unwrap().is_empty()); // body text is not an alias
    let off = installed_retrieval(&runtime, &off_config, Transport::Stdio)
        .await
        .unwrap();
    let on = installed_retrieval(&runtime, &on_config, Transport::Stdio)
        .await
        .unwrap();
    let baseline = hits(&wire_stdio(off.clone(), vec![query(2)]).await[1]);
    let output = wire_stdio(
        on.clone(),
        vec![
            query(2),
            related_request(3, json!({"page_id":PAGE,"depth":2})),
            related_request(4, json!({"page_id":OTHER})),
        ],
    )
    .await;
    let boosted = hits(&output[1]);
    assert_eq!(baseline.len(), 3);
    assert_same_candidates_and_scores(&baseline, &boosted);
    let rank = |rows: &[Value], page: &str| {
        rows.iter()
            .position(|h| h["source"]["page_id"] == page)
            .unwrap()
    };
    assert_eq!(rank(&baseline, PAGE), 0);
    assert!(
        rank(&boosted, OTHER) < rank(&baseline, OTHER),
        "baseline={baseline:?} boosted={boosted:?}"
    );
    assert_eq!(rank(&boosted, PAGE), 0);
    let edges = output[2]["result"]["structuredContent"]["relations"]
        .as_array()
        .unwrap();
    assert!(
        edges
            .iter()
            .all(|e| e["target_page_id"] == OTHER && e["source_page_id"] == PAGE)
    );
    assert!(edges.iter().any(|e| {
        e["relation_type"]
            .as_str()
            .unwrap()
            .starts_with("relation:")
    }));
    assert!(
        edges
            .iter()
            .any(|e| e["relation_type"].as_str().unwrap().starts_with("link:"))
    );
    assert!(
        !output[3]["result"]["structuredContent"]["relations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // Caller JSON cannot toggle settings or choose a graph authority.
    let mut injected = query(2);
    injected["params"]["arguments"]["graph_boost"] = json!({"enabled":true,"index":"/tmp/foreign"});
    let result = wire_stdio(off, vec![injected]).await;
    assert_eq!(result[1]["error"]["code"], -32602);

    runtime.source.change(|m| {
        m.markdown = "Saffron fyr ANKARE. Updated energy plan. Ny elplan.".into();
        m.title = "Updated fyr".into();
        m.linked = false;
        m.revision = "2026-10-10T13:00:00Z".into();
    });
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.content_updated"))
        .unwrap();
    tick(&runtime.worker, false).await;
    let output = wire_stdio(
        on.clone(),
        vec![query(2), related_request(3, json!({"page_id":PAGE}))],
    )
    .await;
    assert_eq!(
        output[2]["result"]["structuredContent"]["relations"],
        json!([])
    );
    assert!(
        hits(&output[1])
            .iter()
            .any(|h| h["text"].as_str().unwrap().contains("Ny elplan"))
    );
    assert!(
        read.lookup_alias("Saffron fyr Lighthouse")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        read.lookup_alias("updated fyr").unwrap(),
        vec![PAGE.to_string()]
    );
    runtime.source.change(|m| m.parent = FOREIGN.into());
    let output = wire_stdio(
        on.clone(),
        vec![query(2), related_request(3, json!({"page_id":PAGE}))],
    )
    .await;
    assert!(
        output
            .iter()
            .skip(1)
            .all(|v| !v.to_string().contains("Ny elplan"))
    );
    runtime.source.change(|m| {
        m.parent = ROOT.into();
        m.archived = true;
        m.revision = "2026-10-10T14:00:00Z".into();
    });
    runtime
        .worker
        .state
        .receive(&event(PAGE, "page.deleted"))
        .unwrap();
    tick(&runtime.worker, false).await;
    assert!(read.lookup_alias("updated fyr").unwrap().is_empty());
    assert_eq!(
        read.lookup_alias("Fyren / Lighthouse").unwrap(),
        vec![OTHER.to_string()]
    );
    let output = wire_stdio(on, vec![query(2)]).await;
    assert!(
        hits(&output[1])
            .iter()
            .all(|h| h["source"]["page_id"] != PAGE)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_boost_holds_one_committed_read_fence_and_fails_closed_on_pending_effects() {
    let runtime = fixture().await;
    let config = config(&runtime, true);
    let on = installed_retrieval(&runtime, &config, Transport::Stdio)
        .await
        .unwrap();
    runtime.source.pause_next.store(true, Ordering::SeqCst);
    let pending = tokio::spawn(wire_stdio(on.clone(), vec![query(2)]));
    tokio::time::timeout(Duration::from_secs(10), runtime.source.entered.notified())
        .await
        .unwrap();
    // Authorization may await network I/O while the graph+index revision remains fenced.
    let writer = std::fs::File::open(&runtime.index).unwrap();
    assert!(matches!(
        writer.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    runtime.source.proceed.notify_one();
    assert_eq!(hits(&pending.await.unwrap()[1]).len(), 3);
    assert!(writer.try_lock().is_ok());
    drop(writer);
    rusqlite::Connection::open(&runtime.db)
        .unwrap()
        .execute(
            "UPDATE index_commit_receipts SET status=0 WHERE status=1",
            [],
        )
        .unwrap();
    let output = wire_stdio(on, vec![query(2)]).await;
    assert!(output[1].to_string().contains("retrieval_unavailable"));
    assert!(!output[1].to_string().contains("ANKARE"));
}

async fn http_calls(
    runtime: &TestRuntime,
    mut config: notion_knowledge_server::config::Config,
    requests: Vec<Value>,
) -> Vec<Value> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.http_bind = listener.local_addr().unwrap();
    let address = config.http_bind;
    drop(listener);
    let handler = installed_retrieval(runtime, &config, Transport::Http)
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let serving = tokio::spawn(notion_knowledge_server::http::serve_installed(
        config,
        handler,
        None,
        stop.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::net::TcpStream::connect(address).await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let url = format!("http://{address}/mcp");
    let initialize = client.post(&url).bearer_auth("fixture-private-authentication-token")
        .header("accept","application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"joined-boost","version":"1"}}}))
        .send().await.unwrap();
    assert!(initialize.status().is_success());
    let session = initialize.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    http_json(initialize).await;
    let response = client
        .post(&url)
        .bearer_auth("fixture-private-authentication-token")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let before = runtime.embeddings.load(Ordering::SeqCst);
    for token in [None, Some("wrong")] {
        let mut request = client
            .post(&url)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .json(&query(2));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(runtime.embeddings.load(Ordering::SeqCst), before);
    let mut output = vec![];
    for request in requests {
        let response = client
            .post(&url)
            .bearer_auth("fixture-private-authentication-token")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        output.push(http_json(response).await);
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_http_uses_same_committed_boost_and_navigation_with_fresh_root_denial() {
    let runtime = fixture().await;
    let before = std::fs::read(&runtime.db).unwrap();
    let index = storage_files(&runtime.index);
    let off = http_calls(&runtime, config(&runtime, false), vec![query(2)]).await;
    let on = http_calls(
        &runtime,
        config(&runtime, true),
        vec![
            query(2),
            related_request(3, json!({"page_id":PAGE,"depth":2,"limit":2})),
        ],
    )
    .await;
    let baseline = hits(&off[0]);
    let boosted = hits(&on[0]);
    assert_same_candidates_and_scores(&baseline, &boosted);
    assert_ne!(
        baseline.iter().map(identity).collect::<Vec<_>>(),
        boosted.iter().map(identity).collect::<Vec<_>>()
    );
    let edges = on[1]["result"]["structuredContent"]["relations"]
        .as_array()
        .unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().all(|e| e["target_page_id"] == OTHER));
    assert_eq!(before, std::fs::read(&runtime.db).unwrap());
    assert_eq!(index, storage_files(&runtime.index));
    runtime.source.change(|m| m.parent = FOREIGN.into());
    let denied = http_calls(
        &runtime,
        config(&runtime, true),
        vec![query(2), related_request(3, json!({"page_id":PAGE}))],
    )
    .await;
    for result in denied {
        assert!(!result.to_string().contains("ANKARE"));
        assert!(result["result"]["isError"].as_bool().unwrap_or(false));
    }
}
