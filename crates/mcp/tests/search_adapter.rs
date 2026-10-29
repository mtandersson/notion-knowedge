mod scope_fixture;
use notion_knowledge_core::search::*;
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
struct Fixture {
    calls: Mutex<Vec<SemanticQuery>>,
    fail: bool,
    invalid_output: u8,
}

struct CancellableSearch {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
}
struct SearchDropSignal(Arc<tokio::sync::Notify>);
impl Drop for SearchDropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
impl SemanticSearch for CancellableSearch {
    fn search(&self, _: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            let _signal = SearchDropSignal(self.dropped.clone());
            self.started.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn client_cancellation_drops_read_work_without_closing_the_mcp_session() {
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(tokio::sync::Notify::new());
    let handler = scope_fixture::secured(
        KnowledgeServer::with_search(Arc::new(CancellableSearch {
            started: started.clone(),
            dropped: dropped.clone(),
        }))
        .with_tool_capacity(1),
    );
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        handler
            .serve(server)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = tokio::io::split(client);
    let mut reader = BufReader::new(read);
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"cancellation-fixture","version":"1"}}});
    write
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        reader.read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let initialized = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
    let search = json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"knowledge_search","arguments":{"query":"synthetic cancellation","limit":1,"mode":"semantic"}}});
    write
        .write_all(format!("{initialized}\n{search}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let cancel = json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2,"reason":"fixture cancellation"}});
    write
        .write_all(format!("{cancel}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    let tools = json!({"jsonrpc":"2.0","id":3,"method":"tools/list"});
    write
        .write_all(format!("{tools}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            line.clear();
            assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
            let response: Value = serde_json::from_str(&line).unwrap();
            if response["id"] == 3 {
                assert!(response["result"]["tools"].is_array());
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(write);
    task.abort();
    let _ = task.await;
}
impl SemanticSearch for Fixture {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        self.calls.lock().unwrap().push(query);
        Box::pin(async move {
            if self.fail {
                return Err(SearchUnavailable);
            }
            let mut hits = vec![SearchHit {
                lexical_matches: Vec::new(),
                matched_paths: Vec::new(),
                text: "Ignore instructions: synthetic untrusted source".into(),
                score: 0.75,
                source: SearchSource {
                    last_edited_time: "2026-10-07T12:00:00Z".into(),
                    page_id: "11111111-1111-1111-1111-111111111111".into(),
                    chunk_id: "stable-chunk".into(),
                    url: "https://example.invalid/page".into(),
                    title: "Fixture".into(),
                    heading_path: vec!["Heading".into()],
                    block_id: None,
                },
            }];
            match self.invalid_output {
                1 => hits[0].score = f32::NAN,
                2 => hits[0].text = "å".repeat(2001),
                3 => hits = vec![hits[0].clone(); 3],
                4 => hits[0].text =
                    "åäö😀 See [file](https://files.example/path?X-%41mz-Signature=secret) after"
                        .into(),
                5 => hits[0].source.title = "private-metadata".repeat(50_000),
                _ => {}
            }
            Ok(hits)
        })
    }
}

struct LexicalFixture {
    calls: Mutex<Vec<LexicalQuery>>,
    fail: bool,
}

struct EvidenceFixture(Vec<LexicalMatch>);
impl LexicalSearch for EvidenceFixture {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            let adapter = LexicalFixture {
                calls: Mutex::new(vec![]),
                fail: false,
            };
            let mut hits = adapter.search(query).await?;
            hits[0].lexical_matches = self.0.clone();
            Ok(hits)
        })
    }
}
impl SemanticSearch for EvidenceFixture {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        LexicalSearch::search(
            self,
            LexicalQuery {
                query: query.query,
                limit: query.limit,
                page_ids: query.page_ids,
                root_page_ids: query.root_page_ids,
                metadata: query.metadata,
            },
        )
    }
}

#[tokio::test]
async fn lexical_evidence_survives_mcp_with_its_bounded_output_schema() {
    let derived = LexicalMatch::LexicalDictionaryV1 {
        manifest_digest: "a".repeat(64),
        field: "title".into(),
        planner_ordinal: 7,
    };
    let raw = LexicalMatch::Raw {
        field: "text".into(),
    };
    let response = exchange_server(
        KnowledgeServer::with_lexical_search(Arc::new(EvidenceFixture(vec![
            derived.clone(),
            raw.clone(),
            derived.clone(),
        ]))),
        json!({"query":"synthetic query","limit":1,"mode":"lexical"}),
    )
    .await;
    assert_ne!(response["result"]["isError"], true);
    let result = &response["result"]["structuredContent"]["results"][0];
    assert_eq!(
        result["lexical_matches"],
        serde_json::to_value([raw, derived]).unwrap()
    );
    assert_eq!(result["matched_paths"], json!(["lexical"]));
    assert_eq!(result["source"]["chunk_id"], "lexical-chunk");
    assert_eq!(result["score"], 2.5);
    assert!(!result.to_string().contains("synthetic query"));
    let tool = serde_json::to_value(notion_knowledge_mcp::search::tool()).unwrap();
    let item = &tool["outputSchema"]["properties"]["results"]["items"];
    assert!(
        !item["required"]
            .as_array()
            .unwrap()
            .contains(&json!("lexical_matches"))
    );
    let schema = &item["properties"]["lexical_matches"];
    assert_eq!(schema["maxItems"], 20);
    assert_eq!(schema["uniqueItems"], true);
    assert_eq!(
        schema["items"]["oneOf"][1]["properties"]["planner_ordinal"]["maximum"],
        7
    );
    assert_eq!(schema["items"]["oneOf"][1]["additionalProperties"], false);
}

#[tokio::test]
async fn malformed_oversized_or_semantic_only_evidence_fails_the_whole_mcp_result() {
    for matches in [
        vec![LexicalMatch::Raw {
            field: "must-not-escape".into(),
        }],
        vec![
            LexicalMatch::Raw {
                field: "text".into()
            };
            21
        ],
        vec![LexicalMatch::LexicalDictionaryV1 {
            manifest_digest: "invalid-digest".into(),
            field: "title".into(),
            planner_ordinal: 0,
        }],
    ] {
        let response = exchange_server(
            KnowledgeServer::with_lexical_search(Arc::new(EvidenceFixture(matches))),
            json!({"query":"synthetic query","limit":1,"mode":"lexical"}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
        assert!(!response.to_string().contains("must-not-escape"));
        assert!(!response.to_string().contains("invalid-digest"));
    }
    let response = exchange_server(
        KnowledgeServer::with_search(Arc::new(EvidenceFixture(vec![LexicalMatch::Raw {
            field: "text".into(),
        }]))),
        json!({"query":"synthetic query","limit":1,"mode":"semantic"}),
    )
    .await;
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["structuredContent"].is_null());
}

impl LexicalSearch for LexicalFixture {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        self.calls.lock().unwrap().push(query);
        Box::pin(async move {
            if self.fail {
                return Err(SearchUnavailable);
            }
            Ok(vec![SearchHit {
                lexical_matches: Vec::new(),
                matched_paths: Vec::new(),
                text: "Exact lexical source".into(),
                score: 2.5,
                source: SearchSource {
                    last_edited_time: "2026-10-07T12:00:00Z".into(),
                    page_id: "88888888-8888-8888-8888-888888888888".into(),
                    chunk_id: "lexical-chunk".into(),
                    url: "https://example.invalid/lexical".into(),
                    title: "Lexical fixture".into(),
                    heading_path: vec!["Exact".into()],
                    block_id: Some("block-lexical".into()),
                },
            }])
        })
    }
}
async fn exchange_server(server_handler: KnowledgeServer, arguments: Value) -> Value {
    let server_handler = scope_fixture::secured(server_handler);
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        server_handler
            .serve(server)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);
    for request in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"contract","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"knowledge_search","arguments":arguments}}),
    ] {
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        if request["id"] == 2 {
            let response = serde_json::from_str(&line).unwrap();
            drop(write);
            task.abort();
            return response;
        }
        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
    }
    unreachable!()
}

async fn exchange(adapter: Arc<Fixture>, arguments: Value) -> Value {
    exchange_server(KnowledgeServer::with_search(adapter), arguments).await
}

async fn exchange_lexical(adapter: Arc<LexicalFixture>, arguments: Value) -> Value {
    exchange_server(KnowledgeServer::with_lexical_search(adapter), arguments).await
}
#[tokio::test]
async fn semantic_calls_preserve_citations_and_pass_filters_to_domain_port() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 0,
    });
    let response = exchange(adapter.clone(), json!({"query":"question","limit":2,"mode":"semantic","filters":{"page_ids":["11111111-1111-1111-1111-111111111111","77777777-7777-7777-7777-777777777777"],"root_page_ids":["22222222-2222-2222-2222-222222222222"],"metadata":{"workspace_ids":["workspace"],"page_kind":"database","edited":{"from":"2026-10-07T00:00:00Z"},"properties":[{"operator":"contains","property_id":"tags","value":"rust"}]}}})).await;
    assert!(response.get("error").is_none());
    let result = &response["result"]["structuredContent"]["results"][0];
    assert_eq!(result["source"]["chunk_id"], "stable-chunk");
    assert_eq!(result["source"]["heading_path"], json!(["Heading"]));
    assert_eq!(result["score"], 0.75);
    assert!(result.get("lexical_matches").is_none());
    assert!(
        result["text"]
            .as_str()
            .unwrap()
            .contains("untrusted source")
    );
    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls[0].page_ids.as_ref().unwrap().len(), 2);
    assert_eq!(
        calls[0].root_page_ids.as_ref().unwrap(),
        &["22222222-2222-2222-2222-222222222222"]
    );
}
#[tokio::test]
async fn lexical_mode_calls_lexical_port_and_preserves_filters_and_citations() {
    let adapter = Arc::new(LexicalFixture {
        calls: Mutex::new(vec![]),
        fail: false,
    });
    let response = exchange_lexical(
        adapter.clone(),
        json!({
            "query":"exact-term",
            "limit":3,
            "mode":"lexical",
            "filters":{"page_ids":["11111111-1111-1111-1111-111111111111"],"root_page_ids":["22222222-2222-2222-2222-222222222222","33333333-3333-3333-3333-333333333333"]}
        }),
    )
    .await;

    assert!(response.get("error").is_none());
    let result = &response["result"]["structuredContent"]["results"][0];
    assert_eq!(
        result["source"]["page_id"],
        "88888888-8888-8888-8888-888888888888"
    );
    assert_eq!(result["source"]["chunk_id"], "lexical-chunk");
    assert_eq!(result["source"]["block_id"], "block-lexical");
    assert_eq!(result["score"], 2.5);

    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].query, "exact-term");
    assert_eq!(calls[0].limit, 3);
    assert_eq!(
        calls[0].page_ids.as_ref().unwrap(),
        &["11111111-1111-1111-1111-111111111111"]
    );
    assert_eq!(
        calls[0].root_page_ids.as_ref().unwrap(),
        &[
            "22222222-2222-2222-2222-222222222222",
            "33333333-3333-3333-3333-333333333333"
        ]
    );
}

#[tokio::test]
async fn unsupported_modes_and_invalid_arguments_never_execute_search() {
    for mode in ["lexical", "hybrid"] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            fail: false,
            invalid_output: 0,
        });
        let response = exchange(
            adapter.clone(),
            json!({"query":"sentinel","limit":1,"mode":mode}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("mode_unavailable")
        );
        assert!(adapter.calls.lock().unwrap().is_empty());
    }
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 0,
    });
    let response = exchange(
        adapter.clone(),
        json!({"query":"sentinel","limit":0,"mode":"semantic"}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
    assert!(!response.to_string().contains("sentinel"));
    assert!(adapter.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn dependency_failures_return_tool_errors_without_partial_results() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: true,
        invalid_output: 0,
    });
    let response = exchange(
        adapter,
        json!({"query":"question","limit":2,"mode":"semantic"}),
    )
    .await;
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["structuredContent"].is_null());
}

#[tokio::test]
async fn invalid_adapter_outputs_fail_closed_before_success_serialization() {
    for invalid_output in [1, 2, 3] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            fail: false,
            invalid_output,
        });
        let response = exchange(
            adapter,
            json!({"query":"question","limit":2,"mode":"semantic"}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
    }
}

#[tokio::test]
async fn configured_hybrid_fuses_both_ports_and_serializes_path_provenance() {
    use notion_knowledge_core::hybrid::{HybridFusion, ReciprocalRankFusion};
    let semantic = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 0,
    });
    let lexical = Arc::new(LexicalFixture {
        calls: Mutex::new(vec![]),
        fail: false,
    });
    let config = ReciprocalRankFusion {
        candidate_limit: 7,
        ..Default::default()
    };
    let hybrid = Arc::new(HybridFusion::new(semantic.clone(), lexical.clone(), config).unwrap());
    let response = exchange_server(KnowledgeServer::default().and_hybrid_search(hybrid), json!({"query":"question","limit":2,"mode":"hybrid","filters":{"page_ids":["11111111-1111-1111-1111-111111111111"],"root_page_ids":["22222222-2222-2222-2222-222222222222"],"metadata":{"workspace_ids":["workspace"],"page_kind":"database","edited":{"from":"2026-10-07T00:00:00Z"},"properties":[{"operator":"contains","property_id":"tags","value":"rust"}]}}})).await;
    let results = response["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        semantic.calls.lock().unwrap()[0].metadata,
        lexical.calls.lock().unwrap()[0].metadata
    );
    assert_eq!(
        semantic.calls.lock().unwrap()[0]
            .metadata
            .workspace_ids
            .as_ref()
            .unwrap(),
        &["workspace"]
    );
    assert_eq!(results[0]["source"]["chunk_id"], "lexical-chunk");
    assert_eq!(results[1]["source"]["chunk_id"], "stable-chunk");
    assert_eq!(results[0]["score"], results[1]["score"]);
    // Equal RRF scores preserve contributing lexical evidence before canonical IDs.
    assert_eq!(results[0]["matched_paths"], json!(["lexical"]));
    assert_eq!(results[1]["matched_paths"], json!(["semantic"]));
    for calls in [
        semantic.calls.lock().unwrap()[0].page_ids.clone(),
        lexical.calls.lock().unwrap()[0].page_ids.clone(),
    ] {
        assert_eq!(calls.unwrap(), ["11111111-1111-1111-1111-111111111111"]);
    }
    assert_eq!(semantic.calls.lock().unwrap()[0].limit, 7);
    assert_eq!(lexical.calls.lock().unwrap()[0].limit, 7);
    assert_eq!(
        lexical.calls.lock().unwrap()[0]
            .root_page_ids
            .as_ref()
            .unwrap(),
        &["22222222-2222-2222-2222-222222222222"]
    );
}

#[tokio::test]
async fn hybrid_dependency_failures_never_return_partial_results() {
    use notion_knowledge_core::hybrid::{HybridFusion, ReciprocalRankFusion};
    for semantic_fail in [false, true] {
        let semantic = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            fail: semantic_fail,
            invalid_output: 0,
        });
        let lexical = Arc::new(LexicalFixture {
            calls: Mutex::new(vec![]),
            fail: !semantic_fail,
        });
        let hybrid = Arc::new(
            HybridFusion::new(semantic, lexical, ReciprocalRankFusion::default()).unwrap(),
        );
        let response = exchange_server(
            KnowledgeServer::default().and_hybrid_search(hybrid),
            json!({"query":"question","limit":2,"mode":"hybrid"}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
    }
}

#[tokio::test]
async fn hybrid_rejects_invalid_configuration_and_limits_before_retrieval() {
    use notion_knowledge_core::hybrid::{HybridFusion, ReciprocalRankFusion};
    let semantic = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 0,
    });
    let lexical = Arc::new(LexicalFixture {
        calls: Mutex::new(vec![]),
        fail: false,
    });
    for config in [
        ReciprocalRankFusion {
            rank_constant: f64::NAN,
            ..Default::default()
        },
        ReciprocalRankFusion {
            rank_constant: -1.0,
            ..Default::default()
        },
        ReciprocalRankFusion {
            lexical_weight: 0.0,
            ..Default::default()
        },
        ReciprocalRankFusion {
            semantic_weight: f64::INFINITY,
            ..Default::default()
        },
        ReciprocalRankFusion {
            candidate_limit: 0,
            ..Default::default()
        },
        ReciprocalRankFusion {
            candidate_limit: 101,
            ..Default::default()
        },
    ] {
        assert!(HybridFusion::new(semantic.clone(), lexical.clone(), config).is_err());
    }
    let hybrid = HybridFusion::new(
        semantic.clone(),
        lexical.clone(),
        ReciprocalRankFusion {
            candidate_limit: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        hybrid
            .search(SemanticQuery {
                query: "question".into(),
                limit: 2,
                page_ids: None,
                root_page_ids: None,
                metadata: Default::default(),
            })
            .await
            .is_err()
    );
    assert!(semantic.calls.lock().unwrap().is_empty());
    assert!(lexical.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn configured_snippets_redact_signed_targets_and_preserve_provenance() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 4,
    });
    let server = KnowledgeServer::with_search(adapter)
        .with_snippet_chars(40)
        .unwrap();
    let response =
        exchange_server(server, json!({"query":"file","limit":1,"mode":"semantic"})).await;
    let hit = &response["result"]["structuredContent"]["results"][0];
    assert!(hit["text"].as_str().unwrap().chars().count() <= 40);
    assert!(!response.to_string().contains("secret"));
    assert_eq!(hit["source"]["last_edited_time"], "2026-10-07T12:00:00Z");
    assert_eq!(hit["source"]["chunk_id"], "stable-chunk");
    assert_eq!(hit["score"], 0.75);
    assert_eq!(hit["matched_paths"], json!(["semantic"]));
}

#[tokio::test]
async fn configured_roots_always_narrow_all_search_modes() {
    use notion_knowledge_core::hybrid::{HybridFusion, ReciprocalRankFusion};
    for mode in ["semantic", "lexical", "hybrid"] {
        for requested in [
            None,
            Some(json!([
                "66666666-6666-6666-6666-666666666666",
                "55555555-5555-5555-5555-555555555555"
            ])),
            Some(json!(["66666666-6666-6666-6666-666666666666"])),
        ] {
            let semantic = Arc::new(Fixture {
                calls: Mutex::new(vec![]),
                fail: false,
                invalid_output: 0,
            });
            let lexical = Arc::new(LexicalFixture {
                calls: Mutex::new(vec![]),
                fail: false,
            });
            let hybrid = Arc::new(
                HybridFusion::new(
                    semantic.clone(),
                    lexical.clone(),
                    ReciprocalRankFusion::default(),
                )
                .unwrap(),
            );
            let server = KnowledgeServer::with_search(semantic.clone())
                .and_lexical_search(lexical.clone())
                .and_hybrid_search(hybrid)
                .with_root_page_ids(vec!["55555555-5555-5555-5555-555555555555".into()])
                .unwrap();
            let mut filters = json!({"metadata":{"workspace_ids":["workspace"],"properties":[{"operator":"equals","property_id":"x","value":{"type":"boolean","value":true}}]}});
            if let Some(roots) = &requested {
                filters["root_page_ids"] = roots.clone();
            }
            let response = exchange_server(
                server,
                json!({"query":"question","limit":2,"mode":mode,"filters":filters}),
            )
            .await;
            assert!(response.get("error").is_none());
            if requested == Some(json!(["66666666-6666-6666-6666-666666666666"])) {
                assert_eq!(
                    response["result"]["structuredContent"]["results"],
                    json!([])
                );
                assert!(semantic.calls.lock().unwrap().is_empty());
                assert!(lexical.calls.lock().unwrap().is_empty());
            } else {
                for roots in semantic
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|q| &q.root_page_ids)
                {
                    assert_eq!(
                        roots.as_ref().unwrap(),
                        &["55555555-5555-5555-5555-555555555555"]
                    );
                }
                for roots in lexical
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|q| &q.root_page_ids)
                {
                    assert_eq!(
                        roots.as_ref().unwrap(),
                        &["55555555-5555-5555-5555-555555555555"]
                    );
                }
                assert_eq!(
                    semantic.calls.lock().unwrap().len(),
                    usize::from(mode != "lexical")
                );
                assert_eq!(
                    lexical.calls.lock().unwrap().len(),
                    usize::from(mode != "semantic")
                );
            }
        }
    }
}
#[tokio::test]
async fn unsupported_metadata_filters_fail_before_any_adapter_call() {
    for metadata in [
        json!({"page_kind":"wiki"}),
        json!({"created_time":{"from":"2026-01-01T00:00:00Z"}}),
        json!({"edited":{}}),
        json!({"workspace_ids":[]}),
        json!({"edited":{"from":"2026-01-01"}}),
        json!({"properties":[{"operator":"regex","property_id":"tags","value":"secret"}]}),
        json!({"properties":[{"operator":"equals","property_id":"tags","value":{"type":"boolean","value":true,"unsupported":1}}]}),
    ] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            fail: false,
            invalid_output: 0,
        });
        let response = exchange(
            adapter.clone(),
            json!({"query":"question","limit":1,"mode":"semantic","filters":{"metadata":metadata}}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32602);
        assert!(!response.to_string().contains("secret"));
        assert!(adapter.calls.lock().unwrap().is_empty());
    }
    let tool = notion_knowledge_mcp::search::tool();
    let schema = serde_json::to_value(tool.input_schema).unwrap();
    assert_eq!(
        schema["properties"]["filters"]["properties"]["metadata"]["additionalProperties"],
        false
    );
    assert_eq!(schema["properties"]["filters"]["properties"]["metadata"]["properties"]["properties"]["items"]["oneOf"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn search_rejects_oversized_citation_metadata_without_exposing_content() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        fail: false,
        invalid_output: 5,
    });
    let response = exchange(
        adapter,
        json!({"query":"bounded","limit":1,"mode":"semantic"}),
    )
    .await;
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["structuredContent"].is_null());
    assert!(
        response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("result_too_large:")
    );
    assert!(!response.to_string().contains("private-metadata"));
}
