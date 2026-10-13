mod scope_fixture;
use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    graph_boost::GraphBoostConfig,
    search::{HybridSearch, RetrievalPath, SearchFuture, SearchHit, SearchSource, SemanticQuery},
    sync_state::SyncStateError,
};
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const ANCHOR: &str = "11111111-1111-1111-1111-111111111111";
const LINKED: &str = "77777777-7777-7777-7777-777777777777";
const UNRELATED: &str = "88888888-8888-8888-8888-888888888888";

struct HybridFixture {
    queried: Mutex<Vec<SemanticQuery>>,
}
impl HybridSearch for HybridFixture {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        self.queried.lock().unwrap().push(query.clone());
        Box::pin(async move {
            let hits = [
                (ANCHOR, "seed", 1.0, true),
                (UNRELATED, "unrelated", 0.91, false),
                (LINKED, "related", 0.85, false),
            ];
            Ok(hits
                .into_iter()
                .take(query.limit)
                .map(|(page, chunk, score, joined)| SearchHit {
                    text: "fixture".into(),
                    score,
                    source: SearchSource {
                        page_id: page.into(),
                        chunk_id: chunk.into(),
                        url: format!("https://example.invalid/{page}"),
                        title: chunk.into(),
                        heading_path: vec![],
                        block_id: None,
                        last_edited_time: "2026-10-10T00:00:00Z".into(),
                    },
                    matched_paths: if joined {
                        vec![RetrievalPath::Semantic, RetrievalPath::Lexical]
                    } else {
                        vec![RetrievalPath::Semantic]
                    },
                })
                .collect())
        })
    }
}
#[derive(Default)]
struct GraphFixture {
    reads: AtomicUsize,
}
impl GraphEdgeStore for GraphFixture {
    fn replace_page_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn replace_page_relation_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn replace_page_link_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn edges_from(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if page == ANCHOR {
            Ok(vec![
                GraphEdge::new(
                    ANCHOR.into(),
                    GraphTarget::Page {
                        page_id: LINKED.into(),
                    },
                    "link:page".into(),
                    "markdown:1".into(),
                )
                .unwrap(),
            ])
        } else {
            Ok(vec![])
        }
    }
    fn edges_to(&self, _: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
}

async fn exchange(handler: KnowledgeServer) -> Value {
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
    let calls = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"graph-test","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
            "name":"knowledge_search","arguments":{"query":"fixture","limit":3,"mode":"hybrid"}}}),
    ];
    for call in calls {
        write
            .write_all(format!("{call}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        if call["id"] == 2 {
            task.abort();
            return serde_json::from_str(&line).unwrap();
        }
        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
    }
    unreachable!()
}

#[tokio::test]
async fn mcp_hybrid_graph_boost_preserves_score_and_applies_scope_gate() {
    let hybrid = Arc::new(HybridFixture {
        queried: Mutex::new(vec![]),
    });
    let graph = Arc::new(GraphFixture::default());
    let base = scope_fixture::secured(KnowledgeServer::default().and_hybrid_search(hybrid.clone()));
    let boosted = base
        .and_graph_boost(
            graph.clone(),
            GraphBoostConfig {
                enabled: true,
                max_hops: 1,
                weight: 0.2,
                candidate_limit: 3,
            },
        )
        .unwrap();
    let response = exchange(boosted).await;
    assert!(response.get("error").is_none(), "{response}");
    let output = response["result"]["structuredContent"]["results"]
        .as_array()
        .unwrap();
    assert_eq!(output.len(), 3);
    let chunks: Vec<_> = output
        .iter()
        .map(|item| item["source"]["chunk_id"].as_str().unwrap())
        .collect();
    assert_eq!(chunks, ["seed", "related", "unrelated"]);
    assert!((output[1]["score"].as_f64().unwrap() - 0.85).abs() < 1e-6);
    assert_eq!(output[1]["matched_paths"], json!(["semantic"]));
    assert_eq!(hybrid.queried.lock().unwrap()[0].limit, 3);
    assert_eq!(graph.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mcp_rejects_boost_without_authoritative_scope_or_hybrid() {
    let graph = Arc::new(GraphFixture::default());
    let default = KnowledgeServer::default();
    assert!(
        default
            .and_graph_boost(
                graph.clone(),
                GraphBoostConfig {
                    enabled: true,
                    ..Default::default()
                }
            )
            .is_err()
    );
    let secured = scope_fixture::secured(KnowledgeServer::default());
    assert!(
        secured
            .and_graph_boost(
                graph.clone(),
                GraphBoostConfig {
                    enabled: true,
                    ..Default::default()
                }
            )
            .is_err()
    );
    let scoped = scope_fixture::secured(KnowledgeServer::default().and_hybrid_search(Arc::new(
        HybridFixture {
            queried: Mutex::new(vec![]),
        },
    )));
    assert!(
        scoped
            .and_graph_boost(
                graph,
                GraphBoostConfig {
                    enabled: true,
                    max_hops: 8,
                    ..Default::default()
                }
            )
            .is_err()
    );
}
