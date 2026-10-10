//! End-to-end MCP authorization regressions: no indexed content escapes a
//! missing, foreign, moved or unverifiable physical Notion root scope.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::ExclusionRules,
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    search::{
        SearchFuture, SearchHit, SearchSource, SemanticQuery, SemanticSearch,
    },
    source::{
        ExpandedSource, SourceContentScope, SourceExpandQuery, SourceExpansion,
        SourceExpansionFuture, SourceProvenance,
    },
};
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServiceExt;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";

#[derive(Clone, Copy)]
enum Ancestry {
    Allowed,
    Foreign,
    Missing,
    MovedAfterFirst,
}

struct Physical {
    mode: Ancestry,
    calls: AtomicUsize,
}
impl Physical {
    fn new(mode: Ancestry) -> Arc<Self> {
        Arc::new(Self { mode, calls: AtomicUsize::new(0) })
    }
}

fn failure() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unavailable,
        operation: "fixture.physical_root",
        retry_after: None,
        committed_page_id: None,
    }
}

fn physical_node(id: &str, parent: PhysicalParent) -> LifecycleNode {
    LifecycleNode {
        id: id.into(),
        kind: LifecycleKind::Page,
        revision: "2026-10-10T12:00:00Z".into(),
        inactive: false,
        parent,
    }
}

impl PageLifecycle for Physical {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Ancestry::Missing) {
                return Err(failure());
            }
            let allowed = match self.mode {
                Ancestry::Allowed => true,
                Ancestry::Foreign | Ancestry::Missing => false,
                Ancestry::MovedAfterFirst => attempt == 0,
            };
            let parent_id = if allowed { ROOT } else { WORKSPACE };
            Ok(LifecycleEvidence {
                scope: scope.clone(),
                status: if allowed {
                    LifecycleStatus::Allowed { roots: vec![ROOT.into()] }
                } else {
                    LifecycleStatus::OutsideScope
                },
                ancestry: vec![
                    physical_node(
                        &page.0,
                        PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: parent_id.into(),
                        },
                    ),
                    physical_node(parent_id, PhysicalParent::Workspace),
                ],
            })
        })
    }

    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        current_scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if &evidence.scope == current_scope {
                Ok(())
            } else {
                Err(failure())
            }
        })
    }
}

struct Search {
    calls: AtomicUsize,
}
impl Search {
    fn new() -> Arc<Self> {
        Arc::new(Self { calls: AtomicUsize::new(0) })
    }
}
impl SemanticSearch for Search {
    fn search(&self, _query: SemanticQuery) -> SearchFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(vec![SearchHit {
                text: "PRIVATE_CONTENT_SHOULD_NOT_LEAK".into(),
                score: 0.9,
                matched_paths: vec![],
                source: SearchSource {
                    page_id: PAGE.into(),
                    chunk_id: "chunk-id".into(),
                    url: "https://example.invalid/page".into(),
                    title: "private title".into(),
                    last_edited_time: "2026-10-10T12:00:00Z".into(),
                    heading_path: vec![],
                    block_id: None,
                },
            }])
        })
    }
}

struct Expand {
    calls: AtomicUsize,
}
impl Expand {
    fn new() -> Arc<Self> {
        Arc::new(Self { calls: AtomicUsize::new(0) })
    }
}
impl SourceExpansion for Expand {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(query.refs.into_iter().map(|reference| ExpandedSource {
                reference,
                text: "PRIVATE_CONTENT_SHOULD_NOT_LEAK".into(),
                truncated: false,
                content_scope: SourceContentScope::Indexed,
                provenance: SourceProvenance {
                    page_id: PAGE.into(),
                    root_page_id: ROOT.into(),
                    url: "https://example.invalid/page".into(),
                    title: "private title".into(),
                    heading_path: vec![],
                    block_id: None,
                    chunk_ids: vec!["chunk-id".into()],
                    indexed_last_edited_time: "2026-10-10T12:00:00Z".into(),
                    refreshed_last_edited_time: None,
                    index_stale: None,
                },
            }).collect())
        })
    }
}

fn secure(server: KnowledgeServer, source: Arc<Physical>) -> KnowledgeServer {
    server.and_authoritative_scope(
        source,
        LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        }
    ).unwrap()
}

async fn invoke(handler: KnowledgeServer, tool: &str, arguments: Value) -> Value {
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        handler.serve(server).await.unwrap().waiting().await.unwrap();
    });
    let (reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(reader);
    let messages = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"scope-test","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":tool,"arguments":arguments}}),
    ];
    for message in messages {
        writer.write_all(format!("{message}\n").as_bytes()).await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        if message["id"] == 2 {
            task.abort();
            return serde_json::from_str(&line).unwrap();
        }
        writer.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n").await.unwrap();
    }
    unreachable!()
}

fn denied(response: Value) {
    assert_eq!(response["result"]["isError"], true, "{response}");
    assert!(!response.to_string().contains("PRIVATE_CONTENT_SHOULD_NOT_LEAK"));
    assert!(!response.to_string().contains("private title"));
}

#[tokio::test]
async fn search_denies_missing_scope_without_calling_index() {
    let index = Search::new();
    let response = invoke(
        KnowledgeServer::with_search(index.clone()),
        "knowledge_search",
        json!({"query":"secret","limit":1,"mode":"semantic"}),
    ).await;
    denied(response);
    assert_eq!(index.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn search_denies_foreign_or_unverifiable_index_hits() {
    for mode in [Ancestry::Foreign, Ancestry::Missing] {
        let index = Search::new();
        let source = Physical::new(mode);
        let response = invoke(
            secure(KnowledgeServer::with_search(index.clone()), source),
            "knowledge_search",
            json!({"query":"secret","limit":1,"mode":"semantic"}),
        ).await;
        denied(response);
        assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn direct_page_filter_is_rejected_before_index_search() {
    let index = Search::new();
    let source = Physical::new(Ancestry::Foreign);
    let response = invoke(
        secure(KnowledgeServer::with_search(index.clone()), source),
        "knowledge_search",
        json!({"query":"secret","limit":1,"mode":"semantic","filters":{"page_ids":[PAGE]}}),
    ).await;
    denied(response);
    assert_eq!(index.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_denies_direct_page_when_scope_is_missing() {
    let source = Expand::new();
    let handler = KnowledgeServer::with_source_expansion(source.clone(), vec![ROOT.into()]).unwrap();
    let response = invoke(
        handler,
        "knowledge_get",
        json!({"refs":[{"kind":"page","id":PAGE}],"max_chars":100}),
    ).await;
    denied(response);
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_refuses_moved_page_between_preflight_and_read() {
    let adapter = Expand::new();
    let source = Physical::new(Ancestry::MovedAfterFirst);
    let handler = secure(
        KnowledgeServer::with_source_expansion(adapter.clone(), vec![ROOT.into()]).unwrap(),
        source.clone(),
    );
    let response = invoke(
        handler,
        "knowledge_get",
        json!({"refs":[{"kind":"page","id":PAGE}],"max_chars":100}),
    ).await;
    denied(response);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert!(source.calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn get_resolves_chunk_reference_then_checks_real_page_scope() {
    let adapter = Expand::new();
    let handler = secure(
        KnowledgeServer::with_source_expansion(adapter.clone(), vec![ROOT.into()]).unwrap(),
        Physical::new(Ancestry::Foreign),
    );
    let response = invoke(
        handler,
        "knowledge_get",
        json!({"refs":[{"kind":"chunk","id":"chunk-id"}],"max_chars":100}),
    ).await;
    denied(response);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn search_and_get_allow_only_authorized_pages() {
    let search = Search::new();
    let response = invoke(
        secure(KnowledgeServer::with_search(search), Physical::new(Ancestry::Allowed)),
        "knowledge_search",
        json!({"query":"secret","limit":1,"mode":"semantic"}),
    ).await;
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert!(response.to_string().contains("PRIVATE_CONTENT_SHOULD_NOT_LEAK"));

    let adapter = Expand::new();
    let response = invoke(
        secure(
            KnowledgeServer::with_source_expansion(adapter, vec![ROOT.into()]).unwrap(),
            Physical::new(Ancestry::Allowed),
        ),
        "knowledge_get",
        json!({"refs":[{"kind":"chunk","id":"chunk-id"}],"max_chars":100}),
    ).await;
    assert_eq!(response["result"]["isError"], Value::Null, "{response}");
    assert!(response.to_string().contains("PRIVATE_CONTENT_SHOULD_NOT_LEAK"));
}
