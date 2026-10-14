//! Contract tests for #63: relation direction, bounded traversal and physical scope.
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::ExclusionRules,
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    root_scope::RootScopeGate,
    sync_state::SyncStateError,
};
use notion_knowledge_mcp::{
    KnowledgeServer,
    related::{self, RelatedError, RelatedRequest},
};
use rmcp::{ServerHandler, ServiceExt};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const ROOT: &str = "11111111-1111-1111-1111-111111111111";
const A: &str = "22222222-2222-2222-2222-222222222222";
const B: &str = "33333333-3333-3333-3333-333333333333";
const C: &str = "44444444-4444-4444-4444-444444444444";
const BLOCKED: &str = "55555555-5555-5555-5555-555555555555";
const WORKSPACE: &str = "66666666-6666-6666-6666-666666666666";

fn denied() -> BackendError {
    BackendError {
        kind: BackendErrorKind::PermissionDenied,
        operation: "related.fixture",
        retry_after: None,
        committed_page_id: None,
    }
}
fn node(id: &str, parent: PhysicalParent) -> LifecycleNode {
    LifecycleNode {
        id: id.into(),
        kind: LifecycleKind::Page,
        revision: "2026-10-14T00:00:00Z".into(),
        inactive: false,
        parent,
    }
}
struct LifecycleFixture;
impl PageLifecycle for LifecycleFixture {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            if page.0 == BLOCKED {
                return Ok(LifecycleEvidence {
                    scope: scope.clone(),
                    status: LifecycleStatus::OutsideScope,
                    ancestry: vec![node(BLOCKED, PhysicalParent::Workspace)],
                });
            }
            if page.0 == "77777777-7777-7777-7777-777777777777" {
                return Err(denied());
            }
            let ancestry = if page.0 == ROOT {
                vec![node(ROOT, PhysicalParent::Workspace)]
            } else {
                vec![
                    node(
                        &page.0,
                        PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: ROOT.into(),
                        },
                    ),
                    node(ROOT, PhysicalParent::Workspace),
                ]
            };
            Ok(LifecycleEvidence {
                scope: scope.clone(),
                status: LifecycleStatus::Allowed {
                    roots: vec![ROOT.into()],
                },
                ancestry,
            })
        })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if &evidence.scope == scope {
                Ok(())
            } else {
                Err(denied())
            }
        })
    }
}
fn gate() -> RootScopeGate {
    RootScopeGate::new(
        Arc::new(LifecycleFixture),
        LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        },
    )
    .unwrap()
}
fn edge(source: &str, target: &str, relation: &str, provenance: &str) -> GraphEdge {
    GraphEdge::new(
        source.into(),
        GraphTarget::Page {
            page_id: target.into(),
        },
        relation.into(),
        provenance.into(),
    )
    .unwrap()
}
struct GraphFixture {
    fail: bool,
    huge: bool,
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
        if self.fail {
            return Err(SyncStateError::Unavailable);
        }
        if page == ROOT {
            if self.huge {
                return Ok((0..257)
                    .map(|i| edge(ROOT, A, "link:page", &format!("block:{i}")))
                    .collect());
            }
            let mut out = vec![
                edge(ROOT, A, "relation", "property:friend"),
                edge(ROOT, A, "relation", "property:friend"),
                edge(ROOT, BLOCKED, "relation", "property:secret"),
                GraphEdge::new(
                    ROOT.into(),
                    GraphTarget::Unresolved {
                        reference: "unresolved private link".into(),
                    },
                    "link:page".into(),
                    "block:unknown".into(),
                )
                .unwrap(),
            ];
            out.reverse();
            Ok(out)
        } else if page == A {
            Ok(vec![edge(A, C, "link:page", "block:child")])
        } else {
            Ok(vec![])
        }
    }
    fn edges_to(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        if self.fail {
            return Err(SyncStateError::Unavailable);
        }
        if page == ROOT {
            Ok(vec![edge(B, ROOT, "link:page", "block:parent")])
        } else {
            Ok(vec![])
        }
    }
}
fn graph() -> GraphFixture {
    GraphFixture {
        fail: false,
        huge: false,
    }
}
fn request(depth: u8, limit: usize) -> RelatedRequest {
    RelatedRequest {
        page_id: ROOT.into(),
        depth,
        limit,
    }
}
#[test]
fn request_defaults_and_validates_bounds() {
    let default: RelatedRequest = serde_json::from_value(json!({"page_id":ROOT})).unwrap();
    assert_eq!(default.depth, 1);
    assert_eq!(default.limit, 50);
    assert!(default.validate().is_ok());
    assert!(request(0, 5).validate().is_err());
    assert!(request(3, 5).validate().is_err());
    assert!(request(1, 0).validate().is_err());
    assert!(request(1, 101).validate().is_err());
    assert!(
        serde_json::from_value::<RelatedRequest>(json!({"page_id":ROOT,"root_page_id":BLOCKED}))
            .is_err()
    );
    let uppercase: RelatedRequest = serde_json::from_value(
        json!({"page_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_ascii_uppercase()}),
    )
    .unwrap();
    assert!(uppercase.validate().is_err());
}
#[tokio::test]
async fn traversal_is_deduplicated_directional_and_depth_bounded() {
    let roots = vec![ROOT.to_owned()];
    let one = related::navigate(&request(1, 20), &graph(), &gate(), &roots)
        .await
        .unwrap();
    assert_eq!(one.relations.len(), 2);
    assert!(!one.limit_reached);
    assert!(
        one.relations
            .iter()
            .all(|e| e.depth == 1 && e.target_page_id != BLOCKED)
    );
    let outgoing = one
        .relations
        .iter()
        .find(|e| e.source_page_id == ROOT)
        .unwrap();
    assert_eq!(outgoing.target_page_id, A);
    assert_eq!(outgoing.direction, "outgoing");
    assert_eq!(outgoing.relation_type, "relation");
    assert_eq!(outgoing.provenance, "property:friend");
    let incoming = one
        .relations
        .iter()
        .find(|e| e.source_page_id == B)
        .unwrap();
    assert_eq!(
        incoming.target_page_id, ROOT,
        "incoming edge endpoints must not be reversed"
    );
    assert_eq!(incoming.direction, "incoming");

    let two = related::navigate(&request(2, 20), &graph(), &gate(), &roots)
        .await
        .unwrap();
    assert_eq!(two.relations.len(), 3);
    assert!(
        two.relations
            .iter()
            .any(|e| e.source_page_id == A && e.target_page_id == C && e.depth == 2)
    );
    let limited = related::navigate(&request(2, 1), &graph(), &gate(), &roots)
        .await
        .unwrap();
    assert_eq!(limited.relations.len(), 1);
    assert!(limited.limit_reached);
}
#[tokio::test]
async fn inaccessible_root_and_storage_failures_are_closed() {
    let roots = vec![ROOT.to_owned()];
    let mut blocked = request(1, 10);
    blocked.page_id = BLOCKED.into();
    assert!(matches!(
        related::navigate(&blocked, &graph(), &gate(), &roots).await,
        Err(RelatedError::Inaccessible)
    ));
    // Unknown provider access must not silently produce incomplete results.
    let mut unverifiable = request(1, 10);
    unverifiable.page_id = "77777777-7777-7777-7777-777777777777".into();
    assert!(matches!(
        related::navigate(&unverifiable, &graph(), &gate(), &roots).await,
        Err(RelatedError::Unavailable)
    ));
    assert!(matches!(
        related::navigate(&request(1, 10), &graph(), &gate(), &[BLOCKED.into()]).await,
        Err(RelatedError::Inaccessible)
    ));
    assert!(matches!(
        related::navigate(
            &request(1, 10),
            &GraphFixture {
                fail: true,
                huge: false
            },
            &gate(),
            &roots
        )
        .await,
        Err(RelatedError::Unavailable)
    ));
    assert!(matches!(
        related::navigate(
            &request(1, 10),
            &GraphFixture {
                fail: false,
                huge: true
            },
            &gate(),
            &roots
        )
        .await,
        Err(RelatedError::Unavailable)
    ));
}

async fn exchange(handler: KnowledgeServer, arguments: Value) -> Value {
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
    for call in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"related-test","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
            "name":"knowledge_related","arguments":arguments}}),
    ] {
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
async fn mcp_exposes_scoped_graph_only_when_installed_and_checks_wire_contract() {
    assert!(
        KnowledgeServer::default()
            .get_tool("knowledge_related")
            .is_none()
    );
    assert!(
        KnowledgeServer::default()
            .and_related_graph(Arc::new(graph()))
            .is_err()
    );
    let configured = KnowledgeServer::default()
        .and_authoritative_scope(
            Arc::new(LifecycleFixture),
            gate().configured_scope().clone(),
        )
        .unwrap()
        .and_related_graph(Arc::new(graph()))
        .unwrap();
    let tool = configured.get_tool("knowledge_related").unwrap();
    let wire = serde_json::to_value(tool).unwrap();
    assert_eq!(wire["annotations"]["readOnlyHint"], true);
    assert_eq!(wire["inputSchema"]["properties"]["depth"]["default"], 1);
    assert_eq!(wire["inputSchema"]["properties"]["depth"]["maximum"], 2);
    assert!(
        configured
            .tool_catalog()
            .iter()
            .any(|tool| tool.name.as_ref() == "knowledge_related")
    );

    let response = exchange(configured, json!({"page_id":ROOT})).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        response["result"]["structuredContent"]["relations"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let invalid = KnowledgeServer::default()
        .and_authoritative_scope(
            Arc::new(LifecycleFixture),
            gate().configured_scope().clone(),
        )
        .unwrap()
        .and_related_graph(Arc::new(graph()))
        .unwrap();
    let response = exchange(invalid, json!({"page_id":ROOT,"depth":9})).await;
    assert!(response.get("error").is_some(), "{response}");
}
