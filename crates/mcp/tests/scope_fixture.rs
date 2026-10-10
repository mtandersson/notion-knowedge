//! Strict test-only physical ancestry fixture for MCP boundary contract tests.
use std::sync::Arc;

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::ExclusionRules,
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
};
use notion_knowledge_mcp::KnowledgeServer;

pub const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const OTHER_ROOT: &str = "33333333-3333-3333-3333-333333333333";
const ALLOWED_ROOT: &str = "55555555-5555-5555-5555-555555555555";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

struct Fixture;

fn node(id: &str, parent: PhysicalParent) -> LifecycleNode {
    LifecycleNode {
        id: id.to_owned(),
        kind: LifecycleKind::Page,
        revision: "2026-10-10T11:00:00Z".into(),
        inactive: false,
        parent,
    }
}

fn error() -> BackendError {
    BackendError {
        kind: BackendErrorKind::PermissionDenied,
        operation: "fixture.scope",
        retry_after: None,
        committed_page_id: None,
    }
}

impl PageLifecycle for Fixture {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            let parent = if page.0 == ROOT {
                PhysicalParent::Object {
                    kind: LifecycleKind::Page,
                    id: ALLOWED_ROOT.into(),
                }
            } else {
                PhysicalParent::Object {
                    kind: LifecycleKind::Page,
                    id: ROOT.into(),
                }
            };
            let nodes = if page.0 == ROOT {
                vec![
                    node(ROOT, parent),
                    node(ALLOWED_ROOT, PhysicalParent::Workspace),
                ]
            } else {
                vec![
                    node(&page.0, parent),
                    node(
                        ROOT,
                        PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: ALLOWED_ROOT.into(),
                        },
                    ),
                    node(ALLOWED_ROOT, PhysicalParent::Workspace),
                ]
            };
            Ok(LifecycleEvidence {
                scope: scope.clone(),
                status: LifecycleStatus::Allowed {
                    roots: vec![ROOT.into(), ALLOWED_ROOT.into()],
                },
                ancestry: nodes,
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
                Err(error())
            }
        })
    }
}

pub fn secured(handler: KnowledgeServer) -> KnowledgeServer {
    handler
        .and_authoritative_scope(
            Arc::new(Fixture),
            LifecycleScope {
                workspace_id: WORKSPACE.into(),
                generation: 1,
                roots: vec![
                    PageId(ROOT.into()),
                    PageId(OTHER_ROOT.into()),
                    PageId(ALLOWED_ROOT.into()),
                ],
                exclusions: ExclusionRules::default(),
            },
        )
        .unwrap()
}
