//! Strict test-only page inventory fixture for MCP boundary contract tests.
use std::sync::Arc;

use notion_knowledge_core::{
    backend::PageId,
    discovery::ExclusionRules,
    inventory::{InventoryEntry, InventoryParent},
    lifecycle::LifecycleScope,
    root_scope::{Membership, ScopeError},
};
use notion_knowledge_mcp::KnowledgeServer;

pub const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const OTHER_ROOT: &str = "33333333-3333-3333-3333-333333333333";
const ALLOWED_ROOT: &str = "55555555-5555-5555-5555-555555555555";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

/// Every page was walked directly under `ROOT`.
struct Fixture;

fn entry(id: &str, parent: InventoryParent) -> InventoryEntry {
    InventoryEntry {
        page_id: id.to_owned(),
        parent,
        root_id: ROOT.to_owned(),
        last_edited_time: "2026-10-10T11:00:00Z".into(),
        in_trash: false,
    }
}

impl Membership for Fixture {
    fn parent_chain(&self, page: &str) -> Result<Option<Vec<InventoryEntry>>, ScopeError> {
        let root = entry(ROOT, InventoryParent::Root);
        Ok(Some(if page == ROOT {
            vec![root]
        } else {
            vec![entry(page, InventoryParent::Page(ROOT.into())), root]
        }))
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
