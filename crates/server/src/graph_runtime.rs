//! Default-off graph installation from compatible existing committed state.
use crate::config::Config;
use notion_knowledge_mcp::KnowledgeServer;
use notion_knowledge_retrieval::{
    commit::{CommitBinding, IndexCommitCoordinator},
    graph::CommittedGraph,
    planner::production_scope,
};
use std::{os::unix::fs::MetadataExt, sync::Arc};

pub(crate) fn attach(
    handler: KnowledgeServer,
    config: &Config,
) -> Result<KnowledgeServer, &'static str> {
    attach_mode(handler, config, false)
}

pub(crate) fn attach_request(
    handler: KnowledgeServer,
    config: &Config,
) -> Result<KnowledgeServer, &'static str> {
    attach_mode(handler, config, true)
}
fn attach_mode(
    handler: KnowledgeServer,
    config: &Config,
    request_bound: bool,
) -> Result<KnowledgeServer, &'static str> {
    let Some(index) = &config.graph_index else {
        return Ok(handler);
    };
    let state = config
        .webhook_state_file
        .as_deref()
        .ok_or("graph state unavailable")?;
    let own = std::fs::metadata("/proc/self")
        .map_err(|_| "graph ownership unavailable")?
        .uid();
    crate::retrieval_runtime::validate_storage(index, state, own)?;
    let scope = config
        .notion_scope
        .as_ref()
        .ok_or("graph scope unavailable")?;
    let journal = production_scope(scope).map_err(|_| "graph scope unavailable")?;
    let binding = CommitBinding::new(
        "chunks",
        &scope.workspace_id,
        &journal,
        &scope.generation.to_string(),
        scope.generation,
    )
    .map_err(|_| "graph binding invalid")?;
    let coordinator = IndexCommitCoordinator::open_read_only(index, state, binding)
        .map_err(|_| "graph state unavailable or incompatible")?;
    let handler =
        handler.with_root_page_ids(scope.roots.iter().map(|root| root.0.clone()).collect())?;
    let graph = Arc::new(CommittedGraph::new(coordinator));
    if request_bound {
        Ok(handler.and_request_graph(graph, config.graph_boost))
    } else {
        handler.and_related_graph(graph)
    }
}
