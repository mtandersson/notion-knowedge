//! Explicit guarded mutations sharing the integration sync worker's authority.
use crate::{
    config::{Config, NotionAuth},
    root_scope::Transport,
    webhook_worker::runtime::Worker,
};
use notion_knowledge_core::audit::{AuditActor, AuditRetention};
use notion_knowledge_mcp::{KnowledgeServer, mutations::MutationRuntime};
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::{audit::SqliteAuditStore, idempotency::SqliteIdempotencyStore};
use std::sync::Arc;

pub async fn configured(
    handler: KnowledgeServer,
    config: &Config,
    worker: &Worker,
) -> Result<KnowledgeServer, &'static str> {
    let NotionAuth::Integration(token) = &config.notion_auth else {
        return Err("integration write source unavailable");
    };
    let source = Arc::new(
        NotionClient::integration(token.expose_secret())
            .map_err(|_| "integration write source unavailable")?,
    );
    compose(handler, config, worker, source).await
}

/// Typed local-provider seam; no endpoint or model can be selected by tool input.
pub async fn compose(
    handler: KnowledgeServer,
    config: &Config,
    worker: &Worker,
    source: Arc<NotionClient>,
) -> Result<KnowledgeServer, &'static str> {
    let stores = config
        .integration_write_stores
        .as_ref()
        .ok_or("integration write stores unavailable")?;
    if config.read_only
        || config.oauth_discovery.is_some()
        || !matches!(config.notion_auth, NotionAuth::Integration(_))
        || !crate::root_scope::trusted_transport(config, Transport::Http)
    {
        return Err("integration write transport unavailable");
    }
    crate::oauth_writes::validate_stores(config, stores)?;
    let scope = config
        .notion_scope
        .clone()
        .ok_or("integration write scope unavailable")?;
    if worker.scope.workspace_id != scope.workspace_id
        || !Arc::ptr_eq(&worker.state, worker.coordinator.state())
    {
        return Err("integration write worker scope mismatch");
    }
    let ledger = Arc::new(
        SqliteIdempotencyStore::open_existing(&stores.ledger)
            .map_err(|_| "existing integration mutation ledger unavailable")?,
    );
    let audit = Arc::new(
        SqliteAuditStore::open(&stores.audit, AuditRetention::default())
            .map_err(|_| "integration mutation audit unavailable")?,
    );
    let handler = crate::root_scope::attach_client(
        handler,
        config,
        Transport::Http,
        source.as_ref().clone(),
    )?;
    let handler = crate::retrieval_runtime::attach_existing(
        handler,
        config,
        Transport::Http,
        source.clone(),
        worker.provider.clone(),
        worker.handler.chunks().clone(),
        worker.coordinator.clone(),
    )
    .await?;
    let handler = if let Some(index) = &config.graph_index {
        if std::fs::canonicalize(index).ok()
            != std::fs::canonicalize(worker.coordinator.index_directory()).ok()
        {
            return Err("integration graph index must match the shared sync worker");
        }
        handler.and_related_graph(Arc::new(
            notion_knowledge_retrieval::graph::CommittedGraph::new(worker.coordinator.clone()),
        ))?
    } else {
        handler
    };
    let mutations = MutationRuntime::new(
        source,
        scope,
        ledger,
        worker.state.clone(),
        audit,
        AuditActor::Automation,
    )
    .map_err(|_| "integration mutation scope unavailable")?;
    Ok(handler.and_mutations(Arc::new(mutations)))
}
