//! Explicit operator-owned read activation. Request data never selects providers.
use crate::{
    config::{Config, NotionAuth},
    root_scope::Transport,
};
use notion_knowledge_core::{
    embedding::EmbeddingProvider,
    hybrid::{HybridFusion, ReciprocalRankFusion},
    search::{
        HybridSearch, LexicalQuery, LexicalSearch, SearchFuture, SearchUnavailable, SemanticQuery,
        SemanticSearch,
    },
};
use notion_knowledge_mcp::KnowledgeServer;
use notion_knowledge_notion::NotionClient;
use notion_knowledge_retrieval::chunks::{GuardedChunkTable, LanceSemanticSearch};
use std::sync::Arc;

fn validate_owned(config: &Config) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    let own = std::fs::metadata("/proc/self")
        .map_err(|_| "retrieval ownership unavailable")?
        .uid();
    validate_storage(
        index_path(config)?,
        config
            .webhook_state_file
            .as_deref()
            .ok_or("retrieval state unavailable")?,
        own,
    )
}
fn index_path(config: &Config) -> Result<&std::path::Path, &'static str> {
    Ok(&config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?
        .index)
}
pub(crate) fn validate_storage(
    index: &std::path::Path,
    state: &std::path::Path,
    own: u32,
) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    for (path, directory) in [(index, true), (state, false)] {
        let metadata =
            std::fs::symlink_metadata(path).map_err(|_| "retrieval storage unavailable")?;
        if metadata.uid() != own
            || metadata.mode() & 0o077 != 0
            || (directory && !metadata.is_dir())
            || (!directory && !metadata.is_file())
        {
            return Err("retrieval storage must be private and operator-owned");
        }
        for ancestor in path
            .parent()
            .ok_or("retrieval storage unavailable")?
            .ancestors()
        {
            let metadata = std::fs::symlink_metadata(ancestor)
                .map_err(|_| "retrieval ownership unavailable")?;
            if !metadata.is_dir()
                || (metadata.uid() != own && metadata.uid() != 0)
                || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
            {
                return Err("retrieval storage must be operator-controlled");
            }
        }
    }
    Ok(())
}

pub async fn configured(
    handler: KnowledgeServer,
    config: &Config,
    transport: Transport,
) -> Result<KnowledgeServer, &'static str> {
    if config.retrieval.is_none() || !crate::root_scope::trusted_transport(config, transport) {
        return Ok(handler);
    }
    validate_owned(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let NotionAuth::Integration(token) = &config.notion_auth else {
        return Err("authoritative retrieval source unavailable");
    };
    let source = Arc::new(
        NotionClient::integration(token.expose_secret())
            .map_err(|_| "authoritative retrieval source unavailable")?,
    );
    let provider = Arc::new(
        notion_knowledge_retrieval::qwen::QwenProvider::load(
            notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
        )
        .await
        .map_err(|_| "retrieval model unavailable or incompatible")?,
    );
    compose(handler, config, transport, source, provider).await
}

/// Typed dependency seam for credential-free joined tests, never environment-selectable.
pub async fn compose(
    handler: KnowledgeServer,
    config: &Config,
    transport: Transport,
    source: Arc<NotionClient>,
    provider: Arc<dyn EmbeddingProvider>,
) -> Result<KnowledgeServer, &'static str> {
    if config.retrieval.is_none() || !crate::root_scope::trusted_transport(config, transport) {
        return Ok(handler);
    }
    validate_owned(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let scope = config
        .notion_scope
        .clone()
        .ok_or("retrieval scope unavailable")?;
    let runtime = crate::index_runtime::IndexRuntime::compose(
        settings,
        config
            .webhook_state_file
            .as_deref()
            .ok_or("retrieval state unavailable")?,
        scope.clone(),
        source.clone(),
        provider.clone(),
        crate::index_runtime::OpenMode::ReadOnly,
    )
    .await?;
    let table = GuardedChunkTable::bind(
        &settings.index,
        "chunks",
        provider.metadata().clone(),
        runtime.coordinator.clone(),
    )
    .map_err(|_| "retrieval binding incompatible")?;
    table
        .search_snapshot()
        .await
        .map_err(|_| "retrieval index unavailable or incompatible")?;
    let hybrid = ScopedHybrid {
        table: table.clone(),
        provider: provider.clone(),
    };
    let semantic: Arc<dyn SemanticSearch> = Arc::new(ScopedSearch {
        table: table.clone(),
        provider,
    });
    let lexical: Arc<dyn LexicalSearch> = Arc::new(ScopedLexical {
        table: table.clone(),
    });
    let handler = handler
        .and_source_expansion(
            Arc::new(table),
            scope.roots.iter().map(|r| r.0.clone()).collect(),
        )?
        .and_authoritative_scope(source.clone(), scope)
        .map(|h| {
            h.and_fresh_source(source)
                .and_search(semantic)
                .and_lexical_search(lexical)
                .and_hybrid_search(Arc::new(hybrid))
        })?;
    if config.graph_boost.enabled {
        if config.graph_index.as_ref() != Some(&settings.index) {
            return Err("graph boost requires the retrieval index binding");
        }
        handler.and_graph_boost(
            Arc::new(notion_knowledge_retrieval::graph::CommittedGraph::new(
                runtime.coordinator,
            )),
            config.graph_boost,
        )
    } else {
        Ok(handler)
    }
}
/// Read-only OAuth query composition. No concrete token client, writer,
/// reconciliation handler, migrations or index repair is constructed here.
pub(crate) async fn configured_oauth(
    handler: KnowledgeServer,
    config: &Config,
) -> Result<KnowledgeServer, &'static str> {
    if config.retrieval.is_none() {
        return Ok(handler);
    }
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let provider = Arc::new(
        notion_knowledge_retrieval::qwen::QwenProvider::load(
            notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
        )
        .await
        .map_err(|_| "retrieval model unavailable or incompatible")?,
    );
    compose_oauth_queries(handler, config, provider).await
}

pub(crate) async fn compose_oauth_queries(
    handler: KnowledgeServer,
    config: &Config,
    provider: Arc<dyn EmbeddingProvider>,
) -> Result<KnowledgeServer, &'static str> {
    Ok(compose_oauth_index(handler, config, provider, false)
        .await?
        .0)
}

/// One index authority shared by request queries and the grant refresh worker.
pub(crate) async fn compose_oauth_index(
    handler: KnowledgeServer,
    config: &Config,
    provider: Arc<dyn EmbeddingProvider>,
    writable: bool,
) -> Result<
    (
        KnowledgeServer,
        Arc<notion_knowledge_retrieval::commit::IndexCommitCoordinator>,
        GuardedChunkTable,
    ),
    &'static str,
> {
    use notion_knowledge_retrieval::{
        commit::{CommitBinding, IndexCommitCoordinator},
        planner::production_scope,
    };
    validate_owned(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let scope = config
        .notion_scope
        .as_ref()
        .ok_or("retrieval scope unavailable")?;
    let journal = production_scope(scope).map_err(|_| "invalid index scope")?;
    let binding = CommitBinding::new(
        "chunks",
        &scope.workspace_id,
        &journal,
        &scope.generation.to_string(),
        scope.generation,
    )
    .map_err(|_| "invalid index binding")?;
    let coordinator = IndexCommitCoordinator::open_read_only(
        &settings.index,
        config
            .webhook_state_file
            .as_deref()
            .ok_or("retrieval state unavailable")?,
        binding.clone(),
    )
    .map_err(|_| "existing index authority unavailable or incompatible")?;
    let table = GuardedChunkTable::bind(
        &settings.index,
        "chunks",
        provider.metadata().clone(),
        coordinator.clone(),
    )
    .map_err(|_| "retrieval binding incompatible")?;
    table
        .search_snapshot()
        .await
        .map_err(|_| "retrieval index unavailable or incompatible")?;
    let (coordinator, table) = if writable {
        // Validate existing vector metadata before opening writable authority.
        let coordinator = IndexCommitCoordinator::open_existing(
            &settings.index,
            config
                .webhook_state_file
                .as_deref()
                .ok_or("retrieval state unavailable")?,
            binding,
        )
        .map_err(|_| "existing index authority unavailable or incompatible")?;
        let table = GuardedChunkTable::bind(
            &settings.index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
        )
        .map_err(|_| "retrieval binding incompatible")?;
        (coordinator, table)
    } else {
        (coordinator, table)
    };
    let handler = handler
        .with_request_bound_content()
        .with_root_page_ids(scope.roots.iter().map(|root| root.0.clone()).collect())?
        .and_search(Arc::new(ScopedSearch {
            table: table.clone(),
            provider: provider.clone(),
        }))
        .and_lexical_search(Arc::new(ScopedLexical {
            table: table.clone(),
        }))
        .and_hybrid_search(Arc::new(ScopedHybrid {
            table: table.clone(),
            provider,
        }))
        .and_source_expansion(
            Arc::new(table.clone()),
            scope.roots.iter().map(|root| root.0.clone()).collect(),
        )?;
    if config.graph_index.is_some() || config.graph_boost.enabled {
        if config.graph_index.as_ref() != Some(&settings.index) {
            return Err("graph requires retrieval index binding");
        }
        Ok((
            handler.and_request_graph(
                Arc::new(notion_knowledge_retrieval::graph::CommittedGraph::new(
                    coordinator.clone(),
                )),
                config.graph_boost,
            ),
            coordinator,
            table,
        ))
    } else {
        Ok((handler, coordinator, table))
    }
}

struct ScopedSearch {
    table: GuardedChunkTable,
    provider: Arc<dyn EmbeddingProvider>,
}
impl SemanticSearch for ScopedSearch {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            let table = Arc::new(
                self.table
                    .search_snapshot()
                    .await
                    .map_err(|_| SearchUnavailable)?,
            );
            LanceSemanticSearch::new(table, self.provider.clone(), 1)
                .map_err(|_| SearchUnavailable)?
                .search(query)
                .await
        })
    }
}
struct ScopedLexical {
    table: GuardedChunkTable,
}
impl LexicalSearch for ScopedLexical {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            LexicalSearch::search(
                &self
                    .table
                    .search_snapshot()
                    .await
                    .map_err(|_| SearchUnavailable)?,
                query,
            )
            .await
        })
    }
}

struct ScopedHybrid {
    table: GuardedChunkTable,
    provider: Arc<dyn EmbeddingProvider>,
}
impl HybridSearch for ScopedHybrid {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            // Both rankers use one validated committed revision per call.
            let table = Arc::new(
                self.table
                    .search_snapshot()
                    .await
                    .map_err(|_| SearchUnavailable)?,
            );
            let semantic = Arc::new(
                LanceSemanticSearch::new(table.clone(), self.provider.clone(), 1)
                    .map_err(|_| SearchUnavailable)?,
            );
            HybridFusion::new(semantic, table, ReciprocalRankFusion::default())?
                .search(query)
                .await
        })
    }
}
