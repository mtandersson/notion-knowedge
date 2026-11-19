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

/// Validate existing storage and parent ownership before recovery may create
/// missing cache paths. A missing cache does not relax the trust boundary.
fn validate_recovery_paths(config: &Config) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    let own = std::fs::metadata("/proc/self")
        .map_err(|_| "retrieval ownership unavailable")?
        .uid();
    let state = config
        .webhook_state_file
        .as_deref()
        .ok_or("retrieval state unavailable")?;
    for (path, directory) in [(index_path(config)?, true), (state, false)] {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                if metadata.uid() != own
                    || metadata.mode() & 0o077 != 0
                    || (directory && !metadata.is_dir())
                    || (!directory && !metadata.is_file())
                {
                    return Err("retrieval storage must be private and operator-owned");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err("retrieval storage unavailable"),
        }
        for ancestor in path
            .parent()
            .ok_or("retrieval storage unavailable")?
            .ancestors()
        {
            match std::fs::symlink_metadata(ancestor) {
                Ok(metadata) => {
                    if !metadata.is_dir()
                        || (metadata.uid() != own && metadata.uid() != 0)
                        || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
                    {
                        return Err("retrieval storage must be operator-controlled");
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err("retrieval ownership unavailable"),
            }
        }
    }
    Ok(())
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
    validate_recovery_paths(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    // Reject unusable lexical assets before loading the larger model.
    crate::index_runtime::configured_plan(settings)?;
    let NotionAuth::Integration(token) = &config.notion_auth else {
        return Err("authoritative retrieval source unavailable");
    };
    let source = Arc::new(
        NotionClient::integration(token.expose_secret())
            .map_err(|_| "authoritative retrieval source unavailable")?,
    );
    let provider = notion_knowledge_retrieval::embedder::load_configured_embedder(
        &notion_knowledge_retrieval::embedder::EmbedderConfig::LocalQwen {
            assets: settings.assets.clone(),
            batch_size: settings.embed_batch_size,
        },
        None,
    )
    .await
    .map_err(|_| "retrieval model unavailable or incompatible")?;
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
    let plan = crate::index_runtime::configured_plan(config.retrieval.as_ref().unwrap())?;
    validate_recovery_paths(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let scope = config
        .notion_scope
        .clone()
        .ok_or("retrieval scope unavailable")?;
    validate_plan(settings, plan.as_ref())?;
    let state = config
        .webhook_state_file
        .as_deref()
        .ok_or("retrieval state unavailable")?;
    let runtime = if let Some(plan) = plan.clone() {
        crate::index_runtime::IndexRuntime::recover_lexical_cache(
            settings,
            state,
            scope.clone(),
            source.clone(),
            provider.clone(),
            plan,
            crate::index_runtime::OpenMode::ReadOnly,
        )
        .await?
    } else {
        crate::index_runtime::IndexRuntime::recover_for_reads(
            settings,
            state,
            scope.clone(),
            source.clone(),
            provider.clone(),
        )
        .await?
    };
    attach_existing(
        handler,
        config,
        transport,
        source,
        provider,
        runtime.chunks().clone(),
        runtime.coordinator,
    )
    .await
}

/// Attach queries to the sync worker's existing model, table and coordinator.
pub(crate) async fn attach_existing(
    handler: KnowledgeServer,
    config: &Config,
    transport: Transport,
    source: Arc<NotionClient>,
    provider: Arc<dyn EmbeddingProvider>,
    table: GuardedChunkTable,
    coordinator: Arc<notion_knowledge_retrieval::commit::IndexCommitCoordinator>,
) -> Result<KnowledgeServer, &'static str> {
    validate_owned(config)?;
    let settings = config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?;
    let scope = config
        .notion_scope
        .clone()
        .ok_or("retrieval scope unavailable")?;
    let policy = notion_knowledge_retrieval::planner::production_scope(&scope)
        .map_err(|_| "retrieval scope unavailable")?;
    if !coordinator
        .binding()
        .matches_policy(&scope.workspace_id, &policy)
        || table.binding() != coordinator.binding()
        || std::fs::canonicalize(coordinator.index_directory()).ok()
            != Some(
                std::fs::canonicalize(&settings.index)
                    .map_err(|_| "retrieval index unavailable")?,
            )
    {
        return Err("shared retrieval authority mismatch");
    }
    let plan = crate::index_runtime::configured_plan(settings)?;
    table
        .search_snapshot()
        .await
        .map_err(|_| "retrieval index unavailable or incompatible")?;
    let exact_id = plan.is_some();
    let adapters = search_adapters(settings, table.clone(), provider, plan)?;
    let handler = if matches!(transport, Transport::Http) {
        handler.with_index_health_probe(crate::index_readiness::observe(table.clone(), exact_id))
    } else {
        handler
    };
    let handler = handler
        .and_source_expansion(
            Arc::new(table),
            scope.roots.iter().map(|r| r.0.clone()).collect(),
        )?
        .and_authoritative_scope(crate::inventory_membership::from_config(config)?, scope)
        .map(|h| {
            h.and_fresh_source(source)
                .and_search(adapters.semantic)
                .and_lexical_search(adapters.lexical)
                .and_hybrid_search(adapters.hybrid)
        })?;
    if config.graph_boost.enabled {
        if config.graph_index.as_ref() != Some(&settings.index) {
            return Err("graph boost requires the retrieval index binding");
        }
        handler.and_graph_boost(
            Arc::new(notion_knowledge_retrieval::graph::CommittedGraph::new(
                coordinator,
            )),
            config.graph_boost,
        )
    } else {
        Ok(handler)
    }
}
fn validate_plan(
    settings: &crate::webhook_worker::WorkerSettings,
    plan: Option<&notion_knowledge_retrieval::chunks::lexical_binding::LexicalCachePlan>,
) -> Result<(), &'static str> {
    match (settings.lexical.as_ref(), plan) {
        (None, None) => Ok(()),
        (Some(settings), Some(plan))
            if settings.expected_sha256() == plan.dictionary().manifest_digest() =>
        {
            Ok(())
        }
        _ => Err("lexical configuration and verified plan differ"),
    }
}

struct SearchAdapters {
    semantic: Arc<dyn SemanticSearch>,
    lexical: Arc<dyn LexicalSearch>,
    hybrid: Arc<dyn HybridSearch>,
}
fn search_adapters(
    settings: &crate::webhook_worker::WorkerSettings,
    table: GuardedChunkTable,
    provider: Arc<dyn EmbeddingProvider>,
    plan: Option<notion_knowledge_retrieval::chunks::lexical_binding::LexicalCachePlan>,
) -> Result<SearchAdapters, &'static str> {
    validate_plan(settings, plan.as_ref())?;
    if let Some(plan) = plan {
        let pool = notion_knowledge_retrieval::chunks::bounded::CandidateBudget::new(
            settings.lexical.as_ref().unwrap().max_requests(),
        )
        .map_err(|_| "lexical admission unavailable")?;
        let search = Arc::new(
            notion_knowledge_retrieval::lexical_runtime::LexicalSearchRuntime::new(
                table,
                provider,
                plan,
                pool,
                std::time::Duration::from_secs(settings.timeout_seconds),
            )
            .map_err(|_| "configured lexical retrieval unavailable")?,
        );
        return Ok(SearchAdapters {
            semantic: search.clone(),
            lexical: search.clone(),
            hybrid: search,
        });
    }
    Ok(SearchAdapters {
        semantic: Arc::new(ScopedSearch {
            table: table.clone(),
            provider: provider.clone(),
        }),
        lexical: Arc::new(ScopedLexical {
            table: table.clone(),
        }),
        hybrid: Arc::new(ScopedHybrid { table, provider }),
    })
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
