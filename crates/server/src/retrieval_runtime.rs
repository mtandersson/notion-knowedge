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

fn permitted(config: &Config, transport: Transport) -> bool {
    matches!(transport, Transport::Stdio)
        || (config.bearer_fallback.is_some()
            && config.http_bind.ip().is_loopback()
            && config.oauth_discovery.is_none())
}

fn validate_owned(config: &Config) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    let own = std::fs::metadata("/proc/self")
        .map_err(|_| "retrieval ownership unavailable")?
        .uid();
    let index = &config
        .retrieval
        .as_ref()
        .ok_or("retrieval settings unavailable")?
        .index;
    let state = config
        .webhook_state_file
        .as_deref()
        .ok_or("retrieval state unavailable")?;
    for (path, directory) in [(index.as_path(), true), (state, false)] {
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
    if config.retrieval.is_none() || !permitted(config, transport) {
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
    if config.retrieval.is_none() || !permitted(config, transport) {
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
        runtime.coordinator,
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
    let lexical: Arc<dyn LexicalSearch> = Arc::new(ScopedLexical { table });
    handler
        .with_root_page_ids(scope.roots.iter().map(|r| r.0.clone()).collect())?
        .and_authoritative_scope(source, scope)
        .map(|h| {
            h.and_search(semantic)
                .and_lexical_search(lexical)
                .and_hybrid_search(Arc::new(hybrid))
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
