//! Both transports compose the same lazy read-only local inspection.
use crate::{config::Config, webhook::WebhookConfig};
use notion_knowledge_core::sync_status::{SyncStatus, SyncStatusSource};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use std::{path::PathBuf, sync::Arc};
#[derive(Clone)]
pub struct LocalStatus {
    state: Option<PathBuf>,
    inbox: Option<(String, String, String)>,
    #[cfg(all(unix, feature = "webhook-worker"))]
    index: Option<(PathBuf, notion_knowledge_retrieval::commit::CommitBinding)>,
}
impl LocalStatus {
    pub fn from_config(config: &Config) -> Self {
        let inbox = match &config.webhook {
            WebhookConfig::Verified {
                workspace_id,
                subscription_id,
                integration_id,
                ..
            } => Some((
                workspace_id.clone(),
                subscription_id.clone(),
                integration_id.clone(),
            )),
            _ => None,
        };
        #[cfg(all(unix, feature = "webhook-worker"))]
        let index = config
            .webhook_worker
            .as_ref()
            .or(config.reconciliation_index.as_ref())
            .zip(config.notion_scope.as_ref())
            .and_then(|(settings, scope)| {
                let journal = notion_knowledge_retrieval::planner::production_scope(scope).ok()?;
                let binding = notion_knowledge_retrieval::commit::CommitBinding::new(
                    "chunks",
                    &scope.workspace_id,
                    &journal,
                    &scope.generation.to_string(),
                    scope.generation,
                )
                .ok()?;
                Some((settings.index.clone(), binding))
            });
        Self {
            state: config.webhook_state_file.clone(),
            inbox,
            #[cfg(all(unix, feature = "webhook-worker"))]
            index,
        }
    }
    async fn inspect(&self) -> SyncStatus {
        let mut result = SyncStatus::default();
        let Some(path) = &self.state else {
            return result;
        };
        let path = path.clone();
        let inbox = self.inbox.clone();
        let inspected = tokio::task::spawn_blocking(move || {
            let state = SqliteSyncStateStore::open_read_only(path).ok()?;
            let schema = state.schema_version().ok()?;
            let counts = inbox.and_then(|(w, s, i)| state.scoped_webhook_counts(&w, &s, &i).ok());
            Some((schema, counts))
        })
        .await
        .ok()
        .flatten();
        if let Some((schema, counts)) = inspected {
            result.operational_schema_version = Some(schema);
            result.webhooks = counts;
        } else {
            return result;
        }
        #[cfg(all(unix, feature = "webhook-worker"))]
        if let Some((index, binding)) = &self.index {
            let index = index.clone();
            let state = self.state.clone().unwrap();
            let binding = binding.clone();
            if let Some(coordinator) = tokio::task::spawn_blocking(move || {
                notion_knowledge_retrieval::commit::IndexCommitCoordinator::open_read_only(
                    index, state, binding,
                )
                .ok()
            })
            .await
            .ok()
            .flatten()
            {
                let metadata = notion_knowledge_retrieval::qwen::metadata();
                let table = notion_knowledge_retrieval::chunks::GuardedChunkTable::bind(
                    &self.index.as_ref().unwrap().0,
                    "chunks",
                    metadata.clone(),
                    coordinator.clone(),
                );
                if let Ok(table) = table
                    && let Ok(version) = table.inventory_version().await
                    && let Ok(last_success) = coordinator.last_successful_reconciliation()
                {
                    result.index = Some(notion_knowledge_core::sync_status::IndexStatus {
                        version,
                        chunk_schema_version: 1,
                        canonical_schema_version: 1,
                        model: notion_knowledge_core::sync_status::ModelIdentity::Qwen3Embedding06b,
                        model_version: metadata.model_version().into(),
                        provider: metadata.provider_id().into(),
                    });
                    result.last_successful_reconciliation_unix_seconds = last_success;
                }
            }
        }
        result
    }
}
impl SyncStatusSource for LocalStatus {
    fn status(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncStatus> + Send + '_>> {
        Box::pin(self.inspect())
    }
}
pub fn attach(
    handler: notion_knowledge_mcp::KnowledgeServer,
    config: &Config,
) -> notion_knowledge_mcp::KnowledgeServer {
    handler.and_sync_status(Arc::new(LocalStatus::from_config(config)))
}
