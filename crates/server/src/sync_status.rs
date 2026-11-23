//! Both transports compose the same lazy read-only local inspection.
use crate::{config::Config, webhook::WebhookConfig};
use notion_knowledge_core::sync_status::{SyncStatus, SyncStatusSource};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
use std::{path::PathBuf, sync::Arc};
#[derive(Clone)]
pub struct LocalStatus {
    state: Option<PathBuf>,
    work: Arc<tokio::sync::Semaphore>,
    inbox: Option<(String, String, String)>,
    #[cfg(all(unix, feature = "webhook-worker"))]
    index: Option<(PathBuf, notion_knowledge_retrieval::commit::CommitBinding)>,
    #[cfg(all(unix, feature = "webhook-worker"))]
    lexical: Option<crate::lexical_expansion::Settings>,
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
        let index_settings = config
            .webhook_worker
            .as_ref()
            .or(config.reconciliation_index.as_ref())
            .or(config.retrieval.as_ref());
        #[cfg(all(unix, feature = "webhook-worker"))]
        let lexical = index_settings.and_then(|settings| settings.lexical.clone());
        #[cfg(all(unix, feature = "webhook-worker"))]
        let index =
            index_settings
                .zip(config.notion_scope.as_ref())
                .and_then(|(settings, scope)| {
                    let journal =
                        notion_knowledge_retrieval::planner::production_scope(scope).ok()?;
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
            work: Arc::new(tokio::sync::Semaphore::new(config.mcp_tool_capacity)),
            inbox,
            #[cfg(all(unix, feature = "webhook-worker"))]
            index,
            #[cfg(all(unix, feature = "webhook-worker"))]
            lexical,
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
            Some((schema, counts, inventory(&state)))
        })
        .await
        .ok()
        .flatten();
        if let Some((schema, counts, inventory)) = inspected {
            result.operational_schema_version = Some(schema);
            result.webhooks = counts;
            result.inventory = inventory;
        } else {
            return result;
        }
        #[cfg(all(unix, feature = "webhook-worker"))]
        if let Some((index, binding)) = &self.index {
            use notion_knowledge_core::sync_status::{
                IndexStatus, LexicalProjectionStatus, ModelIdentity, SemanticProjectionStatus,
            };
            use notion_knowledge_retrieval::{
                commit::IndexCommitCoordinator, page_documents::PageDocuments, qwen,
            };
            let index_path = index.clone();
            let state = self.state.clone().unwrap();
            let binding = binding.clone();
            if let Some(coordinator) = tokio::task::spawn_blocking(move || {
                IndexCommitCoordinator::open_read_only(index_path, state, binding).ok()
            })
            .await
            .ok()
            .flatten()
            {
                result.last_successful_reconciliation_unix_seconds =
                    coordinator.last_successful_reconciliation().ok().flatten();
                result.source_documents =
                    PageDocuments::new(coordinator.clone()).count().await.ok();
                let metadata = qwen::metadata();
                let plan = match self
                    .lexical
                    .as_ref()
                    .map(crate::lexical_expansion::Settings::load_plan)
                    .transpose()
                {
                    Ok(plan) => plan,
                    Err(_) => return result,
                };
                let table = crate::index_runtime::bind_cache(
                    index,
                    metadata.clone(),
                    coordinator,
                    plan.as_ref(),
                );
                if let Ok(table) = table {
                    if let Ok(version) = table.inventory_version().await {
                        result.index = Some(IndexStatus {
                            version,
                            chunk_schema_version: 1,
                            canonical_schema_version: 1,
                            model: ModelIdentity::Qwen3Embedding06b,
                            model_version: metadata.model_version().into(),
                            provider: metadata.provider_id().into(),
                        });
                    }
                    if let Ok((version, chunks)) = table.text_status().await {
                        result.lexical_projection =
                            Some(LexicalProjectionStatus { version, chunks });
                    }
                    if let Ok((vectors, backlog)) = table.semantic_status().await {
                        result.semantic_projection = Some(SemanticProjectionStatus {
                            vectors,
                            queued_pages: backlog.queued,
                            model_unavailable_pages: backlog.model_unavailable,
                            input_limit_pages: backlog.input_limit,
                            rejected_pages: backlog.rejected,
                        });
                    }
                }
            }
        }
        result
    }
}
/// `None` until a sync has inventoried at least one page.
fn inventory(
    state: &SqliteSyncStateStore,
) -> Option<notion_knowledge_core::sync_status::InventoryStatus> {
    use notion_knowledge_core::{
        inventory::InventoryOutcome,
        sync_status::{Drift, InventoryStatus, OutOfSyncPage},
    };
    let counts = state.inventory_counts().ok()?;
    let last = state.last_sync().ok()?;
    if counts.total == 0 && last.is_none() {
        return None;
    }
    let drift = last.as_ref().map(|l| l.drift).unwrap_or_default();
    let out_of_sync = state
        .out_of_sync_pages(100)
        .ok()?
        .into_iter()
        .map(|(page_id, outcome)| OutOfSyncPage {
            page_id,
            outcome: match outcome {
                InventoryOutcome::Skipped(reason) => format!("skipped:{}", reason.as_str()),
                InventoryOutcome::Failed(class) => format!("failed:{}", class.as_str()),
                InventoryOutcome::Indexed { .. } => "indexed".into(),
            },
        })
        .collect();
    Some(InventoryStatus {
        total: counts.total,
        indexed: counts.indexed,
        skipped: counts.skipped,
        failed: counts.failed,
        pending: counts.pending,
        walk_complete: last.as_ref().map(|l| l.complete),
        held: last.as_ref().map_or(0, |l| l.held),
        drift: Drift {
            added: drift.added,
            changed: drift.changed,
            moved: drift.moved,
            removed: drift.removed,
            trashed: drift.trashed,
            unplaced: drift.unplaced,
        },
        out_of_sync,
    })
}
impl SyncStatusSource for LocalStatus {
    fn status(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncStatus> + Send + '_>> {
        Box::pin(async move {
            // Admission happens before spawning. The task owns the permit and
            // runs to completion even if the MCP waiter disconnects. In turn,
            // inspect awaits each blocking operation before releasing capacity.
            let Ok(permit) = self.work.clone().try_acquire_owned() else {
                return SyncStatus::default();
            };
            let source = self.clone();
            finish_status_work(permit, async move { source.inspect().await }).await
        })
    }
}
pub fn attach(
    handler: notion_knowledge_mcp::KnowledgeServer,
    config: &Config,
) -> notion_knowledge_mcp::KnowledgeServer {
    handler.and_sync_status(Arc::new(LocalStatus::from_config(config)))
}

/// Detach only bounded, read-only inspection; never detach/replay a mutation.
async fn finish_status_work(
    permit: tokio::sync::OwnedSemaphorePermit,
    inspect: impl std::future::Future<Output = SyncStatus> + Send + 'static,
) -> SyncStatus {
    let correlation = notion_knowledge_core::logging::current_id();
    tokio::spawn(notion_knowledge_core::logging::scope(
        correlation,
        async move {
            let _permit = permit;
            inspect.await
        },
    ))
    .await
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_status_waiter_cannot_admit_work_until_blocking_inspection_finishes() {
        let work = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = work.clone().try_acquire_owned().unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(finish_status_work(permit, async move {
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            })
            .await
            .unwrap();
            finished.send(()).unwrap();
            SyncStatus::default()
        }));
        ready.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        // Real blocking work still runs. Repeated cancelled callers cannot
        // reserve capacity or enqueue another blocking task.
        for _ in 0..32 {
            assert!(work.clone().try_acquire_owned().is_err());
        }
        release.send(()).unwrap();
        done.await.unwrap();
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(2), work.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(recovered);
        assert_eq!(work.available_permits(), 1);
    }
}
