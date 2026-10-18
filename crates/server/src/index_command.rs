//! Trusted local maintenance of derived search indexes; never an MCP operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Ensure,
    Rebuild,
}
impl Command {
    pub fn parse(args: &[String]) -> Result<Self, &'static str> {
        match args
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["--index", "ensure"] => Ok(Self::Ensure),
            ["--index", "rebuild"] => Ok(Self::Rebuild),
            _ => Err("expected --index ensure | rebuild"),
        }
    }
}

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod runtime {
    use super::Command;
    use crate::{
        config::Config,
        reconciliation_command::Output,
        webhook_worker::{WorkerSettings, runtime::now_ms},
    };
    use notion_knowledge_core::{
        embedding::EmbeddingMetadata,
        lifecycle::LifecycleScope,
        reconciliation::{JournalError, ReconciliationJournal},
    };
    use notion_knowledge_retrieval::{
        chunks::{GuardedChunkTable, VectorDistance, VectorIndexConfig},
        commit::{CommitBinding, IndexCommitCoordinator},
        planner::production_scope,
    };
    use std::{path::Path, sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;

    fn output(status: &'static str, exit: u8) -> Output {
        Output {
            json: serde_json::json!({"operation":"index", "status":status}),
            exit,
        }
    }

    pub async fn configured(command: Command, stop: CancellationToken) -> Output {
        let config = match Config::from_env() {
            Ok(c) if c.validate_grant_store().is_ok() => c,
            _ => return output("configuration", 2),
        };
        let Some(scope) = config.notion_scope else {
            return output("configuration", 2);
        };
        let settings = match WorkerSettings::index_settings(&mut |name| std::env::var_os(name)) {
            Ok(s) => s,
            Err(_) => return output("configuration", 2),
        };
        let Some(state) = std::env::var_os("NK_WEBHOOK_STATE_FILE")
            .map(std::path::PathBuf::from)
            .filter(|p| p.is_absolute() && !p.to_string_lossy().chars().any(char::is_control))
        else {
            return output("configuration", 2);
        };
        // Validate exact external assets in the shipping runtime, even though
        // ANN training consumes already committed vectors and makes no API call.
        use notion_knowledge_core::embedding::EmbeddingProvider;
        let provider = match notion_knowledge_retrieval::qwen::QwenProvider::load(
            notion_knowledge_retrieval::qwen::QwenConfig::cpu(&settings.assets),
        )
        .await
        {
            Ok(p) => p,
            Err(_) => return output("model_configuration", 2),
        };
        let embedding = provider.metadata().clone();
        drop(provider);
        execute(command, &settings, &state, scope, embedding, stop).await
    }

    /// Shared actual command service; deterministic fixtures supply vector identity.
    pub async fn execute(
        command: Command,
        settings: &WorkerSettings,
        state: &Path,
        scope: LifecycleScope,
        embedding: EmbeddingMetadata,
        stop: CancellationToken,
    ) -> Output {
        if stop.is_cancelled() {
            return output("cancelled", 5);
        }
        let journal = match production_scope(&scope) {
            Ok(s) => s,
            Err(_) => return output("configuration", 2),
        };
        let binding = match CommitBinding::new(
            "chunks",
            &scope.workspace_id,
            &journal,
            &scope.generation.to_string(),
            scope.generation,
        ) {
            Ok(b) => b,
            Err(_) => return output("configuration", 2),
        };
        // Inspect existing authority and vectors before any writable open.
        let inspected =
            match IndexCommitCoordinator::open_read_only(&settings.index, state, binding.clone()) {
                Ok(c) => c,
                Err(_) => return output("index_state_configuration", 2),
            };
        let table = match GuardedChunkTable::bind(
            &settings.index,
            "chunks",
            embedding.clone(),
            inspected.clone(),
        ) {
            Ok(t) => t,
            Err(_) => return output("index_state_configuration", 2),
        };
        if table.inventory_version().await.is_err() {
            return output("index_state_configuration", 2);
        }
        drop(table);
        drop(inspected);
        let coordinator =
            match IndexCommitCoordinator::open_existing(&settings.index, state, binding) {
                Ok(c) => c,
                Err(_) => return output("index_state_configuration", 2),
            };
        let table = match GuardedChunkTable::bind(
            &settings.index,
            "chunks",
            embedding,
            coordinator.clone(),
        ) {
            Ok(t) => t,
            Err(_) => return output("index_state_configuration", 2),
        };
        let lease = match coordinator
            .state()
            .acquire_lease(now_ms() / 1000, settings.timeout_seconds as i64 + 30)
        {
            Ok(l) => l,
            Err(JournalError::Busy) => return output("busy", 4),
            Err(_) => return output("state_failure", 3),
        };
        let attempt = stop.child_token();
        let cancelled = attempt.clone();
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
            if cancelled.is_cancelled() {
                i64::MAX
            } else {
                now_ms() / 1000
            }
        });
        let config = VectorIndexConfig {
            distance: VectorDistance::Cosine,
            ..Default::default()
        };
        let work = async {
            match command {
                Command::Ensure => {
                    table
                        .ensure_vector_index(config, lease.clone(), clock)
                        .await
                }
                Command::Rebuild => {
                    table
                        .rebuild_vector_index(config, lease.clone(), clock)
                        .await
                }
            }
        };
        let result = tokio::select! { biased;
            _ = stop.cancelled() => { attempt.cancel(); output("cancelled", 5) },
            _ = tokio::time::sleep(Duration::from_secs(settings.timeout_seconds)) => { attempt.cancel(); output("timeout", 3) },
            result = work => if result.is_ok() { output("complete", 0) } else { output("index_failure", 3) },
        };
        // A dropped observer cannot release a lease while native IO still owns it.
        let drained = coordinator.drain_owned().await;
        let released = coordinator.state().release_lease(&lease, now_ms() / 1000);
        if result.exit == 0 && (drained.is_err() || released.is_err()) {
            output("state_failure", 3)
        } else {
            result
        }
    }
}
