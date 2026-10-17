//! Real committed local retrieval state for the OAuth HTTP content tests.
use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    lifecycle::LifecycleScope,
    reconciliation::ReconciliationJournal,
    sync_state::PageSyncState,
};
use notion_knowledge_retrieval::{
    chunks::{GuardedChunkTable, VectorIndexConfig},
    commit::{
        CommitBinding, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation,
        RelationProjection,
    },
    planner::production_scope,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Seeded {
    pub index: PathBuf,
    pub state: PathBuf,
    pub provider: Arc<dyn EmbeddingProvider>,
}
struct LocalProvider(EmbeddingMetadata);
impl EmbeddingProvider for LocalProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move { Ok(inputs.iter().map(|_| vec![1.0, 0.0, 0.0]).collect()) })
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub async fn seed(dir: &Path, scope: &LifecycleScope, page_id: &str) -> Seeded {
    let index = dir.join("content-index");
    let state = dir.join("content-state.sqlite");
    fs::create_dir(&index).unwrap();
    fs::set_permissions(&index, fs::Permissions::from_mode(0o700)).unwrap();
    let journal = production_scope(scope).unwrap();
    let binding = CommitBinding::new(
        "chunks",
        &scope.workspace_id,
        &journal,
        &scope.generation.to_string(),
        scope.generation,
    )
    .unwrap();
    let coordinator = IndexCommitCoordinator::initialize(&index, &state, binding).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
    let provider: Arc<dyn EmbeddingProvider> = Arc::new(LocalProvider(
        EmbeddingMetadata::new("oauth-fixture".into(), "local".into(), "1".into(), 3).unwrap(),
    ));
    let table = GuardedChunkTable::bind(
        &index,
        "chunks",
        provider.metadata().clone(),
        coordinator.clone(),
    )
    .unwrap();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(now);
    let lease = coordinator.state().acquire_lease(now(), 120).unwrap();
    table
        .create_empty(lease.clone(), clock.clone())
        .await
        .unwrap();
    let text = "Saffron private evidence";
    let revision = "2026-10-17T00:00:00Z";
    let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
    let checkpoint =
        PageSyncState::present(page_id.into(), hash.clone(), Some(revision.into())).unwrap();
    let operation = PageOperation::new(
        "oauth-content-seed",
        revision,
        PageAction::Refresh,
        checkpoint,
    )
    .unwrap()
    .with_relation_projection(
        RelationProjection::new(coordinator.binding(), page_id, false, vec![])
            .unwrap()
            .with_links(vec![])
            .unwrap(),
    )
    .unwrap();
    let chunk = IndexedChunk {
        schema_version: SchemaVersion::V1,
        chunk_id: format!(
            "{:x}",
            Sha256::digest(format!("{page_id}:{hash}").as_bytes())
        ),
        metadata: IndexedMetadata {
            page_id: page_id.into(),
            block_id: None,
            url: format!("https://www.notion.so/{page_id}"),
            title: "Saffron private evidence".into(),
            heading_path: vec![],
            last_edited_time: revision.into(),
            source: SourceMetadata {
                workspace_id: scope.workspace_id.clone(),
                root_page_id: scope
                    .roots
                    .first()
                    .expect("fixture scope needs a physical root")
                    .0
                    .clone(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: text.into(),
        content_hash: hash,
        links: vec![],
    };
    let prepared = table
        .prepare_page(provider.as_ref(), operation, &[chunk])
        .await
        .unwrap();
    assert_eq!(
        table
            .commit_page(prepared, lease.clone(), clock.clone(), || async { Ok(()) })
            .await,
        Ok(CommitOutcome::Applied)
    );
    table
        .ensure_vector_index(
            VectorIndexConfig {
                num_partitions: Some(1),
                sample_rate: 8,
                max_iterations: 20,
                ..Default::default()
            },
            lease.clone(),
            clock,
        )
        .await
        .unwrap();
    coordinator.state().release_lease(&lease, now()).unwrap();
    // Startup composition must accept this exact committed read-only binding.
    table.search_snapshot().await.unwrap();
    Seeded {
        index,
        state,
        provider,
    }
}
