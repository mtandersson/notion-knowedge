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
    seed_with_plan(dir, scope, page_id, None).await
}

async fn seed_with_plan(
    dir: &Path,
    scope: &LifecycleScope,
    page_id: &str,
    plan: Option<notion_knowledge_retrieval::chunks::lexical_binding::LexicalCachePlan>,
) -> Seeded {
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
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(now);
    let lease = coordinator.state().acquire_lease(now(), 120).unwrap();
    let table = if let Some(plan) = plan {
        GuardedChunkTable::create_fresh_lexical(
            &index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
            plan.identity(),
            lease.clone(),
            clock.clone(),
        )
        .await
        .unwrap()
    } else {
        let table = GuardedChunkTable::bind(
            &index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
        )
        .unwrap();
        table
            .create_empty(lease.clone(), clock.clone())
            .await
            .unwrap();
        table
    };
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
    // Refresh validates prior canonical fingerprints, unlike read-only search.
    let chunks = notion_knowledge_core::fingerprint::identify_chunks(
        vec![notion_knowledge_core::chunking::ChunkDraft {
            metadata: chunk.metadata,
            source_bytes: 0..chunk.text.len(),
            text: chunk.text,
        }],
        &[],
    )
    .unwrap();
    let prepared = table
        .prepare_page(provider.as_ref(), operation, &chunks)
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

#[tokio::test]
async fn configured_oauth_lexical_reads_require_native_exact_id_readiness() {
    use notion_knowledge_core::backend::PageId;
    use notion_knowledge_retrieval::{
        chunks::lexical_binding::LexicalCachePlan, lexical_dictionary::test_fixtures::dictionary,
    };
    let dir = tempfile::tempdir().unwrap();
    let scope = LifecycleScope {
        workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        roots: vec![PageId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into())],
        generation: 1,
        exclusions: Default::default(),
    };
    let dictionary = Arc::new(dictionary(&[("saffron", "saffran")]));
    let plan = LexicalCachePlan::verified(dictionary.clone()).unwrap();
    let seeded = seed_with_plan(
        dir.path(),
        &scope,
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
        Some(plan),
    )
    .await;
    let lexical = crate::lexical_expansion::Settings::parse(&mut |key| match key {
        "NK_LEXICAL_EXPANSION" => Some("true".into()),
        "NK_LEXICAL_ASSET_BUNDLE" => Some("/tmp/unused-oauth-dictionary".into()),
        "NK_LEXICAL_ASSET_SHA256" => Some(dictionary.manifest_digest().into()),
        _ => None,
    })
    .unwrap()
    .unwrap()
    .with_verified_dictionary(dictionary)
    .unwrap();
    let mut config = crate::config::Config::from_lookup(|_| None).unwrap();
    config.notion_scope = Some(scope);
    config.webhook_state_file = Some(seeded.state.clone());
    config.retrieval = Some(crate::webhook_worker::WorkerSettings {
        lexical: Some(lexical),
        alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![]).unwrap(),
        index: seeded.index.clone(),
        assets: "/tmp/unused-oauth-model".into(),
        poll_ms: 100,
        timeout_seconds: 30,
    });
    let before = fs::read(&seeded.state).unwrap();
    let (_, _, table) = crate::retrieval_runtime::compose_oauth_index(
        notion_knowledge_mcp::KnowledgeServer::default(),
        &config,
        seeded.provider.clone(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(fs::read(&seeded.state).unwrap(), before);
    table.probe_lexical_cache().await.unwrap();
    // Keep catalog entries/identity intact while removing only this fixture's
    // native index payload. Layout validation alone cannot prove usable search.
    fs::remove_dir_all(seeded.index.join("chunks.lance/_indices")).unwrap();
    assert!(table.search_snapshot().await.is_ok());
    assert!(table.probe_lexical_cache().await.is_err());
    for writable in [false, true] {
        assert!(
            crate::retrieval_runtime::compose_oauth_index(
                notion_knowledge_mcp::KnowledgeServer::default(),
                &config,
                seeded.provider.clone(),
                writable,
            )
            .await
            .is_err()
        );
        assert_eq!(fs::read(&seeded.state).unwrap(), before);
    }
}
