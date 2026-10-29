use super::*;
use crate::chunks::{
    FtsIndexConfig,
    lexical_binding::{self, LexicalIndexBinding, LexicalPlanIdentity},
};

fn dictionary() -> crate::lexical_dictionary::Dictionary {
    crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")])
}

#[tokio::test]
async fn configured_search_reserves_before_model_work_and_releases_on_cancel_or_deadline() {
    use crate::{chunks::bounded::CandidateBudget, lexical_runtime::LexicalSearchRuntime};
    use notion_knowledge_core::search::{LexicalQuery, SemanticQuery, SemanticSearch};
    struct WaitingProvider {
        metadata: EmbeddingMetadata,
        started: Arc<tokio::sync::Notify>,
    }
    impl notion_knowledge_core::embedding::EmbeddingProvider for WaitingProvider {
        fn metadata(&self) -> &EmbeddingMetadata {
            &self.metadata
        }
        fn embed_batch<'a>(
            &'a self,
            _: &'a [String],
        ) -> notion_knowledge_core::embedding::EmbeddingFuture<'a> {
            self.started.notify_one();
            Box::pin(std::future::pending())
        }
    }
    let f = Fixture::new();
    let plan = lexical_binding::LexicalCachePlan::verified(Arc::new(dictionary())).unwrap();
    let table = GuardedChunkTable::create_fresh_lexical(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
        plan.identity(),
        f.lease.clone(),
        clock(),
    )
    .await
    .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let provider = Arc::new(WaitingProvider {
        metadata: f.embedding.clone(),
        started: started.clone(),
    });
    let pool = CandidateBudget::new(1).unwrap();
    let search = Arc::new(
        LexicalSearchRuntime::new(
            table.clone(),
            provider.clone(),
            plan.clone(),
            pool.clone(),
            std::time::Duration::from_secs(30),
        )
        .unwrap(),
    );
    let query = || SemanticQuery {
        query: "cinnamon".into(),
        limit: 1,
        page_ids: None,
        root_page_ids: None,
        metadata: Default::default(),
    };
    let running = search.clone();
    let task = tokio::spawn(async move { SemanticSearch::search(running.as_ref(), query()).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    assert!(
        pool.try_reserve().is_err(),
        "admission must precede model execution"
    );
    assert!(
        notion_knowledge_core::search::LexicalSearch::search(
            search.as_ref(),
            LexicalQuery {
                query: "cinnamon".into(),
                limit: 1,
                page_ids: None,
                root_page_ids: None,
                metadata: Default::default()
            }
        )
        .await
        .is_err()
    );
    async fn assert_fence_released(index: &std::path::Path) {
        let fence = std::fs::File::open(index).unwrap();
        // Parallel libtests spawn children. A fork-before-exec can briefly
        // inherit the open file description even after our reader drops it.
        // A retained search guard still fails this bounded acquisition.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match fence.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    Err(error) => panic!("exclusive search fence unavailable: {error}"),
                }
            }
        })
        .await
        .expect("cancelled search must promptly release its read fence");
        fence.unlock().unwrap();
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(pool.try_reserve().is_ok());
    assert_fence_released(&f.index).await;
    let timed = LexicalSearchRuntime::new(
        table,
        provider,
        plan,
        pool.clone(),
        std::time::Duration::from_nanos(1),
    )
    .unwrap();
    assert!(SemanticSearch::search(&timed, query()).await.is_err());
    assert!(pool.try_reserve().is_ok());
    assert_fence_released(&f.index).await;
}
fn enabled(
    f: &Fixture,
    plan: &LexicalPlanIdentity,
) -> Result<GuardedChunkTable, crate::chunks::ChunkTableError> {
    GuardedChunkTable::bind_with_lexical_plan(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
        plan,
    )
}
// Explicit test-only fixture installation. Production has no stamping API; the
// compatibility checks remain independent of installed expansion acceptance.
async fn install(f: &Fixture, plan: LexicalPlanIdentity) -> Vec<u8> {
    let binding = LexicalIndexBinding::for_cache(
        &f.index,
        f.embedding.clone(),
        f.coordinator.binding(),
        plan,
    )
    .unwrap();
    let bytes = binding.canonical_bytes().unwrap();
    let path = f.index.join(lexical_binding::FILE);
    let copy = bytes.clone();
    let index = f.index.clone();
    let embedding = f.embedding.clone();
    f.coordinator
        .maintain(f.lease.clone(), clock(), move |_| async move {
            let table = LanceChunkTable::open(&index, "chunks", embedding)
                .await
                .map_err(|_| FailureClass::Index)?;
            table
                .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
                .await
                .map_err(|_| FailureClass::Index)?;
            fs::write(path, copy).unwrap();
            Ok(())
        })
        .await
        .unwrap();
    bytes
}

#[tokio::test]
async fn fresh_lexical_cache_pins_its_verified_plan_and_never_installs_over_existing_rows() {
    let f = Fixture::new();
    let plan = lexical_binding::LexicalCachePlan::verified(Arc::new(dictionary())).unwrap();
    let table = GuardedChunkTable::create_fresh_lexical(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
        plan.identity(),
        f.lease.clone(),
        clock(),
    )
    .await
    .unwrap();
    assert_eq!(
        table
            .search_snapshot()
            .await
            .unwrap()
            .count_rows()
            .await
            .unwrap(),
        0
    );
    let bytes = fs::read(f.index.join(lexical_binding::FILE)).unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(wire["version"], 2);
    assert!(wire.get("source_snapshot_sha256").is_none());
    assert!(enabled(&f, plan.identity()).is_ok());
    assert!(
        GuardedChunkTable::bind(
            &f.index,
            "chunks",
            f.embedding.clone(),
            f.coordinator.clone()
        )
        .is_err()
    );

    let old = Fixture::new();
    old.create().await;
    let raw = old.guarded();
    let provider = CountingProvider::new(old.embedding.clone());
    let prepared = raw
        .prepare_page(
            &provider,
            operation("existing", "source", false),
            &[chunk("one", "cinnamon", "Spice")],
        )
        .await
        .unwrap();
    raw.commit_page(prepared, old.lease.clone(), clock(), || async { Ok(()) })
        .await
        .unwrap();
    let version = old.readable().await.table.version().await.unwrap();
    assert!(
        GuardedChunkTable::create_fresh_lexical(
            &old.index,
            "chunks",
            old.embedding.clone(),
            old.coordinator.clone(),
            plan.identity(),
            old.lease.clone(),
            clock(),
        )
        .await
        .is_err()
    );
    assert_eq!(old.readable().await.table.version().await.unwrap(), version);
    assert_eq!(raw.page_chunks("page").await.unwrap().len(), 1);
    assert!(!old.index.join(lexical_binding::FILE).exists());

    let mut previous = wire;
    previous["version"] = serde_json::json!(1);
    previous["source_snapshot_sha256"] = serde_json::json!("a".repeat(64));
    fs::write(
        f.index.join(lexical_binding::FILE),
        serde_json::to_vec(&previous).unwrap(),
    )
    .unwrap();
    assert!(enabled(&f, plan.identity()).is_err());
    assert!(table.page_chunks("page").await.is_err());
}

#[tokio::test]
async fn lexical_identity_requires_explicit_binding_without_runtime_stamping() {
    let f = Fixture::new();
    f.create().await;
    let dictionary = dictionary();
    let plan = LexicalPlanIdentity::verified(&dictionary).unwrap();
    let raw = f.guarded();
    assert!(enabled(&f, &plan).is_err());
    assert!(!f.index.join(lexical_binding::FILE).exists());
    let provider = CountingProvider::new(f.embedding.clone());
    let before = raw
        .prepare_page(
            &provider,
            operation("before", "before", false),
            &[chunk("one", "cinnamon", "Spice")],
        )
        .await
        .unwrap();
    let bytes = install(&f, plan.clone()).await;
    assert!(
        GuardedChunkTable::bind(
            &f.index,
            "chunks",
            f.embedding.clone(),
            f.coordinator.clone()
        )
        .is_err()
    );
    assert!(raw.page_chunks("page").await.is_err());
    assert!(
        raw.prepare_page(&provider, operation("stale", "stale", false), &[])
            .await
            .is_err()
    );
    let current = enabled(&f, &plan).unwrap();
    assert!(current.page_chunks("page").await.unwrap().is_empty());
    // New coordinator/process-equivalent open retains the trusted physical and
    // operational authority; binding is read/validated, never recreated.
    let reopened = IndexCommitCoordinator::open(
        &f.index,
        f.root.join("state.sqlite"),
        f.coordinator.binding().clone(),
    )
    .unwrap();
    let restarted = GuardedChunkTable::bind_with_lexical_plan(
        &f.index,
        "chunks",
        f.embedding.clone(),
        reopened,
        &plan,
    )
    .unwrap();
    assert!(restarted.page_chunks("page").await.unwrap().is_empty());
    assert_eq!(
        fs::read(f.index.join(lexical_binding::FILE)).unwrap(),
        bytes
    );
    assert!(
        raw.commit_page(before, f.lease.clone(), clock(), || async {
            panic!("stale preparation must fail before source effects")
        })
        .await
        .is_err()
    );
}

#[tokio::test]
async fn lexical_identity_rejects_changed_plan_authority_storage_and_physical_metadata() {
    let f = Fixture::new();
    f.create().await;
    let plan = LexicalPlanIdentity::verified(&dictionary()).unwrap();
    let bytes = install(&f, plan.clone()).await;
    let current = enabled(&f, &plan).unwrap();
    let path = f.index.join(lexical_binding::FILE);
    let other = crate::lexical_dictionary::tests::dictionary(&[("pepper", "peppar")]);
    assert!(enabled(&f, &LexicalPlanIdentity::verified(&other).unwrap()).is_err());
    // Canonically serialize each mismatch so rejection tests semantic identity,
    // not merely JSON whitespace/order validation.
    for field in [
        "version",
        "workspace",
        "roots",
        "scope",
        "generation",
        "epoch",
        "device",
        "inode",
        "table",
        "embedding",
        "storage",
        "plan",
    ] {
        let mut wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        match field {
            "version" | "epoch" | "device" | "inode" => {
                wire[field] = serde_json::json!(wire[field].as_u64().unwrap() + 1)
            }
            "roots" => wire[field] = serde_json::json!(["other-root"]),
            "embedding" => wire[field]["model_version"] = serde_json::json!("other-model"),
            "storage" => wire[field]["profile"]["with_positions"] = serde_json::json!(false),
            "plan" => wire[field]["asset_sha256"] = serde_json::json!("b".repeat(64)),
            _ => wire[field] = serde_json::json!("mismatch"),
        }
        let changed: LexicalIndexBinding = serde_json::from_value(wire).unwrap();
        fs::write(&path, changed.canonical_bytes().unwrap()).unwrap();
        assert!(enabled(&f, &plan).is_err(), "{field}");
        assert!(current.page_chunks("page").await.is_err(), "{field}");
    }
    fs::write(&path, &bytes).unwrap();
    let provider = CountingProvider::new(f.embedding.clone());
    let prepared = current
        .prepare_page(
            &provider,
            operation("prepared", "prepared", false),
            &[chunk("one", "cinnamon", "Spice")],
        )
        .await
        .unwrap();
    fs::remove_file(&path).unwrap();
    assert!(enabled(&f, &plan).is_err());
    assert!(current.page_chunks("page").await.is_err());
    assert!(
        current
            .commit_page(prepared, f.lease.clone(), clock(), || async {
                panic!("removed binding must fail before effects")
            })
            .await
            .is_err()
    );
    assert!(
        !path.exists(),
        "reads/commits must not repair missing binding"
    );
}

#[tokio::test]
async fn lexical_identity_rejects_corruption_links_and_unbounded_metadata() {
    let f = Fixture::new();
    f.create().await;
    let plan = LexicalPlanIdentity::verified(&dictionary()).unwrap();
    let bytes = install(&f, plan.clone()).await;
    let current = enabled(&f, &plan).unwrap();
    let path = f.index.join(lexical_binding::FILE);
    for invalid in [
        b"{}".to_vec(),
        [bytes.clone(), b"\n".to_vec()].concat(),
        vec![b' '; 32769],
    ] {
        fs::write(&path, invalid).unwrap();
        assert!(enabled(&f, &plan).is_err());
        assert!(current.page_chunks("page").await.is_err());
    }
    fs::remove_file(&path).unwrap();
    let target = f.root.join("binding");
    fs::write(&target, &bytes).unwrap();
    std::os::unix::fs::symlink(&target, &path).unwrap();
    assert!(enabled(&f, &plan).is_err());
    fs::remove_file(&path).unwrap();
    fs::hard_link(&target, &path).unwrap();
    assert!(enabled(&f, &plan).is_err());
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[tokio::test]
async fn lexical_identity_enabled_commit_and_bilingual_search_preserve_vectors_and_citations() {
    use crate::chunks::bounded::{CandidateBudget, LexicalMatch};
    use notion_knowledge_core::search::LexicalQuery;
    let f = Fixture::new();
    f.create().await;
    let original = chunk("one", "cinnamon", "Spice");
    let raw = f.guarded();
    let provider = CountingProvider::new(f.embedding.clone());
    let prepared = raw
        .prepare_page(
            &provider,
            operation("seed", "seed", false),
            std::slice::from_ref(&original),
        )
        .await
        .unwrap();
    raw.commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
        .await
        .unwrap();
    raw.ensure_vector_index(
        VectorIndexConfig {
            num_partitions: Some(1),
            sample_rate: 8,
            max_iterations: 20,
            ..Default::default()
        },
        f.lease.clone(),
        clock(),
    )
    .await
    .unwrap();
    let before = f
        .readable()
        .await
        .rows_matching("true".into())
        .await
        .unwrap();
    let dictionary = dictionary();
    let plan = LexicalPlanIdentity::verified(&dictionary).unwrap();
    install(&f, plan.clone()).await;
    let table = enabled(&f, &plan).unwrap();
    let unchanged = CountingProvider::new(f.embedding.clone());
    let prepared = table
        .prepare_page(
            &unchanged,
            operation("enabled", "enabled", false),
            std::slice::from_ref(&original),
        )
        .await
        .unwrap();
    assert_eq!(prepared.metrics().skipped, 1);
    assert_eq!(
        unchanged.total(),
        0,
        "query expansion must not re-embed compatible chunks"
    );
    table
        .commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(
        table.page_chunks("page").await.unwrap(),
        vec![original.clone()]
    );
    let snapshot = table.search_snapshot().await.unwrap();
    assert_eq!(snapshot.embedding_metadata(), &f.embedding);
    let after = snapshot.rows_matching("true".into()).await.unwrap();
    assert_eq!(before.len(), after.len());
    assert_eq!(before[0].vector, after[0].vector);
    let query = || LexicalQuery {
        query: "kanel".into(),
        limit: 5,
        page_ids: None,
        root_page_ids: Some(vec!["root".into()]),
        metadata: Default::default(),
    };
    let pool = CandidateBudget::new(1).unwrap();
    let results = table
        .expanded_lexical_search(
            query(),
            &dictionary,
            &pool,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            std::future::pending(),
        )
        .await
        .unwrap();
    assert_eq!(results.candidates.len(), 1);
    let result = &results.candidates[0];
    assert_eq!(result.hit.text, original.text);
    assert_eq!(result.hit.source.page_id, original.metadata.page_id);
    assert_eq!(result.hit.source.chunk_id, original.chunk_id);
    assert_eq!(result.hit.source.url, original.metadata.url);
    assert_eq!(result.hit.source.title, original.metadata.title);
    assert_eq!(
        result.hit.source.heading_path,
        original.metadata.heading_path
    );
    assert_eq!(
        result.hit.source.last_edited_time,
        original.metadata.last_edited_time
    );
    assert!(result.matches.iter().any(|m| matches!(m, LexicalMatch::LexicalDictionaryV1 { manifest_digest, .. } if manifest_digest == dictionary.manifest_digest())));
    let changed = crate::lexical_dictionary::tests::dictionary(&[("pepper", "peppar")]);
    assert!(
        table
            .expanded_lexical_search(
                query(),
                &changed,
                &pool,
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                std::future::pending()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lexical_identity_retained_handle_rejects_physical_directory_replacement() {
    let f = Fixture::new();
    f.create().await;
    let plan = LexicalPlanIdentity::verified(&dictionary()).unwrap();
    let bytes = install(&f, plan.clone()).await;
    let table = enabled(&f, &plan).unwrap();
    fs::rename(&f.index, f.root.join("old-index")).unwrap();
    fs::create_dir(&f.index).unwrap();
    fs::write(f.index.join(lexical_binding::FILE), bytes).unwrap();
    assert!(table.page_chunks("page").await.is_err());
    assert!(enabled(&f, &plan).is_err());
}
