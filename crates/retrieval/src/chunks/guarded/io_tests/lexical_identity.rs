use super::*;
use crate::chunks::{
    FtsIndexConfig,
    lexical_binding::{self, LexicalIndexBinding, LexicalPlanIdentity},
};

fn dictionary() -> crate::lexical_dictionary::Dictionary {
    crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")])
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
// later offline candidate builder must verify all unchanged rows and vectors.
async fn install(f: &Fixture, plan: LexicalPlanIdentity) -> Vec<u8> {
    let binding = LexicalIndexBinding::for_candidate(
        &f.index,
        f.embedding.clone(),
        f.coordinator.binding(),
        plan,
        &"a".repeat(64),
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
        "source_snapshot_sha256",
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
            "source_snapshot_sha256" => wire[field] = serde_json::json!("invalid"),
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
            &[original.clone()],
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
            &[original.clone()],
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
