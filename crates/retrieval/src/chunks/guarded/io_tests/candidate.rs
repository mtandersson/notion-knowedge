use super::*;
use crate::chunks::CandidateLimits;
use crate::chunks::lexical_binding::{self, LexicalPlanIdentity};
use notion_knowledge_core::write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey};
use std::os::unix::fs::PermissionsExt;

#[test]
fn offline_candidate_preserves_rows_vectors_pending_work_and_original_generation() {
    let f = Fixture::new();
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let rows = runtime.block_on(async {
        f.create().await;
        let guarded = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let prepared = guarded
            .prepare_page(
                &provider,
                operation("seed", "seed", false),
                &[
                    chunk("one", "cinnamon roll", "Baking notes"),
                    chunk("two", "cardamom bun", "Spice notes"),
                ],
            )
            .await
            .unwrap();
        guarded
            .commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
            .await
            .unwrap();
        f.readable()
            .await
            .rows_matching("true".into())
            .await
            .unwrap()
    });
    let guarded = f.guarded();
    f.coordinator.state().release_lease(&f.lease, 11).unwrap();
    let pending = RefreshKey {
        workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        mutation_digest: "b".repeat(64),
        page_id: "cccccccc-cccc-cccc-cccc-cccccccccccc".into(),
    };
    f.coordinator.state().admit(&pending).unwrap();
    let original = fs::read(f.root.join("state.sqlite")).unwrap();
    let next = CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        2,
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    let backup = f.root.join("backup.sqlite");
    let prepared = guarded
        .prepare_lexical_candidate(
            &candidate,
            &backup,
            &next,
            &dictionary,
            12,
            CandidateLimits::default(),
        )
        .unwrap();
    assert_eq!(prepared.rows, 2);
    assert_eq!(
        f.coordinator.state().status(&pending).unwrap(),
        Some(IndexStatus::Queued)
    );
    assert_eq!(fs::read(&backup).unwrap(), original);
    assert_eq!(fs::read(f.root.join("state.sqlite")).unwrap(), original);
    assert_eq!(f.coordinator.binding().generation(), "v1");
    assert!(
        !candidate.join(".nk-operational-binding").exists(),
        "preparation cannot adopt operational authority"
    );
    assert!(
        candidate
            .join(".nk-lexical-candidate-ready-v1.json")
            .exists()
    );
    runtime.block_on(async {
        let plan = LexicalPlanIdentity::verified(&dictionary).unwrap();
        let binding = lexical_binding::read(&candidate).unwrap().unwrap();
        binding
            .validate(&candidate, &f.embedding, &next, &plan)
            .unwrap();
        let copied = super::super::open_current(&candidate, "chunks", &f.embedding, &Some(binding))
            .await
            .unwrap();
        let copied = copied.rows_matching("true".into()).await.unwrap();
        for row in &rows {
            let current = copied.iter().find(|c| c.chunk_id == row.chunk_id).unwrap();
            assert_eq!(current.text, row.text);
            assert_eq!(current.content_hash, row.content_hash);
            assert_eq!(
                current
                    .vector
                    .iter()
                    .map(|n| n.to_bits())
                    .collect::<Vec<_>>(),
                row.vector.iter().map(|n| n.to_bits()).collect::<Vec<_>>()
            );
            assert_eq!(current.url, row.url);
            assert_eq!(current.heading_path, row.heading_path);
        }
        assert_eq!(guarded.page_chunks("page").await.unwrap().len(), 2);
    });
}

#[test]
fn offline_candidate_limit_failure_retains_backup_and_never_publishes_ready_metadata() {
    let f = Fixture::new();
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(f.create());
    f.coordinator.state().release_lease(&f.lease, 11).unwrap();
    let guarded = f.guarded();
    let original = fs::read(f.root.join("state.sqlite")).unwrap();
    let next = CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        2,
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    let backup = f.root.join("backup.sqlite");
    assert!(
        guarded
            .prepare_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits {
                    file_bytes: 1,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert_eq!(fs::read(&backup).unwrap(), original);
    assert_eq!(fs::read(f.root.join("state.sqlite")).unwrap(), original);
    assert!(
        !candidate
            .join(".nk-lexical-candidate-ready-v1.json")
            .exists()
    );
    runtime.block_on(async {
        assert!(guarded.page_chunks("page").await.unwrap().is_empty());
    });
}
