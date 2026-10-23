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
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .unwrap(),
        prepared
    );
    let ready_path = candidate.join(".nk-lexical-candidate-ready-v1.json");
    let ready_bytes = fs::read(&ready_path).unwrap();
    fs::remove_file(&ready_path).unwrap();
    assert!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err()
    );
    fs::write(&ready_path, b"{}").unwrap();
    assert!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err()
    );
    fs::write(&ready_path, &ready_bytes).unwrap();
    let changed_dictionary = crate::lexical_dictionary::tests::dictionary(&[("pepper", "peppar")]);
    assert!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &changed_dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err()
    );
    let backup_bytes = fs::read(&backup).unwrap();
    fs::write(&backup, b"corrupt recovery copy").unwrap();
    assert!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err()
    );
    fs::write(&backup, &backup_bytes).unwrap();
    assert_eq!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .unwrap(),
        prepared
    );
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
        let binding = lexical_binding::read(&candidate).unwrap().unwrap();
        let corrupt =
            super::super::open_current(&candidate, "chunks", &f.embedding, &Some(binding))
                .await
                .unwrap();
        corrupt
            .upsert(
                &f.embedding,
                &[crate::chunks::EmbeddedChunk::new(
                    chunk("one", "cinnamon roll", "Baking notes"),
                    vec![99.0, 1.0, 0.0],
                )],
            )
            .await
            .unwrap();
    });
    assert!(
        guarded
            .verify_lexical_candidate(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err(),
        "changed vector bits cannot be accepted under old ready metadata"
    );
    assert_eq!(
        fs::read(&ready_path).unwrap(),
        ready_bytes,
        "verification must never repair ready metadata"
    );
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

#[test]
fn offline_candidate_migrates_prior_plan_build_without_enabling_its_runtime_or_relabeling_vectors()
{
    use crate::chunks::{FtsIndexConfig, OfflineLexicalSource};
    let f = Fixture::new();
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        f.create().await;
        let raw = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let page = raw
            .prepare_page(
                &provider,
                operation("seed", "seed", false),
                &[chunk("one", "cinnamon", "Spice")],
            )
            .await
            .unwrap();
        raw.commit_page(page, f.lease.clone(), clock(), || async { Ok(()) })
            .await
            .unwrap();
        f.readable()
            .await
            .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
            .await
            .unwrap();
    });
    f.coordinator.state().release_lease(&f.lease, 11).unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let plan = LexicalPlanIdentity::verified(&dictionary).unwrap();
    let binding = crate::chunks::lexical_binding::LexicalIndexBinding::for_candidate(
        &f.index,
        f.embedding.clone(),
        f.coordinator.binding(),
        plan.clone(),
        &"a".repeat(64),
    )
    .unwrap();
    let mut old: serde_json::Value =
        serde_json::from_slice(&binding.canonical_bytes().unwrap()).unwrap();
    old["plan"]["importer_sha256"] = serde_json::json!("b".repeat(64));
    old["plan"]["executor_sha256"] = serde_json::json!("c".repeat(64));
    old["storage"]["implementation_sha256"] = serde_json::json!("d".repeat(64));
    let old: crate::chunks::lexical_binding::LexicalIndexBinding =
        serde_json::from_value(old).unwrap();
    let old_bytes = old.canonical_bytes().unwrap();
    fs::write(f.index.join(lexical_binding::FILE), &old_bytes).unwrap();
    assert!(
        GuardedChunkTable::bind_with_lexical_plan(
            &f.index,
            "chunks",
            f.embedding.clone(),
            f.coordinator.clone(),
            &plan
        )
        .is_err()
    );
    let offline = OfflineLexicalSource::bind(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
    )
    .unwrap();
    let next = CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        2,
    )
    .unwrap();
    let candidate = f.root.join("candidate");
    let backup = f.root.join("backup.sqlite");
    let prepared = offline
        .prepare(
            &candidate,
            &backup,
            &next,
            &dictionary,
            12,
            CandidateLimits::default(),
        )
        .unwrap();
    assert_eq!(prepared.rows, 1);
    assert_eq!(
        offline
            .verify(
                &candidate,
                &backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .unwrap(),
        prepared
    );
    assert_eq!(
        fs::read(f.index.join(lexical_binding::FILE)).unwrap(),
        old_bytes
    );
    assert_eq!(f.coordinator.binding().generation(), "v1");
    assert!(
        GuardedChunkTable::bind_with_lexical_plan(
            &f.index,
            "chunks",
            f.embedding.clone(),
            f.coordinator.clone(),
            &plan
        )
        .is_err()
    );
}

struct CandidateChild(std::process::Child);
impl Drop for CandidateChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn candidate_child(f: &Fixture, action: &str, phase: &str) -> CandidateChild {
    CandidateChild(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "chunks::guarded::io_tests::candidate::offline_candidate_process_child",
                "--ignored",
                "--nocapture",
            ])
            .env("NK_LEXICAL_PREP_TEST_ROOT", &f.root)
            .env("NK_LEXICAL_PREP_TEST_ACTION", action)
            .env("NK_LEXICAL_PREP_TEST_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    )
}
fn finish_candidate_child(child: &mut CandidateChild) {
    let mut status = None;
    wait_for(|| {
        status = child.0.try_wait().unwrap();
        status.is_some()
    });
    assert!(status.unwrap().success(), "candidate child process failed");
}
fn next_binding() -> CommitBinding {
    CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        2,
    )
    .unwrap()
}
fn offline_process_fixture() -> Fixture {
    let f = Fixture::new();
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700)).unwrap();
    runtime().block_on(async {
        f.create().await;
        let guarded = f.guarded();
        let provider = CountingProvider::new(f.embedding.clone());
        let prepared = guarded
            .prepare_page(
                &provider,
                operation("seed", "seed", false),
                &[chunk("one", "cinnamon", "Spice")],
            )
            .await
            .unwrap();
        guarded
            .commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
            .await
            .unwrap();
    });
    f.coordinator.state().release_lease(&f.lease, 11).unwrap();
    f
}
#[test]
#[ignore = "only launched by offline preparation process tests"]
fn offline_candidate_process_child() {
    let root = PathBuf::from(std::env::var_os("NK_LEXICAL_PREP_TEST_ROOT").unwrap());
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let coordinator = IndexCommitCoordinator::open(
        root.join("index"),
        root.join("state.sqlite"),
        CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap(),
    )
    .unwrap();
    let embedding =
        EmbeddingMetadata::new("count".into(), "test-model".into(), "v1".into(), 3).unwrap();
    let source = crate::chunks::OfflineLexicalSource::bind(
        &root.join("index"),
        "chunks",
        embedding,
        coordinator,
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let result = match std::env::var("NK_LEXICAL_PREP_TEST_ACTION")
        .unwrap()
        .as_str()
    {
        "prepare" => source.prepare(
            &root.join("candidate"),
            &root.join("backup.sqlite"),
            &next_binding(),
            &dictionary,
            12,
            CandidateLimits::default(),
        ),
        "verify" => source.verify(
            &root.join("candidate"),
            &root.join("backup.sqlite"),
            &next_binding(),
            &dictionary,
            12,
            CandidateLimits::default(),
        ),
        _ => panic!("unknown child operation"),
    }
    .unwrap();
    assert_eq!(result.rows, 1);
}
#[test]
fn offline_candidate_preparation_and_verification_survive_independent_process_restart() {
    let f = offline_process_fixture();
    let original = fs::read(f.root.join("state.sqlite")).unwrap();
    finish_candidate_child(&mut candidate_child(&f, "prepare", ""));
    finish_candidate_child(&mut candidate_child(&f, "verify", ""));
    assert_eq!(fs::read(f.root.join("state.sqlite")).unwrap(), original);
    assert_eq!(fs::read(f.root.join("backup.sqlite")).unwrap(), original);
    assert_eq!(f.coordinator.binding().generation(), "v1");
}
#[test]
fn offline_candidate_process_interruption_never_activates_or_publishes_incomplete_generation() {
    for phase in ["before-copy", "after-copy", "before-ready"] {
        let f = offline_process_fixture();
        let original = fs::read(f.root.join("state.sqlite")).unwrap();
        let mut child = candidate_child(&f, "prepare", phase);
        wait_for(|| f.root.join("observed-phase").exists());
        assert_eq!(
            fs::read_to_string(f.root.join("observed-phase")).unwrap(),
            phase
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert_eq!(
            fs::read(f.root.join("state.sqlite")).unwrap(),
            original,
            "{phase}"
        );
        assert_eq!(
            fs::read(f.root.join("backup.sqlite")).unwrap(),
            original,
            "{phase}"
        );
        assert!(
            !f.root
                .join("candidate/.nk-lexical-candidate-ready-v1.json")
                .exists(),
            "{phase}"
        );
        let reopened = IndexCommitCoordinator::open(
            &f.index,
            f.root.join("state.sqlite"),
            f.coordinator.binding().clone(),
        )
        .unwrap();
        let source = crate::chunks::OfflineLexicalSource::bind(
            &f.index,
            "chunks",
            f.embedding.clone(),
            reopened,
        )
        .unwrap();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        assert!(
            source
                .verify(
                    &f.root.join("candidate"),
                    &f.root.join("backup.sqlite"),
                    &next_binding(),
                    &dictionary,
                    12,
                    CandidateLimits::default()
                )
                .is_err()
        );
        let fresh = f.root.join("fresh-candidate");
        let fresh_backup = f.root.join("fresh-backup.sqlite");
        assert_eq!(
            source
                .prepare(
                    &fresh,
                    &fresh_backup,
                    &next_binding(),
                    &dictionary,
                    12,
                    CandidateLimits::default()
                )
                .unwrap()
                .rows,
            1
        );
        assert_eq!(
            source
                .verify(
                    &fresh,
                    &fresh_backup,
                    &next_binding(),
                    &dictionary,
                    12,
                    CandidateLimits::default()
                )
                .unwrap()
                .rows,
            1
        );
        assert_eq!(
            runtime()
                .block_on(f.guarded().page_chunks("page"))
                .unwrap()
                .len(),
            1
        );
    }
}
