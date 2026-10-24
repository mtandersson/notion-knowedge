use super::*;
use crate::chunks::CandidateLimits;
use crate::chunks::lexical_binding::{self, LexicalPlanIdentity};
use notion_knowledge_core::write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey};
use std::os::unix::fs::PermissionsExt;

#[test]
fn transition_native_verification_checks_pinned_pair_before_and_after_installation() {
    use crate::chunks::{EmbeddedChunk, TransitionCandidateInputs, verify_transition_candidate};
    use crate::commit::transition::*;
    use sha2::Digest;
    use std::os::unix::fs::MetadataExt;
    for (source_head_changes, publication_side_effect, resume_published, unsupported_work) in [
        (false, false, false, false),
        (false, false, true, false),
        (true, false, false, false),
        (false, true, false, false),
        (false, false, false, true),
    ] {
        let f = Fixture::new();
        for path in [&f.root, &f.index] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            f.create().await;
            let table = f.guarded();
            let prepared = table
                .prepare_page(
                    &CountingProvider::new(f.embedding.clone()),
                    operation("seed", "seed", false),
                    &[
                        chunk("one", "NK-1234 cinnamon roll", "Baking notes"),
                        chunk("two", "cardamom bun", "Spice notes"),
                    ],
                )
                .await
                .unwrap();
            table
                .commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
                .await
                .unwrap();
        });
        f.coordinator.state().release_lease(&f.lease, 11).unwrap();
        let source = f.coordinator.binding().clone();
        let next = CommitBinding::new(
            "chunks",
            "workspace",
            &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
            "v2",
            3,
        )
        .unwrap();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        let candidate = f.root.join("candidate");
        let preparation_backup = f.root.join("preparation.sqlite");
        let prepared = f
            .guarded()
            .prepare_lexical_candidate(
                &candidate,
                &preparation_backup,
                &next,
                &dictionary,
                12,
                CandidateLimits::default(),
            )
            .unwrap();
        let ready_bytes = fs::read(candidate.join(".nk-lexical-candidate-ready-v1.json")).unwrap();
        let state_path = f.root.join("state.sqlite");
        if unsupported_work {
            rusqlite::Connection::open(&state_path).unwrap().execute(
                "INSERT INTO page_relation_projections VALUES('page','v1',1,'relation-v1:invalid')", []
            ).unwrap();
        }
        if publication_side_effect {
            rusqlite::Connection::open(&state_path).unwrap().execute_batch(
                "CREATE TABLE opaque_state(value TEXT); INSERT INTO opaque_state VALUES('retained');
                 CREATE TRIGGER unexpected_publication_effect AFTER UPDATE OF generation ON index_commit_binding
                 WHEN NEW.generation='v2' BEGIN UPDATE opaque_state SET value='changed'; END;"
            ).unwrap();
        }
        let activation_backup = f.root.join("activation.sqlite");
        let backup = f
            .coordinator
            .with_offline_backup(&activation_backup, 12, |backup| Ok(backup.clone()))
            .unwrap();
        let state = fs::metadata(&state_path).unwrap();
        let old_index = fs::metadata(&f.index).unwrap();
        let new_index = fs::metadata(&candidate).unwrap();
        let sql = rusqlite::Connection::open(&state_path).unwrap();
        let mut record = TransitionRecord {
            version: 1,
            phase: TransitionPhase::Prepared,
            state_device: state.dev(),
            state_inode: state.ino(),
            state_token: sql
                .query_row(
                    "SELECT identity FROM index_commit_state_identity",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
            source: TransitionEndpoint {
                path: f.index.clone(),
                device: old_index.dev(),
                inode: old_index.ino(),
                binding: serde_json::to_string(&source).unwrap(),
            },
            target: TransitionEndpoint {
                path: f.index.clone(),
                device: new_index.dev(),
                inode: new_index.ino(),
                binding: serde_json::to_string(&next).unwrap(),
            },
            candidate_path: candidate.clone(),
            archive_path: f.root.join("archive"),
            quarantine_generation: "quarantine".into(),
            quarantine_epoch: 2,
            snapshot_sha256: prepared.snapshot_sha256.clone(),
            asset_sha256: dictionary.manifest_digest().into(),
            ready_sha256: format!("{:x}", sha2::Sha256::digest(&ready_bytes)),
            adoption_sha256: "d".repeat(64),
            backup_path: activation_backup.clone(),
            backup,
        };
        record.adoption_sha256 = manifest::comparison::capture_backup_manifest(
            &record,
            &manifest::comparison::ComparisonInputs {
                source: &source,
                target: &next,
                // This fixture has no active durable plan to rewrite.
                target_version: 1,
                plan_bytes: adoption::AdoptionLimits::default().plan_bytes,
                backup_bytes: 1024 * 1024 * 1024,
                limits: manifest::ManifestLimits::default(),
            },
        )
        .unwrap()
        .sha256;
        let owner = TransitionOwnership::acquire(&state_path, &record).unwrap();
        record.persist(&owner, None).unwrap();
        let verify = |dictionary: &crate::lexical_dictionary::Dictionary| {
            owner.with_exclusive_sql(12, |transaction| {
                let verified = verify_transition_candidate(
                    &owner,
                    transaction,
                    TransitionCandidateInputs {
                        source: &source,
                        target: &next,
                        embedding: &f.embedding,
                        dictionary,
                        preparation_backup: &preparation_backup,
                        limits: CandidateLimits::default(),
                    },
                )?;
                Ok((
                    verified.prepared().clone(),
                    verified.target_version(),
                    verified.record().phase,
                ))
            })
        };
        let before = fs::read(&state_path).unwrap();
        let verified = verify(&dictionary).unwrap();
        assert_eq!(verified.0, prepared);
        assert!(verified.1 > 0);
        assert_eq!(verified.2, TransitionPhase::Prepared);
        assert_eq!(fs::read(&state_path).unwrap(), before);
        let preview = publication::preview_publication(
            &owner,
            12,
            TransitionCandidateInputs {
                source: &source,
                target: &next,
                embedding: &f.embedding,
                dictionary: &dictionary,
                preparation_backup: &preparation_backup,
                limits: CandidateLimits::default(),
            },
            adoption::AdoptionLimits::default(),
            manifest::ManifestLimits::default(),
            1 << 30,
        );
        assert_eq!(fs::read(&state_path).unwrap(), before);
        assert_eq!(
            TransitionRecord::read(&state_path).unwrap().unwrap().phase,
            TransitionPhase::Prepared
        );
        if unsupported_work {
            assert!(preview.is_err());
            drop(owner);
            assert!(IndexCommitCoordinator::open_existing(&f.index, &state_path, source).is_ok());
            continue;
        }
        preview.unwrap();
        let wrong = crate::lexical_dictionary::tests::dictionary(&[("bread", "bröd")]);
        assert!(verify(&wrong).is_err());
        fs::write(candidate.join(".nk-lexical-candidate-ready-v1.json"), b"{}").unwrap();
        assert!(verify(&dictionary).is_err());
        fs::write(
            candidate.join(".nk-lexical-candidate-ready-v1.json"),
            &ready_bytes,
        )
        .unwrap();
        let moved_backup = f.root.join("retained-preparation.sqlite");
        fs::rename(&preparation_backup, &moved_backup).unwrap();
        assert!(verify(&dictionary).is_err());
        fs::rename(&moved_backup, &preparation_backup).unwrap();
        owner.quarantine(12).unwrap();
        owner.install_candidate(12).unwrap();
        let installed = verify(&dictionary).unwrap();
        assert_eq!(installed.0, prepared);
        assert_eq!(installed.1, verified.1);
        assert_eq!(installed.2, TransitionPhase::TargetInstalled);
        assert!(
            IndexCommitCoordinator::open_existing(&f.index, &state_path, next.clone()).is_err()
        );
        let binding_path = f.index.join(lexical_binding::FILE);
        let binding_bytes = fs::read(&binding_path).unwrap();
        fs::write(&binding_path, b"{}").unwrap();
        assert!(verify(&dictionary).is_err());
        fs::write(&binding_path, &binding_bytes).unwrap();
        let anchor_path = record.archive_path.join(".nk-operational-binding");
        let anchor = fs::read(&anchor_path).unwrap();
        fs::write(&anchor_path, b"wrong").unwrap();
        assert!(verify(&dictionary).is_err());
        fs::write(&anchor_path, anchor).unwrap();
        assert!(verify(&dictionary).is_ok());
        assert_eq!(
            sql.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            "quarantine"
        );
        let publish = || {
            publication::publish_derived_state(
                &owner,
                12,
                TransitionCandidateInputs {
                    source: &source,
                    target: &next,
                    embedding: &f.embedding,
                    dictionary: &dictionary,
                    preparation_backup: &preparation_backup,
                    limits: CandidateLimits::default(),
                },
                adoption::AdoptionLimits::default(),
                manifest::ManifestLimits::default(),
                1024 * 1024 * 1024,
            )
        };
        let complete = |owner: &TransitionOwnership| {
            publication::complete_publication(
                owner,
                12,
                TransitionCandidateInputs {
                    source: &source,
                    target: &next,
                    embedding: &f.embedding,
                    dictionary: &dictionary,
                    preparation_backup: &preparation_backup,
                    limits: CandidateLimits::default(),
                },
                adoption::AdoptionLimits::default().plan_bytes,
                manifest::ManifestLimits::default(),
                1024 * 1024 * 1024,
            )
        };
        // Quarantine SQL cannot be treated as an already committed publication.
        assert!(complete(&owner).is_err());
        if source_head_changes {
            runtime.block_on(async {
                let source =
                    LanceChunkTable::open(&record.archive_path, "chunks", f.embedding.clone())
                        .await
                        .unwrap();
                source
                    .upsert(
                        &f.embedding,
                        &[EmbeddedChunk::new(
                            chunk("new", "changed", "changed"),
                            vec![1., 0., 0.],
                        )],
                    )
                    .await
                    .unwrap();
            });
            assert!(verify(&dictionary).is_err());
            assert!(publish().is_err());
        } else if publication_side_effect {
            assert!(publish().is_err());
            assert_eq!(
                sql.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
                "quarantine"
            );
            assert_eq!(
                sql.query_row("SELECT value FROM opaque_state", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "retained"
            );
            assert_eq!(
                sql.query_row(
                    "SELECT COUNT(*) FROM index_commit_receipts WHERE generation='v2'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                0
            );
        } else {
            publish().unwrap();
            assert_eq!(
                sql.query_row("SELECT generation FROM index_commit_binding", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
                "v2"
            );
            assert_eq!(
                TransitionRecord::read(&state_path).unwrap().unwrap().phase,
                TransitionPhase::TargetInstalled
            );
            assert!(
                IndexCommitCoordinator::open_existing(&f.index, &state_path, next.clone()).is_err()
            );
            // Repeating publication cannot adopt or replay work a second time.
            assert!(publish().is_err());
            drop(owner);
            let mut recovered = TransitionRecord::read(&state_path).unwrap().unwrap();
            let owner = TransitionOwnership::acquire(&state_path, &recovered).unwrap();
            if resume_published {
                let previous = recovered.clone();
                recovered.phase = TransitionPhase::Published;
                recovered.persist(&owner, Some(&previous)).unwrap();
            }
            let target_anchor = f.index.join(".nk-operational-binding");
            fs::write(&target_anchor, b"foreign").unwrap();
            assert!(complete(&owner).is_err());
            assert_eq!(
                TransitionRecord::read(&state_path).unwrap().unwrap().phase,
                recovered.phase
            );
            fs::remove_file(&target_anchor).unwrap();
            assert_eq!(complete(&owner).unwrap().phase, TransitionPhase::Complete);
            assert_eq!(
                fs::read(&target_anchor).unwrap(),
                fs::read(&anchor_path).unwrap()
            );
            drop(owner);
            assert!(
                IndexCommitCoordinator::open_existing(&f.index, &state_path, next.clone()).is_ok()
            );
            assert!(
                IndexCommitCoordinator::open_existing(&f.index, &state_path, source.clone())
                    .is_err()
            );
            // A later transition retains immutable history before replacing
            // the completed active journal; the source remains v2 throughout.
            let completed = TransitionRecord::read(&state_path).unwrap().unwrap();
            let history_bytes = fs::read(journal_path(&state_path).unwrap()).unwrap();
            let future_candidate = f.root.join("future-candidate");
            fs::create_dir(&future_candidate).unwrap();
            fs::set_permissions(&future_candidate, fs::Permissions::from_mode(0o700)).unwrap();
            let identity = fs::metadata(&future_candidate).unwrap();
            let third = CommitBinding::new(
                "chunks",
                "workspace",
                &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
                "v3",
                5,
            )
            .unwrap();
            let mut future = completed.clone();
            future.phase = TransitionPhase::Prepared;
            future.source = completed.target.clone();
            future.target = TransitionEndpoint {
                path: f.index.clone(),
                device: identity.dev(),
                inode: identity.ino(),
                binding: serde_json::to_string(&third).unwrap(),
            };
            future.candidate_path = future_candidate;
            future.archive_path = f.root.join("future-archive");
            future.quarantine_generation = "future-quarantine".into();
            future.quarantine_epoch = 4;
            let owner = TransitionOwnership::acquire(&state_path, &future).unwrap();
            let history = owner.retire_completed(12, &completed).unwrap();
            assert_eq!(fs::read(&history).unwrap(), history_bytes);
            assert!(TransitionRecord::read(&state_path).unwrap().is_none());
            // Simulate a retained history copy with the old active record still
            // present, then the remove-before-return retry window.
            fs::copy(&history, journal_path(&state_path).unwrap()).unwrap();
            assert_eq!(owner.retire_completed(12, &completed).unwrap(), history);
            assert_eq!(owner.retire_completed(12, &completed).unwrap(), history);
            fs::write(&history, b"corrupt").unwrap();
            assert!(owner.retire_completed(12, &completed).is_err());
            fs::write(&history, &history_bytes).unwrap();
            drop(owner);
            assert!(
                IndexCommitCoordinator::open_existing(&f.index, &state_path, next.clone()).is_ok()
            );
            assert!(completed.backup_path.exists());
        }
    }
}

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
    // A wrong native FTS profile is incompatible even though source rows,
    // vectors, exact-ID indices and the declared binding are unchanged.
    runtime.block_on(async {
        let binding = lexical_binding::read(&candidate).unwrap().unwrap();
        let table = super::super::open_current(&candidate, "chunks", &f.embedding, &Some(binding))
            .await
            .unwrap();
        table
            .rebuild_fts_index(&crate::chunks::FtsIndexConfig::default())
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
            .is_err()
    );
    runtime.block_on(async {
        let binding = lexical_binding::read(&candidate).unwrap().unwrap();
        let table = super::super::open_current(&candidate, "chunks", &f.embedding, &Some(binding))
            .await
            .unwrap();
        table
            .rebuild_fts_index(&crate::chunks::FtsIndexConfig::lexical_expansion_v1())
            .await
            .unwrap();
    });
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
    let coordinator = IndexCommitCoordinator::open_existing(
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
        let reopened = IndexCommitCoordinator::open_existing(
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

#[test]
fn offline_candidate_native_preflight_rejects_oversized_persisted_rows_before_copy() {
    let f = offline_process_fixture();
    let table = runtime().block_on(f.readable());
    runtime().block_on(async {
        table.upsert(&f.embedding,&[crate::chunks::EmbeddedChunk::new(chunk("oversized",&"x".repeat(100_000),"Large source"),vec![1.0,1.0,0.0])]).await.unwrap();
        let result=super::super::candidate::preflight_rows(&table,CandidateLimits::default()).await;
        assert!(matches!(result,Err(crate::chunks::ChunkTableError::InvalidRows(message)) if message=="offline row exceeds preflight materialization budget"));
    });
    let source = crate::chunks::OfflineLexicalSource::bind(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    let backup = f.root.join("backup.sqlite");
    assert!(
        source
            .prepare(
                &candidate,
                &backup,
                &next_binding(),
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .is_err()
    );
    assert!(
        !candidate.exists(),
        "full source copy must not begin after rejected native metadata preflight"
    );
    assert!(backup.exists());
}

#[test]
fn offline_candidate_rejects_backup_inside_candidate_before_creating_recovery_file() {
    let f = offline_process_fixture();
    let source = crate::chunks::OfflineLexicalSource::bind(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    fs::create_dir(&candidate).unwrap();
    fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700)).unwrap();
    let backup = candidate.join("recovery.sqlite");
    assert_eq!(
        source.prepare(
            &candidate,
            &backup,
            &next_binding(),
            &dictionary,
            12,
            CandidateLimits::default()
        ),
        Err(CommitError::InvalidInput)
    );
    assert!(!backup.exists());
}

#[test]
fn offline_candidate_empty_source_has_verifiable_native_profiles_without_relabeling() {
    let f = Fixture::new();
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700)).unwrap();
    runtime().block_on(f.create());
    f.coordinator.state().release_lease(&f.lease, 11).unwrap();
    let source = crate::chunks::OfflineLexicalSource::bind(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    let backup = f.root.join("backup.sqlite");
    let prepared = source
        .prepare(
            &candidate,
            &backup,
            &next_binding(),
            &dictionary,
            12,
            CandidateLimits::default(),
        )
        .unwrap();
    assert_eq!(prepared.rows, 0);
    assert_eq!(
        source
            .verify(
                &candidate,
                &backup,
                &next_binding(),
                &dictionary,
                12,
                CandidateLimits::default()
            )
            .unwrap(),
        prepared
    );
    assert_eq!(f.coordinator.binding().generation(), "v1");
}

fn transition_child(f: &Fixture, phase: &str) -> CandidateChild {
    CandidateChild(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "chunks::guarded::io_tests::candidate::transition_process_child",
                "--ignored",
                "--nocapture",
            ])
            .env("NK_TRANSITION_TEST_ROOT", &f.root)
            .env("NK_TRANSITION_TEST_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
#[ignore = "only launched by transition process tests"]
fn transition_process_child() {
    use crate::commit::transition::*;
    let root = PathBuf::from(std::env::var_os("NK_TRANSITION_TEST_ROOT").unwrap());
    let state = root.join("state.sqlite");
    let index = root.join("index");
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let source = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
    let target = CommitBinding::new("chunks", "workspace", &scope, "v2", 3).unwrap();
    let embedding =
        EmbeddingMetadata::new("count".into(), "test-model".into(), "v1".into(), 3).unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let preparation_backup = root.join("preparation.sqlite");
    let journal = TransitionRecord::read(&state).unwrap();
    if journal
        .as_ref()
        .is_some_and(|record| record.phase == TransitionPhase::Complete)
    {
        let coordinator = IndexCommitCoordinator::open_existing(&index, &state, target).unwrap();
        let table = GuardedChunkTable::bind_with_lexical_plan(
            &index,
            "chunks",
            embedding,
            coordinator,
            &LexicalPlanIdentity::verified(&dictionary).unwrap(),
        )
        .unwrap();
        runtime().block_on(async {
            assert_eq!(table.page_chunks("page").await.unwrap().len(), 1);
            assert_eq!(table.indexed_page_ids().await.unwrap(), vec!["page"]);
        });
        return;
    }
    let record = journal.unwrap_or_else(|| {
        let coordinator =
            IndexCommitCoordinator::open_existing(&index, &state, source.clone()).unwrap();
        let offline = crate::chunks::OfflineLexicalSource::bind(
            &index,
            "chunks",
            embedding.clone(),
            coordinator.clone(),
        )
        .unwrap();
        offline
            .prepare(
                &root.join("candidate"),
                &preparation_backup,
                &target,
                &dictionary,
                12,
                CandidateLimits::default(),
            )
            .unwrap();
        preparation::prepare_activation(
            &coordinator,
            12,
            crate::chunks::TransitionCandidateInputs {
                source: &source,
                target: &target,
                embedding: &embedding,
                dictionary: &dictionary,
                preparation_backup: &preparation_backup,
                limits: CandidateLimits::default(),
            },
            preparation::ActivationPaths {
                candidate: &root.join("candidate"),
                archive: &root.join("archive"),
                backup: &root.join("activation.sqlite"),
                quarantine_generation: "quarantine",
            },
            adoption::AdoptionLimits::default(),
            manifest::ManifestLimits::default(),
            1 << 30,
        )
        .unwrap()
    });
    let owner = TransitionOwnership::acquire(&state, &record).unwrap();
    if TransitionRecord::read(&state).unwrap().is_none() {
        record.persist(&owner, None).unwrap();
    }
    let candidate = crate::chunks::TransitionCandidateInputs {
        source: &source,
        target: &target,
        embedding: &embedding,
        dictionary: &dictionary,
        preparation_backup: &preparation_backup,
        limits: CandidateLimits::default(),
    };
    publication::reconcile_forward(
        &owner,
        12,
        candidate,
        adoption::AdoptionLimits::default(),
        manifest::ManifestLimits::default(),
        1 << 30,
    )
    .unwrap();
}

#[test]
fn killed_transition_recovers_every_durable_phase_in_a_fresh_process() {
    use crate::commit::transition::*;
    for phase in [
        "prepared",
        "quarantine-sql",
        "quarantined",
        "source-renamed",
        "source-retained",
        "target-renamed",
        "target-installed",
        "sql-before-commit",
        "sql-published",
        "anchor-installed",
        "published",
        "complete",
    ] {
        let f = offline_process_fixture();
        fs::set_permissions(&f.index, fs::Permissions::from_mode(0o700)).unwrap();
        let original_inode = std::os::unix::fs::MetadataExt::ino(
            &fs::metadata(f.root.join("state.sqlite")).unwrap(),
        );
        let mut child = transition_child(&f, phase);
        wait_for(|| f.root.join("observed-transition").exists());
        assert_eq!(
            fs::read_to_string(f.root.join("observed-transition")).unwrap(),
            phase
        );
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        if phase == "sql-before-commit" {
            let journal = fs::read(f.root.join("state.sqlite-journal")).unwrap();
            assert!(journal.len() > 512);
            assert_eq!(
                &journal[..8],
                &[0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7]
            );
        }
        // Avoid another writable opener recovering the hot journal before the
        // transition operator itself has demonstrated that recovery.
        if !matches!(phase, "prepared" | "complete" | "sql-before-commit") {
            assert!(
                IndexCommitCoordinator::open_existing(
                    &f.index,
                    f.root.join("state.sqlite"),
                    f.coordinator.binding().clone()
                )
                .is_err(),
                "{phase}"
            );
        }
        finish_candidate_child(&mut transition_child(&f, ""));
        finish_candidate_child(&mut transition_child(&f, ""));
        assert_eq!(
            TransitionRecord::read(&f.root.join("state.sqlite"))
                .unwrap()
                .unwrap()
                .phase,
            TransitionPhase::Complete
        );
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(
                &fs::metadata(f.root.join("state.sqlite")).unwrap()
            ),
            original_inode
        );
        let sql = rusqlite::Connection::open(f.root.join("state.sqlite")).unwrap();
        assert_eq!(
            sql.query_row(
                "SELECT COUNT(*) FROM index_commit_receipts WHERE generation='v2' AND status=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            sql.query_row("SELECT COUNT(*) FROM index_commit_generations", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            3
        );
    }
}

#[test]
fn activation_preparation_rejects_occupied_archive_without_creating_backup() {
    use crate::commit::transition::*;
    let f = offline_process_fixture();
    let target = CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        3,
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate = f.root.join("candidate");
    let preparation_backup = f.root.join("preparation.sqlite");
    f.guarded()
        .prepare_lexical_candidate(
            &candidate,
            &preparation_backup,
            &target,
            &dictionary,
            12,
            CandidateLimits::default(),
        )
        .unwrap();
    let archive = f.root.join("occupied-archive");
    fs::write(&archive, b"retain this artifact").unwrap();
    let backup = f.root.join("activation.sqlite");
    let state = f.root.join("state.sqlite");
    let original = fs::read(&state).unwrap();
    assert!(
        preparation::prepare_activation(
            &f.coordinator,
            12,
            crate::chunks::TransitionCandidateInputs {
                source: f.coordinator.binding(),
                target: &target,
                embedding: &f.embedding,
                dictionary: &dictionary,
                preparation_backup: &preparation_backup,
                limits: CandidateLimits::default()
            },
            preparation::ActivationPaths {
                candidate: &candidate,
                archive: &archive,
                backup: &backup,
                quarantine_generation: "quarantine"
            },
            adoption::AdoptionLimits::default(),
            manifest::ManifestLimits::default(),
            1 << 30
        )
        .is_err()
    );
    assert!(!backup.exists());
    assert!(TransitionRecord::read(&state).unwrap().is_none());
    assert_eq!(fs::read(&state).unwrap(), original);
    assert_eq!(fs::read(&archive).unwrap(), b"retain this artifact");
    for bad_backup in [
        candidate.join("forbidden-state.sqlite"),
        f.root.join("unused-archive"),
    ] {
        let unused_archive = f.root.join("unused-archive");
        assert!(
            preparation::prepare_activation(
                &f.coordinator,
                12,
                crate::chunks::TransitionCandidateInputs {
                    source: f.coordinator.binding(),
                    target: &target,
                    embedding: &f.embedding,
                    dictionary: &dictionary,
                    preparation_backup: &preparation_backup,
                    limits: CandidateLimits::default()
                },
                preparation::ActivationPaths {
                    candidate: &candidate,
                    archive: &unused_archive,
                    backup: &bad_backup,
                    quarantine_generation: "quarantine"
                },
                adoption::AdoptionLimits::default(),
                manifest::ManifestLimits::default(),
                1 << 30
            )
            .is_err()
        );
        assert!(!bad_backup.exists());
        assert!(!unused_archive.exists());
        assert!(TransitionRecord::read(&state).unwrap().is_none());
        assert_eq!(fs::read(&state).unwrap(), original);
    }
}
