use super::*;

#[test]
fn quarantined_source_capture_preserves_rows_sql_and_revoked_admission() {
    capture_case(None);
}
#[test]
fn quarantined_source_capture_refuses_live_leases_and_replaced_authority() {
    for refusal in ["lease", "state", "source", "journal"] {
        capture_case(Some(refusal));
    }
}
fn capture_case(refusal: Option<&str>) {
    use crate::chunks::{OfflineLexicalSource, TransitionCandidateInputs, canonical_transfer};
    use crate::commit::transition::{self, source::SourceCaptureLimits, *};
    let f = offline_process_fixture();
    fs::set_permissions(&f.index, fs::Permissions::from_mode(0o700)).unwrap();
    let state = f.root.join("state.sqlite");
    let pending = pending_work(&f);
    let completed = f.coordinator.receipt("seed").unwrap().unwrap();
    assert_eq!(completed.status, ReceiptStatus::Applied);
    let source = f.coordinator.binding().clone();
    let target = CommitBinding::new(
        "chunks",
        "workspace",
        &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap(),
        "v2",
        3,
    )
    .unwrap();
    let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
    let candidate_path = f.root.join("candidate");
    let preparation_backup = f.root.join("preparation.sqlite");
    OfflineLexicalSource::bind(
        &f.index,
        "chunks",
        f.embedding.clone(),
        f.coordinator.clone(),
    )
    .unwrap()
    .prepare(
        &candidate_path,
        &preparation_backup,
        &target,
        &dictionary,
        12,
        CandidateLimits::default(),
    )
    .unwrap();
    let candidate = TransitionCandidateInputs {
        source: &source,
        target: &target,
        embedding: &f.embedding,
        dictionary: &dictionary,
        preparation_backup: &preparation_backup,
        limits: CandidateLimits::default(),
    };
    let mut record = preparation::prepare_activation(
        &f.coordinator,
        12,
        candidate,
        preparation::ActivationPaths {
            candidate: &candidate_path,
            archive: &f.root.join("archive"),
            backup: &f.root.join("activation.sqlite"),
            quarantine_generation: "quarantine",
        },
        adoption::AdoptionLimits::default(),
        manifest::ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    let owner = TransitionOwnership::acquire(&state, &record).unwrap();
    assert!(
        owner
            .with_quarantined_source_inspection(
                12,
                candidate,
                SourceCaptureLimits::default(),
                |_| Ok(())
            )
            .is_err(),
        "unquarantined state is not a source-choice capture"
    );
    record = owner.quarantine(12).unwrap();
    assert!(f.coordinator.search_guard().is_err());
    let original = crate::rollback_runtime::candidate::tests::bytes(&f.index);
    let sql = fs::read(&state).unwrap();
    if let Some(refusal) = refusal {
        let output = f.root.join("refused-capture");
        if refusal == "lease" {
            let lease = f.coordinator.state().acquire_lease(13, 100).unwrap();
            let active_sql = fs::read(&state).unwrap();
            let invoked = std::cell::Cell::new(false);
            let result = owner.with_quarantined_source_inspection(
                14,
                candidate,
                SourceCaptureLimits::default(),
                |_| {
                    invoked.set(true);
                    Ok(())
                },
            );
            assert!(matches!(result, Err(CommitError::Conflict)));
            assert!(!invoked.get());
            assert!(f.coordinator.search_guard().is_err());
            assert_eq!(fs::read(&state).unwrap(), active_sql);
            assert_eq!(
                crate::rollback_runtime::candidate::tests::bytes(&f.index),
                original
            );
            f.coordinator.state().release_lease(&lease, 15).unwrap();
            return;
        }
        let path = match refusal {
            "state" => state.clone(),
            "source" => f.index.clone(),
            "journal" => transition::journal_path(&state).unwrap(),
            _ => unreachable!(),
        };
        let retained = f.root.join("retained-replaced-authority");
        fs::rename(&path, &retained).unwrap();
        if refusal == "source" {
            fs::create_dir(&path).unwrap();
        } else {
            fs::copy(&retained, &path).unwrap();
        }
        if refusal == "journal" {
            fs::write(&path, b"corrupt replacement journal").unwrap();
        }
        let invoked = std::cell::Cell::new(false);
        let result = owner.with_quarantined_source_inspection(
            12,
            candidate,
            SourceCaptureLimits::default(),
            |inspection| {
                invoked.set(true);
                canonical_transfer::export_quarantined(
                    inspection,
                    &output,
                    &f.embedding,
                    Default::default(),
                )
                .map_err(|_| CommitError::BindingMismatch)
            },
        );
        if refusal == "source" {
            fs::remove_dir_all(&path).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
        }
        fs::rename(&retained, &path).unwrap();
        assert!(result.is_err(), "{refusal} replacement was accepted");
        assert!(!invoked.get());
        assert!(!output.exists());
        assert_eq!(fs::read(&state).unwrap(), sql);
        assert_eq!(
            crate::rollback_runtime::candidate::tests::bytes(&f.index),
            original
        );
        return;
    }
    let mut previous_authority = None;
    for phase in 0..4 {
        let output = f.root.join(format!("captured-source-{phase}"));
        let captured = owner
            .with_quarantined_source_inspection(
                12,
                candidate,
                SourceCaptureLimits::default(),
                |inspection| {
                    assert_eq!(inspection.binding(), &source);
                    for invalid in [
                        inspection.index_directory().join("invalid-transfer"),
                        f.root.join("state.sqlite-journal"),
                        transition::journal_path(&state).unwrap(),
                    ] {
                        assert!(
                            canonical_transfer::export_quarantined(
                                inspection,
                                &invalid,
                                &f.embedding,
                                Default::default()
                            )
                            .is_err()
                        );
                    }
                    let wrong =
                        EmbeddingMetadata::new("other".into(), "test-model".into(), "v1".into(), 3)
                            .unwrap();
                    let invalid = f.root.join(format!("wrong-embedding-{phase}"));
                    assert!(
                        canonical_transfer::export_quarantined(
                            inspection,
                            &invalid,
                            &wrong,
                            Default::default()
                        )
                        .is_err()
                    );
                    assert!(!invalid.exists());
                    let capture = canonical_transfer::export_quarantined(
                        inspection,
                        &output,
                        &f.embedding,
                        Default::default(),
                    )
                    .unwrap();
                    assert_eq!(capture.metadata.source.rows, 1);
                    assert_eq!(
                        capture.metadata.source.snapshot_sha256,
                        record.snapshot_sha256
                    );
                    let mut rows = 0;
                    canonical_transfer::read(&output, &capture, Default::default(), |batch| {
                        rows += batch.num_rows();
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(rows, 1);
                    Ok(capture)
                },
            )
            .unwrap();
        assert_eq!(fs::read(&state).unwrap(), sql);
        assert_eq!(
            f.coordinator.state().status(&pending).unwrap(),
            Some(IndexStatus::Queued)
        );
        assert_eq!(f.coordinator.receipt("seed").unwrap().unwrap(), completed);
        assert!(f.coordinator.search_guard().is_err());
        if let Some(previous) =
            previous_authority.replace(captured.metadata.ownership_sha256.clone())
        {
            assert_ne!(
                previous, captured.metadata.ownership_sha256,
                "each changed operation/physical phase needs a fresh capture"
            );
        }
        if phase == 0 {
            fs::rename(&f.index, &record.archive_path).unwrap();
        } else if phase == 1 {
            let previous = record.clone();
            record.phase = TransitionPhase::SourceRetained;
            record.persist(&owner, Some(&previous)).unwrap();
            fs::rename(&candidate_path, &f.index).unwrap();
        } else if phase == 2 {
            let previous = record.clone();
            record.phase = TransitionPhase::TargetInstalled;
            record.persist(&owner, Some(&previous)).unwrap();
        }
    }
    assert_eq!(
        crate::rollback_runtime::candidate::tests::bytes(&record.archive_path),
        original
    );
    publication::publish_derived_state(
        &owner,
        12,
        candidate,
        adoption::AdoptionLimits::default(),
        manifest::ManifestLimits::default(),
        64 << 20,
    )
    .unwrap();
    assert!(
        owner
            .with_quarantined_source_inspection(
                12,
                candidate,
                SourceCaptureLimits::default(),
                |_| Ok(())
            )
            .is_err(),
        "committed publication requires forward completion"
    );
}

#[test]
fn killed_transition_source_capture_uses_actual_sql_authority_after_restart() {
    use crate::{chunks::canonical_transfer, commit::transition::*};
    for (phase, allowed) in [
        ("runtime-quarantine-before-commit", false),
        ("quarantine-sql", true),
        ("source-renamed", true),
        ("target-renamed", true),
        ("sql-before-commit", true),
        ("sql-published", false),
        ("anchor-installed", false),
    ] {
        let f = offline_process_fixture();
        fs::set_permissions(&f.index, fs::Permissions::from_mode(0o700)).unwrap();
        let pending = pending_work(&f);
        let completed = f.coordinator.receipt("seed").unwrap().unwrap();
        let original = crate::rollback_runtime::candidate::tests::bytes(&f.index);
        let state = f.root.join("state.sqlite");
        let inode = std::os::unix::fs::MetadataExt::ino(&fs::metadata(&state).unwrap());
        let mut child = transition_child(&f, phase);
        wait_for(|| {
            fs::read_to_string(f.root.join("observed-transition")).is_ok_and(|value| value == phase)
        });
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        if phase.ends_with("before-commit") {
            assert!(
                fs::metadata(f.root.join("state.sqlite-journal"))
                    .unwrap()
                    .len()
                    > 512
            );
        }
        let mut stable_sql = None;
        for attempt in 0..2 {
            let action = format!(
                "capture-{}-{attempt}",
                if allowed { "allowed" } else { "refused" }
            );
            let mut fresh = transition_child_action(&f, "", &action);
            finish_candidate_child(&mut fresh);
            assert_eq!(
                std::os::unix::fs::MetadataExt::ino(&fs::metadata(&state).unwrap()),
                inode
            );
            let sql = fs::read(&state).unwrap();
            if let Some(previous) = stable_sql.replace(sql.clone()) {
                assert_eq!(sql, previous);
            }
            assert_eq!(
                f.coordinator.state().status(&pending).unwrap(),
                Some(IndexStatus::Queued)
            );
            assert_eq!(f.coordinator.receipt("seed").unwrap().unwrap(), completed);
            if allowed {
                let path = f.root.join(&action);
                let captured: canonical_transfer::TransferRecord =
                    serde_json::from_slice(&fs::read(path.with_extension("json")).unwrap())
                        .unwrap();
                let record = TransitionRecord::read(&state).unwrap().unwrap();
                assert_eq!(
                    captured.metadata.source.snapshot_sha256,
                    record.snapshot_sha256
                );
                let mut rows = 0;
                canonical_transfer::read(&path, &captured, Default::default(), |batch| {
                    rows += batch.num_rows();
                    Ok(())
                })
                .unwrap();
                assert_eq!(rows, 1);
            }
        }
        let source_path = if f.root.join("archive").exists() {
            f.root.join("archive")
        } else {
            f.index.clone()
        };
        assert_eq!(
            crate::rollback_runtime::candidate::tests::bytes(&source_path),
            original
        );
        let mut forward = transition_child(&f, "");
        finish_candidate_child(&mut forward);
        assert_eq!(
            TransitionRecord::read(&state).unwrap().unwrap().phase,
            TransitionPhase::Complete
        );
    }
}

fn pending_work(f: &Fixture) -> RefreshKey {
    let key = RefreshKey {
        workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        mutation_digest: "b".repeat(64),
        page_id: "cccccccc-cccc-cccc-cccc-cccccccccccc".into(),
    };
    assert_eq!(
        f.coordinator.state().admit(&key).unwrap(),
        IndexStatus::Queued
    );
    key
}
