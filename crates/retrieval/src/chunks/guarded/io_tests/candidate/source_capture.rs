use super::*;

#[test]
fn quarantined_source_capture_preserves_rows_sql_and_revoked_admission() {
    use crate::chunks::{OfflineLexicalSource, TransitionCandidateInputs, canonical_transfer};
    use crate::commit::transition::{self, source::SourceCaptureLimits, *};
    let f = offline_process_fixture();
    fs::set_permissions(&f.index, fs::Permissions::from_mode(0o700)).unwrap();
    let state = f.root.join("state.sqlite");
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
