use super::*;
use crate::{
    commit::{CommitError, IndexCommitCoordinator},
    lexical_dictionary::prepare_archive,
};
use std::collections::BTreeMap;

fn private_directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn bytes(root: &Path) -> BTreeMap<PathBuf, String> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, String>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().into(),
                    format!("{:x}", Sha256::digest(std::fs::read(path).unwrap())),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}
fn asset_file(path: &Path, body: &[u8]) -> String {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    f.write_all(body).unwrap();
    format!("{:x}", Sha256::digest(body))
}
#[test]
fn opaque_asset_pinning_refuses_changed_bytes_and_replaced_or_aliased_files() {
    let parent = private_directory();
    let path = parent.path().join("asset");
    let sha = asset_file(&path, b"opaque selected asset");
    let asset = PinnedAsset::pin(&path, &sha).unwrap();
    assert!(PinnedAsset::pin(&path, &"0".repeat(64)).is_err());
    let link = parent.path().join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(PinnedAsset::pin(&link, &sha).is_err());
    let hard = parent.path().join("hard");
    std::fs::hard_link(&path, &hard).unwrap();
    assert!(asset.revalidate().is_err());
    std::fs::remove_file(hard).unwrap();
    std::fs::write(&path, b"changed selected asset").unwrap();
    assert!(asset.revalidate().is_err());
    std::fs::remove_file(&path).unwrap();
    asset_file(&path, b"opaque selected asset");
    assert!(asset.revalidate().is_err());
}
fn normal_runtime_case(archive: Option<&Path>) {
    let binary = std::env::var_os("NK_LEXICAL_RUNTIME").expect("freshly built normal executable");
    let path = Path::new(&binary);
    assert!(path.is_absolute());
    let sha = format!("{:x}", Sha256::digest(std::fs::read(path).unwrap()));
    let runtime = PinnedRuntime::pin(path, &sha, 1024 * 1024 * 1024).unwrap();
    let parent = private_directory();
    let root = parent.path().join("actual-source");
    let asset = archive.map(|archive| {
        let path = parent.path().join("prior.bundle");
        let info = prepare_archive(archive, &path).unwrap();
        PinnedAsset::pin(&path, &info.sha256).unwrap()
    });
    let mode = asset
        .as_ref()
        .map(PinnedAsset::mode)
        .unwrap_or_else(|| vec!["legacy".into()]);
    let args = [
        vec!["probe-create".into(), root.clone().into_os_string()],
        mode,
    ]
    .concat();
    runtime.run(&args, Duration::from_secs(60), 65536).unwrap();
    // Use the matching fixture's authority without loading an opaque prior
    // bundle in the host. The selected normal runtime owns that interpretation.
    let (binding, embedding) = probe::fixture_configuration(archive.is_none()).unwrap();
    let c = IndexCommitCoordinator::open_existing(
        root.join("index"),
        root.join("state.sqlite"),
        binding,
    )
    .unwrap();
    let before = bytes(&root);
    let mut used = false;
    c.with_owned_offline_inspection(10, |owner| {
        let proof = verify(
            owner,
            &runtime,
            asset.as_ref(),
            &embedding,
            VerificationLimits {
                process_budget: Duration::from_secs(60),
                ..VerificationLimits::default()
            },
        )
        .map_err(|_| CommitError::BindingMismatch)?;
        assert_eq!(proof.runtime_sha256(), sha);
        assert_eq!(proof.source().rows, 1);
        assert_eq!(proof.source().lexical.is_some(), archive.is_some());
        assert_eq!(
            proof.asset_sha256(),
            asset.as_ref().map(|a| a.sha256.as_str())
        );
        assert_eq!(proof.authority_sha256(), owner.authority_sha256()?);
        assert_eq!(
            proof.operational_snapshot(),
            &owner.state_snapshot(64 * 1024 * 1024)?
        );
        assert_eq!(proof.descriptor().is_legacy(), archive.is_none());
        let evidence_path = parent.path().join("runtime-evidence.json");
        let retained =
            crate::rollback_runtime::evidence::publish(&evidence_path, owner, &proof).unwrap();
        let original = std::fs::read(&evidence_path).unwrap();
        assert_eq!(
            crate::rollback_runtime::evidence::RetainedEvidence::read(&evidence_path).unwrap(),
            retained
        );
        assert_eq!(
            crate::rollback_runtime::evidence::publish(&evidence_path, owner, &proof).unwrap(),
            retained
        );
        assert_eq!(std::fs::read(&evidence_path).unwrap(), original);
        assert!(
            crate::rollback_runtime::evidence::publish(
                &root.join("index/forbidden.json"),
                owner,
                &proof
            )
            .is_err()
        );
        let alias = parent.path().join("alias.json");
        std::os::unix::fs::symlink(&evidence_path, &alias).unwrap();
        assert!(crate::rollback_runtime::evidence::RetainedEvidence::read(&alias).is_err());
        std::fs::remove_file(&alias).unwrap();
        std::fs::hard_link(&evidence_path, &alias).unwrap();
        assert!(crate::rollback_runtime::evidence::RetainedEvidence::read(&evidence_path).is_err());
        std::fs::remove_file(&alias).unwrap();
        std::fs::set_permissions(&evidence_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(crate::rollback_runtime::evidence::RetainedEvidence::read(&evidence_path).is_err());
        std::fs::set_permissions(&evidence_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        for invalid in [
            b"{".to_vec(),
            vec![b'x'; 65537],
            [original.clone(), b"\n".to_vec()].concat(),
        ] {
            std::fs::write(&evidence_path, &invalid).unwrap();
            assert!(
                crate::rollback_runtime::evidence::RetainedEvidence::read(&evidence_path).is_err()
            );
            assert!(
                crate::rollback_runtime::evidence::publish(&evidence_path, owner, &proof).is_err()
            );
            assert_eq!(std::fs::read(&evidence_path).unwrap(), invalid);
        }
        if archive.is_some() {
            let mut metadata: serde_json::Value = serde_json::from_slice(&original).unwrap();
            metadata["observation"]["source"]["lexical"]["version"] = 999.into();
            // Re-serialize through the descriptive type to preserve canonical
            // field order: refusal must be the unsupported nested schema.
            let unsupported: crate::rollback_runtime::evidence::RetainedEvidence =
                serde_json::from_value(metadata).unwrap();
            let canonical = serde_json::to_vec(&unsupported).unwrap();
            std::fs::write(&evidence_path, &canonical).unwrap();
            assert!(
                crate::rollback_runtime::evidence::RetainedEvidence::read(&evidence_path).is_err()
            );
            assert_eq!(std::fs::read(&evidence_path).unwrap(), canonical);
        }
        std::fs::write(&evidence_path, &original).unwrap();
        used = true;
        Ok(())
    })
    .unwrap();
    assert!(used);
    assert_eq!(bytes(&root), before);
    refusal_and_freshness_cases(&c, &runtime, asset.as_ref(), &embedding, &root, path);
    // A newly acknowledged webhook changes operational state even when the
    // native row/vector snapshot remains identical. Old evidence must not be
    // rewritten or accepted for this newer acknowledged state.
    use notion_knowledge_core::webhook::{WebhookEvent, WebhookInbox};
    let event = WebhookEvent {
        id: "00000000-0000-4000-8000-000000000001".into(),
        workspace_id: "00000000-0000-4000-8000-000000000002".into(),
        subscription_id: "00000000-0000-4000-8000-000000000003".into(),
        integration_id: "00000000-0000-4000-8000-000000000004".into(),
        entity_id: "00000000-0000-4000-8000-000000000005".into(),
        timestamp: "2026-10-25T00:00:00Z".into(),
        event_type: "page.content_updated".into(),
        entity_type: "page".into(),
        attempt_number: 1,
    };
    c.state().receive(&event).unwrap();
    let acknowledged = bytes(&root);
    assert_ne!(acknowledged, before);
    let evidence_path = parent.path().join("runtime-evidence.json");
    let retained = std::fs::read(&evidence_path).unwrap();
    c.with_owned_offline_inspection(10, |owner| {
        let fresh = verify(
            owner,
            &runtime,
            asset.as_ref(),
            &embedding,
            VerificationLimits::default(),
        )
        .map_err(|_| CommitError::BindingMismatch)?;
        assert!(crate::rollback_runtime::evidence::publish(&evidence_path, owner, &fresh).is_err());
        Ok(())
    })
    .unwrap();
    assert_eq!(std::fs::read(&evidence_path).unwrap(), retained);
    assert_eq!(bytes(&root), acknowledged);
    assert!(c.state().event(&event.key()).unwrap().is_some());
}
#[test]
#[ignore = "requires NK_LEXICAL_RUNTIME pointing to the freshly built normal executable"]
fn verified_prior_runtime_legacy_uses_actual_owned_source_without_sql_changes() {
    normal_runtime_case(None);
}
#[test]
#[ignore = "requires NK_LEXICAL_RUNTIME and the local pinned public NK_LEXICAL_TEST_ARCHIVE"]
fn verified_prior_runtime_bound_uses_actual_owned_source_and_exact_prior_asset() {
    let archive = std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").expect("local pinned public archive");
    normal_runtime_case(Some(Path::new(&archive)));
}

fn refusal_and_freshness_cases(
    c: &IndexCommitCoordinator,
    runtime: &PinnedRuntime,
    asset: Option<&PinnedAsset>,
    embedding: &EmbeddingMetadata,
    root: &Path,
    binary: &Path,
) {
    let baseline = bytes(root);
    // Mutation occurs only after real source inspection and both normal runtime
    // probes succeeded. Hooks are absent from production builds and subprocesses.
    let faults = if asset.is_some() {
        vec!["asset"]
    } else {
        vec!["index", "sql", "native"]
    };
    for fault in faults {
        let original = match fault {
            "asset" => asset.unwrap().path.clone(),
            "sql" => root.join("state.sqlite"),
            "index" => root.join("index"),
            _ => root.join("index/.nk-operational-binding"),
        };
        let retained = root.parent().unwrap().join(format!("retained-{fault}"));
        let change = original.clone();
        let keep = retained.clone();
        AFTER_PROBES.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                if fault == "native" {
                    let f = File::open(&change).unwrap();
                    let time = f.metadata().unwrap().modified().unwrap() + Duration::from_secs(1);
                    f.set_modified(time).unwrap();
                } else {
                    std::fs::rename(&change, &keep).unwrap();
                    if fault == "index" {
                        std::fs::create_dir(&change).unwrap();
                    } else {
                        std::fs::copy(&keep, &change).unwrap();
                    }
                }
            }))
        });
        c.with_owned_offline_inspection(10, |owner| {
            assert!(
                verify(
                    owner,
                    runtime,
                    asset,
                    embedding,
                    VerificationLimits::default()
                )
                .is_err(),
                "{fault} replacement must refuse a proof"
            );
            assert!(
                AFTER_PROBES.with(|hook| hook.borrow().is_none()),
                "real subprocess probes must have completed before mutation"
            );
            if fault != "native" {
                if fault == "index" {
                    std::fs::remove_dir(&original).unwrap();
                } else {
                    std::fs::remove_file(&original).unwrap();
                }
                std::fs::rename(&retained, &original).unwrap();
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(bytes(root), baseline);
    }
    if asset.is_some() {
        return;
    }
    let fixtures = private_directory();
    let source_path = fixtures.path().join("fixture.rs");
    std::fs::write(&source_path, include_str!("refusal_fixture.rs")).unwrap();
    for behavior in ["malformed", "oversized", "coupled"] {
        let executable = fixtures.path().join(behavior);
        let marker = fixtures.path().join(format!("{behavior}.reached-probe"));
        let status =
            std::process::Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
                .env("NK_VERIFIER_RUNTIME", binary)
                .env("NK_VERIFIER_BEHAVIOR", behavior)
                .env("NK_VERIFIER_MARKER", &marker)
                .args(["--edition=2024", "-C", "debuginfo=0", "-o"])
                .arg(&executable)
                .arg(&source_path)
                .status()
                .unwrap();
        assert!(status.success());
        let sha = format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap()));
        let fixture = PinnedRuntime::pin(&executable, &sha, 16 * 1024 * 1024).unwrap();
        c.with_owned_offline_inspection(10, |owner| {
            assert!(
                verify(
                    owner,
                    &fixture,
                    None,
                    embedding,
                    VerificationLimits::default()
                )
                .is_err(),
                "{behavior} runtime must not authorize use"
            );
            Ok(())
        })
        .unwrap();
        if behavior == "coupled" {
            assert_eq!(
                std::fs::read_to_string(&marker).unwrap(),
                "separated generation refused"
            );
        }
        assert_eq!(bytes(root), baseline);
    }
}
