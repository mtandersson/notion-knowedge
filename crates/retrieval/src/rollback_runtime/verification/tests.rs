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
        used = true;
        Ok(())
    })
    .unwrap();
    assert!(used);
    assert_eq!(bytes(&root), before);
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
