//! Disposable real native protocol probe. These inputs never describe a serving
//! installation: creation requires a new private directory and fixed probe IDs.
use super::{Result, RuntimeError};
use crate::{
    chunks::{
        CandidateLimits, GuardedChunkTable, TransitionCandidateInputs,
        lexical_binding::{LexicalPlanIdentity, runtime::RuntimeDescriptor},
    },
    commit::{
        CommitBinding, IndexCommitCoordinator, PageAction, PageOperation,
        transition::{
            TransitionOwnership,
            adoption::AdoptionLimits,
            manifest::ManifestLimits,
            preparation::{ActivationPaths, prepare_activation},
            publication::reconcile_forward,
        },
    },
    lexical_dictionary::Dictionary,
};
use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    reconciliation::{ReconciliationJournal, ReconciliationScope},
    sync_state::PageSyncState,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::Path,
    sync::Arc,
};

const PAGE: &str = "NK-PROBE-PAGE-123";
const CHUNK: &str = "NK-PROBE-CHUNK-123";
const NOW: i64 = 10;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeReport {
    version: u32,
    descriptor: RuntimeDescriptor,
    policy: String,
    generation: String,
    epoch: u64,
    page: String,
    chunk: String,
    snapshot: crate::chunks::RuntimeSourceSnapshot,
}
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}
fn scope() -> Result<ReconciliationScope> {
    ReconciliationScope::new(
        vec!["probe-root".into()],
        vec![],
        "runtime-probe",
        "source-policy",
    )
    .map_err(err)
}
fn binding(generation: &str, epoch: u64) -> Result<CommitBinding> {
    CommitBinding::new(
        "probe_chunks",
        "probe-workspace",
        &scope()?,
        generation,
        epoch,
    )
    .map_err(err)
}
fn final_binding(dictionary: Option<&Dictionary>) -> Result<CommitBinding> {
    if dictionary.is_some() {
        binding("probe-expanded", 5)
    } else {
        binding("probe-current", 3)
    }
}
fn embedding() -> Result<EmbeddingMetadata> {
    EmbeddingMetadata::new(
        "offline-probe".into(),
        "fixed-vector".into(),
        "v1".into(),
        3,
    )
    .map_err(err)
}
fn clock() -> Arc<dyn Fn() -> i64 + Send + Sync> {
    Arc::new(|| NOW)
}
struct Provider(EmbeddingMetadata);
impl EmbeddingProvider for Provider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move { Ok(inputs.iter().map(|_| vec![1.0, 0.0, 0.0]).collect()) })
    }
}
fn chunk() -> IndexedChunk {
    IndexedChunk {
        schema_version: SchemaVersion::V1,
        chunk_id: CHUNK.into(),
        metadata: IndexedMetadata {
            page_id: PAGE.into(),
            block_id: None,
            url: "https://example.invalid/probe".into(),
            title: "Baking proof".into(),
            heading_path: vec![],
            last_edited_time: "probe-revision".into(),
            source: SourceMetadata {
                workspace_id: "probe-workspace".into(),
                root_page_id: "probe-root".into(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: "cinnamon roll".into(),
        content_hash: "probe-content-v1".into(),
        links: vec![],
    }
}
fn operation(id: &str) -> Result<PageOperation> {
    PageOperation::new(
        id,
        "probe-revision",
        PageAction::Refresh,
        PageSyncState::present(
            PAGE.into(),
            "probe-content-v1".into(),
            Some("probe-revision".into()),
        )
        .map_err(err)?,
    )
    .map_err(err)
}
fn bind_table(
    root: &Path,
    coordinator: Arc<IndexCommitCoordinator>,
    dictionary: Option<&Dictionary>,
) -> Result<GuardedChunkTable> {
    let plan = dictionary
        .map(LexicalPlanIdentity::verified)
        .transpose()
        .map_err(err)?;
    match plan {
        Some(plan) => GuardedChunkTable::bind_with_lexical_plan(
            root.join("index"),
            "probe_chunks",
            embedding()?,
            coordinator,
            &plan,
        ),
        None => GuardedChunkTable::bind(
            root.join("index"),
            "probe_chunks",
            embedding()?,
            coordinator,
        ),
    }
    .map_err(err)
}
fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(err)
}

/// Exercise actual coordinator separation, native page effects, stale prepared
/// work and (for expansion) the existing full fenced publication protocol.
/// Never accepts an existing root or arbitrary authority/configuration.
pub fn create(root: &Path, dictionary: Option<&Dictionary>) -> Result<ProbeReport> {
    if !root.is_absolute()
        || root.file_name().is_none()
        || root.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(RuntimeError);
    }
    let parent = root.parent().ok_or(RuntimeError)?;
    let meta = std::fs::symlink_metadata(parent)?;
    if !meta.is_dir()
        || meta.mode() & 0o077 != 0
        || meta.uid() != std::fs::metadata("/proc/self")?.uid()
    {
        return Err(RuntimeError);
    }
    std::fs::DirBuilder::new().mode(0o700).create(root)?;
    let index = root.join("index");
    std::fs::DirBuilder::new().mode(0o700).create(&index)?;
    let state = root.join("state.sqlite");
    let initial = IndexCommitCoordinator::initialize(&index, &state, binding("probe-revoked", 1)?)
        .map_err(err)?;
    let lease = initial.state().acquire_lease(NOW, 100).map_err(err)?;
    let old_table = bind_table(root, initial.clone(), None)?;
    let rt = runtime()?;
    let provider = Provider(embedding()?);
    rt.block_on(old_table.create_empty(lease.clone(), clock()))
        .map_err(err)?;
    let stale = rt
        .block_on(old_table.prepare_page(&provider, operation("stale-probe")?, &[chunk()]))
        .map_err(err)?;
    let current_binding = binding("probe-current", 3)?;
    let current = initial
        .change_scope(current_binding.clone(), &lease, NOW)
        .map_err(err)?;
    if rt
        .block_on(old_table.commit_page(stale, lease.clone(), clock(), || async { Ok(()) }))
        .is_ok()
    {
        return Err(RuntimeError);
    }
    if IndexCommitCoordinator::open_existing(&index, &state, binding("probe-revoked", 1)?).is_ok() {
        return Err(RuntimeError);
    }
    let table = bind_table(root, current.clone(), None)?;
    let mut initial_chunk = chunk();
    if dictionary.is_some() {
        initial_chunk.text = "initial cinnamon roll".into();
        initial_chunk.content_hash = "probe-initial-content".into();
    }
    let prepared = rt
        .block_on(table.prepare_page(&provider, operation("current-probe")?, &[initial_chunk]))
        .map_err(err)?;
    rt.block_on(table.commit_page(prepared, lease.clone(), clock(), || async { Ok(()) }))
        .map_err(err)?;
    current.state().release_lease(&lease, NOW).map_err(err)?;
    if let Some(dictionary) = dictionary {
        let candidate = root.join("candidate");
        let preparation_backup = root.join("preparation.sqlite");
        let target = final_binding(Some(dictionary))?;
        table
            .prepare_lexical_candidate(
                &candidate,
                &preparation_backup,
                &target,
                dictionary,
                NOW,
                CandidateLimits::default(),
            )
            .map_err(err)?;
        let input = TransitionCandidateInputs {
            source: &current_binding,
            target: &target,
            embedding: &embedding()?,
            dictionary,
            preparation_backup: &preparation_backup,
            limits: CandidateLimits::default(),
        };
        let archive = root.join("archive");
        let backup = root.join("activation.sqlite");
        let record = prepare_activation(
            &current,
            NOW,
            input,
            ActivationPaths {
                candidate: &candidate,
                archive: &archive,
                backup: &backup,
                quarantine_generation: "probe-quarantine",
            },
            AdoptionLimits::default(),
            ManifestLimits::default(),
            64 * 1024 * 1024,
        )
        .map_err(err)?;
        let owner = TransitionOwnership::acquire(&state, &record).map_err(err)?;
        reconcile_forward(
            &owner,
            NOW,
            input,
            AdoptionLimits::default(),
            ManifestLimits::default(),
            64 * 1024 * 1024,
        )
        .map_err(err)?;
        drop(owner);
        if IndexCommitCoordinator::open_existing(&index, &state, current_binding).is_ok() {
            return Err(RuntimeError);
        }
        // The chosen expansion runtime must perform real native writes under
        // the new physical binding, not merely describe the installed metadata.
        let target = IndexCommitCoordinator::open_existing(&index, &state, target).map_err(err)?;
        let target_table = bind_table(root, target.clone(), Some(dictionary))?;
        let lease = target.state().acquire_lease(NOW, 100).map_err(err)?;
        let prepared = rt
            .block_on(target_table.prepare_page(
                &provider,
                operation("expanded-probe")?,
                &[chunk()],
            ))
            .map_err(err)?;
        if prepared.metrics().changed != 1 {
            return Err(RuntimeError);
        }
        rt.block_on(
            target_table.commit_page(prepared, lease.clone(), clock(), || async { Ok(()) }),
        )
        .map_err(err)?;
        target.state().release_lease(&lease, NOW).map_err(err)?;
    }
    reopen(root, dictionary)
}

/// Read-only fresh-process admission and exact row/identifier probes. No repair,
/// initialization, migration, lease acquisition or operational writes.
pub fn reopen(root: &Path, dictionary: Option<&Dictionary>) -> Result<ProbeReport> {
    let authority = final_binding(dictionary)?;
    let coordinator = IndexCommitCoordinator::open_read_only(
        root.join("index"),
        root.join("state.sqlite"),
        authority.clone(),
    )
    .map_err(err)?;
    let table = bind_table(root, coordinator, dictionary)?;
    let rt = runtime()?;
    rt.block_on(async {
        if table.page_chunks(PAGE).await.map_err(err)? != vec![chunk()] {
            return Err(RuntimeError);
        }
        Ok(())
    })?;
    let snapshot = crate::chunks::inspect_runtime_source(
        &root.join("index"),
        &embedding()?,
        &authority,
        CandidateLimits::default(),
    )
    .map_err(err)?;
    if snapshot.rows != 1 {
        return Err(RuntimeError);
    }
    Ok(ProbeReport {
        version: 1,
        descriptor: RuntimeDescriptor::current(dictionary).map_err(err)?,
        policy: scope()?.identity().into(),
        generation: authority.generation().into(),
        epoch: authority.epoch(),
        page: PAGE.into(),
        chunk: CHUNK.into(),
        snapshot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::PermissionsExt;
    fn bytes(path: &Path) -> BTreeMap<std::path::PathBuf, String> {
        fn walk(root: &Path, path: &Path, result: &mut BTreeMap<std::path::PathBuf, String>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(root, &path, result);
                } else {
                    result.insert(
                        path.strip_prefix(root).unwrap().to_owned(),
                        format!("{:x}", Sha256::digest(std::fs::read(path).unwrap())),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        walk(path, path, &mut result);
        result
    }
    #[test]
    fn native_probe_verifies_both_modes_without_repairing_state_on_reopen() {
        let parent = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let expansion_dictionary =
            crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        for (name, dictionary) in [("legacy", None), ("expansion", Some(&expansion_dictionary))] {
            let root = parent.path().join(name);
            let report = create(&root, dictionary).unwrap();
            assert_eq!(report.snapshot.rows, 1);
            assert_eq!(report.snapshot.lexical.is_some(), dictionary.is_some());
            let before = bytes(&root);
            assert_eq!(reopen(&root, dictionary).unwrap(), report);
            assert_eq!(bytes(&root), before);
            assert!(create(&root, dictionary).is_err());
            assert_eq!(bytes(&root), before);
            let wrong_mode = if dictionary.is_some() {
                None
            } else {
                Some(&expansion_dictionary)
            };
            assert!(reopen(&root, wrong_mode).is_err());
            assert_eq!(bytes(&root), before);
        }
    }
    fn normal_binary_case(archive: Option<&Path>) {
        let binary = std::env::var_os("NK_LEXICAL_RUNTIME").expect("freshly built normal runtime");
        let binary = Path::new(&binary);
        assert!(binary.is_absolute());
        let sha = format!("{:x}", Sha256::digest(std::fs::read(binary).unwrap()));
        let runtime =
            crate::rollback_runtime::PinnedRuntime::pin(binary, &sha, 1024 * 1024 * 1024).unwrap();
        let parent = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let root = parent.path().join("probe");
        let mut mode: Vec<std::ffi::OsString> = vec!["legacy".into()];
        if let Some(archive) = archive {
            let asset = parent.path().join("asset.bundle");
            let info = crate::lexical_dictionary::prepare_archive(archive, &asset).unwrap();
            mode = vec![
                "expansion".into(),
                asset.into_os_string(),
                info.sha256.into(),
            ];
        }
        let run = |action: &str| {
            let args = [
                vec![action.into(), root.clone().into_os_string()],
                mode.clone(),
            ]
            .concat();
            let bytes = runtime
                .run(&args, std::time::Duration::from_secs(60), 65536)
                .unwrap();
            let report: ProbeReport = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_vec(&report).unwrap(), bytes);
            report
        };
        let created = run("probe-create");
        let before = bytes(&root);
        assert_eq!(run("probe-reopen"), created);
        assert_eq!(bytes(&root), before);
        assert_eq!(created.snapshot.rows, 1);
        assert_eq!(created.snapshot.lexical.is_some(), archive.is_some());
    }
    #[test]
    #[ignore = "requires NK_LEXICAL_RUNTIME pointing to the freshly built normal executable"]
    fn pinned_normal_runtime_reopens_legacy_native_probe() {
        normal_binary_case(None);
    }
    #[test]
    #[ignore = "requires NK_LEXICAL_RUNTIME and the local pinned public NK_LEXICAL_TEST_ARCHIVE"]
    fn pinned_normal_runtime_reopens_bound_native_probe_with_public_asset() {
        let archive = std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").expect("pinned public archive");
        normal_binary_case(Some(Path::new(&archive)));
    }

    #[test]
    fn native_source_evidence_refuses_budget_overflow_and_changed_binding() {
        let parent = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let root = parent.path().join("source");
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        create(&root, Some(&dictionary)).unwrap();
        let before = bytes(&root);
        for limits in [
            CandidateLimits {
                files: 1,
                ..CandidateLimits::default()
            },
            CandidateLimits {
                file_bytes: 1,
                ..CandidateLimits::default()
            },
            CandidateLimits {
                batch_bytes: 1,
                ..CandidateLimits::default()
            },
            CandidateLimits {
                identifier_bytes: 1,
                ..CandidateLimits::default()
            },
        ] {
            assert!(
                crate::chunks::inspect_runtime_source(
                    &root.join("index"),
                    &embedding().unwrap(),
                    &final_binding(Some(&dictionary)).unwrap(),
                    limits
                )
                .is_err()
            );
            assert_eq!(bytes(&root), before);
        }
        let other = crate::lexical_dictionary::tests::dictionary(&[("river", "flod")]);
        assert!(reopen(&root, Some(&other)).is_err());
        assert_eq!(bytes(&root), before);
        let binding = root.join("index/.nk-lexical-binding-v1.json");
        std::fs::write(binding, b"{}").unwrap();
        let corrupt = bytes(&root);
        assert!(reopen(&root, Some(&dictionary)).is_err());
        assert_eq!(bytes(&root), corrupt);
    }
}
