//! Bounded read-only protocol for an independently selected runtime to inspect
//! the actual source. Requests/reports are descriptive, never ownership proof.
use super::{Result, RuntimeError};
use crate::{
    chunks::{
        CandidateLimits, RuntimeSourceSnapshot, inspect_runtime_source,
        lexical_binding::{self, LexicalPlanIdentity, runtime::RuntimeDescriptor},
    },
    commit::{CommitBinding, OwnedOfflineInspection},
    lexical_dictionary::Dictionary,
};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
const MAX_REQUEST: usize = 65536;
const MAX_REPORT: usize = 65536;
fn err<T>(_: T) -> RuntimeError {
    RuntimeError
}

struct BoundedJson {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max)
        {
            return Err(std::io::Error::other("offline metadata exceeds bound"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn canonical(value: &impl Serialize, max: usize) -> Result<Vec<u8>> {
    let mut output = BoundedJson {
        bytes: Vec::with_capacity(max),
        max,
    };
    serde_json::to_writer(&mut output, value).map_err(err)?;
    Ok(output.bytes)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRequest {
    version: u32,
    directory: PathBuf,
    authority_json: String,
    embedding: EmbeddingMetadata,
    limits: CandidateLimits,
}
fn limits_valid(limits: CandidateLimits) -> bool {
    let maximum = CandidateLimits::default();
    limits.files > 0
        && limits.files <= maximum.files
        && limits.file_bytes > 0
        && limits.file_bytes <= maximum.file_bytes
        && limits.rows > 0
        && limits.rows <= maximum.rows
        && limits.batch_bytes > 0
        && limits.batch_bytes <= maximum.batch_bytes
        && limits.identifier_bytes > 0
        && limits.identifier_bytes <= maximum.identifier_bytes
}
/// Produce a bounded request only while the original source owner is retained.
/// This omits the operational database path, token and all source content.
pub fn request_bytes(
    owner: &OwnedOfflineInspection<'_>,
    embedding: &EmbeddingMetadata,
    limits: CandidateLimits,
) -> Result<Vec<u8>> {
    owner.revalidate().map_err(err)?;
    archived_request_bytes(owner.index_directory(), owner.binding(), embedding, limits)
}

// Descriptive subprocess configuration only. The integrated caller separately
// retains current ownership and validates the archive's physical lineage.
pub(super) fn archived_request_bytes(
    directory: &Path,
    authority: &CommitBinding,
    embedding: &EmbeddingMetadata,
    limits: CandidateLimits,
) -> Result<Vec<u8>> {
    if !limits_valid(limits) {
        return Err(RuntimeError);
    }
    let authority_json = String::from_utf8(canonical(authority, MAX_REQUEST / 2)?).map_err(err)?;
    canonical(
        &SourceRequest {
            version: 1,
            directory: directory.into(),
            authority_json,
            embedding: embedding.clone(),
            limits,
        },
        MAX_REQUEST,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReport {
    pub version: u32,
    pub descriptor: RuntimeDescriptor,
    pub source: RuntimeSourceSnapshot,
}
impl SourceReport {
    /// Validate bounded canonical metadata, not its truth or permission to use
    /// a source. The host must compare it against its still-owned actual source.
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_REPORT {
            return Err(RuntimeError);
        }
        let report: Self = serde_json::from_slice(bytes).map_err(err)?;
        if report.version != 1
            || report.source.version != 2
            || canonical(&report, MAX_REPORT)? != bytes
        {
            return Err(RuntimeError);
        }
        RuntimeDescriptor::parse_metadata(&report.descriptor.canonical_bytes().map_err(err)?)
            .map_err(err)?;
        Ok(report)
    }
}
fn read_request(path: &Path) -> Result<SourceRequest> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !path.is_absolute()
        || !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != std::fs::metadata("/proc/self")?.uid()
        || metadata.mode() & 0o077 != 0
        || metadata.len() > MAX_REQUEST as u64
    {
        return Err(RuntimeError);
    }
    let mut bytes = vec![0; metadata.len() as usize];
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(RuntimeError);
    }
    let current = std::fs::symlink_metadata(path)?;
    if current.dev() != metadata.dev()
        || current.ino() != metadata.ino()
        || current.len() != metadata.len()
    {
        return Err(RuntimeError);
    }
    let request: SourceRequest = serde_json::from_slice(&bytes).map_err(err)?;
    if request.version != 1
        || canonical(&request, MAX_REQUEST)? != bytes
        || !limits_valid(request.limits)
        || !request.directory.is_absolute()
        || std::fs::canonicalize(&request.directory)? != request.directory
    {
        return Err(RuntimeError);
    }
    Ok(request)
}
/// Selected runtime entrypoint. Never opens original operational SQL, starts a
/// worker, repairs FTS or executes source effects. It requires its own compiled
/// storage/plan identities and loaded asset to match the complete actual source.
pub fn inspect_request(path: &Path, dictionary: Option<&Dictionary>) -> Result<Vec<u8>> {
    let request = read_request(path)?;
    let authority =
        CommitBinding::parse_retained(request.authority_json.as_bytes()).map_err(err)?;
    let descriptor = RuntimeDescriptor::current(dictionary).map_err(err)?;
    let lexical = lexical_binding::read(&request.directory).map_err(err)?;
    match (&lexical, dictionary) {
        (Some(binding), Some(dictionary)) if descriptor.describes_bound_source(binding) => {
            binding
                .validate(
                    &request.directory,
                    &request.embedding,
                    &authority,
                    &LexicalPlanIdentity::verified(dictionary).map_err(err)?,
                )
                .map_err(err)?;
        }
        (None, None) => {}
        _ => return Err(RuntimeError),
    }
    let source = inspect_runtime_source(
        &request.directory,
        &request.embedding,
        &authority,
        request.limits,
    )
    .map_err(err)?;
    if source.lexical != lexical {
        return Err(RuntimeError);
    }
    canonical(
        &SourceReport {
            version: 1,
            descriptor,
            source,
        },
        MAX_REPORT,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{commit::IndexCommitCoordinator, rollback_runtime::probe};
    use std::os::unix::fs::PermissionsExt;
    fn file(path: &Path, bytes: &[u8]) {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        f.write_all(bytes).unwrap();
    }
    fn private_directory() -> tempfile::TempDir {
        tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap()
    }
    #[test]
    fn actual_owned_source_is_verified_without_reopening_operational_sql_or_relabeling_the_plan() {
        let parent = private_directory();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        for (name, dictionary) in [("legacy", None), ("bound", Some(&dictionary))] {
            let root = parent.path().join(name);
            probe::create(&root, dictionary).unwrap();
            let (binding, embedding) = probe::fixture_configuration(dictionary.is_none()).unwrap();
            let c = IndexCommitCoordinator::open_existing(
                root.join("index"),
                root.join("state.sqlite"),
                binding,
            )
            .unwrap();
            let before = std::fs::read(root.join("state.sqlite")).unwrap();
            c.with_owned_offline_inspection(10, |owner| {
                let request = request_bytes(owner, &embedding, CandidateLimits::default()).unwrap();
                let path = parent.path().join(format!("{name}.json"));
                file(&path, &request);
                let report =
                    SourceReport::parse_metadata(&inspect_request(&path, dictionary).unwrap())
                        .unwrap();
                let expected = inspect_runtime_source(
                    owner.index_directory(),
                    &embedding,
                    owner.binding(),
                    CandidateLimits::default(),
                )
                .unwrap();
                assert_eq!(report.source, expected);
                assert_eq!(report.descriptor.is_legacy(), dictionary.is_none());
                if let Some(binding) = &report.source.lexical {
                    assert!(report.descriptor.describes_bound_source(binding));
                    let other = crate::lexical_dictionary::tests::dictionary(&[("river", "flod")]);
                    assert_eq!(inspect_request(&path, Some(&other)), Err(RuntimeError));
                    let metadata_path = root.join("index/.nk-lexical-binding-v1.json");
                    let original = std::fs::read(&metadata_path).unwrap();
                    for (section, field) in [
                        ("storage", "implementation_sha256"),
                        ("plan", "executor_sha256"),
                        ("plan", "importer_sha256"),
                        ("plan", "lock_sha256"),
                        ("plan", "asset_sha256"),
                    ] {
                        let mut json: serde_json::Value =
                            serde_json::from_slice(&original).unwrap();
                        json[section][field] = "0".repeat(64).into();
                        let changed: crate::chunks::lexical_binding::LexicalIndexBinding =
                            serde_json::from_value(json).unwrap();
                        let changed = changed.canonical_bytes().unwrap();
                        std::fs::write(&metadata_path, &changed).unwrap();
                        assert_eq!(
                            inspect_request(&path, dictionary),
                            Err(RuntimeError),
                            "wrong {section}/{field} must be refused"
                        );
                        assert_eq!(std::fs::read(&metadata_path).unwrap(), changed);
                        std::fs::write(&metadata_path, &original).unwrap();
                    }
                } else {
                    assert!(report.source.lexical.is_none());
                }
                let other = crate::lexical_dictionary::tests::dictionary(&[("river", "flod")]);
                let wrong_mode = if dictionary.is_some() {
                    None
                } else {
                    Some(&other)
                };
                assert_eq!(inspect_request(&path, wrong_mode), Err(RuntimeError));
                owner.revalidate()
            })
            .unwrap();
            assert_eq!(std::fs::read(root.join("state.sqlite")).unwrap(), before);
        }
    }
    #[test]
    fn source_snapshot_fences_changed_artifact_metadata_even_when_rows_and_bytes_match() {
        let parent = private_directory();
        let root = parent.path().join("source");
        probe::create(&root, None).unwrap();
        let (binding, embedding) = probe::fixture_configuration(true).unwrap();
        let c = IndexCommitCoordinator::open_existing(
            root.join("index"),
            root.join("state.sqlite"),
            binding,
        )
        .unwrap();
        c.with_owned_offline_inspection(10, |owner| {
            let before = inspect_runtime_source(
                owner.index_directory(),
                &embedding,
                owner.binding(),
                CandidateLimits::default(),
            )
            .unwrap();
            let path = root.join("index/.nk-operational-binding");
            let bytes = std::fs::read(&path).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            let modified =
                file.metadata().unwrap().modified().unwrap() + std::time::Duration::from_secs(1);
            file.set_modified(modified).unwrap();
            let after = inspect_runtime_source(
                owner.index_directory(),
                &embedding,
                owner.binding(),
                CandidateLimits::default(),
            )
            .unwrap();
            assert_eq!(before.version, 2);
            assert_eq!(before.rows, after.rows);
            assert_eq!(before.snapshot_sha256, after.snapshot_sha256);
            assert_eq!(before.source_version, after.source_version);
            assert_ne!(before.authority_sha256, after.authority_sha256);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            owner.revalidate()
        })
        .unwrap();
    }
    #[test]
    fn source_requests_refuse_nonprivate_aliased_oversized_and_noncanonical_metadata() {
        let parent = private_directory();
        let root = parent.path().join("source");
        probe::create(&root, None).unwrap();
        let (binding, embedding) = probe::fixture_configuration(true).unwrap();
        let c = IndexCommitCoordinator::open_existing(
            root.join("index"),
            root.join("state.sqlite"),
            binding,
        )
        .unwrap();
        c.with_owned_offline_inspection(10, |owner| {
            let request = request_bytes(owner, &embedding, CandidateLimits::default()).unwrap();
            for (i, bytes) in [vec![b'x'; MAX_REQUEST + 1], {
                let mut b = request.clone();
                b.push(b'\n');
                b
            }]
            .iter()
            .enumerate()
            {
                let path = parent.path().join(format!("bad-{i}.json"));
                file(&path, bytes);
                assert_eq!(inspect_request(&path, None), Err(RuntimeError));
            }
            let path = parent.path().join("private.json");
            file(&path, &request);
            let link = parent.path().join("symlink.json");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert_eq!(inspect_request(&link, None), Err(RuntimeError));
            let hard = parent.path().join("hard.json");
            std::fs::hard_link(&path, &hard).unwrap();
            assert_eq!(inspect_request(&path, None), Err(RuntimeError));
            std::fs::remove_file(hard).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(inspect_request(&path, None), Err(RuntimeError));
            let mut malformed: SourceRequest = serde_json::from_slice(&request).unwrap();
            malformed.version = 2;
            let path = parent.path().join("version.json");
            file(&path, &canonical(&malformed, MAX_REQUEST).unwrap());
            assert_eq!(inspect_request(&path, None), Err(RuntimeError));
            Ok(())
        })
        .unwrap();
    }
}
