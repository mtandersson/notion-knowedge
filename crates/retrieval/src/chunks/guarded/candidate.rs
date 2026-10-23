//! Explicit offline sibling preparation; no activation, rename, or source writes.
use super::*;
use crate::commit::OfflineBackup;
use lancedb::query::QueryExecutionOptions;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

const READY: &str = ".nk-lexical-candidate-ready-v1.json";

#[derive(Debug, Clone, Copy)]
pub struct CandidateLimits {
    pub files: usize,
    pub file_bytes: u64,
    pub rows: u64,
    pub batch_bytes: usize,
    pub identifier_bytes: usize,
}
impl Default for CandidateLimits {
    fn default() -> Self {
        Self {
            files: 100_000,
            file_bytes: 128 * 1024 * 1024 * 1024,
            rows: 1_000_000,
            batch_bytes: 16 * 1024 * 1024,
            identifier_bytes: 256 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreparedLexicalCandidate {
    pub version: u32,
    pub source_version: u64,
    pub rows: u64,
    pub snapshot_sha256: String,
    pub binding_sha256: String,
    pub backup: OfflineBackup,
}
fn bad() -> ChunkTableError {
    ChunkTableError::InvalidRows("offline lexical candidate verification failed; authoritative rebuild required for incompatible source".into())
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<(), ChunkTableError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn copy_tree(
    source: &Path,
    target: &Path,
    limits: CandidateLimits,
    files: &mut usize,
    bytes: &mut u64,
    depth: usize,
) -> Result<(), ChunkTableError> {
    if depth > 32 {
        return Err(bad());
    }
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        if depth == 0
            && [".nk-operational-binding", lexical_binding::FILE, READY]
                .iter()
                .any(|n| name == *n)
        {
            continue;
        }
        *files = files.checked_add(1).ok_or_else(bad)?;
        if *files > limits.files {
            return Err(bad());
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.is_dir() {
            let path = target.join(&name);
            fs::create_dir(&path)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
            copy_tree(&entry.path(), &path, limits, files, bytes, depth + 1)?;
        } else if metadata.is_file() && metadata.nlink() == 1 {
            *bytes = bytes.checked_add(metadata.len()).ok_or_else(bad)?;
            if *files > limits.files || *bytes > limits.file_bytes {
                return Err(bad());
            }
            let mut input = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(entry.path())?;
            let actual = input.metadata()?;
            if !actual.is_file()
                || actual.dev() != metadata.dev()
                || actual.ino() != metadata.ino()
                || actual.len() != metadata.len()
            {
                return Err(bad());
            }
            let mut output = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(target.join(name))?;
            // Fixed-size userspace copy, bounded by verified source size.
            let copied = std::io::copy(&mut (&mut input).take(metadata.len() + 1), &mut output)?;
            if copied != metadata.len() {
                return Err(bad());
            }
            output.sync_all()?;
        } else {
            return Err(bad());
        }
    }
    fs::File::open(target)?.sync_all()?;
    Ok(())
}
// Order-independent multiset digest: two 256-bit accumulators plus row count.
// Each row hashes exact typed field bytes, including f32 bit patterns and JSON
// source strings; scan batching/order cannot disguise altered vectors/citations.
#[derive(Default)]
struct SnapshotDigest {
    xor: [u8; 32],
    sum: [u8; 32],
    rows: u64,
}
impl SnapshotDigest {
    fn row(&mut self, batch: &RecordBatch, row: usize) -> Result<(), ChunkTableError> {
        let mut hash = Sha256::new();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            hash.update((field.name().len() as u64).to_le_bytes());
            hash.update(field.name().as_bytes());
            if column.is_null(row) {
                if !field.is_nullable() {
                    return Err(bad());
                }
                hash.update([0]);
                continue;
            }
            hash.update([1]);
            if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                let value = strings.value(row).as_bytes();
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value);
            } else if let Some(vectors) = column.as_any().downcast_ref::<FixedSizeListArray>() {
                let values = vectors.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(bad)?;
                if values.null_count() != 0 {
                    return Err(bad());
                }
                hash.update((values.len() as u64).to_le_bytes());
                for value in values.values() {
                    if !value.is_finite() {
                        return Err(bad());
                    }
                    hash.update(value.to_bits().to_le_bytes());
                }
            } else {
                return Err(bad());
            }
        }
        let row_hash = hash.finalize();
        let mut carry = 0u16;
        for (i, value) in row_hash.iter().enumerate() {
            self.xor[i] ^= value;
            let sum = self.sum[i] as u16 + *value as u16 + carry;
            self.sum[i] = sum as u8;
            carry = sum >> 8;
        }
        self.rows = self.rows.checked_add(1).ok_or_else(bad)?;
        Ok(())
    }
    fn finish(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"nk-canonical-row-multiset-v1");
        hash.update(self.rows.to_le_bytes());
        hash.update(self.xor);
        hash.update(self.sum);
        format!("{:x}", hash.finalize())
    }
}
async fn verify_rows(
    table: &LanceChunkTable,
    authority: &crate::commit::CommitBinding,
    limits: CandidateLimits,
    probe: bool,
) -> Result<SnapshotDigest, ChunkTableError> {
    let mut options = QueryExecutionOptions::default();
    options.max_batch_length = 64;
    let mut stream = table.table.query().execute_with_options(options).await?;
    let mut digest = SnapshotDigest::default();
    let mut ids = HashSet::new();
    let mut id_bytes = 0usize;
    while let Some(batch) = stream.try_next().await? {
        if batch.get_array_memory_size() > limits.batch_bytes
            || digest
                .rows
                .checked_add(batch.num_rows() as u64)
                .is_none_or(|rows| rows > limits.rows)
        {
            return Err(bad());
        }
        // Bound retained IDs before decode/string allocation.
        let chunk_ids = string_column(&batch, "chunk_id")?;
        for id in chunk_ids.iter() {
            let id = id.ok_or_else(bad)?;
            id_bytes = id_bytes.checked_add(id.len() + 128).ok_or_else(bad)?;
            if id_bytes > limits.identifier_bytes || !ids.insert(id.to_owned()) {
                return Err(bad());
            }
        }
        for (row_index, row) in decode_stored_chunks(std::slice::from_ref(&batch))?
            .into_iter()
            .enumerate()
        {
            if row.workspace_id != authority.workspace()
                || !authority.roots().contains(&row.root_page_id)
                || row.chunk_id.trim().is_empty()
                || row.page_id.trim().is_empty()
                || row.content_hash.trim().is_empty()
                || row.text.trim().is_empty()
                || row.vector.len() != table.embedding.dimension()
            {
                return Err(bad());
            }
            if probe {
                for (field, id) in [("page_id", &row.page_id), ("chunk_id", &row.chunk_id)] {
                    let hits = table.fts_query(field, id, 1).await?;
                    if !hits.iter().any(|hit| {
                        if field == "page_id" {
                            hit.page_id == *id
                        } else {
                            hit.chunk_id == *id
                        }
                    }) {
                        return Err(bad());
                    }
                }
            }
            digest.row(&batch, row_index)?;
        }
    }
    if digest.rows != table.table.count_rows(None).await? as u64 {
        return Err(bad());
    }
    Ok(digest)
}
impl GuardedChunkTable {
    /// Run outside a Tokio runtime, after stopping the serving process. Retains
    /// verified operational backup and old index; failed siblings have no READY
    /// marker and cannot be activated. This never changes current authority.
    pub fn prepare_lexical_candidate(
        &self,
        candidate: &Path,
        backup: &Path,
        next: &crate::commit::CommitBinding,
        dictionary: &crate::lexical_dictionary::Dictionary,
        now: i64,
        limits: CandidateLimits,
    ) -> Result<PreparedLexicalCandidate, CommitError> {
        if tokio::runtime::Handle::try_current().is_ok()
            || limits.files == 0
            || limits.file_bytes == 0
            || limits.rows == 0
            || limits.batch_bytes == 0
            || limits.identifier_bytes == 0
        {
            return Err(CommitError::InvalidInput);
        }
        let old = self.coordinator.binding();
        if next.table() != old.table()
            || next.workspace() != old.workspace()
            || next.roots() != old.roots()
            || next.scope() != old.scope()
            || next.epoch() <= old.epoch()
            || next.generation() == old.generation()
        {
            return Err(CommitError::BindingMismatch);
        }
        let parent = candidate.parent().ok_or(CommitError::InvalidInput)?;
        if !candidate.is_absolute()
            || candidate.file_name().is_none()
            || candidate.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || fs::canonicalize(parent).map_err(|_| CommitError::InvalidInput)?
                != self
                    .index_directory
                    .parent()
                    .ok_or(CommitError::InvalidInput)?
        {
            return Err(CommitError::InvalidInput);
        }
        let parent_meta = fs::symlink_metadata(parent).map_err(|_| CommitError::InvalidInput)?;
        if !parent_meta.is_dir()
            || parent_meta.mode() & 0o077 != 0
            || parent_meta.uid()
                != fs::metadata("/proc/self")
                    .map_err(|_| CommitError::Unavailable)?
                    .uid()
        {
            return Err(CommitError::InvalidInput);
        }
        let plan =
            LexicalPlanIdentity::verified(dictionary).map_err(|_| CommitError::BindingMismatch)?;
        self.coordinator
            .with_offline_backup(backup, now, |verified| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| CommitError::Unavailable)?;
                runtime
                    .block_on(async {
                        let source = open_current(
                            &self.index_directory,
                            &self.table_name,
                            &self.embedding,
                            &self.lexical_binding,
                        )
                        .await?
                        .read_snapshot()
                        .await?;
                        let source_version = source.table.version().await?;
                        let before = verify_rows(&source, old, limits, false).await?;
                        fs::create_dir(candidate)?;
                        fs::set_permissions(candidate, fs::Permissions::from_mode(0o700))?;
                        copy_tree(&self.index_directory, candidate, limits, &mut 0, &mut 0, 0)?;
                        let copied =
                            open_current(candidate, &self.table_name, &self.embedding, &None)
                                .await?;
                        let copied_digest = verify_rows(&copied, old, limits, false).await?;
                        if before.finish() != copied_digest.finish() {
                            return Err(bad());
                        }
                        copied
                            .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
                            .await?;
                        let reopened =
                            open_current(candidate, &self.table_name, &self.embedding, &None)
                                .await?;
                        reopened.validate_vector_index_layout().await?;
                        let after = verify_rows(&reopened, old, limits, true).await?;
                        if before.finish() != after.finish() {
                            return Err(bad());
                        }
                        let binding = LexicalIndexBinding::for_candidate(
                            candidate,
                            self.embedding.clone(),
                            next,
                            plan,
                            &before.finish(),
                        )?;
                        let binding_bytes = binding.canonical_bytes()?;
                        let ready = PreparedLexicalCandidate {
                            version: 1,
                            source_version,
                            rows: before.rows,
                            snapshot_sha256: before.finish(),
                            binding_sha256: format!("{:x}", Sha256::digest(&binding_bytes)),
                            backup: verified.clone(),
                        };
                        write_new(&candidate.join(lexical_binding::FILE), &binding_bytes)?;
                        lexical_binding::verify(candidate, &Some(binding))?;
                        write_new(&candidate.join(READY), &serde_json::to_vec(&ready)?)?;
                        fs::File::open(candidate)?.sync_all()?;
                        Ok::<_, ChunkTableError>(ready)
                    })
                    .map_err(|e| CommitError::Operation(failure(e)))
            })
    }
}
