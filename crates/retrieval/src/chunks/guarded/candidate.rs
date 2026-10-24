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
    pub source_authority_sha256: String,
    pub rows: u64,
    pub snapshot_sha256: String,
    pub binding_sha256: String,
    pub backup: OfflineBackup,
}

#[derive(Clone, Copy)]
pub struct TransitionCandidateInputs<'a> {
    pub source: &'a crate::commit::CommitBinding,
    pub target: &'a crate::commit::CommitBinding,
    pub embedding: &'a EmbeddingMetadata,
    pub dictionary: &'a crate::lexical_dictionary::Dictionary,
    pub preparation_backup: &'a Path,
    pub limits: CandidateLimits,
}

/// Full native verification evidence pinned to retained transition ownership.
/// This does not authorize serving or publish operational authority.
/// Publication must reverify inside its actual publishing transaction; this
/// value records a verification moment rather than permanent SQL freshness.
pub struct VerifiedTransitionCandidate<'owner> {
    prepared: PreparedLexicalCandidate,
    target_version: u64,
    record: crate::commit::transition::TransitionRecord,
    _owner: &'owner crate::commit::transition::TransitionOwnership,
}
impl VerifiedTransitionCandidate<'_> {
    pub fn prepared(&self) -> &PreparedLexicalCandidate {
        &self.prepared
    }
    pub fn target_version(&self) -> u64 {
        self.target_version
    }
    pub fn record(&self) -> &crate::commit::transition::TransitionRecord {
        &self.record
    }
}

/// Verify a physical pair within the owner's existing SQL transaction, both
/// before quarantine and after installation/recovery.
/// No coordinator admission, schema repair, anchor creation or source effects.
pub fn verify_transition_candidate<'owner>(
    owner: &'owner crate::commit::transition::TransitionOwnership,
    transaction: &rusqlite::Transaction<'_>,
    input: TransitionCandidateInputs<'_>,
) -> Result<VerifiedTransitionCandidate<'owner>, CommitError> {
    use crate::commit::transition::{TransitionPhase, TransitionRecord};
    if tokio::runtime::Handle::try_current().is_ok()
        || input.limits.files == 0
        || input.limits.file_bytes == 0
        || input.limits.rows == 0
        || input.limits.batch_bytes == 0
        || input.limits.identifier_bytes == 0
    {
        return Err(CommitError::InvalidInput);
    }
    owner.validate_owned_transaction(transaction)?;
    let record = TransitionRecord::read(Path::new(
        transaction.path().ok_or(CommitError::BindingMismatch)?,
    ))?
    .ok_or(CommitError::BindingMismatch)?;
    let mut expected = owner.record().clone();
    expected.phase = record.phase;
    if expected != record
        || record.phase == TransitionPhase::Complete
        || serde_json::to_string(input.source).map_err(|_| CommitError::InvalidInput)?
            != record.source.binding
        || serde_json::to_string(input.target).map_err(|_| CommitError::InvalidInput)?
            != record.target.binding
        || input.dictionary.manifest_digest() != record.asset_sha256
    {
        return Err(CommitError::BindingMismatch);
    }
    let (source, target) = owner.physical_paths()?;
    let paths_valid = match record.phase {
        TransitionPhase::Prepared => {
            source == record.source.path && target == record.candidate_path
        }
        TransitionPhase::Quarantined => true, // Locations resolved by retained inodes.
        TransitionPhase::SourceRetained => source == record.archive_path,
        TransitionPhase::TargetInstalled | TransitionPhase::Published => {
            source == record.archive_path && target == record.target.path
        }
        TransitionPhase::Complete => false,
    };
    if !paths_valid {
        return Err(CommitError::BindingMismatch);
    }
    record.backup.verify(&record.backup_path)?;
    let (ready, bytes) =
        read_ready(&target).map_err(|error| CommitError::Operation(failure(error)))?;
    if format!("{:x}", Sha256::digest(&bytes)) != record.ready_sha256
        || ready.snapshot_sha256 != record.snapshot_sha256
    {
        return Err(CommitError::BindingMismatch);
    }
    let source_lexical =
        lexical_binding::read(&source).map_err(|error| CommitError::Operation(failure(error)))?;
    let plan = LexicalPlanIdentity::verified(input.dictionary)
        .map_err(|_| CommitError::BindingMismatch)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| CommitError::Unavailable)?;
    let (prepared, target_version) = runtime
        .block_on(verify_candidate_owned(CandidateVerification {
            source_directory: &source,
            candidate: &target,
            backup: input.preparation_backup,
            source_binding: input.source,
            target_binding: input.target,
            embedding: input.embedding,
            source_lexical: &source_lexical,
            plan: &plan,
            limits: input.limits,
        }))
        .map_err(|error| CommitError::Operation(failure(error)))?;
    if prepared != ready
        || read_ready(&target)
            .map_err(|error| CommitError::Operation(failure(error)))?
            .1
            != bytes
    {
        return Err(CommitError::BindingMismatch);
    }
    record.backup.verify(&record.backup_path)?;
    owner.validate_owned_transaction(transaction)?;
    if TransitionRecord::read(Path::new(
        transaction.path().ok_or(CommitError::BindingMismatch)?,
    ))?
    .as_ref()
        != Some(&record)
    {
        return Err(CommitError::BindingMismatch);
    }
    Ok(VerifiedTransitionCandidate {
        prepared,
        target_version,
        record,
        _owner: owner,
    })
}
/// Source-only offline handle. It cannot search, embed, prepare page writes, or
/// commit; older plan binaries/assets are never executed or relabeled by it.
pub struct OfflineLexicalSource {
    table: GuardedChunkTable,
}
impl OfflineLexicalSource {
    pub fn bind(
        directory: &Path,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
    ) -> Result<Self, ChunkTableError> {
        validate_table_name(table_name)?;
        let directory = fs::canonicalize(directory)?;
        if directory != coordinator.index_directory() || table_name != coordinator.binding().table()
        {
            return Err(bad());
        }
        coordinator.identities().map_err(|_| bad())?;
        chunk_schema(&embedding)?;
        let lexical_binding = lexical_binding::read(&directory)?;
        if let Some(binding) = &lexical_binding {
            binding.validate_offline_source(&directory, &embedding, coordinator.binding())?;
        }
        coordinator.identities().map_err(|_| bad())?;
        Ok(Self {
            table: GuardedChunkTable {
                index_directory: directory,
                table_name: table_name.into(),
                embedding,
                coordinator,
                lexical_binding,
            },
        })
    }
    pub fn prepare(
        &self,
        candidate: &Path,
        backup: &Path,
        next: &crate::commit::CommitBinding,
        dictionary: &crate::lexical_dictionary::Dictionary,
        now: i64,
        limits: CandidateLimits,
    ) -> Result<PreparedLexicalCandidate, CommitError> {
        self.table
            .prepare_lexical_candidate(candidate, backup, next, dictionary, now, limits)
    }
    pub fn verify(
        &self,
        candidate: &Path,
        backup: &Path,
        next: &crate::commit::CommitBinding,
        dictionary: &crate::lexical_dictionary::Dictionary,
        now: i64,
        limits: CandidateLimits,
    ) -> Result<PreparedLexicalCandidate, CommitError> {
        self.table
            .verify_lexical_candidate(candidate, backup, next, dictionary, now, limits)
    }
}
// Child-process crash observations exist only in unit-test binaries. Production
// builds have no environment switch, pause, or interruption injection.
#[cfg(test)]
fn preparation_checkpoint(candidate: &Path, phase: &str) {
    let Some(root) = std::env::var_os("NK_LEXICAL_PREP_TEST_ROOT").map(PathBuf::from) else {
        return;
    };
    if candidate.parent() != Some(root.as_path())
        || std::env::var("NK_LEXICAL_PREP_TEST_PHASE").ok().as_deref() != Some(phase)
    {
        return;
    }
    fs::write(root.join("observed-phase"), phase).unwrap();
    loop {
        std::thread::park();
    }
}
fn bad() -> ChunkTableError {
    ChunkTableError::InvalidRows("offline lexical candidate verification failed; authoritative rebuild required for incompatible source".into())
}
fn source_authority(table: &GuardedChunkTable) -> Result<String, ChunkTableError> {
    source_authority_at(&table.index_directory, table.coordinator.binding())
}
fn source_authority_at(
    directory: &Path,
    binding: &crate::commit::CommitBinding,
) -> Result<String, ChunkTableError> {
    let metadata = fs::metadata(directory)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(
            binding,
            metadata.dev(),
            metadata.ino()
        ))?)
    ))
}

fn read_ready(candidate: &Path) -> Result<(PreparedLexicalCandidate, Vec<u8>), ChunkTableError> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(candidate.join(READY))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > 32768 {
        return Err(bad());
    }
    let mut bytes = Vec::new();
    file.take(32769).read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err(bad());
    }
    let ready: PreparedLexicalCandidate = serde_json::from_slice(&bytes)?;
    if ready.version != 1 || ready.source_version == 0 || serde_json::to_vec(&ready)? != bytes {
        return Err(bad());
    }
    Ok((ready, bytes))
}
async fn verify_profile(table: &LanceChunkTable) -> Result<(), ChunkTableError> {
    let indices = table.table.list_indices().await?;
    for (name, column) in CHUNK_FTS_INDEXES {
        if !indices.iter().any(|index| {
            index.name == name
                && index.index_type == IndexType::FTS
                && index.columns == vec![column.to_string()]
        }) {
            return Err(bad());
        }
    }
    for (name, column) in CHUNK_FTS_INDEXES {
        let index = indices
            .iter()
            .find(|index| index.name == name)
            .ok_or_else(bad)?;
        let details = index.index_details.as_deref().ok_or_else(bad)?;
        if details.len() > 16384 {
            return Err(bad());
        }
        let actual: serde_json::Value = serde_json::from_str(details)?;
        let mut expected = serde_json::to_value(fts_parameters(
            column,
            &FtsIndexConfig::lexical_expansion_v1(),
        )?)?;
        // Native scalar-string indices resolve document extraction to text.
        // The stored physical settings omit logical row granularity, which is
        // fixed by our exact scalar Utf8 schema (never a list/JSON document).
        expected
            .as_object_mut()
            .ok_or_else(bad)?
            .insert("lance_tokenizer".into(), serde_json::json!("text"));
        if actual != expected {
            return Err(bad());
        }
    }
    table.validate_vector_index_layout().await
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
fn sync_tree(
    path: &Path,
    limits: CandidateLimits,
    entries: &mut usize,
    bytes: &mut u64,
    depth: usize,
) -> Result<(), ChunkTableError> {
    if depth > 32 {
        return Err(bad());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        *entries = entries.checked_add(1).ok_or_else(bad)?;
        if *entries > limits.files {
            return Err(bad());
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.is_dir() {
            sync_tree(&entry.path(), limits, entries, bytes, depth + 1)?;
        } else if metadata.is_file() && metadata.nlink() == 1 {
            *bytes = bytes.checked_add(metadata.len()).ok_or_else(bad)?;
            if *bytes > limits.file_bytes {
                return Err(bad());
            }
            let file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(entry.path())?;
            let actual = file.metadata()?;
            if !actual.is_file() || actual.dev() != metadata.dev() || actual.ino() != metadata.ino()
            {
                return Err(bad());
            }
            file.sync_all()?;
        } else {
            return Err(bad());
        }
    }
    fs::File::open(path)?.sync_all()?;
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
pub(super) async fn preflight_rows(
    table: &LanceChunkTable,
    limits: CandidateLimits,
) -> Result<u64, ChunkTableError> {
    const BATCH_ROWS: usize = 64;
    let row_budget = limits.batch_bytes.checked_div(BATCH_ROWS).ok_or_else(bad)?;
    let fixed = table
        .embedding
        .dimension()
        .checked_mul(4)
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(bad)?;
    if fixed > row_budget {
        return Err(bad());
    }
    let schema = chunk_schema(&table.embedding)?;
    let lengths = schema
        .fields()
        .iter()
        .filter(|field| field.data_type() == &DataType::Utf8)
        .map(|field| format!("coalesce(CAST(char_length({}) AS BIGINT),0)", field.name()))
        .collect::<Vec<_>>()
        .join(" + ");
    let nulls = schema
        .fields()
        .iter()
        .filter(|field| !field.is_nullable())
        .map(|field| format!("{} IS NULL", field.name()))
        .collect::<Vec<_>>()
        .join(" OR ");
    let expression = format!("({nulls}) OR ((({lengths}) * 4 + {fixed}) > {row_budget})");
    let mut options = QueryExecutionOptions::default();
    options.max_batch_length = 64;
    let count_limit =
        usize::try_from(limits.rows.checked_add(1).ok_or_else(bad)?).map_err(|_| bad())?;
    let mut stream = table
        .table
        .query()
        .select(Select::Dynamic(vec![("_oversized".into(), expression)]))
        .limit(count_limit)
        .execute_with_options(options)
        .await?;
    let mut count = 0u64;
    while let Some(batch) = stream.try_next().await? {
        count = count.checked_add(batch.num_rows() as u64).ok_or_else(bad)?;
        if count > limits.rows {
            return Err(bad());
        }
        let overflow = batch
            .column_by_name("_oversized")
            .and_then(|column| column.as_any().downcast_ref::<arrow_array::BooleanArray>())
            .ok_or_else(bad)?;
        if overflow.null_count() != 0 || overflow.iter().flatten().any(|value| value) {
            return Err(ChunkTableError::InvalidRows(
                "offline row exceeds preflight materialization budget".into(),
            ));
        }
    }
    Ok(count)
}
async fn verify_rows(
    table: &LanceChunkTable,
    authority: &crate::commit::CommitBinding,
    limits: CandidateLimits,
    probe: bool,
) -> Result<SnapshotDigest, ChunkTableError> {
    let preflight_count = preflight_rows(table, limits).await?;
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
    if digest.rows != preflight_count || digest.rows != table.table.count_rows(None).await? as u64 {
        return Err(bad());
    }
    Ok(digest)
}
struct CandidateVerification<'a> {
    source_directory: &'a Path,
    candidate: &'a Path,
    backup: &'a Path,
    source_binding: &'a crate::commit::CommitBinding,
    target_binding: &'a crate::commit::CommitBinding,
    embedding: &'a EmbeddingMetadata,
    source_lexical: &'a Option<LexicalIndexBinding>,
    plan: &'a LexicalPlanIdentity,
    limits: CandidateLimits,
}
async fn verify_candidate_owned(
    input: CandidateVerification<'_>,
) -> Result<(PreparedLexicalCandidate, u64), ChunkTableError> {
    if let Some(binding) = input.source_lexical {
        binding.validate_offline_source(
            input.source_directory,
            input.embedding,
            input.source_binding,
        )?;
    }
    let (ready, ready_bytes) = read_ready(input.candidate)?;
    if ready.source_authority_sha256
        != source_authority_at(input.source_directory, input.source_binding)?
    {
        return Err(bad());
    }
    ready.backup.verify(input.backup).map_err(|_| bad())?;
    let binding = lexical_binding::read(input.candidate)?.ok_or_else(bad)?;
    binding.validate(
        input.candidate,
        input.embedding,
        input.target_binding,
        input.plan,
    )?;
    if format!("{:x}", Sha256::digest(binding.canonical_bytes()?)) != ready.binding_sha256 {
        return Err(bad());
    }
    let source = open_current(
        input.source_directory,
        input.source_binding.table(),
        input.embedding,
        input.source_lexical,
    )
    .await?
    .read_snapshot()
    .await?;
    if source.table.version().await? != ready.source_version {
        return Err(bad());
    }
    let old = verify_rows(&source, input.source_binding, input.limits, false).await?;
    let reopened = open_current(
        input.candidate,
        input.source_binding.table(),
        input.embedding,
        &Some(binding.clone()),
    )
    .await?
    .read_snapshot()
    .await?;
    let version = reopened.table.version().await?;
    verify_profile(&reopened).await?;
    let actual = verify_rows(&reopened, input.target_binding, input.limits, true).await?;
    if actual.rows != ready.rows
        || actual.finish() != ready.snapshot_sha256
        || old.finish() != ready.snapshot_sha256
    {
        return Err(bad());
    }
    lexical_binding::verify(input.candidate, &Some(binding.clone()))?;
    let fresh = open_current(
        input.candidate,
        input.source_binding.table(),
        input.embedding,
        &Some(binding),
    )
    .await?;
    if fresh.table.version().await? != version || read_ready(input.candidate)?.1 != ready_bytes {
        return Err(bad());
    }
    ready.backup.verify(input.backup).map_err(|_| bad())?;
    let fresh_source = open_current(
        input.source_directory,
        input.source_binding.table(),
        input.embedding,
        input.source_lexical,
    )
    .await?;
    if fresh_source.table.version().await? != ready.source_version {
        return Err(bad());
    }
    Ok((ready, version))
}

impl GuardedChunkTable {
    /// Reopen a completed sibling without repairs, under offline source
    /// ownership. A returned manifest is evidence only while that ownership and
    /// candidate immutability persist; activation must revalidate under its fence.
    pub fn verify_lexical_candidate(
        &self,
        candidate: &Path,
        backup: &Path,
        next: &crate::commit::CommitBinding,
        dictionary: &crate::lexical_dictionary::Dictionary,
        now: i64,
        limits: CandidateLimits,
    ) -> Result<PreparedLexicalCandidate, CommitError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(CommitError::InvalidInput);
        }
        let plan =
            LexicalPlanIdentity::verified(dictionary).map_err(|_| CommitError::BindingMismatch)?;
        self.coordinator.with_offline_inspection(now, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| CommitError::Unavailable)?;
            runtime
                .block_on(async {
                    verify_candidate_owned(CandidateVerification {
                        source_directory: &self.index_directory,
                        candidate,
                        backup,
                        source_binding: self.coordinator.binding(),
                        target_binding: next,
                        embedding: &self.embedding,
                        source_lexical: &self.lexical_binding,
                        plan: &plan,
                        limits,
                    })
                    .await
                    .map(|(ready, _)| ready)
                })
                .map_err(|e| CommitError::Operation(failure(e)))
        })
    }
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
            || backup.starts_with(candidate)
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
                        #[cfg(test)]
                        preparation_checkpoint(candidate, "before-copy");
                        fs::create_dir(candidate)?;
                        fs::set_permissions(candidate, fs::Permissions::from_mode(0o700))?;
                        copy_tree(&self.index_directory, candidate, limits, &mut 0, &mut 0, 0)?;
                        #[cfg(test)]
                        preparation_checkpoint(candidate, "after-copy");
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
                        verify_profile(&reopened).await?;
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
                            source_authority_sha256: source_authority(self)?,
                            rows: before.rows,
                            snapshot_sha256: before.finish(),
                            binding_sha256: format!("{:x}", Sha256::digest(&binding_bytes)),
                            backup: verified.clone(),
                        };
                        sync_tree(candidate, limits, &mut 0, &mut 0, 0)?;
                        self.coordinator.identities().map_err(|_| bad())?;
                        lexical_binding::verify(&self.index_directory, &self.lexical_binding)?;
                        write_new(&candidate.join(lexical_binding::FILE), &binding_bytes)?;
                        lexical_binding::verify(candidate, &Some(binding))?;
                        #[cfg(test)]
                        preparation_checkpoint(candidate, "before-ready");
                        write_new(&candidate.join(READY), &serde_json::to_vec(&ready)?)?;
                        fs::File::open(candidate)?.sync_all()?;
                        fs::File::open(candidate.parent().ok_or_else(bad)?)?.sync_all()?;
                        Ok::<_, ChunkTableError>(ready)
                    })
                    .map_err(|e| CommitError::Operation(failure(e)))
            })
    }
}
