//! Private canonical rows, never serialized ownership or candidate authority.
use super::*;
use crate::{
    commit::OwnedOfflineInspection,
    rollback_runtime::{
        evidence::{check_parent, parent},
        source::canonical,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::FileExt,
};
const MAGIC: &[u8; 8] = b"NKROWS\0\x01";
const HEADER_BYTES: usize = 65536;
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferLimits {
    pub native: CandidateLimits,
    pub sql_bytes: u64,
    pub bytes: u64,
}
impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            native: CandidateLimits::default(),
            sql_bytes: 64 << 20,
            bytes: 1 << 30,
        }
    }
}
impl TransferLimits {
    fn validate(self, embedding: &EmbeddingMetadata) -> Result<(), ChunkTableError> {
        let max = CandidateLimits::default();
        if self.sql_bytes == 0
            || self.bytes == 0
            || self.bytes > max.file_bytes
            || self.native.files == 0
            || self.native.files > max.files
            || self.native.file_bytes == 0
            || self.native.file_bytes > max.file_bytes
            || self.native.rows == 0
            || self.native.rows > max.rows
            || self.native.batch_bytes > max.batch_bytes
            || self.native.identifier_bytes == 0
            || self.native.identifier_bytes > max.identifier_bytes
            || embedding
                .dimension()
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .is_none_or(|n| n > self.native.batch_bytes / 64)
        {
            return Err(bad());
        }
        Ok(())
    }
    fn frame(self) -> usize {
        self.native.batch_bytes / 8
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferMetadata {
    pub version: u32,
    pub canonical_schema: String,
    pub embedding: EmbeddingMetadata,
    pub authority_json: String,
    pub ownership_sha256: String,
    pub operational: OfflineBackup,
    pub source: RuntimeSourceSnapshot,
}
/// Descriptive metadata only. Reuse under another operation requires fresh
/// source/SQL ownership and comparison; deserialization grants no authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferRecord {
    pub metadata: TransferMetadata,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    strings: [Option<String>; 16],
    #[serde(deserialize_with = "vector_bits")]
    vector_bits: Vec<u32>,
}
fn vector_bits<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u32>, D::Error> {
    struct Bits;
    impl<'de> serde::de::Visitor<'de> for Bits {
        type Value = Vec<u32>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded exact f32 bits")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u32>, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element()? {
                if values.len() == 65536 {
                    return Err(serde::de::Error::custom("vector exceeds bound"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    d.deserialize_seq(Bits)
}
fn json<T: Serialize>(v: &T, cap: usize) -> Result<Vec<u8>, ChunkTableError> {
    canonical(v, cap).map_err(|_| bad())
}
fn frame(out: &mut impl Write, value: &impl Serialize, cap: usize) -> Result<(), ChunkTableError> {
    let bytes = json(value, cap)?;
    out.write_all(&u32::try_from(bytes.len()).map_err(|_| bad())?.to_le_bytes())?;
    out.write_all(&bytes)?;
    Ok(())
}
fn read_frame<T: serde::de::DeserializeOwned + Serialize>(
    input: &mut impl Read,
    cap: usize,
) -> Result<T, ChunkTableError> {
    let mut size = [0; 4];
    input.read_exact(&mut size)?;
    let size = u32::from_le_bytes(size) as usize;
    if size > cap {
        return Err(bad());
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes)?;
    let value = serde_json::from_slice(&bytes)?;
    if json(&value, cap)? != bytes {
        return Err(bad());
    }
    Ok(value)
}
struct Output {
    file: File,
    hash: Sha256,
    bytes: u64,
    max: u64,
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > self.max)
        {
            return Err(std::io::Error::other("canonical transfer exceeds bound"));
        }
        let n = self.file.write(bytes)?;
        self.hash.update(&bytes[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
fn row(batch: &RecordBatch, index: usize) -> Result<Row, ChunkTableError> {
    let mut strings = std::array::from_fn(|_| None);
    for (i, value) in strings.iter_mut().enumerate() {
        let column = batch
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(bad)?;
        if !column.is_null(index) {
            *value = Some(column.value(index).into());
        }
    }
    let vectors = batch
        .column(16)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(bad)?
        .value(index);
    let values = vectors
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(bad)?;
    Ok(Row {
        strings,
        vector_bits: values.values().iter().map(|v| v.to_bits()).collect(),
    })
}
fn batch(row: Row, embedding: &EmbeddingMetadata) -> Result<RecordBatch, ChunkTableError> {
    if row.vector_bits.len() != embedding.dimension() {
        return Err(bad());
    }
    let mut columns: Vec<arrow_array::ArrayRef> = row
        .strings
        .iter()
        .map(|s| Arc::new(StringArray::from_iter([s.as_deref()])) as arrow_array::ArrayRef)
        .collect();
    columns.push(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        i32::try_from(embedding.dimension()).map_err(|_| bad())?,
        Arc::new(Float32Array::from(
            row.vector_bits
                .into_iter()
                .map(f32::from_bits)
                .collect::<Vec<_>>(),
        )),
        None,
    )?));
    Ok(RecordBatch::try_new(chunk_schema(embedding)?, columns)?)
}
fn same_file(file: &File, path: &Path, before: &fs::Metadata) -> Result<(), ChunkTableError> {
    for m in [file.metadata()?, fs::symlink_metadata(path)?] {
        if !m.is_file()
            || m.dev() != before.dev()
            || m.ino() != before.ino()
            || m.len() != before.len()
            || m.nlink() != 1
            || m.uid() != before.uid()
            || m.mode() != before.mode()
            || m.ctime() != before.ctime()
            || m.ctime_nsec() != before.ctime_nsec()
        {
            return Err(bad());
        }
    }
    Ok(())
}
/// Export only while the caller retains original SQL/index ownership. A failed
/// or interrupted explicit output remains diagnostic and cannot grant authority.
/// All source processes must remain stopped. No prior target plan is executed.
pub fn export(
    owner: &OwnedOfflineInspection<'_>,
    path: &Path,
    embedding: &EmbeddingMetadata,
    limits: TransferLimits,
) -> Result<TransferRecord, ChunkTableError> {
    export_owned(owner, path, embedding, limits)
}
/// Capture from an actually verified quarantined source; this never restores
/// serving admission for the revoked source generation.
pub fn export_quarantined(
    owner: &crate::commit::transition::source::QuarantinedSourceInspection<'_>,
    path: &Path,
    embedding: &EmbeddingMetadata,
    limits: TransferLimits,
) -> Result<TransferRecord, ChunkTableError> {
    if owner.embedding() != embedding {
        return Err(bad());
    }
    export_owned(owner, path, embedding, limits)
}
trait TransferSource {
    fn binding(&self) -> &crate::commit::CommitBinding;
    fn index_directory(&self) -> &Path;
    fn revalidate(&self) -> Result<(), crate::commit::CommitError>;
    fn authority_sha256(&self) -> Result<String, crate::commit::CommitError>;
    fn state_snapshot(&self, bytes: u64) -> Result<OfflineBackup, crate::commit::CommitError>;
    fn allows_output(&self, path: &Path) -> bool;
}
impl TransferSource for OwnedOfflineInspection<'_> {
    fn binding(&self) -> &crate::commit::CommitBinding {
        self.binding()
    }
    fn index_directory(&self) -> &Path {
        self.index_directory()
    }
    fn revalidate(&self) -> Result<(), crate::commit::CommitError> {
        self.revalidate()
    }
    fn authority_sha256(&self) -> Result<String, crate::commit::CommitError> {
        self.authority_sha256()
    }
    fn state_snapshot(&self, bytes: u64) -> Result<OfflineBackup, crate::commit::CommitError> {
        self.state_snapshot(bytes)
    }
    fn allows_output(&self, path: &Path) -> bool {
        !path
            .parent()
            .is_none_or(|p| p.starts_with(self.index_directory()))
    }
}
impl TransferSource for crate::commit::transition::source::QuarantinedSourceInspection<'_> {
    fn binding(&self) -> &crate::commit::CommitBinding {
        self.binding()
    }
    fn index_directory(&self) -> &Path {
        self.index_directory()
    }
    fn revalidate(&self) -> Result<(), crate::commit::CommitError> {
        self.revalidate()
    }
    fn authority_sha256(&self) -> Result<String, crate::commit::CommitError> {
        self.authority_sha256()
    }
    fn state_snapshot(&self, bytes: u64) -> Result<OfflineBackup, crate::commit::CommitError> {
        self.state_snapshot(bytes)
    }
    fn allows_output(&self, path: &Path) -> bool {
        self.allows_output(path)
    }
}
fn export_owned(
    owner: &impl TransferSource,
    path: &Path,
    embedding: &EmbeddingMetadata,
    limits: TransferLimits,
) -> Result<TransferRecord, ChunkTableError> {
    limits.validate(embedding)?;
    let directory = parent(path).map_err(|_| bad())?;
    if !owner.allows_output(path) {
        return Err(bad());
    }
    owner.revalidate().map_err(|_| bad())?;
    let metadata = TransferMetadata {
        version: 1,
        canonical_schema: CANONICAL_CHUNK_SCHEMA_VERSION.into(),
        embedding: embedding.clone(),
        authority_json: String::from_utf8(json(owner.binding(), 32768)?).map_err(|_| bad())?,
        ownership_sha256: owner.authority_sha256().map_err(|_| bad())?,
        operational: owner.state_snapshot(limits.sql_bytes).map_err(|_| bad())?,
        source: inspect_runtime_source(
            owner.index_directory(),
            embedding,
            owner.binding(),
            limits.native,
        )?,
    };
    // Retain a private parent fd so namespace replacement cannot redirect writes.
    use std::os::fd::AsRawFd;
    let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
        .join(path.file_name().ok_or_else(bad)?);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&anchored)?;
    let mut out = Output {
        file,
        hash: Sha256::new(),
        bytes: 0,
        max: limits.bytes,
    };
    out.write_all(MAGIC)?;
    frame(&mut out, &metadata, HEADER_BYTES)?;
    let observed = inspect_source_rows(
        owner.index_directory(),
        embedding,
        owner.binding(),
        limits.native,
        |batch| {
            for index in 0..batch.num_rows() {
                frame(&mut out, &row(batch, index)?, limits.frame())?;
            }
            Ok(())
        },
    )?;
    if observed != metadata.source
        || owner.authority_sha256().map_err(|_| bad())? != metadata.ownership_sha256
        || owner.state_snapshot(limits.sql_bytes).map_err(|_| bad())? != metadata.operational
    {
        return Err(bad());
    }
    owner.revalidate().map_err(|_| bad())?;
    out.file.sync_all()?;
    directory.sync_all()?;
    check_parent(&directory, path).map_err(|_| bad())?;
    let before = out.file.metadata()?;
    if before.len() != out.bytes
        || before.mode() & 0o077 != 0
        || before.uid() != fs::metadata("/proc/self")?.uid()
    {
        return Err(bad());
    }
    same_file(&out.file, path, &before)?;
    Ok(TransferRecord {
        metadata,
        bytes: out.bytes,
        sha256: format!("{:x}", out.hash.finalize()),
    })
}
/// Validate a descriptive interchange and visit bounded canonical batches. The
/// caller must discard any derived candidate if this returns an error. This
/// never proves source/SQL freshness and never constructs candidate authority.
pub fn read(
    path: &Path,
    expected: &TransferRecord,
    limits: TransferLimits,
    mut visit: impl FnMut(&RecordBatch) -> Result<(), ChunkTableError>,
) -> Result<(), ChunkTableError> {
    let mut reader = Reader::open(path, expected, limits)?;
    while let Some(batch) = reader.next_native()? {
        visit(&batch)?;
    }
    Ok(())
}
/// Bounded Arrow reader for a selected runtime's native streaming writer.
/// All batches and final EOF must be consumed; this is never source authority.
pub struct Reader {
    file: File,
    directory: File,
    path: PathBuf,
    before: fs::Metadata,
    metadata: TransferMetadata,
    authority: crate::commit::CommitBinding,
    limits: TransferLimits,
    validation: RowValidation,
    finished: bool,
    schema: SchemaRef,
}
impl Reader {
    pub fn open(
        path: &Path,
        expected: &TransferRecord,
        limits: TransferLimits,
    ) -> Result<Self, ChunkTableError> {
        limits.validate(&expected.metadata.embedding)?;
        let directory = parent(path).map_err(|_| bad())?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let before = file.metadata()?;
        if !before.is_file()
            || before.nlink() != 1
            || before.uid() != fs::metadata("/proc/self")?.uid()
            || before.mode() & 0o077 != 0
            || before.len() != expected.bytes
            || before.len() > limits.bytes
        {
            return Err(bad());
        }
        let mut hash = Sha256::new();
        let mut offset = 0;
        let mut buffer = [0; 65536];
        loop {
            let n = file.read_at(&mut buffer, offset)?;
            if n == 0 {
                break;
            }
            offset = offset.checked_add(n as u64).ok_or_else(bad)?;
            if offset > expected.bytes {
                return Err(bad());
            }
            hash.update(&buffer[..n]);
        }
        if offset != expected.bytes || format!("{:x}", hash.finalize()) != expected.sha256 {
            return Err(bad());
        }
        same_file(&file, path, &before)?;
        let mut magic = [0; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(bad());
        }
        let metadata: TransferMetadata = read_frame(&mut file, HEADER_BYTES)?;
        if metadata != expected.metadata
            || metadata.version != 1
            || metadata.canonical_schema != CANONICAL_CHUNK_SCHEMA_VERSION
            || metadata.source.version != 2
            || metadata.source.source_version == 0
            || metadata.operational.bytes == 0
            || metadata.operational.bytes > limits.sql_bytes
            || metadata.source.rows > limits.native.rows
        {
            return Err(bad());
        }
        for hash in [
            &metadata.ownership_sha256,
            &metadata.operational.sha256,
            &metadata.source.authority_sha256,
            &metadata.source.snapshot_sha256,
        ] {
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(bad());
            }
        }
        let authority =
            crate::commit::CommitBinding::parse_retained(metadata.authority_json.as_bytes())
                .map_err(|_| bad())?;
        if let Some(binding) = &metadata.source.lexical {
            binding.validate_transfer_metadata(&metadata.embedding, &authority)?;
        }
        let schema = chunk_schema(&metadata.embedding)?;
        Ok(Self {
            file,
            directory,
            path: path.into(),
            before,
            metadata,
            authority,
            limits,
            validation: RowValidation::default(),
            finished: false,
            schema,
        })
    }
    fn next_native(&mut self) -> Result<Option<RecordBatch>, ChunkTableError> {
        if self.finished {
            return Ok(None);
        }
        if self.validation.digest.rows < self.metadata.source.rows {
            let row: Row = read_frame(&mut self.file, self.limits.frame())?;
            let batch = batch(row, &self.metadata.embedding)?;
            self.validation.batch(
                &batch,
                &self.authority,
                &self.metadata.embedding,
                self.limits.native,
            )?;
            return Ok(Some(batch));
        }
        self.finished = true;
        if self.file.read(&mut [0])? != 0
            || self.validation.digest.finish() != self.metadata.source.snapshot_sha256
        {
            return Err(bad());
        }
        same_file(&self.file, &self.path, &self.before)?;
        check_parent(&self.directory, &self.path).map_err(|_| bad())?;
        Ok(None)
    }
}
impl Iterator for Reader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_native() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(_) => {
                self.finished = true;
                Some(Err(arrow_schema::ArrowError::ParseError(
                    "invalid canonical transfer".into(),
                )))
            }
        }
    }
}
impl arrow_array::RecordBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
#[cfg(test)]
mod tests;
