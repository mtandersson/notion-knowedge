//! Trusted derived metadata. Reads never create, repair, or migrate this file.
use super::{ChunkTableError, FtsIndexConfig};
use crate::{commit::CommitBinding, lexical_dictionary::Dictionary};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

pub mod runtime;

pub(crate) const FILE: &str = ".nk-lexical-binding-v1.json";
const MAX_BYTES: u64 = 32 * 1024;
fn incompatible() -> ChunkTableError {
    ChunkTableError::InvalidSchema("incompatible lexical index binding".into())
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Identity of verified immutable assets and this exact planner/executor build.
/// Source hashing covers all bounds, phrase/ID bypass, scoring, and tie behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LexicalPlanIdentity {
    asset_sha256: String,
    importer_sha256: String,
    executor_sha256: String,
    lock_sha256: String,
    embedding_text_contract: String,
}
impl LexicalPlanIdentity {
    pub fn verified(dictionary: &Dictionary) -> Result<Self, ChunkTableError> {
        if !valid_digest(dictionary.manifest_digest()) {
            return Err(incompatible());
        }
        Ok(Self {
            asset_sha256: dictionary.manifest_digest().into(),
            importer_sha256: dictionary.importer_identity_sha256(),
            executor_sha256: digest(
                concat!(
                    include_str!("bounded.rs"),
                    include_str!("../lexical_dictionary.rs")
                )
                .as_bytes(),
            ),
            lock_sha256: digest(include_bytes!("../../../../Cargo.lock")),
            embedding_text_contract:
                "canonical-indexed-chunk-v1;embedding-input=chunk.text;unchanged-vector-space"
                    .into(),
        })
    }
    /// Canonical digest for installed/evaluation provenance; contains no query text.
    pub fn sha256(&self) -> String {
        digest(&serde_json::to_vec(self).expect("fixed identity fields serialize"))
    }
    pub fn matches(&self, dictionary: &Dictionary) -> bool {
        Self::verified(dictionary).is_ok_and(|identity| identity == *self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageIdentity {
    schema: String,
    canonical_schema: String,
    fields: Vec<(String, String)>,
    profile: FtsIndexConfig,
    raw_identifier_profile: String,
    backend: String,
    implementation_sha256: String,
}
impl StorageIdentity {
    fn current() -> Self {
        Self {
            schema: super::CHUNK_TABLE_SCHEMA_VERSION.into(),
            canonical_schema: super::CANONICAL_CHUNK_SCHEMA_VERSION.into(),
            fields: super::CHUNK_FTS_INDEXES
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            profile: FtsIndexConfig::lexical_expansion_v1(),
            raw_identifier_profile: "raw;case-sensitive;no-stem;no-stopwords;no-ascii-folding"
                .into(),
            backend: "lancedb=0.39.0;lance=12.0.0".into(),
            implementation_sha256: digest(include_bytes!("../chunks.rs")),
        }
    }
}

/// Complete candidate binding. Building bytes does not authorize installing them
/// on an existing table: only the offline candidate/activation workflow may do so.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LexicalIndexBinding {
    version: u32,
    embedding: EmbeddingMetadata,
    storage: StorageIdentity,
    plan: LexicalPlanIdentity,
    table: String,
    workspace: String,
    roots: Vec<String>,
    scope: String,
    generation: String,
    epoch: u64,
    device: u64,
    inode: u64,
    source_snapshot_sha256: String,
}
impl LexicalIndexBinding {
    /// Trusted offline primitive; caller must verify every canonical row/vector
    /// and the four native index profiles before publishing a sibling candidate.
    /// This function does not write files or change coordinator authority.
    pub fn for_candidate(
        directory: &Path,
        embedding: EmbeddingMetadata,
        authority: &CommitBinding,
        plan: LexicalPlanIdentity,
        source_snapshot_sha256: &str,
    ) -> Result<Self, ChunkTableError> {
        if !valid_digest(source_snapshot_sha256) {
            return Err(incompatible());
        }
        let metadata = std::fs::metadata(directory)?;
        if !metadata.is_dir() {
            return Err(incompatible());
        }
        Ok(Self {
            version: 1,
            embedding,
            storage: StorageIdentity::current(),
            plan,
            table: authority.table().into(),
            workspace: authority.workspace().into(),
            roots: authority.roots().to_vec(),
            scope: authority.scope().into(),
            generation: authority.generation().into(),
            epoch: authority.epoch(),
            device: metadata.dev(),
            inode: metadata.ino(),
            source_snapshot_sha256: source_snapshot_sha256.into(),
        })
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChunkTableError> {
        let bytes = serde_json::to_vec(self).map_err(|_| incompatible())?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(incompatible());
        }
        Ok(bytes)
    }
    pub(crate) fn storage_profile(&self) -> FtsIndexConfig {
        self.storage.profile.clone()
    }
    pub fn plan(&self) -> &LexicalPlanIdentity {
        &self.plan
    }
    /// Validate supported descriptive metadata without asserting live source
    /// identity or equivalence to this build's implementation.
    pub(crate) fn validate_metadata(&self) -> Result<(), ChunkTableError> {
        let current = StorageIdentity::current();
        if self.version != 1
            || !valid_digest(&self.source_snapshot_sha256)
            || self.canonical_bytes()?.len() as u64 > MAX_BYTES
            || self.storage.schema != current.schema
            || self.storage.canonical_schema != current.canonical_schema
            || self.storage.fields != current.fields
            || self.storage.backend != current.backend
            || self.storage.raw_identifier_profile != current.raw_identifier_profile
            || ![
                FtsIndexConfig::default(),
                FtsIndexConfig::lexical_expansion_v1(),
            ]
            .contains(&self.storage.profile)
            || !valid_digest(&self.storage.implementation_sha256)
            || ![
                &self.plan.asset_sha256,
                &self.plan.importer_sha256,
                &self.plan.executor_sha256,
                &self.plan.lock_sha256,
            ]
            .into_iter()
            .all(|value| valid_digest(value))
            || self.plan.embedding_text_contract
                != "canonical-indexed-chunk-v1;embedding-input=chunk.text;unchanged-vector-space"
        {
            return Err(incompatible());
        }
        Ok(())
    }
    /// A canonical transfer describes the current native source opaquely; a
    /// prior runtime must not interpret that source's backend or lexical plan.
    /// Only the shared canonical/embedding contract is interpreted.
    pub(crate) fn validate_transfer_metadata(
        &self,
        embedding: &EmbeddingMetadata,
        authority: &CommitBinding,
    ) -> Result<(), ChunkTableError> {
        if self.version != 1
            || self.embedding != *embedding
            || self.storage.canonical_schema != super::CANONICAL_CHUNK_SCHEMA_VERSION
            || self.plan.embedding_text_contract
                != "canonical-indexed-chunk-v1;embedding-input=chunk.text;unchanged-vector-space"
            || self.table != authority.table()
            || self.workspace != authority.workspace()
            || self.roots != authority.roots()
            || self.scope != authority.scope()
            || self.generation != authority.generation()
            || self.epoch != authority.epoch()
            || ![
                &self.storage.implementation_sha256,
                &self.source_snapshot_sha256,
                &self.plan.asset_sha256,
                &self.plan.importer_sha256,
                &self.plan.executor_sha256,
                &self.plan.lock_sha256,
            ]
            .into_iter()
            .all(|h| valid_digest(h))
        {
            return Err(incompatible());
        }
        self.canonical_bytes()?;
        Ok(())
    }
    /// Offline migration may read an earlier approved plan/storage build without
    /// executing it. Admit only known schema/backend/tokenizer and the unchanged
    /// embedding text contract, authority and physical generation. Complete rows
    /// and vectors are subsequently verified; runtime validation remains exact.
    pub(crate) fn validate_offline_source(
        &self,
        directory: &Path,
        embedding: &EmbeddingMetadata,
        authority: &CommitBinding,
    ) -> Result<(), ChunkTableError> {
        self.validate_metadata()?;
        let expected = Self::for_candidate(
            directory,
            embedding.clone(),
            authority,
            self.plan.clone(),
            &self.source_snapshot_sha256,
        )?;
        // Older implementation digests are evidence about the source generation,
        // never new runtime compatibility or permission to relabel its vectors.
        if self.version != expected.version
            || self.embedding != expected.embedding
            || self.table != expected.table
            || self.workspace != expected.workspace
            || self.roots != expected.roots
            || self.scope != expected.scope
            || self.generation != expected.generation
            || self.epoch != expected.epoch
            || self.device != expected.device
            || self.inode != expected.inode
        {
            return Err(incompatible());
        }
        Ok(())
    }
    pub(crate) fn validate(
        &self,
        directory: &Path,
        embedding: &EmbeddingMetadata,
        authority: &CommitBinding,
        plan: &LexicalPlanIdentity,
    ) -> Result<(), ChunkTableError> {
        let expected = Self::for_candidate(
            directory,
            embedding.clone(),
            authority,
            plan.clone(),
            &self.source_snapshot_sha256,
        )?;
        if expected != *self {
            return Err(incompatible());
        }
        Ok(())
    }
}

pub(crate) fn read(directory: &Path) -> Result<Option<LexicalIndexBinding>, ChunkTableError> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(directory.join(FILE))
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(incompatible()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > MAX_BYTES {
        return Err(incompatible());
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(incompatible());
    }
    let binding: LexicalIndexBinding =
        serde_json::from_slice(&bytes).map_err(|_| incompatible())?;
    // Reject noncanonical/unknown fields and accidental partial metadata writes.
    if binding.canonical_bytes()? != bytes {
        return Err(incompatible());
    }
    Ok(Some(binding))
}
pub(crate) fn verify(
    directory: &Path,
    expected: &Option<LexicalIndexBinding>,
) -> Result<(), ChunkTableError> {
    if &read(directory)? != expected {
        return Err(incompatible());
    }
    if let Some(binding) = expected {
        let metadata = std::fs::metadata(directory)?;
        if metadata.dev() != binding.device || metadata.ino() != binding.inode {
            return Err(incompatible());
        }
    }
    Ok(())
}
