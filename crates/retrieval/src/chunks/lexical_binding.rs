//! Trusted derived metadata. Reads never create, repair, or migrate this file.
use super::{ChunkTableError, FtsIndexConfig};
use crate::{commit::CommitBinding, lexical_dictionary::Dictionary};
use notion_knowledge_core::embedding::EmbeddingMetadata;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};

// Keep the artifact path stable so older metadata is refused and reset,
// rather than silently ignored when its payload format changes.
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
                    include_str!("../lexical_runtime.rs"),
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

/// A verified identity and the immutable local asset handle it describes.
/// Configuration owns this value; tool inputs cannot construct a dictionary.
#[derive(Debug, Clone)]
pub struct LexicalCachePlan {
    dictionary: Arc<Dictionary>,
    identity: LexicalPlanIdentity,
}
impl LexicalCachePlan {
    pub fn verified(dictionary: Arc<Dictionary>) -> Result<Self, ChunkTableError> {
        let identity = LexicalPlanIdentity::verified(&dictionary)?;
        Ok(Self {
            dictionary,
            identity,
        })
    }
    pub fn identity(&self) -> &LexicalPlanIdentity {
        &self.identity
    }
    pub fn dictionary(&self) -> &Dictionary {
        &self.dictionary
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

/// Complete lexical identity. Constructing metadata does not authorize installing
/// it on an existing table; readers require an exact compatible binding.
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
}
impl LexicalIndexBinding {
    /// Construct identity metadata for a fresh authoritative cache and native
    /// index profile. Construction alone never grants installation authority.
    /// This function does not write files or change coordinator authority.
    pub fn for_cache(
        directory: &Path,
        embedding: EmbeddingMetadata,
        authority: &CommitBinding,
        plan: LexicalPlanIdentity,
    ) -> Result<Self, ChunkTableError> {
        let metadata = std::fs::metadata(directory)?;
        if !metadata.is_dir() {
            return Err(incompatible());
        }
        Ok(Self {
            version: 2,
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
        })
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChunkTableError> {
        let bytes = serde_json::to_vec(self).map_err(|_| incompatible())?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(incompatible());
        }
        Ok(bytes)
    }
    pub fn plan(&self) -> &LexicalPlanIdentity {
        &self.plan
    }
    pub(crate) fn validate(
        &self,
        directory: &Path,
        embedding: &EmbeddingMetadata,
        authority: &CommitBinding,
        plan: &LexicalPlanIdentity,
    ) -> Result<(), ChunkTableError> {
        let expected = Self::for_cache(directory, embedding.clone(), authority, plan.clone())?;
        if expected != *self {
            return Err(incompatible());
        }
        Ok(())
    }
}

pub(crate) fn write_fresh(
    directory: &Path,
    binding: &LexicalIndexBinding,
) -> Result<(), ChunkTableError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join(FILE))?;
    file.write_all(&binding.canonical_bytes()?)?;
    Ok(())
}

pub(crate) fn read(directory: &Path) -> Result<Option<LexicalIndexBinding>, ChunkTableError> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(directory.join(FILE))
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) if e.raw_os_error() == Some(libc::ELOOP) => return Err(incompatible()),
        Err(e) => return Err(e.into()),
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
