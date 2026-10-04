//! Replaceable embedding execution and persisted vector-space identity.
use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};

use crate::indexed::SchemaVersion;

/// Persist this alongside an index, including when the index contains no rows.
/// A different identity requires an explicit rebuild before mixing vectors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MetadataWire")]
pub struct EmbeddingMetadata {
    schema_version: SchemaVersion,
    provider_id: String,
    model_id: String,
    model_version: String,
    dimension: usize,
}

#[derive(Deserialize)]
struct MetadataWire {
    schema_version: SchemaVersion,
    provider_id: String,
    model_id: String,
    model_version: String,
    dimension: usize,
}

impl TryFrom<MetadataWire> for EmbeddingMetadata {
    type Error = EmbeddingError;
    fn try_from(value: MetadataWire) -> Result<Self, Self::Error> {
        let mut metadata = Self::new(
            value.provider_id,
            value.model_id,
            value.model_version,
            value.dimension,
        )?;
        metadata.schema_version = value.schema_version;
        Ok(metadata)
    }
}

impl EmbeddingMetadata {
    pub fn new(
        provider_id: String,
        model_id: String,
        model_version: String,
        dimension: usize,
    ) -> Result<Self, EmbeddingError> {
        if dimension == 0
            || [&provider_id, &model_id, &model_version]
                .iter()
                .any(|id| id.trim().is_empty() || id.chars().any(char::is_control))
        {
            return Err(EmbeddingError::InvalidMetadata);
        }
        Ok(Self {
            schema_version: SchemaVersion::V1,
            provider_id,
            model_id,
            model_version,
            dimension,
        })
    }
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }
    pub fn model_id(&self) -> &str {
        &self.model_id
    }
    pub fn model_version(&self) -> &str {
        &self.model_version
    }
    pub fn dimension(&self) -> usize {
        self.dimension
    }
    /// Reject mixed vector spaces, even when their dimensions match.
    pub fn ensure_compatible(&self, other: &Self) -> Result<(), EmbeddingError> {
        if self == other {
            Ok(())
        } else {
            Err(EmbeddingError::IncompatibleIndex)
        }
    }
}

/// Payload-free errors: adapters must not include input text or runtime secrets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingError {
    Unavailable,
    ResourceExhausted,
    InvalidInput,
    UnsupportedModel,
    InvalidMetadata,
    InvalidOutput,
    IncompatibleIndex,
}
impl EmbeddingError {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::ResourceExhausted)
    }
}
impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "embedding failed: {self:?}")
    }
}
impl std::error::Error for EmbeddingError {}

pub type EmbeddingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a>>;

/// Metadata is immutable for a provider instance. Results preserve input order,
/// return exactly one finite vector of the advertised dimension per input, and
/// fail the whole batch on error. Retry scheduling belongs to the caller.
pub trait EmbeddingProvider: Send + Sync {
    fn metadata(&self) -> &EmbeddingMetadata;
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a>;
}

/// Shared application boundary validates adapter output before index insertion.
/// Empty batches succeed without loading a runtime. Text is never logged here.
pub async fn embed_batch(
    provider: &dyn EmbeddingProvider,
    inputs: &[String],
) -> Result<Vec<Vec<f32>>, EmbeddingError> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let vectors = provider.embed_batch(inputs).await?;
    if vectors.len() != inputs.len()
        || vectors
            .iter()
            .any(|v| v.len() != provider.metadata().dimension() || v.iter().any(|n| !n.is_finite()))
    {
        return Err(EmbeddingError::InvalidOutput);
    }
    Ok(vectors)
}
