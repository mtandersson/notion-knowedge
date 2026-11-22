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
    /// Deterministic text/token admission limit; inference has not accepted it.
    InputLimit,
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
pub type EmbeddingIdleFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), EmbeddingError>> + Send + 'a>>;

pub type EmbeddingPartitionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<usize>, EmbeddingError>> + Send + 'a>>;

pub trait EmbeddingProvider: Send + Sync {
    /// Wait for work retained after a cancelled embedding observer. Providers
    /// with cancellation-safe futures need no additional drain. Blocking adapters
    /// must retain admission until their work ends and override this method.
    fn wait_idle(&self) -> EmbeddingIdleFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn metadata(&self) -> &EmbeddingMetadata;
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a>;
    /// Return the subset of ordered UTF-8 byte boundaries needed to fit one
    /// indexing input. An empty result preserves the original input. Adapters
    /// without a partitioner leave admission to `embed_batch`.
    fn partition_input<'a>(
        &'a self,
        _input: &'a str,
        _boundaries: Vec<usize>,
    ) -> EmbeddingPartitionFuture<'a> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Try an unchanged input first. On an input limit, bisect only at the
/// supplied structural boundaries and check the actual input of each piece.
/// The split budget limits work on pathological Markdown, and an indivisible
/// input retains the original typed failure.
pub fn partition_at_boundaries(
    input: &str,
    boundaries: &[usize],
    mut validate: impl FnMut(&str) -> Result<(), EmbeddingError>,
) -> Result<Vec<usize>, EmbeddingError> {
    const MAX_SPLITS: usize = 127;
    let mut pending = vec![(0, input.len(), boundaries)];
    let mut cuts = Vec::new();
    while let Some((start, end, boundaries)) = pending.pop() {
        match validate(&input[start..end]) {
            Ok(()) => continue,
            Err(EmbeddingError::InputLimit) => {}
            Err(error) => return Err(error),
        }
        if cuts.len() == MAX_SPLITS {
            return Err(EmbeddingError::InputLimit);
        }
        let middle = start + (end - start) / 2;
        let after = boundaries.partition_point(|&position| position < middle);
        let position = match (after.checked_sub(1), boundaries.get(after)) {
            (Some(before), Some(&right)) if middle - boundaries[before] < right - middle => before,
            (_, Some(_)) => after,
            (Some(before), None) => before,
            (None, None) => return Err(EmbeddingError::InputLimit),
        };
        let cut = boundaries[position];
        pending.push((cut, end, &boundaries[position + 1..]));
        pending.push((start, cut, &boundaries[..position]));
        cuts.push(cut);
    }
    cuts.sort_unstable();
    Ok(cuts)
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
