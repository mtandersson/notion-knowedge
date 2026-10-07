//! Persisted canonical chunk rows for embedded LanceDB retrieval.
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lancedb::{
    DistanceType, Table,
    index::{
        Index, IndexType,
        scalar::{FtsIndexBuilder, FullTextSearchQuery},
        vector::IvfFlatIndexBuilder,
    },
    query::{ExecutableQuery, QueryBase, Select},
    table::{OptimizeOptions, optimize::OptimizeAction},
};
use notion_knowledge_core::{
    embedding::{self, EmbeddingError, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, SchemaVersion},
    search::{
        LexicalQuery, LexicalSearch, SearchFuture, SearchHit, SearchSource, SearchUnavailable,
        SemanticQuery, SemanticSearch,
    },
    source::{
        ExpandedSource, SourceExpandQuery, SourceExpansion, SourceExpansionError,
        SourceExpansionFuture, SourceProvenance, StableSourceRef,
    },
};

pub const CHUNK_TABLE_SCHEMA_VERSION: &str = "1";
const CANONICAL_CHUNK_SCHEMA_VERSION: &str = "1";
const TABLE_SCHEMA_KEY: &str = "notion_knowledge.chunk_table.schema_version";
const CANONICAL_SCHEMA_KEY: &str = "notion_knowledge.chunk_table.canonical_schema_version";
const EMBEDDING_KEY: &str = "notion_knowledge.chunk_table.embedding";
pub const CHUNK_VECTOR_INDEX_NAME: &str = "chunks_vector_ivf_flat_v1";
const CHUNK_FTS_INDEXES: [(&str, &str); 4] = [
    ("chunks_fts_text_v1", "text"),
    ("chunks_fts_title_v1", "title"),
    ("chunks_fts_page_id_v1", "page_id"),
    ("chunks_fts_chunk_id_v1", "chunk_id"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkTableError {
    Storage,
    Serialization,
    Io,
    Embedding(EmbeddingError),
    InvalidPath,
    InvalidSchema(String),
    InvalidRows(String),
}

impl std::fmt::Display for ChunkTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage => write!(f, "local LanceDB storage operation failed"),
            Self::Serialization => write!(f, "chunk-table serialization failed"),
            Self::Io => write!(f, "local index path operation failed"),
            Self::Embedding(error) => write!(f, "embedding identity failed: {error}"),
            Self::InvalidPath => write!(f, "local index path must be valid UTF-8"),
            Self::InvalidSchema(message) => write!(f, "incompatible chunk table schema: {message}"),
            Self::InvalidRows(message) => write!(f, "invalid chunk rows: {message}"),
        }
    }
}

impl std::error::Error for ChunkTableError {}

impl ChunkTableError {
    /// True only for persisted index contracts that cannot be safely reused.
    /// Storage/I/O failures are never treated as rebuildable compatibility errors.
    pub fn is_incompatible_index(&self) -> bool {
        matches!(
            self,
            Self::InvalidSchema(_) | Self::Embedding(EmbeddingError::IncompatibleIndex)
        )
    }
}

/// Startup behavior when persisted index identity is incompatible with the
/// requested production contract. Fail is deliberately the safe default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexCompatibilityPolicy {
    #[default]
    Fail,
    Rebuild,
}

/// Observable result of opening a compatible index generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStartupAction {
    Opened,
    Rebuilt,
}

impl From<lancedb::Error> for ChunkTableError {
    fn from(_: lancedb::Error) -> Self {
        Self::Storage
    }
}
impl From<arrow_schema::ArrowError> for ChunkTableError {
    fn from(_: arrow_schema::ArrowError) -> Self {
        Self::Serialization
    }
}
impl From<serde_json::Error> for ChunkTableError {
    fn from(_: serde_json::Error) -> Self {
        Self::Serialization
    }
}
impl From<std::io::Error> for ChunkTableError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}
impl From<EmbeddingError> for ChunkTableError {
    fn from(value: EmbeddingError) -> Self {
        Self::Embedding(value)
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddedChunk {
    pub chunk: IndexedChunk,
    pub vector: Vec<f32>,
}

impl EmbeddedChunk {
    pub fn new(chunk: IndexedChunk, vector: Vec<f32>) -> Self {
        Self { chunk, vector }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkDiffMetrics {
    pub added: usize,
    pub changed: usize,
    pub skipped: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsIndexConfig {
    pub language: String,
    pub stem: bool,
    pub remove_stop_words: bool,
    pub ascii_folding: bool,
    pub block_size: usize,
}

impl Default for FtsIndexConfig {
    fn default() -> Self {
        Self {
            language: "Swedish".into(),
            stem: true,
            remove_stop_words: true,
            ascii_folding: false,
            block_size: 128,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FtsIndexHit {
    pub chunk_id: String,
    pub page_id: String,
    pub root_page_id: String,
    pub block_id: Option<String>,
    pub url: String,
    pub title: String,
    pub heading_path: Vec<String>,
    pub text: String,
    pub score: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDistance {
    L2,
    Cosine,
}

impl VectorDistance {
    fn lance(self) -> DistanceType {
        match self {
            Self::L2 => DistanceType::L2,
            Self::Cosine => DistanceType::Cosine,
        }
    }

    fn matches(self, actual: &DistanceType) -> bool {
        matches!(
            (self, actual),
            (Self::L2, DistanceType::L2) | (Self::Cosine, DistanceType::Cosine)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexConfig {
    pub distance: VectorDistance,
    pub num_partitions: Option<u32>,
    pub sample_rate: u32,
    pub max_iterations: u32,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            distance: VectorDistance::L2,
            num_partitions: None,
            sample_rate: 256,
            max_iterations: 50,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexAction {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndexHit {
    pub chunk_id: String,
    pub page_id: String,
    pub root_page_id: String,
    pub block_id: Option<String>,
    pub url: String,
    pub title: String,
    pub heading_path: Vec<String>,
    pub text: String,
    pub distance: f32,
}

/// Production chunk storage. LanceDB remains an adapter detail: callers pass
/// canonical chunks and vectors, and receive storage-neutral errors.
pub struct LanceChunkTable {
    table: Table,
    embedding: EmbeddingMetadata,
}

/// Semantic composition over an already configured local table and embedding
/// provider. Construction rejects mixed vector spaces before a request runs.
pub struct LanceSemanticSearch {
    table: Arc<LanceChunkTable>,
    provider: Arc<dyn EmbeddingProvider>,
    nprobes: usize,
}

impl LanceSemanticSearch {
    pub fn new(
        table: Arc<LanceChunkTable>,
        provider: Arc<dyn EmbeddingProvider>,
        nprobes: usize,
    ) -> Result<Self, ChunkTableError> {
        table
            .embedding_metadata()
            .ensure_compatible(provider.metadata())?;
        if nprobes == 0 {
            return Err(ChunkTableError::InvalidRows(
                "nprobes must be positive".into(),
            ));
        }
        Ok(Self {
            table,
            provider,
            nprobes,
        })
    }

    async fn semantic_search(
        &self,
        query: SemanticQuery,
    ) -> Result<Vec<SearchHit>, ChunkTableError> {
        let scope = LexicalQuery {
            query: query.query.clone(),
            limit: query.limit,
            page_ids: query.page_ids,
            root_page_ids: query.root_page_ids,
        };
        let predicate = lexical_filter_predicate(&scope)?;
        let vectors = embedding::embed_batch(self.provider.as_ref(), &[query.query]).await?;
        let mut hits = self
            .table
            .vector_query_scoped(&vectors[0], query.limit, self.nprobes, predicate.as_deref())
            .await?
            .into_iter()
            .map(|hit| SearchHit {
                matched_paths: Vec::new(),
                text: bounded_text(&hit.text, 2000),
                // Native distance is lower-is-better for every supported metric.
                // Negation preserves ranking without claiming cross-metric calibration.
                score: -hit.distance,
                source: SearchSource {
                    page_id: hit.page_id,
                    chunk_id: hit.chunk_id,
                    url: hit.url,
                    title: hit.title,
                    heading_path: hit.heading_path,
                    block_id: hit.block_id,
                },
            })
            .collect::<Vec<_>>();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.source.page_id.cmp(&b.source.page_id))
                .then_with(|| a.source.chunk_id.cmp(&b.source.chunk_id))
        });
        Ok(hits)
    }
}

impl SemanticSearch for LanceSemanticSearch {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            self.semantic_search(query)
                .await
                .map_err(|_| SearchUnavailable)
        })
    }
}

impl LanceChunkTable {
    /// Create a new empty table with an explicit vector dimension and persisted
    /// canonical/embedding schema identity.
    pub async fn create(
        database_path: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
    ) -> Result<Self, ChunkTableError> {
        validate_table_name(table_name)?;
        let uri = local_database_uri(database_path.as_ref())?;
        let database = lancedb::connect(&uri).execute().await?;
        let schema = chunk_schema(&embedding)?;
        let table = database
            .create_empty_table(table_name, schema)
            .execute()
            .await?;
        validate_table_schema(&table, &embedding).await?;
        let result = Self { table, embedding };
        if let Err(error) = result.ensure_fts_index(&FtsIndexConfig::default()).await {
            drop(result);
            database.drop_table(table_name, &[]).await?;
            return Err(error);
        }
        result.validate_vector_index_layout().await?;
        Ok(result)
    }

    /// Open an existing table only when both its storage schema and vector-space
    /// identity match the requested production contract.
    pub async fn open(
        database_path: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
    ) -> Result<Self, ChunkTableError> {
        Self::open_with_policy(
            database_path,
            table_name,
            embedding,
            IndexCompatibilityPolicy::Fail,
        )
        .await
        .map(|(table, _)| table)
    }

    /// Startup compatibility gate for the local index.
    ///
    /// Fail preserves the incompatible table untouched. Rebuild is explicit
    /// and destructive only for the named derived LanceDB table: it drops the
    /// incompatible generation and creates a new empty table with the requested
    /// schema/vector identity. Storage and I/O errors never trigger a rebuild.
    pub async fn open_with_policy(
        database_path: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
        policy: IndexCompatibilityPolicy,
    ) -> Result<(Self, IndexStartupAction), ChunkTableError> {
        validate_table_name(table_name)?;
        // Validate the requested target contract before any destructive recovery.
        // Otherwise an invalid runtime dimension could be mistaken for a persisted
        // incompatibility and cause the existing derived table to be dropped.
        let target_schema = chunk_schema(&embedding)?;
        let uri = local_database_uri(database_path.as_ref())?;
        let database = lancedb::connect(&uri).execute().await?;
        let table = database.open_table(table_name).execute().await?;

        match validate_table_schema(&table, &embedding).await {
            Ok(()) => {
                let result = Self { table, embedding };
                result.ensure_fts_index(&FtsIndexConfig::default()).await?;
                result.validate_vector_index_layout().await?;
                Ok((result, IndexStartupAction::Opened))
            }
            Err(error)
                if policy == IndexCompatibilityPolicy::Rebuild && error.is_incompatible_index() =>
            {
                drop(table);
                database.drop_table(table_name, &[]).await?;
                let table = database
                    .create_empty_table(table_name, target_schema)
                    .execute()
                    .await?;
                validate_table_schema(&table, &embedding).await?;
                let result = Self { table, embedding };
                result.ensure_fts_index(&FtsIndexConfig::default()).await?;
                Ok((result, IndexStartupAction::Rebuilt))
            }
            Err(error) => Err(error),
        }
    }

    pub fn embedding_metadata(&self) -> &EmbeddingMetadata {
        &self.embedding
    }

    pub async fn count_rows(&self) -> Result<usize, ChunkTableError> {
        Ok(self.table.count_rows(None).await?)
    }

    async fn validate_vector_index_layout(&self) -> Result<(), ChunkTableError> {
        if let Some(index) = self
            .table
            .list_indices()
            .await?
            .into_iter()
            .find(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
            && (index.index_type != IndexType::IvfFlat
                || index.columns != vec!["vector".to_string()])
        {
            return Err(ChunkTableError::InvalidSchema(format!(
                "vector index {CHUNK_VECTOR_INDEX_NAME} must be IVF_FLAT on vector"
            )));
        }
        Ok(())
    }

    fn validate_vector_index_config(config: &VectorIndexConfig) -> Result<(), ChunkTableError> {
        if config.num_partitions == Some(0) || config.sample_rate == 0 || config.max_iterations == 0
        {
            return Err(ChunkTableError::InvalidSchema(
                "vector index parameters must be positive".into(),
            ));
        }
        Ok(())
    }

    async fn validate_vector_index_config_matches(
        &self,
        config: &VectorIndexConfig,
    ) -> Result<(), ChunkTableError> {
        Self::validate_vector_index_config(config)?;
        self.validate_vector_index_layout().await?;
        let Some(stats) = self.table.index_stats(CHUNK_VECTOR_INDEX_NAME).await? else {
            return Err(ChunkTableError::InvalidSchema(
                "vector index exists but statistics are unavailable".into(),
            ));
        };
        let Some(distance) = stats.distance_type.as_ref() else {
            return Err(ChunkTableError::InvalidSchema(
                "vector index is missing persisted distance metadata".into(),
            ));
        };
        if !config.distance.matches(distance) {
            return Err(ChunkTableError::InvalidSchema(
                "vector index distance does not match requested configuration".into(),
            ));
        }
        Ok(())
    }

    /// Build the production IVF_FLAT ANN index once the initial crawl has
    /// populated the vector column. Existing compatible indices are preserved.
    pub async fn ensure_vector_index(
        &self,
        config: &VectorIndexConfig,
    ) -> Result<VectorIndexAction, ChunkTableError> {
        Self::validate_vector_index_config(config)?;
        self.validate_vector_index_layout().await?;
        if self
            .table
            .list_indices()
            .await?
            .iter()
            .any(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
        {
            self.validate_vector_index_config_matches(config).await?;
            return Ok(VectorIndexAction::Existing);
        }
        if self.count_rows().await? == 0 {
            return Err(ChunkTableError::InvalidRows(
                "cannot train vector index on an empty chunk table".into(),
            ));
        }
        self.create_vector_index(config, false).await?;
        self.validate_vector_index_config_matches(config).await?;
        Ok(VectorIndexAction::Created)
    }

    /// Explicitly retrain/replace the derived vector index. The chunk table and
    /// authoritative source data are never replaced by this operation.
    pub async fn rebuild_vector_index(
        &self,
        config: &VectorIndexConfig,
    ) -> Result<(), ChunkTableError> {
        Self::validate_vector_index_config(config)?;
        self.validate_vector_index_layout().await?;
        if self.count_rows().await? == 0 {
            return Err(ChunkTableError::InvalidRows(
                "cannot rebuild vector index on an empty chunk table".into(),
            ));
        }
        self.create_vector_index(config, true).await?;
        self.validate_vector_index_config_matches(config).await
    }

    async fn create_vector_index(
        &self,
        config: &VectorIndexConfig,
        replace: bool,
    ) -> Result<(), ChunkTableError> {
        let mut builder = IvfFlatIndexBuilder::default()
            .distance_type(config.distance.lance())
            .sample_rate(config.sample_rate)
            .max_iterations(config.max_iterations);
        if let Some(num_partitions) = config.num_partitions {
            builder = builder.num_partitions(num_partitions);
        }
        self.table
            .create_index(&["vector"], Index::IvfFlat(builder))
            .name(CHUNK_VECTOR_INDEX_NAME.to_string())
            .replace(replace)
            .execute()
            .await?;
        Ok(())
    }

    /// Fold rows written after the last index build into the named vector
    /// index. This creates the index first if the initial crawl has completed.
    pub async fn optimize_vector_index(
        &self,
        config: &VectorIndexConfig,
    ) -> Result<VectorIndexAction, ChunkTableError> {
        let action = self.ensure_vector_index(config).await?;
        self.table
            .optimize(OptimizeAction::Index(
                OptimizeOptions::new().index_names(vec![CHUNK_VECTOR_INDEX_NAME.to_string()]),
            ))
            .await?;
        self.validate_vector_index_config_matches(config).await?;
        Ok(action)
    }

    /// Low-level indexed nearest-neighbor probe for #40. Distances are native
    /// backend distances where lower values rank nearer. #45 owns conversion to
    /// the semantic search application's higher-is-better score contract.
    pub async fn vector_query(
        &self,
        query_vector: &[f32],
        limit: usize,
        nprobes: usize,
    ) -> Result<Vec<VectorIndexHit>, ChunkTableError> {
        self.vector_query_scoped(query_vector, limit, nprobes, None)
            .await
    }

    async fn vector_query_scoped(
        &self,
        query_vector: &[f32],
        limit: usize,
        nprobes: usize,
        predicate: Option<&str>,
    ) -> Result<Vec<VectorIndexHit>, ChunkTableError> {
        if query_vector.len() != self.embedding.dimension()
            || query_vector.iter().any(|value| !value.is_finite())
            || limit == 0
            || limit > 100
            || nprobes == 0
        {
            return Err(ChunkTableError::InvalidRows(
                "vector query requires matching finite dimensions and positive bounds".into(),
            ));
        }
        self.validate_vector_index_layout().await?;
        let stats = self
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await?
            .ok_or_else(|| ChunkTableError::InvalidSchema("vector index is not built".into()))?;
        let distance = stats.distance_type.ok_or_else(|| {
            ChunkTableError::InvalidSchema("vector index is missing distance metadata".into())
        })?;
        if matches!(distance, DistanceType::Cosine)
            && query_vector.iter().all(|value| *value == 0.0)
        {
            return Err(ChunkTableError::InvalidRows(
                "cosine vector queries require a non-zero query vector".into(),
            ));
        }

        let mut search = self
            .table
            .query()
            .nearest_to(query_vector)?
            .column("vector")
            .distance_type(distance)
            .nprobes(nprobes)
            .select(Select::Columns(vec![
                "chunk_id".into(),
                "page_id".into(),
                "root_page_id".into(),
                "block_id".into(),
                "url".into(),
                "title".into(),
                "heading_path_json".into(),
                "text".into(),
            ]))
            .limit(limit);
        if let Some(predicate) = predicate {
            search = search.only_if(predicate);
        }
        let batches: Vec<RecordBatch> = search.execute().await?.try_collect().await?;
        decode_vector_hits(&batches)
    }

    /// Ensure the production BM25/FTS indices exist without rebuilding them on
    /// every startup. Native Lance FTS is one-column-per-index, so title, text
    /// and stable identifiers each get a versioned index.
    pub async fn ensure_fts_index(&self, config: &FtsIndexConfig) -> Result<(), ChunkTableError> {
        let existing = self.table.list_indices().await?;
        for (index_name, column) in CHUNK_FTS_INDEXES {
            if let Some(index) = existing.iter().find(|index| index.name == index_name) {
                if index.index_type != IndexType::FTS || index.columns != vec![column.to_string()] {
                    return Err(ChunkTableError::InvalidSchema(format!(
                        "FTS index {index_name} does not match column {column}"
                    )));
                }
                continue;
            }
            self.create_fts_index(index_name, column, config, false)
                .await?;
        }
        Ok(())
    }

    /// Explicitly replace all FTS indices when tokenizer configuration changes.
    pub async fn rebuild_fts_index(&self, config: &FtsIndexConfig) -> Result<(), ChunkTableError> {
        for (index_name, column) in CHUNK_FTS_INDEXES {
            self.create_fts_index(index_name, column, config, true)
                .await?;
        }
        Ok(())
    }

    async fn create_fts_index(
        &self,
        index_name: &str,
        column: &str,
        config: &FtsIndexConfig,
        replace: bool,
    ) -> Result<(), ChunkTableError> {
        let params = if matches!(column, "page_id" | "chunk_id") {
            FtsIndexBuilder::default()
                .base_tokenizer("raw".to_string())
                .lower_case(false)
                .stem(false)
                .remove_stop_words(false)
                .ascii_folding(false)
                .block_size(config.block_size)
                .map_err(|_| ChunkTableError::InvalidSchema("invalid FTS block size".into()))?
        } else {
            FtsIndexBuilder::default()
                .language(&config.language)
                .map_err(|_| ChunkTableError::InvalidSchema("unsupported FTS language".into()))?
                .stem(config.stem)
                .remove_stop_words(config.remove_stop_words)
                .ascii_folding(config.ascii_folding)
                .block_size(config.block_size)
                .map_err(|_| ChunkTableError::InvalidSchema("invalid FTS block size".into()))?
        };
        let train = self.table.count_rows(None).await? != 0;
        self.table
            .create_index(&[column], Index::FTS(params))
            .name(index_name.to_string())
            .replace(replace)
            .train(train)
            .execute()
            .await?;
        Ok(())
    }

    /// Fold delta fragments created by incremental updates into all FTS indices.
    /// Callers choose when to pay this maintenance cost.
    pub async fn optimize_fts_index(&self) -> Result<(), ChunkTableError> {
        self.table
            .optimize(OptimizeAction::Index(
                OptimizeOptions::new().index_names(
                    CHUNK_FTS_INDEXES
                        .iter()
                        .map(|(name, _)| (*name).to_string())
                        .collect(),
                ),
            ))
            .await?;
        Ok(())
    }

    /// Low-level one-column FTS probe used by the retrieval adapter and tests.
    /// The user-facing lexical search contract is implemented below without
    /// concatenating user text into SQL predicates.
    pub async fn fts_query(
        &self,
        column: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<FtsIndexHit>, ChunkTableError> {
        self.fts_query_scoped(column, query, limit, None).await
    }

    async fn fts_query_scoped(
        &self,
        column: &str,
        query: &str,
        limit: usize,
        predicate: Option<&str>,
    ) -> Result<Vec<FtsIndexHit>, ChunkTableError> {
        if query.trim().is_empty()
            || limit == 0
            || !CHUNK_FTS_INDEXES
                .iter()
                .any(|(_, indexed_column)| *indexed_column == column)
        {
            return Err(ChunkTableError::InvalidRows(
                "FTS query requires a supported column, nonempty text and positive limit".into(),
            ));
        }
        let fts_query = FullTextSearchQuery::new(query.to_owned())
            .with_column(column.to_owned())
            .map_err(|_| ChunkTableError::InvalidRows("invalid FTS query column".into()))?;
        let mut search = self
            .table
            .query()
            .full_text_search(fts_query)
            .select(Select::Columns(vec![
                "chunk_id".into(),
                "page_id".into(),
                "root_page_id".into(),
                "block_id".into(),
                "url".into(),
                "title".into(),
                "heading_path_json".into(),
                "text".into(),
            ]))
            .limit(limit);
        if let Some(predicate) = predicate {
            search = search.only_if(predicate);
        }
        let batches: Vec<RecordBatch> = search.execute().await?.try_collect().await?;
        decode_fts_hits(&batches)
    }

    async fn lexical_search(&self, query: LexicalQuery) -> Result<Vec<SearchHit>, ChunkTableError> {
        validate_lexical_query(&query)?;
        let predicate = lexical_filter_predicate(&query)?;
        let candidate_limit = query.limit.saturating_mul(4).min(400);
        let fields = [
            ("chunk_id", 4.0_f32),
            ("page_id", 4.0_f32),
            ("title", 2.0_f32),
            ("text", 1.0_f32),
        ];
        let mut best = HashMap::<(String, String), SearchHit>::new();
        let phrase = quoted_phrase(&query.query);

        for (column, band) in fields {
            for hit in self
                .fts_query_scoped(column, &query.query, candidate_limit, predicate.as_deref())
                .await?
            {
                if phrase.is_some_and(|phrase| !hit_matches_phrase(column, &hit, phrase)) {
                    continue;
                }
                let native = hit.score.max(0.0);
                let score = band + native / (1.0 + native);
                if !score.is_finite() {
                    return Err(ChunkTableError::InvalidRows(
                        "lexical ranking score must be finite".into(),
                    ));
                }
                let key = (hit.page_id.clone(), hit.chunk_id.clone());
                let candidate = SearchHit {
                    matched_paths: Vec::new(),
                    text: bounded_text(&hit.text, 2000),
                    score,
                    source: SearchSource {
                        page_id: hit.page_id,
                        chunk_id: hit.chunk_id,
                        url: hit.url,
                        title: hit.title,
                        heading_path: hit.heading_path,
                        block_id: hit.block_id,
                    },
                };
                match best.get(&key) {
                    Some(existing) if existing.score >= candidate.score => {}
                    _ => {
                        best.insert(key, candidate);
                    }
                }
            }
        }

        let mut hits = best.into_values().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.source.page_id.cmp(&right.source.page_id))
                .then_with(|| left.source.chunk_id.cmp(&right.source.chunk_id))
        });
        hits.truncate(query.limit);
        Ok(hits)
    }

    /// Insert new stable chunk IDs and replace existing rows with the same ID.
    ///
    /// A source batch may not repeat a chunk ID because LanceDB merge semantics
    /// do not define which duplicate source row should win.
    pub async fn upsert(
        &self,
        embedding: &EmbeddingMetadata,
        rows: &[EmbeddedChunk],
    ) -> Result<(), ChunkTableError> {
        self.embedding.ensure_compatible(embedding)?;
        if rows.is_empty() {
            return Ok(());
        }
        validate_rows(rows, self.embedding.dimension())?;
        let batch = rows_to_batch(rows, &self.embedding)?;
        let schema = batch.schema();
        let mut merge = self.table.merge_insert(&["chunk_id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)].into_iter(),
                schema,
            )))
            .await?;
        Ok(())
    }

    /// Reconcile one complete page snapshot against persisted chunks.
    ///
    /// Unchanged content hashes reuse their existing vectors while still
    /// refreshing citation metadata. Changed/new chunks are embedded once in a
    /// single provider batch. The final merge deletes target-page rows absent
    /// from the incoming snapshot, so retries converge to the same state.
    pub async fn apply_page_diff(
        &self,
        provider: &dyn EmbeddingProvider,
        page_id: &str,
        chunks: &[IndexedChunk],
    ) -> Result<ChunkDiffMetrics, ChunkTableError> {
        self.embedding.ensure_compatible(provider.metadata())?;
        validate_page_snapshot(page_id, chunks)?;

        let page_predicate = format!("page_id = {}", sql_string(page_id));
        let existing = self.rows_matching(page_predicate.clone()).await?;
        let mut existing_by_id = HashMap::with_capacity(existing.len());
        for row in existing {
            let chunk_id = row.chunk_id.clone();
            if existing_by_id.insert(chunk_id.clone(), row).is_some() {
                return Err(ChunkTableError::InvalidRows(format!(
                    "persisted page contains duplicate chunk_id: {chunk_id}"
                )));
            }
        }

        let incoming_ids: HashSet<&str> =
            chunks.iter().map(|chunk| chunk.chunk_id.as_str()).collect();
        let mut metrics = ChunkDiffMetrics {
            removed: existing_by_id
                .keys()
                .filter(|chunk_id| !incoming_ids.contains(chunk_id.as_str()))
                .count(),
            ..ChunkDiffMetrics::default()
        };
        let mut embed_positions = Vec::new();
        let mut embed_inputs = Vec::new();

        for (position, chunk) in chunks.iter().enumerate() {
            match existing_by_id.get(&chunk.chunk_id) {
                Some(stored) if stored.content_hash == chunk.content_hash => {
                    metrics.skipped += 1;
                }
                Some(_) => {
                    metrics.changed += 1;
                    embed_positions.push(position);
                    embed_inputs.push(chunk.text.clone());
                }
                None => {
                    metrics.added += 1;
                    embed_positions.push(position);
                    embed_inputs.push(chunk.text.clone());
                }
            }
        }

        // Finish all fallible embedding work before mutating the table.
        let fresh_vectors = embedding::embed_batch(provider, &embed_inputs).await?;

        if chunks.is_empty() {
            if metrics.removed != 0 {
                self.table.delete(page_predicate.as_str()).await?;
            }
            return Ok(metrics);
        }

        let mut fresh_by_position = embed_positions
            .into_iter()
            .zip(fresh_vectors)
            .collect::<HashMap<_, _>>();
        let mut rows = Vec::with_capacity(chunks.len());
        for (position, chunk) in chunks.iter().enumerate() {
            let vector = if let Some(vector) = fresh_by_position.remove(&position) {
                vector
            } else {
                existing_by_id
                    .get(&chunk.chunk_id)
                    .filter(|stored| stored.content_hash == chunk.content_hash)
                    .map(|stored| stored.vector.clone())
                    .ok_or_else(|| {
                        ChunkTableError::InvalidRows(format!(
                            "missing reusable vector for unchanged chunk {}",
                            chunk.chunk_id
                        ))
                    })?
            };
            rows.push(EmbeddedChunk::new(chunk.clone(), vector));
        }
        if !fresh_by_position.is_empty() {
            return Err(ChunkTableError::InvalidRows(
                "embedding result could not be reconciled with page snapshot".into(),
            ));
        }

        validate_rows(&rows, self.embedding.dimension())?;
        let batch = rows_to_batch(&rows, &self.embedding)?;
        let schema = batch.schema();
        let mut merge = self.table.merge_insert(&["page_id", "chunk_id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some(page_predicate));
        merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)].into_iter(),
                schema,
            )))
            .await?;

        Ok(metrics)
    }

    async fn rows_matching(&self, predicate: String) -> Result<Vec<StoredChunk>, ChunkTableError> {
        let batches: Vec<RecordBatch> = self
            .table
            .query()
            .only_if(predicate)
            .execute()
            .await?
            .try_collect()
            .await?;
        decode_stored_chunks(&batches)
    }

    async fn scoped_rows_for_ref(
        &self,
        source_ref: &StableSourceRef,
        roots: &[String],
    ) -> Result<Vec<StoredChunk>, SourceExpansionError> {
        if roots.is_empty() {
            return Err(SourceExpansionError::OutOfScope);
        }
        let identity = reference_predicate(source_ref);
        let scoped = format!("({identity}) AND ({})", root_predicate(roots));
        let rows = self
            .rows_matching(scoped)
            .await
            .map_err(|_| SourceExpansionError::Unavailable)?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        // Distinguish an unknown ref from one outside the configured roots for
        // adapter tests and diagnostics. The MCP boundary deliberately maps both
        // cases to the same public error, so this metadata-only lookup cannot
        // become an existence oracle for callers.
        let exists = self
            .table
            .count_rows(Some(identity))
            .await
            .map_err(|_| SourceExpansionError::Unavailable)?;
        if exists == 0 {
            Err(SourceExpansionError::Missing)
        } else {
            Err(SourceExpansionError::OutOfScope)
        }
    }

    async fn expand_one(
        &self,
        source_ref: StableSourceRef,
        roots: &[String],
        max_chars: usize,
    ) -> Result<ExpandedSource, SourceExpansionError> {
        let matched = self.scoped_rows_for_ref(&source_ref, roots).await?;
        let rows = match &source_ref {
            StableSourceRef::Page(page_id) => {
                let predicate = format!(
                    "page_id = {} AND ({})",
                    sql_string(page_id),
                    root_predicate(roots)
                );
                self.rows_matching(predicate)
                    .await
                    .map_err(|_| SourceExpansionError::Unavailable)?
            }
            StableSourceRef::Chunk(chunk_id) => {
                if matched.len() != 1 {
                    return Err(SourceExpansionError::Unavailable);
                }
                let anchor = &matched[0];
                if anchor.chunk_id != *chunk_id {
                    return Err(SourceExpansionError::Unavailable);
                }
                let headings = serde_json::to_string(&anchor.heading_path)
                    .map_err(|_| SourceExpansionError::Unavailable)?;
                let predicate = format!(
                    "page_id = {} AND heading_path_json = {} AND root_page_id = {}",
                    sql_string(&anchor.page_id),
                    sql_string(&headings),
                    sql_string(&anchor.root_page_id)
                );
                self.rows_matching(predicate)
                    .await
                    .map_err(|_| SourceExpansionError::Unavailable)?
            }
        };

        if rows.is_empty() {
            return Err(SourceExpansionError::Missing);
        }
        // LanceDB preserves the persisted scan sequence here; keep it rather than
        // sorting stable IDs (which are content hashes and do not encode source
        // order). Provenance carries the exact contributing chunk sequence.
        let first = &rows[0];
        if !roots.iter().any(|root| root == &first.root_page_id)
            || rows.iter().any(|row| {
                row.page_id != first.page_id
                    || row.root_page_id != first.root_page_id
                    || row.url != first.url
                    || row.title != first.title
            })
        {
            return Err(SourceExpansionError::OutOfScope);
        }
        if let StableSourceRef::Page(page_id) = &source_ref
            && &first.page_id != page_id
        {
            return Err(SourceExpansionError::Unavailable);
        }
        if let StableSourceRef::Chunk(chunk_id) = &source_ref
            && !rows.iter().any(|row| &row.chunk_id == chunk_id)
        {
            return Err(SourceExpansionError::Unavailable);
        }

        let heading_path = match &source_ref {
            StableSourceRef::Page(_) => Vec::new(),
            StableSourceRef::Chunk(_) => first.heading_path.clone(),
        };
        let block_id = match &source_ref {
            StableSourceRef::Page(_) => None,
            StableSourceRef::Chunk(_) => first.block_id.clone(),
        };
        let chunk_ids = rows.iter().map(|row| row.chunk_id.clone()).collect();
        let (text, truncated) = bounded_join(&rows, max_chars);

        Ok(ExpandedSource {
            reference: source_ref,
            text,
            truncated,
            provenance: SourceProvenance {
                page_id: first.page_id.clone(),
                root_page_id: first.root_page_id.clone(),
                url: first.url.clone(),
                title: first.title.clone(),
                heading_path,
                block_id,
                chunk_ids,
            },
        })
    }
}

impl LexicalSearch for LanceChunkTable {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            self.lexical_search(query)
                .await
                .map_err(|_| SearchUnavailable)
        })
    }
}

impl SourceExpansion for LanceChunkTable {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_> {
        Box::pin(async move {
            if query.refs.is_empty() || query.root_page_ids.is_empty() || query.max_chars == 0 {
                return Err(SourceExpansionError::Unavailable);
            }
            let mut remaining = query.max_chars;
            let mut sources = Vec::with_capacity(query.refs.len());
            for source_ref in query.refs {
                let source = self
                    .expand_one(source_ref, &query.root_page_ids, remaining)
                    .await?;
                remaining = remaining.saturating_sub(source.text.chars().count());
                sources.push(source);
            }
            Ok(sources)
        })
    }
}

#[derive(Debug, Clone)]
struct StoredChunk {
    chunk_id: String,
    page_id: String,
    block_id: Option<String>,
    url: String,
    title: String,
    heading_path: Vec<String>,
    root_page_id: String,
    text: String,
    content_hash: String,
    vector: Vec<f32>,
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn reference_predicate(source_ref: &StableSourceRef) -> String {
    match source_ref {
        StableSourceRef::Page(page_id) => format!("page_id = {}", sql_string(page_id)),
        StableSourceRef::Chunk(chunk_id) => format!("chunk_id = {}", sql_string(chunk_id)),
    }
}

fn root_predicate(roots: &[String]) -> String {
    roots
        .iter()
        .map(|root| format!("root_page_id = {}", sql_string(root)))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn id_list_predicate(column: &str, ids: &[String]) -> String {
    ids.iter()
        .map(|id| format!("{column} = {}", sql_string(id)))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn validate_filter_ids(ids: &[String]) -> bool {
    !ids.is_empty()
        && ids.len() <= 100
        && ids.iter().all(|id| {
            !id.trim().is_empty() && id.chars().count() <= 128 && !id.chars().any(char::is_control)
        })
}

fn validate_lexical_query(query: &LexicalQuery) -> Result<(), ChunkTableError> {
    if query.query.trim().is_empty()
        || query.query.chars().count() > 4096
        || query.limit == 0
        || query.limit > 100
        || query
            .page_ids
            .as_ref()
            .is_some_and(|ids| !validate_filter_ids(ids))
        || query
            .root_page_ids
            .as_ref()
            .is_some_and(|ids| !validate_filter_ids(ids))
    {
        return Err(ChunkTableError::InvalidRows(
            "invalid lexical query or metadata filters".into(),
        ));
    }
    Ok(())
}

fn lexical_filter_predicate(query: &LexicalQuery) -> Result<Option<String>, ChunkTableError> {
    validate_lexical_query(query)?;
    let mut predicates = Vec::new();
    if let Some(page_ids) = &query.page_ids {
        predicates.push(format!("({})", id_list_predicate("page_id", page_ids)));
    }
    if let Some(root_page_ids) = &query.root_page_ids {
        predicates.push(format!(
            "({})",
            id_list_predicate("root_page_id", root_page_ids)
        ));
    }
    Ok((!predicates.is_empty()).then(|| predicates.join(" AND ")))
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn quoted_phrase(query: &str) -> Option<&str> {
    let query = query.trim();
    let phrase = query.strip_prefix('"')?.strip_suffix('"')?.trim();
    (!phrase.is_empty() && !phrase.contains('"')).then_some(phrase)
}

fn hit_matches_phrase(column: &str, hit: &FtsIndexHit, phrase: &str) -> bool {
    match column {
        "chunk_id" => hit.chunk_id == phrase,
        "page_id" => hit.page_id == phrase,
        "title" => contains_case_insensitive(&hit.title, phrase),
        "text" => contains_case_insensitive(&hit.text, phrase),
        _ => false,
    }
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, ChunkTableError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ChunkTableError::InvalidSchema(format!("missing UTF-8 column {name}")))
}

fn float32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float32Array, ChunkTableError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| ChunkTableError::InvalidSchema(format!("missing float32 column {name}")))
}

fn decode_vector_hits(batches: &[RecordBatch]) -> Result<Vec<VectorIndexHit>, ChunkTableError> {
    let mut hits = Vec::new();
    for batch in batches {
        let chunk_ids = string_column(batch, "chunk_id")?;
        let page_ids = string_column(batch, "page_id")?;
        let roots = string_column(batch, "root_page_id")?;
        let block_ids = string_column(batch, "block_id")?;
        let urls = string_column(batch, "url")?;
        let titles = string_column(batch, "title")?;
        let headings = string_column(batch, "heading_path_json")?;
        let texts = string_column(batch, "text")?;
        let distances = float32_column(batch, "_distance")?;
        for row in 0..batch.num_rows() {
            if distances.is_null(row) || !distances.value(row).is_finite() {
                return Err(ChunkTableError::InvalidRows(
                    "vector result distance must be finite".into(),
                ));
            }
            hits.push(VectorIndexHit {
                chunk_id: chunk_ids.value(row).to_owned(),
                page_id: page_ids.value(row).to_owned(),
                root_page_id: roots.value(row).to_owned(),
                block_id: (!block_ids.is_null(row)).then(|| block_ids.value(row).to_owned()),
                url: urls.value(row).to_owned(),
                title: titles.value(row).to_owned(),
                heading_path: serde_json::from_str(headings.value(row))?,
                text: texts.value(row).to_owned(),
                distance: distances.value(row),
            });
        }
    }
    Ok(hits)
}

fn decode_fts_hits(batches: &[RecordBatch]) -> Result<Vec<FtsIndexHit>, ChunkTableError> {
    let mut hits = Vec::new();
    for batch in batches {
        let chunk_ids = string_column(batch, "chunk_id")?;
        let page_ids = string_column(batch, "page_id")?;
        let roots = string_column(batch, "root_page_id")?;
        let block_ids = string_column(batch, "block_id")?;
        let urls = string_column(batch, "url")?;
        let titles = string_column(batch, "title")?;
        let headings = string_column(batch, "heading_path_json")?;
        let texts = string_column(batch, "text")?;
        let scores = float32_column(batch, "_score")?;
        for row in 0..batch.num_rows() {
            if scores.is_null(row) || !scores.value(row).is_finite() {
                return Err(ChunkTableError::InvalidRows(
                    "FTS result score must be finite".into(),
                ));
            }
            hits.push(FtsIndexHit {
                chunk_id: chunk_ids.value(row).to_owned(),
                page_id: page_ids.value(row).to_owned(),
                root_page_id: roots.value(row).to_owned(),
                block_id: (!block_ids.is_null(row)).then(|| block_ids.value(row).to_owned()),
                url: urls.value(row).to_owned(),
                title: titles.value(row).to_owned(),
                heading_path: serde_json::from_str(headings.value(row))?,
                text: texts.value(row).to_owned(),
                score: scores.value(row),
            });
        }
    }
    Ok(hits)
}

fn vector_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeListArray, ChunkTableError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeListArray>())
        .ok_or_else(|| ChunkTableError::InvalidSchema(format!("missing vector column {name}")))
}

fn decode_vector(column: &FixedSizeListArray, row: usize) -> Result<Vec<f32>, ChunkTableError> {
    if column.is_null(row) {
        return Err(ChunkTableError::InvalidRows(
            "persisted vector must not be null".into(),
        ));
    }
    let values = column.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| ChunkTableError::InvalidSchema("vector values must be float32".into()))?;
    if values.null_count() != 0 || values.iter().flatten().any(|value| !value.is_finite()) {
        return Err(ChunkTableError::InvalidRows(
            "persisted vector contains null or non-finite values".into(),
        ));
    }
    Ok(values.values().to_vec())
}

fn decode_stored_chunks(batches: &[RecordBatch]) -> Result<Vec<StoredChunk>, ChunkTableError> {
    let mut rows = Vec::new();
    for batch in batches {
        let chunk_ids = string_column(batch, "chunk_id")?;
        let page_ids = string_column(batch, "page_id")?;
        let block_ids = string_column(batch, "block_id")?;
        let urls = string_column(batch, "url")?;
        let titles = string_column(batch, "title")?;
        let headings = string_column(batch, "heading_path_json")?;
        let roots = string_column(batch, "root_page_id")?;
        let texts = string_column(batch, "text")?;
        let hashes = string_column(batch, "content_hash")?;
        let vectors = vector_column(batch, "vector")?;
        for row in 0..batch.num_rows() {
            rows.push(StoredChunk {
                chunk_id: chunk_ids.value(row).to_owned(),
                page_id: page_ids.value(row).to_owned(),
                block_id: (!block_ids.is_null(row)).then(|| block_ids.value(row).to_owned()),
                url: urls.value(row).to_owned(),
                title: titles.value(row).to_owned(),
                heading_path: serde_json::from_str(headings.value(row))?,
                root_page_id: roots.value(row).to_owned(),
                text: texts.value(row).to_owned(),
                content_hash: hashes.value(row).to_owned(),
                vector: decode_vector(vectors, row)?,
            });
        }
    }
    Ok(rows)
}

fn bounded_join(rows: &[StoredChunk], max_chars: usize) -> (String, bool) {
    let mut output = String::new();
    let mut remaining = max_chars;
    let mut truncated = false;

    for (index, row) in rows.iter().enumerate() {
        let separator = if index == 0 { "" } else { "\n\n" };
        for part in [separator, row.text.as_str()] {
            if part.is_empty() {
                continue;
            }
            let count = part.chars().count();
            if count <= remaining {
                output.push_str(part);
                remaining -= count;
                continue;
            }
            output.extend(part.chars().take(remaining));
            remaining = 0;
            truncated = true;
            break;
        }
        if remaining == 0 {
            if index + 1 < rows.len()
                || output.chars().count()
                    < rows.iter().map(|r| r.text.chars().count()).sum::<usize>()
            {
                truncated = true;
            }
            break;
        }
    }

    (output, truncated)
}

fn validate_table_name(table_name: &str) -> Result<(), ChunkTableError> {
    if table_name.trim().is_empty() || table_name.chars().any(char::is_control) {
        return Err(ChunkTableError::InvalidSchema(
            "table name must be nonempty and contain no control characters".into(),
        ));
    }
    Ok(())
}

fn local_database_uri(path: &Path) -> Result<String, ChunkTableError> {
    let absolute = std::path::absolute(path)?;
    absolute
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or(ChunkTableError::InvalidPath)
}

fn schema_version(value: SchemaVersion) -> &'static str {
    match value {
        SchemaVersion::V1 => CANONICAL_CHUNK_SCHEMA_VERSION,
    }
}

fn chunk_schema(embedding: &EmbeddingMetadata) -> Result<SchemaRef, ChunkTableError> {
    let dimension = i32::try_from(embedding.dimension()).map_err(|_| {
        ChunkTableError::InvalidSchema("embedding dimension exceeds Arrow list limit".into())
    })?;
    let vector = DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dimension,
    );
    let metadata = HashMap::from([
        (
            TABLE_SCHEMA_KEY.to_string(),
            CHUNK_TABLE_SCHEMA_VERSION.to_string(),
        ),
        (
            CANONICAL_SCHEMA_KEY.to_string(),
            CANONICAL_CHUNK_SCHEMA_VERSION.to_string(),
        ),
        (EMBEDDING_KEY.to_string(), serde_json::to_string(embedding)?),
    ]);
    Ok(Arc::new(
        Schema::new(vec![
            Field::new("schema_version", DataType::Utf8, false),
            Field::new("chunk_id", DataType::Utf8, false),
            Field::new("page_id", DataType::Utf8, false),
            Field::new("block_id", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("heading_path_json", DataType::Utf8, false),
            Field::new("last_edited_time", DataType::Utf8, false),
            Field::new("workspace_id", DataType::Utf8, false),
            Field::new("root_page_id", DataType::Utf8, false),
            Field::new("database_id", DataType::Utf8, true),
            Field::new("data_source_id", DataType::Utf8, true),
            Field::new("properties_json", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("content_hash", DataType::Utf8, false),
            Field::new("links_json", DataType::Utf8, false),
            Field::new("vector", vector, false),
        ])
        .with_metadata(metadata),
    ))
}

async fn validate_table_schema(
    table: &Table,
    expected_embedding: &EmbeddingMetadata,
) -> Result<(), ChunkTableError> {
    let actual = table.schema().await?;
    let expected = chunk_schema(expected_embedding)?;
    if actual.fields().len() != expected.fields().len() {
        return Err(ChunkTableError::InvalidSchema(format!(
            "expected {} columns, found {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }
    for (actual_field, expected_field) in actual.fields().iter().zip(expected.fields()) {
        if actual_field.name() != expected_field.name()
            || actual_field.data_type() != expected_field.data_type()
            || actual_field.is_nullable() != expected_field.is_nullable()
        {
            return Err(ChunkTableError::InvalidSchema(format!(
                "column {} does not match the production contract",
                expected_field.name()
            )));
        }
    }
    if actual.metadata().get(TABLE_SCHEMA_KEY).map(String::as_str)
        != Some(CHUNK_TABLE_SCHEMA_VERSION)
        || actual
            .metadata()
            .get(CANONICAL_SCHEMA_KEY)
            .map(String::as_str)
            != Some(CANONICAL_CHUNK_SCHEMA_VERSION)
    {
        return Err(ChunkTableError::InvalidSchema(
            "missing or unsupported persisted schema version".into(),
        ));
    }
    let persisted_embedding: EmbeddingMetadata =
        serde_json::from_str(actual.metadata().get(EMBEDDING_KEY).ok_or_else(|| {
            ChunkTableError::InvalidSchema("missing persisted embedding identity".into())
        })?)
        .map_err(|_| {
            ChunkTableError::InvalidSchema("invalid persisted embedding identity".into())
        })?;
    persisted_embedding.ensure_compatible(expected_embedding)?;
    Ok(())
}

fn validate_page_snapshot(page_id: &str, chunks: &[IndexedChunk]) -> Result<(), ChunkTableError> {
    if page_id.trim().is_empty() || page_id.chars().any(char::is_control) {
        return Err(ChunkTableError::InvalidRows(
            "page_id must be nonempty and contain no control characters".into(),
        ));
    }
    let mut ids = HashSet::with_capacity(chunks.len());
    for chunk in chunks {
        if chunk.metadata.page_id != page_id {
            return Err(ChunkTableError::InvalidRows(format!(
                "chunk {} belongs to a different page",
                chunk.chunk_id
            )));
        }
        if chunk.chunk_id.trim().is_empty()
            || chunk.content_hash.trim().is_empty()
            || !ids.insert(chunk.chunk_id.as_str())
        {
            return Err(ChunkTableError::InvalidRows(
                "page snapshot requires unique nonempty chunk IDs and content hashes".into(),
            ));
        }
    }
    Ok(())
}

fn validate_rows(rows: &[EmbeddedChunk], dimension: usize) -> Result<(), ChunkTableError> {
    let mut ids = HashSet::with_capacity(rows.len());
    for row in rows {
        if row.chunk.chunk_id.trim().is_empty() {
            return Err(ChunkTableError::InvalidRows(
                "chunk_id must be nonempty".into(),
            ));
        }
        if !ids.insert(row.chunk.chunk_id.as_str()) {
            return Err(ChunkTableError::InvalidRows(format!(
                "duplicate chunk_id in one upsert batch: {}",
                row.chunk.chunk_id
            )));
        }
        if row.vector.len() != dimension || row.vector.iter().any(|value| !value.is_finite()) {
            return Err(ChunkTableError::InvalidRows(format!(
                "chunk {} has an incompatible vector",
                row.chunk.chunk_id
            )));
        }
    }
    Ok(())
}

fn rows_to_batch(
    rows: &[EmbeddedChunk],
    embedding: &EmbeddingMetadata,
) -> Result<RecordBatch, ChunkTableError> {
    let headings = rows
        .iter()
        .map(|row| serde_json::to_string(&row.chunk.metadata.heading_path))
        .collect::<Result<Vec<_>, _>>()?;
    let properties = rows
        .iter()
        .map(|row| serde_json::to_string(&row.chunk.metadata.properties))
        .collect::<Result<Vec<_>, _>>()?;
    let links = rows
        .iter()
        .map(|row| serde_json::to_string(&row.chunk.links))
        .collect::<Result<Vec<_>, _>>()?;
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.iter().map(|row| {
            Some(
                row.vector
                    .iter()
                    .copied()
                    .map(Some)
                    .collect::<Vec<Option<f32>>>(),
            )
        }),
        i32::try_from(embedding.dimension()).map_err(|_| {
            ChunkTableError::InvalidSchema("embedding dimension exceeds Arrow list limit".into())
        })?,
    );
    let schema = chunk_schema(embedding)?;
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| schema_version(row.chunk.schema_version))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.chunk_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.page_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.block_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.url.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.title.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                headings.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.last_edited_time.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.source.workspace_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.source.root_page_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.source.database_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.metadata.source.data_source_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                properties.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.text.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.chunk.content_hash.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                links.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lancedb::query::{ExecutableQuery, QueryBase};
    use notion_knowledge_core::{
        embedding::{EmbeddingFuture, EmbeddingProvider},
        indexed::{IndexedMetadata, SourceMetadata},
    };

    use super::*;

    fn embedding(revision: &str, dimension: usize) -> EmbeddingMetadata {
        EmbeddingMetadata::new(
            "test-provider".into(),
            "test-model".into(),
            revision.into(),
            dimension,
        )
        .expect("valid embedding metadata")
    }

    fn chunk(root_page_id: &str, title: &str, text: &str) -> IndexedChunk {
        IndexedChunk {
            schema_version: SchemaVersion::V1,
            chunk_id: "stable-chunk".into(),
            metadata: IndexedMetadata {
                page_id: "page-1".into(),
                block_id: Some("block-1".into()),
                url: "https://example.invalid/page-1".into(),
                title: title.into(),
                heading_path: vec!["Section".into()],
                last_edited_time: "2026-10-05T12:00:00Z".into(),
                source: SourceMetadata {
                    workspace_id: "workspace-1".into(),
                    root_page_id: root_page_id.into(),
                    database_id: Some("database-1".into()),
                    data_source_id: Some("source-1".into()),
                },
                properties: BTreeMap::new(),
            },
            text: text.into(),
            content_hash: format!("hash:{text}"),
            links: vec![],
        }
    }

    fn temp_database(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "notion-knowledge-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    struct CountingProvider {
        metadata: EmbeddingMetadata,
        embedded_inputs: AtomicUsize,
        calls: AtomicUsize,
        fail: AtomicBool,
    }

    impl CountingProvider {
        fn new(metadata: EmbeddingMetadata) -> Self {
            Self {
                metadata,
                embedded_inputs: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
                fail: AtomicBool::new(false),
            }
        }

        fn embedded_inputs(&self) -> usize {
            self.embedded_inputs.load(Ordering::SeqCst)
        }

        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, Ordering::SeqCst);
        }
    }

    impl EmbeddingProvider for CountingProvider {
        fn metadata(&self) -> &EmbeddingMetadata {
            &self.metadata
        }

        fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.embedded_inputs
                .fetch_add(inputs.len(), Ordering::SeqCst);
            let fail = self.fail.load(Ordering::SeqCst);
            Box::pin(async move {
                if fail {
                    return Err(EmbeddingError::Unavailable);
                }
                Ok(inputs
                    .iter()
                    .map(|text| vec![text.len() as f32, 1.0, 0.0])
                    .collect())
            })
        }
    }

    fn page_chunk(chunk_id: &str, text: &str) -> IndexedChunk {
        let mut value = chunk("root", "Page title", text);
        value.chunk_id = chunk_id.into();
        value
    }

    fn fts_chunk(page_id: &str, chunk_id: &str, title: &str, text: &str) -> IndexedChunk {
        let mut value = chunk("root", title, text);
        value.chunk_id = chunk_id.into();
        value.metadata.page_id = page_id.into();
        value.metadata.url = format!("https://example.invalid/{page_id}");
        value
    }

    #[tokio::test]
    async fn create_and_open_from_scratch_preserves_explicit_schema() {
        let path = temp_database("schema");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create empty chunk table");
        assert_eq!(table.count_rows().await.expect("count rows"), 0);

        let schema = table.table.schema().await.expect("read schema");
        let vector = schema.field_with_name("vector").expect("vector column");
        assert!(matches!(
            vector.data_type(),
            DataType::FixedSizeList(item, 3) if item.data_type() == &DataType::Float32
        ));
        assert_eq!(
            schema.metadata().get(TABLE_SCHEMA_KEY).map(String::as_str),
            Some(CHUNK_TABLE_SCHEMA_VERSION)
        );
        assert_eq!(
            schema
                .metadata()
                .get(CANONICAL_SCHEMA_KEY)
                .map(String::as_str),
            Some(CANONICAL_CHUNK_SCHEMA_VERSION)
        );
        let persisted_embedding: EmbeddingMetadata = serde_json::from_str(
            schema
                .metadata()
                .get(EMBEDDING_KEY)
                .expect("persisted embedding identity"),
        )
        .expect("valid persisted embedding identity");
        assert_eq!(persisted_embedding.model_id(), "test-model");
        assert_eq!(persisted_embedding.dimension(), 3);
        assert_eq!(persisted_embedding, metadata);
        assert!(schema.field_with_name("page_id").is_ok());
        assert!(schema.field_with_name("root_page_id").is_ok());
        assert!(schema.field_with_name("text").is_ok());

        drop(table);
        let reopened = LanceChunkTable::open(&path, "chunks", metadata.clone())
            .await
            .expect("open persisted table");
        assert_eq!(reopened.embedding_metadata(), &metadata);
        assert_eq!(reopened.count_rows().await.expect("count reopened rows"), 0);
        drop(reopened);

        assert!(matches!(
            LanceChunkTable::open(&path, "chunks", embedding("revision-2", 3)).await,
            Err(ChunkTableError::Embedding(
                EmbeddingError::IncompatibleIndex
            ))
        ));

        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn semantic_search_embeds_once_and_filters_before_top_k_without_vectors() {
        let path = temp_database("semantic-fixture");
        let metadata = embedding("revision-1", 3);
        let table = Arc::new(
            LanceChunkTable::create(&path, "chunks", metadata.clone())
                .await
                .unwrap(),
        );
        let mut allowed = fts_chunk(
            "allowed'page",
            "allowed",
            "Swedish",
            "Säkerhetskopior körs varje natt.",
        );
        allowed.metadata.source.root_page_id = "scope".into();
        let rows = vec![
            EmbeddedChunk::new(
                fts_chunk("excluded", "nearest", "English", "Backups run nightly."),
                vec![1.0, 1.0, 0.0],
            ),
            EmbeddedChunk::new(allowed, vec![2.0, 1.0, 0.0]),
            EmbeddedChunk::new(
                fts_chunk("other", "far", "Other", "Bread baking."),
                vec![8.0, 1.0, 0.0],
            ),
        ];
        table.upsert(&metadata, &rows).await.unwrap();
        table
            .ensure_vector_index(&vector_config(VectorDistance::L2))
            .await
            .unwrap();
        let provider = Arc::new(CountingProvider::new(metadata));
        let search = LanceSemanticSearch::new(table, provider.clone(), 1).unwrap();
        let hits = search
            .search(SemanticQuery {
                query: "x".into(),
                limit: 1,
                page_ids: Some(vec!["allowed'page".into()]),
                root_page_ids: Some(vec!["scope".into()]),
            })
            .await
            .unwrap();
        assert_eq!(provider.embedded_inputs(), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].source.chunk_id, "allowed");
        assert_eq!(hits[0].source.page_id, "allowed'page");
        assert!(hits[0].score.is_finite());
        assert_eq!(hits[0].score, -1.0);
        let json = serde_json::to_value(&hits).unwrap();
        assert_eq!(json[0].as_object().unwrap().len(), 3);
        assert!(json[0].get("vector").is_none());
        assert!(json[0].get("distance").is_none());
        let hits = search
            .search(SemanticQuery {
                query: "x".into(),
                limit: 2,
                page_ids: None,
                root_page_ids: None,
            })
            .await
            .unwrap();
        assert_eq!(provider.embedded_inputs(), 2);
        assert_eq!(hits[0].source.chunk_id, "nearest");
        assert!(hits[0].score > hits[1].score);
        assert!(
            search
                .search(SemanticQuery {
                    query: "".into(),
                    limit: 1,
                    page_ids: None,
                    root_page_ids: None
                })
                .await
                .is_err()
        );
        assert_eq!(provider.embedded_inputs(), 2);
        search
            .table
            .rebuild_vector_index(&vector_config(VectorDistance::Cosine))
            .await
            .unwrap();
        let cosine = search
            .search(SemanticQuery {
                query: "x".into(),
                limit: 2,
                page_ids: None,
                root_page_ids: None,
            })
            .await
            .unwrap();
        assert_eq!(cosine[0].source.chunk_id, "nearest");
        assert!(cosine[0].score > cosine[1].score);
        assert!(cosine.iter().all(|hit| hit.score.is_finite()));
        let scoped = search
            .search(SemanticQuery {
                query: "x".into(),
                limit: 2,
                page_ids: Some(vec!["missing".into(), "allowed'page".into()]),
                root_page_ids: Some(vec!["wrong".into(), "scope".into()]),
            })
            .await
            .unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].source.chunk_id, "allowed");
        let empty = search
            .search(SemanticQuery {
                query: "x".into(),
                limit: 2,
                page_ids: Some(vec!["allowed'page".into()]),
                root_page_ids: Some(vec!["wrong".into()]),
            })
            .await
            .unwrap();
        assert!(empty.is_empty());
        provider.set_fail(true);
        assert!(
            search
                .search(SemanticQuery {
                    query: "x".into(),
                    limit: 1,
                    page_ids: None,
                    root_page_ids: None
                })
                .await
                .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn semantic_composition_rejects_same_dimension_different_model_identity() {
        let path = temp_database("semantic-identity");
        let table = Arc::new(
            LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
                .await
                .unwrap(),
        );
        let provider = Arc::new(CountingProvider::new(embedding("revision-2", 3)));
        assert!(LanceSemanticSearch::new(table.clone(), provider, 1).is_err());
        assert!(
            LanceSemanticSearch::new(
                table,
                Arc::new(CountingProvider::new(embedding("revision-1", 3))),
                0
            )
            .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn vector_config(distance: VectorDistance) -> VectorIndexConfig {
        VectorIndexConfig {
            distance,
            num_partitions: Some(1),
            sample_rate: 8,
            max_iterations: 20,
        }
    }

    #[tokio::test]
    async fn vector_index_builds_from_initial_crawl_and_returns_stable_nearest_neighbors() {
        let path = temp_database("vector-index");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");

        let rows = vec![
            EmbeddedChunk::new(
                fts_chunk("page-a", "chunk-a", "A", "nearest a"),
                vec![1.0, 0.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk("page-b", "chunk-b", "B", "nearest b"),
                vec![0.8, 0.2, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk("page-c", "chunk-c", "C", "nearest c"),
                vec![0.0, 1.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk("page-d", "chunk-d", "D", "nearest d"),
                vec![0.0, 0.0, 1.0],
            ),
        ];
        table
            .upsert(&metadata, &rows)
            .await
            .expect("seed crawl rows");

        let config = vector_config(VectorDistance::L2);
        assert_eq!(
            table
                .ensure_vector_index(&config)
                .await
                .expect("build vector index"),
            VectorIndexAction::Created
        );

        let index = table
            .table
            .list_indices()
            .await
            .expect("list indices")
            .into_iter()
            .find(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
            .expect("vector index");
        assert_eq!(index.index_type, IndexType::IvfFlat);
        assert_eq!(index.columns, vec!["vector".to_string()]);

        let stats = table
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await
            .expect("vector stats")
            .expect("vector stats exist");
        assert_eq!(stats.num_indexed_rows, 4);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert!(matches!(stats.distance_type, Some(DistanceType::L2)));

        let hits = table
            .vector_query(&[1.0, 0.0, 0.0], 3, 1)
            .await
            .expect("nearest-neighbor query");
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].chunk_id, "chunk-a");
        assert_eq!(hits[1].chunk_id, "chunk-b");
        assert_eq!(hits[0].page_id, "page-a");
        assert_eq!(hits[0].url, "https://example.invalid/page-a");
        assert_eq!(hits[0].heading_path, vec!["Section"]);
        assert_eq!(hits[0].block_id.as_deref(), Some("block-1"));
        assert!(hits[0].distance <= hits[1].distance);
        assert!(hits[1].distance <= hits[2].distance);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn vector_index_ensure_is_idempotent_and_rebuild_validates_before_replace() {
        let path = temp_database("vector-rebuild");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        table
            .upsert(
                &metadata,
                &[
                    EmbeddedChunk::new(page_chunk("chunk-a", "one"), vec![1.0, 0.0, 0.0]),
                    EmbeddedChunk::new(page_chunk("chunk-b", "two"), vec![0.0, 1.0, 0.0]),
                ],
            )
            .await
            .expect("seed rows");

        let l2 = vector_config(VectorDistance::L2);
        assert_eq!(
            table.ensure_vector_index(&l2).await.expect("first ensure"),
            VectorIndexAction::Created
        );
        assert_eq!(
            table.ensure_vector_index(&l2).await.expect("second ensure"),
            VectorIndexAction::Existing
        );

        let cosine = vector_config(VectorDistance::Cosine);
        assert!(matches!(
            table.ensure_vector_index(&cosine).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));

        let invalid = VectorIndexConfig {
            num_partitions: Some(0),
            ..l2.clone()
        };
        assert!(matches!(
            table.rebuild_vector_index(&invalid).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));
        let stats = table
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await
            .expect("stats after rejected rebuild")
            .expect("existing vector index");
        assert!(matches!(stats.distance_type, Some(DistanceType::L2)));

        table
            .rebuild_vector_index(&cosine)
            .await
            .expect("explicit cosine rebuild");
        let stats = table
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await
            .expect("stats after rebuild")
            .expect("rebuilt vector index");
        assert!(matches!(stats.distance_type, Some(DistanceType::Cosine)));

        let hits = table
            .vector_query(&[1.0, 0.0, 0.0], 1, 1)
            .await
            .expect("query rebuilt index");
        assert_eq!(hits[0].chunk_id, "chunk-a");
        assert!(matches!(
            table.vector_query(&[0.0, 0.0, 0.0], 1, 1).await,
            Err(ChunkTableError::InvalidRows(_))
        ));

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn optimize_vector_index_folds_incremental_rows_into_existing_index() {
        let path = temp_database("vector-optimize");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        table
            .upsert(
                &metadata,
                &[
                    EmbeddedChunk::new(page_chunk("chunk-a", "one"), vec![1.0, 0.0, 0.0]),
                    EmbeddedChunk::new(page_chunk("chunk-b", "two"), vec![0.0, 1.0, 0.0]),
                ],
            )
            .await
            .expect("seed initial rows");

        let config = vector_config(VectorDistance::L2);
        table
            .ensure_vector_index(&config)
            .await
            .expect("build initial index");

        let mut added = page_chunk("chunk-c", "three");
        added.metadata.page_id = "page-3".into();
        added.metadata.url = "https://example.invalid/page-3".into();
        table
            .upsert(&metadata, &[EmbeddedChunk::new(added, vec![0.0, 0.0, 1.0])])
            .await
            .expect("append incremental row");

        let before = table
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await
            .expect("stats before optimize")
            .expect("vector index");
        assert!(before.num_unindexed_rows >= 1);

        assert_eq!(
            table
                .optimize_vector_index(&config)
                .await
                .expect("optimize vector index"),
            VectorIndexAction::Existing
        );
        let after = table
            .table
            .index_stats(CHUNK_VECTOR_INDEX_NAME)
            .await
            .expect("stats after optimize")
            .expect("vector index");
        assert_eq!(after.num_unindexed_rows, 0);
        assert_eq!(after.num_indexed_rows, 3);

        let hits = table
            .vector_query(&[0.0, 0.0, 1.0], 1, 1)
            .await
            .expect("query optimized index");
        assert_eq!(hits[0].chunk_id, "chunk-c");

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn reserved_vector_index_name_with_wrong_layout_fails_closed() {
        let path = temp_database("vector-name-collision");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        table
            .upsert(
                &metadata,
                &[EmbeddedChunk::new(
                    page_chunk("chunk-a", "one"),
                    vec![1.0, 0.0, 0.0],
                )],
            )
            .await
            .expect("seed row");

        let params = FtsIndexBuilder::default();
        table
            .table
            .create_index(&["text"], Index::FTS(params))
            .name(CHUNK_VECTOR_INDEX_NAME.to_string())
            .execute()
            .await
            .expect("create conflicting reserved index");

        let config = vector_config(VectorDistance::L2);
        assert!(matches!(
            table.ensure_vector_index(&config).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));
        assert!(matches!(
            table.rebuild_vector_index(&config).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));

        let index = table
            .table
            .list_indices()
            .await
            .expect("list indices")
            .into_iter()
            .find(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
            .expect("conflicting index remains");
        assert_eq!(index.index_type, IndexType::FTS);
        assert_eq!(index.columns, vec!["text".to_string()]);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn vector_index_and_query_validation_fail_closed() {
        let path = temp_database("vector-validation");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata)
            .await
            .expect("create table");

        assert!(matches!(
            table
                .ensure_vector_index(&VectorIndexConfig::default())
                .await,
            Err(ChunkTableError::InvalidRows(_))
        ));
        assert!(matches!(
            table.vector_query(&[1.0, 0.0, 0.0], 1, 1).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));
        assert!(matches!(
            table.vector_query(&[1.0, 0.0], 1, 1).await,
            Err(ChunkTableError::InvalidRows(_))
        ));
        assert!(matches!(
            table.vector_query(&[f32::NAN, 0.0, 0.0], 1, 1).await,
            Err(ChunkTableError::InvalidRows(_))
        ));

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn create_installs_versioned_fts_indices_for_text_title_and_identifiers() {
        let path = temp_database("fts-indexes");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table with FTS indices");

        let indices = table.table.list_indices().await.expect("list indices");
        for (expected_name, expected_column) in CHUNK_FTS_INDEXES {
            let index = indices
                .iter()
                .find(|index| index.name == expected_name)
                .unwrap_or_else(|| panic!("missing FTS index {expected_name}"));
            assert_eq!(index.columns, vec![expected_column.to_string()]);
        }

        table
            .ensure_fts_index(&FtsIndexConfig::default())
            .await
            .expect("ensure existing indices is idempotent");
        assert_eq!(
            table
                .table
                .list_indices()
                .await
                .expect("list indices")
                .len(),
            CHUNK_FTS_INDEXES.len()
        );

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn fts_index_retrieves_swedish_english_titles_and_exact_identifiers() {
        let path = temp_database("fts-search");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");

        let rows = vec![
            EmbeddedChunk::new(
                fts_chunk(
                    "page-98765",
                    "nk-chunk-v1:4821abcdef",
                    "Projekt Aurora",
                    "Bilen har snabb laddning i Lund och fungerar bra på vintern.",
                ),
                vec![1.0, 0.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk(
                    "page-12345",
                    "nk-chunk-v1:7777abcd",
                    "Telemetry notes",
                    "The charging telemetry pipeline records vehicle sessions.",
                ),
                vec![0.0, 1.0, 0.0],
            ),
        ];
        table
            .upsert(&metadata, &rows)
            .await
            .expect("insert FTS fixtures");
        table
            .optimize_fts_index()
            .await
            .expect("index inserted rows");

        let swedish = table
            .fts_query("text", "bilar", 10)
            .await
            .expect("Swedish stemming query");
        assert_eq!(swedish.len(), 1);
        assert_eq!(swedish[0].chunk_id, "nk-chunk-v1:4821abcdef");
        assert!(swedish[0].score.is_finite());

        let english = table
            .fts_query("text", "telemetry", 10)
            .await
            .expect("English text query");
        assert_eq!(english.len(), 1);
        assert_eq!(english[0].chunk_id, "nk-chunk-v1:7777abcd");

        let title = table
            .fts_query("title", "Aurora", 10)
            .await
            .expect("title query");
        assert_eq!(title.len(), 1);
        assert_eq!(title[0].page_id, "page-98765");

        let page_id = table
            .fts_query("page_id", "page-98765", 10)
            .await
            .expect("page identifier query");
        assert_eq!(page_id.len(), 1);
        assert_eq!(page_id[0].chunk_id, "nk-chunk-v1:4821abcdef");

        let chunk_id = table
            .fts_query("chunk_id", "nk-chunk-v1:4821abcdef", 10)
            .await
            .expect("chunk identifier query");
        assert_eq!(chunk_id.len(), 1);
        assert_eq!(chunk_id[0].page_id, "page-98765");

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn lexical_search_ranks_exact_names_and_preserves_provenance() {
        let path = temp_database("lexical-ranking");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");

        let rows = vec![
            EmbeddedChunk::new(
                fts_chunk(
                    "page-title",
                    "chunk-title",
                    "Projekt Aurora",
                    "Ordinary body text.",
                ),
                vec![1.0, 0.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk(
                    "page-body",
                    "chunk-body",
                    "Other page",
                    "Aurora appears only in this body.",
                ),
                vec![0.0, 1.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk(
                    "page-id",
                    "nk-chunk-v1:aurora",
                    "Identifier page",
                    "No lexical name in the body.",
                ),
                vec![0.0, 0.0, 1.0],
            ),
        ];
        table
            .upsert(&metadata, &rows)
            .await
            .expect("seed lexical rows");
        table
            .optimize_fts_index()
            .await
            .expect("optimize lexical indices");

        let name_hits = LexicalSearch::search(
            &table,
            LexicalQuery {
                query: "Aurora".into(),
                limit: 10,
                page_ids: None,
                root_page_ids: None,
            },
        )
        .await
        .expect("search name");
        assert_eq!(name_hits.len(), 2);
        assert_eq!(name_hits[0].source.chunk_id, "chunk-title");
        assert_eq!(name_hits[0].source.title, "Projekt Aurora");
        assert_eq!(
            name_hits[0].source.url,
            "https://example.invalid/page-title"
        );
        assert_eq!(name_hits[0].source.heading_path, vec!["Section"]);
        assert_eq!(name_hits[0].source.block_id.as_deref(), Some("block-1"));
        assert!(name_hits[0].score > name_hits[1].score);

        let id_hits = LexicalSearch::search(
            &table,
            LexicalQuery {
                query: "nk-chunk-v1:aurora".into(),
                limit: 10,
                page_ids: None,
                root_page_ids: None,
            },
        )
        .await
        .expect("search exact chunk id");
        assert!(!id_hits.is_empty());
        assert_eq!(id_hits[0].source.page_id, "page-id");
        assert_eq!(id_hits[0].source.chunk_id, "nk-chunk-v1:aurora");

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn lexical_search_applies_combined_metadata_filters_with_escaped_ids() {
        let path = temp_database("lexical-filters");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");

        let mut quoted = fts_chunk(
            "page-'one",
            "chunk-one",
            "Quoted identifiers",
            "needle appears here",
        );
        quoted.metadata.source.root_page_id = "root-'alpha".into();

        let mut other = fts_chunk(
            "page-two",
            "chunk-two",
            "Other root",
            "needle appears here too",
        );
        other.metadata.source.root_page_id = "root-beta".into();

        table
            .upsert(
                &metadata,
                &[
                    EmbeddedChunk::new(quoted, vec![1.0, 0.0, 0.0]),
                    EmbeddedChunk::new(other, vec![0.0, 1.0, 0.0]),
                ],
            )
            .await
            .expect("seed filtered rows");
        table
            .optimize_fts_index()
            .await
            .expect("optimize lexical indices");

        let query = LexicalQuery {
            query: "needle".into(),
            limit: 10,
            page_ids: Some(vec!["page-'one".into()]),
            root_page_ids: Some(vec!["root-'alpha".into()]),
        };
        assert_eq!(
            lexical_filter_predicate(&query).expect("filter predicate"),
            Some("(page_id = 'page-''one') AND (root_page_id = 'root-''alpha')".into())
        );

        let hits = LexicalSearch::search(&table, query)
            .await
            .expect("filtered lexical search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].source.page_id, "page-'one");
        assert_eq!(hits[0].source.chunk_id, "chunk-one");

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn lexical_search_supports_phrases_and_bounds_returned_text() {
        let path = temp_database("lexical-phrases");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");

        let long_tail = "x".repeat(2200);
        let rows = vec![
            EmbeddedChunk::new(
                fts_chunk(
                    "phrase-page",
                    "phrase-chunk",
                    "Phrase fixture",
                    &format!("charging telemetry {long_tail}"),
                ),
                vec![1.0, 0.0, 0.0],
            ),
            EmbeddedChunk::new(
                fts_chunk(
                    "reversed-page",
                    "reversed-chunk",
                    "Reversed fixture",
                    "telemetry noise charging",
                ),
                vec![0.0, 1.0, 0.0],
            ),
        ];
        table
            .upsert(&metadata, &rows)
            .await
            .expect("seed phrase rows");
        table
            .optimize_fts_index()
            .await
            .expect("optimize phrase indices");

        let hits = LexicalSearch::search(
            &table,
            LexicalQuery {
                query: "\"charging telemetry\"".into(),
                limit: 10,
                page_ids: None,
                root_page_ids: None,
            },
        )
        .await
        .expect("phrase search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].source.chunk_id, "phrase-chunk");
        assert_eq!(hits[0].text.chars().count(), 2000);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn lexical_search_rejects_invalid_direct_port_arguments() {
        let path = temp_database("lexical-validation");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");

        for query in [
            LexicalQuery {
                query: " ".into(),
                limit: 1,
                page_ids: None,
                root_page_ids: None,
            },
            LexicalQuery {
                query: "valid".into(),
                limit: 0,
                page_ids: None,
                root_page_ids: None,
            },
            LexicalQuery {
                query: "valid".into(),
                limit: 1,
                page_ids: Some(vec![]),
                root_page_ids: None,
            },
        ] {
            assert!(
                LexicalSearch::search(&table, query).await.is_err(),
                "invalid direct port input must fail closed"
            );
        }

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn optimizing_fts_after_page_diff_makes_updated_terms_searchable() {
        let path = temp_database("fts-update");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        table
            .apply_page_diff(
                &provider,
                "page-1",
                &[fts_chunk(
                    "page-1",
                    "chunk-a",
                    "Refresh test",
                    "legacyterm remains in the old chunk",
                )],
            )
            .await
            .expect("initial page snapshot");
        table
            .optimize_fts_index()
            .await
            .expect("index initial page");

        assert_eq!(
            table
                .fts_query("text", "legacyterm", 10)
                .await
                .expect("query initial term")
                .len(),
            1
        );

        table
            .apply_page_diff(
                &provider,
                "page-1",
                &[fts_chunk(
                    "page-1",
                    "chunk-a",
                    "Refresh test",
                    "freshterm replaces the previous searchable marker",
                )],
            )
            .await
            .expect("updated page snapshot");
        table
            .optimize_fts_index()
            .await
            .expect("optimize updated FTS indices");

        assert_eq!(
            table
                .fts_query("text", "freshterm", 10)
                .await
                .expect("query updated term")
                .len(),
            1
        );
        assert!(
            table
                .fts_query("text", "legacyterm", 10)
                .await
                .expect("query removed term")
                .is_empty()
        );

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn fts_configuration_and_query_validation_fail_without_mutating_index() {
        let path = temp_database("fts-validation");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");

        assert!(matches!(
            table.fts_query("workspace_id", "value", 10).await,
            Err(ChunkTableError::InvalidRows(_))
        ));
        assert!(matches!(
            table.fts_query("text", "", 10).await,
            Err(ChunkTableError::InvalidRows(_))
        ));
        assert!(matches!(
            table.fts_query("text", "value", 0).await,
            Err(ChunkTableError::InvalidRows(_))
        ));

        let before: HashSet<_> = table
            .table
            .list_indices()
            .await
            .expect("list initial indices")
            .into_iter()
            .map(|index| index.name)
            .collect();
        let invalid = FtsIndexConfig {
            language: "not-a-language".into(),
            ..FtsIndexConfig::default()
        };
        assert!(matches!(
            table.rebuild_fts_index(&invalid).await,
            Err(ChunkTableError::InvalidSchema(_))
        ));
        let after: HashSet<_> = table
            .table
            .list_indices()
            .await
            .expect("list preserved indices")
            .into_iter()
            .map(|index| index.name)
            .collect();
        assert_eq!(before, after);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn startup_policy_fails_closed_or_explicitly_rebuilds_embedding_mismatch() {
        let path = temp_database("compatibility-policy");
        let original = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", original.clone())
            .await
            .expect("create table");
        table
            .upsert(
                table.embedding_metadata(),
                &[EmbeddedChunk::new(
                    chunk("root", "Original", "preserve on fail"),
                    vec![1.0, 0.0, 0.0],
                )],
            )
            .await
            .expect("seed original generation");
        drop(table);

        let replacement = embedding("revision-2", 3);
        let error = match LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            replacement.clone(),
            IndexCompatibilityPolicy::Fail,
        )
        .await
        {
            Ok(_) => panic!("default policy must reject incompatible vector identity"),
            Err(error) => error,
        };
        assert!(error.is_incompatible_index());

        let preserved = LanceChunkTable::open(&path, "chunks", original.clone())
            .await
            .expect("failed startup must preserve original generation");
        assert_eq!(
            preserved.count_rows().await.expect("count preserved rows"),
            1
        );
        drop(preserved);

        let (rebuilt, action) = LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            replacement.clone(),
            IndexCompatibilityPolicy::Rebuild,
        )
        .await
        .expect("explicit rebuild policy");
        assert_eq!(action, IndexStartupAction::Rebuilt);
        assert_eq!(rebuilt.embedding_metadata(), &replacement);
        assert_eq!(rebuilt.count_rows().await.expect("count rebuilt rows"), 0);
        drop(rebuilt);

        let (reopened, action) = LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            replacement,
            IndexCompatibilityPolicy::Fail,
        )
        .await
        .expect("reopen rebuilt generation");
        assert_eq!(action, IndexStartupAction::Opened);
        assert_eq!(reopened.count_rows().await.expect("count reopened rows"), 0);
        drop(reopened);

        assert!(matches!(
            LanceChunkTable::open(&path, "chunks", original).await,
            Err(error) if error.is_incompatible_index()
        ));

        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn rebuild_policy_validates_target_schema_before_dropping_existing_table() {
        let path = temp_database("invalid-target-schema");
        let original = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", original.clone())
            .await
            .expect("create table");
        table
            .upsert(
                table.embedding_metadata(),
                &[EmbeddedChunk::new(
                    chunk("root", "Original", "must survive invalid target"),
                    vec![1.0, 0.0, 0.0],
                )],
            )
            .await
            .expect("seed original generation");
        drop(table);

        let invalid_target = embedding("revision-2", i32::MAX as usize + 1);
        let error = match LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            invalid_target,
            IndexCompatibilityPolicy::Rebuild,
        )
        .await
        {
            Ok(_) => panic!("invalid target schema must fail before rebuild"),
            Err(error) => error,
        };
        assert!(matches!(error, ChunkTableError::InvalidSchema(_)));

        let preserved = LanceChunkTable::open(&path, "chunks", original)
            .await
            .expect("invalid target must not drop existing table");
        assert_eq!(
            preserved.count_rows().await.expect("count preserved rows"),
            1
        );
        drop(preserved);

        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn explicit_rebuild_handles_schema_version_mismatch_but_not_storage_errors() {
        let path = temp_database("schema-version-policy");
        let metadata = embedding("revision-1", 3);
        let uri = local_database_uri(&path).expect("local database uri");
        let database = lancedb::connect(&uri)
            .execute()
            .await
            .expect("connect database");

        let expected = chunk_schema(&metadata).expect("expected schema");
        let mut persisted = expected.metadata().clone();
        persisted.insert(TABLE_SCHEMA_KEY.into(), "unsupported-version".into());
        let incompatible =
            Arc::new(Schema::new(expected.fields().clone()).with_metadata(persisted));
        database
            .create_empty_table("chunks", incompatible)
            .execute()
            .await
            .expect("create incompatible table");

        let error = match LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            metadata.clone(),
            IndexCompatibilityPolicy::Fail,
        )
        .await
        {
            Ok(_) => panic!("unsupported schema version must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(error, ChunkTableError::InvalidSchema(_)));

        let (rebuilt, action) = LanceChunkTable::open_with_policy(
            &path,
            "chunks",
            metadata,
            IndexCompatibilityPolicy::Rebuild,
        )
        .await
        .expect("explicit schema rebuild");
        assert_eq!(action, IndexStartupAction::Rebuilt);
        assert_eq!(rebuilt.count_rows().await.expect("empty rebuilt table"), 0);
        drop(rebuilt);

        let missing = match LanceChunkTable::open_with_policy(
            path.join("missing-database"),
            "missing-table",
            embedding("revision-1", 3),
            IndexCompatibilityPolicy::Rebuild,
        )
        .await
        {
            Ok(_) => panic!("storage errors must not be converted into rebuilds"),
            Err(error) => error,
        };
        assert!(!missing.is_incompatible_index());

        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn upsert_replaces_stable_chunk_and_filter_columns_are_queryable() {
        let path = temp_database("upsert");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");

        table
            .upsert(
                table.embedding_metadata(),
                &[EmbeddedChunk::new(
                    chunk("root-a", "Old title", "old text"),
                    vec![1.0, 0.0, 0.0],
                )],
            )
            .await
            .expect("insert row");
        table
            .upsert(
                table.embedding_metadata(),
                &[EmbeddedChunk::new(
                    chunk("root-b", "New title", "new text"),
                    vec![0.0, 1.0, 0.0],
                )],
            )
            .await
            .expect("replace row");

        assert_eq!(table.count_rows().await.expect("count rows"), 1);
        let batches: Vec<RecordBatch> = table
            .table
            .query()
            .only_if("page_id = 'page-1' AND root_page_id = 'root-b'")
            .execute()
            .await
            .expect("query filter columns")
            .try_collect()
            .await
            .expect("collect filtered row");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = batches.first().expect("one filtered batch");
        let text = batch
            .column_by_name("text")
            .expect("text column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text string array");
        let title = batch
            .column_by_name("title")
            .expect("title column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title string array");
        assert_eq!(text.value(0), "new text");
        assert_eq!(title.value(0), "New title");

        let stale: Vec<RecordBatch> = table
            .table
            .query()
            .only_if("root_page_id = 'root-a'")
            .execute()
            .await
            .expect("query old filter")
            .try_collect()
            .await
            .expect("collect old filter");
        assert_eq!(stale.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn upsert_rejects_ambiguous_or_incompatible_vectors() {
        let path = temp_database("validation");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");
        let first = EmbeddedChunk::new(chunk("root", "A", "one"), vec![1.0, 0.0, 0.0]);
        assert!(matches!(
            table
                .upsert(
                    &embedding("revision-2", 3),
                    &[EmbeddedChunk::new(
                        chunk("root", "wrong identity", "wrong identity"),
                        vec![1.0, 0.0, 0.0],
                    )],
                )
                .await,
            Err(ChunkTableError::Embedding(
                EmbeddingError::IncompatibleIndex
            ))
        ));
        let duplicate = EmbeddedChunk::new(chunk("root", "B", "two"), vec![0.0, 1.0, 0.0]);
        assert!(matches!(
            table.upsert(table.embedding_metadata(), &[first, duplicate]).await,
            Err(ChunkTableError::InvalidRows(message)) if message.contains("duplicate chunk_id")
        ));
        assert!(matches!(
            table
                .upsert(table.embedding_metadata(), &[EmbeddedChunk::new(
                    chunk("root", "C", "three"),
                    vec![1.0, f32::NAN, 0.0],
                )])
                .await,
            Err(ChunkTableError::InvalidRows(message)) if message.contains("incompatible vector")
        ));
        assert_eq!(table.count_rows().await.expect("count rows"), 0);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn page_diff_embeds_only_changed_and_new_chunks_and_is_idempotent() {
        let path = temp_database("page-diff");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        let first = vec![
            page_chunk("chunk-a", "alpha"),
            page_chunk("chunk-b", "bravo"),
            page_chunk("chunk-c", "charlie"),
        ];
        assert_eq!(
            table
                .apply_page_diff(&provider, "page-1", &first)
                .await
                .expect("initial page snapshot"),
            ChunkDiffMetrics {
                added: 3,
                changed: 0,
                skipped: 0,
                removed: 0,
            }
        );
        assert_eq!(provider.embedded_inputs(), 3);

        let initial_rows = table
            .rows_matching("page_id = 'page-1'".into())
            .await
            .expect("read initial rows");
        let original_a_vector = initial_rows
            .iter()
            .find(|row| row.chunk_id == "chunk-a")
            .expect("chunk a")
            .vector
            .clone();

        let mut unchanged_with_new_metadata = page_chunk("chunk-a", "alpha");
        unchanged_with_new_metadata.metadata.title = "Updated page title".into();
        unchanged_with_new_metadata.metadata.last_edited_time = "2026-10-06T18:00:00Z".into();
        let changed = page_chunk("chunk-b", "bravo changed");
        let added = page_chunk("chunk-d", "delta");
        let second = vec![unchanged_with_new_metadata, changed, added];

        assert_eq!(
            table
                .apply_page_diff(&provider, "page-1", &second)
                .await
                .expect("incremental page snapshot"),
            ChunkDiffMetrics {
                added: 1,
                changed: 1,
                skipped: 1,
                removed: 1,
            }
        );
        assert_eq!(provider.embedded_inputs(), 5);

        let rows = table
            .rows_matching("page_id = 'page-1'".into())
            .await
            .expect("read reconciled rows");
        assert_eq!(rows.len(), 3);
        let by_id: HashMap<_, _> = rows
            .iter()
            .map(|row| (row.chunk_id.as_str(), row))
            .collect();
        assert!(!by_id.contains_key("chunk-c"));
        assert_eq!(by_id["chunk-a"].title, "Updated page title");
        assert_eq!(by_id["chunk-a"].vector, original_a_vector);
        assert_eq!(by_id["chunk-b"].text, "bravo changed");
        assert_eq!(by_id["chunk-d"].text, "delta");

        assert_eq!(
            table
                .apply_page_diff(&provider, "page-1", &second)
                .await
                .expect("idempotent retry"),
            ChunkDiffMetrics {
                added: 0,
                changed: 0,
                skipped: 3,
                removed: 0,
            }
        );
        assert_eq!(provider.embedded_inputs(), 5);
        assert_eq!(table.count_rows().await.expect("count rows"), 3);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn empty_page_snapshot_removes_only_that_page_without_embedding() {
        let path = temp_database("page-delete");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        table
            .apply_page_diff(&provider, "page-1", &[page_chunk("page-1-a", "one")])
            .await
            .expect("seed page one");

        let mut other = page_chunk("page-2-a", "two");
        other.metadata.page_id = "page-2".into();
        other.metadata.url = "https://example.invalid/page-2".into();
        table
            .apply_page_diff(&provider, "page-2", &[other])
            .await
            .expect("seed page two");
        assert_eq!(provider.embedded_inputs(), 2);

        assert_eq!(
            table
                .apply_page_diff(&provider, "page-1", &[])
                .await
                .expect("delete page snapshot"),
            ChunkDiffMetrics {
                added: 0,
                changed: 0,
                skipped: 0,
                removed: 1,
            }
        );
        assert_eq!(provider.embedded_inputs(), 2);
        assert_eq!(table.count_rows().await.expect("count rows"), 1);
        assert!(
            table
                .rows_matching("page_id = 'page-1'".into())
                .await
                .expect("query page one")
                .is_empty()
        );
        assert_eq!(
            table
                .rows_matching("page_id = 'page-2'".into())
                .await
                .expect("query page two")
                .len(),
            1
        );

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn page_diff_cannot_update_or_delete_same_chunk_id_on_another_page() {
        let path = temp_database("page-isolation");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        let page_one = page_chunk("shared-id", "page one");
        table
            .apply_page_diff(&provider, "page-1", &[page_one])
            .await
            .expect("seed page one");

        let mut page_two = page_chunk("shared-id", "page two");
        page_two.metadata.page_id = "page-2".into();
        page_two.metadata.url = "https://example.invalid/page-2".into();
        table
            .apply_page_diff(&provider, "page-2", &[page_two])
            .await
            .expect("seed page two");

        let mut page_one_changed = page_chunk("shared-id", "page one changed");
        page_one_changed.metadata.title = "Changed page one".into();
        assert_eq!(
            table
                .apply_page_diff(&provider, "page-1", &[page_one_changed])
                .await
                .expect("update page one"),
            ChunkDiffMetrics {
                added: 0,
                changed: 1,
                skipped: 0,
                removed: 0,
            }
        );

        let page_one_rows = table
            .rows_matching("page_id = 'page-1'".into())
            .await
            .expect("read page one");
        let page_two_rows = table
            .rows_matching("page_id = 'page-2'".into())
            .await
            .expect("read page two");
        assert_eq!(page_one_rows.len(), 1);
        assert_eq!(page_two_rows.len(), 1);
        assert_eq!(page_one_rows[0].text, "page one changed");
        assert_eq!(page_two_rows[0].text, "page two");

        table
            .apply_page_diff(&provider, "page-1", &[])
            .await
            .expect("remove page one");
        assert!(
            table
                .rows_matching("page_id = 'page-1'".into())
                .await
                .expect("query deleted page")
                .is_empty()
        );
        assert_eq!(
            table
                .rows_matching("page_id = 'page-2'".into())
                .await
                .expect("query preserved page")
                .len(),
            1
        );

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn embedding_failure_leaves_the_existing_page_generation_unchanged() {
        let path = temp_database("page-diff-failure");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        table
            .apply_page_diff(&provider, "page-1", &[page_chunk("chunk-a", "old text")])
            .await
            .expect("seed page");
        provider.set_fail(true);

        let error = table
            .apply_page_diff(&provider, "page-1", &[page_chunk("chunk-a", "new text")])
            .await
            .expect_err("embedding failure");
        assert_eq!(
            error,
            ChunkTableError::Embedding(EmbeddingError::Unavailable)
        );

        let rows = table
            .rows_matching("page_id = 'page-1'".into())
            .await
            .expect("read preserved page");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text, "old text");

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn invalid_page_snapshot_is_rejected_before_embedding_or_mutation() {
        let path = temp_database("page-diff-validation");
        let metadata = embedding("revision-1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create table");
        let provider = CountingProvider::new(metadata);

        let duplicate = vec![page_chunk("same-id", "one"), page_chunk("same-id", "two")];
        assert!(matches!(
            table.apply_page_diff(&provider, "page-1", &duplicate).await,
            Err(ChunkTableError::InvalidRows(_))
        ));

        let mut wrong_page = page_chunk("other", "other page");
        wrong_page.metadata.page_id = "page-2".into();
        assert!(matches!(
            table
                .apply_page_diff(&provider, "page-1", &[wrong_page])
                .await,
            Err(ChunkTableError::InvalidRows(_))
        ));
        assert_eq!(provider.embedded_inputs(), 0);
        assert_eq!(table.count_rows().await.expect("count rows"), 0);

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn source_expansion_returns_scoped_section_and_page_content_with_provenance() {
        let path = temp_database("source-expansion");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");

        let mut first = chunk("root-a", "Page", "first section part");
        first.chunk_id = "chunk-a".into();
        let mut second = chunk("root-a", "Page", "second section part");
        second.chunk_id = "chunk-b".into();
        let mut other_section = chunk("root-a", "Page", "other section");
        other_section.chunk_id = "chunk-c".into();
        other_section.metadata.heading_path = vec!["Other".into()];
        let mut other_root = chunk("root-b", "Other root", "secret other root");
        other_root.chunk_id = "chunk-d".into();
        other_root.metadata.page_id = "page-2".into();
        other_root.metadata.url = "https://example.invalid/page-2".into();

        table
            .upsert(
                table.embedding_metadata(),
                &[
                    EmbeddedChunk::new(first, vec![1.0, 0.0, 0.0]),
                    EmbeddedChunk::new(second, vec![0.0, 1.0, 0.0]),
                    EmbeddedChunk::new(other_section, vec![0.0, 0.0, 1.0]),
                    EmbeddedChunk::new(other_root, vec![0.5, 0.5, 0.0]),
                ],
            )
            .await
            .expect("insert source fixtures");

        let section = table
            .expand(SourceExpandQuery {
                refs: vec![StableSourceRef::Chunk("chunk-a".into())],
                max_chars: 256,
                root_page_ids: vec!["root-a".into()],
            })
            .await
            .expect("expand section");
        assert_eq!(section.len(), 1);
        assert_eq!(section[0].provenance.page_id, "page-1");
        assert_eq!(section[0].provenance.root_page_id, "root-a");
        assert_eq!(section[0].provenance.heading_path, vec!["Section"]);
        assert_eq!(
            section[0].provenance.chunk_ids,
            vec!["chunk-a".to_owned(), "chunk-b".to_owned()]
        );
        assert!(section[0].text.contains("first section part"));
        assert!(section[0].text.contains("second section part"));
        assert!(!section[0].text.contains("other section"));
        assert!(!section[0].text.contains("secret other root"));
        assert!(!section[0].truncated);

        let page = table
            .expand(SourceExpandQuery {
                refs: vec![StableSourceRef::Page("page-1".into())],
                max_chars: 32,
                root_page_ids: vec!["root-a".into()],
            })
            .await
            .expect("expand page");
        assert_eq!(page[0].provenance.page_id, "page-1");
        assert_eq!(page[0].provenance.heading_path, Vec::<String>::new());
        assert!(page[0].text.chars().count() <= 32);
        assert!(page[0].truncated);

        let bounded_many = table
            .expand(SourceExpandQuery {
                refs: vec![
                    StableSourceRef::Chunk("chunk-a".into()),
                    StableSourceRef::Page("page-1".into()),
                ],
                max_chars: 20,
                root_page_ids: vec!["root-a".into()],
            })
            .await
            .expect("expand multiple refs under one budget");
        assert_eq!(bounded_many.len(), 2);
        assert!(
            bounded_many
                .iter()
                .map(|source| source.text.chars().count())
                .sum::<usize>()
                <= 20
        );
        assert!(bounded_many.iter().any(|source| source.truncated));

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[tokio::test]
    async fn source_expansion_distinguishes_missing_from_out_of_scope_without_returning_content() {
        let path = temp_database("source-scope");
        let table = LanceChunkTable::create(&path, "chunks", embedding("revision-1", 3))
            .await
            .expect("create table");
        let mut outside = chunk("root-b", "Outside", "outside");
        outside.chunk_id = "outside-chunk".into();
        outside.metadata.page_id = "outside-page".into();
        table
            .upsert(
                table.embedding_metadata(),
                &[EmbeddedChunk::new(outside, vec![1.0, 0.0, 0.0])],
            )
            .await
            .expect("insert outside fixture");

        assert_eq!(
            table
                .expand(SourceExpandQuery {
                    refs: vec![StableSourceRef::Chunk("missing".into())],
                    max_chars: 128,
                    root_page_ids: vec!["root-a".into()],
                })
                .await,
            Err(SourceExpansionError::Missing)
        );
        assert_eq!(
            table
                .expand(SourceExpandQuery {
                    refs: vec![StableSourceRef::Chunk("outside-chunk".into())],
                    max_chars: 128,
                    root_page_ids: vec!["root-a".into()],
                })
                .await,
            Err(SourceExpansionError::OutOfScope)
        );

        drop(table);
        std::fs::remove_dir_all(path).expect("remove temporary database");
    }

    #[test]
    fn source_predicates_quote_untrusted_ids() {
        assert_eq!(sql_string("a'b"), "'a''b'");
        assert_eq!(
            reference_predicate(&StableSourceRef::Page("a' OR 1=1 --".into())),
            "page_id = 'a'' OR 1=1 --'"
        );
    }
}
