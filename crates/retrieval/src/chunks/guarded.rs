//! Guarded, complete-page LanceDB mutations using the durable SQLite coordinator.
//!
//! Preparation reads a pinned Lance version and finishes embedding without the
//! commit lock. A prepared value is only a proposal, never permission to write.
//! Every externally visible effect is performed by an operation-owned runtime
//! while the directory-inode guard is held.
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::lexical_binding::{self, LexicalIndexBinding, LexicalPlanIdentity};
use super::*;
use crate::commit::{
    CommitError, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation,
};

use notion_knowledge_core::{
    embedding::{self, EmbeddingMetadata, EmbeddingProvider},
    indexed::IndexedChunk,
    reconciliation::{FailureClass, Lease},
};

/// A trusted binding to the same canonical local Lance directory that the
/// operational SQLite database exclusively owns.
#[derive(Clone)]
pub struct GuardedChunkTable {
    index_directory: PathBuf,
    table_name: String,
    embedding: EmbeddingMetadata,
    coordinator: Arc<IndexCommitCoordinator>,
    lexical_binding: Option<LexicalIndexBinding>,
    cadence: Arc<std::sync::Mutex<OptimizeCadence>>,
}

/// Committed pages after which the commit path runs index optimization.
/// Between optimizations at most this many pages' rows are outside the FTS and
/// vector indexes; search still finds them by scanning unindexed fragments.
pub const INDEX_OPTIMIZE_EVERY_PAGES: u32 = 32;
/// A pending tail older than this is optimized at the next committed page.
pub const INDEX_OPTIMIZE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// In-process count of pages committed since the last successful index
/// optimization. Shared by clones of one binding. It is only a scheduling hint:
/// after a restart, a crash or a failed pass the next pass folds every
/// unindexed row regardless of this count.
#[derive(Default)]
struct OptimizeCadence {
    /// Pages committed through this binding.
    committed: u64,
    /// Highest `committed` value a successful pass has covered.
    indexed: u64,
    /// When the oldest still-pending page was counted.
    oldest: Option<std::time::Instant>,
    /// A commit-triggered pass is running; later commits do not stack another.
    in_flight: bool,
    /// After a failed pass, commits do not retry before this instant.
    retry_after: Option<std::time::Instant>,
}

impl OptimizeCadence {
    fn pending(&self) -> u64 {
        self.committed - self.indexed
    }
}

/// Clears the in-flight mark however the commit-triggered pass ends,
/// including when its observer is dropped.
struct InFlight(Arc<std::sync::Mutex<OptimizeCadence>>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).in_flight = false;
    }
}

/// Expand content only from a compatible committed snapshot. Hold the shared
/// directory fence through all reads and reject replaced operational authority
/// before releasing any content. This read path never repairs indexes.
impl SourceExpansion for GuardedChunkTable {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_> {
        Box::pin(async move {
            let _guard = self
                .coordinator
                .search_guard()
                .map_err(|_| SourceExpansionError::Unavailable)?;
            let snapshot = open_current(
                &self.index_directory,
                &self.table_name,
                &self.embedding,
                &self.lexical_binding,
            )
            .await
            .map_err(|_| SourceExpansionError::Unavailable)?
            .read_snapshot()
            .await
            .map_err(|_| SourceExpansionError::Unavailable)?;
            let result = snapshot.expand(query).await;
            self.coordinator
                .identities()
                .map_err(|_| SourceExpansionError::Unavailable)?;
            result
        })
    }
}

/// An immutable, one-shot page proposal. A new table version requires a new
/// preparation/embedding pass; the caller cannot bypass that comparison.
pub struct PreparedPageMutation {
    operation: PageOperation,
    table_version: u64,
    embedding: EmbeddingMetadata,
    rows: Vec<EmbeddedChunk>,
    metrics: ChunkDiffMetrics,
    lexical_binding: Option<LexicalIndexBinding>,
}

impl PreparedPageMutation {
    pub fn metrics(&self) -> ChunkDiffMetrics {
        self.metrics
    }
    pub fn table_version(&self) -> u64 {
        self.table_version
    }
}

fn failure(error: ChunkTableError) -> FailureClass {
    match error {
        ChunkTableError::Storage
        | ChunkTableError::Serialization
        | ChunkTableError::InvalidSchema(_)
        | ChunkTableError::InvalidRows(_) => FailureClass::Index,
        ChunkTableError::Io | ChunkTableError::InvalidPath => FailureClass::Unavailable,
        ChunkTableError::Embedding(_) => FailureClass::Source,
    }
}

fn fence_failure(error: CommitError) -> FailureClass {
    match error {
        CommitError::ProjectionRepairRequired
        | CommitError::InvalidInput
        | CommitError::BindingMismatch
        | CommitError::LeaseLost
        | CommitError::Conflict => FailureClass::Conflict,
        CommitError::Unavailable | CommitError::Operation(_) => FailureClass::Unavailable,
    }
}

async fn open_current(
    directory: &Path,
    table_name: &str,
    embedding: &EmbeddingMetadata,
    lexical: &Option<LexicalIndexBinding>,
) -> Result<LanceChunkTable, ChunkTableError> {
    lexical_binding::verify(directory, lexical)?;
    let uri = local_database_uri(directory)?;
    #[cfg(all(test, unix))]
    let uri = io_tests::test_uri(directory, uri);
    let database = lancedb::connect(&uri).execute().await?;
    // Do not use LanceChunkTable::open here: its legacy startup helper may
    // mutate FTS indices before a guard has been acquired.
    let builder = database.open_table(table_name);
    #[cfg(all(test, unix))]
    let builder = io_tests::instrument(directory, builder);
    let table = builder.execute().await?;
    // Keep the reopened handle writable. Only preparation pins a read snapshot.
    validate_table_schema(&table, embedding).await?;
    lexical_binding::verify(directory, lexical)?;
    Ok(LanceChunkTable {
        table,
        embedding: embedding.clone(),
    })
}

impl GuardedChunkTable {
    /// The bound index directory (test pause hooks are keyed by it).
    pub(crate) fn index_directory(&self) -> &std::path::Path {
        &self.index_directory
    }

    /// Verify trusted operator configuration, including the directory inode
    /// identity already pinned by the SQLite/index commit coordinator.
    pub fn bind(
        directory: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
    ) -> Result<Self, ChunkTableError> {
        Self::bind_inner(directory.as_ref(), table_name, embedding, coordinator, None)
    }

    /// Require the configured exact plan. Reads never stamp missing metadata.
    pub fn bind_with_lexical_plan(
        directory: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
        plan: &LexicalPlanIdentity,
    ) -> Result<Self, ChunkTableError> {
        Self::bind_inner(
            directory.as_ref(),
            table_name,
            embedding,
            coordinator,
            Some(plan),
        )
    }
    /// Initialize only a fresh cache. Existing rows or binding metadata require
    /// reset and authoritative resync; this never relabels an existing table.
    pub async fn create_fresh_lexical(
        directory: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
        plan: &LexicalPlanIdentity,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Self, ChunkTableError> {
        let raw = Self::bind(directory, table_name, embedding, coordinator.clone())?;
        let directory = raw.index_directory.clone();
        let name = raw.table_name.clone();
        let metadata = raw.embedding.clone();
        let identity = plan.clone();
        coordinator
            .maintain(lease, clock, move |context| async move {
                context.revalidate().map_err(fence_failure)?;
                lexical_binding::verify(&directory, &None).map_err(failure)?;
                match std::fs::symlink_metadata(directory.join(format!("{name}.lance"))) {
                    Ok(_) => return Err(FailureClass::Conflict),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(FailureClass::Index),
                }
                let table = LanceChunkTable::create(&directory, &name, metadata.clone())
                    .await
                    .map_err(failure)?;
                table
                    .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                let binding = lexical_binding::LexicalIndexBinding::for_cache(
                    &directory,
                    metadata,
                    context.binding(),
                    identity,
                )
                .map_err(failure)?;
                lexical_binding::write_fresh(&directory, &binding).map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                Ok(())
            })
            .await
            .map_err(|_| ChunkTableError::InvalidPath)?;
        Self::bind_with_lexical_plan(
            &raw.index_directory,
            &raw.table_name,
            raw.embedding,
            coordinator,
            plan,
        )
    }

    fn bind_inner(
        directory: &Path,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
        plan: Option<&LexicalPlanIdentity>,
    ) -> Result<Self, ChunkTableError> {
        validate_table_name(table_name)?;
        let canonical = std::fs::canonicalize(directory)?;
        if canonical != coordinator.index_directory() || table_name != coordinator.binding().table()
        {
            return Err(ChunkTableError::InvalidSchema(
                "untrusted index binding".into(),
            ));
        }
        chunk_schema(&embedding)?;
        coordinator
            .identities()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        let lexical_binding = lexical_binding::read(&canonical)?;
        match (&lexical_binding, plan) {
            (Some(binding), Some(plan)) => {
                binding.validate(&canonical, &embedding, coordinator.binding(), plan)?
            }
            (None, None) => {}
            _ => {
                return Err(ChunkTableError::InvalidSchema(
                    "missing or unexpected lexical plan binding".into(),
                ));
            }
        }
        coordinator
            .identities()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        Ok(Self {
            index_directory: canonical,
            table_name: table_name.to_owned(),
            embedding,
            coordinator,
            lexical_binding,
            cadence: Default::default(),
        })
    }

    /// Create the initial table (including default lexical indices) only
    /// under the coordinator's cross-process guard and current SQLite fence.
    /// This must not run concurrently with legacy writers.
    pub fn create_empty(
        &self,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let lexical = self.lexical_binding.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                context.revalidate().map_err(fence_failure)?;
                lexical_binding::verify(&directory, &lexical).map_err(failure)?;
                if lexical.is_some() {
                    return Err(FailureClass::Conflict);
                }
                LanceChunkTable::create(directory, &table_name, embedding)
                    .await
                    .map_err(failure)?;
                Ok(())
            })
    }

    /// Guarded startup repair for any missing FTS index. This is not a page
    /// checkpoint, and never acknowledges a source-side update.
    pub fn ensure_startup_indexes(
        &self,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let lexical = self.lexical_binding.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                let current = open_current(&directory, &table_name, &embedding, &lexical)
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                current
                    .ensure_fts_index(&if lexical.is_some() {
                        FtsIndexConfig::lexical_expansion_v1()
                    } else {
                        FtsIndexConfig::default()
                    })
                    .await
                    .map_err(failure)?;
                current
                    .validate_vector_index_layout()
                    .await
                    .map_err(failure)?;
                Ok(())
            })
    }

    /// Build or refresh the configured vector index under the same external
    /// guard. Call after the initial crawl has populated real vectors; page
    /// commits then keep any existing vector index current through
    /// [`Self::optimize_indexes`] on a bounded cadence.
    pub fn ensure_vector_index(
        &self,
        config: VectorIndexConfig,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        self.maintain_vector_index(config, lease, clock, false)
    }

    /// Explicit operator retraining of derived ANN state under owned fencing.
    pub fn rebuild_vector_index(
        &self,
        config: VectorIndexConfig,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        self.maintain_vector_index(config, lease, clock, true)
    }

    fn maintain_vector_index(
        &self,
        config: VectorIndexConfig,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        rebuild: bool,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let lexical = self.lexical_binding.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                let current = open_current(&directory, &table_name, &embedding, &lexical)
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                let indexes = current
                    .table
                    .list_indices()
                    .await
                    .map_err(|e| failure(e.into()))?;
                #[cfg(all(test, unix))]
                io_tests::pause_maintenance(&directory).await;
                for (name, column) in CHUNK_FTS_INDEXES {
                    if !indexes.iter().any(|index| {
                        index.name == name
                            && index.index_type == IndexType::FTS
                            && index.columns == vec![column.to_string()]
                    }) {
                        return Err(failure(ChunkTableError::InvalidSchema(
                            "required FTS index unavailable".into(),
                        )));
                    }
                }
                if rebuild
                    && indexes
                        .iter()
                        .any(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
                {
                    current
                        .validate_vector_index_config_matches(&config)
                        .await
                        .map_err(failure)?;
                }
                context.revalidate().map_err(fence_failure)?;
                if rebuild {
                    current
                        .rebuild_vector_index(&config)
                        .await
                        .map_err(failure)?;
                } else {
                    current
                        .optimize_vector_index(&config)
                        .await
                        .map_err(failure)?;
                }
                context.revalidate().map_err(fence_failure)?;
                Ok(())
            })
    }

    /// Open the current compatible search snapshot without index repair or writes.
    /// The operational authority is rechecked before and after opening storage.
    pub async fn search_snapshot(&self) -> Result<LanceChunkTable, ChunkTableError> {
        let _guard = self
            .coordinator
            .search_guard()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        self.open_search_snapshot().await
    }

    pub(crate) fn require_lexical_plan(
        &self,
        plan: &lexical_binding::LexicalCachePlan,
    ) -> Result<(), ChunkTableError> {
        if self.lexical_binding.as_ref().is_some_and(|binding| {
            binding.plan() == plan.identity() && plan.identity().matches(plan.dictionary())
        }) {
            Ok(())
        } else {
            Err(ChunkTableError::InvalidSchema(
                "configured lexical plan is not bound".into(),
            ))
        }
    }

    /// Keep both rankers and metadata filtering on one committed revision.
    /// Only retrieval adapters may execute inside this read fence.
    pub(crate) async fn search_with_snapshot<F, Fut>(
        &self,
        operation: F,
    ) -> Result<Vec<SearchHit>, SearchUnavailable>
    where
        F: FnOnce(LanceChunkTable) -> Fut,
        Fut: Future<Output = Result<Vec<SearchHit>, SearchUnavailable>>,
    {
        let _guard = self
            .coordinator
            .search_guard()
            .map_err(|_| SearchUnavailable)?;
        let snapshot = self
            .open_search_snapshot()
            .await
            .map_err(|_| SearchUnavailable)?;
        let results = operation(snapshot).await?;
        self.coordinator
            .identities()
            .map_err(|_| SearchUnavailable)?;
        lexical_binding::verify(&self.index_directory, &self.lexical_binding)
            .map_err(|_| SearchUnavailable)?;
        Ok(results)
    }
    /// Inspect search layout without admitting content while rebuild is pending.
    pub async fn verify_search_indexes(&self) -> Result<(), ChunkTableError> {
        let _guard = self
            .coordinator
            .index_read_guard()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        self.open_search_snapshot().await?;
        Ok(())
    }

    /// Readiness probes stay inside the startup boundary and expose no content.
    /// A real cached page ID must be found through the native exact-ID profile.
    pub async fn probe_lexical_cache(&self) -> Result<(), ChunkTableError> {
        let _guard = self
            .coordinator
            .index_read_guard()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        let snapshot = self.open_search_snapshot().await?;
        let mut stream = snapshot
            .table
            .query()
            .select(Select::columns(&["page_id"]))
            .limit(1)
            .execute()
            .await?;
        let Some(batch) = stream.try_next().await? else {
            return Ok(());
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ChunkTableError::InvalidSchema("invalid page identity column".into()))?;
        let id = ids.value(0).to_owned();
        let query = LexicalQuery {
            query: id.clone(),
            limit: 1,
            page_ids: Some(vec![id.clone()]),
            root_page_ids: None,
            metadata: Default::default(),
        };
        let hits = snapshot.lexical_search(query).await?;
        if !hits.iter().any(|hit| hit.source.page_id == id) {
            return Err(ChunkTableError::InvalidSchema(
                "exact-ID readiness probe failed".into(),
            ));
        }
        Ok(())
    }

    async fn open_search_snapshot(&self) -> Result<LanceChunkTable, ChunkTableError> {
        let current = open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await?
        .read_snapshot()
        .await?;
        current.validate_vector_index_layout().await?;
        let indexes = current.table.list_indices().await?;
        if !indexes
            .iter()
            .any(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
            && current.count_rows().await? != 0
        {
            return Err(ChunkTableError::InvalidSchema(
                "required vector index unavailable".into(),
            ));
        }
        for (name, column) in CHUNK_FTS_INDEXES {
            if !indexes.iter().any(|index| {
                index.name == name
                    && index.index_type == IndexType::FTS
                    && index.columns == vec![column.to_string()]
            }) {
                return Err(ChunkTableError::InvalidSchema(
                    "required FTS index unavailable".into(),
                ));
            }
        }
        self.coordinator
            .identities()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        Ok(current)
    }
    /// Execute under one directory/authority fence. Installation additionally
    /// requires the exact versioned lexical identity binding.
    pub async fn expanded_lexical_search<C>(
        &self,
        query: LexicalQuery,
        dictionary: &crate::lexical_dictionary::Dictionary,
        pool: &super::bounded::CandidateBudget,
        deadline: tokio::time::Instant,
        cancel: C,
    ) -> Result<super::bounded::BoundedLexicalResult, ChunkTableError>
    where
        C: Future<Output = ()> + Send,
    {
        let operation = async {
            if !self
                .lexical_binding
                .as_ref()
                .is_some_and(|binding| binding.plan().matches(dictionary))
            {
                return Err(ChunkTableError::InvalidSchema(
                    "expanded lexical plan is not bound".into(),
                ));
            }
            let _guard = self
                .coordinator
                .search_guard()
                .map_err(|_| ChunkTableError::InvalidPath)?;
            let snapshot = self.open_search_snapshot().await?;
            let result = snapshot
                .expanded_lexical_inner(query, dictionary, pool)
                .await;
            self.coordinator
                .identities()
                .map_err(|_| ChunkTableError::InvalidPath)?;
            result
        };
        tokio::select! {biased;
            _ = cancel => Err(ChunkTableError::InvalidRows("lexical request cancelled".into())),
            _ = tokio::time::sleep_until(deadline) => Err(ChunkTableError::InvalidRows("lexical deadline exceeded".into())),
            result = operation => result,
        }
    }
    /// Read the version of a pinned Lance inventory without startup repair.
    pub async fn inventory_version(&self) -> Result<u64, ChunkTableError> {
        Ok(open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await?
        .table
        .version()
        .await?)
    }
    pub async fn indexed_page_ids(&self) -> Result<Vec<String>, ChunkTableError> {
        let current = open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await?
        .read_snapshot()
        .await?;
        let rows = current.rows_matching("true".into()).await?;
        Ok(rows
            .into_iter()
            .map(|row| row.page_id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect())
    }
    /// Verify exact shared commit ownership before composing another worker.
    pub fn is_bound_to(&self, coordinator: &Arc<IndexCommitCoordinator>) -> bool {
        Arc::ptr_eq(&self.coordinator, coordinator)
    }
    pub fn binding(&self) -> &crate::commit::CommitBinding {
        self.coordinator.binding()
    }
    pub async fn page_chunks(&self, page_id: &str) -> Result<Vec<IndexedChunk>, ChunkTableError> {
        open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await?
        .page_chunks(page_id)
        .await
    }

    /// Prepare a complete source page snapshot outside the commit guard.
    /// Neither embedding nor this immutable table revision authorizes a write.
    /// A deleted page must supply an empty chunk list and tombstone operation.
    pub async fn prepare_page(
        &self,
        provider: &dyn EmbeddingProvider,
        operation: PageOperation,
        chunks: &[IndexedChunk],
    ) -> Result<PreparedPageMutation, ChunkTableError> {
        self.embedding.ensure_compatible(provider.metadata())?;
        let page_id = operation.checkpoint().page_id();
        if matches!(operation.action(), PageAction::Unchanged)
            || (operation.action() == PageAction::Delete && !chunks.is_empty())
        {
            return Err(ChunkTableError::InvalidRows(
                "invalid complete page action".into(),
            ));
        }
        validate_page_snapshot(page_id, chunks)?;
        if chunks.iter().any(|chunk| {
            chunk.metadata.source.workspace_id != self.coordinator.binding().workspace()
                || operation
                    .checkpoint()
                    .last_edited_time()
                    .is_some_and(|revision| chunk.metadata.last_edited_time != revision)
        }) {
            return Err(ChunkTableError::InvalidRows(
                "source snapshot identity mismatch".into(),
            ));
        }
        let snapshot = open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await?
        .read_snapshot()
        .await?;
        let table_version = snapshot.table.version().await?;
        let predicate = format!("page_id = {}", sql_string(page_id));
        let old = snapshot.rows_matching(predicate).await?;
        let mut existing = HashMap::new();
        for row in old {
            if existing.insert(row.chunk_id.clone(), row).is_some() {
                return Err(ChunkTableError::InvalidRows(
                    "duplicate persisted chunk identity".into(),
                ));
            }
        }
        let ids: HashSet<&str> = chunks.iter().map(|chunk| chunk.chunk_id.as_str()).collect();
        let mut metrics = ChunkDiffMetrics {
            removed: existing
                .keys()
                .filter(|id| !ids.contains(id.as_str()))
                .count(),
            ..ChunkDiffMetrics::default()
        };
        let mut embed_positions = Vec::new();
        let mut embed_inputs = Vec::new();
        for (position, chunk) in chunks.iter().enumerate() {
            match existing.get(&chunk.chunk_id) {
                Some(old) if old.content_hash == chunk.content_hash => {
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
        // The page is the atomic storage snapshot, not a provider call budget.
        // Keep each request bounded independently of page chunk count; finish
        // all embedding before preparing any mutation. Production Qwen already
        // uses one-text inference batches, retaining the same accepted vectors.
        let mut vectors = Vec::with_capacity(embed_inputs.len());
        for input in &embed_inputs {
            vectors.extend(embedding::embed_batch(provider, std::slice::from_ref(input)).await?);
        }
        let mut generated = embed_positions
            .into_iter()
            .zip(vectors)
            .collect::<HashMap<_, _>>();
        let mut rows = Vec::with_capacity(chunks.len());
        for (position, chunk) in chunks.iter().enumerate() {
            let vector = match generated.remove(&position) {
                Some(vector) => vector,
                None => existing
                    .get(&chunk.chunk_id)
                    .filter(|old| old.content_hash == chunk.content_hash)
                    .map(|old| old.vector.clone())
                    .ok_or_else(|| {
                        ChunkTableError::InvalidRows("missing unchanged vector".into())
                    })?,
            };
            rows.push(EmbeddedChunk::new(chunk.clone(), vector));
        }
        if !generated.is_empty() {
            return Err(ChunkTableError::InvalidRows(
                "inconsistent embedding result".into(),
            ));
        }
        validate_rows(&rows, self.embedding.dimension())?;
        lexical_binding::verify(&self.index_directory, &self.lexical_binding)?;
        self.coordinator
            .identities()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        Ok(PreparedPageMutation {
            lexical_binding: self.lexical_binding.clone(),
            operation,
            table_version,
            embedding: self.embedding.clone(),
            rows,
            metrics,
        })
    }

    /// Atomically replace one page's chunk membership in LanceDB, maintain
    /// the real lexical and any present vector index, THEN checkpoint SQLite.
    ///
    /// The fallible async callback runs while the external lock is held, for
    /// fenced local bookkeeping and policy authority. It makes no source
    /// request and MUST NOT spawn detached I/O. A rejected callback leaves
    /// searchable state unchanged.
    ///
    /// Conflict means discard preparation and prepare again; in particular
    /// this is required when replaying apply-before-checkpoint interruptions.
    pub fn commit_page<F, Fut>(
        &self,
        prepared: PreparedPageMutation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        source: F,
    ) -> impl Future<Output = Result<CommitOutcome, CommitError>> + Send + 'static
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), FailureClass>> + Send + 'static,
    {
        self.commit_page_with_authority(prepared, lease, clock, source)
    }

    /// Retain the callback's policy authority through all effects and final
    /// acknowledgment.
    pub fn commit_page_with_authority<F, Fut, Authority>(
        &self,
        prepared: PreparedPageMutation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        fenced: F,
    ) -> impl Future<Output = Result<CommitOutcome, CommitError>> + Send + 'static
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Authority, FailureClass>> + Send + 'static,
        Authority: Send + 'static,
    {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let lexical = self.lexical_binding.clone();
        let operation = prepared.operation.clone();
        let this = self.clone();
        let maintenance = (lease.clone(), clock.clone());
        let commit = self.coordinator.submit_with_authority(
            operation,
            lease,
            clock,
            move |context| async move {
                if prepared.embedding != embedding
                    || prepared.lexical_binding != lexical
                    || context.binding().table() != table_name
                {
                    return Err(FailureClass::Conflict);
                }
                // Always reopen under the guard: a retained Table handle is not
                // proof of the actual current version after another process wrote.
                let current = open_current(&directory, &table_name, &embedding, &lexical)
                    .await
                    .map_err(failure)?;
                let version = current
                    .table
                    .version()
                    .await
                    .map_err(|_| FailureClass::Index)?;
                if version != prepared.table_version {
                    return Err(FailureClass::Conflict);
                }
                if !prepared.rows.is_empty() {
                    let chunk_ids = prepared
                        .rows
                        .iter()
                        .map(|row| row.chunk.chunk_id.clone())
                        .collect::<Vec<_>>();
                    let collisions = current
                        .rows_matching(id_list_predicate("chunk_id", &chunk_ids))
                        .await
                        .map_err(failure)?;
                    if collisions
                        .iter()
                        .any(|row| row.page_id != prepared.operation.checkpoint().page_id())
                    {
                        return Err(FailureClass::Conflict);
                    }
                }
                context.revalidate().map_err(fence_failure)?;
                let authority = fenced().await?;
                context.revalidate().map_err(fence_failure)?;
                lexical_binding::verify(&directory, &lexical).map_err(failure)?;
                let predicate = format!(
                    "page_id = {}",
                    sql_string(prepared.operation.checkpoint().page_id())
                );
                if prepared.rows.is_empty() {
                    current
                        .table
                        .delete(&predicate)
                        .await
                        .map_err(|_| FailureClass::Index)?;
                } else {
                    let batch = rows_to_batch(&prepared.rows, &embedding).map_err(failure)?;
                    let schema = batch.schema();
                    let mut merge = current.table.merge_insert(&["page_id", "chunk_id"]);
                    merge
                        .when_matched_update_all(None)
                        .when_not_matched_insert_all()
                        .when_not_matched_by_source_delete(Some(predicate));
                    merge
                        .execute(Box::new(RecordBatchIterator::new(
                            vec![Ok(batch)].into_iter(),
                            schema,
                        )))
                        .await
                        .map_err(|_| FailureClass::Index)?;
                }
                Ok(authority)
            },
        );
        async move {
            let outcome = commit.await?;
            if outcome == CommitOutcome::Applied && this.record_commit() {
                // The page is durable. A failed pass keeps its pages pending
                // and delays the next commit-triggered attempt by one interval,
                // so persistent failure costs one attempt per interval, not per
                // commit; the next successful pass folds every row in.
                let _in_flight = InFlight(this.cadence.clone());
                if let Err(error) = this.optimize_indexes(maintenance.0, maintenance.1).await {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "operation": "index_optimize",
                            "outcome": "deferred",
                            "error": format!("{error:?}"),
                        })
                    );
                }
            }
            Ok(outcome)
        }
    }

    /// Whether pages were committed through this binding since the last pass.
    pub fn has_pending_optimization(&self) -> bool {
        self.cadence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending()
            > 0
    }

    /// Count one committed page and report whether optimization is due.
    fn record_commit(&self) -> bool {
        let mut cadence = self.cadence.lock().unwrap_or_else(|e| e.into_inner());
        cadence.committed += 1;
        let now = std::time::Instant::now();
        let oldest = *cadence.oldest.get_or_insert(now);
        let due = !cadence.in_flight
            && cadence.retry_after.is_none_or(|retry| now >= retry)
            && (cadence.pending() >= u64::from(INDEX_OPTIMIZE_EVERY_PAGES)
                || now.duration_since(oldest) >= INDEX_OPTIMIZE_INTERVAL);
        cadence.in_flight |= due;
        due
    }

    /// Fold every row written since the last pass into the FTS indexes and any
    /// existing vector index, as one guarded maintenance step. Page commits
    /// call this on a bounded cadence; sync calls it once at the end. It writes
    /// no receipt and is idempotent, so a crash or cancel before it completes
    /// leaves committed rows searchable and the next pass repairs the indexes.
    pub fn optimize_indexes(
        &self,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let lexical = self.lexical_binding.clone();
        let cadence = self.cadence.clone();
        let covered = self
            .cadence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .committed;
        let pass = self
            .coordinator
            .maintain(lease, clock, move |context| async move {
                let current = open_current(&directory, &table_name, &embedding, &lexical)
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                lexical_binding::verify(&directory, &lexical).map_err(failure)?;
                current
                    .ensure_fts_index(&if lexical.is_some() {
                        FtsIndexConfig::lexical_expansion_v1()
                    } else {
                        FtsIndexConfig::default()
                    })
                    .await
                    .map_err(failure)?;
                current.optimize_fts_index().await.map_err(failure)?;
                let indices = current
                    .table
                    .list_indices()
                    .await
                    .map_err(|_| FailureClass::Index)?;
                if indices
                    .iter()
                    .any(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
                {
                    current
                        .validate_vector_index_layout()
                        .await
                        .map_err(failure)?;
                    context.revalidate().map_err(fence_failure)?;
                    current
                        .table
                        .optimize(OptimizeAction::Index(
                            OptimizeOptions::new()
                                .index_names(vec![CHUNK_VECTOR_INDEX_NAME.to_owned()]),
                        ))
                        .await
                        .map_err(|_| FailureClass::Index)?;
                }
                Ok(())
            });
        async move {
            let result = pass.await;
            let mut cadence = cadence.lock().unwrap_or_else(|e| e.into_inner());
            let now = std::time::Instant::now();
            if result.is_ok() {
                // Pages counted after the pass was requested may be unindexed;
                // they stay pending so the bound covers them.
                cadence.indexed = cadence.indexed.max(covered);
                cadence.retry_after = None;
                cadence.oldest = (cadence.pending() > 0).then_some(now);
            } else {
                cadence.retry_after = Some(now + INDEX_OPTIMIZE_INTERVAL);
            }
            result
        }
    }
}

#[cfg(all(test, unix))]
mod io_tests;
