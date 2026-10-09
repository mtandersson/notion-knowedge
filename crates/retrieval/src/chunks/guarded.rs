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
}

/// An immutable, one-shot page proposal. A new table version requires a new
/// preparation/embedding pass; the caller cannot bypass that comparison.
pub struct PreparedPageMutation {
    operation: PageOperation,
    table_version: u64,
    embedding: EmbeddingMetadata,
    rows: Vec<EmbeddedChunk>,
    metrics: ChunkDiffMetrics,
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
        CommitError::InvalidInput
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
) -> Result<LanceChunkTable, ChunkTableError> {
    let uri = local_database_uri(directory)?;
    let database = lancedb::connect(&uri).execute().await?;
    // Do not use LanceChunkTable::open here: its legacy startup helper may
    // mutate FTS indices before a guard has been acquired.
    let table = database.open_table(table_name).execute().await?;
    // Keep the reopened handle writable. Only preparation pins a read snapshot.
    validate_table_schema(&table, embedding).await?;
    Ok(LanceChunkTable {
        table,
        embedding: embedding.clone(),
    })
}

impl GuardedChunkTable {
    /// Verify trusted operator configuration, including the directory inode
    /// identity already pinned by the SQLite/index commit coordinator.
    pub fn bind(
        directory: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
        coordinator: Arc<IndexCommitCoordinator>,
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
        Ok(Self {
            index_directory: canonical,
            table_name: table_name.to_owned(),
            embedding,
            coordinator,
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
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                context.revalidate().map_err(fence_failure)?;
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
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                let current = open_current(&directory, &table_name, &embedding)
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                current
                    .ensure_fts_index(&FtsIndexConfig::default())
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
    /// guard. Call after the initial crawl has populated real vectors; new page
    /// commits subsequently keep any existing vector index up to date.
    pub fn ensure_vector_index(
        &self,
        config: VectorIndexConfig,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + 'static {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                let current = open_current(&directory, &table_name, &embedding)
                    .await
                    .map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                current
                    .optimize_vector_index(&config)
                    .await
                    .map_err(failure)?;
                Ok(())
            })
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
        let snapshot = open_current(&self.index_directory, &self.table_name, &self.embedding)
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
        let vectors = if embed_inputs.is_empty() {
            Vec::new()
        } else {
            embedding::embed_batch(provider, &embed_inputs).await?
        };
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
        Ok(PreparedPageMutation {
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
    /// The fallible async callback re-reads source revision, ancestry and
    /// effective scope while the external lock is held. It MUST NOT spawn
    /// detached I/O. A rejected callback leaves searchable state unchanged.
    ///
    /// Conflict means discard preparation and prepare again; in particular
    /// this is required when replaying apply-before-checkpoint interruptions.
    pub fn commit_page<F, Fut>(
        &self,
        prepared: PreparedPageMutation,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        precommit_source_check: F,
    ) -> impl Future<Output = Result<CommitOutcome, CommitError>> + Send + 'static
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), FailureClass>> + Send + 'static,
    {
        let directory = self.index_directory.clone();
        let table_name = self.table_name.clone();
        let embedding = self.embedding.clone();
        let operation = prepared.operation.clone();
        self.coordinator
            .submit(operation, lease, clock, move |context| async move {
                if prepared.embedding != embedding || context.binding().table() != table_name {
                    return Err(FailureClass::Conflict);
                }
                // Always reopen under the guard: a retained Table handle is not
                // proof of the actual current version after another process wrote.
                let current = open_current(&directory, &table_name, &embedding)
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
                precommit_source_check().await?;
                context.revalidate().map_err(fence_failure)?;
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
                // No detached maintenance: serialization extends to the end of
                // both index families' real on-disk mutations.
                current
                    .ensure_fts_index(&FtsIndexConfig::default())
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
            })
    }
}
