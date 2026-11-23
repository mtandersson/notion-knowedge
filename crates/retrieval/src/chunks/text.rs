//! Canonical search text, with no vector column or model assets.
use super::*;
use crate::commit::CommitBinding;
use futures::TryStreamExt;
use lancedb::{
    Connection,
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
};

const TEXT_SCHEMA_KEY: &str = "notion_knowledge.text_chunks.schema_version";
const TEXT_BINDING_KEY: &str = "notion_knowledge.text_chunks.binding";

fn schema(binding: &CommitBinding) -> SchemaRef {
    let identity = format!("{}:{}", binding.generation(), binding.epoch());
    Arc::new(Schema::new(text_fields()).with_metadata(HashMap::from([
        (TEXT_SCHEMA_KEY.into(), "1".into()),
        (TEXT_BINDING_KEY.into(), identity),
        (
            CANONICAL_SCHEMA_KEY.into(),
            CANONICAL_CHUNK_SCHEMA_VERSION.into(),
        ),
    ])))
}

fn name(binding: &CommitBinding) -> String {
    format!("{}_text", binding.table())
}

async fn connection(directory: &Path) -> Result<Connection, ChunkTableError> {
    Ok(lancedb::connect(&local_database_uri(directory)?)
        .execute()
        .await?)
}

async fn open(
    database: &Connection,
    binding: &CommitBinding,
    embedding: &EmbeddingMetadata,
) -> Result<Option<LanceChunkTable>, ChunkTableError> {
    let table = match database.open_table(name(binding)).execute().await {
        Ok(table) => table,
        Err(lancedb::Error::TableNotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if table.schema().await? != schema(binding) {
        return Err(ChunkTableError::InvalidSchema(
            "text projection schema or authority does not match".into(),
        ));
    }
    Ok(Some(LanceChunkTable {
        table,
        embedding: embedding.clone(),
        text_only: true,
    }))
}

pub(super) async fn current(
    directory: &Path,
    binding: &CommitBinding,
    embedding: &EmbeddingMetadata,
) -> Result<Option<LanceChunkTable>, ChunkTableError> {
    let database = connection(directory).await?;
    let current = open(&database, binding, embedding).await?;
    if current.is_none() && super::semantic_jobs::installed(directory)? {
        return Err(ChunkTableError::InvalidSchema(
            "installed text projection is missing".into(),
        ));
    }
    Ok(current)
}

/// Build a v1-compatible text table from a single pinned semantic snapshot.
/// The caller owns the writer guard through table creation and FTS setup.
pub(super) async fn provision(
    directory: &Path,
    binding: &CommitBinding,
    semantic: &LanceChunkTable,
    fts: &FtsIndexConfig,
) -> Result<(), ChunkTableError> {
    let database = connection(directory).await?;
    let projection = match open(&database, binding, semantic.embedding_metadata()).await? {
        Some(current) => current,
        None => {
            if super::semantic_jobs::installed(directory)? {
                return Err(ChunkTableError::InvalidSchema(
                    "installed text projection is missing".into(),
                ));
            }
            let schema = schema(binding);
            let columns = schema.fields().iter().map(|c| c.name().clone()).collect();
            let source = semantic.read_snapshot().await?;
            let stream = source
                .table
                .query()
                .select(Select::Columns(columns))
                .execute()
                .await?;
            let target = schema.clone();
            let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
                schema,
                stream: stream.and_then(move |batch| {
                    std::future::ready(
                        RecordBatch::try_new(target.clone(), batch.columns().to_vec())
                            .map_err(Into::into),
                    )
                }),
            });
            database
                .create_table(name(binding), stream)
                .execute()
                .await?;
            open(&database, binding, semantic.embedding_metadata())
                .await?
                .ok_or(ChunkTableError::Storage)?
        }
    };
    projection.ensure_fts_index(fts).await?;
    super::semantic_jobs::mark_installed(directory)
}

/// Atomically replace the canonical chunks of one page, including deletion.
pub(super) async fn replace(
    table: &LanceChunkTable,
    binding: &CommitBinding,
    page: &str,
    chunks: &[IndexedChunk],
) -> Result<(), ChunkTableError> {
    let predicate = format!("page_id = {}", sql_string(page));
    if chunks.is_empty() {
        table.table.delete(&predicate).await?;
        return Ok(());
    }
    let schema = schema(binding);
    let batch = RecordBatch::try_new(schema.clone(), text_columns(chunks.iter())?)?;
    let mut merge = table.table.merge_insert(&["page_id", "chunk_id"]);
    merge
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(Some(predicate));
    merge
        .execute(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
        .await?;
    Ok(())
}
