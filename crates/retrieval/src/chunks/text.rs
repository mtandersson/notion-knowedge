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

/// "2" added `property_text` (#844). "1" is a known older layout.
const TEXT_SCHEMA_VERSION: &str = "2";
const OUTDATED_TEXT_SCHEMA_VERSIONS: [&str; 1] = ["1"];

fn schema(binding: &CommitBinding) -> SchemaRef {
    let identity = format!("{}:{}", binding.generation(), binding.epoch());
    Arc::new(Schema::new(text_fields()).with_metadata(HashMap::from([
        (TEXT_SCHEMA_KEY.into(), TEXT_SCHEMA_VERSION.into()),
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
    let actual = table.schema().await?;
    let expected = schema(binding);
    if actual != expected {
        // Only a table of this binding under a known older version is
        // reported as outdated; any other mismatch still fails closed.
        let same_binding =
            actual.metadata().get(TEXT_BINDING_KEY) == expected.metadata().get(TEXT_BINDING_KEY);
        if same_binding
            && actual
                .metadata()
                .get(TEXT_SCHEMA_KEY)
                .is_some_and(|version| OUTDATED_TEXT_SCHEMA_VERSIONS.contains(&version.as_str()))
        {
            return Err(ChunkTableError::OutdatedSchema);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::reconciliation::ReconciliationScope;

    fn binding(generation: u64) -> CommitBinding {
        let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "1").unwrap();
        CommitBinding::new("chunks", "workspace", &scope, "1", generation).unwrap()
    }

    /// The text layout before #844, as an older binary wrote it.
    fn version_1_schema(binding: &CommitBinding) -> SchemaRef {
        let mut metadata = schema(binding).metadata().clone();
        metadata.insert(TEXT_SCHEMA_KEY.into(), "1".into());
        let fields = text_fields()
            .into_iter()
            .filter(|field| field.name() != "property_text")
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields).with_metadata(metadata))
    }

    #[tokio::test]
    async fn an_older_text_layout_of_this_binding_is_outdated_and_others_fail_closed() {
        let embedding = super::super::tests::embedding("revision-1", 3);
        for (written_by, expect_outdated) in [(binding(1), true), (binding(2), false)] {
            let path = super::super::tests::temp_database("text-outdated");
            let database = connection(&path).await.unwrap();
            database
                .create_empty_table(name(&binding(1)), version_1_schema(&written_by))
                .execute()
                .await
                .unwrap();
            let opened = open(&database, &binding(1), &embedding).await;
            if expect_outdated {
                assert!(matches!(opened, Err(ChunkTableError::OutdatedSchema)));
            } else {
                assert!(matches!(opened, Err(ChunkTableError::InvalidSchema(_))));
            }
            drop(database);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
