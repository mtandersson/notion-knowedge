//! Persisted canonical chunk rows for embedded LanceDB retrieval.
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lancedb::{
    Table,
    query::{ExecutableQuery, QueryBase},
};
use notion_knowledge_core::{
    embedding::{EmbeddingError, EmbeddingMetadata},
    indexed::{IndexedChunk, SchemaVersion},
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

/// Production chunk storage. LanceDB remains an adapter detail: callers pass
/// canonical chunks and vectors, and receive storage-neutral errors.
pub struct LanceChunkTable {
    table: Table,
    embedding: EmbeddingMetadata,
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
        Ok(Self { table, embedding })
    }

    /// Open an existing table only when both its storage schema and vector-space
    /// identity match the requested production contract.
    pub async fn open(
        database_path: impl AsRef<Path>,
        table_name: &str,
        embedding: EmbeddingMetadata,
    ) -> Result<Self, ChunkTableError> {
        validate_table_name(table_name)?;
        let uri = local_database_uri(database_path.as_ref())?;
        let database = lancedb::connect(&uri).execute().await?;
        let table = database.open_table(table_name).execute().await?;
        validate_table_schema(&table, &embedding).await?;
        Ok(Self { table, embedding })
    }

    pub fn embedding_metadata(&self) -> &EmbeddingMetadata {
        &self.embedding
    }

    pub async fn count_rows(&self) -> Result<usize, ChunkTableError> {
        Ok(self.table.count_rows(None).await?)
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
        let mut rows = match &source_ref {
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

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, ChunkTableError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ChunkTableError::InvalidSchema(format!("missing UTF-8 column {name}")))
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
            if index + 1 < rows.len() || output.chars().count() < rows.iter().map(|r| r.text.chars().count()).sum::<usize>() {
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
        })?)?;
    persisted_embedding.ensure_compatible(expected_embedding)?;
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
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lancedb::query::{ExecutableQuery, QueryBase};
    use notion_knowledge_core::indexed::{IndexedMetadata, SourceMetadata};

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
