//! Explicit CPU feasibility experiment, not the production adapter.
use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, StringArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use candle_core::{DType, Device};
use candle_nn::VarBuilder;
use fastembed::{Qwen3Config, Qwen3Model, Qwen3TextEmbedding};
use futures::TryStreamExt;
use lancedb::{
    DistanceType,
    query::{ExecutableQuery, QueryBase},
};
use notion_knowledge_core::embedding::{
    self, EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider,
};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::{path::Path, sync::Arc, time::Instant};

use notion_knowledge_core::indexed::{
    IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata,
};

const REVISION: &str = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3";
const DIM: usize = 1024;
const QUERY: &str = "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: Var förvaras säkerhetskopiorna?";
const TEXTS: [&str; 2] = [
    "Säkerhetskopiorna förvaras i det låsta skåpet i biblioteket. Backups are stored in the locked library cabinet.",
    "Trädgårdens tomater behöver vatten varje morgon. Garden tomatoes need water every morning.",
];
fn chunks() -> Vec<IndexedChunk> {
    TEXTS
        .iter()
        .enumerate()
        .map(|(i, text)| IndexedChunk {
            schema_version: SchemaVersion::V1,
            chunk_id: ["fixture-backups", "fixture-tomatoes"][i].to_string(),
            metadata: IndexedMetadata {
                page_id: format!("synthetic-page-{i}"),
                block_id: None,
                url: format!("https://example.invalid/fixture/{i}"),
                title: ["Backup storage", "Garden care"][i].to_string(),
                heading_path: vec![],
                last_edited_time: "2026-10-04T00:00:00Z".into(),
                source: SourceMetadata {
                    workspace_id: "synthetic".into(),
                    root_page_id: "synthetic-root".into(),
                    database_id: None,
                    data_source_id: None,
                },
                properties: Default::default(),
            },
            text: text.to_string(),
            content_hash: format!("sha256:{:x}", Sha256::digest(text.as_bytes())),
            links: vec![],
        })
        .collect()
}
struct Provider {
    model: Qwen3TextEmbedding,
    metadata: EmbeddingMetadata,
}
fn metadata() -> Result<EmbeddingMetadata> {
    Ok(EmbeddingMetadata::new(
        "spike-fastembed-candle-cpu-f32".into(),
        "Qwen/Qwen3-Embedding-0.6B".into(),
        format!(
            "{REVISION}:fastembed7.1.0:last-token:left-padding:l2:query-instruction-v1:untruncated"
        ),
        DIM,
    )?)
}
impl Provider {
    fn load(assets: &Path) -> Result<Self> {
        for name in ["config.json", "tokenizer.json", "model.safetensors"] {
            ensure!(
                assets.join(name).is_file(),
                "missing model asset: {name}; run the documented asset download"
            );
        }
        for (name, expected) in [
            (
                "config.json",
                "b5bf1f51fc45be473a54718cef92448d90a1be001bf9b9a44b8c7f10a19feaa9",
            ),
            (
                "tokenizer.json",
                "def76fb086971c7867b829c23a26261e38d9d74e02139253b38aeb9df8b4b50a",
            ),
            (
                "model.safetensors",
                "0437e45c94563b09e13cb7a64478fc406947a93cb34a7e05870fc8dcd48e23fd",
            ),
        ] {
            let mut file = std::fs::File::open(assets.join(name))?;
            let mut hash = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            ensure!(
                format!("{:x}", hash.finalize()) == expected,
                "model asset hash mismatch: {name}; download pinned revision"
            );
        }
        let cfg: Qwen3Config = serde_json::from_slice(&std::fs::read(assets.join("config.json"))?)?;
        ensure!(cfg.hidden_size == DIM, "unsupported model dimension");
        // Owned bytes avoid mmap lifetime/file-mutation safety requirements.
        let vb = VarBuilder::from_buffered_safetensors(
            std::fs::read(assets.join("model.safetensors"))?,
            DType::F32,
            &Device::Cpu,
        )?;
        let model = Qwen3Model::new(cfg, vb)?;
        let mut tokenizer = tokenizers::Tokenizer::from_file(assets.join("tokenizer.json"))
            .map_err(|_| anyhow::anyhow!("invalid tokenizer asset"))?;
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            direction: tokenizers::PaddingDirection::Left,
            ..Default::default()
        }));
        Ok(Self {
            model: Qwen3TextEmbedding::new(model, tokenizer),
            metadata: metadata()?,
        })
    }
}
impl EmbeddingProvider for Provider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.model
                .embed(inputs)
                .map_err(|_| EmbeddingError::Unavailable)
        })
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        (args.len() == 4 && matches!(args[1].as_str(), "create" | "query"))
            || (args.len() == 5 && args[1] == "query-page")
            || (args.len() >= 7 && args[1] == "create-notion"),
        "usage: qwen-lance-spike <create|query> ASSETS INDEX; query-page ASSETS INDEX QUESTION; create-notion ASSETS INDEX ROOT PAGE WORKSPACE [EXCLUDED_PAGE ...]"
    );
    if args[1] == "query-page" {
        ensure!(!args[4].trim().is_empty(), "question must not be empty");
    }
    // `remote` is a compile workaround; always pass LanceDB a local absolute path.
    let index = std::path::absolute(&args[3]).context("resolve local index directory")?;
    let expected = metadata()?;
    if matches!(args[1].as_str(), "query" | "query-page") {
        let persisted: EmbeddingMetadata = serde_json::from_slice(
            &std::fs::read(index.join("embedding.json"))
                .context("missing persisted embedding metadata")?,
        )?;
        persisted
            .ensure_compatible(&expected)
            .context("incompatible vector metadata; explicitly rebuild index")?;
    } else {
        ensure!(
            !index.exists(),
            "index already exists; use a new path for an explicit rebuild"
        );
    }
    let source_chunks = if args[1] == "create-notion" {
        ensure!(
            std::env::var("NK_NOTION_AUTH").as_deref() == Ok("integration"),
            "NK_NOTION_AUTH=integration is required"
        );
        let token = std::env::var("NOTION_TOKEN")
            .map_err(|_| anyhow::anyhow!("NOTION_TOKEN is required"))?;
        let client = notion_knowledge_notion::NotionClient::integration(&token)?;
        let mut rules = notion_knowledge_core::discovery::ExclusionRules::default();
        for excluded in &args[7..] {
            rules
                .page_ids
                .insert(notion_knowledge_notion::pages::page_id(excluded)?.0);
        }
        let snapshot = client
            .discover_selected_document(
                &args[4],
                &args[5],
                &args[6],
                &rules,
                notion_knowledge_core::chunking::ChunkConfig::default(),
            )
            .await?;
        ensure!(
            snapshot.documents.len() == 1 && !snapshot.chunks.is_empty(),
            "selected page has no indexable content"
        );
        ensure!(
            snapshot.chunks.len() <= 32,
            "spike page exceeds 32 chunk bound"
        );
        println!(
            "selected_documents=1 allowed_discovered={} skipped={} selected_chunks={}",
            snapshot.discovery.pages.len(),
            snapshot.discovery.skipped.len(),
            snapshot.chunks.len()
        );
        Some(snapshot.chunks)
    } else {
        None
    };
    let start = Instant::now();
    let provider = Provider::load(Path::new(&args[2]))?;
    println!("model_load_ms={}", start.elapsed().as_millis());
    let db = lancedb::connect(index.to_str().context("index path must be UTF-8")?)
        .execute()
        .await?;
    if !matches!(args[1].as_str(), "query" | "query-page") {
        let start = Instant::now();
        let chunks = source_chunks.unwrap_or_else(chunks);
        let vectors = embedding::embed_batch(
            &provider,
            &chunks.iter().map(|c| c.text.clone()).collect::<Vec<_>>(),
        )
        .await?;
        println!("document_embed_ms={}", start.elapsed().as_millis());
        let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vectors
                .into_iter()
                .map(|v| Some(v.into_iter().map(Some).collect::<Vec<_>>())),
            DIM as i32,
        );
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("chunk_id", DataType::Utf8, false),
                Field::new("text", DataType::Utf8, false),
                Field::new("chunk_record", DataType::Utf8, false),
                Field::new("vector", vector.data_type().clone(), true),
            ])
            .with_metadata(std::collections::HashMap::from([(
                "embedding".to_string(),
                serde_json::to_string(provider.metadata())?,
            )])),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    chunks
                        .iter()
                        .map(|c| c.chunk_id.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunks
                        .iter()
                        .map(serde_json::to_string)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )),
                Arc::new(vector),
            ],
        )?;
        db.create_table("chunks", batch).execute().await?;
        std::fs::write(
            index.join("embedding.json"),
            serde_json::to_vec_pretty(provider.metadata())?,
        )?;
        println!(
            "persisted_rows={} metadata_schema=1 dimension={DIM} model_revision={REVISION}",
            chunks.len()
        );
    } else {
        let start = Instant::now();
        let question = if args[1] == "query-page" {
            format!(
                "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: {}",
                args[4]
            )
        } else {
            QUERY.to_string()
        };
        let query = embedding::embed_batch(&provider, &[question])
            .await?
            .remove(0);
        println!("query_embed_ms={}", start.elapsed().as_millis());
        let table = db.open_table("chunks").execute().await?;
        let schema = table.schema().await?;
        let actual: EmbeddingMetadata = serde_json::from_str(
            schema
                .metadata()
                .get("embedding")
                .context("table missing embedding identity")?,
        )?;
        actual
            .ensure_compatible(provider.metadata())
            .context("incompatible table vector metadata; rebuild index")?;
        let vector = schema
            .field_with_name("vector")
            .context("table missing vector")?;
        ensure!(
            matches!(vector.data_type(), DataType::FixedSizeList(item, dimension) if *dimension == DIM as i32 && item.data_type() == &DataType::Float32),
            "incompatible table vector schema; rebuild index"
        );
        let start = Instant::now();
        let batches: Vec<RecordBatch> = table
            .vector_search(query)?
            .distance_type(DistanceType::Cosine)
            .bypass_vector_index()
            .limit(2)
            .execute()
            .await?
            .try_collect()
            .await?;
        println!("vector_query_ms={}", start.elapsed().as_millis());
        let batch = batches.first().context("no results")?;
        let ids = batch
            .column_by_name("chunk_id")
            .context("missing chunk_id")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("invalid chunk_id schema")?;
        let distances = batch
            .column_by_name("_distance")
            .context("missing distance")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid distance schema")?;
        ensure!(
            !ids.is_empty()
                && (args[1] == "query-page"
                    || (ids.len() == 2 && ids.value(0) == "fixture-backups")),
            "unexpected fixture ranking"
        );
        let records = batch
            .column_by_name("chunk_record")
            .context("missing canonical record")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("invalid canonical record schema")?;
        for row in 0..ids.len() {
            ensure!(distances.value(row).is_finite(), "nonfinite score");
            if args[1] == "query-page" {
                let chunk: IndexedChunk = serde_json::from_str(records.value(row))?;
                // Explicit local output contains selected source provenance; do not commit live output.
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "chunk_id": chunk.chunk_id, "page_id": chunk.metadata.page_id,
                        "title": chunk.metadata.title, "url": chunk.metadata.url,
                        "heading_path": chunk.metadata.heading_path, "last_edited_time": chunk.metadata.last_edited_time,
                        "content_hash": chunk.content_hash, "text": chunk.text,
                        "cosine_similarity": 1.0 - distances.value(row)
                    }))?
                );
            }
            println!(
                "chunk_id={} cosine_similarity={}",
                ids.value(row),
                1.0 - distances.value(row)
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_assets_fail_before_runtime_initialization() {
        let dir = std::env::temp_dir().join(format!("nk-missing-assets-{}", std::process::id()));
        assert!(!dir.exists());
        assert!(
            Provider::load(&dir)
                .err()
                .unwrap()
                .to_string()
                .contains("missing model asset: config.json")
        );
    }
    #[test]
    fn same_dimension_different_model_space_requires_rebuild() {
        let incompatible = EmbeddingMetadata::new(
            "different-provider".into(),
            "Qwen/Qwen3-Embedding-0.6B".into(),
            REVISION.into(),
            DIM,
        )
        .unwrap();
        assert_eq!(
            metadata().unwrap().ensure_compatible(&incompatible),
            Err(EmbeddingError::IncompatibleIndex)
        );
    }
    #[test]
    fn synthetic_chunks_preserve_canonical_identity_and_provenance() {
        for chunk in chunks() {
            let restored: IndexedChunk =
                serde_json::from_str(&serde_json::to_string(&chunk).unwrap()).unwrap();
            assert_eq!(restored, chunk);
            assert!(
                chunk
                    .metadata
                    .url
                    .starts_with("https://example.invalid/fixture/")
            );
        }
    }
}
