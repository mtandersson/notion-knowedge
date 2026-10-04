//! Explicit experimental composition; normal server builds do not load models.
use super::*;
use notion_knowledge_core::search::{
    SearchFuture, SearchHit, SearchSource, SearchUnavailable, SemanticQuery, SemanticSearch,
};
use rmcp::{ServiceExt, transport::stdio};
use std::sync::Mutex;

struct Adapter {
    state: Option<(Arc<Mutex<Provider>>, lancedb::Table)>,
}
impl Adapter {
    async fn open(assets: &Path, index: &Path) -> Result<Self> {
        let expected = metadata()?;
        let index = std::path::absolute(index)?;
        let persisted: EmbeddingMetadata =
            serde_json::from_slice(&std::fs::read(index.join("embedding.json"))?)?;
        persisted.ensure_compatible(&expected)?;
        let db = lancedb::connect(index.to_str().context("invalid local index")?)
            .execute()
            .await?;
        let table = db.open_table("chunks").execute().await?;
        let schema = table.schema().await?;
        let actual: EmbeddingMetadata = serde_json::from_str(
            schema
                .metadata()
                .get("embedding")
                .context("missing table identity")?,
        )?;
        actual.ensure_compatible(&expected)?;
        ensure!(
            matches!(schema.field_with_name("vector")?.data_type(), DataType::FixedSizeList(item, dimension) if *dimension == DIM as i32 && item.data_type() == &DataType::Float32),
            "incompatible vector schema"
        );
        ensure!(table.count_rows(None).await? <= 32, "spike exceeds 32 rows");
        let assets = assets.to_owned();
        let start = Instant::now();
        let provider = tokio::task::spawn_blocking(move || Provider::load(&assets)).await??;
        eprintln!("spike_model_load_ms={}", start.elapsed().as_millis());
        Ok(Self {
            state: Some((Arc::new(Mutex::new(provider)), table)),
        })
    }
    async fn execute(&self, query: SemanticQuery) -> Result<Vec<SearchHit>> {
        let (provider, table) = self
            .state
            .as_ref()
            .context("semantic dependencies unavailable")?;
        let provider = provider.clone();
        let question = format!(
            "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: {}",
            query.query
        );
        let start = Instant::now();
        // Bound concurrent inference by one owned model; blocking CPU work does
        // not monopolize the HTTP/stdio async runtime. Cancellation is deferred.
        let vector = tokio::task::spawn_blocking(move || -> Result<Vec<f32>> {
            let provider = provider
                .lock()
                .map_err(|_| anyhow::anyhow!("model unavailable"))?;
            let vectors = provider
                .model
                .embed(&[question])
                .map_err(|_| anyhow::anyhow!("embedding unavailable"))?;
            ensure!(
                vectors.len() == 1
                    && vectors[0].len() == DIM
                    && vectors[0].iter().all(|v| v.is_finite()),
                "invalid embedding output"
            );
            Ok(vectors.into_iter().next().unwrap())
        })
        .await??;
        eprintln!("spike_query_embed_ms={}", start.elapsed().as_millis());
        // Exact scan of at most 32 authorized rows. Filter before truncation;
        // never interpolate untrusted IDs into SQL predicates.
        let batches: Vec<RecordBatch> = table
            .vector_search(vector)?
            .distance_type(DistanceType::Cosine)
            .bypass_vector_index()
            .limit(33)
            .execute()
            .await?
            .try_collect()
            .await?;
        let mut results = vec![];
        let mut count = 0;
        for batch in batches {
            let records = batch
                .column_by_name("chunk_record")
                .context("missing records")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid records")?;
            let ids = batch
                .column_by_name("chunk_id")
                .context("missing ids")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid ids")?;
            let texts = batch
                .column_by_name("text")
                .context("missing texts")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid texts")?;
            let distances = batch
                .column_by_name("_distance")
                .context("missing distances")?
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("invalid distances")?;
            for row in 0..batch.num_rows() {
                count += 1;
                ensure!(
                    count <= 32
                        && !records.is_null(row)
                        && !ids.is_null(row)
                        && !texts.is_null(row)
                        && !distances.is_null(row),
                    "invalid persisted row"
                );
                let chunk: IndexedChunk = serde_json::from_str(records.value(row))?;
                ensure!(
                    chunk.chunk_id == ids.value(row) && chunk.text == texts.value(row),
                    "inconsistent canonical row"
                );
                let score = 1.0 - distances.value(row);
                ensure!(score.is_finite(), "invalid score");
                if !selected(&query, &chunk) {
                    continue;
                }
                results.push(hit(chunk, score));
            }
        }
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.source.chunk_id.cmp(&b.source.chunk_id))
        });
        results.truncate(query.limit);
        Ok(results)
    }
}
fn selected(query: &SemanticQuery, chunk: &IndexedChunk) -> bool {
    query
        .page_ids
        .as_ref()
        .is_none_or(|ids| ids.contains(&chunk.metadata.page_id))
        && query
            .root_page_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&chunk.metadata.source.root_page_id))
}
fn hit(chunk: IndexedChunk, score: f32) -> SearchHit {
    SearchHit {
        text: chunk.text.chars().take(2000).collect(),
        score,
        source: SearchSource {
            page_id: chunk.metadata.page_id,
            chunk_id: chunk.chunk_id,
            url: chunk.metadata.url,
            title: chunk.metadata.title,
            heading_path: chunk.metadata.heading_path,
            block_id: chunk.metadata.block_id,
        },
    }
}
impl SemanticSearch for Adapter {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move { self.execute(query).await.map_err(|_| SearchUnavailable) })
    }
}
pub async fn run(mode: &str, assets: &Path, index: &Path) -> Result<()> {
    let adapter = match Adapter::open(assets, index).await {
        Ok(adapter) => adapter,
        Err(_) => {
            eprintln!("spike_semantic_dependencies=unavailable; serving structured errors");
            Adapter { state: None }
        }
    };
    let handler = notion_knowledge_mcp::KnowledgeServer::with_search(Arc::new(adapter));
    if mode == "serve-http" {
        let settings = notion_knowledge_server::config::Config::from_env()
            .map_err(|_| anyhow::anyhow!("invalid server configuration"))?;
        notion_knowledge_server::http::serve_with_handler(settings, handler).await?;
    } else {
        let service = handler.serve(stdio()).await?;
        service.waiting().await?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filters_use_or_within_fields_and_and_across_fields() {
        let chunk = chunks().remove(0);
        let mut query = SemanticQuery {
            query: "question".into(),
            limit: 1,
            page_ids: None,
            root_page_ids: None,
        };
        assert!(selected(&query, &chunk));
        query.page_ids = Some(vec!["other".into(), chunk.metadata.page_id.clone()]);
        assert!(selected(&query, &chunk));
        query.root_page_ids = Some(vec!["other".into()]);
        assert!(!selected(&query, &chunk));
        query.root_page_ids = Some(vec![
            "other".into(),
            chunk.metadata.source.root_page_id.clone(),
        ]);
        assert!(selected(&query, &chunk));
        query.page_ids = Some(vec!["other".into()]);
        assert!(!selected(&query, &chunk));
    }
    #[test]
    fn excerpts_are_unicode_bounded_and_keep_canonical_citations() {
        let mut chunk = chunks().remove(0);
        chunk.text = "å🦀".repeat(2000);
        let result = hit(chunk.clone(), 0.25);
        assert_eq!(result.text.chars().count(), 2000);
        assert_eq!(result.source.page_id, chunk.metadata.page_id);
        assert_eq!(result.source.chunk_id, chunk.chunk_id);
        assert_eq!(result.source.url, chunk.metadata.url);
    }
}
