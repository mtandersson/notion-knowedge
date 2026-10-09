//! Credential-free regression against production LanceDB FTS and hybrid ports.
//! A deterministic fixture vector provider exercises the real ANN/fusion path;
//! it is NOT a production embedding-quality benchmark.
#![cfg(feature = "local-lancedb")]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    hybrid::{HybridFusion, ReciprocalRankFusion},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    search::{HybridSearch, LexicalQuery, LexicalSearch, SemanticQuery},
};
use notion_knowledge_retrieval::chunks::{
    EmbeddedChunk, LanceChunkTable, LanceSemanticSearch, VectorDistance, VectorIndexConfig,
};
use serde_json::Value;

fn dataset() -> Value {
    serde_json::from_str(include_str!(
        "../../../eval/retrieval/exact-identifiers-v1.json"
    ))
    .expect("versioned exact identifier dataset")
}

fn field<'a>(value: &'a Value, name: &str) -> &'a str {
    value[name].as_str().expect("required string field")
}

fn fixture_vector(text: &str) -> Vec<f32> {
    // Deliberately weak but nonconstant vectors: the test checks whether
    // lexical evidence survives the actual vector index and RRF fusion.
    let checksum = text.bytes().fold(0_u64, |sum, value| sum + u64::from(value));
    vec![
        (text.len() % 29 + 1) as f32,
        (checksum % 31 + 1) as f32,
        1.0,
    ]
}

struct FixtureEmbedding(EmbeddingMetadata);

impl EmbeddingProvider for FixtureEmbedding {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }

    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move { Ok(inputs.iter().map(|text| fixture_vector(text)).collect()) })
    }
}

fn temp_database() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("valid clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "notion-exact-ids-{}-{nonce}",
        std::process::id()
    ))
}

fn passes_recall_gate(hits: usize, total: usize) -> bool {
    // >= 80% recall@3 on each actual retrieval path; never hide missing cases
    // by using an average across modes or dropping empty-query responses.
    total > 0 && hits * 5 >= total * 4
}

#[test]
fn exact_identifier_fixture_has_unique_sources_and_explicit_near_neighbors() {
    let data = dataset();
    assert_eq!(data["schema_version"], 1);
    assert_eq!(data["dataset_id"], "exact-identifier-fixtures-v1");
    let pages = data["pages"].as_array().unwrap();
    let mut sources = BTreeSet::new();
    for page in pages {
        assert!(field(page, "id").starts_with("fixture:page:"));
        assert!(field(page, "updated_at") <= field(&data, "as_of"));
        for chunk in page["chunks"].as_array().unwrap() {
            assert!(sources.insert(field(chunk, "id")));
            assert!(!field(chunk, "text").trim().is_empty());
        }
    }
    let queries = data["queries"].as_array().unwrap();
    assert!(queries.len() >= 10);
    assert!(queries.iter().any(|q| q["language"] == "sv"));
    assert!(queries.iter().any(|q| q["language"] == "en"));
    let mut query_ids = BTreeSet::new();
    for query in queries {
        assert!(query_ids.insert(field(query, "id")));
        assert!(!field(query, "text").trim().is_empty());
        let relevant = query["relevant"].as_array().unwrap();
        let excluded = query["excluded_source_ids"].as_array().unwrap();
        assert_eq!(relevant.len(), 1);
        assert!(!excluded.is_empty());
        let target = field(&relevant[0], "source_id");
        assert!(sources.contains(target));
        assert_eq!(relevant[0]["grade"], 3);
        for other in excluded {
            let id = other.as_str().unwrap();
            assert!(sources.contains(id) && id != target);
        }
    }
    assert!(passes_recall_gate(8, 10));
    assert!(!passes_recall_gate(7, 10));
}

#[tokio::test]
async fn actual_fts_and_hybrid_preserve_exact_identifiers_at_three() {
    let data = dataset();
    let path = temp_database();
    let metadata = EmbeddingMetadata::new(
        "fixture-provider".into(),
        "deterministic-regression".into(),
        "v1".into(),
        3,
    )
    .unwrap();
    let table = Arc::new(
        LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .expect("create real LanceDB index"),
    );
    let mut rows = Vec::new();
    for page in data["pages"].as_array().unwrap() {
        for chunk in page["chunks"].as_array().unwrap() {
            let id = field(chunk, "id");
            let body = field(chunk, "text");
            rows.push(EmbeddedChunk::new(
                IndexedChunk {
                    schema_version: SchemaVersion::V1,
                    chunk_id: id.into(),
                    metadata: IndexedMetadata {
                        page_id: field(page, "id").into(),
                        block_id: None,
                        url: format!("https://example.invalid/{}", field(page, "id")),
                        title: field(page, "title").into(),
                        heading_path: Vec::new(),
                        last_edited_time: "2026-02-10T12:00:00Z".into(),
                        source: SourceMetadata {
                            workspace_id: "fixture-workspace".into(),
                            root_page_id: "fixture-root".into(),
                            database_id: None,
                            data_source_id: None,
                        },
                        properties: BTreeMap::new(),
                    },
                    text: body.into(),
                    content_hash: format!("fixture:{id}"),
                    links: Vec::new(),
                },
                fixture_vector(body),
            ));
        }
    }
    table.upsert(&metadata, &rows).await.expect("insert real rows");
    table.optimize_fts_index().await.expect("maintain BM25 indices");
    table
        .ensure_vector_index(&VectorIndexConfig {
            distance: VectorDistance::L2,
            num_partitions: Some(1),
            sample_rate: 8,
            max_iterations: 20,
        })
        .await
        .expect("train real vector index");

    let provider = Arc::new(FixtureEmbedding(metadata));
    let semantic = Arc::new(
        LanceSemanticSearch::new(Arc::clone(&table), provider, 1)
            .expect("compatible fixture provider"),
    );
    let hybrid = HybridFusion::new(
        semantic,
        table.clone(),
        ReciprocalRankFusion {
            // Test the documented lexical-favoring configuration, not a claim
            // about semantic model relevance or the production default.
            semantic_weight: 0.1,
            lexical_weight: 1.0,
            candidate_limit: 10,
            ..ReciprocalRankFusion::default()
        },
    )
    .expect("valid hybrid fusion");

    let queries = data["queries"].as_array().unwrap();
    let mut fts_hits = 0;
    let mut hybrid_hits = 0;
    let mut fts_misses = Vec::new();
    let mut hybrid_misses = Vec::new();
    for query in queries {
        let id = field(query, "id");
        let text = field(query, "text").to_owned();
        let target = field(&query["relevant"][0], "source_id");
        let fts = table
            .search(LexicalQuery {
                query: text.clone(),
                limit: 3,
                page_ids: None,
                root_page_ids: None,
                metadata: Default::default(),
            })
            .await
            .expect("real FTS query");
        let fused = hybrid
            .search(SemanticQuery {
                query: text,
                limit: 3,
                page_ids: None,
                root_page_ids: None,
                metadata: Default::default(),
            })
            .await
            .expect("real vector + FTS fusion");
        if fts.iter().any(|h| h.source.chunk_id == target) {
            fts_hits += 1;
        } else {
            fts_misses.push(id);
        }
        if fused.iter().any(|h| h.source.chunk_id == target) {
            hybrid_hits += 1;
        } else {
            hybrid_misses.push(id);
        }
        if id == "chunk-id-en" || id == "page-id-sv" {
            assert_eq!(fts.first().map(|h| h.source.chunk_id.as_str()), Some(target));
        }
    }
    assert!(
        passes_recall_gate(fts_hits, queries.len()),
        "FTS direct-answer recall@3 is {fts_hits}/{}; missed {fts_misses:?}",
        queries.len()
    );
    assert!(
        passes_recall_gate(hybrid_hits, queries.len()),
        "hybrid direct-answer recall@3 is {hybrid_hits}/{}; missed {hybrid_misses:?}",
        queries.len()
    );
    drop(hybrid);
    drop(table);
    std::fs::remove_dir_all(path).expect("remove test index");
}
