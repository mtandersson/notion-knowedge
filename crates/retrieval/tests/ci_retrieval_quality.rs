//! Small offline regression gate for actual LanceDB FTS and vector retrieval.
//!
//! This checks known-answer ranking for a tiny bilingual synthetic corpus;
//! it does not claim model relevance, live Notion coverage or production SLAs.
#![cfg(feature = "local-lancedb")]

use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use notion_knowledge_core::{
    embedding::EmbeddingMetadata,
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
};
use notion_knowledge_retrieval::chunks::{
    EmbeddedChunk, LanceChunkTable, VectorDistance, VectorIndexConfig,
};

const THRESHOLD_TOP1: f64 = 0.75;

fn fixture_chunk(id: &str, page: &str, title: &str, text: &str) -> IndexedChunk {
    IndexedChunk {
        schema_version: SchemaVersion::V1,
        chunk_id: format!("ci-regression:{id}"),
        metadata: IndexedMetadata {
            page_id: format!("ci-regression:{page}"),
            block_id: None,
            url: format!("https://example.invalid/ci-regression/{page}"),
            title: title.into(),
            heading_path: vec![],
            last_edited_time: "2026-10-09T00:00:00Z".into(),
            source: SourceMetadata {
                workspace_id: "ci-fixtures".into(),
                root_page_id: "ci-root".into(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: text.into(),
        content_hash: format!("fixture-hash:{id}"),
        links: vec![],
    }
}

#[derive(Clone, Copy)]
struct Probe {
    id: &'static str,
    text: &'static str,
    vector: [f32; 3],
    expected_chunk: &'static str,
}

const PROBES: &[Probe] = &[
    Probe {
        id: "en-telemetry",
        text: "telemetry",
        vector: [1.0, 0.0, 0.0],
        expected_chunk: "ci-regression:vehicle",
    },
    Probe {
        id: "en-sourdough",
        text: "sourdough",
        vector: [0.0, 1.0, 0.0],
        expected_chunk: "ci-regression:bread",
    },
    Probe {
        id: "sv-vandring",
        text: "vandring",
        vector: [0.0, 0.0, 1.0],
        expected_chunk: "ci-regression:hike",
    },
    Probe {
        id: "sv-hamtning",
        text: "hämtning",
        vector: [2.0, 2.0, 2.0],
        expected_chunk: "ci-regression:pickup",
    },
];

fn fixture_path() -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!(
        "notion-knowledge-ci-retrieval-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test]
async fn native_fts_and_vector_top1_quality_gate() {
    let path = fixture_path();
    let embedding = EmbeddingMetadata::new(
        "ci-fixture-provider".into(),
        "ci-synthetic-vectors".into(),
        "v1".into(),
        3,
    )
    .unwrap();
    let table = LanceChunkTable::create(&path, "chunks", embedding.clone())
        .await
        .unwrap();
    table
        .upsert(
            &embedding,
            &[
                EmbeddedChunk::new(
                    fixture_chunk("vehicle", "vehicle", "Vehicle logs", "Vehicle telemetry records"),
                    vec![1.0, 0.0, 0.0],
                ),
                EmbeddedChunk::new(
                    fixture_chunk("bread", "bread", "Baking", "Sourdough bread starter"),
                    vec![0.0, 1.0, 0.0],
                ),
                EmbeddedChunk::new(
                    fixture_chunk("hike", "hike", "Vandring", "Planera en vandring med ryggsäck"),
                    vec![0.0, 0.0, 1.0],
                ),
                EmbeddedChunk::new(
                    fixture_chunk("pickup", "pickup", "Paket", "Tid för hämtning av paket"),
                    vec![2.0, 2.0, 2.0],
                ),
            ],
        )
        .await
        .unwrap();

    table.optimize_fts_index().await.unwrap();
    table
        .ensure_vector_index(&VectorIndexConfig {
            distance: VectorDistance::L2,
            num_partitions: Some(1),
            sample_rate: 8,
            max_iterations: 20,
        })
        .await
        .unwrap();

    let mut lexical_misses = Vec::new();
    let mut vector_misses = Vec::new();
    for probe in PROBES {
        let lexical = table.fts_query("text", probe.text, 1).await.unwrap();
        if lexical.first().map(|hit| hit.chunk_id.as_str()) != Some(probe.expected_chunk) {
            lexical_misses.push(probe.id);
        }
        let semantic = table.vector_query(&probe.vector, 1, 1).await.unwrap();
        if semantic.first().map(|hit| hit.chunk_id.as_str()) != Some(probe.expected_chunk) {
            vector_misses.push(probe.id);
        }
    }
    let lexical_top1 = (PROBES.len() - lexical_misses.len()) as f64 / PROBES.len() as f64;
    let vector_top1 = (PROBES.len() - vector_misses.len()) as f64 / PROBES.len() as f64;
    assert!(
        lexical_top1 >= THRESHOLD_TOP1,
        "native FTS recall@1 {lexical_top1:.3} below {THRESHOLD_TOP1:.3}; failed: {lexical_misses:?}"
    );
    assert!(
        vector_top1 >= THRESHOLD_TOP1,
        "native vector recall@1 {vector_top1:.3} below {THRESHOLD_TOP1:.3}; failed: {vector_misses:?}"
    );

    drop(table);
    fs::remove_dir_all(path).unwrap();
}
