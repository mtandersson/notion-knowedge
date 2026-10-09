//! Credential-free recovery checks for the two separate local storage classes.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use notion_knowledge_core::sync_state::{CrawlCheckpoint, PageSyncState, SyncStateStore};
use notion_knowledge_retrieval::sync_state::{LATEST_SCHEMA_VERSION, SqliteSyncStateStore};

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("nk-recovery-{name}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn stopped_operational_database_restores_migrations_and_progress() {
    let temp = TempDir::new("sqlite");
    let source = temp.path("state.sqlite3");
    let backup = temp.path("backup.sqlite3");
    let destination = temp.path("restored.sqlite3");
    let page = PageSyncState::present("page-1".into(), "sha256:known".into(), None).unwrap();
    let checkpoint = CrawlCheckpoint::new("root".into(), Some("cursor-9".into())).unwrap();

    let store = SqliteSyncStateStore::open(&source).unwrap();
    store.put_page_state(&page).unwrap();
    store.put_checkpoint(&checkpoint).unwrap();
    assert!(store.register_webhook_event("event-9").unwrap());
    drop(store); // Offline copy: copying an active SQLite file alone is unsafe.

    fs::copy(&source, &backup).unwrap();
    fs::copy(&backup, &destination).unwrap();
    let restored = SqliteSyncStateStore::open(&destination).unwrap();
    assert_eq!(restored.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(restored.page_state("page-1").unwrap(), Some(page));
    assert_eq!(
        restored.checkpoint("root").unwrap(),
        Some(checkpoint)
    );
    assert!(!restored.register_webhook_event("event-9").unwrap());
    // Separate coordinator index/DB identity and inode binding is not claimed.
}

#[cfg(feature = "local-lancedb")]
mod derived_index {
    use super::*;
    use std::collections::BTreeMap;

    use notion_knowledge_core::{
        embedding::EmbeddingMetadata,
        indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    };
    use notion_knowledge_retrieval::chunks::{
        EmbeddedChunk, LanceChunkTable, VectorDistance, VectorIndexConfig,
    };

    fn chunk(id: &str) -> IndexedChunk {
        IndexedChunk {
            schema_version: SchemaVersion::V1,
            chunk_id: id.into(),
            metadata: IndexedMetadata {
                page_id: "page-1".into(),
                block_id: Some("block-1".into()),
                url: "https://example.invalid/page-1".into(),
                title: "Recovery fixture".into(),
                heading_path: vec!["Runbook".into()],
                last_edited_time: "2026-10-09T00:00:00Z".into(),
                source: SourceMetadata {
                    workspace_id: "workspace-1".into(),
                    root_page_id: "root-1".into(),
                    database_id: None,
                    data_source_id: None,
                },
                properties: BTreeMap::new(),
            },
            text: "Vehicle telemetry recovery procedure".into(),
            content_hash: "sha256:stable-source".into(),
            links: vec![],
        }
    }

    fn row(id: &str, page: &str, text: &str, vector: Vec<f32>) -> EmbeddedChunk {
        let mut value = chunk(id);
        value.metadata.page_id = page.into();
        value.metadata.url = format!("https://example.invalid/{page}");
        value.text = text.into();
        value.content_hash = format!("hash:{text}");
        EmbeddedChunk::new(value, vector)
    }

    fn seed_rows(id: &str) -> Vec<EmbeddedChunk> {
        vec![
            row(
                id,
                "page-1",
                "Vehicle telemetry recovery procedure",
                vec![1.0, 0.0, 0.0],
            ),
            row(
                "decoy-bread",
                "page-2",
                "Baking sourdough bread",
                vec![0.0, 1.0, 0.0],
            ),
            row(
                "decoy-forest",
                "page-3",
                "Hiking in the forest",
                vec![0.0, 0.0, 1.0],
            ),
            row(
                "decoy-calendar",
                "page-4",
                "Appointments next week",
                vec![5.0, 5.0, 5.0],
            ),
        ]
    }

    async fn assert_search(table: &LanceChunkTable, id: &str) {
        table.optimize_fts_index().await.unwrap();
        let hits = table.fts_query("text", "telemetry", 5).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].page_id, "page-1");
        assert_eq!(hits[0].chunk_id, id);

        table
            .ensure_vector_index(&VectorIndexConfig {
                distance: VectorDistance::L2,
                num_partitions: Some(1),
                sample_rate: 8,
                max_iterations: 20,
            })
            .await
            .unwrap();
        let hits = table
            .vector_query(&[1.0, 0.0, 0.0], 1, 1)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].page_id, "page-1");
        assert_eq!(hits[0].chunk_id, id);
    }

    #[tokio::test]
    async fn derived_lancedb_can_be_discarded_and_rebuilt_without_old_ids() {
        let temp = TempDir::new("lance");
        let path = temp.path("index");
        let metadata = EmbeddingMetadata::new(
            "fixture-provider".into(),
            "fixture-model".into(),
            "revision-1".into(),
            3,
        )
        .unwrap();

        let old = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .unwrap();
        old.upsert(&metadata, &seed_rows("old-id")).await.unwrap();
        assert_search(&old, "old-id").await;
        drop(old);
        fs::remove_dir_all(&path).unwrap();

        let rebuilt = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .unwrap();
        assert_eq!(rebuilt.count_rows().await.unwrap(), 0);
        rebuilt
            .upsert(&metadata, &seed_rows("new-id"))
            .await
            .unwrap();
        assert_search(&rebuilt, "new-id").await;
        assert!(
            rebuilt
                .fts_query("chunk_id", "old-id", 5)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
