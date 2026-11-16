#![cfg(all(target_os = "linux", target_env = "gnu", feature = "local-lancedb"))]
//! Manual #656 probe: resident memory against committed pages, split into
//! live heap and freed-but-retained allocator bytes. With the fake embedder
//! it isolates Lance commits; with `NK_QWEN_ASSETS` (feature local-qwen) it
//! adds real model activations. Compare `MALLOC_ARENA_MAX` unset vs 2:
//! `NK_PROBE_PAGES=40 NK_PROBE_REPS=16 NK_PROBE_GAP_MS=2000 cargo test
//! --release --features local-lancedb,local-qwen --test commit_memory --
//! --ignored --nocapture`.
use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    reconciliation::{ReconciliationJournal, ReconciliationScope},
    sync_state::PageSyncState,
};
use notion_knowledge_retrieval::{
    chunks::GuardedChunkTable,
    commit::{CommitBinding, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation},
};
use std::{collections::BTreeMap, sync::Arc};

const DIM: usize = 1024;

struct Fake(EmbeddingMetadata);
impl EmbeddingProvider for Fake {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            Ok(inputs
                .iter()
                .map(|s| (0..DIM).map(|i| ((s.len() + i) % 7) as f32 + 0.5).collect())
                .collect())
        })
    }
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap()
}

#[repr(C)]
#[derive(Default)]
struct MallInfo2 {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}
unsafe extern "C" {
    fn mallinfo2() -> MallInfo2;
}
fn heap() -> String {
    let m = unsafe { mallinfo2() };
    format!(
        "arena_mib={} in_use_mib={} free_held_mib={} mmap_mib={}",
        m.arena >> 20,
        m.uordblks >> 20,
        m.fordblks >> 20,
        m.hblkhd >> 20
    )
}

fn chunk(page: &str, n: usize) -> IndexedChunk {
    IndexedChunk {
        schema_version: SchemaVersion::V1,
        chunk_id: format!("{page}-{n}"),
        metadata: IndexedMetadata {
            page_id: page.into(),
            block_id: Some("block".into()),
            url: "https://example.invalid/page".into(),
            title: format!("Title {page}"),
            heading_path: vec!["Section".into()],
            last_edited_time: "revision-v1".into(),
            source: SourceMetadata {
                workspace_id: "workspace".into(),
                root_page_id: "root".into(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: format!("{page} chunk {n} ").repeat(
            std::env::var("NK_PROBE_REPS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(80)
                + n * 2,
        ),
        content_hash: format!("hash:{page}:{n}"),
        links: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual RSS probe"]
async fn probe_rss_against_committed_pages() {
    let pages: usize = std::env::var("NK_PROBE_PAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let root = std::env::temp_dir().join(format!("nk-rss-probe-{}", std::process::id()));
    let index = root.join("index");
    std::fs::create_dir_all(&index).unwrap();
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let binding = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
    let coordinator =
        IndexCommitCoordinator::initialize(&index, root.join("state.sqlite"), binding).unwrap();
    let lease = coordinator.state().acquire_lease(10, 100).unwrap();
    let embedding = EmbeddingMetadata::new("fake".into(), "m".into(), "v1".into(), DIM).unwrap();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 10);
    #[cfg(feature = "local-qwen")]
    let provider: Box<dyn EmbeddingProvider> = match std::env::var("NK_QWEN_ASSETS") {
        Ok(assets) => Box::new(
            notion_knowledge_retrieval::qwen::QwenProvider::load(
                notion_knowledge_retrieval::qwen::QwenConfig::cpu(assets),
            )
            .await
            .unwrap(),
        ),
        Err(_) => Box::new(Fake(embedding)),
    };
    #[cfg(not(feature = "local-qwen"))]
    let provider: Box<dyn EmbeddingProvider> = Box::new(Fake(embedding));
    let embedding = provider.metadata().clone();
    let guarded =
        GuardedChunkTable::bind(&index, "chunks", embedding.clone(), coordinator.clone()).unwrap();
    guarded
        .create_empty(lease.clone(), clock.clone())
        .await
        .unwrap();
    for p in 0..pages {
        let page = format!("{p:08}-0000-0000-0000-000000000000");
        let checkpoint =
            PageSyncState::present(page.clone(), format!("h{p}"), Some("revision-v1".into()))
                .unwrap();
        let op = PageOperation::new(
            &format!("op{p}"),
            "revision-v1",
            PageAction::Refresh,
            checkpoint,
        )
        .unwrap();
        let chunks: Vec<_> = (0..4).map(|n| chunk(&page, n)).collect();
        let prepared = guarded
            .prepare_page(provider.as_ref(), op, &chunks)
            .await
            .unwrap();
        assert_eq!(
            guarded
                .commit_page(prepared, lease.clone(), clock.clone(), || async { Ok(()) })
                .await,
            Ok(CommitOutcome::Applied)
        );
        tokio::time::sleep(std::time::Duration::from_millis(
            std::env::var("NK_PROBE_GAP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        ))
        .await;
        if p % 10 == 0 || p + 1 == pages {
            eprintln!("probe pages={} rss_kib={} {}", p + 1, rss_kib(), heap());
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}
