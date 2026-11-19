#![cfg(all(
    target_os = "linux",
    target_env = "gnu",
    feature = "local-lancedb",
    feature = "local-qwen"
))]
//! Manual #739 probe: resident memory by phase, as the worker composes it:
//! Qwen load, embedding at the configured thread count, page commits, then
//! search preparation (FTS + vector index). Peaks use VmHWM, reset per phase.
//! `NK_QWEN_ASSETS=... NK_PROBE_PAGES=2850 RAYON_NUM_THREADS=8 cargo test
//! --release --features local-lancedb,local-qwen --test it memory_phases --
//! --ignored --nocapture`.
use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    indexed::{IndexedChunk, IndexedMetadata, SchemaVersion, SourceMetadata},
    reconciliation::{ReconciliationJournal, ReconciliationScope},
    sync_state::PageSyncState,
};
use notion_knowledge_retrieval::{
    chunks::{GuardedChunkTable, VectorDistance, VectorIndexConfig},
    commit::{CommitBinding, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation},
    qwen::{QwenConfig, QwenProvider},
};
use std::{collections::BTreeMap, sync::Arc};

fn status(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix(key))
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
    fn malloc_trim(pad: usize) -> i32;
}
fn report(phase: &str) {
    let m = unsafe { mallinfo2() };
    eprintln!(
        "phase={phase} rss_mib={} hwm_mib={} heap_in_use_mib={} heap_free_held_mib={} mmap_mib={}",
        status("VmRSS:") >> 10,
        status("VmHWM:") >> 10,
        m.uordblks >> 20,
        m.fordblks >> 20,
        m.hblkhd >> 20
    );
    // Reset the peak so the next phase reports its own high-water mark.
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
}
fn trim(phase: &str) {
    unsafe { malloc_trim(0) };
    report(&format!("{phase}+trim"));
}

/// Cheap deterministic vectors so commit and index phases do not need the
/// model; spread over the sphere so IVF training sees realistic variety.
struct Fake(EmbeddingMetadata);
impl EmbeddingProvider for Fake {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            Ok(inputs
                .iter()
                .map(|s| {
                    let mut x = s.bytes().fold(1469598103u64, |h, b| {
                        (h ^ b as u64).wrapping_mul(1099511628211)
                    });
                    (0..1024)
                        .map(|_| {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            (x % 2001) as f32 / 1000.0 - 1.0
                        })
                        .collect()
                })
                .collect())
        })
    }
}

fn prose(seed: usize, chars: usize) -> String {
    const WORDS: &[&str] = &[
        "projektet",
        "planering",
        "mötet",
        "beslut",
        "kunden",
        "leverans",
        "the",
        "service",
        "deploy",
        "memory",
        "index",
        "search",
        "veckan",
        "ansvarig",
        "uppföljning",
        "risk",
        "budget",
        "arkitektur",
        "dokumentation",
        "integration",
        "Notion",
        "databas",
        "sidan",
    ];
    let mut out = String::new();
    let mut i = seed;
    while out.len() < chars {
        out.push_str(WORDS[i % WORDS.len()]);
        out.push(if i.is_multiple_of(11) { '.' } else { ' ' });
        i = i.wrapping_mul(31).wrapping_add(7);
    }
    out
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
        text: prose(page.len() * 7 + n * 13 + page.as_bytes()[7] as usize, 1400),
        content_hash: format!("hash:{page}:{n}"),
        links: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual RSS probe"]
async fn memory_phases() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    report("start");
    let assets = std::env::var("NK_QWEN_ASSETS").expect("NK_QWEN_ASSETS");
    let qwen = QwenProvider::load(QwenConfig::cpu(assets)).await.unwrap();
    report("qwen_loaded");
    // The installed server loads one provider for the sync worker and one for
    // MCP retrieval from the same assets (#739); both must share the weights.
    let query = QwenProvider::load(QwenConfig::cpu(std::env::var("NK_QWEN_ASSETS").unwrap()))
        .await
        .unwrap();
    report("second_provider_loaded");
    let (pages, queries) = ([prose(1, 1400), prose(2, 1400)], [prose(1, 1400)]);
    let (a, b) = tokio::join!(qwen.embed_batch(&pages), query.embed_batch(&queries));
    assert_eq!(a.unwrap()[0], b.unwrap()[0]);
    report("concurrent_embeds");
    drop(query);
    for len in [500, 1400, 2000, 3800] {
        for i in 0..env("NK_PROBE_EMBEDS", 4) {
            qwen.embed_batch(&[prose(i + len, len)]).await.unwrap();
        }
        report(&format!("embedded_chars_{len}"));
    }
    trim("embedded");

    let pages = env("NK_PROBE_PAGES", 2850);
    let root = std::env::temp_dir().join(format!("nk-phase-probe-{}", std::process::id()));
    let index = root.join("index");
    std::fs::create_dir_all(&index).unwrap();
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
    let binding = CommitBinding::new("chunks", "workspace", &scope, "v1", 1).unwrap();
    let coordinator =
        IndexCommitCoordinator::initialize(&index, root.join("state.sqlite"), binding).unwrap();
    let lease = coordinator.state().acquire_lease(10, 100).unwrap();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 10);
    let fake = Fake(qwen.metadata().clone());
    let guarded =
        GuardedChunkTable::bind(&index, "chunks", fake.0.clone(), coordinator.clone()).unwrap();
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
        let prepared = guarded.prepare_page(&fake, op, &chunks).await.unwrap();
        assert_eq!(
            guarded
                .commit_page(prepared, lease.clone(), clock.clone(), || async { Ok(()) })
                .await,
            Ok(CommitOutcome::Applied)
        );
        if (p + 1) % 500 == 0 {
            report(&format!("committed_{}", p + 1));
        }
    }
    report("committed");
    trim("committed");
    guarded
        .ensure_startup_indexes(lease.clone(), clock.clone())
        .await
        .unwrap();
    report("fts_ready");
    guarded
        .ensure_vector_index(
            VectorIndexConfig {
                distance: VectorDistance::Cosine,
                ..Default::default()
            },
            lease.clone(),
            clock.clone(),
        )
        .await
        .unwrap();
    report("vector_index_ready");
    trim("prepared");
    guarded.verify_search_indexes().await.unwrap();
    report("verified");
    let _ = std::fs::remove_dir_all(&root);
}
