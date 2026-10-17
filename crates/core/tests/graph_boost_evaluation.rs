//! Integration with the *real* #65 graph reranker and graph-edge contract.
//! Baseline hit scores are artificial for deterministic behavioral coverage:
//! this is NOT an embedding/model quality score or production uplift estimate.
use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    graph_boost::{GraphBoostConfig, GraphScoredHit, rank_candidates},
    search::{RetrievalPath, SearchHit, SearchSource},
    sync_state::SyncStateError,
};
use serde_json::Value;
use std::collections::BTreeMap;

struct FixtureGraph {
    edges: Vec<GraphEdge>,
}
impl GraphEdgeStore for FixtureGraph {
    fn replace_page_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn replace_page_relation_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn replace_page_link_edges(&self, _: &str, _: &[GraphEdge]) -> Result<(), SyncStateError> {
        Err(SyncStateError::Unavailable)
    }
    fn edges_from(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        Ok(self
            .edges
            .iter()
            .filter(|e| e.source_page_id() == page)
            .cloned()
            .collect())
    }
    fn edges_to(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        Ok(self
            .edges
            .iter()
            .filter(|e| matches!(e.target(), GraphTarget::Page { page_id } if page_id == page))
            .cloned()
            .collect())
    }
    fn edges_from_bounded(
        &self,
        page: &str,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, SyncStateError> {
        let edges = self.edges_from(page)?;
        if edges.len() > limit {
            return Err(SyncStateError::Unavailable);
        }
        Ok(edges)
    }
    fn edges_to_bounded(&self, page: &str, limit: usize) -> Result<Vec<GraphEdge>, SyncStateError> {
        let edges = self.edges_to(page)?;
        if edges.len() > limit {
            return Err(SyncStateError::Unavailable);
        }
        Ok(edges)
    }
}
fn hit(page: &str, chunk: &str, score: f32, dual_path: bool) -> SearchHit {
    SearchHit {
        text: "Fictional indexed content".into(),
        score,
        source: SearchSource {
            page_id: page.to_owned(),
            chunk_id: chunk.to_owned(),
            url: format!("https://example.invalid/{}", page.replace(':', "/")),
            title: "Fictional".into(),
            heading_path: vec![],
            block_id: None,
            last_edited_time: "2026-03-02T00:00:00Z".into(),
        },
        matched_paths: if dual_path {
            vec![RetrievalPath::Semantic, RetrievalPath::Lexical]
        } else {
            vec![RetrievalPath::Semantic]
        },
    }
}
fn pos(rows: &[GraphScoredHit], id: &str) -> Option<usize> {
    rows.iter().position(|r| r.hit.source.chunk_id == id)
}
fn read_fixture() -> (Value, Value) {
    (
        serde_json::from_str(include_str!(
            "../../../eval/retrieval/personal-knowledge-v1.json"
        ))
        .expect("versioned fictional corpus"),
        serde_json::from_str(include_str!("../../../eval/retrieval/graph-boost-v1.json"))
            .expect("graph case manifest"),
    )
}
#[test]
fn bilingual_graph_cases_exercise_real_reranker_without_new_candidates() {
    let (dataset, cases) = read_fixture();
    let mut pages = BTreeMap::<String, Vec<String>>::new();
    let mut owner = BTreeMap::<String, String>::new();
    for page in dataset["pages"].as_array().unwrap() {
        let id = page["id"].as_str().unwrap().to_owned();
        let chunks: Vec<_> = page["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|chunk| chunk["id"].as_str().unwrap().to_owned())
            .collect();
        for chunk in &chunks {
            owner.insert(chunk.clone(), id.clone());
        }
        pages.insert(id, chunks);
    }
    let mut edges = Vec::new();
    for page in dataset["pages"].as_array().unwrap() {
        let source = page["id"].as_str().unwrap();
        for target in page["links"].as_array().unwrap() {
            let target_id = target.as_str().unwrap();
            assert!(pages.contains_key(target_id), "fixture edge must resolve");
            edges.push(
                GraphEdge::new(
                    source.into(),
                    GraphTarget::Page {
                        page_id: target_id.into(),
                    },
                    "link:page".into(),
                    "markdown:1".into(),
                )
                .unwrap(),
            );
        }
    }
    let graph = FixtureGraph { edges };
    let config = GraphBoostConfig {
        enabled: true,
        max_hops: cases["config"]["max_hops"].as_u64().unwrap() as usize,
        weight: cases["config"]["weight"].as_f64().unwrap(),
        candidate_limit: cases["config"]["candidate_limit"].as_u64().unwrap() as usize,
    };
    config.validate().expect("must match #65 constraints");
    let mut linked = 0;
    let mut controls = 0;
    let mut controls_with_guard_promotion = 0;
    let mut improvements = 0;
    for case in cases["cases"].as_array().unwrap() {
        let anchor = case["anchor_page_id"].as_str().unwrap();
        let seed = &pages[anchor][0];
        let guard = case["guard_source_ids"][0].as_str().unwrap();
        let target = case["target_source_id"].as_str();
        let neutral = "fixture:chunk:travel:list";
        assert_ne!(seed, neutral);
        assert_ne!(guard, neutral);

        // This rank order is intentionally predetermined: it verifies the
        // production reranker's behavior, NOT real lexical/vector accuracy.
        let mut before = vec![
            hit(anchor, seed, 1.0, true),
            hit(&owner[neutral], neutral, 0.93, false),
        ];
        if let Some(target) = target {
            linked += 1;
            assert_ne!(seed, target);
            before.push(hit(&owner[target], target, 0.85, false));
        } else {
            controls += 1;
        }
        if guard != seed && !before.iter().any(|h| h.source.chunk_id == guard) {
            before.push(hit(&owner[guard], guard, 0.82, false));
        }
        let baseline = rank_candidates(
            before.clone(),
            &graph,
            GraphBoostConfig {
                enabled: false,
                ..config
            },
            before.len(),
            None,
        )
        .unwrap();
        let boosted = rank_candidates(before, &graph, config, baseline.len(), None).unwrap();
        assert_eq!(baseline.len(), boosted.len());
        let baseline_ids: std::collections::BTreeSet<_> = baseline
            .iter()
            .map(|r| r.hit.source.chunk_id.as_str())
            .collect();
        let boosted_ids: std::collections::BTreeSet<_> = boosted
            .iter()
            .map(|r| r.hit.source.chunk_id.as_str())
            .collect();
        assert_eq!(baseline_ids, boosted_ids, "graph must never add a chunk");
        for candidate in &boosted {
            let original = baseline
                .iter()
                .find(|b| b.hit.source.chunk_id == candidate.hit.source.chunk_id)
                .unwrap();
            assert_eq!(candidate.hit.score, original.hit.score);
            assert_eq!(candidate.baseline_score, original.baseline_score);
        }
        assert_eq!(
            boosted[0].hit.source.chunk_id, *seed,
            "graph may not overtake seed"
        );
        if let Some(target) = target {
            if pos(&boosted, target) < pos(&baseline, target) {
                improvements += 1;
            }
        } else if pos(&boosted, guard) < pos(&baseline, guard) {
            controls_with_guard_promotion += 1;
        }
    }
    assert_eq!(linked, 6);
    assert_eq!(controls, 6);
    assert_eq!(
        improvements, 6,
        "fixture edges ought to lift linked answers"
    );
    assert_eq!(
        controls_with_guard_promotion, 2,
        "date-review negative controls must expose the irrelevant connected decision"
    );
}
