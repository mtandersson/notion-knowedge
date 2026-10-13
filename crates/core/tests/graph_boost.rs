use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    graph_boost::{GraphBoostConfig, GraphBoostedHybrid, rank_candidates},
    search::{
        HybridSearch, RetrievalPath, SearchFuture, SearchHit, SearchSource,
        SearchUnavailable, SemanticQuery,
    },
    sync_state::SyncStateError,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

fn hit(page: &str, id: &str, score: f32, joined: bool) -> SearchHit {
    SearchHit {
        text: "Safe indexed text".into(),
        score,
        source: SearchSource {
            page_id: page.into(), chunk_id: id.into(),
            last_edited_time: "2026-10-10T00:00:00Z".into(),
            url: format!("https://example.invalid/{page}"),
            title: page.into(), heading_path: vec![], block_id: None,
        },
        matched_paths: if joined {
            vec![RetrievalPath::Semantic, RetrievalPath::Lexical]
        } else { vec![RetrievalPath::Semantic] },
    }
}
fn edge(from: &str, to: &str) -> GraphEdge {
    GraphEdge::new(
        from.into(),
        GraphTarget::Page { page_id: to.into() },
        "link:page".into(),
        "markdown:1".into(),
    ).unwrap()
}
fn unresolved(from: &str) -> GraphEdge {
    GraphEdge::new(
        from.into(),
        GraphTarget::Unresolved { reference: "unknown".into() },
        "link:invalid".into(),
        "markdown:2".into(),
    ).unwrap()
}
#[derive(Default)]
struct GraphFixture {
    edges: Vec<GraphEdge>,
    calls: AtomicUsize,
    fail: bool,
}
impl GraphFixture {
    fn with_edges(edges: Vec<GraphEdge>) -> Self {
        Self { edges, ..Self::default() }
    }
    fn lookups(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
impl GraphEdgeStore for GraphFixture {
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail { return Err(SyncStateError::Unavailable); }
        Ok(self.edges.iter().filter(|e| e.source_page_id() == page).cloned().collect())
    }
    fn edges_to(&self, page: &str) -> Result<Vec<GraphEdge>, SyncStateError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail { return Err(SyncStateError::Unavailable); }
        Ok(self.edges.iter().filter(|e| {
            matches!(e.target(), GraphTarget::Page { page_id } if page_id == page)
        }).cloned().collect())
    }
}
fn on(max_hops: usize, weight: f64, candidate_limit: usize) -> GraphBoostConfig {
    GraphBoostConfig { enabled: true, max_hops, weight, candidate_limit }
}
fn corpus() -> Vec<SearchHit> {
    vec![
        hit("root-a", "a", 1.0, true),
        hit("unrelated", "x", 0.91, false),
        hit("linked", "b", 0.85, false),
        hit("second-hop", "c", 0.80, false),
    ]
}
fn ids(hits: &[notion_knowledge_core::graph_boost::GraphScoredHit]) -> Vec<&str> {
    hits.iter().map(|r| r.hit.source.chunk_id.as_str()).collect()
}
#[test]
fn disabled_by_default_does_not_even_read_graph() {
    let g = GraphFixture { fail: true, ..Default::default() };
    let config = GraphBoostConfig::default();
    assert!(!config.enabled);
    let output = rank_candidates(corpus(), &g, config, 2, None).unwrap();
    assert_eq!(ids(&output), ["a", "x"]);
    assert_eq!(g.lookups(), 0);
    assert!(output.iter().all(|h| h.graph_bonus == 0.0 && h.hops.is_none()));
}
#[test]
fn one_hop_boosts_only_previously_retrieved_neighbor_and_preserves_scores() {
    let g = GraphFixture::with_edges(vec![
        edge("root-a", "linked"),
        edge("root-a", "graph-only"),
        unresolved("root-a"),
    ]);
    let output = rank_candidates(corpus(), &g, on(1, 0.2, 8), 4, None).unwrap();
    assert_eq!(ids(&output), ["a", "b", "x", "c"]);
    assert_eq!(output.len(), 4);
    let b = output.iter().find(|r| r.hit.source.chunk_id == "b").unwrap();
    assert_eq!(b.hops, Some(1));
    assert!((b.baseline_score - 0.85).abs() < 0.000001);
    assert_eq!(b.hit.score, b.baseline_score);
    assert!((b.graph_bonus - 0.1425).abs() < 0.000001);
    assert_eq!(g.lookups(), 2);
    assert!(!output.iter().any(|r| r.hit.source.page_id == "graph-only"));
}
#[test]
fn two_hops_require_each_intermediate_page_to_be_hybrid_candidate() {
    let g = GraphFixture::with_edges(vec![
        edge("root-a", "linked"),
        edge("linked", "second-hop"),
        edge("root-a", "external-bridge"),
        edge("external-bridge", "unrelated"),
    ]);
    let one = rank_candidates(corpus(), &g, on(1, 0.2, 8), 4, None).unwrap();
    let two = rank_candidates(corpus(), &g, on(2, 0.2, 8), 4, None).unwrap();
    assert_eq!(one.iter().find(|r| r.hit.source.page_id == "second-hop").unwrap().hops, None);
    assert_eq!(two.iter().find(|r| r.hit.source.page_id == "second-hop").unwrap().hops, Some(2));
    assert_eq!(two.iter().find(|r| r.hit.source.page_id == "unrelated").unwrap().hops, None);
    assert_eq!(two.iter().find(|r| r.hit.source.page_id == "second-hop").unwrap().graph_bonus, 0.1);
}
#[test]
fn graph_only_or_single_path_candidates_never_seed_expansion() {
    let g = GraphFixture::with_edges(vec![edge("root-a", "linked")]);
    let mut hits = corpus();
    hits[0].matched_paths = vec![RetrievalPath::Semantic];
    let output = rank_candidates(hits, &g, on(2, 0.2, 8), 4, None).unwrap();
    assert_eq!(ids(&output), ["a", "x", "b", "c"]);
    assert_eq!(g.lookups(), 0);
}
#[test]
fn explicit_scope_rejects_unapproved_candidates_before_any_graph_lookup() {
    let g = GraphFixture::with_edges(vec![edge("root-a", "linked")]);
    let scoped = vec!["root-a".into(), "linked".into()];
    assert!(rank_candidates(corpus(), &g, on(1, 0.2, 8), 3, Some(&scoped)).is_err());
    assert_eq!(g.lookups(), 0);
    // A real filtered hybrid index returns only in-scope pages.
    let filtered = vec![hit("root-a", "a", 1.0, true), hit("linked", "b", 0.85, false)];
    let output = rank_candidates(filtered, &g, on(1, 0.2, 8), 2, Some(&scoped)).unwrap();
    assert_eq!(ids(&output), ["a", "b"]);
}
#[test]
fn misconfigured_or_unavailable_graph_fails_closed_not_silent_fallback() {
    let g = GraphFixture { fail: true, ..Default::default() };
    assert!(rank_candidates(corpus(), &g, on(1, 0.2, 8), 2, None).is_err());
    assert_eq!(g.lookups(), 1);
    for config in [
        on(0, 0.2, 8), on(3, 0.2, 8),
        on(1, 0.0, 8), on(1, -1.0, 8),
        on(1, f64::NAN, 8), on(1, f64::INFINITY, 8),
        on(1, 0.51, 8), on(1, 0.2, 0), on(1, 0.2, 101),
    ] {
        assert!(config.validate().is_err());
    }
    assert!(rank_candidates(corpus(), &g, on(1, 0.2, 8), 0, None).is_err());
    assert!(rank_candidates(corpus(), &g, on(1, 0.2, 3), 3, None).is_err());
}
#[test]
fn malformed_duplicate_and_nonfinite_source_scores_cannot_seed_graph() {
    let g = GraphFixture::default();
    let mut duplicate = corpus();
    duplicate.push(duplicate[0].clone());
    assert!(rank_candidates(duplicate, &g, on(1, 0.2, 8), 3, None).is_err());
    let mut invalid = corpus();
    invalid[0].score = f32::NAN;
    assert!(rank_candidates(invalid, &g, on(1, 0.2, 8), 3, None).is_err());
    let mut negative = corpus();
    negative[1].score = -0.1;
    assert!(rank_candidates(negative, &g, on(1, 0.2, 8), 3, None).is_err());
    assert_eq!(g.lookups(), 0);
}
struct HybridFixture {
    queries: Mutex<Vec<SemanticQuery>>,
    hits: Vec<SearchHit>,
}
impl HybridSearch for HybridFixture {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        self.queries.lock().unwrap().push(query.clone());
        Box::pin(async move { Ok(self.hits.iter().take(query.limit).cloned().collect()) })
    }
}
fn query(limit: usize) -> SemanticQuery {
    SemanticQuery {
        query: "test".into(), limit, page_ids: None,
        root_page_ids: Some(vec!["trusted-root".into()]),
        metadata: Default::default(),
    }
}
#[tokio::test]
async fn wrapper_changes_only_order_and_fetch_budget_on_explicit_opt_in() {
    let base = Arc::new(HybridFixture { queries: Mutex::new(vec![]), hits: corpus() });
    let graph = Arc::new(GraphFixture::with_edges(vec![edge("root-a", "linked")]));
    let off = GraphBoostedHybrid::new(base.clone(), graph.clone(), GraphBoostConfig::default()).unwrap();
    let plain = off.search(query(3)).await.unwrap();
    assert_eq!(plain.iter().map(|h| h.source.chunk_id.as_str()).collect::<Vec<_>>(), ["a", "x", "b"]);
    assert_eq!(base.queries.lock().unwrap()[0].limit, 3);
    assert_eq!(graph.lookups(), 0);
    let on = GraphBoostedHybrid::new(base.clone(), graph.clone(), on(1, 0.2, 4)).unwrap();
    let boosted = on.search(query(3)).await.unwrap();
    assert_eq!(boosted.iter().map(|h| h.source.chunk_id.as_str()).collect::<Vec<_>>(), ["a", "b", "x"]);
    assert_eq!(base.queries.lock().unwrap()[1].limit, 4);
    assert_eq!(boosted[1].score, 0.85);
    assert_eq!(boosted[1].matched_paths, [RetrievalPath::Semantic]);
}
