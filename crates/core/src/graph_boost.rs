//! Conservative opt-in graph neighborhood reranking of existing hybrid hits.
//! Graph edges are not an authorization or retrieval source. Only page IDs
//! returned by the scope-filtered hybrid index may become BFS nodes.
//! SearchHit.score always stays the original hybrid/RRF baseline score.

use crate::{
    backend::PageId,
    graph::{GraphEdgeStore, GraphTarget},
    root_scope::RootScopeGate,
    search::{
        HybridSearch, RetrievalPath, SearchFuture, SearchHit, SearchUnavailable, SemanticQuery,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

/// Maximum combined incoming/outgoing edges per expanded page. With at most
/// 100 candidate pages this bounds each search to 25,600 accepted edges.
pub const MAX_GRAPH_BOOST_ADJACENCY: usize = 256;

/// Feature is disabled by default and must be explicitly opted into.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphBoostConfig {
    pub enabled: bool,
    pub max_hops: usize,
    pub weight: f64,
    pub candidate_limit: usize,
}
impl Default for GraphBoostConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_hops: 1,
            weight: 0.15,
            candidate_limit: 20,
        }
    }
}
impl GraphBoostConfig {
    pub fn validate(self) -> Result<Self, SearchUnavailable> {
        if !(1..=2).contains(&self.max_hops)
            || !self.weight.is_finite()
            || self.weight <= 0.0
            || self.weight > 0.5
            || !(1..=100).contains(&self.candidate_limit)
        {
            return Err(SearchUnavailable);
        }
        Ok(self)
    }
}

/// Inspectable per-candidate breakdown; the original hit score is unmodified.
#[derive(Debug, Clone)]
pub struct GraphScoredHit {
    pub hit: SearchHit,
    pub baseline_score: f32,
    pub graph_bonus: f32,
    pub rank_score: f32,
    pub hops: Option<usize>,
}

/// Opt-in hybrid decorator. MCP must authorize returned sources independently.
pub struct GraphBoostedHybrid {
    inner: Arc<dyn HybridSearch>,
    graph: Arc<dyn GraphEdgeStore>,
    config: GraphBoostConfig,
    scope: Option<RootScopeGate>,
}
impl GraphBoostedHybrid {
    pub fn new(
        inner: Arc<dyn HybridSearch>,
        graph: Arc<dyn GraphEdgeStore>,
        config: GraphBoostConfig,
    ) -> Result<Self, SearchUnavailable> {
        Ok(Self {
            inner,
            graph,
            config: config.validate()?,
            scope: None,
        })
    }

    pub fn config(&self) -> GraphBoostConfig {
        self.config
    }

    /// All hybrid candidates must be authorized BEFORE graph traversal.
    /// Otherwise an excluded intermediate could change visible result order.
    pub fn with_root_gate(mut self, gate: RootScopeGate) -> Self {
        self.scope = Some(gate);
        self
    }
}
impl HybridSearch for GraphBoostedHybrid {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            if !self.config.enabled {
                return self.inner.search(query).await;
            }
            if !(1..=100).contains(&query.limit) || self.config.candidate_limit < query.limit {
                return Err(SearchUnavailable);
            }
            // Hold one owned graph read fence across the underlying index read,
            // graph traversal and awaited physical authorization. Persistent
            // adapters must allow nested read snapshots but exclude all writers.
            let snapshot = self.graph.read_snapshot().map_err(|_| SearchUnavailable)?;
            let graph = snapshot.as_deref().unwrap_or(self.graph.as_ref());
            let limit = query.limit;
            let allowed_page_ids = query.page_ids.clone();
            let requested_roots = query.root_page_ids.clone();
            let candidates = self
                .inner
                .search(SemanticQuery {
                    limit: self.config.candidate_limit,
                    ..query
                })
                .await?;
            if let Some(scope) = &self.scope {
                let roots = requested_roots.unwrap_or_else(|| {
                    scope
                        .configured_scope()
                        .roots
                        .iter()
                        .map(|root| root.0.clone())
                        .collect()
                });
                for candidate in &candidates {
                    let permit = scope
                        .authorize(&PageId(candidate.source.page_id.clone()))
                        .await
                        .map_err(|_| SearchUnavailable)?;
                    if !permit.belongs_to_any(&roots)
                        || scope
                            .revalidate(&permit, scope.configured_scope())
                            .await
                            .is_err()
                    {
                        return Err(SearchUnavailable);
                    }
                }
            }
            let reranked = rank_candidates(
                candidates,
                graph,
                self.config,
                limit,
                allowed_page_ids.as_deref(),
            )?;
            Ok(reranked.into_iter().map(|entry| entry.hit).collect())
        })
    }
}

/// Graph traversal cannot expand beyond the hybrid candidate page identities,
/// even as intermediate nodes in a two-hop traversal. Explicit page filters
/// enforce an additional fail-closed boundary on all candidate hits.
pub fn rank_candidates(
    hits: Vec<SearchHit>,
    graph: &dyn GraphEdgeStore,
    config: GraphBoostConfig,
    limit: usize,
    explicit_page_ids: Option<&[String]>,
) -> Result<Vec<GraphScoredHit>, SearchUnavailable> {
    config.validate()?;
    if !(1..=config.candidate_limit).contains(&limit) || hits.len() > config.candidate_limit {
        return Err(SearchUnavailable);
    }
    let mut unique = BTreeSet::new();
    for hit in &hits {
        if !hit.score.is_finite()
            || hit.score < 0.0
            || hit.source.page_id.is_empty()
            || hit.source.chunk_id.is_empty()
            || !unique.insert((hit.source.page_id.clone(), hit.source.chunk_id.clone()))
            || explicit_page_ids.is_some_and(|filter| !filter.contains(&hit.source.page_id))
        {
            return Err(SearchUnavailable);
        }
    }
    if !config.enabled {
        return Ok(hits
            .into_iter()
            .take(limit)
            .map(|hit| GraphScoredHit {
                baseline_score: hit.score,
                rank_score: hit.score,
                graph_bonus: 0.0,
                hops: None,
                hit,
            })
            .collect());
    }
    // Require high-confidence overlap of two independent retrieval paths
    // among the first three hits. Single-path hits never seed expansion.
    let seed = hits.iter().take(3).find(|hit| {
        hit.score > 0.0
            && hit.matched_paths.contains(&RetrievalPath::Semantic)
            && hit.matched_paths.contains(&RetrievalPath::Lexical)
    });
    let Some(seed) = seed else {
        return Ok(hits
            .into_iter()
            .take(limit)
            .map(|hit| GraphScoredHit {
                baseline_score: hit.score,
                rank_score: hit.score,
                graph_bonus: 0.0,
                hops: None,
                hit,
            })
            .collect());
    };
    let seed_page = seed.source.page_id.clone();
    let eligible_pages: BTreeSet<_> = hits.iter().map(|hit| hit.source.page_id.clone()).collect();
    let mut distance = BTreeMap::from([(seed_page.clone(), 0_usize)]);
    let mut queue = VecDeque::from([seed_page]);
    // The candidate budget bounds the BFS graph reads. Unresolved references
    // and out-of-candidate intermediate pages are never traversed.
    while let Some(page) = queue.pop_front() {
        let depth = distance[&page];
        if depth >= config.max_hops {
            continue;
        }
        let outgoing = graph
            .edges_from_bounded(&page, MAX_GRAPH_BOOST_ADJACENCY)
            .map_err(|_| SearchUnavailable)?;
        let incoming = graph
            .edges_to_bounded(
                &page,
                MAX_GRAPH_BOOST_ADJACENCY.saturating_sub(outgoing.len()),
            )
            .map_err(|_| SearchUnavailable)?;
        if outgoing.len().saturating_add(incoming.len()) > MAX_GRAPH_BOOST_ADJACENCY {
            return Err(SearchUnavailable);
        }
        for edge in outgoing.into_iter().chain(incoming) {
            let other = if edge.source_page_id() == page {
                match edge.target() {
                    GraphTarget::Page { page_id } => Some(page_id.as_str()),
                    GraphTarget::Unresolved { .. } => None,
                }
            } else if matches!(edge.target(), GraphTarget::Page { page_id } if page_id == &page) {
                Some(edge.source_page_id())
            } else {
                return Err(SearchUnavailable);
            };
            if let Some(other) = other
                && eligible_pages.contains(other)
                && !distance.contains_key(other)
            {
                distance.insert(other.to_owned(), depth + 1);
                queue.push_back(other.to_owned());
            }
        }
    }
    let seed_score = f64::from(seed.score);
    let mut scored: Vec<_> = hits
        .into_iter()
        .map(|hit| {
            let baseline_score = hit.score;
            let hops = distance
                .get(&hit.source.page_id)
                .copied()
                .filter(|hop| *hop > 0);
            // Never promote a graph neighbor above the trusted dual-path seed.
            let graph_bonus = hops.map_or(0.0, |hop| {
                let raw = seed_score * config.weight / hop as f64;
                let seed_gap = (seed_score - f64::from(baseline_score)).max(0.0);
                raw.min(seed_gap * 0.95)
            });
            let rank_score = f64::from(baseline_score) + graph_bonus;
            GraphScoredHit {
                baseline_score,
                graph_bonus: graph_bonus as f32,
                rank_score: rank_score as f32,
                hops,
                hit,
            }
        })
        .collect();
    scored.sort_by(|a, b| {
        b.rank_score.total_cmp(&a.rank_score).then_with(|| {
            (&a.hit.source.page_id, &a.hit.source.chunk_id)
                .cmp(&(&b.hit.source.page_id, &b.hit.source.chunk_id))
        })
    });
    scored.truncate(limit);
    Ok(scored)
}
