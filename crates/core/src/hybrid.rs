//! Weighted reciprocal-rank fusion. Native scores are not compared across paths.
use crate::search::{
    HybridSearch, LexicalQuery, LexicalSearch, RetrievalPath, SearchFuture, SearchHit,
    SearchUnavailable, SemanticQuery, SemanticSearch,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Each path contributes weight / (rank_constant + one-based rank).
#[derive(Debug, Clone, Copy)]
pub struct ReciprocalRankFusion {
    pub rank_constant: f64,
    pub semantic_weight: f64,
    pub lexical_weight: f64,
    /// Fixed candidate budget per path, independent of final output limit.
    pub candidate_limit: usize,
}
impl Default for ReciprocalRankFusion {
    fn default() -> Self {
        Self {
            rank_constant: 60.0,
            semantic_weight: 1.0,
            lexical_weight: 1.0,
            candidate_limit: 100,
        }
    }
}
pub struct HybridFusion {
    semantic: Arc<dyn SemanticSearch>,
    lexical: Arc<dyn LexicalSearch>,
    config: ReciprocalRankFusion,
}
impl HybridFusion {
    pub fn new(
        semantic: Arc<dyn SemanticSearch>,
        lexical: Arc<dyn LexicalSearch>,
        config: ReciprocalRankFusion,
    ) -> Result<Self, SearchUnavailable> {
        if !config.rank_constant.is_finite()
            || config.rank_constant < 0.0
            || [config.semantic_weight, config.lexical_weight]
                .iter()
                .any(|w| !w.is_finite() || *w <= 0.0 || *w > 1.0)
            || !(1..=100).contains(&config.candidate_limit)
        {
            return Err(SearchUnavailable);
        }
        Ok(Self {
            semantic,
            lexical,
            config,
        })
    }
}
impl HybridSearch for HybridFusion {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_> {
        Box::pin(async move {
            if query.query.trim().is_empty()
                || query.query.chars().count() > 4096
                || !(1..=self.config.candidate_limit).contains(&query.limit)
                || [&query.page_ids, &query.root_page_ids]
                    .into_iter()
                    .flatten()
                    .any(|ids| {
                        ids.is_empty()
                            || ids.len() > 100
                            || ids
                                .iter()
                                .any(|id| id.trim().is_empty() || id.chars().count() > 128)
                    })
            {
                return Err(SearchUnavailable);
            }
            let lexical_query = LexicalQuery {
                query: query.query.clone(),
                limit: self.config.candidate_limit,
                page_ids: query.page_ids.clone(),
                root_page_ids: query.root_page_ids.clone(),
            };
            let limit = query.limit;
            let semantic = self
                .semantic
                .search(SemanticQuery {
                    limit: self.config.candidate_limit,
                    ..query
                })
                .await?;
            let lexical = self.lexical.search(lexical_query).await?;
            fuse(semantic, lexical, self.config, limit)
        })
    }
}
fn fuse(
    semantic: Vec<SearchHit>,
    lexical: Vec<SearchHit>,
    config: ReciprocalRankFusion,
    limit: usize,
) -> Result<Vec<SearchHit>, SearchUnavailable> {
    let mut candidates: BTreeMap<(String, String), (SearchHit, f64)> = BTreeMap::new();
    for (hits, path, weight) in [
        (semantic, RetrievalPath::Semantic, config.semantic_weight),
        (lexical, RetrievalPath::Lexical, config.lexical_weight),
    ] {
        if hits.len() > config.candidate_limit {
            return Err(SearchUnavailable);
        }
        let mut seen = BTreeSet::new();
        for (index, mut hit) in hits.into_iter().enumerate() {
            if !hit.score.is_finite()
                || hit.source.page_id.is_empty()
                || hit.source.chunk_id.is_empty()
                || hit.source.url.is_empty()
                || hit.text.chars().count() > 2000
            {
                return Err(SearchUnavailable);
            }
            let key = (hit.source.page_id.clone(), hit.source.chunk_id.clone());
            if !seen.insert(key.clone()) {
                continue;
            }
            let contribution = weight / (config.rank_constant + (index + 1) as f64);
            hit.matched_paths = vec![path];
            match candidates.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((hit, contribution));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let (existing, score) = entry.get_mut();
                    // Conflicting snapshots cannot be presented as one trustworthy chunk.
                    if existing.text != hit.text
                        || existing.source.url != hit.source.url
                        || existing.source.title != hit.source.title
                        || existing.source.heading_path != hit.source.heading_path
                        || existing.source.block_id != hit.source.block_id
                    {
                        return Err(SearchUnavailable);
                    }
                    existing.matched_paths.push(path);
                    *score += contribution;
                }
            }
        }
    }
    let mut ranked: Vec<_> = candidates.into_iter().collect();
    ranked.sort_by(|(ka, (_, sa)), (kb, (_, sb))| sb.total_cmp(sa).then_with(|| ka.cmp(kb)));
    Ok(ranked
        .into_iter()
        .take(limit)
        .map(|(_, (mut hit, score))| {
            hit.score = score as f32;
            hit
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::SearchSource;
    fn hit(id: &str) -> SearchHit {
        SearchHit {
            text: id.into(),
            score: 999.0,
            matched_paths: vec![],
            source: SearchSource {
                page_id: "page".into(),
                chunk_id: id.into(),
                url: "https://example.invalid/page".into(),
                title: "Title".into(),
                heading_path: vec![],
                block_id: None,
            },
        }
    }
    #[test]
    fn disagreement_merges_shared_chunks_and_retains_single_path_candidates() {
        let results = fuse(
            vec![hit("semantic"), hit("shared")],
            vec![hit("lexical"), hit("shared")],
            ReciprocalRankFusion::default(),
            3,
        )
        .unwrap();
        assert_eq!(
            results
                .iter()
                .map(|h| h.source.chunk_id.as_str())
                .collect::<Vec<_>>(),
            ["shared", "lexical", "semantic"]
        );
        assert_eq!(
            results[0].matched_paths,
            [RetrievalPath::Semantic, RetrievalPath::Lexical]
        );
        assert_eq!(results[1].matched_paths, [RetrievalPath::Lexical]);
        assert_eq!(results[2].matched_paths, [RetrievalPath::Semantic]);
        assert!((results[0].score - 2.0 / 62.0).abs() < 1e-7);
    }
    #[test]
    fn repeated_candidates_do_not_gain_duplicate_rank_credit() {
        let results = fuse(
            vec![hit("a"), hit("a"), hit("b")],
            vec![hit("b")],
            ReciprocalRankFusion::default(),
            2,
        )
        .unwrap();
        assert_eq!(results[0].source.chunk_id, "b");
        assert_eq!(results[1].matched_paths, [RetrievalPath::Semantic]);
        assert!((results[1].score - 1.0 / 61.0).abs() < 1e-7);
    }
    #[test]
    fn weights_and_rank_constant_control_order_and_scores() {
        let config = ReciprocalRankFusion {
            rank_constant: 0.0,
            semantic_weight: 0.1,
            ..Default::default()
        };
        let results = fuse(vec![hit("a")], vec![hit("b")], config, 1).unwrap();
        assert_eq!(results[0].source.chunk_id, "b");
        assert_eq!(results[0].score, 1.0);
    }
    #[test]
    fn empty_rankers_and_conflicting_snapshots_are_explicit() {
        assert!(
            fuse(vec![], vec![], ReciprocalRankFusion::default(), 3)
                .unwrap()
                .is_empty()
        );
        let mut conflicting = hit("a");
        conflicting.text = "changed".into();
        assert!(
            fuse(
                vec![hit("a")],
                vec![conflicting],
                ReciprocalRankFusion::default(),
                1
            )
            .is_err()
        );
        assert_eq!(
            fuse(vec![], vec![hit("a")], ReciprocalRankFusion::default(), 1).unwrap()[0]
                .matched_paths,
            [RetrievalPath::Lexical]
        );
    }
}
