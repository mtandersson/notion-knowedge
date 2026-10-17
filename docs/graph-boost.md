# Bounded graph-neighborhood boost (#65)

Graph boost is an **explicit, off-by-default** decorator on the existing hybrid
reciprocal-rank fusion port. It is NOT a new search data source and does not
enable graph-only query hits or introduce new MCP tools.

## Scope and ranking

The trusted composition root installs an existing scoped HybridSearch, a
GraphEdgeStore (the existing derived SQLite Notion page-edges port), and a
RootScopeGate, then opts in via KnowledgeServer::and_graph_boost(graph, config).
A caller cannot enable boosting from untrusted query JSON. By default the
configuration is enabled=false, max_hops=1, weight=0.15, candidate_limit=20.
Allowed parameters: hop count 1..2, finite weight in (0,0.5], candidate limit
1..100. An invalid setting is rejected; enabled mode rejects result limits
larger than its candidate budget.

The base adapter fetches the configured number of *already scope-filtered*
hybrid candidates. No graph page is added to those hits, fetched as content,
or used as a traversal node unless it was also in those hybrid candidates;
all intermediate nodes at hop 2 must satisfy the same condition. Only resolved
page-to-page GraphTarget::Page edges participate, in either direction.
Unresolved references and neighbors outside the candidate set do not confer
retrieval evidence or authorization. Per-call explicit page filters are
enforced before any graph lookup. The MCP knowledge_search handler continues
to authorize **each returned page** with fresh physical ancestry and trusted
root scope before exposing any snippet.

Each expanded page has a combined incoming/outgoing adjacency budget of 256,
including edges to pages outside the candidate set and unresolved references.
The incoming read receives only the budget remaining after outgoing edges.
With at most 100 candidate pages, this bounds accepted adjacency to 25,600
entries per search; SQLite consumes at most one additional overflow sentinel
per read. Committed adapters additionally cap each source projection at 256
outgoing entries before validating its full receipt. Any overflow fails the
search without returning partially reranked results.

Expansion is seeded from the first double-matched (semantic + lexical) hit
among the top 3. Single-path hits cannot seed another boost; without such a
seed, results are unchanged and no graph reads occur. For eligible neighbors
with shortest hop distance h, the internal ranking bonus is the lesser of
(seed baseline score * weight / h) and 95% of the gap to the seed's score.
The seed itself gets zero boost, and graph neighbors cannot overtake it
solely due to graph evidence. Tie ordering is stable by page/chunk ID.
Enabled graph read errors fail closed rather than silently claiming a
graph-boosted response.

**Crucial:** SearchHit.score remains the original hybrid fusion/RRF score,
unchanged even if graph boost reorders the results. The detailed pure
rank_candidates() API returns GraphScoredHit records containing baseline_score,
graph_bonus, rank_score, and hop count for inspectable comparisons. Raw native
semantic and FTS scores are not added; the original hybrid score is always
retained. The MCP output's score may therefore not be monotonically descending
when graph reranking is enabled; document that instead of pretending it is the
graph-adjusted score.

## Evaluation and deployment

Compare the original HybridSearch with GraphBoostedHybrid using identical
filtered candidates and trusted roots, or query the two modes through an
adapter that preserves and explicitly echoes its configuration for #102's
paired offline evaluation. The #102 harness and fixture tests are separate;
they do not prove production uplift. Do not activate graph boost as a
production default without real corpus measurements, false-positive review
and root-scope validation.

Core and MCP tests cover opt-out identity, full base-score preservation,
hop limits, resolved/unresolved and off-candidate edges, root filtering,
seed qualification, invalid config, storage failure, deterministic ranks,
and unchanged MCP authorization.

This functionality remains independent of #63 (graph navigation tool).
