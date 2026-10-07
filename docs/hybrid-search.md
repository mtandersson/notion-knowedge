# Hybrid retrieval fusion

The core `HybridFusion` service combines configured `SemanticSearch` and
`LexicalSearch` ports using weighted reciprocal-rank fusion (RRF). It depends
on neither a model runtime nor an index backend. Configure the production
Lance semantic and lexical adapters, construct `HybridFusion::new` with their
`Arc`s and a `ReciprocalRankFusion` configuration, and inject it through
`KnowledgeServer::and_hybrid_search`. The default server still reports hybrid
retrieval unavailable until composition explicitly configures the dependencies.

Each path contributes `weight / (rank_constant + one_based_rank)` for each
unique page/chunk identity. Default rank constant is 60, both weights are 1,
and each path requests at most 100 candidates. Rank constant must be finite
and nonnegative; weights finite in `(0, 1]`; candidate limit 1–100. Final
request limit must not exceed the configured candidate limit. These settings
are construction-time policy, not MCP client-controlled parameters. Raw BM25
and vector scores are never compared across paths. Input ordering from each
ranker determines rank; final ties use page ID then chunk ID.

Both paths receive identical query and page/root filters, with the configured
candidate depth rather than the final output limit. Duplicate entries within
one ranker contribute only once, at their first occurrence. Shared chunks
receive both contributions and serialize `matched_paths: ["semantic",
"lexical"]`; single-path chunks retain their own path and can survive the
final top-k. Every search hit includes `matched_paths`; nonhybrid results identify their
single retrieval path. The MCP output schema requires it.

Matching page/chunk IDs across rankers with conflicting excerpts or citation metadata fail
closed as unavailable rather than silently mixing snapshots. Failure of either
retrieval path also fails the entire request with no partial results. Both
paths must share configured indexing scope; filters narrow it, never grant
access. This service does not refresh indexes or reconcile their generations.
Candidate truncation and approximate vector recall limit fusion recall; RRF
scores are ranking values rather than probabilities or relevance thresholds.

Tests cover ranker disagreement, duplicate credit, deterministic ties,
single-path survival, configuration, metadata conflict, dependency failure,
and actual MCP protocol serialization through the shared handler:

```sh
nix develop --command cargo test -p notion-knowledge-core -p notion-knowledge-mcp --locked
```
