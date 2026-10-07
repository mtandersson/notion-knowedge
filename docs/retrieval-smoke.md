# Integrated retrieval smoke

The hybrid knowledge-search epic (#6) provides semantic and lexical adapters,
core fusion, typed narrowing, compact MCP results, indexed source expansion and
optional authoritative reads. Run the following from the repository root in the
pinned Nix environment. Tests use temporary indexes and deterministic fixtures;
they need neither Notion credentials nor model downloads.

```sh
nix develop --command cargo test -p notion-knowledge-core -p notion-knowledge-mcp --locked
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --locked
nix develop --command cargo test -p notion-knowledge-server --test stdio --test http --locked
```

The checks exercise the integration boundaries separately:

| Behavior | Authoritative automated evidence |
| --- | --- |
| Actual vector and BM25 retrieval with narrowing before top-k | `crates/retrieval/src/chunks.rs` tests, particularly `typed_metadata_filters_select_lower_ranked_candidates_before_both_limits`: higher-ranked disallowed rows lose to the allowed row at limit 1 in both paths. Root/source/property/date filters combine, including escaped IDs. |
| Hybrid ranking and identical filter propagation | Core fusion tests and `crates/mcp/tests/search_adapter.rs::configured_hybrid_fuses_both_ports_and_serializes_path_provenance`: the real `HybridFusion` receives fixture ranker ports through the MCP handler, returns structured path provenance, and forwards identical metadata with configured candidate depth. |
| Server scope cannot be expanded by the caller | `configured_roots_always_narrow_all_search_modes` covers semantic, lexical and hybrid calls; outside-only requests never invoke a retrieval port. |
| Compact safe excerpts and stable freshness/citation metadata | `configured_snippets_redact_signed_targets_and_preserve_provenance` checks Unicode bounds, URL target removal, timestamp, chunk ID, score and matched path. Invalid adapter outputs fail closed. |
| Indexed page/chunk expansion | `crates/mcp/tests/get_adapter.rs::expands_page_and_chunk_refs_with_server_owned_scope_and_provenance` checks stable references, root scope, lineage and untrusted-content annotation. Lance adapter tests separately exercise persisted source resolution. |
| Optional fresh-source verification | The same MCP test suite checks stale and unchanged timestamps, stable page-ID reads, indexed default behavior, shared-page caching, Unicode budgets, authorization rejection, concurrent edits and failure without partial output. Fresh reads leave the subsequent indexed snapshot unchanged. |
| Public transport contract | HTTP and stdio process tests discover and validate the shared search/get catalog, including typed filters and freshness fields, and explicitly reject unavailable dependencies. |

These are component integration checks, not a single live-Notion or full-server
retrieval test. The default binary still reports retrieval unavailable; production
composition remains #104. To compose an application, inject the Lance semantic
and lexical ports into `HybridFusion`, configure the same authorized root scope
for search and source expansion, and inject `NotionRead` for fresh mode. See
[hybrid fusion](hybrid-search.md), [search filters](knowledge-search.md), and
[fresh-source behavior](fresh-source.md) for contracts and limitations.

The smoke does not establish real-model relevance or live Notion permissions.
It also does not promise one atomic snapshot across hybrid rankers or different
fresh pages; each adapter enforces its own documented read consistency boundary.
