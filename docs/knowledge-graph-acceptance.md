# Knowledge graph acceptance map (#8)

The original epic requires #60–#65 plus integrated behavior. Closed component
tickets alone do not establish installed acceptance. This map identifies both
component contracts and the production-path tests to run before closing the epic.

| Original requirement | Current authoritative implementation and coverage |
| --- | --- |
| #60 source/target/type/provenance, stable page IDs, unresolved targets, bidirectional lookup and versioned migrations | `core/src/graph.rs`; retrieval graph migrations and `tests/graph.rs` (`graph_edges_roundtrip_across_restart_and_query_both_directions`, schema target/index constraints and explicit label migration). SQLite graph storage remains independent of vector records. |
| #61 idempotent relation updates, stale removal, stable property type plus display name, out-of-scope representation without content indexing, multiple relations | `retrieval/src/refresh.rs` builds authorized relation proposals; `commit.rs` commits projections with checkpoint/receipt. `tests/relations.rs` covers multi-relations/unresolved targets, property identity and family isolation. `authoritative_refresh::installed_relations_replace_only_their_family_and_repair_both_noop_paths` exercises actual commits, names, denied targets, stale removal and repair. Target-only changes and explicit rebuild are covered by reconciliation convergence/rebuild cases in that suite. |
| #62 internal links/mentions, deterministic deduplication, diagnosable broken targets, external URL exclusion and removed-link updates | `notion/src/link_graph.rs` and authoritative refresh's combined relation/body-link projection. `tests/link_graph.rs` covers each parser/storage contract. `authoritative_refresh::owned_body_links_follow_authorized_snapshot_cleanup_and_durable_repair` includes real links, mentions, block links, denied/broken targets, external/lookalike URLs, code/escaped exclusions, removal, tombstones and repair; reconciliation proves missing rows cannot be acknowledged. |
| #63 stable ID input, default depth 1/bounded depth, type/provenance, root enforcement, compact deduplicated output | `mcp/src/related.rs`, `mcp/tests/related.rs` defaults/bounds/directions/denial/output cases; `server/src/graph_runtime.rs` installs committed graph under the common scope composition. Server webhook-worker tests exercise actual stdio/HTTP discovery/calls, source edits/tombstones, explicit exclusions, unfinished commits and unsuitable transports. |
| #64 explicit title/property sources, canonical IDs, multiple ambiguous candidates, stale removal and Swedish/English names | `core/src/aliases.rs`; runtime startup `WorkerSettings.alias_sources`; `IndexRuntime::lookup_alias` remains a trusted local identity lookup and grants no content authority. `tests/aliases.rs`, guarded alias I/O cases and server reconciliation-command installed-alias tests cover config/ambiguity/Unicode, exact replacement, restart, failures and owned rebuild. The joined smoke checks normalized bilingual alternate names, ambiguity and edits/removal against the same graph/vector/FTS commits. |
| #65 configurable/off by default, bounded hops/weight, inspectable baseline scores, root-bounded traversal and paired off/on evaluation | `core/src/graph_boost.rs` and its focused scope/candidate-only intermediate/seed/error tests; startup `Config.graph_boost` plus `retrieval_runtime` install the existing decorator over real hybrid retrieval. New joined stdio/HTTP cases show reordered real candidates with unchanged scores/path provenance and fresh root denial. The trusted installed evaluation adapter runs the ordinary model-backed server against an unchanged existing index with full config echo, source verification and paired revision/candidate checks. |

Implementation paths in the table are relative to `crates/`; unqualified test
paths belong to the crate named in that row.
For the integrated endpoint-to-producer smoke, run:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --locked
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --locked
nix develop .#spike --command cargo test --workspace --locked
python3 scripts/test-graph-boost-eval.py
```

The full server suite includes `reconciliation_command`, proving explicit
relation/alias rebuilds through the same production runtime and lease authority.
The Lance suite includes real SQLite/vector/FTS authoritative commits and durable
repair/failure/restart tests. These complement, rather than replace, the final
`webhook_worker::joined_graph` stdio/HTTP tests.

Deployment and reproducible smoke/evaluation commands are in
[installed graph boost](installed-graph-boost.md). Actual multilingual production
model quality is a separate measured gate: behavioral fixtures do not certify
uplift. Boost stays off by default, and a harness decision never enables it.
Epic completion means the optional path and its evaluation are installed and
verified; it does not assert a positive quality result that has not been measured.
