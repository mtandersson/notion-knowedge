# Local storage pipeline acceptance smoke

Epic #5 delivers the reusable local persistence and retrieval adapters from
#36–#43. Notion remains authoritative; SQLite coordination state, chunks,
vectors and physical indexes are derived local state.

## Reproducible automated checks

Run from the repository root with the pinned matched Rust/LLVM shell:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-core -p notion-knowledge-retrieval --features notion-knowledge-retrieval/local-lancedb,notion-knowledge-retrieval/local-qwen --locked
```

The suite needs no credentials, Notion requests, model download or private
corpus. SQLite and LanceDB tests use temporary filesystem stores. LanceDB
integration tests invoke the production adapter with a deterministic counting
embedding provider; provider identity validation and native vector/FTS indexes
remain real. The Qwen tests cover its production port boundary with model-free
fixtures. Real neural inference is the separate opt-in smoke below.

| Contract | Executable evidence |
| --- | --- |
| SQLite migrations, page/checkpoint/index state and durable webhook deduplication survive reopening | `tests/sync_state.rs::state_survives_reopen_with_versioned_schema` |
| Corrupt or newer SQLite schema is rejected; deleting derived state recreates an empty store | `altered_migration_history_is_rejected_as_corrupt_state`, `newer_unknown_schema_is_rejected_instead_of_reinterpreted`, `deleting_the_database_rebuilds_an_empty_store_without_notion` |
| Canonical chunk fields and embedding identity persist in the typed Lance schema | `chunks::tests::create_and_open_from_scratch_preserves_explicit_schema` |
| Page snapshots embed only new/changed chunks, preserve vectors while refreshing metadata, delete removed chunks and remain idempotent | `page_diff_embeds_only_changed_and_new_chunks_and_is_idempotent` |
| Failed embeddings preserve the previous generation; an empty snapshot only removes the selected page | `embedding_failure_leaves_the_existing_page_generation_unchanged`, `empty_page_snapshot_removes_only_that_page_without_embedding` |
| Versioned IVF index retrieves stable provenance and optimization incorporates incremental rows | `vector_index_builds_from_initial_crawl_and_returns_stable_nearest_neighbors`, `optimize_vector_index_folds_incremental_rows_into_existing_index` |
| Production embedding → page diff → native FTS maintenance → query sees the refreshed term and excludes the removed term | `optimizing_fts_after_page_diff_makes_updated_terms_searchable` |
| Swedish/English body text, titles and exact identifiers use the versioned FTS indexes | `fts_index_retrieves_swedish_english_titles_and_exact_identifiers` |
| Same-dimensional model revision changes fail without deleting data; explicit rebuild creates a compatible empty generation that reopens successfully | `startup_policy_fails_closed_or_explicitly_rebuilds_embedding_mismatch` |
| Invalid rebuild configuration and storage errors cannot trigger destructive compatibility recovery | `rebuild_policy_validates_target_schema_before_dropping_existing_table`, `explicit_rebuild_handles_schema_version_mismatch_but_not_storage_errors` |

The chunk tests live in `crates/retrieval/src/chunks.rs`; SQLite integration
tests live in `crates/retrieval/tests/sync_state.rs`. Core embedding tests
validate cardinality, dimensions, finite vectors and vector-space identity
before adapters may persist a batch. See the component documents linked below
for error and maintenance semantics.

## Opt-in real Qwen smoke

For pinned public assets and the actual production provider, use the documented
[Qwen smoke](local-qwen.md#checks-and-real-model-smoke). It checks ordered finite
1024-dimensional vectors, duplicate stability, bilingual similarity and
persisted identity compatibility. Its existing measured run is recorded there.
It does not contact Notion or constitute a general retrieval-quality benchmark.
No real-model inference is part of the credential-free automated command above.

## Composition and recovery boundaries

SQLite and LanceDB own separate transactions. These checks do not claim an
atomic transaction across both stores, or a finished production crawl/sync
scheduler. The default server still reports retrieval unavailable until
adapters are explicitly configured; deployment composition belongs to #104
and #145. The adapter acceptance scope does include persistence, embedding
contracts, incremental page writes, index maintenance and compatibility gates.

On model/schema incompatibility, the safe startup policy preserves the existing
generation. Explicit rebuild drops only the named derived chunk table and
returns an empty compatible table; authoritative repopulation remains the
caller’s responsibility. It does not alter SQLite state or Notion.

Further contracts: [sync state](sync-state.md), [embedding port](embeddings.md),
[chunk table](lancedb-chunks.md), [incremental indexing](incremental-indexing.md),
[vector index](vector-index.md), [FTS index](fts-index.md) and
[compatibility/rebuild](index-compatibility.md).
