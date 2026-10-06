# Incremental chunk reconciliation

Issue #42 replaces full-corpus rebuilds for ordinary page updates with a page-scoped reconciliation operation in the production LanceDB adapter.

## Contract

`LanceChunkTable::apply_page_diff(provider, page_id, chunks)` accepts the complete current chunk snapshot for one page. Every incoming chunk must carry the same `page_id`, unique non-empty `chunk_id` values and non-empty content hashes.

The operation compares the incoming snapshot with the currently persisted rows for that page:

- same `chunk_id` and same `content_hash`: **skipped** for embedding;
- same `chunk_id` and changed `content_hash`: **changed** and re-embedded;
- new `chunk_id`: **added** and embedded;
- persisted `chunk_id` absent from the new snapshot: **removed**.

It returns `ChunkDiffMetrics { added, changed, skipped, removed }`.

## Vector reuse and metadata refresh

A skipped chunk reuses its existing persisted vector. It still participates in the final merge, so citation metadata such as title, edit timestamp, properties and links can advance without paying the embedding cost again.

Changed and added texts are sent to the configured `EmbeddingProvider` in one batch. The provider's persisted vector-space identity must match the table identity from #43.

All embedding work completes before the LanceDB mutation starts. If embedding fails, the existing page generation is left untouched.

## Mutation semantics

For a non-empty snapshot, the adapter sends the complete page rows to one LanceDB `merge_insert`:

1. matching chunk IDs are updated;
2. new chunk IDs are inserted;
3. target rows not present in the source snapshot are deleted only when their `page_id` matches the reconciled page.

An empty snapshot uses an explicit page-scoped delete. Other pages are not removed.

Repeating the same snapshot is idempotent: all current chunks are reported as skipped, no embedding request is made, and no duplicate chunks are created.

## Relationship to fingerprint reconciliation

Stable chunk identity remains owned by `core::fingerprint::identify_chunks`. That stage reuses previous IDs for unchanged fingerprints and, where unambiguous, for edits. Incremental storage therefore does not guess chunk correspondence from offsets or row order.

The stored `content_hash` is the embedding-reuse key. A model/schema identity change is not treated as a normal page diff; #43 requires fail/rebuild handling for that case.

## Recovery

Notion remains authoritative. If the local derived index is corrupt or incompatible, use the explicit rebuild path from `docs/index-compatibility.md` and repopulate from source. Incremental reconciliation is an optimization for compatible generations, not a backup mechanism.
