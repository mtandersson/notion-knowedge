# LanceDB vector index

Issue #40 adds the production approximate-nearest-neighbor index lifecycle for canonical chunk embeddings.

## Index choice

The first production vector index is a named `IVF_FLAT` index over the canonical `vector` column:

`chunks_vector_ivf_flat_v1`

IVF partitions the corpus into clusters and searches the nearest partitions instead of scanning every vector. IVF_FLAT keeps the original float vectors inside the index rather than quantizing them. This makes it a useful quality baseline before evaluating PQ/SQ/HNSW trade-offs.

The default distance is L2. This matches the repository's existing vector-search/spike behavior and avoids silently changing semantic ranking while #45 is still open. Cosine can be selected explicitly and requires an explicit rebuild of an existing L2 index.

## Build configuration

`VectorIndexConfig` controls:

- `distance`: `L2` or `Cosine`;
- `num_partitions`: optional explicit IVF partition count; when absent LanceDB chooses its default from corpus size;
- `sample_rate`: vectors sampled per IVF partition while training, default 256;
- `max_iterations`: k-means training iteration ceiling, default 50.

All numeric values must be positive.

`ensure_vector_index` is intended to run after the initial crawl has written embeddings. It refuses to train on an empty table. If the named index already exists, its type, vector column and persisted distance metric are validated before it is reused.

## Search

`vector_query(query_vector, limit, nprobes)` is the low-level #40 probe. It:

- requires the exact persisted embedding dimension;
- rejects non-finite vectors and invalid bounds;
- requires the named vector index to exist;
- reads the index's persisted distance metric and uses that same metric for the query;
- returns stable page/chunk provenance plus the native `_distance`.

Distances are lower-is-better backend distances, not the higher-is-better application score used by `knowledge_search`. Production semantic score conversion, query embedding and metadata filtering remain #45.

`nprobes` controls how many IVF partitions are searched. Higher values improve recall at increased query cost. The application should tune it against retrieval-quality benchmarks rather than treating the default as universal.

## Incremental maintenance

Rows added or changed after an index build can be reported by LanceDB as unindexed rows. `optimize_vector_index` first ensures that the vector index exists, then optimizes only the named vector index so those delta rows are folded into indexed state.

This complements #42 page-diff writes. The write path does not synchronously retrain the ANN index on every row mutation; the caller chooses the maintenance boundary.

## Rebuild and recovery

`rebuild_vector_index` validates the requested configuration and verifies that the table is non-empty before asking LanceDB to replace the named derived index. Invalid configuration therefore cannot destroy the existing index.

Rebuild only replaces local derived vector-index state. It does not replace the chunk table, embeddings, sync state or authoritative Notion content.

A distance change such as L2 → Cosine must use explicit rebuild. `ensure_vector_index` fails closed if the existing index metric does not match the requested configuration.

## Versioning

The versioned index name owns the physical index family contract. A future incompatible change in index family or assumptions should introduce a new versioned name or an explicit migration/rebuild path instead of silently reinterpreting an existing index.
