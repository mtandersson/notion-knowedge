# Production semantic vector search

`LanceSemanticSearch` implements the core `SemanticSearch` port behind the
retrieval crate's `local-lancedb` feature. Construct it with an `Arc<LanceChunkTable>`,
an `Arc<dyn EmbeddingProvider>` and a positive IVF probe count, then inject it
with `KnowledgeServer::with_search`. The embedding provider can be the local
Qwen adapter; construction rejects any provider/model/revision/dimension mismatch
with the identity persisted in the table. The default bootstrap remains unavailable
until its separate composition work configures assets and indexes.

Each valid request executes one embedding batch containing exactly the query.
Provider output is validated before an indexed LanceDB nearest-neighbor query.
The table's existing IVF_FLAT index and persisted distance metric are required;
a missing index or dependency failure fails explicitly rather than falling back.
The probe count controls approximate-search recall. This adapter does not claim
model-quality or perfect-recall guarantees from its deterministic fixture tests.

Page and root filters use OR within each list and AND across lists, SQL-escape
identifiers, and apply inside LanceDB before top-k selection. They narrow the
configured indexed scope. Invalid direct port requests fail before embedding.

Results contain bounded excerpts (2000 Unicode characters), canonical citation
metadata and finite higher-is-better scores. The score is negative native distance
for both supported index metrics (L2 and cosine), preserving their ordering.
Scores are backend-specific and are neither probabilities nor calibrated across
metrics or retrieval modes. Equal scores use page/chunk IDs to order the returned
candidate set. No raw vectors or backend distances enter `SearchHit` or MCP JSON.

The fixed bilingual fixture tests use a deterministic counting embedding provider
and real temporary LanceDB tables/indexes without credentials or model assets:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --locked
nix develop .#spike --command cargo clippy -p notion-knowledge-retrieval --all-targets --features local-lancedb --locked -- -D warnings
```

This promotes the production query/embedding/filter boundary demonstrated by the
bounded spike; the experimental spike remains separate. Hybrid fusion and broader
filters remain separate work.
