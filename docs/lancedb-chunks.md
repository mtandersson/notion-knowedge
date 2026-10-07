# LanceDB canonical chunk table

Issue #37 promotes the persisted-table part of the bounded Qwen/LanceDB spike
into the production retrieval crate. The authoritative domain record remains
`notion_knowledge_core::indexed::IndexedChunk`; LanceDB is a derived,
rebuildable representation behind the `local-lancedb` feature.

## Schema

The table has a versioned Arrow schema and persists the full embedding identity
in schema metadata. The vector column is
`FixedSizeList<Float32, embedding.dimension>`, so opening a table with a
different provider/model/version/dimension fails before reads or writes.

Every canonical chunk field has a persisted representation:

- scalar citation/filter fields are first-class columns:
  `chunk_id`, `page_id`, optional `block_id`, `url`, `title`,
  `last_edited_time`, `workspace_id`, `root_page_id`, optional
  `database_id` and optional `data_source_id`;
- `text` is a first-class column for vector and later FTS retrieval;
- `schema_version` and `content_hash` are first-class integrity fields;
- heading paths, normalized properties and links are deterministic JSON columns;
- `vector` stores the embedding and is never nullable.

MCP search filters use page/root and source-provenance columns directly.
Typed property/date filtering scans only scoped metadata, then constrains vector
and FTS retrieval before limits; see [the filter contract](knowledge-search.md). The canonical contract is not duplicated as an opaque JSON record.

Schema metadata contains the chunk-table schema version, canonical chunk schema
version and complete `EmbeddingMetadata`. Schema changes that alter field
meaning, required columns or vector identity require an explicit rebuild or
migration instead of silent coercion.

## Writes

`LanceChunkTable::upsert` first requires the batch embedding identity to match\nthe identity persisted with the table, then uses `chunk_id` as the merge key.\nThis prevents same-dimensional vectors from another model/revision from being\nmixed into an index. Existing stable IDs are replaced and new IDs are inserted.\nDuplicate IDs in one source batch are rejected before LanceDB because multiple\nsource matches do not have a well-defined winner. Vectors must exactly match\nthe persisted dimension and contain only finite values.

The adapter accepts only local filesystem database paths. LanceDB's `remote`
Cargo feature is compiled solely because 0.39.0 references its HTTP error
variant in embedded code; it does not widen this storage boundary. Storage
implementation errors are mapped to adapter errors instead of leaking LanceDB
types into the application layer.

## Verification

The optional adapter uses the same matched Rust/LLVM shell as the verified
spike and the local Qwen feature:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --locked
nix develop .#spike --command cargo clippy -p notion-knowledge-retrieval --all-targets --features local-lancedb --locked -- -D warnings
```

Focused tests create and reopen an empty table from scratch, verify the explicit
vector schema and persisted model identity, exercise stable-ID replacement and
SQL filtering over page/root columns, and reject ambiguous or invalid vector
batches. No model assets or credentials are required.
