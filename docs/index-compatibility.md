# Index compatibility and rebuild policy

The local LanceDB chunk table is derived, disposable retrieval state. Notion remains authoritative. Production startup must never silently reuse an index whose persisted schema or embedding vector-space identity differs from the runtime contract.

## Persisted identity

Every chunk table persists three pieces of compatibility metadata in the Arrow/Lance schema:

- `notion_knowledge.chunk_table.schema_version` — storage/table contract version.
- `notion_knowledge.chunk_table.canonical_schema_version` — canonical `IndexedChunk` schema version written into rows.
- `notion_knowledge.chunk_table.embedding` — serialized `EmbeddingMetadata`, including provider ID, model ID, model version/revision, canonical metadata schema version and vector dimension.

The vector column itself is also validated as `FixedSizeList<Float32, dimension>`. A same-dimension model change is still incompatible because model/provider/version identity is part of the vector space.

## Startup gate

Use `LanceChunkTable::open_with_policy` when composing an existing local index into a running service.

```rust
let (table, action) = LanceChunkTable::open_with_policy(
    index_path,
    "chunks",
    provider.metadata().clone(),
    IndexCompatibilityPolicy::Fail,
).await?;
```

`IndexCompatibilityPolicy::Fail` is the safe default. It returns the compatibility error and leaves the existing table untouched.

An operator may explicitly choose `IndexCompatibilityPolicy::Rebuild`. Rebuild is allowed only after the table has been opened successfully and validation has identified an incompatible persisted contract. Storage, path, permission and other I/O failures never trigger destructive recovery.

## Rebuild behavior

An explicit rebuild:

1. validates that the failure is a schema/vector-space incompatibility;
2. drops only the named derived LanceDB table;
3. creates a new empty table using the requested schema and embedding identity;
4. revalidates the new table before returning `IndexStartupAction::Rebuilt`.

The new table is intentionally empty. The caller must run the normal full indexing/refresh path to repopulate it from authoritative Notion content. Rebuild does not modify Notion, sync-state databases, model assets or unrelated LanceDB tables.

If the process exits after the old table is dropped but before the new generation is populated, the correct recovery is another explicit rebuild/full refresh from Notion. The index is disposable and must not be treated as a backup.

## Migration policy

There is no in-place migration in schema version 1. A future change that can be migrated safely must add an explicit migration implementation and tests before startup may use it. Until then, any changed table schema, canonical chunk schema, embedding provider/model/version or dimension requires either:

- fail startup and preserve the existing generation; or
- explicit rebuild followed by a full refresh.

Never coerce metadata, resize vectors, or mix rows from different vector spaces.

## Spike reconciliation

Spike #215 demonstrated the same fail/rebuild rule on the disposable end-to-end corpus: incompatible persisted vector identity fails before source reads/model reuse, and deleting/rebuilding derived index state from unchanged Notion pages reproduces the expected retrieval result. Production #43 moves that policy into the reusable LanceDB adapter rather than keeping it in spike-only code.

The [incremental reconciliation operation](incremental-indexing.md) implements page diffing. This compatibility gate deliberately does not decide which chunks need re-embedding; it only decides whether the existing index generation is safe to use at all.
