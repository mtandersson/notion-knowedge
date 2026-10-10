# Local knowledge graph edges

Issue [#60](https://github.com/mtandersson/notion-knowedge/issues/60) introduces the
derived graph foundation for follow-up extraction (#61–#62) and graph reads
(#63–#65). This is **not** an exposed MCP tool and does not authorize Notion
page reads or writes.

## Contract

`notion-knowledge-core::graph` defines `GraphEdge`, `GraphTarget` and the
`GraphEdgeStore` port. One directed edge records:

- `source_page_id`: stable Notion page ID (never title or chunk ID);
- `target`: a resolved stable `Page { page_id }` or an
  `Unresolved { reference }` that must **not** be treated as a page identity;
- `relation_type`: source relationship kind (e.g. `relation`, `link`);
- `provenance`: source property/block reference (e.g. `property:<id>`).

Keep provenance concise, stable, and free of page body text, tokens and URLs
with query strings. Producing adapters are responsible for validating page
scope and resolving only authoritative target IDs. An unresolved reference is
not a license to fetch a linked page or expand an allowed root.

## SQLite and versioning

SQLite migration `0007_graph_edges.sql` extends the existing operational-state
database and its integrity-checked `schema_migrations` sequence (schema v7).
The independent `graph_edges` table has a CHECK constraint requiring exactly
one of `target_page_id` or `unresolved_reference`. Partial unique indexes
deduplicate both kinds of targets, and source/target indexes support both
directions of lookup. These relationships are metadata, not embeddings,
full-text or a second copy of page content.

`SqliteSyncStateStore` implements `GraphEdgeStore`:

- `replace_page_edges` validates every edge's source first, then deletes and
  inserts the page's complete edge snapshot in one IMMEDIATE transaction;
  an empty snapshot removes stale relationships;
- `edges_from` returns resolved and unresolved outgoing edges;
- `edges_to` returns only resolved incoming page edges, never unresolved
  references guessed into an identity.

All output ordering is deterministic. Other pages' edges remain intact.
The graph can be rebuilt from authoritative Notion metadata after intentional
state loss. Subsequent ingestion work must wire extraction through the same
scope, retry and durable-state boundaries; this ticket does not imply that
those production workflows are already connected.

## Verification

```sh
cargo test -p notion-knowledge-retrieval --test graph
cargo test -p notion-knowledge-retrieval --test sync_state
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

The graph tests cover persistence across reopen, bidirectional lookups,
unresolved targets, duplicate elimination, isolated atomic replacement,
rejected invalid inputs, migration identity and SQLite target/index
constraints.
