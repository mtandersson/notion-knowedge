# Scoped Notion relation-property graph edges (#61)

This adds a **provider-independent, repeatable graph edge projection** for
normalized Notion `PropertyValue::PageIds` relation properties, plus the
`DocumentSnapshot::persist_relation_edges` index-commit hook.

Only a completed authorized root discovery supplies candidate page identities;
relation links **never expand crawler scope**. Targets present in that
discovery become canonical `GraphTarget::Page` references. All other targets,
including deleted, inaccessible and excluded pages, become
`GraphTarget::Unresolved` with the original normalized reference only.
No referenced target content is fetched or indexed.

Each edge uses `relation:<stable_property_id>` as its relation type and
`property:<stable_property_id>` as provenance. Stable Notion property IDs,
not mutable display names, preserve which property produced an edge. Duplicate
target IDs within one relation are collapsed deterministically; distinct
properties linking to the same page produce separate edges. Nonrelation
properties, including people IDs, are ignored.

The new `GraphEdgeStore::replace_page_relation_edges` API updates **only**
the relation-edge family for a source page, in an immediate SQLite transaction.
Existing Markdown/link edges are preserved for #62. Empty replacements remove
stale relations after property removal/tombstone, without affecting links.
Invalid and cross-family batches fail before the transaction deletes prior
edges. Writes are idempotent under repeated identical snapshots.

The snapshot hook validates that each document is within the completed
discovery set, has the expected root and consistent workspace, and first
prepares all edges before touching the store. It does not activate automatic
crawl synchronization, external Notion writes or new MCP endpoints. The
orchestration/index service remains responsible for only invoking this hook
after a fresh *authorized* complete discovery and while holding the
appropriate reconciliation/commit fence, including an explicit empty
replacement on page deletion. A multi-page hook run is not a single SQLite
transaction; source-level atomicity is guaranteed by the graph store.

Tests verify multi-relations, stable property provenance, scoped unresolved
targets, idempotent upserts, stale deletion, preservation of unrelated link
edges and fail-closed malformed input. The graph continues to be a derived
local metadata index with no authority over page reads.
