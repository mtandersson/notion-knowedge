# Internal Notion page links and mentions as scoped graph edges (#62)

The local graph now supports links extracted from authoritative page body
Markdown, enhanced `<page ...>` tags, and `<mention-page ...>` tags.
The parser is the existing lossless `links::extract_relationships` adapter;
fenced/inline code, escaped tags and comments are never interpreted as links.

## Resolution and provenance

- A resolved target becomes `GraphTarget::Page` **only** when its canonical
  Notion page ID appears in the completed, trusted, exclusion-aware discovery
  for snapshot helpers, or current physical target evidence authorizes it in
  the installed refresh described below.
- Out-of-scope, inaccessible and removed page targets remain
  `GraphTarget::Unresolved` references, never authoritative graph nodes.
  Reading an edge must not grant read permission for its target.
- External URLs, deceptive Notion lookalike hosts, relative URLs, unsupported
  mention kinds and Notion person IDs are **not** graph page nodes.
- Unparseable page tags and malformed URLs pointing at actual Notion hosts are
  retained as `link:invalid` unresolved edges, carrying a SHA-256 diagnostic
  fingerprint rather than copying raw untrusted URLs/queries into SQLite.
- Normal internal page links and page mentions share `link:page`; block
  anchors use `link:block` and retain block-level distinctions for dedup.
  Duplicate equivalent targets are collapsed deterministically in first
  encounter order. Each persisted edge retains a `markdown:<byte offset>`
  provenance pointing into the source snapshot.

## Reconciliation

`GraphEdgeStore::replace_page_link_edges` uses an IMMEDIATE SQLite transaction
and replaces only the `link:` family for the source page. The relation
family from #61 is never removed. Replacing with an empty collection removes
stale links after editing a page or deleting it.

`DocumentSnapshot::persist_link_edges` composes the parsed body references
with the full authorized discovery inventory. It rejects missing/duplicate
source IDs, unexpected roots and inconsistent workspaces, and validates the
complete snapshot before updating any page. Each page replacement is atomic;
the calling index/reconciliation worker must hold its own commit fence for
cross-page consistency, ensure the snapshot is fresh, and invoke empty
replacement on tombstones. **This does not enable a new MCP tool, automatic
crawler schedule or any Notion content write.**

The new integration tests cover actual Markdown link/tag parsing; dedup and
block provenance; excluded targets; malformed/notion-lookalike/external URLs;
escaped and fenced content; idempotent replacement; relation-edge preservation;
stale link removal and validation-before-delete.

## Installed authoritative refresh (#375)

The shared production refresh/reconciliation path now prepares body links from
its fresh, lossless `ScopedPageSnapshot`. The same trusted physical lifecycle
policy used for relation targets authorizes each candidate page; target
metadata/ancestry is revalidated inside the owned commit. This never crawls a
link or reads/indexes its target body. Denied targets remain unresolved, and
policy changes or target moves invalidate the prepared proposal before effects.

`RelationProjection::with_links` binds both independent graph families to the
existing durable operation proposal. Its `graph-v2` effect includes the exact
link targets and byte provenance, including a valid empty link family. The final
fenced SQLite transaction replaces links and relations from their respective
fresh proposals, maintains configured aliases, writes completeness/checkpoint,
and acknowledges the operation together. Link replacement does not delete
relation or alias data. Empty bodies, removed links, tombstones and scope removal
clean stale links. No graph writes occur after a successful receipt.

Existing relation-only receipts cannot attest link completeness. A normal refresh
or the existing explicit graph rebuild repairs them using fresh source authority
and a new durable repair operation; immutable Applied receipts are retained.
Missing or modified link rows invalidate completeness, including on no-op and
replay paths. A failed graph write or final acknowledgment rolls back the whole
SQLite transaction; retry/restart reuses pending repair work and revalidates the
current source before acknowledging it. Operator graph rebuild invalidates the
shared completeness marker and rebuilds both graph families; it is not needed
for each edit. The read-only metadata planner checks combined stored completeness
without reading bodies; actual unchanged reconciliation rechecks the full fresh
body and target authority before its success acknowledgment.

The owning-boundary integration fixtures still execute every typed HTTP request
and real SQLite/Lance mutation. Their explicitly compiled loopback-only Notion
client omits provider admission delays and retries: sleeping 334 ms per local
fixture request otherwise exhausts hosted CI time after compilation. Normal
integration clients retain the production admission interval, and dedicated
clock-driven transport tests verify pacing, cancellation and upstream deferral.
Authority-race and lease tests continue to use explicit barriers and injected
clocks, rather than relying on provider sleep time.
