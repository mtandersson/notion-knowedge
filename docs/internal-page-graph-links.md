# Internal Notion page links and mentions as scoped graph edges (#62)

The local graph now supports links extracted from authoritative page body
Markdown, enhanced `<page ...>` tags, and `<mention-page ...>` tags.
The parser is the existing lossless `links::extract_relationships` adapter;
fenced/inline code, escaped tags and comments are never interpreted as links.

## Resolution and provenance

- A resolved target becomes `GraphTarget::Page` **only** when its canonical
  Notion page ID appears in the completed, trusted, exclusion-aware discovery.
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
