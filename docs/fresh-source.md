# Optional authoritative source verification

`knowledge_get` accepts optional `freshness: "indexed" | "fresh"`. Omission
preserves indexed expansion. A caller who sees an old search result's
`last_edited_time`, or needs authoritative content now, requests `fresh` using
the stable page or chunk reference already returned by retrieval:

```json
{
  "refs": [{"kind": "chunk", "id": "stable-indexed-chunk"}],
  "max_chars": 4096,
  "freshness": "fresh"
}
```

The handler first resolves every reference through the configured index and
validates the server-owned allowed roots, page identity, chunk lineage and
output budget. Arbitrary unindexed IDs and unauthorized references cannot use
fresh mode to bypass this gate. Then the configured read-only `NotionRead`
backend reads authoritative Markdown by the resolved stable **page ID**.
The existing Notion client implements this capability. Compose it with
`KnowledgeServer::and_fresh_source`; the default bootstrap has no authoritative
backend configured and reports that explicitly.

Fresh mode returns **whole-page content**, including when a chunk reference was
the indexed anchor. `content_scope: "page"` makes this explicit; indexed mode
uses `content_scope: "indexed"`. Original reference and indexed chunk IDs remain
as anchor lineage, while fresh heading paths are empty and fresh block IDs are
omitted because the old section may have moved or disappeared. Fresh title and
citation URL come from authoritative metadata. Text remains untrusted data,
never instructions. The existing total `max_chars` budget applies in request
order with Unicode character counting and an explicit `truncated` flag.

Every provenance object contains `indexed_last_edited_time`, from the persisted
source metadata. A successful fresh read additionally contains
`refreshed_last_edited_time` and `index_stale`; the latter compares the indexed
and authoritative edit timestamps. Unchanged pages still carry both timestamps
and `index_stale: false`. Indexed responses omit these refresh-only fields.
`index_stale` concerns the page edit timestamp; it is not a claim that every
indexed chunk is semantically different.

All references resolving to the same page share one content read and metadata
verification within a call. After the content read, the service fetches metadata
again and rejects changes rather than pairing content with an inconsistent
freshness claim. This detects edits/archive changes during the read; it cannot
promise that the page will never change after the final verification, nor does
it provide an atomic snapshot across different pages.

Missing/out-of-scope indexed references keep the same public inaccessible
error. Missing or permission-denied authoritative pages and archived pages
also fail safely. An unconfigured or failing Notion backend returns
`notion_unavailable`; concurrent metadata changes return `notion_conflict`.
Failures return neither partial fresh content nor a silent indexed fallback.
No upstream content, credentials or raw errors appear in failure messages.

Authorization here uses the configured indexed root provenance. `NotionRead`
metadata does not expose authoritative ancestry, so a relocation since indexing
cannot be detected by this mode. Root membership must be refreshed through the
scoped discovery/index lifecycle; fresh mode does not establish new root access.

Fresh reads have no index or Notion write capability. They do not persist new
content, timestamps, fingerprints or embeddings, and do not schedule an implicit
reindex. Subsequent indexed calls continue to reflect the indexed snapshot.

The [versioned stale-index evaluation matrix](../eval/retrieval/README.md#stale-index-and-authoritative-read-evaluation-101) exercises
explicit and default indexed reads, newer and unchanged authoritative content,
unavailable/archived/concurrently edited sources, and subsequent indexed
read-back. It asserts per-scenario timestamp provenance and that an
authoritative read never silently mutates the derived index. These
synthetic scenarios test behavior, not production reconciliation latency.

Verification uses credential-free MCP protocol fixtures for fresh/stale and
unchanged pages, indexed default compatibility, stable-ID mapping, Unicode total
bounds, repeated-page reads, concurrent edits, invalid metadata, unavailable
backends and root rejection. Shared HTTP/stdio catalog checks assert the same
freshness input and output contract.
