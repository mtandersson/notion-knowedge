# Source expansion and whole-page reads

`knowledge_get` accepts `freshness: "indexed" | "fresh"`. Indexed expansion is the default. An indexed page or chunk reference expands stored search text within the configured root. A fresh read accepts an explicit lowercase, hyphenated Notion page UUID or an indexed chunk reference and returns a whole-page view within the requested response budget.

A fresh request can use the complete canonical Markdown published by synchronization. The stored document must agree with the inventory placement and latest observed edit timestamp, with no newer observed source signal. A verified change webhook or admitted local mutation invalidates that page even when Notion reports the same edit timestamp. An observed ancestor move or removal invalidates dependent cached pages until an authoritative walk settles the placement. Sync publishes whole-page text independently of embedding success.

A missing, stale or unverifiable local copy causes a verified Notion read. For an explicit page UUID, Notion's integration permissions govern access to this live read. The configured roots govern the local corpus and resolution of chunk references. A live direct read outside that corpus leaves local provenance absent and does not index the page.

For example, to read a particular page:

```json
{
  "refs": [{"kind": "page", "id": "11111111-1111-1111-1111-111111111111"}],
  "max_chars": 4096,
  "freshness": "fresh"
}
```

Fresh reads return `content_scope: "page"` and `read_from: "cache" | "notion"`. Live reads include `refreshed_last_edited_time`. Indexed provenance is present when available, and `index_stale` compares that edit timestamp with the returned page timestamp. A cached document can supply its known root even when lexical/vector publication has failed. Cached Markdown uses canonical media references without Notion-issued expiring access credentials.

One logical source read per unique page verifies metadata around the Markdown fetch. At most one full body is buffered for a multi-page request. Disclosed text respects the existing total Unicode character and response-size budgets, with a `truncated` flag. If a page is inaccessible, its data changed during verification or its local evidence becomes stale during the request, the tool fails the complete request. Errors omit raw upstream bodies and credentials.

A sharing change that produces no event takes effect for cached reads when synchronization observes it. There is no separate freshness TTL. The read-only path neither updates Notion nor admits local indexing work.
