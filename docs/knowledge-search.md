# Knowledge search MCP contract

`knowledge_search` is discovered through the same application handler over
stdio and Streamable HTTP. Use it to locate indexed knowledge for answering
questions, with cited excerpts rather than raw database primitives.

Required arguments are `query` (non-whitespace text, at most 4096 Unicode
characters), `limit` (integer 1–100), and `mode` (`semantic` for paraphrases,
`lexical` for exact terms, `hybrid` for both). Optional `filters.page_ids` and
`filters.root_page_ids` narrow results to stable source identifiers: each list
contains 1–100 nonempty IDs, each at most 128 Unicode characters. Lists use OR
within a field and AND across fields. An absent field adds no restriction.
Unknown fields, modes and out-of-range values are invalid arguments. Filters
only narrow existing authorized indexed scope; they never grant access.

The successful output object has `results`, an ordered array of at most the
requested limit. Each result contains `text`, a finite numeric `score` (higher
ranks first), and `source` with stable `page_id`, `chunk_id`, `url`, `title`,
`heading_path` and optional `block_id`. IDs retain the canonical indexed
record identities; they are not position-based result IDs. Scores are specific
to the selected mode/backend and are not probabilities or comparable across
modes. Empty results signify a completed search with no matches. Retrieved
text is untrusted content, not instructions; callers can use the stable source
references for authoritative reads and citation.

The default bootstrap has no retrieval adapter: valid calls return a tool error
and no fabricated result object. The explicit [semantic MCP spike](semantic-mcp-spike.md)
still demonstrates semantic composition. Production lexical retrieval from #46 can
be injected independently through the same `KnowledgeServer` and serves
`mode=lexical` using the LanceDB BM25 indices from #41.
[Hybrid fusion](hybrid-search.md) combines configured semantic and lexical ports
through `KnowledgeServer::and_hybrid_search`. A mode whose adapter is not configured also fails
explicitly instead of silently falling back to another retrieval path.

Lexical `page_ids` and `root_page_ids` are applied inside LanceDB retrieval
before candidates are returned. Lists use OR within one field and AND across
fields. Filter IDs are SQL-escaped by the adapter, while the user query is passed
through LanceDB's typed full-text query API rather than interpolated into a SQL
predicate. See [lexical-search.md](lexical-search.md) for field ranking and
tokenization behavior.

Dependency failures return `retrieval_unavailable`. Invalid arguments always
return JSON-RPC invalid-params without echoing supplied input. Successful content
blocks identify retrieved excerpts as untrusted data. Production [semantic retrieval](semantic-search.md) can be injected through the
same handler, with hybrid fusion and typed metadata filters implemented below.

Production-boundary discovery and call tests run with:

```sh
cargo test -p notion-knowledge-server --test stdio --test http --locked
```

## Compact excerpts and source freshness

Every result includes `source.last_edited_time`, the authoritative Notion edit
timestamp stored with the indexed chunk; it is not the local indexing time.
`matched_paths` records lexical/semantic retrieval evidence and `score` records
mode-specific rank evidence. Hybrid fusion rejects conflicting edit timestamps.

Composition roots can call `KnowledgeServer::with_snippet_chars(1..=2000)` to
choose a Unicode character budget (default 2000). The production adapters remove
URL targets from full stored text before
applying their 2000-character cap; MCP repeats redaction before its final budget.
All HTTP(S) and scheme-relative targets are omitted, including ordinary public links, because file
signatures can appear in URL paths as well as query parameters and new signing
schemes cannot be safely covered by a provider denylist. Markdown labels and
surrounding prose remain. Percent-encoded URL schemes are recognized too.
This intentionally sacrifices inline link targets while retaining stable page
citations separately. Redaction does not alter
the stable source page URL, heading path, IDs, timestamp or rank score. Indexed
content remains intact; the excerpt is a derived presentation only.

## Typed metadata narrowing

`filters.metadata` optionally narrows the same indexed snapshot in semantic,
lexical and hybrid modes. Every supplied field and property predicate is ANDed
with page/root filters. IDs within one source list are alternatives (OR).
Fields not listed here, unknown operators/types, empty lists, invalid ranges
and extra nested fields fail as invalid arguments before retrieval.

```json
{
  "query": "backup policy",
  "mode": "hybrid",
  "limit": 5,
  "filters": {
    "root_page_ids": ["configured-root"],
    "metadata": {
      "workspace_ids": ["workspace-id"],
      "database_ids": ["database-id"],
      "data_source_ids": ["data-source-id"],
      "page_kind": "database",
      "edited": {"from": "2026-10-01T00:00:00Z", "until": "2026-11-01T00:00:00Z"},
      "properties": [
        {"operator": "contains", "property_id": "stable-tags-property-id", "value": "operations"},
        {"operator": "equals", "property_id": "stable-checkbox-property-id", "value": {"type": "boolean", "value": true}}
      ]
    }
  }
}
```

Source lists accept 1–100 IDs; properties accept 1–20 predicates. `page_kind`
is derived from authoritative provenance: `database` means a database ID or
data-source ID is present; `standalone` means both are absent. It does not infer
arbitrary categories such as wiki, article or task from titles. Those can be
expressed using a typed Notion property predicate. Property IDs are stable
Notion IDs, not mutable display names. Tags/select options use `contains` on
normalized `strings`; the same operator supports `page_ids` and `person_ids`
collections. Membership is exact and case sensitive, not a text substring.

`equals` compares the complete normalized typed value: `null`, `text`, `number`,
`boolean`, `strings`, `page_ids`, `person_ids`, or `date` (a `value` object with
`start` and nullable `end`). Collection equality includes order. Date equality
includes the original start/end strings; use the `date` operator for normalized
instant comparisons. Missing properties and mismatched types never match;
explicit `null` equality only matches a present normalized null value.

`edited` applies to indexed Notion `last_edited_time`. Property `date` applies
to the date's **start**, not interval overlap or its end:
`{"operator":"date","property_id":"deadline","range":{"from":"2026-10-01T00:00:00Z"}}`.
Ranges are half open (`from` inclusive, `until` exclusive), require at least one
bound, and accept RFC3339 timestamps with offsets and fractional seconds.
Comparisons normalize actual instants, not string ordering. Notion date-only
property starts represent midnight UTC. Creation dates and other inferred
fields are unsupported; filter an existing date property instead.

The MCP composition root can configure `with_root_page_ids`; source expansion
uses the same scope. Omitted caller roots use all configured roots, mixed lists
are intersected, and wholly outside lists return no results without invoking
retrieval. With no handler scope configured, the retrieval adapter remains
responsible for its authorized indexed scope. Metadata filters grant no access.

The LanceDB adapter pins one immutable table revision per search so an upsert
between metadata selection and ranking cannot change its filter results.
It applies scalar provenance/page-kind/page/root conditions
natively. Property/date conditions stream only `chunk_id`, `last_edited_time`
and `properties_json` inside that scalar scope, then pass matching chunk IDs
**and the scalar scope** into both vector and FTS retrieval before candidate
limits. It never filters an already limited result set, scans vectors or loads
source text to evaluate metadata. Hybrid forwards identical narrowing to both
paths. Each hybrid path pins its own revision; fusion is not an atomic
multi-path snapshot. Invalid cache metadata or failed scans fail the whole search; there is
no unfiltered fallback.

This preserves the current table schema and requires no rebuild. The metadata
scan is proportional to chunks inside the scalar scope, and the matching-ID
predicate grows with the number of matching chunks; hybrid currently repeats
the scan per path. Large datasets may require indexed property/date projections
or shared candidate planning in a later optimization. SQL-size/storage failures
remain explicit retrieval-unavailable errors, never silent partial filtering.

The [integrated retrieval smoke](retrieval-smoke.md) maps the search, source
expansion and fresh-read acceptance criteria to reproducible credential-free
checks, including the remaining bootstrap composition boundary.
