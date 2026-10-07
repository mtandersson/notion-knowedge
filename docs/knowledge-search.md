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
`mode=lexical` using the LanceDB BM25 indices from #41. Hybrid remains
`mode_unavailable` until #47. A mode whose adapter is not configured also fails
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
same handler; hybrid fusion remains #47 and broader typed filters remain #48.

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
recognized credential-bearing signed URL targets from full stored text before
applying their 2000-character cap; MCP repeats redaction before its final budget.
AWS/Google signing keys, Azure-style `sig`, signature and access-token query keys
are recognized case-insensitively, including percent-encoded keys/URLs. Ordinary
public links and surrounding Markdown labels remain. Malformed suspicious
credential-bearing tokens are conservatively omitted. Redaction does not alter
the stable source page URL, heading path, IDs, timestamp or rank score. Indexed
content remains intact; the excerpt is a derived presentation only.
