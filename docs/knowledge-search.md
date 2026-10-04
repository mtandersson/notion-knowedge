# Semantic search MCP contract

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

The default bootstrap has no retrieval adapter: valid calls return `isError:
true` with `retrieval_unavailable` and no fabricated result object. The explicit
[semantic MCP spike](semantic-mcp-spike.md) injects a local Qwen/LanceDB adapter
into the same handler and HTTP wiring. It supports semantic mode only;
lexical/hybrid return `mode_unavailable`. Dependency failures return
`retrieval_unavailable`. Invalid arguments always return JSON-RPC invalid-params
without echoing supplied input. Successful content blocks also identify retrieved
excerpts as untrusted data. Production implementation remains in #45/#49.

Production-boundary discovery and call tests run with:

```sh
cargo test -p notion-knowledge-server --test stdio --test http --locked
```
