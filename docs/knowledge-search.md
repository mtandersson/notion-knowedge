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

This issue defines the tool contract only. No retrieval adapter, embeddings or
LanceDB queries are wired yet. Every valid call returns `isError: true` with
`retrieval_unavailable` and no fabricated result object. Invalid arguments
return JSON-RPC invalid-params without echoing supplied input. Later retrieval
work must implement the declared success schema and filter semantics.

Production-boundary discovery and call tests run with:

```sh
cargo test -p notion-knowledge-server --test stdio --test http --locked
```
