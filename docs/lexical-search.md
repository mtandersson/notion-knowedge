# Lexical BM25 search

Issue #46 exposes the native LanceDB FTS indices from #41 through the core retrieval port and the existing `knowledge_search` MCP contract.

## Application port

`LexicalSearch::search(LexicalQuery)` is storage-independent. The query carries:

- non-whitespace query text, at most 4096 characters;
- a result limit from 1 to 100;
- optional `page_ids`;
- optional `root_page_ids`.

The two filter lists use OR within one field and AND across fields. The retrieval adapter validates direct port callers as well as relying on MCP validation.

## Candidate fields

The LanceDB adapter queries four native BM25 indices:

1. exact `chunk_id`;
2. exact `page_id`;
3. `title`;
4. chunk `text`.

Stable IDs use the raw tokenizer from #41. Title and body text use the Swedish language profile documented in [fts-index.md](fts-index.md).

A chunk may match more than one field. The adapter deduplicates by `(page_id, chunk_id)` and keeps the strongest field score. Separate FTS fields have different length distributions, so raw BM25 scores are not directly comparable. The adapter maps each native score into a bounded field band:

- stable ID: `4 + s/(1+s)`;
- title: `2 + s/(1+s)`;
- body text: `1 + s/(1+s)`;

where `s = max(native_bm25_score, 0)`.

This intentionally gives exact stable identifiers the highest priority and title/name matches precedence over body-only matches. The returned score is a backend-specific ranking value, not a probability and not comparable with semantic mode. Vector/BM25 fusion is deliberately left to #47.

## Filters and query safety

`page_ids` and `root_page_ids` are turned into LanceDB predicates with quoted SQL literals. Embedded apostrophes are doubled. User full-text query text is never concatenated into that predicate; it is supplied separately through `FullTextSearchQuery`.

This separation means FTS syntax cannot widen a page/root filter. Broader source/property/date filters remain #48.

## Result contract

Results preserve stable citation provenance: page ID, chunk ID, Notion/source URL, title, heading path and optional block ID. Returned text is capped at 2000 Unicode characters to satisfy the MCP contract.

Queries are deterministic for equal scores using page ID and chunk ID as tie-breakers. The adapter fetches up to four times the requested limit per FTS field (capped at 400), merges duplicates, sorts by final lexical score and then truncates to the requested limit.

## Phrase and multilingual behavior

Quoted phrases are passed through to LanceDB's full-text query parser. Tests cover a quoted English phrase, Swedish stemming, exact identifiers, title/name lookup and English keyword matching.

The current text tokenizer is Swedish-first. English terms still tokenize, but English-specific stemming/stop-word rules are not enabled simultaneously. That trade-off is documented in [fts-index.md](fts-index.md).

## MCP exposure

`KnowledgeServer` may be configured with semantic and lexical adapters independently. `mode=lexical` calls only the lexical port; it never falls back to semantic search. `mode=hybrid` remains unavailable until #47.
