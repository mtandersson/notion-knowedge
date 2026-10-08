# LanceDB BM25 / FTS index

Issue #41 maintains native LanceDB full-text indices over the canonical chunk table. This layer owns storage/index lifecycle only; the [search contract](knowledge-search.md) and [hybrid service](hybrid-search.md) compose lexical retrieval, filters and reciprocal-rank fusion.

## Indexed fields

LanceDB native FTS indexes one string field at a time, so the chunk table keeps four independently named, versioned indices:

| Index | Column | Tokenization |
| --- | --- | --- |
| `chunks_fts_text_v1` | `text` | Swedish language profile |
| `chunks_fts_title_v1` | `title` | Swedish language profile |
| `chunks_fts_page_id_v1` | `page_id` | raw/exact |
| `chunks_fts_chunk_id_v1` | `chunk_id` | raw/exact |

The versioned names are part of the local derived-index contract. Startup creates missing indices and fails closed if one of these names is present with the wrong index type or column.

## Swedish text behavior

The default text/title profile uses LanceDB's simple tokenizer with:

- `language = "Swedish"`;
- lower-casing left at the backend default;
- stemming enabled;
- language stop-word removal enabled;
- ASCII folding disabled so Swedish characters such as å, ä and ö remain distinct;
- posting block size 128.

This is deliberately a Swedish-first profile for Martin's personal corpus. English words are still tokenized by the same Unicode/simple tokenizer and are covered by fixture tests, but English-specific stemming/stop-word behavior is not enabled at the same time. If corpus evaluation later shows a multilingual quality problem, that is a measured configuration/version change rather than a silent tokenizer swap.

Stable IDs use the raw tokenizer with lower-casing, stemming, stop-word removal and ASCII folding disabled. The complete ID remains one token, including separators such as `-` and `:`.

## Lifecycle

`LanceChunkTable::create` and `open` call `ensure_fts_index`. On a new empty table, the named empty indices are installed without training; on a populated compatible table, missing indices are trained from the current rows.

After incremental writes from #42, call `optimize_fts_index` at an operationally appropriate boundary. It asks LanceDB to fold delta/unindexed rows into all four named FTS indices. Ordinary queries do not enable the index-only fast path, so maintenance timing can be chosen independently from correctness; optimization is primarily index freshness/efficiency maintenance.

`rebuild_fts_index(config)` explicitly replaces all four FTS indices when text tokenizer configuration changes. Index replacement is derived-state maintenance, not authoritative data mutation. As with other local index rebuilds, Notion remains authoritative.

## Configuration

`FtsIndexConfig` controls the language, stemming, stop-word removal, ASCII folding and posting block size used by the title/text indices. The default block size is 128. Unsupported language or block-size configuration fails before a usable replacement is reported.

Changing tokenizer semantics should also bump the versioned index names in a future schema change. Merely reopening a table never silently replaces an existing versioned index with different tokenizer settings.

## Query probe

`fts_query(column, query, limit)` is intentionally a narrow adapter probe. It requires one of the four indexed columns and returns BM25-scored chunk/page/title/text data. It exists to verify the physical index and gives #46 a reusable storage primitive; it is not the final MCP/API lexical search surface.

Tests cover Swedish and English body terms, title lookup, exact page/chunk IDs, index lifecycle, update optimization and invalid configuration.
