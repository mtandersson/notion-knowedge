# Installed local hybrid retrieval

The operator can explicitly install semantic, lexical and hybrid search in the
ordinary MCP server. Defaults remain unavailable. This read activation requires
Unix and a binary built with `webhook-worker`, which already includes the pinned
offline Qwen provider and LanceDB adapter:

```sh
nix develop .#spike --command cargo build -p notion-knowledge-server --features webhook-worker --locked
NK_RETRIEVAL=true target/debug/notion-knowledge-server
```

Supply these immutable startup settings through the service environment:

- `NK_NOTION_AUTH=integration` and a locally held `NOTION_TOKEN` for fresh physical
  ancestry authorization. No token is taken from MCP requests.
- `NK_NOTION_SCOPE_WORKSPACE_ID`, `NK_NOTION_SCOPE_ROOTS` and the existing scope
  generation and exclusions, exactly matching the operator's committed binding.
- `NK_WEBHOOK_STATE_FILE`: existing protected operational SQLite database.
- `NK_WEBHOOK_INDEX_DIR`: existing owned index directory containing the committed
  `chunks` Lance table, binding anchor, vector metadata and production FTS indexes.
- `NK_WEBHOOK_QWEN_ASSETS`: the existing pinned offline Qwen assets described in
  [local Qwen](local-qwen.md). No model download or provider fallback occurs.

`NK_RETRIEVAL` accepts only `true` or `false`. Retrieval does not require webhook
admission or `NK_WEBHOOK_WORKER=true`. Use the existing authoritative indexing or
reconciliation workflow to populate and maintain the shared index first. After
population, the operator must build the IVF_FLAT vector index using
`GuardedChunkTable::ensure_vector_index` under the shared writer lease and commit
coordinator, as described in [index commits](index-commits.md). Worker table
creation installs FTS only; it does not train IVF automatically. Read
activation requires an operator-owned mode-0700 index directory and mode-0600
state file, beneath operator-controlled ancestors. Use existing protected storage;
activation never changes permissions. Read startup performs no state initialization, migration, FTS repair or writer lease.
Missing/corrupt state, foreign inode bindings, changed scope/generation, missing
FTS indexes and incompatible provider/model/vector-space metadata fail startup
with a bounded error. `--check` validates settings only; actual transport startup
also validates existing storage and loads the offline provider.

Trusted operator stdio may activate directly. HTTP additionally requires
`NK_HTTP_AUTH=bearer`, `NK_HTTP_BEARER_TOKEN` and a loopback listener. The ordinary
middleware enforces bearer authentication on initialization, session continuation,
SSE and deletion. Unauthenticated and OAuth-discovery-only HTTP leave retrieval
unavailable without opening the private index or model. Physical scope authorizes
data; it does not authenticate an HTTP caller. Private ChatGPT OAuth and container
model packaging remain separate work.

Search modes use the existing Lance semantic adapter, BM25 lexical adapter and
weighted reciprocal-rank fusion (100 candidates per path, rank constant 60,
equal weights). Results preserve scores and matched paths. Each call opens the
current committed revision; hybrid paths share one snapshot. Concurrent/unfinished
commits fail closed. Every returned snippet is freshly checked against authoritative
physical ancestry and configured exclusions by the common MCP handler.

For a production smoke, launch the configured binary over stdio or authenticated
HTTP, initialize MCP, then call `knowledge_search` with `query`, `mode` set to
`semantic`, `lexical` or `hybrid`, and a narrow `filters.page_ids`. Verify source
IDs, text, scores and `matched_paths`. A foreign root filter yields no results;
a source moved outside the configured physical roots returns no snippets.
Do not put private corpus text or credentials in smoke logs.

Credential-free joined tests use a typed source/provider fixture, commit actual
SQLite/Lance vector and FTS data, reopen through the installed runtime, and call
search over stdio and authenticated HTTP:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --locked --test webhook_worker
```
