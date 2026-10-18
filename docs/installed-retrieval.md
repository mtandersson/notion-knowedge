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

- For trusted operator stdio/local bearer: `NK_NOTION_AUTH=integration` and a
  locally held `NOTION_TOKEN` for fresh physical ancestry authorization. For
  production OAuth: `NK_NOTION_AUTH=none` and the complete
  [OAuth configuration](oauth-production-setup.md); content uses the request-bound
  approved grant. No Notion token is taken from MCP requests.
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
population, the operator must build the IVF_FLAT vector index with the trusted
local command below. It uses the shared writer lease and commit coordinator,
as described in [index commits](index-commits.md). Worker table
creation installs FTS only; it does not train IVF automatically. Read
activation requires an operator-owned mode-0700 index directory and mode-0600
state file, beneath operator-controlled ancestors. Use existing protected storage;
activation never changes permissions. Read startup performs no state initialization, migration, FTS repair or writer lease.
Missing/corrupt state, foreign inode bindings, changed scope/generation, missing
FTS indexes and incompatible provider/model/vector-space metadata fail startup
with a bounded error. `--check` validates settings only; actual transport startup
also validates existing storage and loads the offline provider.

Trusted operator stdio may activate directly. Local fallback HTTP requires
`NK_HTTP_AUTH=bearer`, its configured token and a loopback listener. Production
OAuth HTTP instead requires the full [OAuth setup](oauth-production-setup.md)
and exact matching scope; its [request-bound source](oauth-content.md) authorizes
each content disclosure. Anonymous and discovery-only HTTP leave retrieval
unavailable without opening the private index, model or state, including lazy
sync-status readers. OAuth content never installs webhook/reconciliation writers.
The [production image](container.md) packages this feature; exact model assets
and compatible index/state remain externally provisioned.

## Provision and rebuild the derived vector index

After the authoritative worker has populated the table, stop its writers and
run the feature-built binary with the same scope, generation and
`NK_WEBHOOK_INDEX_DIR`, `NK_WEBHOOK_STATE_FILE`, `NK_WEBHOOK_QWEN_ASSETS` settings:

```sh
notion-knowledge-server --index ensure
# Later, explicitly retrain derived ANN state when required:
notion-knowledge-server --index rebuild
```

`ensure` creates the production Cosine IVF_FLAT index if absent, validates an
existing compatible index, and folds in unindexed committed rows. `rebuild`
explicitly replaces the derived ANN index over the same committed vectors.
Neither command changes canonical chunks, source text, ownership, page receipts
or Notion content. Neither calls Notion or embeds source text. The configured
offline Qwen assets are validated and loaded before storage access, so an
incompatible model identity cannot be attached to existing vectors. No model
download or provider fallback occurs. Preserve the normal integration or complete
OAuth configuration required for the configured scope; those credentials remain
unused by this local operation. Configuration validation is unchanged.

Both commands require an existing compatible populated table and current SQLite
schema. They do not initialize, migrate or silently repair state. Empty tables,
foreign binding/model metadata and missing FTS fail. An existing incompatible
vector-index layout also fails, including on rebuild; use explicit operator
recovery with a separately rebuilt derived index rather than changing identity
in place. The shared SQLite writer lease prevents concurrent webhook/manual
writers. Stop those writers before maintenance and restart retrieval afterward.
Timeout and Ctrl-C invalidate admission, drain any already owned native IO and
then release the lease. Abrupt process death may require expiry of the persisted
lease before retry; it cannot authorize a second simultaneous writer.

The command emits only `{"operation":"index","status":"complete"}` on
success. Exit codes are 0 success, 2 invalid arguments/configuration or incompatible
source/vector binding, 3 failed index work (including incompatible existing ANN
layout), timeout or state failure, 4 busy writer and 5
operator cancellation. Results never include credentials, paths or corpus text.
After success, enable `NK_RETRIEVAL=true` and run the source-filtered MCP smoke
below. Ordinary CI exercises real fixture Notion reads, SQLite/Lance population,
these command services and actual vector queries without model downloads:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command index_commands --locked
```

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

Optional bounded graph reranking is installed over this same hybrid adapter through
[immutable graph boost settings and paired evaluation](installed-graph-boost.md).
