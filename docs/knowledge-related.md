# Bounded knowledge_related MCP navigation (#63)

The read-only `knowledge_related` tool follows the **derived local graph** of
canonical Notion page IDs (not wiki titles or embedding chunk IDs). Both stdio
and HTTP use the shared MCP handler. It does not fetch live Notion content,
resolve unknown aliases or use graph edges as a permission grant.

## Activation and inputs

The trusted composition root must first install an authoritative
`RootScopeGate` via `KnowledgeServer::and_authoritative_scope`, then inject
an existing `GraphEdgeStore` through `and_related_graph`. Without a
graph adapter, the tool is **not advertised**. The default bootstrap does not
automatically open an arbitrary local SQLite file for graph access.

```json
{"page_id":"11111111-1111-1111-1111-111111111111","depth":1,"limit":50}
```

- `page_id` must be a canonical lowercase Notion page UUID.
- `depth` defaults to 1; only 1 or 2 hops are accepted.
- `limit` defaults to 50 and caps output at 100 relationships.
- Callers **cannot** supply workspace, root scope, or authorization flags.
- An enabled read capability policy is still required. Read-only mode allows
  this tool, but enabling it does not enable writes.

Output has `page_id`, `relations`, and `limit_reached`. Each relation has
`source_page_id`, `target_page_id`, `relation_type`, `provenance`,
`depth` and `direction` (relative to the node visited during expansion).
An incoming relation keeps its original endpoints. Results are deterministically
ordered and deduplicated by source, target, relation type and provenance.

## Safety guarantees

1. The seed must pass a fresh physical-ancestry authorization/revalidation
   against server-configured roots. No graph read happens before this.
2. Every resolved neighbor is freshly checked and revalidated *before*
   disclosure or use as an intermediate node. Explicitly out-of-scope and
   unresolved references are excluded, including from the second-hop frontier.
   Unverifiable lifecycle metadata fails the entire call without partial data.
3. Each node's incoming + outgoing read is capped at 256 graph entries to
   avoid unbounded local traversal, with at most two hops and 100 emitted edges.
   Database failure or invalid graph direction fails the request without
   returning partial metadata.
4. The shared MCP serialized-output budget (512 KiB) applies after traversing.
   This tool returns relationship metadata only, not page titles, body text
   or unresolved reference strings.

Like other backend-mediated read tools, a scope may change between individual
authorization checks; the physical gate is revalidated immediately before
use. No persisted positive authorization cache is introduced.

## Verification

```sh
cargo test -p notion-knowledge-mcp --test related
cargo test -p notion-knowledge-mcp --test capabilities
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

The integration fixture covers depth defaults and bounds, incoming/outgoing
orientation, duplicate relation collapse, propagation of property provenance,
unresolved and denied edge suppression, disallowed seed pages, fanout bounds,
store failures, read-only MCP discovery and JSON-RPC dispatch. The installed joined checks below additionally exercise production composition.

## Installed operator activation (#377)

Build with `--features webhook-worker` on Unix. `NK_GRAPH=true` explicitly
installs graph navigation in the common stdio/HTTP composition. It defaults to
false and is independent of `NK_RETRIEVAL`; graph-only reads load no embedding
model. Configure `NK_NOTION_AUTH=integration`, `NOTION_TOKEN`,
`NK_NOTION_SCOPE_WORKSPACE_ID`, `NK_NOTION_SCOPE_ROOTS`,
`NK_WEBHOOK_INDEX_DIR` and `NK_WEBHOOK_STATE_FILE` to match the existing
production writer. These immutable operator paths cannot be supplied by MCP.
The index directory and SQLite state must be private and operator-owned.
Startup opens existing state read-only and rejects missing, foreign,
incompatible or corrupt authority; it never initializes or migrates it.

Trusted operator stdio may activate directly. HTTP additionally requires
`NK_HTTP_AUTH=bearer` and `NK_HTTP_BEARER_TOKEN` on a loopback listener.
The ordinary middleware authenticates every request, including continued
sessions and SSE. Physical Notion root scope is data authorization, not caller
authentication. Bootstrap and OAuth-discovery-only HTTP do not install or open
private graph/index/model adapters. Remote ChatGPT OAuth remains unavailable.

Reads use the production coordinator's storage identity, binding and pending
commit fence. One bounded navigation holds the read fence across incoming/outgoing
reads and awaited neighbor authorization. Only exact current, complete combined graph projections are
returned; unfinished effects fail closed. Each call still freshly authorizes
the seed and discovered neighbors through the authoritative root gate.

Credential-free joined smoke, using typed local Notion fixtures, actual
production commits, reopened SQLite authority and real MCP transports:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test webhook_worker installed_graph --locked
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test webhook_worker authenticated_http_serves_committed --locked
```
