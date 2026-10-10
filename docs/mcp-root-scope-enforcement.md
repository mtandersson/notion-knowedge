# MCP authoritative root policy (#307)

All content-bearing MCP retrieval must use the same physical Notion ancestry
policy, independent of whether the client speaks stdio or Streamable HTTP.
Without a trusted policy, `knowledge_search` and `knowledge_get` **fail closed**.
There is no fallback to an indexed `root_page_id`, caller-supplied root claim,
search result, relation, or cache. Both handlers are gated in `KnowledgeServer`,
not in one particular transport.

## Operator configuration

For existing Notion integration authentication (`NK_NOTION_AUTH=integration`,
`NOTION_TOKEN` held in secret configuration), configure:

- `NK_NOTION_SCOPE_WORKSPACE_ID`: trusted canonical lowercase hyphenated UUID
  of the workspace associated with the integration credential
- `NK_NOTION_SCOPE_ROOTS`: comma-separated canonical Notion page UUIDs for
  operator-approved roots, from 1 to 100 entries
- `NK_NOTION_SCOPE_GENERATION`: optional positive integer (default `1`).
  Increment and restart the process when changing scope policy.

Roots and workspace must be set together. Invalid or partial settings abort
startup; a configured scope requires integration credentials. Absent scope
keeps the handler in the **no data access** state. Credentials and raw Notion
API errors are never included in public denial responses.

The operator should only pair a workspace ID with a credential actually
provisioned for that workspace. Notion's workspace parent object contains
`workspace: true`, not a verifiable workspace UUID. Operator credential
binding remains a prerequisite; a caller cannot supply or change this identity.

The same `attach_root_scope` composition path configures HTTP and stdio.
The current normal server bootstrap has no retrieval adapters by default; when
future deployment composition installs search or source expansion, the handler
itself refuses their output until the physical gate is configured.

## Per-operation ordering

1. Validate MCP input and require the trusted `RootScopeGate`. The supplied
   root filters can only narrow a trusted configured root set.
2. Authorize explicit page IDs before calling the search or get adapter.
3. Treat returned index/chunk provenance as untrusted. Authorize **every**
   returned physical page against the effective roots and revalidate its
   ancestry. One failing item rejects the entire result, with no content
   or page metadata disclosed.
4. For chunk-only `knowledge_get`, first obtain the page identity through the
   expansion adapter, then authorize it before sending any content.
5. Before returning any `knowledge_get` content, recheck live ancestry again
   (including when a requested fresh read ran between checks).
6. Missing metadata, an archived/excluded parent, changed root membership or
   changed policy denies. No positive authorization cache exists.

The multi-read preflight is not an atomic Notion transaction, so moving a page
between the last check and disclosure is a residual time-of-check/time-of-use
window. It narrows exposure, but must not be described as linearizable.

## Planned mutation/file tools

Production mutation tools are **still hidden/unavailable**. The upload tool
remains explicitly unavailable and attempts no download or upload. The
development-only write schema preview also performs no mutation. Their future
implementations must reuse the physical gate and revalidate immediately before
each external create/append/update/archive/file action, together with approved
confirmation, revision, idempotency and authoritative read-back workflows.
Do not equate schema preview or a request-supplied root claim with authority.

## Verification

```sh
cargo test -p notion-knowledge-mcp --test scope_enforcement --locked
cargo test -p notion-knowledge-mcp --test search_adapter --locked
cargo test -p notion-knowledge-mcp --test get_adapter --locked
```

See CI for whole-workspace format, Clippy, type checks, core/Notion tests,
release build, and container smoke.
