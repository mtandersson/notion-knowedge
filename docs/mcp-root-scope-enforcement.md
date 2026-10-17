# MCP authoritative root policy (#307)

All content-bearing MCP retrieval must use the same physical Notion ancestry
policy, independent of whether the client speaks stdio or Streamable HTTP.
Without a trusted policy, `knowledge_search` and `knowledge_get` **fail closed**.
There is no fallback to an indexed `root_page_id`, caller-supplied root claim,
search result, relation, or cache. Both handlers are gated in `KnowledgeServer`,
not in one particular transport.

## Operator configuration

For trusted operator integration mode (`NK_NOTION_AUTH=integration`,
`NOTION_TOKEN` held in secret configuration), or complete request-bound
[OAuth mode](oauth-production-setup.md) (`NK_NOTION_AUTH=none`), configure:

- `NK_NOTION_SCOPE_WORKSPACE_ID`: trusted canonical lowercase hyphenated UUID
  of the workspace associated with the integration credential
- `NK_NOTION_SCOPE_ROOTS`: comma-separated canonical Notion page UUIDs for
  operator-approved roots, from 1 to 100 entries
- `NK_NOTION_SCOPE_GENERATION`: optional positive integer (default `1`).
  Increment and restart the process when changing scope policy.

Roots and workspace must be set together. Invalid or partial settings abort
startup; a configured scope requires either integration credentials or the complete
OAuth client/callback/store composition with exactly matching allowlisted workspace. Absent scope
keeps the handler in the **no data access** state. Credentials and raw Notion
API errors are never included in public denial responses.

The operator should only pair a workspace ID with a credential actually
provisioned for that workspace. Notion's workspace parent object contains
`workspace: true`, not a verifiable workspace UUID. Operator credential
binding remains a prerequisite; a caller cannot supply or change this identity.

Integration mode uses the shared `attach_root_scope` path for HTTP and stdio.
OAuth HTTP uses [request-bound adapters](oauth-content.md) with the same policy
checks and live grant evidence; it never substitutes the integration client.
The normal bootstrap does not install retrieval by default. Explicit
[`NK_RETRIEVAL=true` activation](installed-retrieval.md) installs the scoped
semantic/lexical/hybrid adapters for trusted stdio or authenticated loopback
bearer HTTP, or fully configured OAuth HTTP. The common handler denies data
without the physical gate.
The optional [`knowledge_related` graph](knowledge-related.md) also
authorizes each discovered page using fresh physical ancestry rather than
treating graph edges as permission.

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

## Installed audited file ingestion (explicit opt-in)

`knowledge_upload_file` is **not enabled by default**. With
`NK_READ_ONLY=false`, `NK_FILE_INGESTION=true`, the integration credential,
configured roots/exclusions, admitted HTTPS source hosts and protected durable
audit storage, it installs a real native Notion attachment workflow; not merely
a schema preview. Stdio is treated as a trusted local operator transport.
HTTP additionally requires `NK_HTTP_AUTH=bearer`, a configured strong bearer
token and a loopback listener. Anonymous HTTP and discovery-only OAuth do not
install private ingestion. Tool allow/block policy still applies *before*
adapter execution.

The server-owned target and its physical ancestry/exclusions are checked
against Notion before source download, upload and append. An incoming target
ID, file reference or claimed root never grants permission. Ingestion validates
source URLs and actual bytes, creates a native file/image block, verifies
Notion readback and writes minimized durable audit outcomes. Batch partial
success, cancellation and possible indeterminate outcomes are not an atomic
Notion transaction: see [installed ingestion](installed-file-ingestion.md)
and [startup settings](file-ingestion-settings.md) for exact contracts and
negative transport tests. Scope checks are intentionally repeated, but a
page move between checks and effects remains a non-atomic TOCTOU risk.

## Other write tools

The remaining page-create/append/update/archive MCP operations are not
currently installed as production mutations. Their development preview is not
a mutation. Future implementations must reuse fresh physical root checks
immediately before effects, plus confirmation, revision/idempotency, safe
audit and authoritative readback. Do not equate a schema preview or a
caller-supplied root claim with authority.

## Verification

```sh
cargo test -p notion-knowledge-mcp --test scope_enforcement --locked
cargo test -p notion-knowledge-mcp --test search_adapter --locked
cargo test -p notion-knowledge-mcp --test get_adapter --locked
cargo test -p notion-knowledge-mcp --test related --locked
# Integration of authenticated transport, root/exclusion and audited file paths:
nix develop .#spike --command cargo test -p notion-knowledge-server --lib ingestion_tests --locked
```

See CI for whole-workspace format, Clippy, type checks, core/Notion tests,
release build, and container smoke.
