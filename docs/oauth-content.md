# Request-bound OAuth content (#432)

Authenticated OAuth HTTP can read an existing private committed index. Configure
the complete registered-client/browser/grant settings, authoritative
`NK_NOTION_SCOPE_ROOTS` and `NK_NOTION_SCOPE_WORKSPACE_ID`, plus the existing
[retrieval settings](installed-retrieval.md). The scope workspace must equal
`NK_NOTION_ALLOWED_WORKSPACE_ID`. Leave `NK_NOTION_AUTH=none` (the default):
OAuth content rejects integration mode and never falls back to `NOTION_TOKEN`.
The Unix binary needs `webhook-worker` for local LanceDB/Qwen retrieval support;
this does not enable the webhook worker. OAuth content rejects writer activation.

Startup constructs one credential runtime, shared by callback persistence, token
minting, resource authorization and request-bound content ports. Read-only query
composition opens the existing SQLite authority and guarded Lance table without
constructing reconciliation handlers, taking writer leases, repairing indexes,
or creating new authority. Missing/incompatible vector or FTS indexes fail closed.
Existing model/vector metadata, ownership and private-storage checks remain.

Each verified HTTP request carries non-secret token/grant evidence and owned
read/lifecycle ports through SDK request Parts. A tool clones the shared handler
and binds that request's physical root gate and fresh-read source; cloned sessions
retain the same weighted tool pool. Admission covers initial validation, backend
work, buffered results and final validation. Optional graph scoring is constructed
with the request's root gate, never a static integration gate.

Every Notion operation obtains a temporary OAuth client only after active grant
verification and exact grant ID/epoch/user/workspace matching. It checks the live
sealed snapshot and token deadline again before returning, and preserves sanitized
backend error classifications. A replacement callback cannot switch an in-flight
tool to a newer grant. Token-preserving upstream refresh remains usable with the
same MCP token/session. Request completion verifies the active binding again
before disclosing indexed or authoritative content. No task-local context or
cached concrete credential client is used.

Search supports lexical, semantic and hybrid queries through guarded committed
snapshots. `knowledge_get` expands page/chunk references through the guarded source
adapter; `freshness: "fresh"` uses the request-bound Notion read port after indexed
provenance and physical ancestry checks. Root exclusions, live page permissions,
metadata validation, output limits and read-only scope continue to apply. Missing
request context fails before content adapters run.

This is read-only content composition. It does not enable indexing/reconciliation,
uploads or mutations through OAuth. [Local logout and operator reset](oauth-revocation.md) cancel token evidence and
prevent pending results from disclosing content after revocation. See the
[joined HTTP acceptance suite](oauth-e2e-tests.md) and
[production setup](oauth-production-setup.md); a real deployed ChatGPT smoke
remains an operator responsibility.

Tests use real local HTTP/SDK sessions and an actual committed SQLite/Lance index,
with typed local Notion and embedding fixtures. No production provider credentials
or external authenticated requests are required:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --locked
nix develop --command cargo test -p notion-knowledge-mcp --locked
```
