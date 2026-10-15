# MCP operation allowlist and blocklist (#86)

Every MCP tool published by `KnowledgeServer` is subject to the same
server-owned capability policy in both stdio and HTTP. Discovery (`list_tools`,
`get_tool`) **and** invocation (`call_tool`) use identical effective gates.

## Configuration

- `NK_TOOL_ALLOWLIST` is optional. When omitted, currently implemented or
  intentionally previewed tools retain their existing independent access
  restrictions. An explicitly empty value disables every tool.
- `NK_TOOL_BLOCKLIST` is optional. When omitted/empty, no operations are
  removed by this policy.
- Each setting is a comma-separated list of exact canonical tool names or
  the categories `read`, `write`, `admin`.
- Current exact **read** operations: `knowledge_search`, `knowledge_get`,
  `knowledge_related` and `knowledge_sync_status`.
- Current exact **write** operations: `knowledge_upload_file`,
  `knowledge_create_page`, `knowledge_append`,
  `knowledge_update_section` and `knowledge_archive_page`.
- These nine names are the canonical set in
  [`crates/mcp/src/capabilities.rs`](../crates/mcp/src/capabilities.rs).
  `read` selects all four read operations; `write` selects all five write
  operations. No admin tool exists today: `admin` is a reserved selector, not
  permission to invoke an unknown or future administrator tool.
- Selection is **not activation**: search/get need installed retrieval and
  authoritative root authorization; `knowledge_related` needs an installed
  graph plus that gate; `knowledge_sync_status` returns only bounded
  operational metadata. Upload is a real optional audited file operation only
  with explicit write/ingestion settings and a trusted transport; the other
  write tools are currently non-mutating previews or unavailable.
- Values with unknown, empty interior, or malformed selectors cause startup
  to fail, identifying only the setting name and never logging raw values.

A tool may be used only when (1) it is a known canonical operation,
(2) explicitly allowed by an existing per-tool implementation and read-only
mode, (3) permitted by independent destructive-operation policy, (4) selected
by the operator allowlist when present, and (5) **not** selected by the
blocklist. A blocklist category overrides an allowlist exact name and vice
versa. These settings cannot enable writes, experimental previews, OAuth
authorization, or administrator capabilities which do not already exist.

## Examples

- `NK_TOOL_ALLOWLIST=read`: select search, get, related and sync status
  (subject to each tool's own installation/scope restrictions).
- `NK_TOOL_ALLOWLIST=read,knowledge_append` and
  `NK_TOOL_BLOCKLIST=knowledge_get`: allow search; deny get; append remains
  unavailable unless the independent write-preview and read-only gates allow
  it. The preview is never a real Notion mutation.
- `NK_TOOL_BLOCKLIST=write`: deny both installed audited upload and every
  write-preview schema, even if read-only mode and upload settings permit them.

The policy is immutable after bootstrap and preserved when an MCP session
handler is cloned. The common `KnowledgeServer::tool_permitted` gate applies
at tool discovery and dispatch across HTTP and stdio. Tests in
[`crates/mcp/tests/capabilities.rs`](../crates/mcp/tests/capabilities.rs)
cover exact/category precedence, unknown selectors, empty lists, discovery,
read-only and destructive gates; installed ingestion also tests policy rejection
before external effects (see [installed-file-ingestion.md](installed-file-ingestion.md)).
Do not interpret a tool's read-only annotation as its authorization decision.
