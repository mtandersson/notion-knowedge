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
- Names: `knowledge_search`, `knowledge_get`, `knowledge_upload_file`,
  `knowledge_create_page`, `knowledge_append`,
  `knowledge_update_section`, `knowledge_archive_page`.
- The read category is search/get. The write category includes the file-upload
  stub and all write preview tools. No admin tool exists today; `admin` is
  recognized as a reserved selector, not permission for future unknown names.
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

- `NK_TOOL_ALLOWLIST=read`: permit search and get only (subject to their
  existing root-scope constraints).
- `NK_TOOL_ALLOWLIST=read,knowledge_append` and
  `NK_TOOL_BLOCKLIST=knowledge_get`: allow search; deny get; append remains
  unavailable unless the independent write-preview and read-only gates allow
  it. The preview is never a real Notion mutation.
- `NK_TOOL_BLOCKLIST=write`: deny upload and every write-preview schema
  even if read-only mode is disabled.

The policy is immutable after bootstrap and preserved when an MCP session
handler is cloned. Tests cover exact/category precedence, unknown selectors,
empty values, both discovery APIs, and the original read-only and destructive
write gates.
