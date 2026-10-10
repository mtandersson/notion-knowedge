# Semantic Notion write-tool surface (#76)

This is a **contract design**, not a working write implementation. The
production `KnowledgeServer` advertises read-only `knowledge_search` and
`knowledge_get`. Explicit writable configuration can additionally expose the
unavailable `knowledge_upload_file` schema. Nothing in this issue grants Notion
credentials, a writable scope, a confirmation policy or an API invocation.

## Agent-facing tools

All future write tools require the server to authorize the target in its own
configured workspace/root, independently of the caller-supplied
`root_page_id`. No tool accepts arbitrary Notion HTTP paths or block JSON.
`root_page_id` is a **claim to verify**, never a grant; only a verified
authoritative source ID is actionable. The proposed tool names, input/output
schemas and MCP annotations are in
[`crates/mcp/src/write_contract.rs`](../crates/mcp/src/write_contract.rs).

| Tool | Narrow intent | Target/provenance | Mutation risk |
| --- | --- | --- | --- |
| `knowledge_create_page` | Create one child page with a title and Markdown | Explicit authorized `parent_page_id` within server-approved `root_page_id`; stable verified page receipt | Non-destructive, non-idempotent without reservation |
| `knowledge_append` | Append Markdown at the end, preserving unrelated content | Existing `page_id`, claimed root and `expected_last_edited_time` | Non-destructive but repeated append duplicates |
| `knowledge_update_section` | Replace one anchored section, **not** a whole page | Existing `page_id`, stable `section_anchor`, claimed root and expected source revision | Content mutation; optimistic concurrency required |
| `knowledge_archive_page` | Explicit archive of one named page | Existing `page_id`, claimed root, expected revision and server-issued `confirmation_id` | **Destructive**; cannot be hidden in a generic update |

Each accepts `idempotency_key` to correlate retries and require a
once-only server-side commit/receipt. The key alone is **not** an implemented
deduplication mechanism: future #81 must persist it with target, operation
and request hash before retries may be safe. For create, there is no existing
page timestamp to compare; the server instead needs parent provenance and a
durable create reservation. Append and update must guard against the
authoritative source revision changing before commit (#80), and section
anchors must be resolved within the bounded current source (#79). Archive
needs explicit approval from a separate confirmation policy (#82).

The output schema is a **verified authoritative receipt** with
`page_id`, `url`, `last_edited_time` and `verified: true`. It may only
be sent after read-back of the intended effect (#83). If a network timeout or
uncertain acknowledgment occurs, do **not** fabricate this receipt or
blindly replay: reconcile source state and the durable idempotency record.

Read tools remain semantically distinct: their
`annotations.readOnlyHint = true`. Proposed write-tool annotations always
specify `readOnlyHint = false`; section replacement and archiving set `destructiveHint = true` because
both can remove existing content. All planned writes have `openWorldHint = true`
because Notion API calls depend on an external system.

## Default discovery vs schema-preview

The default catalog **does not expose any planned write tool**, even when
Notion access is present. An explicitly constructed development test handler
can disable read-only mode and call `KnowledgeServer::with_write_design_preview()` to expose the four
proposed schemas in `tools/list` / `get_tool`. **Every preview invocation
returns `workflow_unavailable` with no mutation attempted**; this is not
an operator setting or production startup option and must not be presented as
working Notion CRUD. No branch of the normal production main executable
enables preview mode.

The catalog and schema checks run without credentials or Notion access:

```sh
nix develop --command cargo test -p notion-knowledge-mcp --test write_catalog --locked
```

That test verifies the normal read-only catalog preserves the two read tools, hides upload/write schemas even in preview, and explicit writable mode preserves the unavailable upload schema; preview
tool names, bounded non-generic schemas, required target/revision/idempotency
fields and destructive annotations are distinct. The shared discovery
construction is used by the actual MCP handler. This test cannot prove that
future write workflows are safe or functional.

## Required implementation before production exposure

The successive #77–#83 workflow tasks own implementation, validations,
idempotency, confirmation, concurrency and authoritative verification.
#87 must enforce allowed root scope on **every** operation, #86 may further
narrow exposed capabilities, #85 supplies global read-only behavior, and
#117 supplies authenticated user/workspace identity. Expose a write tool in
normal discovery **only when all required enforcement and read-back exist**.
No implementation should rely on a chat model promising not to invoke a
destructive or unapproved tool.

**Examples of deliberate non-goals:** generic page/block mutation,
arbitrary property PATCH payloads, replace-whole-page under
`knowledge_update_section`, multi-page transactions, file ingestion (the
separate #9 workflow), raw source URL input as authorization, or returning a
success receipt solely from a 2xx Notion response.
