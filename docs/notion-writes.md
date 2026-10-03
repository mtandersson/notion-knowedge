# Page creation and append primitives

`NotionClient::create_page(CreatePage)` accepts the explicit authorized
`parent_page_id`, title and Markdown from the core contract. A composition
caller supplies its configured parent/root page ID in that field; the current
bootstrap has no parent/root environment setting. IDs must be UUIDs, with or
without hyphens. Notion enforces integration access and content capabilities.
This primitive creates child pages, not database rows or workspace roots.
The receipt contains the authoritative stable page ID and URL.

`append_content(AppendPageContent)` accepts common Markdown directly, including
headings, paragraphs, emphasis, links, lists, tasks and code fences. It sends
only `insert_content` with explicit `position: {type: end}`. Existing content is
preserved. No replacement, delete permission, selection, automatic retries,
batching or asynchronous task execution is requested. Upstream conversion is
Notion's enhanced Markdown parser, so callers do not construct block JSON.

These are adapter primitives; full `NotionWrite` implementation waits for the
metadata and explicit replacement tickets, rather than manufacturing partial
core `Page` metadata. MCP tools and server startup do not invoke them yet.
Request bodies are capped at Notion's 500 KiB request limit; Notion may reject
additional parsing/block-count limits. Responses are capped at 2 MiB. A timeout,
interrupted/malformed response or unexpected async receipt is a failure, not
proof the mutation did not happen. Callers must reconcile rather than blindly
retry a non-idempotent create or append. Raw bodies and credentials never
appear in returned errors. Rate limiting and Retry-After policy belong to #23.

The Markdown operations use `Notion-Version: 2026-03-11`; identity probing
retains its separate `2022-06-28` version. Official references checked
2026-10-03:

- [Create a page](https://developers.notion.com/reference/post-page)
- [Update page Markdown](https://developers.notion.com/reference/update-page-markdown)
- [Working with Markdown](https://developers.notion.com/guides/data-apis/working-with-markdown-content)
- [Request limits](https://developers.notion.com/reference/request-limits)

The update reference still supports `insert_content` but labels it legacy and
warns of possible future deprecation. It supplies append semantics directly;
the recommended replacement commands would violate this operation's contract.

Run credential-free mocked HTTP tests with
`cargo test -p notion-knowledge-notion --locked`.

## Explicit replacement

`NotionClient::replace_content(ReplacePageContent)` replaces one explicit UUID
page target using the recommended `replace_content` command and `new_str`.
Replacement is disabled on every new client. The trusted composition caller
must grant it with `with_replacement_access(true)`; passing `false` disables
it again. This is a local operation capability, separate from Notion integration
permissions and future MCP caller authorization. Create and append remain
separate existing primitives. No MCP tool or environment setting enables this
operation automatically.

Before writing, the adapter reads complete authoritative metadata and Markdown.
Both existing and requested content must fit a conservative parser allowlist:
paragraphs, headings, ordinary quotes, fenced code, lists/tasks,
emphasis, strong, strikethrough, links and rules. HTML/enhanced Notion XML,
unknown blocks, images, tables, footnotes, math and other extensions fail closed.
Indented code is rejected because Notion tabs encode children; heading
attributes and wiki-link extensions are also rejected. Literal HTML inside
fenced code is safe. Archived or incomplete pages also fail.
This prevents whole-page replacement from silently removing content whose
preservation has not been implemented. Empty Markdown explicitly clears an
ordinary page. Notion's child-page/database deletion guard is explicitly kept
with `allow_deleting_content: false`; asynchronous writes are disabled.

A successful mutation receipt must identify the target and complete content.
A separate authoritative read verifies that the result matches the requested
Markdown exactly. Notion formatting normalization can therefore return a
`Conflict` at `notion.replace.verify` after a successful mutation: reconcile by
reading the page, never blindly retry. Read-after-write is verification, not a
transaction or conditional update: concurrent writers can still race between
preflight and mutation. This operation does not claim optimistic concurrency.
Errors expose only the sanitized operation/stage and normalized failure class;
raw upstream validation messages, content and credentials are discarded.

The primitive returns `PageContent` with verified metadata; it does not yet
implement the full `NotionWrite` trait (whose other methods require complete
metadata receipts). Requests remain subject to the shared serialized 500 KiB
limit; replacements additionally reject input above 490 KiB before preflight.
