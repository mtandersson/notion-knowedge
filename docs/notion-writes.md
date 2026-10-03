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
