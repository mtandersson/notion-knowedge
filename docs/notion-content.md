# Authoritative page content reads

`NotionClient::read_content(ID_OR_NOTION_LINK)` returns the existing core
`PageContent`: freshly fetched exact page metadata plus complete enhanced
Markdown. It uses the official `GET /v1/pages/{id}/markdown` endpoint and API
version `2026-03-11`, rather than duplicating Notion's block renderer. This
endpoint renders nested physical block content, headings, paragraphs, lists,
code and links. Nested blocks use tabs. Markdown is preserved byte-for-byte:
trimming, tab expansion or fence rewriting would change child/code semantics.
For unchanged upstream responses the returned content is deterministic.

Notion-specific enhanced Markdown tags remain readable and lossless. Unsupported
blocks are surfaced explicitly by upstream `<unknown url="..." alt="..."/>`
markers; they are retained rather than silently dropped. These markers are not
claims that unsupported content was extracted or can be written back losslessly.
A `truncated` response or any `unknown_block_ids` fails with
`UnsupportedContent`, because those IDs can represent inaccessible content as
well as record-limit truncation. This read does not fetch those subtrees or
return a misleading partial success. Empty Markdown is a valid empty page.
Malformed/wrong-object/wrong-ID responses and responses over 8 MiB fail closed.
Upstream failures use sanitized core error classes; private response bodies,
content and credentials are never logged. There is no retry or cache here.

This is an exact raw authoritative read, not an indexing authorization layer.
Index consumers must first use the fresh `crawl_with_exclusions` report and
fetch content only for its allowed `pages`, as documented in
[discovery](notion-discovery.md). Neither linked pages nor unknown-block URLs
are followed by this adapter. The metadata and Markdown requests are not an
atomic snapshot: concurrent Notion edits can produce metadata/content from
slightly different instants; callers can reread to reconcile. The complete
`NotionBackend` trait is not implemented until its remaining capabilities exist.

Run `cargo test -p notion-knowledge-notion --locked`. Mock HTTP tests check the
actual production request path, version and credential header, lossless nested
lists/code/links, repeated-read determinism, explicit unsupported markers,
empty pages, malformed/incomplete content, bounded reads and sanitized failures.
They use no live Notion credential. Nested rendering is performed by Notion;
tests verify its documented output at our adapter boundary, not a local renderer.

Protocol references:

- [Retrieve page Markdown](https://developers.notion.com/reference/retrieve-page-markdown)
- [Enhanced Markdown](https://developers.notion.com/guides/data-apis/enhanced-markdown)

The separate [relationship normalization step](notion-links.md) extracts page
links, mentions and relation-property edges from this lossless content without
fetching targets or expanding discovery scope.
