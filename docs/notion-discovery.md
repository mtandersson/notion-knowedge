# Scoped Notion discovery

`ScopedDiscovery` in `core` exposes read-only discovery reports; `NotionClient`
implements it with `crawl_roots`. Supply one or more explicit root page IDs (or
Notion links). Roots are included, normalized and deduplicated. Returned pages
are sorted by ID and contain ID, title and authoritative URL, independent of
property normalization and future indexing adapters.

Run a dry-run with an integration credential supplied through the secret
process environment:

```sh
cargo run -p notion-knowledge-server -- --crawl-dry-run ROOT_PAGE_ID SECOND_ROOT_PAGE_ID
```

Set `NK_NOTION_AUTH=integration` and supply `NOTION_TOKEN` through your existing
secret environment. No credential is accepted on the command line. The command
prints a complete JSON report to stdout after successful traversal, including
skipped IDs/reasons; failures print only a sanitized class to stderr and exit
nonzero. Titles and links in the report are private workspace data: treat saved
output accordingly. This command neither starts MCP nor writes an index.

## Scope and traversal

The adapter uses Notion API version `2026-03-11`. It follows physical block
children, nested child pages, child databases and all of their owned data
sources. It handles pagination on both block-child GETs and data-source query
POSTs, including opaque cursor encoding. Wiki queries can return child data
sources: their actual parent chain must reach a configured page root before
querying their content. Database/source metadata is not an indexed page.

Every ordinary child edge must match the authoritative parent in the response;
child page/block/database/source metadata is checked again before exploring it.
Links, mentions and relations never authorize traversal. Linked sources whose
actual parent is elsewhere are skipped. Referenced synced blocks are skipped
because their original content may be outside the roots; original synced blocks
with `synced_from: null` are traversed normally. No global search is used.

Only accessible, nonarchived pages are reported. A denied/missing descendant is
recorded as `inaccessible`; an unreadable configured root fails the run. Archived
objects, moved/outside-scope objects and synced references have explicit skip
reasons. Rate limits, outages, invalid responses and unsupported/malformed
ancestry fail the run rather than return a misleading partial success.

Discovery is deliberately **restart safe**, rather than checkpointed. It makes
only reads (query POSTs are read operations), returns no report on failure, and
keeps no persistent crawl/index state. Rerun the same roots after interruption;
IDs are deduplicated on every run. A future index consumer must replace/upsert by
page ID and commit its own complete scope snapshot; this crawler does not
promise transactional index writes or implement the separate sync/checkpoint
work. Changing roots requires a fresh run; no old root authority is reused.

The report is not an atomic Notion snapshot: concurrent moves/edits can change
results during traversal. Parent checks reduce stale-edge traversal; rerunning
reconciles the current physical tree. Cyclic/duplicate page and container IDs
are explored once. Cursor cycles fail explicitly. A run is bounded to 100,000
visited/pending nodes, 100,000 items per listing, 10,000 list pages per container
and 2 MiB per response; exceeding limits fails without silent truncation.

## Protocol references and verification

The current official API specifies physical block children, child-page and
child-database block IDs, original/referenced synced blocks, databases' data
sources and wiki query result types:

- [Block object](https://developers.notion.com/reference/block)
- [Retrieve block children](https://developers.notion.com/reference/get-block-children)
- [Retrieve database](https://developers.notion.com/reference/retrieve-a-database)
- [Data source object](https://developers.notion.com/reference/data-source)
- [Query data source](https://developers.notion.com/reference/query-a-data-source)
- [Page object](https://developers.notion.com/reference/page)

Run `cargo test -p notion-knowledge-notion --locked` for mock HTTP traversal
contracts and workspace tests for CLI validation. Tests require no Notion
credentials or external account and verify actual request methods, paths,
versions, pagination bodies, scope boundaries, failure/restart semantics and
cycle termination. No live integration crawl was performed for this change.
