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

## Exclusions and ignore rules

Pass repeated `--exclude-page ID`, `--exclude-descendants ID`, and
`--exclude-source-type page|database|data_source` options after
`--crawl-dry-run`, together with explicit roots. IDs accept the same normalized
Notion IDs/links as roots. For example:

```sh
cargo run -p notion-knowledge-server -- --crawl-dry-run ROOT_PAGE_ID \
  --exclude-page PRIVATE_PAGE_ID --exclude-descendants METADATA_ONLY_PAGE_ID \
  --exclude-source-type database
```

`ExclusionRules` is the provider-independent core policy, applied by
`crawl_with_exclusions`. Page exclusions prune the page and its whole physical
subtree. Descendant rules retain the named page's metadata but never enumerate
its children. Source types describe physical Notion objects: `page` excludes
all pages, `database` prunes databases and their rows, and `data_source` prunes
source queries and their rows. They are not file extensions or block types.
Rules combine by exclusion; no root or allow rule overrides them. Additional
explicit roots under an excluded ancestor are excluded too. Roots must still
resolve, including excluded roots. When rules require ancestor checks, denied,
missing, cyclic, archived or malformed ancestry fails closed with no report.

Dry-run reports use `excluded_page`, `excluded_descendants`, and
`excluded_source_type` reasons. A pruned container accounts for its whole
subtree; individual unknown descendants cannot be enumerated without reading
excluded content. Existing `outside_scope` reasons also cover wiki sources
whose ancestry crosses an exclusion boundary. Reasons include IDs only, never
excluded titles/content. Known excluded descendant objects are pruned before
fetching their content. Listing authorized parents can necessarily expose child
IDs or metadata returned by Notion; these are never included as allowed pages.

The completed report's `pages` is the only current indexing handoff: excluded
pages cannot reach downstream extraction, chunking or embeddings through it.
There is currently no embedding/index writer. Future writers must consume a
fresh report from `crawl_with_exclusions`, accept only its page IDs before any
content fetch or embedding call, and replace their previous scope snapshot
(including deleting formerly allowed pages when rules change). Raw backend
reads and `crawl_roots` (the explicitly empty-rule compatibility API) do not
apply a separate configured policy automatically. A failed run provides no
handoff; callers must not treat it as authorization to reuse old scope. The
same concurrent-edit/snapshot limitations above apply to exclusion ancestry.

For a selected page's current physical upward ancestry and safe lifecycle
removal candidates, use [authoritative lifecycle evidence](notion-lifecycle.md).
A missing entry in this downward report never proves deletion or out-of-scope
status. The lifecycle port supplies separate source/scope revalidation evidence.
