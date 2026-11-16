# Scoped Notion discovery: the tree walk

`NotionClient::walk_tree` (`crates/notion/src/walk.rs`) discovers scope by
walking down from the configured root. A page is in scope if and only if the
current walk reached it. See `docs/design/bulk-listing-sync.md` for the design.

Run a read-only dry run with an integration credential supplied through the
secret process environment:

```sh
cargo run -p notion-knowledge-server -- --crawl-dry-run ROOT_PAGE_ID
```

Set `NK_NOTION_AUTH=integration` and supply `NOTION_TOKEN` through your existing
secret environment. The command prints the complete walk as JSON after it
succeeds: page ids, parents, `last_edited_time`, trash state, typed skips, the
`complete` flag and the request count. It prints no titles or content, neither
starts MCP nor writes an index, and failures print only a sanitized class.

## How the walk reaches pages

- Each non-row page costs one `GET pages/{id}` (metadata and physical parent)
  and one `GET pages/{id}/markdown`. Child pages come from `<page url=…>` tags
  and databases from `<database url=…>` tags in that Markdown. Tags inside code
  or escaped are ignored.
- Ids listed in `unknown_block_ids` are read through the same Markdown endpoint
  so their child tags are still reached. An unreadable block makes the walk
  incomplete.
- A database costs one `GET databases/{id}`; each data source is listed with
  paginated `POST data_sources/{id}/query` (100 rows per call). Rows take their
  metadata from the query result and cost one Markdown read each.
- A tagged page or database whose physical parent is not the tagging page
  (through any nesting blocks) is skipped as `outside_scope`. Links, mentions,
  relations and synced-block references are never followed, and search is
  never used.
- Each id is reached once and the first parent wins. The walk fails closed at
  100,000 nodes and runs 8 reads at a time through the shared rate limiter.

## Skips, exclusions and completeness

Every reached page is one inventory entry. A 404/403 child is recorded as
`inaccessible`, a trashed page as `archived`; an unreadable configured root
fails the walk. Page, database and data-source exclusions prune the subtree
before any content read. `descendants_of` keeps the named page and prunes
everything below it.

A repeated or missing data-source cursor, a truncated body or an unreadable
unknown block marks the walk incomplete. Only a complete walk replaces the
persisted `page_inventory` table or lets the planner propose deletions; an
incomplete walk only upserts what it reached.

## Page inventory

The `page_inventory` SQLite table (migration 20) keeps every reached page with
its parent, root, `last_edited_time`, trash state and outcome (`indexed`,
`skipped` with a typed reason, or `failed`). It holds no titles or content. It
is disposable: dropping it is recreated empty on open and only forces the next
full walk. Initial sync and cache resync persist it with each page's outcome.
