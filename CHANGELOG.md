# Changelog

Notion Knowledge follows the release policy in [docs/releasing.md](docs/releasing.md).
Entries describe operator-visible changes, including MCP tool contracts and
local index/operational-state compatibility. `TBD` is a **draft**, not a published release.

## [Unreleased]

### Added
- `knowledge_get` with `data_source_id` alone lists the saved views (id, name, type) of
  an in-scope data source, so a client can find a `view_id` (#839). The tool
  definition grows by about 330 B.
- Release checklist and machine-checkable changelog/version guard.
- Periodic cache-sync progress logs include `rss_kib` and `rss_peak_kib`.

### Fixed
- Hybrid search ranks a page whose title equals the whole query first; plain RRF
  could bury it when only the lexical path found it (#842). Only the page's best
  chunk is promoted, also with graph boost; scores stay RRF, so they need not
  decrease down the list.
- A saved-view read with an unknown `fields` entry answers `invalid_input: unknown_field`
  and lists the view's column names, instead of a misleading UUID/continuation
  error (#838).
- A 401 on a page or row write is reported as a definite `not_authorized` refusal
  (`reason: unauthenticated`) and releases the idempotency key, like a 403 (#832).
- Post-write verification ignores blank lines between Markdown blocks (outside code
  fences), because Notion drops them when it persists a page. A ChatGPT row create
  with a blank line after a heading used to be reported `outcome_unknown` although
  the row had been created (#823).
  Block structure expressed only by blank lines (loose versus tight lists, paragraph
  breaks) is no longer verified; content and order still are.
- A row create through `knowledge_create_page` no longer requires `properties`
  (a row with only a title and Markdown is valid); omitting it used to fail with
  `invalid semantic mutation arguments` (#823).
- Row creates and updates that Notion definitely refuses (400, 403, 404) now
  report the reason and release the idempotency key instead of staying
  `outcome_unknown` (#831, follows #826). The rejected or unknown stage is logged.
- Long syncs no longer grow resident memory with pages processed: the server
  caps glibc malloc arenas at 2 at startup (#656). Set `MALLOC_ARENA_MAX` to
  override.

### MCP tool-schema changes
- Added read-only `knowledge_sync_status` with scoped webhook counts, persisted index/model/schema identity and durable successful reconciliation time.

### Index/state migrations
- Additive SQLite schema 13 stores scoped authoritative reconciliation success receipts. Earlier completions remain unknown; status inspection never migrates state. Back up durable state before a writer upgrades it.
- Published Candle core/NN 0.11.1 replaces the temporary ZIP backport. Production
  Qwen, isolated Qwen/Lance spike and E5 evaluation vector identities change;
  previous runtime indexes are incompatible and require an explicit rebuild
  from authorized source content. Do not relabel old vectors or remove protected
  SQLite, credentials or mutation receipts. This change adds no SQLite migration.

## [0.1.0] - TBD

### Added
- Initial local-first MCP server bootstrap, diagnostics and retrieval contracts.

### MCP tool-schema changes
- Initial `knowledge_search` and `knowledge_get` MCP tool contracts. Review the actual
  exposed schema at release time; staged or unavailable adapters are not production features.

### Index/state migrations
- No general-purpose upgrade migration is provided for the bootstrap. Derived
  LanceDB/search data must be considered rebuildable; durable SQLite operational
  state must be backed up and restored according to its own compatibility contract.
  Do not delete state or perform an incompatible downgrade without a recovery plan.
