# Changelog

Notion Knowledge follows the release policy in [docs/releasing.md](docs/releasing.md).
Entries describe operator-visible changes, including MCP tool contracts and
local index/operational-state compatibility. `TBD` is a **draft**, not a published release.

## [Unreleased]

### Added
- Release checklist and machine-checkable changelog/version guard.

### MCP tool-schema changes
- Added read-only `knowledge_sync_status` with scoped webhook counts, persisted index/model/schema identity and durable successful reconciliation time.

### Index/state migrations
- Additive SQLite schema 13 stores scoped authoritative reconciliation success receipts. Earlier completions remain unknown; status inspection never migrates state. Back up durable state before a writer upgrades it.

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
