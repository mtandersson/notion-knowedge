# Root-scope authorization (#306 / parent #87, #692)

Reads, search result disclosure and graph lookups are authorized by
`ReadScopeGate` (`ReadAuthority::Inventory`) in
`crates/core/src/root_scope.rs`, from the page inventory the last tree walk
recorded. It is synchronous, never calls Notion and keeps no positive
authorization cache. Staleness is bounded by reconciliation: moves and
exclusions in Notion take effect at the next walk.

Relation and link targets are resolved the same way, through the inventory's
parent map (`InventoryPlacement`). Writes run no root gate; they rely on
Notion's own permission check for the token (#691).

The live physical-ancestry gate (`RootScopeGate`, `PageLifecycle`,
`resolve_lifecycle`) was removed in #692: this is a single-user deployment
and the walked inventory is the authority.

## Authorization invariant

A page UUID, indexed source reference, a search-result `root_page_id`, a
Notion relation or a user-supplied root claim **never confer authority**.
The trusted server composition provides a canonical workspace UUID,
canonical root-page UUIDs, exclusions and a configuration generation;
`validate_scope` rejects a malformed policy.

A page is readable only when its recorded chain reaches a currently
configured root through pages and data sources none of which is trashed,
unreadable or excluded. A page absent from the inventory is denied. Errors
contain no page content, page ID, parent chain, credential or backend body.

Run `cargo test -p notion-knowledge-core --test root_scope --locked`.
