# Page lifecycle effects (retired `apply_lifecycle`, #699)

`AuthoritativePageRefresh::apply_lifecycle` and its live ancestry revalidation
were removed. Lifecycle effects now come from the one sync path,
`IndexRuntime::sync_from_walk`, whose discovery decides the effect:

- `SyncMode::Full`: a complete tree walk indexes changed pages and removes
  indexed pages the walk no longer reached (moved out, trashed, revoked,
  deleted) through `AuthoritativePageRefresh::remove_unreached`.
- `SyncMode::Incremental`: pages edited since the inventory's newest edit.
- `SyncMode::Page`: one page placed through the inventory parent map, used by
  webhooks, mutation refresh and grant refresh. A page the source confirms gone
  or revoked is removed.

Allowed pages use the existing normalization, heading-aware chunking, stable
fingerprints and incremental refresh (`prepare_walked` then
`refresh_prefetched`). Staleness is bounded by the next walk; see
[the design](design/bulk-listing-sync.md) and
[manual reconciliation](manual-reconciliation.md).
