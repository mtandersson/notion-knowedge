# Read-only authoritative reconciliation planner

`AuthoritativeReconciliationPlanner` is the production-adapter dry-run API for
#342. It accepts the real `NotionClient`, `GuardedChunkTable`, shared
`IndexCommitCoordinator`, and trusted `LifecycleScope`. `production_scope`
constructs the identical root/exclusion/webhook-v1/generation identity used by
the serving webhook worker. Workspace, generation and epoch must agree.

`plan()` traverses roots with exclusions, unions indexed IDs and current-generation
receipt-owned SQLite IDs, and retains the complete binding, local snapshot and
fresh lifecycle evidence in a typed `ReconciliationPlan`. Bare unscoped SQLite
rows never confer authority. Index-only rows without matching current-generation
ownership fail closed, as do foreign workspace/root chunks or changed bindings.
Empty pages and tombstones remain visible through durable receipts.

Successful exact metadata and lifecycle inspections count distinct `scanned`
pages. Missing, stale (including citation/property/ancestry metadata), restored,
unchanged, and affirmatively validated orphan work remain distinct. The plan
reports `planned_refresh` and `planned_delete`; successful `changed` and `deleted`
are always zero. Per-page errors and run errors have separate classifications.
A failed page does not hide unrelated refresh candidates.

Absence alone never authorizes deletion. Orphans need complete traversal and
fresh affirmative inactive/outside-scope/exclusion evidence, followed by lifecycle
revalidation. Inaccessible skips, failed traversal, inspection errors, or changed
local Lance version/snapshot prevent orphan deletion. Fresh exact metadata is
read twice to reject same-timestamp property/title races. This remains a sequential
source observation, not an atomic Notion snapshot: the apply runner must revalidate
all source evidence inside the existing guarded commit and use current durable
leases. Plans are proposals, never mutation permission.

The planner never extracts content, calls embedding, changes the source/index,
or stores operational metadata/acknowledgments. Dry-run cannot suppress a later
real application. The [guarded durable runner](reconciliation-runner.md) consumes this exact plan; scheduler/CLI remains separate work.

## SQLite migration 9

`page_metadata_identity` stores a versioned SHA-256 identity of exact page metadata
(including properties) and physical lifecycle ancestry under the commit generation.
The existing guarded refresh persists this identity in the same SQLite transaction
as its checkpoint and applied receipt; deletion clears it. The content hash remains
separate. Metadata is hashed without storing private titles/properties here.
Legacy checkpoints have no fabricated identity and conservatively plan as stale;
a real authoritative refresh establishes identity, including for zero-chunk pages.

Credential-free integration verification uses fake authoritative HTTP and real
SQLite/LanceDB adapters in `authoritative_refresh`:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test authoritative_refresh --locked
```
