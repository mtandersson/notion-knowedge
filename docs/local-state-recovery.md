# Local state recovery

Notion is authoritative. This service is one process with a disposable local
retrieval cache. Recovery rebuilds derived data by syncing the configured
Notion scope. Preserving an old index generation or handing recovery authority
between processes is not a product requirement.

## Current support and remaining work

Production webhook indexing, manual reconciliation and periodic reconciliation
exist. The shared startup recovery path rebuilds a missing or incompatible
cache before composing the sync worker or installed integration-backed readers.
Use [cache reset and resync](cache-reset-and-resync.md) for detection, preserved
state, configuration, command and failure behavior. `--index rebuild` remains
ANN-only maintenance; `--index reset` performs authoritative reconstruction.

The [storage inventory and reset contract](cache-reset-and-resync.md) defines
what reset must discard and preserve. #578 removes obsolete recovery machinery;
#579 replaces its large crash-test matrix with bounded sync acceptance.

## Disposable and protected state

| Storage | Recovery policy |
| --- | --- |
| Canonical local chunks, embeddings, FTS/ANN indexes, graph and alias projections | Rebuild from authorized Notion content with compatible configured assets. |
| Page hashes, cache bindings and index/sync bookkeeping | Invalidate assumptions about discarded rows; old acknowledgements cannot prove a replacement cache is complete. |
| Webhook inbox and admitted refresh work | Preserve or reconcile through authoritative sync without repeating upstream mutations. |
| Notion/MCP credentials, encrypted grants and keys | Protect separately; never place them in the disposable index tree. Losing access material requires the supported reauthorization path. |
| External mutation/idempotency records, audit and uncertain write/upload effects | Preserve; resetting derived data must never replay a Notion write or attachment. |
| Embedding and lexical assets | Verified local reconstruction inputs; retain identity and attribution. |

Operational SQLite currently mixes derived projections with admitted work.
Do not delete that entire database to clear search. Corrupt security or external
mutation state must surface an error, rather than silently becoming an empty
ledger. See [grant-store recovery](notion-grant-store.md).

## Supported recovery contract

1. Detect missing, incompatible or unusable derived cache. Coordinate readers
   and writers within the service; expose unavailable/rebuilding status.
2. Reset derived data and invalidated bookkeeping through the supported API.
   Retain credentials and external-effect safety records.
3. Inventory the configured authorized workspace, roots and exclusions; read
   current Notion content and regenerate chunks, embeddings and retrieval
   structures using compatible configured identities.
4. Validate the rebuilt cache and resume serving and incremental sync. Failed
   or interrupted rebuilding can discard partial derived output and resync.

Do not infer deletions from incomplete discovery or permission failures. Missing
assets, authorization failures and transient upstream errors require bounded
retry or configuration repair; they are not proof of cache corruption. No
rollback to historical local data is needed to reconstruct authoritative state.

Use the supported reset API or `--index reset` with the service stopped. Avoid
manually bypassing binding guards or deleting operational SQLite. Report the
actual unavailable condition and keep protected stores intact.

## Backup of protected operational state

Back up credentials and their encryption keys through the private host backup
workflow, with separate access control and no plaintext logging. Stop and drain
the service before maintenance of operational databases. Use SQLite's backup API
for consistent database snapshots, including committed WAL data; copying a live
SQLite file alone is insufficient. Verify backup integrity and permissions in an
isolated destination. Never overwrite the only live copy with an old snapshot.

Restoring an old mutation ledger can make already performed Notion effects
appear absent. Resolve that external-effect uncertainty before accepting writes;
a search-cache resync does not repair a rolled-back idempotency ledger. A backup
of derived retrieval is optional and is not an authority or recovery prerequisite.

## Acceptance evidence

Use small deterministic Notion and embedding fixtures through the production
startup/sync seams: create a cache, delete it, restart and verify reconstructed
scoped results; repeat with unusable derived metadata and a transient sync
failure. Check unavailable search during rebuilding and subsequent incremental
updates. Verify preserved credentials/ledgers and zero upstream writes/uploads
from recovery. The two bounded cache-resync tests in `server/tests/reconciliation_command.rs`
exercise the shared production startup seam. Current-head CI results are
recorded in #577; they do not establish deployed or real Notion acceptance.

Actual deployed resource/overload recovery remains #455/#416; real Notion and
ChatGPT smoke remains separately controlled by #110/#109. Component/spike checks
are useful evidence for their own scope, not production reset/resync acceptance.
