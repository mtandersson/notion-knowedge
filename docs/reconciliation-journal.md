# Durable reconciliation journal

`core::reconciliation::ReconciliationJournal` is the operational port for #58.
`SqliteSyncStateStore` implements it in the existing operational database. Schema
v2 upgrades v1 atomically and preserves page state, checkpoints, webhook IDs and
index versions. This change supplies storage; #242 implements source/index
coordination and #243 supplies manual and periodic execution.

A scope hashes length-prefixed, sorted, deduplicated root and exclusion sets,
plus explicit policy and index-generation identities. IDs must already be
canonical authoritative IDs (the journal trims surrounding whitespace but does
not parse Notion URLs or UUID spelling). Policy must include all behavior that
changes inventory or application, including dry-run where applicable. A run ID
cannot resume under another scope. One unfinished run exists database-wide;
changing scope requires finishing the existing run, rather than silently
reinterpreting its work. No credentials, source bodies, or raw errors are stored.

Inventory batches contain only page ID, revision identity and planned action:
refresh, unchanged, or deletion of a scoped local candidate confirmed absent by
a complete authoritative inventory. Delete rows are local candidates; other
rows are authoritative inventory. The coordinator owns this classification and
must never seal an incomplete inventory or infer deletion from failed reads.
Duplicate identical rows are replay-safe; conflicting revision/action rows fail
the entire batch. Inventory rows and checkpoint commit together. Sealing makes
the inventory immutable and enters application. Work is pending, applied or
failed; failure remains until explicit retry. Generic failed acknowledgments
record the sanitized Index class; run failures also distinguish Source, Conflict
and Unavailable, including during inventory. Recording failure preserves phase,
work and retry deadline. Completion requires every row applied.

Counts are derived transactionally from durable rows, rather than incremented
counters: inventory, pending/applied/failed and applied refresh/delete/unchanged.
Acknowledging the same outcome twice cannot double count. Failed work cannot
become applied without explicit retry. Successful work stays acknowledged.
Next scheduled deadline is durable on completion and on recorded failure.

All mutations use SQLite IMMEDIATE transactions. A database-wide lease lasts
1–3600 seconds, with caller-supplied nonnegative scheduler time. Renewal preserves
the fencing number; release/expiry and reacquisition advance it. At expiry the
old holder cannot renew, acknowledge, or mutate. A new holder must scope-check
`start_or_resume` to bind the run to its fence before any run mutation. Scheduler
time must be monotonic across processes and restarts; test clocks are explicit
and no journal method sleeps. Readers obtain transactionally consistent snapshots.

## Concurrent index changes and crash recovery

This is a cooperative orchestration lease, not automatic isolation of arbitrary
LanceDB or existing `SyncStateStore` callers. Future webhook writers, index rebuilds
and reconciliation must acquire this same global lease and revalidate the actual
index generation against the scope before resume and each external mutation.
The [owned index commit coordinator](index-commits.md) now supplies the external
serialization guard and trusted binding API; concrete LanceDB integration remains
#257. They must hold that guard through the
index write: expiry cannot cancel a write already in flight in another database.
The journal rejects stale acknowledgments, but cannot undo that external write.
The runner must stop applying when renewal or generation validation fails.

The index operation commits **before** SQLite acknowledgment. A crash in between
leaves the row pending. Resume replays the same page revision and action under
a new fence, then acknowledges. Index operations must therefore be idempotent
stable-page replacement/deletion with generation checks (the page-diff contract),
not append-only inserts. No cross-database atomicity is claimed. Failed SQLite
writes roll back their complete command and preserve earlier successful work.

## Verification

Use the pinned standard development shell; no Notion token, model, or network
source is required by these tests:

```sh
nix develop --command cargo test -p notion-knowledge-retrieval --test reconciliation --locked
nix develop --command cargo test --workspace --locked
nix develop --command cargo clippy --workspace --all-targets --locked -- -D warnings
```

Tests exercise real v1 migration/reopen, scope changes, cross-connection lease
contention/expiry/fencing, batch conflict and injected SQL write failure rollback,
and crash after idempotent index application before acknowledgment with durable
failure/retry/count consistency.

## Authoritative plan contract (schema 10, version 1)

With `local-lancedb` enabled, `IndexCommitCoordinator` exposes the runner-facing
`durable_plan` API. `record_reconciliation_plan(lease, now, run, &planner_plan)`
converts the real planner result, records it and seals inventory in a single
IMMEDIATE SQLite transaction. Start/resume the run first with the existing
`ReconciliationJournal::start_or_resume` and the shared `production_scope`.
The additive migration preserves legacy journal, sync-state and commit receipts;
legacy acknowledgment/completion cannot bypass the new contract for these runs.

`reconciliation_plan(run)` opens only against the current trusted coordinator
binding and physical index/state identity. Persisted bytes cannot initialize
index authority. Version 1 retains table/workspace, the complete root/exclusion/
policy scope digest, generation/epoch, index snapshot version, traversal
completeness, distinct successful inspected IDs (including no-work pages),
proposed difference, metadata identity, lifecycle and local snapshot digests,
per-page planning failures, and separately classified run failures. Source bodies,
private property values and credentials are not persisted. Digest algorithms are
explicitly versioned; changing their encoding requires a new contract version.

Each work operation has a stable `reconcile-v1-…` identity derived from the run,
full binding, page, proposed difference and source/local provenance. Exact plan
replay is harmless; conflicting plan replay fails without changing progress.
Replanning a whole inventory requires a new run and fresh authoritative reads.
An operation may change its current action only through explicit per-page
replanning before a successful receipt. This preserves prior successes and
prevents a racing restore from being counted as deletion.

The application handshake is:

1. Reopen with operator-supplied trusted binding, acquire/resume the common lease,
   and load plan/progress. Failed pages require `retry_reconciliation_page`.
2. Prepare from fresh source/lifecycle and local state, compare
   scope/generation/epoch and provenance, and reject conflicts. Repeat these
   checks inside the shared owned index commit boundary before actual effects.
   A persisted digest is a comparison target, **never** deletion authority.
   Delete requires complete traversal plus fresh affirmative inactive/outside-scope
   evidence. A candidate now Allowed requires explicit replanning to refresh;
   do not apply its original deletion. Revalidate source metadata as well as
   lifecycle and local state before effects. Version 1 exposes the digest helpers
   for comparing freshly read planner-local/lifecycle snapshots.
3. Call `prepare_reconciliation_operation` with the stable ID, exact prepared
   checkpoint/revision/action and freshly scoped evidence. Without explicit
   replanning, action and lifecycle digest must match the plan. Preparation is
   fenced and persists the exact receipt target. It does not mutate LanceDB and
   does not replace the runner's fresh authorization or external commit guard.
4. Submit the real, idempotent operation through `IndexCommitCoordinator` and
   await its real Applied/AlreadyApplied receipt. Apply precedes acknowledgment.
   Source revalidation and actual LanceDB mutations are supplied by #346.
5. Call `acknowledge_reconciliation_receipt`. It transactionally checks current
   authority/fence and the real Applied receipt's operation/page/generation and
   exact prepared revision/action/content hash/time before recording success.
   Missing/Pending/Failed/Superseded or mismatched receipts cannot acknowledge.
   Crash after apply but before acknowledgment retains a pending journal item and
   an immutable Applied receipt. Reopen/replay/ack cannot repeat its count.
   `prepared_reconciliation_operation(run, page)` recovers the exact persisted
   revision/action/hash/time after reopen. New source changes cannot replace an
   Applied preparation or reinterpret its outcome. Validate current trusted
   authority, acknowledge the immutable historical receipt, and process a newer
   source edit in a new plan/run. If replaying submit, reconstruct its original
   persisted target rather than submitting the newer source checkpoint.

`reconciliation_progress` derives counts from distinct persisted IDs and current
statuses: scanned is successful inspections; planned refresh/delete are separate;
changed/deleted/unchanged reflect **actual receipt action**, not proposed action.
Page planning/application failures remain independently classified, and
`failed_pages()` deduplicates their page IDs. Run failures use stable caller-owned
failure IDs and retain their own classifications. Duplicate failure recording
cannot inflate counts; a conflicting class for the same ID fails. Successful
items cannot be failed/retried; failed items cannot be acknowledged until explicit
retry. Retry clears the current failure while preserving other work and its
success counts. `complete_reconciliation_plan` requires no pending items and
retains failed items; after completion, a new run must replan them.

Dry-run remains the exact read-only planner and writes **no** plan, progress,
operational metadata, or application acknowledgment. Recording a durable plan is
an explicit real-run operation.

Verification: `cargo test -p notion-knowledge-retrieval --features local-lancedb
--test durable_plan --locked` exercises real SQLite transactions, independent
connections, reopen, schema migration, classified summaries, exact duplicate
replay, explicit retry, stale lease/fence and scope/generation rejection,
transaction rollback and real coordinator apply-before-ack receipt recovery.
These storage tests perform no LanceDB mutation; the integrated source/index
race and application matrix belongs to #346.
