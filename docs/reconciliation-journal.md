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
They must also hold an external commit fence/serialization guard through the
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
