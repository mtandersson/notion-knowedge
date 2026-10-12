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

## Guarded runner and persisted explicit retries (schema 11)

The [authoritative runner](reconciliation-runner.md) now supplies the real shared
source/Lance effects, owned precommit proposal validation and receipt recovery.
`retry_reconciliation_proposal` atomically persists a fresh payload-free
`DurablePage` proposal with Failed-to-Pending, retaining the stable operation ID.
`reconciliation_page_proposal` returns that explicit proposal after reopen; the
runner validates it under the actual owned guard before effects. It is evidence,
never authorization. This closes the cancellation interval before preparation.
Schema 11 preserves all existing journal/plans/receipts in an atomic migration.

## Scoped schedule authority (schema 12)

Schema 12 adds `reconciliation_schedules`, append-only schedule events and failure
history to the same operational SQLite database. Upgrade is one transaction;
existing plans, preparations, page receipts, inboxes and checkpoints survive.
Current-schema read-only inspection still verifies the entire migration ledger,
opens SQLite read-only/query-only, and rejects older schemas and WAL before any
migration or auxiliary-file creation. Back up operational state before upgrading.

The supported storage API is `retrieval::schedule` on the **same**
`IndexCommitCoordinator` used by manual/webhook execution. It validates the
trusted physical index/state identity and persisted binding on each call. The
schedule stores table/workspace, the complete `production_scope` root/exclusion
and `webhook-v1` policy digest, generation and epoch. Neither a persisted schedule
nor its run ID can initialize source/index authority. Caller-supplied binding
comes from validated composition. The API does not start periodic work; #353
owns opt-in server configuration, dispatch, source timeouts, lease renewal and
service shutdown.

Every mutation takes the existing global `Lease` plus a nonnegative Unix time in
**seconds**, and commits in a SQLite IMMEDIATE transaction. The production clock
must not move backwards across holders/restarts. `SchedulePolicy::new` validates
interval 60–2,592,000 seconds (30 days), 1–16 failure attempts, initial backoff
1–86,400 seconds and maximum backoff between initial and 604,800 seconds (7 days).
Deadline overflow is rejected before mutation. Schedule/run/failure IDs use the
journal's bounded identifier validation. Root/exclusion sets are each bounded to
1,000 IDs, with 4,096 bytes per input. Inspection uses bounded pagination (1–1,000).

A minimal caller sequence, after composing trusted runtime and acquiring the
common writer lease, is:

```rust,ignore
use notion_knowledge_retrieval::schedule::SchedulePolicy;
let policy = SchedulePolicy::new(3600, 4, 5, 300)?;
let schedule = coordinator.register_schedule(&lease, now, "periodic", policy, now)?;
for due in coordinator.due_schedules(now, 100)? {
    let claimed = coordinator.claim_schedule(&lease, now, &due.schedule_id, "unique-new-run")?;
    let run = claimed.active_run.as_deref().expect("claimed run");
    // Invoke the actual shared runner's apply_next(run, lease, clock, provider).
    // Renew only between returned inventory/page attempts; never extend an
    // already submitted owned operation. Hold the shared external commit guard
    // through index effects, as the real runner already does.
}
```

Registration is idempotent under identical binding/policy and preserves the
persisted deadline, active run and attempt count even if a caller proposes a new
first deadline. A changed interval/retry policy or authority returns
`ScopeMismatch`; it never resets state. Due selection is advisory and scoped to
the exact binding. Claim checks the deadline again, atomically checks global
manual-run exclusivity and creates one new run, or rebinds the **stored** active
run to the current fence. A proposed new ID is ignored on resume. Existing run
IDs cannot be reused. Claim also tries the common index-directory lock so an
owned operation still running after lease expiry yields `Busy`. Busy does not
change deadlines, counts, inventory or failure budgets. Manual apply continues
to reject another unfinished scheduled run. Cancellation records a checkpoint
with `checkpoint_schedule` and preserves the same run/deadline/budget; the real
runner owns durable inventory and page checkpoints.

Successful `ReconciliationJournal::complete` and the actual runner's
`complete_reconciliation_plan` advance the schedule to `now + interval`, clear
its active identity and reset the current failure budget **in the same
transaction as run completion**. The completed run records that deadline.
Rollback leaves both run and schedule active. Restart therefore cannot repeat
completion or lose deadline progression. Completed run bytes/receipts remain
immutable: even `start_or_resume` no longer rebinds a completed run's fence.

The failure policy is explicit:

- `fail_schedule` records a sanitized run-level class with a stable failure ID.
  Duplicate reporting of that ID/class cannot consume another attempt; a
  conflicting class is rejected. Each retryable failure increments the budget
  and persists `now + min(initial * 2^(attempt-1), maximum)`. A subsequent attempt
  cannot start before that deadline. At the configured attempt limit status is
  `Exhausted`, excluded from due selection until operator recovery.
- Failed page work or immutable planning failures enter `HeldPages` through
  `hold_schedule`, failure reporting, or completion. They consume no run-failure
  budget and never become pending implicitly. The current active identity stays
  inspectable. A real page retry must use the existing explicit manual runner
  retry, which freshens and validates the proposal. `retry_schedule` then permits
  retry only if no failed/planning work remains and the run is still incomplete.
  It explicitly resets the budget/deadline, retains the run and records recovery.
- Immutable planning failures, completed runs containing failures, or work the
  operator chooses not to retry require `abandon_schedule`. It retains all old
  inventory/plans/failure evidence/receipts, marks an incomplete run `Abandoned`,
  and sets an explicit next deadline for a fresh cycle. A completed failed run
  stays completed unchanged. An abandoned run cannot resume. The next due claim
  requires a new unique run ID and fresh authoritative planning. Abandonment
  checks the external writer lock and refuses pending submitted receipts with
  `Busy`; recover their owned operation before abandoning. This does not claim
  cancellation or rollback of an external write. Successful abandonment consumes
  the lease (expires it atomically), fencing queued callers that have not begun
  effects; reacquire a new lease before a new cycle or authority transition.
- To change source policy/generation/epoch, first recover/abandon an old active
  cycle under its **old trusted binding**. Use the coordinator's explicit
  generation/scope transition, including existing unresolved-receipt recovery.
  Then call `reconfigure_schedule` with `ScheduleReconfiguration` containing the
  exact previous `CommitBinding`, new policy and explicit deadline. It rejects
  any active run and any mismatch in the previous binding. Events/history survive
  the replacement. Changing interval alone uses the same explicit API between
  cycles. A configuration restart never silently adopts changed authority.

`schedule`, `schedule_events` and `reconciliation_failure_evidence` provide
sanitized, paginated operator inspection: status, policy, deadline, active run,
cycle, attempts, event times and enum failure classes. SQLite triggers preserve
historical classified page/run failures when explicit retry clears current
failure or later success completes a run; immutable planner failure documents
remain available too. These APIs never store credentials, source bodies, titles,
URLs or upstream error text. IDs and timing remain operational metadata and must
not be logged on public surfaces. #353 must expose meaningful operator status
and explicit recovery using these exact APIs, and prove the actual scheduled
HTTP/SQLite/Lance lifecycle; storage tests alone do not close #350/#243/#58.

Verification covers real SQLite migration/reopen/rollback, two connections,
manual and external-writer contention, same-run checkpoint recovery, due/not-due,
atomic actual-plan completion, bounded backoff/exhaustion, held failures,
immutable receipts, explicit authority replacement and read-only manual commands:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test schedule --test sync_state --test reconciliation --test durable_plan --test authoritative_refresh --test index_commits --locked
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command --locked
```
