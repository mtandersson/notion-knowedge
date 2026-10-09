# Owned index commits

`retrieval::commit::IndexCommitCoordinator` is the production operational API
for cooperative local index writers. Schema v6 adds trusted bindings, a random
durable database identity, generation history and payload-free operation receipts;
all earlier SQLite tables survive the atomic migration. This API does not yet
wrap LanceDB: #257 must integrate real prepare/commit, FTS/vector maintenance,
replay and search checks. #254 and #55 remain open until their children deliver.

## Trusted authority and serialization

Explicit `initialize(existing_index_directory, state_file, CommitBinding)`
creates or verifies the operator's table, workspace, scope and generation.
`open` requires an existing identical binding. One operational database owns one
index directory/table/workspace. IDs come from trusted configuration, never
webhook payloads. The binding includes the canonical directory and SQLite
**device/inode** identities. A durable random SQLite identity also prevents
inode reuse from making a newly created database appear to own an old index.
The index's `.nk-operational-binding` file pins that database identity. A second
state database cannot initialize independent authority for the same index.
Symlink and relative aliases resolve to the same index inode. SQLite hard links
are rejected because SQLite journals use database pathnames. Directory and
state-file replacement are rejected before effects and checkpointing.

The advisory lock is on the **index directory inode itself**, using standard
`File::lock`, with a separate file description for every operation. There is no
replaceable lock file and no lease expiry that releases an active guard.
Initialization, scope changes and effects all acquire that same guard. SQLite
IMMEDIATE transactions protect durable commands; their mutex is released before
asynchronous consumer callbacks. The tested platform is Linux on a local
filesystem supporting advisory directory locks. Unsupported locking or I/O
fails closed. Network filesystems, cross-host writers and multiple independently
created directories for the same physical backend are not supported.

All writers must participate: refresh, lifecycle deletion, reconciliation and
rebuilds acquire the same directory guard. They also use the same database-wide
`ReconciliationJournal` lease. Under the guard, the coordinator validates the
**current persisted** fence/expiry, trusted binding and any active run's scope
and fence. A forged/stale `Lease.expires_at` cannot extend authority. Consumers
call `CommitContext::revalidate()` after source preparation and immediately
before each external effect. The guard serializes effects; the lease determines
whether an effect/checkpoint may start. Expiry cannot undo an already running
external effect. A newer lease holder must wait for the guard before effects.

Scope changes use `change_scope(next, lease, now)` with the current coordinator
as a compare-and-change token. Table/workspace remain fixed. Configuration epochs
are nonnegative signed-64-bit integers and must strictly increase; opaque index
generation identities can never be reused. Old coordinators fail validation.
Finish active reconciliation runs first. Ordinary changes require all receipts
resolved. For permanently failed/ambiguous work, `supersede_scope` requires the
**exact sorted-equivalent set** of unresolved operation IDs; it atomically changes
authority and marks those receipts **Superseded/Conflict**, never Applied. This
is explicit abandonment, not proof of effects or an index rebuild. The new
scope must reconcile source and index with new operation IDs before readiness.

These are cooperative protections, not a security boundary against a local
operator replacing directories, editing SQLite or the anchor file, or issuing
raw LanceDB writes. Existing `SyncStateStore` mutators and uncoordinated LanceDB
callers do not acquire the guard automatically. Keep state/index directories
owned and protected; moving/copying/rebuilding storage requires explicit trusted
reinitialization/reconciliation. Do not delete the binding file to resolve a
normal configuration mismatch.

## Owned operations and crash windows

Construct a `PageOperation` from an immutable operation ID, prepared revision
identity, action (Refresh/Delete/Unchanged) and `PageSyncState` checkpoint. IDs,
revision and hash fields contain operational identities, never page bodies,
credentials or raw error text. An operation ID binds **all** of these fields and
the generation. Reusing it for a different effect fails before the callback.

`submit(operation, lease, clock, effect)` eagerly starts a dedicated owning
thread with its own Tokio runtime and returns an observer future. The effect is
`Send + 'static` and receives a `CommitContext`. It must await every external I/O
operation it starts and report a sanitized Source/Index/Conflict/Unavailable
failure. It must implement idempotent stable-page replacement/deletion; generic
callbacks do not themselves enforce source validity or LanceDB idempotency.
The clock supplies nonnegative monotonic scheduler seconds consistent with the
journal across processes/restarts. Queuing does not snapshot authority: validation
uses the clock when the guard is actually acquired and again at checkpoint.

The owning thread holds serialization through callback completion, runtime
cleanup and the SQLite checkpoint, even if the observer is never polled, aborted
or dropped, or the caller's Tokio runtime shuts down. Observer cancellation is
not operation cancellation. Callback resources tied to a shutting-down caller
runtime may fail; the effect must surface that failure, never claim success.
The dedicated runtime is not aborted by that caller shutdown. Callers must bound
submission/concurrency; each submission owns a thread and can wait for the guard.
A callback that never completes deliberately retains serialization. Detached
external tasks/processes that outlive the callback violate this API contract.

After guard/fence validation, a **Pending** receipt commits before effects.
Only callback success followed by current fence/binding validation can atomically
write the page checkpoint and mark the receipt **Applied**. An already Applied
identical receipt returns AlreadyApplied without running the callback. A callback
failure records **Failed** with a sanitized class and no successful checkpoint;
explicit resubmission of the identical operation retries it. Attempts count
owned executions, including replay. The callback cannot publicly mark its receipt
Applied. Its context may read/revalidate SQLite without deadlocking a held mutex.

If effects commit but the lease expires, SQLite checkpoint fails, a callback
panics, or the process dies, no Applied receipt is fabricated. Pending means
**effects may already have happened**. A fresh owner replays the same idempotent
operation before checkpointing. Killing the process releases the OS lock;
no guarantee is made that an already committed external effect is rolled back.
SQLite and the index do not share a transaction. Persisting a receipt before
effects and a checkpoint after them does not claim cross-database atomicity.

Initialization publishes a fully synced binding file without replacing an
existing one, then commits the SQLite binding. A crash before SQLite commit can
be retried using the same database/identity and trusted configuration. A crash
before anchor publication may leave an inert `.nk-binding-*` temporary file;
a crash between publication and unlink may leave its hard link. Such interrupted
initialization fails closed until an operator, with all writers stopped, verifies
and removes only the matching temporary artifact. No successful index operation
or authority is inferred from a temporary file.

## Verification

No credentials, source calls, embeddings or LanceDB feature are needed:

```sh
nix develop --command cargo test -p notion-knowledge-retrieval --test index_commits --locked
nix develop --command cargo test --workspace --locked
nix develop --command cargo clippy --workspace --all-targets --locked -- -D warnings
```

Real SQLite tests cover v1 migration/reopen, aliases, different databases,
hard links, replaced paths, scope/generation changes, actual journal fencing,
callback revalidation, checkpoint-trigger rollback, durable replay, failure
classes and exact supersession. Independent child processes reach a recorded
lock-attempt boundary before blocked assertions, then prove serialization through
delayed effects, polled observer cancellation, caller runtime drop and lease
expiry. A killed owning process leaves durable Pending work that replays after
reopen. These prove the operational coordinator contract; #257 supplies the
separate actual LanceDB mutation/search acceptance evidence.
