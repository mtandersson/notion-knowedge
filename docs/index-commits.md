# Owned index commits

`retrieval::commit::IndexCommitCoordinator` is the production operational API
for cooperative local index writers. Schema v6 adds trusted bindings, a random
durable database identity, generation history and payload-free operation receipts;
all earlier SQLite tables survive the atomic migration. The guarded local LanceDB adapter now exposes a concrete, opt-in complete-page
prepare/commit path. #254 and #55 remain open until the end-to-end children are
verified; legacy callers and downstream composition are not automatically fenced.

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

## Real guarded LanceDB pages (issue #257)

The `local-lancedb` + Unix `chunks::GuardedChunkTable` API is the **participating**
writer boundary; `chunks::LanceChunkTable::{upsert,apply_page_diff,create,open,open_with_policy}`
and direct FTS/vector maintenance remain legacy/uncoordinated and may NOT be
called concurrently with coordinated writers. The guard is cooperative and
does not intercept arbitrary LanceDB handles or other process code.

1. Initialize the durable `IndexCommitCoordinator` for the **existing** canonical
   index directory and trusted `CommitBinding`; obtain the live journal lease.
   `GuardedChunkTable::bind(index_dir, table_name, embedding, coordinator)`
   validates the same index and bound table. Do not use a separate database
   directory or a second coordinator with independent SQLite state.
2. On first startup call `create_empty(lease, clock)` only if no table exists.
   For an existing table, call `ensure_startup_indexes(lease, clock)`.
   Both are owned directory-locked operations and maintain FTS inside that
   lock. After an initial crawl call `ensure_vector_index(config, lease, clock)`
   to build/optimize the ANN index under the same guard.
3. Build an immutable `PageOperation` with trusted action, page, source
   revision, payload-free hash and checkpoint. Source fetch and full page
   verification must happen before preparation. Call
   `prepare_page(provider, operation, complete_chunks)` outside the guard.
   This pins an actual Lance table version, checks provider/model/vector space
   and workspace/last-edit metadata, reuses equal-hash vectors, and embeds
   only new/changed text. Empty `Refresh` snapshots remove obsolete page
   rows but still checkpoint Present; `Delete` requires a tombstone and
   empty snapshot.
4. Call `commit_page(prepared, lease, clock, async_precommit_check)`.
   The operation-owned cross-process guard reopens the actual current Lance
   table, validates its exact version (a change requires **repreparation**),
   rejects chunk-ID collisions with other pages, verifies journal
   fence/scope/binding, and invokes the async callback after embeddings.
   Callback must **actually reread** source revision, ancestry and scope and
   fail with a sanitized `FailureClass` if they changed. The callback cannot
   establish an atomic Notion snapshot; later upstream edits are handled by
   reconciliation. A rejected check produces no Lance effects.
5. Under the **same guard**, the adapter merges complete page membership or
   deletes the page and awaits lexical FTS and existing vector-index
   maintenance. Only after these external effects complete can the coordinator
   atomically checkpoint SQLite and mark its operation receipt Applied.
   Cancellation of the caller cannot drop the operation-owned lock. If an
   operation succeeds in Lance but fails at the SQLite checkpoint, re-fetch,
   revalidate and **prepare again** from the actual current table version,
   then replay the **same** operation identity under a new live fence.
   Never replay an old prepared vector/table snapshot blindly.

The `PageOperation` source revision/hash are caller-owned opaque identities.
The adapter validates structure and persisted state, not the truth of a
remote source response. An already Applied identical receipt is idempotently
acknowledged without reexecuting its callback. Callback failures and table
version conflicts leave a non-Applied receipt for explicit reconciliation.

**Participating writers:** #255 authoritative refresh should prepare from
current source and use `commit_page` with its final source/ancestry/scope
callback. #252 lifecycle deletion should prepare a tombstone, confirm
authoritative absence and commit that empty page under the same API. #242
periodic reconciliation should replay using new preparation and the current
journal fence. All must coordinate startup index maintenance and avoid direct
LanceDB writes. This integration makes the reusable index boundary available;
it does not claim those downstream orchestrators already use it, nor that
independently written legacy callers acquire locks.

Test with the real credential-free storage path:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test guarded_chunks --locked
nix develop .#spike --command cargo clippy -p notion-knowledge-retrieval --all-targets --features local-lancedb --locked -- -D warnings
```

The test suite exercises true LanceDB vector/FTS query results, SQLite
receipts, failed source checks, stale versions, empty-page deletion, counting
embeddings, provider mismatch and apply-before-checkpoint replay; subprocess
serialization must also be verified before #257 can be closed.
