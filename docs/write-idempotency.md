# Durable idempotency for agent Notion writes (#81)

This feature provides a **storage contract**, not a live write tool. The
`WriteIdempotencyStore` port is implemented by the standalone
`SqliteIdempotencyStore`, entirely separate from disposable search/index/sync
databases. Future #77–#79 and upload workflows must integrate it alongside
authorization, revision preflight and authoritative read-back.

## Protocol

1. Authorize the scope and intended target first. Compute a canonical SHA-256 of
   the *complete intended operation payload*, including options that affect the
   mutation. Construct `MutationClaim` with the **trusted server-side**
   workspace/actor scope, a fresh high-entropy request key (UUIDv4 is
   recommended), operation kind, target and payload digest. **Never put secrets
   or content into keys.** Client-supplied root IDs cannot grant a scope.
2. Call `begin` **before** sending any non-idempotent request. Only
   `ExecuteOnce` permits starting a mutation.
3. `Reconcile` means an earlier reservation is pending or indeterminate,
   including across process restarts. Do not resend a mutation. Read the
   authoritative target and resolve whether the original operation committed;
   if unknown, preserve the reservation and report the uncertainty.
4. After **verified authoritative read-back**, pass the content-free receipt to
   `record_verified`; further calls with the same key and operation return
   `Replay` with its original page identity, URL and timestamp.
5. If an upstream operation times out or its acknowledgment is ambiguous, call
   `mark_uncertain`. Even if the process crashes before this transition,
   the durable pending reservation still returns `Reconcile`.
6. Reuse of a key with another operation, target or payload is a hard
   `KeyConflict`. Do not automatically generate a new key and retry an
   indeterminate write.

The database stores only scoped key hashes, request identity hashes, state,
and after verification the page ID, URL and edit timestamp; never raw keys,
Markdown, credentials or full request bodies. A separate scope has its own
key namespace. SQLite `BEGIN IMMEDIATE` provides serialized reservation
decisions across concurrent connections and processes.

## Retention and persistence policy

**Retention is indefinite; TTL is intentionally disabled.** Deleting an
expired reservation could re-enable an old non-idempotent mutation, which is
unsafe without a durable tombstone/reconciliation policy. Do not prune rows or
automatically clear an unresolved reservation. Explicit operator maintenance
needs a separate, reviewed recovery process. Restrict the ledger and its
parent directory to the service identity, back up the database together with
its SQLite journals, and do **not** treat it as disposable retrieval state.
`initialize_new` is an explicit one-time provisioning operation and refuses
an existing file. Normal startup **must** use `open_existing`; a missing or
invalid ledger stops writes rather than recreating an empty one. On Unix new
files are created 0600. Operator-controlled private parent directories must
prevent races replacing the path with another file.

This controls **retries through this ledger**, not writes from other clients
or the read/write window in Notion. The Notion API has not been proven to
support atomic compare-and-swap; #302 remains necessary. No MCP write tool is
exposed by this change.

Run `cargo test -p notion-knowledge-retrieval --test idempotency --locked`
plus workspace formatting, Clippy, and full CI.
