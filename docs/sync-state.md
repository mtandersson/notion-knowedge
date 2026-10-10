# Local sync-state database

Issue #36 adds the operational state store selected by
[ADR 0001](adr/0001-runtime-and-component-boundaries.md). Notion remains the
authoritative source; this database contains only local coordination and
derived-state metadata.

## Ownership

`notion-knowledge-core::sync_state` owns provider-independent state types and
the `SyncStateStore` port. `notion-knowledge-retrieval::sync_state` implements
that port with embedded SQLite through `rusqlite`.

The adapter does not call Notion and does not depend on LanceDB. Deleting the
SQLite file therefore cannot delete authoritative content, and opening a new
file recreates an empty operational store from migrations alone.

## Schema and migrations

Migrations are ordered, embedded SQL files under
`crates/retrieval/migrations/`. The adapter records each applied migration in
`schema_migrations` inside the same immediate SQLite transaction as the schema
change. On open it verifies that every recorded migration version and name exactly
matches the embedded migration prefix. Altered or gapped history is treated as
corrupt state, while a genuinely newer schema is rejected as unsupported.

Schema version 1 contains:

| Table | Purpose |
| --- | --- |
| `page_sync_state` | Per-page content hash, last-edit marker, and explicit tombstone state |
| `crawl_checkpoints` | Opaque cursor/checkpoint per named crawl scope |
| `webhook_events` | Event IDs only, used as a durable deduplication set |
| `index_versions` | Opaque version marker per derived index/generation |
| `schema_migrations` | Applied operational-store schema migrations |

Schema v2 adds the [reconciliation journal](reconciliation-journal.md).
Schema v3 adds `webhook_inbox`: minimized authenticated hints plus durable
processing state and claim generations. See [webhook semantics](notion-webhooks.md).
Schema v4 adds bounded webhook recovery and v5 adds durable per-page debounce.
Schema v6 adds the [owned commit coordinator](index-commits.md), trusted index
bindings, database identity, generation history and page operation receipts.
Schema v7 adds the independently queryable [derived graph edge table](knowledge-graph.md), with versioned migration history and indexes for both source and resolved target page IDs.
No Notion page body, raw webhook payload, access token, or credential is stored.

## Atomicity and deduplication

Each page-state change is one SQLite UPSERT, so a reader observes either the old
row or the complete new row; a tombstone cannot retain a content hash because
the schema enforces that invariant. Checkpoint and index-version writes use the
same atomic UPSERT pattern.

The legacy identity-only API uses the event ID as the primary key and
`INSERT OR IGNORE`. The call returns `true` only when that ID was inserted for
the first time, including across process restarts.

A five-second SQLite busy timeout bounds lock contention. The adapter uses one
mutex-guarded connection per store instance; SQLite remains the durable
transaction boundary.

## Rebuild behavior

Deleting operational state also deletes pending webhook work and deduplication
history. Authoritative page content survives, but queued notifications cannot be
recreated from this database. Drain or preserve pending work and arrange a full
authoritative reconciliation before an intentional rebuild:

1. stop users of the store;
2. delete the SQLite database and its sidecar files;
3. open the configured path again.

A coordinator-bound index also retains its database identity in the index
directory. A replacement SQLite file cannot claim that existing binding. With
all writers stopped, explicitly rebuild/reinitialize the index and its binding
as part of the same trusted recovery plan; do not remove an anchor to bypass
a mismatch during normal operation. Pending operation receipts can represent
external effects that already committed, so preserve them for idempotent replay
or use the explicit supersession policy before changing authority.

Migrations recreate an empty store without contacting Notion. A later sync can
repopulate page/checkpoint/index state from authoritative or derived sources as
the owning workflow requires.

Index-version semantics and compatibility policy remain the responsibility of
[index-versioning policy](index-compatibility.md); this ticket only provides the durable
opaque storage slot.
