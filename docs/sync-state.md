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

No Notion page body, webhook payload, access token, or credential is stored.

## Atomicity and deduplication

Each page-state change is one SQLite UPSERT, so a reader observes either the old
row or the complete new row; a tombstone cannot retain a content hash because
the schema enforces that invariant. Checkpoint and index-version writes use the
same atomic UPSERT pattern.

Webhook deduplication uses the event ID as the primary key and
`INSERT OR IGNORE`. The call returns `true` only when that ID was inserted for
the first time, including across process restarts.

A five-second SQLite busy timeout bounds lock contention. The adapter uses one
mutex-guarded connection per store instance; SQLite remains the durable
transaction boundary.

## Rebuild behavior

The operational store is intentionally disposable:

1. stop users of the store;
2. delete the SQLite database and its sidecar files;
3. open the configured path again.

Migrations recreate an empty store without contacting Notion. A later sync can
repopulate page/checkpoint/index state from authoritative or derived sources as
the owning workflow requires.

Index-version semantics and compatibility policy remain the responsibility of
[index-versioning policy](index-compatibility.md); this ticket only provides the durable
opaque storage slot.
