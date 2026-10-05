# Local sync-state database

Issue #36 introduces the durable operational store selected by ADR 0001.
Notion remains authoritative; this SQLite file contains only rebuildable local
coordination state and is intentionally separate from LanceDB retrieval data.

## Boundary

core::sync_state::SyncStateStore owns the provider-independent contract.
retrieval::sync_state::SqliteSyncStateStore is the SQLite adapter. Server
composition and crawl scheduling are separate tickets.

The first schema migration persists:

- complete page sync rows, including content hashes and optional tombstones
- named crawl checkpoints
- webhook event IDs used for durable deduplication
- named index/schema version strings

Each page upsert is one SQLite statement, so readers never observe a
partially-replaced row. Webhook deduplication uses the event ID primary key and
INSERT OR IGNORE, so the return value is true only for the first durable insert.

## Migrations and rebuild

Migrations are checked-in SQL under
crates/retrieval/src/sync_state/migrations/. Applied versions are recorded in
schema_migrations inside the same database. Opening a database runs pending
migrations under an IMMEDIATE transaction and rejects a file whose schema is
newer than this binary understands.

Creating the store at an empty path requires no Notion client, token, or
network access. Deleting the file therefore drops only derived operational
state; opening the path again rebuilds the schema from migrations. Repopulating
page/checkpoint contents from Notion is orchestration work, not a database
migration requirement.

## Verification

Focused tests cover migration idempotency and empty-file rebuild, complete
page replacement with tombstones, webhook deduplication across reopen, and
checkpoint/index-version replacement.

Run:

    cargo test -p notion-knowledge-retrieval --test sync_state --locked
    cargo clippy -p notion-knowledge-retrieval --all-targets --locked -- -D warnings
