# Backup and recovery of local state (#111)

Notion is authoritative for page content, permissions, ancestry and edits. The
LanceDB chunk table, its vector embeddings and full-text indexes are **derived
and disposable**. In contrast, the operational SQLite store contains durable
processing history: webhook deduplication/inbox, crawl checkpoints, reconciliation
journal, leases, generations and possibly Pending index operation receipts.
An up-to-date Notion crawl cannot recover all of those records.

See [sync state](sync-state.md), [reconciliation journal](reconciliation-journal.md),
[index commits](index-commits.md), [index compatibility](index-compatibility.md)
and [webhook delivery](notion-webhooks.md).

## What must be preserved

| Asset | Restore policy |
| --- | --- |
| Notion content and attachments | Upstream source of truth; re-read from an authorized root |
| Operational SQLite configured by NK_WEBHOOK_STATE_FILE and any other state DB used by journal/commit | **Back up** consistently; can hold non-reconstructible pending work |
| SQLite -wal/-shm files | Never copy only the live main DB file; use SQLite online backup API or stop and close all connections |
| LanceDB directory, chunk/vector/FTS files | Disposable derived state; optionally snapshot for faster recovery |
| .nk-operational-binding in the LanceDB directory | **Do not delete to bypass validation**; coordinator binds it to durable DB identity and file inodes |
| Notion OAuth grant/key files, integration tokens and configuration | Secure, separate credential/configuration backup; do not include secrets in diagnostics or CI artifacts |
| Local model assets | Re-download/verify the required version and checksum if necessary; not authoritative content |

Enumerate **all** configured state databases and index paths for the
deployment; do not assume that only the webhook state path matters. Protect
backups because the local index may contain private page text.

## Consistent backup procedure

1. Record build version, SQLite/index paths, index table, embedding provider,
   model revision, allowed workspace, roots, exclusions and scope generation.
   Record identifiers in a protected operator log, never access tokens.
2. Quiesce the HTTP server, webhook consumers, reconciliation, scheduled
   workers and **every process** able to write the paired SQLite/Lance files.
   Wait for any owned commit threads to complete. Stopping the listener alone
   does not necessarily stop other writers.
3. For an offline backup, close all SQLite connections, then copy the SQLite
   database to protected backup storage. Never perform a plain file copy of an
   active SQLite main DB in WAL mode; use SQLite's online backup API instead.
4. Optionally snapshot the *paired* LanceDB directory and its binding alongside
   the SQLite snapshot, only with writers quiesced. A half-old/half-new pair is
   not recoverable by simply overriding the binding.
5. Verify the snapshot with SQLite PRAGMA integrity_check (result must be
   'ok'), a manifest of files/sizes/checksums and periodic restore drills.
   Keep the previous known-good backup until the new one is verified.
6. Restart only when the original paths and coordinator bindings still
   validate. A byte-identical copy onto a different inode is not automatically
   an authorized restore.

For a protected offline copy, an operator can verify:

    sqlite3 "/secure/backup/operational.sqlite3" "PRAGMA integrity_check;"

For a live SQLite backup, use the documented SQLite backup API (or the sqlite3
shell .backup command with safely quoted local paths); do not improvise a
non-atomic copy of an actively changing database.

## Restore operational SQLite state

1. Stop **all** writers. Quarantine the current state and index; take another
   snapshot before attempting recovery.
2. Restore a consistent backup into staging. Check PRAGMA integrity_check,
   open using SqliteSyncStateStore and confirm the expected schema migration
   history. Do not repair migrations by manually editing rows.
3. Inventory Pending/Failed operations and journal work before any retry. A
   Pending receipt may represent an index mutation that already succeeded
   before its SQLite acknowledgment. Never mark it Applied on assumptions.
4. Revalidate the trusted pairing: database identity, index table/directory,
   workspace, scope, generation, SQLite and directory device/inode identities.
   The index coordinator pins these specifically to reject path replacement
   and mixed generations. A filesystem move or copy can change inode identity
   even if the SQLite bytes match.
5. **Fail closed** on any mismatched binding. Do not delete
   .nk-operational-binding, rewrite trusted SQLite binding rows, or reuse an
   old index with a freshly copied state file. Portable re-binding must go
   through an explicit trusted initialization and full reconciliation plan;
   the current coordinator does not expose an arbitrary force-rebind.
6. Only with a verified compatible pairing and current source scope may
   work be resumed under a fresh valid fence, respecting idempotent replay and
   the apply-before-ack crash window.
7. When a matched restore cannot be established, preserve the backup for
   recovery/audit and create an **isolated new** operational/index generation
   followed by full authoritative reconciliation once that composition ships.
   A fresh operational database loses prior pending webhook work and
   deduplication history: report the gap rather than marking recovery complete.

## Rebuild the disposable index

1. Stop all writers and preserve operational state first. Never remove a
   bound index directory or its lock while a coordinator is using it.
2. Verify the current authorized Notion user/workspace, roots, exclusions,
   physical ancestry and complete source inventory. Event hints and cached
   index contents are not authorization to delete or re-index.
3. Provision a fresh isolated index directory and trusted generation. Select
   exactly the intended canonical schema and embedding provider/model/revision/
   dimension. An incompatible old vector-space identity must not be reused.
4. Refresh the full authorized source into canonical chunks and vectors.
   Rebuild FTS and vector indexes. Use guarded complete-page commits with
   source/scope revalidation, not raw append-only writes that bypass the
   durable coordinator.
5. Verify representative title/identifier/text lexical and vector searches,
   provenance, correct exclusions, counts and no stale deleted pages. These
   checks must succeed before publishing the new generation.
6. Reconcile outstanding work with current fence and generation. Only cut over
   when the inventory, indexing and replay are complete. Otherwise leave the
   new generation isolated and retain the previous backup for diagnostics.

**Current boundary:** the persisted table and operational SQLite adapters
support independent creation/rebuild, but production end-to-end Notion-to-index
refresh, reconciliation application/scheduling and portable coordinator
re-binding are not yet provided here. See #255, #242 and #243; do not claim a
live source recovery until those paths are implemented and exercised. In
particular, never pretend a new empty index is ready for search.

## Credential-free recovery smoke

Run the two tests from the repository root (the second uses real local LanceDB
but deterministic synthetic vectors, not a downloaded model):

    nix develop --command cargo test -p notion-knowledge-retrieval --test backup_rebuild --locked
    nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test backup_rebuild --locked

- SQLite: create durable page, checkpoint and webhook-dedup records; stop the
  store; copy the file; restore into a fresh path and verify schema and values.
  This demonstrates *store-level* backup/restore, not live WAL backup or a
  coordinator-paired recovery.
- LanceDB: create a real table and insert a canonical synthetic page; verify
  lexical and vector search; destroy only that derived index directory; build
  a new table, populate it from the same source text with a different chunk
  ID and verify retrieval without depending on the old vector row ID.
- Neither test writes to Notion or requires credentials. These component
  proofs do not replace an operational drill against a live authorized source.

### Recovery drill checklist

- [ ] All state/index paths and credential locations identified
- [ ] Writers quiesced and consistent protected SQLite backup verified
- [ ] Restored migrations, pending work and original generation identified
- [ ] Trusted paired binding accepted, or **new isolated generation** chosen
- [ ] Authoritative source inventory/scopes revalidated
- [ ] Lexical/vector/citation search probes and exclusions verified
- [ ] Pending apply-before-ack work reconciled, no work silently discarded
- [ ] Recovery result, gaps and unresolved blockers recorded
