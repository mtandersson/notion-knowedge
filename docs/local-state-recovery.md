# Local state backup and rebuild runbook (#111)

This is the operator procedure for the **local-first** Notion knowledge MCP.
**Notion is authoritative.** Local search records and embeddings are derived
material, not a replacement for Notion or a disaster-recovery source.

> Scope (2026-10-09): reusable SQLite and LanceDB adapters and the isolated
> one-page Notion reindex experiment exist. The normal server image still
> does not run production indexing, scheduled full reconciliation, or a
> production restore command (#145, #242, #243, #255, #257). This page
> distinguishes tested component recovery from the operational steps that
> must be completed and verified before a real deployment can be recovered.
> Do not report a live full-corpus rebuild as supported today.

## Inventory: what to protect

| Resource | Authority / recovery | Required handling |
| --- | --- | --- |
| Notion pages, blocks, databases and files | **Source of truth**; never overwrite them from a local index | Verify approved workspace, roots and exclusions before reading/reindexing |
| SQLite operational state (see [sync-state](sync-state.md)) | Not authoritative, but **not freely disposable**: includes webhook inbox, dedupe, retry/dead-letter state, checkpoints, reconciliation journal, commit receipts and durable binding identity | Back up transactionally; restore deliberately; preserve pending work and receipt history |
| LanceDB table, ANN/FTS indexes, generation sidecars | Derived, can be regenerated from authorized Notion content and the pinned embedding provider | Rebuild into a fresh, explicitly named generation; never trust old vector IDs or reuse an incompatible vector space |
| Index binding file and SQLite coordinator identity | Safety-critical **pairing**, not independent backups to mix-and-match | Record and recover with the matching SQLite/index generation; never delete the binding to evade a mismatch |
| Embedding model/config and pinned revisions | Reproducibility inputs; not authoritative Notion data | Preserve model/provider/version/dimension identity and verified asset manifests; obtain compatible assets for re-embedding |
| Notion OAuth encrypted grant and its encryption key (if configured) | **Secrets / access**, not rebuildable from Notion page content | Back up the state and separate raw 32-byte key securely **as a pair**; losing the key requires reauthorization; never include either in index backups or logs |
| Operator configuration, trusted scope, webhook signing key, private access credentials | Security and recovery inputs | Keep encrypted, access-controlled backups outside Git and outside the derived index tree |

See [grant-store recovery](notion-grant-store.md), [webhook inbox](notion-webhooks.md),
[commit coordinator](index-commits.md) and [compatibility policy](index-compatibility.md).
The container's index/state/models paths are currently reserved, **not active**
production storage mounts; see [container](container.md).

## Before making a backup

1. Record the exact environment, Git revision, SQLite path, LanceDB index
   directory/table, model identity, approved Notion workspace/root/exclusions
   and current generation. Avoid printing secrets or page content.
2. Stop accepting webhook/MCP mutations. Drain or stop every index writer and
   reconciliation/worker process. Confirm **all** processes using the SQLite
   state and index have stopped and the directory guard is released. There is
   no safe point-in-time snapshot of a running two-database transaction.
3. Inspect pending/failed webhook items and pending index-operation receipts.
   A pending receipt **may already have changed LanceDB** even if its SQLite
   checkpoint was not committed. Preserve and reconcile it; never blindly mark
   it applied or discard it.
4. Take a consistent SQLite backup using its backup API (below), not a copy
   of the database file while a WAL writer is active. Back up the index and
   coordinator metadata as one *matched* evidence set only if preserving an
   existing generation; do not promote the index copy as authoritative.
5. Back up OAuth encrypted state **and its separately protected key** only
   through the host's secret-manager/backup workflow. Verify restrictions and
   restore access separately; this document intentionally provides no command
   that prints secret values.

Use a private directory (0700), encrypted off-host retention, restrictive
permissions, and a documented retention policy. Avoid raw body/secret output in
the backup log; verify that the backup is actually recoverable.

### Transactionally back up and inspect SQLite

The following Python 3 standard-library example runs **after all writers are
stopped**. Replace both paths with exact, operator-verified absolute paths.
The destination must not exist; do not use the live state directory for backups.

~~~sh
export NK_RECOVERY_STATE=/absolute/private/state/state.sqlite
export NK_RECOVERY_BACKUP=/absolute/private/backup-YYYYMMDD/state.sqlite
umask 077
mkdir -p -- "$(dirname -- "$NK_RECOVERY_BACKUP")"
chmod 700 -- "$(dirname -- "$NK_RECOVERY_BACKUP")"
python3 - <<'PY'
import os
from pathlib import Path
import sqlite3

source = Path(os.environ["NK_RECOVERY_STATE"])
backup = Path(os.environ["NK_RECOVERY_BACKUP"])
if (not source.is_absolute() or not source.is_file()
        or not backup.is_absolute() or os.path.lexists(backup)):
    raise SystemExit("invalid source or backup destination")
with sqlite3.connect(source) as db:
    with sqlite3.connect(backup) as out:
        db.backup(out)
        if out.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise SystemExit("backup integrity check failed")
os.chmod(backup, 0o600)
print("SQLite backup verified (content not displayed)")
PY
~~~

SQLite's backup API includes committed WAL transactions. This **does not**
atomically snapshot LanceDB, Notion or separate credential files. Preserve
the associated generation/binding inventory and the original SQLite backup
for forensic recovery, including before any attempted restore.

## Restore operational state

**First decide whether restoring an old state snapshot is safe.** It may roll
back dedupe and retry counters or lose committed receipts/webhook events. The
index and state may represent different generations or different physical
database identities. An old backup is **not** evidence that queued work is
complete. Pause delivery and retain incoming events upstream while recovering.

1. Stop *all* writers and make a separate safety copy of the current state,
   WAL/SHM sidecars and index/binding artifacts (if present). Confirm no
   active lock holder; do not restore into a running connection.
2. Verify the backup with SQLite integrity checking and inspect migrations,
   pending/failed jobs, receipts and recorded coordinator identity through
   the normal operator interfaces. If history is corrupt or has a newer
   unsupported schema, **stop**; migrations are not a downgrade tool.
3. Restore into an **isolated staging path**, never over the only live copy.
   The code below verifies that the copy is a valid SQLite database, but
   cannot prove its index binding or application-level recovery.
4. Compare trusted workspace/scope, original database identity, physical
   state/index paths, persisted index generation and outstanding receipts
   against the intended index. The coordinator pins SQLite **device/inode**
   and a durable DB identity plus the index directory's binding. Copying to
   another path changes physical identity: an old bound index **must not**
   be attached simply by replacing files or deleting its anchor.
5. Only promote a state/index pair using a reviewed, matching and
   application-supported restore/reinitialization procedure. If identities
   mismatch, unresolved receipts are ambiguous, or the authoritative
   source cannot be traversed, leave service unavailable, preserve evidence
   and perform a new trusted full-scope reconciliation/rebuild once the
   production runner (#242/#243/#255) is available. Do not silently drop
   pending work to make startup green.

An isolated restore/validation, using a *new* destination file:

~~~sh
export NK_RECOVERY_BACKUP=/absolute/private/backup-YYYYMMDD/state.sqlite
export NK_RECOVERY_STAGE=/absolute/private/restore-stage/state.sqlite
umask 077
mkdir -p -- "$(dirname -- "$NK_RECOVERY_STAGE")"
chmod 700 -- "$(dirname -- "$NK_RECOVERY_STAGE")"
python3 - <<'PY'
import os
from pathlib import Path
import sqlite3

original = Path(os.environ["NK_RECOVERY_BACKUP"])
stage = Path(os.environ["NK_RECOVERY_STAGE"])
if (not original.is_absolute() or not original.is_file()
        or not stage.is_absolute() or os.path.lexists(stage)):
    raise SystemExit("invalid source or restore destination")
with sqlite3.connect(original) as db:
    if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise SystemExit("backup is corrupt")
    with sqlite3.connect(stage) as restored:
        db.backup(restored)
        if restored.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise SystemExit("staged restore is corrupt")
os.chmod(stage, 0o600)
print("Staged SQLite restore verified; NOT activated")
PY
~~~

For a **genuinely new** instance with no existing bound index, an operational
SQLite state store can also be created empty by opening the configured file:
the embedded migrations recreate the schema, but do not restore missed webhook
events or already performed effects. Any such state reset requires new
authoritative full inventory and deliberate receipt/retry disposition. This is
**not** a substitute for a backup when pending work matters.

## Rebuild LanceDB without relying on old vectors

1. Verify Notion credentials, *affirmative* allowed-root ancestry and
   exclusions, complete source reads, compatible embedding model identity,
   and enough free space before touching a derived generation. The webhook
   hint or a missing discovery entry never authorizes deleting content.
2. Quiesce serving and all writers. Keep the old index unchanged for rollback
   and evidence; create a distinct **new** empty directory/generation under
   a trusted local path. Do not delete the parent storage directory, model
   cache, SQLite state, grant key, or coordinator binding.
3. Index **all** selected pages afresh from Notion: canonical normalize,
   chunk/fingerprint, embed with the intended provider/model/version/dimension,
   build vector and lexical structures and validate complete scope. Do not
   copy cached old vectors or old chunk IDs into the new generation; chunk IDs
   are *derived* from current canonical content and may happen to be stable.
   The retrieval index has no foreign-key authority over Notion and no
   requirement to preserve historical vector row identities.
4. Open the new table with strict schema/vector-space compatibility checks,
   verify row and unique-ID counts, source provenance and representative
   lexical/semantic queries. Require an explicitly complete inventory,
   including exclusions and source failures; do not serve a partial generation.
5. Switch serving to the verified generation **only** via a supported guarded
   rebind/scope transition and checkpoint plan. In particular, the binding
   in [index commits](index-commits.md) forbids quietly swapping the directory
   or connecting a newly initialized SQLite DB to an older bound index.
   If production rebind tooling is unavailable, **stop here**. Keep service
   unavailable or retain the old verified generation; do not circumvent the
   guard.

The current adapter also exposes an explicit
**IndexCompatibilityPolicy::Rebuild** for an incompatible *named derived
table*, not an automatic recovery from arbitrary I/O errors; it creates a
compatible **empty** table, and the caller still owes a full source refresh.
See [index compatibility](index-compatibility.md).

## Reproducible recovery smoke tests

### A. Offline, credential-free component checks

Run from a clean checkout with the pinned Nix/LLVM shell. This uses temporary
stores and a deterministic test embedding provider: no private Notion access
or model download is required.

~~~sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --locked \
  deleting_the_database_rebuilds_an_empty_store_without_notion

nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --locked \
  startup_policy_fails_closed_or_explicitly_rebuilds_embedding_mismatch

nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --locked \
  optimizing_fts_after_page_diff_makes_updated_terms_searchable

nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --locked \
  page_diff_embeds_only_changed_and_new_chunks_and_is_idempotent

nix develop --command cargo test -p notion-knowledge-retrieval \
  --test index_commits --locked
~~~

Check that every invocation passes. The tests cover an empty SQLite store
after deletion, incompatible-index fail/rebuild gating, genuinely updated
retrieval results, idempotent page replacement, and durable commit fencing.
They do **not** claim to test backup/restore of a production two-database pair.

The staged SQLite backup/restore commands above are an additional operator
smoke: use a **disposable** test SQLite database with at least one committed
row, verify the copied state/row count and permissions, then destroy *only*
the known disposable test paths. Never test deletion against active production
paths.

### B. Optional live, disposable one-page rebuild

The already demonstrated [manual Notion refresh and recovery experiment
(#215)](manual-refresh-spike.md) provides a stronger real-source smoke with
the isolated qwen-lance-spike executable:

- Start from an explicitly approved **disposable** test root and a newly
  selected index path with no production readers or writers.
- Run its documented create-notion, inspect-index and query-page commands.
- Delete **only the fixed disposable experiment index directory**, then run
  create-notion again from unchanged Notion and verify identical canonical
  corpus digest, unique IDs and successful fresh-process retrieval.
- Confirm that the source page was not modified, credentials were never
  logged, and the old index/model assets were not touched.

The recorded 2026-10-05 test re-embedded all 11 chunks after deleting a
disposable generation, checked the same canonical digest, and retrieved the
authoritative updated passage. No stored vector ID was needed for recovery.
That is **component/spike evidence**, not a production end-to-end restoration.

### C. Production go/no-go after components integrate

Before reopening public traffic, confirm:

- The authorized fresh inventory was complete, or all exceptions were
  safely quarantined; unmatched pages are not treated as deleted.
- Index and SQLite identities/bindings match; no unexpected generation or
  unresolved Pending/Failed receipts have been silently discarded.
- Failed/delayed webhook work is visible and recoverable; idempotent replay
  and deadlines behave as intended across a restart.
- Retrieval passes lexical and semantic queries against known current Notion
  content, with correct citation IDs and no stale/out-of-scope results.
- Secrets remain outside logs/backups meant for derived indexes, health
  checks pass, and shutdown/restart preserves pending work.
- Any missing production recovery orchestration blocks go-live; do not
  mark this checklist passed based on adapter/unit tests alone.

## Failure and rollback rules

- **Backup corrupt/unreadable:** retain original and do not replace live state.
- **Missing secret key:** reauthorize through the supported OAuth path; never
  copy plaintext grant material into the repository or an index.
- **Model/schema mismatch:** fail closed, preserve old generation, then
  explicitly rebuild into a fresh compatible generation.
- **Index/SQLite binding mismatch:** do not delete the anchor or fake a
  physical identity; preserve artifacts and reinitialize with verified scope.
- **Incomplete Notion traversal or permission error:** leave retrieval
  unavailable; never infer a page deletion from absence alone.
- **Uncertain cross-store receipt:** preserve both stores and journal,
  re-read authority and replay only proven-idempotent effects when the
  production coordinator is available.
- **New generation fails validation:** leave old generation untouched.
  Discard only the verified disposable failed output, never the shared parent.

Keep a concise recovery record: revision, UTC time, scope and generation
identities, hashes/counts (not private content), backup/restore location,
test commands/results, ambiguous receipts, operator decision, and which
generation is serving.
