# Cache reset and authoritative resync

Implementation: #577. See the production behavior below and the bounded
credential-free acceptance tests in `server/tests/reconciliation_command.rs`.

Notion is authoritative. One server process owns local retrieval and sync.
Derived data can be discarded and rebuilt. Recovery must not depend on a prior
index generation, a retained handoff journal, a legacy executable, or transfer
of authority between processes. See ADR 0001.

## Production recovery

The sync worker and installed integration-backed retrieval readers call the
shared `IndexRuntime` recovery path before serving. Missing storage, missing
binding metadata, known identity/schema incompatibility, or an interrupted
rebuild trigger reset and a fresh authoritative inventory. A healthy cache is
reused; healthy read-only composition still opens it without writes.

Reset holds the directory guard and current writer lease, clears an explicit
allowlist of derived SQLite bookkeeping, and removes only the canonical chunks
table and its known binding/journal artifacts. It preserves the directory and
operational SQLite file, admitted webhook work, mutation/file refresh intents
and unrelated application tables. A single pending-sync flag remains until the
fresh inventory, page refreshes and search index preparation all succeed.

Each inventory/page attempt uses the configured timeout. Failed resync leaves
startup unavailable and the flag pending; the next start resets partial derived
output and retries. Use normal supervisor restart/backoff for unavailable
Notion or assets. Resource/permission and arbitrary storage errors do not cause
an automatic destructive reset.

For a corrupt cache file that cannot be classified as an identity/schema
mismatch, stop the service and run the explicit trusted operator command with
the normal scope, Notion credential and offline embedding settings:

```sh
notion-knowledge-server --index reset
```

Exit 0 reports complete reset/resync; exit 3 reports an incomplete reset/resync.
Invalid configuration exits 2. This command only reads Notion; it never replays
a write or upload. Existing `--index rebuild` remains ANN-only maintenance over
stored rows, while `--index reset` reconstructs content from Notion. The command
requires the Unix `webhook-worker` feature and integration-backed source
configuration, like the existing manual reconciliation runtime.

## Storage inventory

| Storage | Recovery treatment |
| --- | --- |
| LanceDB canonical chunks, embeddings, FTS and ANN indexes | Disposable; rebuild from authorized Notion content with configured embeddings. |
| SQLite page hashes, metadata identities, graph edges, aliases and projection completeness | Derived; invalidate with the cache so stale metadata cannot skip reconstruction. |
| SQLite index bindings, generations and page commit receipts | Internal cache coordination; reset their assumptions about the discarded cache. They are not evidence that replacement rows exist. |
| SQLite reconciliation runs, inventory, plans and schedule progress | Sync bookkeeping; old completed work must not make a new empty cache appear synchronized. Reuse the reconciliation pipeline for a fresh inventory. |
| SQLite webhook inbox, debounce work and deduplication | Preserve admitted work while rebuilding; a full recrawl recovers missed content events. Do not discard the whole database to reset retrieval. |
| SQLite mutation refresh and file refresh intents | Preserve; they connect already performed Notion effects to local refresh. Rebuilding must only read upstream data, never repeat the original write or upload. |
| Notion grant/key files, MCP credentials, external mutation/idempotency ledgers and audit stores | Outside the disposable retrieval boundary. Do not reset them. |
| Configured offline embedding and lexical assets | Inputs to reconstruction; validate configured identity and report unavailable inputs. Do not download at query time. |

The operational schema is in `crates/retrieval/migrations/0001` through `0019`.
The same SQLite database contains both derived projections and admitted work;
whole-file deletion is not an acceptable production reset strategy. A corrupt
operational database needs an explicit error/recovery policy and must not be
silently treated as an empty mutation ledger.

## Required behavior

On startup, detect whether derived retrieval storage is usable. Missing or
incompatible cache enters rebuilding state. Stop serving it and coordinate
local writers in the same process. Reset only derived state, create compatible
empty retrieval storage, and run a fresh authoritative inventory and page refresh
through the existing Notion, reconciliation and embedding adapters. Apply the
configured workspace, roots and exclusions on every recovered page.

Interrupted rebuilding can discard partial derived results and restart sync.
It needs no durable old/new index handoff. Keep admission unavailable until the
cache is valid for the configured scope and identity. Expose bounded progress,
errors and retries through existing operational reporting. Once ready, ordinary
incremental synchronization continues.

Permission failures, unavailable assets and transient upstream failures must not
be mistaken for cache corruption. A reset cannot fix those failures. Report and
retry them with existing rate limits and bounded backoff.

## Verification

Use small deterministic Notion and embedding fixtures through production seams.
Exercise a fresh startup, deletion of an existing cache, unusable derived
metadata, a failed sync followed by retry, and subsequent incremental updates.
Verify scoped content and search after reconstruction, removed/out-of-scope
content exclusion, and unavailable search while rebuilding. Check that credentials
and external mutation records remain intact and that no write/upload request is
issued during recovery. Keep real Notion smoke separately authorized.

Obsolete durable cache recovery removal is tracked by #578; consolidation of
its tests and CI is #579. These replace conflicting historical mandates in the
lexical transition issue chain, while preserving normal identity checks,
authorization and safe external writes.
