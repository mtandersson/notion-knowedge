# Cache reset and authoritative resync

Implementation: #577. See the production behavior below and the bounded
credential-free acceptance tests in `server/tests/it/reconciliation_command.rs`.

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
allowlist of derived SQLite bookkeeping, and removes the known source cache,
lexical and semantic tables, projection queue and binding artifacts. It preserves the directory and
operational SQLite file, admitted webhook work, mutation/file refresh intents
and unrelated application tables. A single pending-sync flag remains until the
inventory and independent page attempts produce a usable search index.

Each inventory/page attempt uses the configured timeout. Unreadable source pages
remain visible in existing reconciliation errors while readable pages become
searchable. Template references are kept as returned by Notion, without fetching
their originals or requiring source repairs. A later sync can recover skipped
pages. Storage/index failures, invalid ownership, invalid credentials, failed
inventory execution and unusable lexical assets still leave startup unavailable and the
flag pending. The next start retries recovery; arbitrary storage errors do not
trigger destructive reset.

Initial/reset sync overlaps discovery with useful indexing. A bounded channel
holds at most eight discovered page IDs. Two read-only workers freshly authorize
and prepare page content (shared Notion rate limiter) while one consumer
durably commits pages in discovery order. At most one prepared page waits
beyond those in flight. Each
preparation reserves 32 MiB of a 64 MiB retained-byte budget before its first
request, keeps only its measured size while queued and committing, and is
prepared again from the same walked read by the consumer if it exceeds its
reservation. Each page is prepared from the walk's single read (one paged
search listing supplies metadata, so a listed page costs only its Markdown
read); nothing is read again before or inside the commit. Prefetching captures
no index version, lease or receipt decision: the consumer builds the guarded
mutation plan from current index state and re-chunks a page whose own indexed
chunks changed meanwhile. A source edit or move after the read is picked up by
the next walk. Preparation and publication each have their own page deadline, so one
page may take up to twice `timeout_seconds`; both timeouts use the existing settlement. The existing writer
lease is renewed while waiting for more pages; timed-out work is fenced and
drained before releasing it. No Notion write or upload is issued.

Discovery overlaps up to eight independent page/block/database branches; cursors
within a paginated endpoint remain ordered. After traversal and early refreshes
finish, the walk is recorded as the page inventory with each page's outcome.
Only a complete walk replaces the inventory and removes indexed pages it did not
reach as indexable. There is no second planning pass. Page failures are kept as
inventory outcomes (`failed`); no extra recovery ledger or persisted traversal
cursor is introduced. Search preparation and serving
still wait for a valid completed inventory and the available indexed subset.

Operator stderr/journald includes `operation=cache_sync` summaries at phase start,
roughly every ten seconds as work completes, and phase completion. Every normal
operation event within a startup sync also includes `sync_progress`: phase,
`discovered_pages`, `content_reads`, `prefetched_pages`, `embedded_chunks`, `committed_pages`,
`skipped_pages`, and `total_pages`. These are actual metadata discoveries, successful
content reads, pages prepared for publication, completed embedding outputs and acknowledged page commits; reads
happen once per page per sync. They are not estimates
of unique indexed sources. The total stays null until traversal completes; no
workspace-wide page count is guessed. Counts contain no titles, IDs, URLs or text.

Each page the consumer settles also emits one `cache_sync` line with per-phase timings
and the progress line carries their running totals; see
[sync phase timings](sync-phase-timings.md).

Individual requests retain a `worker` label such as `discovery-17` or `indexing-8`
and `runtime_thread`. Counts and worker context follow owned commit threads as well
as awaited async requests. Thread identity can change across awaits. Summary
phase completion is distinct from final search readiness, and progress does not
claim skipped or inaccessible content was indexed.

HTML containers are paired at their matching close, so independent complete
elements retain separate boundaries while genuinely nested containers stay
atomic. The chunk target remains soft and source text is never truncated.
For an existing healthy cache that contains the older coalesced HTML layout,
unchanged verification can report a conflict rather than silently changing its
checkpoint. Use the existing trusted scoped cache reset/resync procedure below
to rebuild that derived layout, preserving operational/external-effect records.
The embedding model/vector identity does not change for this boundary correction.

The source and lexical publication completes before model work. The semantic
worker reads that revision locally and plans and embeds up to eight windows
per attempt. Each attempt resumes from the byte ranges already staged for the
same revision. It publishes the
complete vector set under the writer fence. Long chunks use exact UTF-8
windows admitted by the model's tokenizer and byte limit. Windows prefer
nearby Markdown boundaries and may overlap to retain context.
A model failure or input-limit deferral appears under
`semantic_projection` in status and logs. Fresh reads and lexical search
can use the published text while that work awaits retry.
An installed worker drains due jobs in the background. On a retrieval-only
installation, run `--reconcile apply` to attempt due jobs after source sync.

With `NK_GRAPH=false`, source content, link text and relation properties remain
canonical, but optional foreign-target metadata resolution is disabled. References
are stored as unresolved graph projections, which cannot drive navigation or
graph boosting. Fresh source authorization and guarded commit validation still
run. Enabling graph navigation later preserves unresolved references until normal
reconciliation verifies their targets; unchanged canonical text does not require
new embeddings. Disabling graph enrichment again conservatively replaces the
previously resolved source projection. This setting is trusted immutable runtime
composition and cannot be supplied by source metadata or a prepared proposal.

During initial population, a pre-commit page deadline cancels its observer and
expires its commit clock before owned work is drained. Unresolved/failed effects
and genuine storage, authentication
or ownership errors remain fatal. An applied receipt uses existing bounded
recovery rather than being labeled skipped. A safe page deadline emits the fixed
`page_timeout` reason and becomes `PageTimeout` in durable per-page progress once
the completed inventory is recorded. Before that point, the failure is visible
only in startup progress logs. The same run does not immediately retry that page;
a normal later reconciliation can retry it without requiring source edits.
The final bootstrap page-application stage uses the same timeout settlement.

`EmbeddingInputLimit` and `PageTimeout` are derived reconciliation metadata. Older executables
may reject that cache status on downgrade; use the same scoped disposable-cache
rebuild rather than editing status or relabeling vectors. Preserve the private
operational backup and external mutation/credential records throughout.

For a corrupt cache file that cannot be classified as an identity/schema
mismatch, stop the service and run the explicit trusted operator command with
the normal scope, Notion credential and offline embedding settings:

```sh
notion-knowledge-server --index reset
```

Exit 0 reports `complete` or `ready_with_skips`; skipped content remains in sync
status and is not claimed indexed. Exit 3 reports an unusable reset/resync.
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
cache is valid for the configured scope and identity. Individual source failures
do not invalidate the successfully indexed subset. Expose bounded progress,
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

The obsolete durable cache recovery framework and its dedicated fixtures were
removed in #578; remaining test/CI consolidation is #579. These replace conflicting historical mandates in the
lexical transition issue chain, while preserving normal identity checks,
authorization and safe external writes.
