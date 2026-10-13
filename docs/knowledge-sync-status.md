# Local synchronization status

`knowledge_sync_status` is an installed read-only MCP tool on stdio and HTTP.
Call it with `{}`. It belongs to the `read` capability category and obeys exact
allowlist/blocklist selectors and HTTP access policy. Callers cannot select a
workspace, subscription, database, path or model.

It returns structured content:

- `last_successful_crawl_unix_seconds`: null. The existing read-only crawler
  does not persist a scoped successful completion receipt. Page edit/update
  timestamps and crawl cursors are not successful crawl evidence.
- `last_successful_reconciliation_unix_seconds`: the latest durable successful
  authoritative reconciliation completion for the current table, workspace,
  scope (roots and exclusions), generation and epoch. The additive SQLite
  schema 13 records this in the same fenced transaction as terminal completion.
  Success requires complete traversal, no planning/run errors and no remaining
  failed or pending pages. A terminal failed/held run is not success. Explicitly
  recovered page failures can succeed once all current pages are acknowledged;
  their historical failure records remain. Immutable planning/run failures
  require a new clean run. Repeating completion preserves the original time.
  Earlier runs without this receipt remain null; no timestamp is fabricated.
- `webhooks`: current persisted event counts for the configured workspace,
  subscription and integration. `pending` counts queued/debounced/retry-wait
  events (including future deadlines); `running` counts claimed work, even
  expired leases; `failed` counts current dead-letter events. Successful events
  and historical failures are excluded. Counts describe events, not page batches.
  Null means verified inbox scope or readable current-schema state is absent.
- `operational_schema_version`: the actual validated SQLite migration version,
  or null for absent, legacy, incompatible or inaccessible state.
- `index`: actual Lance table generation (`version`), validated persisted chunk
  and canonical schema versions and audited model/provider identity/version.
  The existing production worker or scheduler settings select the index. Its
  full coordinator binding and persisted vector metadata must match trusted
  scope and the pinned Qwen identity. Unknown/foreign/incompatible identities
  return null without exposing arbitrary metadata strings.

The tool opens existing local state read-only. It never creates/migrates SQLite,
loads/downloads a model, contacts Notion, repairs/creates an index, retries work
or acquires a writer lease. Missing offline model assets do not prevent status
inspection. WAL databases are refused before opening to avoid creating auxiliary
files or hiding live WAL data. Normal serving workers may independently write
state; the tool adds no writes. SQLite counts and Lance generation are observations
of independently changing components, not a cross-store atomic snapshot.

No IDs, paths, titles, source bodies, event payloads, credentials or upstream
error strings are returned. Null availability is honest and does not establish
freshness or production readiness.

Verify with the pinned production toolchain:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test sync_status --locked
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test authoritative_refresh --locked
```
