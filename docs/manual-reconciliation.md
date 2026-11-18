# Manual authoritative reconciliation

Build the Unix server with `--features webhook-worker` using the pinned Nix
`.#spike` environment. Manual commands use the same production `IndexRuntime`
as the webhook worker: authenticated Notion discovery/read, offline Qwen,
guarded LanceDB, SQLite, and one coordinator `Arc` shared with
`AuthoritativePageRefresh`. The canonical `chunks` table, full configured roots
and all exclusions, `webhook-v1` policy, generation and epoch must match the
existing index/state authority.

Configure `NK_NOTION_AUTH=integration`, secret `NOTION_TOKEN`,
`NK_NOTION_SCOPE_WORKSPACE_ID`, `NK_NOTION_SCOPE_ROOTS`, and the optional
`NK_NOTION_SCOPE_EXCLUDED_PAGES`, `NK_NOTION_SCOPE_EXCLUDED_DESCENDANTS` and `NK_NOTION_SCOPE_GENERATION` (default 1). Use the same absolute
`NK_WEBHOOK_INDEX_DIR`, `NK_WEBHOOK_STATE_FILE`, `NK_WEBHOOK_QWEN_ASSETS` and
`NK_WEBHOOK_TIMEOUT_SECONDS` (1–300, default 60) as the serving worker.
Whole-scope inventory uses `NK_RECONCILIATION_INVENTORY_TIMEOUT_SECONDS`
(1..86400, default 900). This includes dry-run and the walk performed by each sync. Ordinary page attempts keep
`NK_WEBHOOK_TIMEOUT_SECONDS`; individual upstream HTTP deadlines are unchanged.
The same distinction applies to cache reset/resync and periodic reconciliation.
Read-only inventory does not hold a writer lease. Page attempts and search
preparation keep the shorter page budget and its existing lease cleanup margin.
Leases renew only between owned attempts.

Manual execution does not require `NK_WEBHOOK_WORKER=true` or verified webhook
subscription/signing configuration. It never changes configured scope.

Provision the index/state with the production webhook worker before manual
execution. All manual commands require an existing compatible index and current
SQLite schema; missing, foreign or older state fails without initializing,
migrating, repairing, or creating an index. Production SQLite uses the default
rollback journal. Read-only commands reject externally WAL-configured databases
before opening SQLite, because a read-only SQLite query can create WAL/SHM
sidecars; they never ignore live WAL data using an immutable snapshot. Load and validate the actual offline
Qwen assets before opening operational state. Model/vector identity changes
require the explicit rebuild/recovery procedure, never an implicit migration.

```sh
notion-knowledge-server --reconcile dry-run
notion-knowledge-server --reconcile apply
notion-knowledge-server --reconcile rebuild-relations
notion-knowledge-server --reconcile rebuild-aliases
notion-knowledge-server --reconcile retry PAGE_UUID
```

`apply` is one full tree walk through `IndexRuntime::sync_from_walk`
(`SyncMode::Full`), the same path as cache resync and the periodic walk. The
walk reads each page once, commits changed pages, records the inventory and,
only when the walk is complete, removes indexed pages it no longer reached.
There are no run identifiers, durable plans, `status`, `resume` or
per-run retry: a failed or interrupted sync leaves its committed pages in place
and the next `apply` finishes the rest. `retry PAGE_UUID` syncs one page
(`SyncMode::Page`) through the inventory's parent map; a page the inventory
cannot place is counted as `unplaced` and left for the next walk. Unknown,
extra or mixed transport/operator arguments (including a former run
identifier) fail before credentials or state are used.

`rebuild-relations` and `rebuild-aliases` first invalidate only the derived
projection completeness under the owned maintenance fence (graph rows, content
vectors, receipts and coordination survive), then run the same full sync, which
recommits every page whose projection was invalidated. See the
[relation rebuild and recovery procedure](authoritative-relations.md#explicit-derived-relation-rebuild).

`dry-run` walks the tree and compares it with the stored inventory. It uses a
SQLite-enforced read-only connection, validates the complete current migration
ledger and performs no writer-lease, inventory, outcome, receipt, embedding or
index writes. Its report is `scanned`, `skipped`, `complete` and `drift`
(`added`, `changed`, `moved`, `removed`, `trashed`). `removed` lists every stored
page the walk did not reach; a real sync may hold some one walk (`held`).

Apply and retry acquire the same global SQLite writer lease as webhooks. A busy
writer returns promptly without reading. The whole command (walk plus indexing) is bounded by
`NK_RECONCILIATION_INVENTORY_TIMEOUT_SECONDS`, each page by
`NK_WEBHOOK_TIMEOUT_SECONDS`; pages committed before a timeout stay. Once submitted, an operation keeps its original
expiry and owns any live index effects even if its observer drops. Ctrl-C
invalidates the active attempt, waits for an already live owned effect to
finish, then releases the lease.

Each command prints one payload-free JSON report. `apply` and `retry` report
`scanned` (pages reached), `changed`, `deleted`, `unchanged`, `skipped`,
`errors` (failed pages), `permanent`, `held`, `complete` and `drift`.
Reports omit content, titles, URLs, credentials and paths.

Exit codes: `0` success (a complete walk with no failed or held pages), `2`
invalid arguments or configuration, `3` work failure, timeout, failed or held
pages or an incomplete walk, `4` busy writer or lost lease, `5` operator
cancellation.

**Breaking:** `status RUN`, `resume RUN` and the run identifier of `apply`,
`rebuild-relations`, `rebuild-aliases` and `retry` were removed.

Periodic scheduling is documented below; production packaging
remains #145. This command is operational manual execution, not a scheduler.

Run credential-free real-command fixtures with:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test it --locked
```

The fixtures invoke the real argument/service/shared-composition boundary with
a fake HTTP Notion server, real SQLite and on-disk guarded LanceDB, and a counting
provider. No runtime test URL, provider selector or authentication bypass exists.

## Periodic serving and explicit recovery

Build with `webhook-worker` and run `--http`. Set
`NK_RECONCILIATION_INTERVAL_SECONDS=60..2592000` to opt in. It requires
`NK_NOTION_AUTH=integration`, the local `NOTION_TOKEN`, trusted Notion root
scope (including configured exclusions), `NK_WEBHOOK_INDEX_DIR`,
`NK_WEBHOOK_QWEN_ASSETS` and an absolute `NK_WEBHOOK_STATE_FILE`.
Periodic serving works with `NK_WEBHOOK_WORKER=false` and webhook mode disabled.
When both workers are enabled, they use one composed coordinator, guarded Lance
table, provider and authoritative pipeline. Production uses authenticated Notion
and offline Qwen; fixture transports/providers cannot be selected by environment.
Packaging remains #145.

Optional retry settings are `NK_RECONCILIATION_MAX_ATTEMPTS` (1..16, default 4),
`NK_RECONCILIATION_BACKOFF_SECONDS` (1..86400, default 5), and
`NK_RECONCILIATION_MAX_BACKOFF_SECONDS` (initial backoff..604800, default 300).
Source/inventory/page attempts use `NK_WEBHOOK_TIMEOUT_SECONDS` (1..300,
default 60); admission polls use `NK_WEBHOOK_POLL_MS` (10..60000, default 1000).
A new schedule is due immediately. Exact configuration replay preserves its
persisted deadline, cycle, failure budget and active run. Completion and the next
`completion time + interval` deadline commit in the same SQLite transaction.
Changing configuration never silently discards existing authority or work.

The lease covers one returned runner operation and is renewed between operations.
Busy manual/webhook/external owners defer work without spending a failure attempt.
Restart claims the stored schedule and runs its next full walk.
Cancellation stops admission, cancels source waits, drains submitted owned effects,
checkpoints pending work without spending retry budget and releases the lease.
Fatal scheduled-worker errors stop the HTTP service through its shared supervisor.

Inspect and recover existing compatible state through the real operator command:

```sh
notion-knowledge-server --reconcile schedule status
notion-knowledge-server --reconcile schedule status AFTER_SEQUENCE
notion-knowledge-server --reconcile schedule retry DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule abandon DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule interval SECONDS DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule configure INTERVAL ATTEMPTS BACKOFF MAX_BACKOFF DEADLINE_UNIX_SECONDS
```

Status is read-only and reports the durable deadline, active run, cycle, attempts,
any hold on the active cycle (`hold` is `held_unauthenticated` with a fixed
`hold_reason` while a rejected credential holds the cycle, including during the
retry, until that retry records an outcome; otherwise `null`; in history the hold
is a `Cancelled` event with failure `Source` that spends no attempt),
classified bounded history and active-run scanned/changed/deleted/unchanged/error
counts. History pages contain at most 1000 events; `history_next` supplies the next cursor.
Source titles, content, URLs, credentials and upstream error strings are excluded.

Retryable run-level failures retain the active run and use exponential capped
backoff; exhaustion stops automatic admission. After independent page work drains,
source-only skips retain their error evidence and end the cycle at the next normal
interval, so the daily/weekly catch-up can try them again without manual repair.
Index, ownership and credential failures still hold the cycle without spending
the transient budget. Held or exhausted runs
reserve no webhook admission; unrelated events continue while failure evidence
remains available for explicit recovery. Checkpointed healthy pending runs reserve
admission until resume or explicit abandonment, without consuming inbox attempts.
Immutable planning/run failures require explicit `schedule abandon` followed by a
fresh cycle. Abandon preserves historical plans, receipts and failures and consumes
the lease; a new worker must acquire a new fence. Pending submitted receipts or an
external owner reject abandonment as busy. Recover their original run first.
Successful later cycles preserve historical failures.

Stop serving before changing the interval with `schedule interval`; it requires no
active cycle and keeps retry bounds. Use `schedule configure` to explicitly change all four policy bounds between
cycles. Then update the environment to match the persisted policy before restart. Root/exclusion/generation authority changes
require recovery under the old binding before an explicit authority transition;
automatic registration fails closed on mismatched authority.
