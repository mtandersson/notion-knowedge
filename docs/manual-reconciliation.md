# Manual authoritative reconciliation

Build the Unix server with `--features webhook-worker` using the pinned Nix
`.#spike` environment. Manual commands use the same production `IndexRuntime`
as the webhook worker: authenticated Notion discovery/read, offline Qwen,
guarded LanceDB, SQLite, and one coordinator `Arc` shared by the planner and
`AuthoritativePageRefresh`. The canonical `chunks` table, full configured roots
and all exclusions, `webhook-v1` policy, generation and epoch must match the
existing index/state authority.

Configure `NK_NOTION_AUTH=integration`, secret `NOTION_TOKEN`,
`NK_NOTION_SCOPE_WORKSPACE_ID`, `NK_NOTION_SCOPE_ROOTS`, and the optional
`NK_NOTION_SCOPE_EXCLUDED_PAGES`, `NK_NOTION_SCOPE_EXCLUDED_DESCENDANTS` and `NK_NOTION_SCOPE_GENERATION` (default 1). Use the same absolute
`NK_WEBHOOK_INDEX_DIR`, `NK_WEBHOOK_STATE_FILE`, `NK_WEBHOOK_QWEN_ASSETS` and
`NK_WEBHOOK_TIMEOUT_SECONDS` (1–300, default 60) as the serving worker.
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
notion-knowledge-server --reconcile apply operator-2026-10-12
notion-knowledge-server --reconcile rebuild-relations relations-2026-10-15
notion-knowledge-server --reconcile status operator-2026-10-12
notion-knowledge-server --reconcile resume operator-2026-10-12
notion-knowledge-server --reconcile retry operator-2026-10-12 PAGE_UUID
```

Run IDs are 1–128 ASCII letters, digits, `_` or `-`. `apply` and `rebuild-relations` require a new ID;
`resume` and `status` require an existing run under the exact configured
binding. `retry` requires a failed application page in an open run and replans
that page explicitly. Unknown, extra or mixed transport/operator arguments fail
before credentials or state are used. Successful pages cannot be retried, and
resume never retries failed pages implicitly. Planning/access failures appear
separately; use a new run after repairing discovery/permissions.

`rebuild-relations` atomically starts inventory and invalidates only derived
relation completeness through owned, fenced maintenance, then uses the same
planner/runner as apply. It preserves graph rows until authoritative replacement,
unrelated links, content vectors, immutable receipts and operational coordination.
Resume/retry use the same run after interruption or graph failure. See the
[relation rebuild and recovery procedure](authoritative-relations.md#explicit-derived-relation-rebuild).

Dry-run only reads local authority and fresh Notion metadata/lifecycle/discovery;
it extracts no Markdown and embeds nothing. Status reads only local durable
state. Both use a SQLite-enforced read-only connection, validate the complete
current migration ledger, and perform no writer-lease, binding, metadata,
journal, checkpoint, receipt, FTS repair or index creation writes.

Apply/resume/retry acquire the same global SQLite writer lease as webhooks.
A busy writer returns promptly without planning or consuming work. Inventory
and each page attempt are bounded by the configured timeout; the lease is
renewed only between completed attempts. Once submitted, an operation keeps
its original expiry and owns any live index effects even if its observer drops.
Ctrl-C invalidates the active attempt, retains durable pending work, waits for
an already live owned effect to finish, then releases the lease. Restart with
`resume` and the same run ID to recover real receipts and finish pending work.
A timeout similarly preserves pending work for explicit resume. Applied
receipts remain immutable; a later new run handles newer source revisions.

Each command prints one payload-free JSON report. Actual `changed`, `deleted`
and `unchanged` counts derive from durable applied receipts; `scanned` counts
distinct inspected pages. `planned_refresh`/`planned_delete` are proposals,
not successful mutations. `errors` counts distinct failed pages plus run-level
failures. `planning_failures`, `application_failures`, `run_failures` and
`failure` expose sanitized classes and canonical page IDs for explicit retry.
Reports omit content, titles, URLs, credentials, paths and user-supplied run IDs.

Exit codes: `0` successful execution/inspection, `2` invalid arguments or
configuration/run identity, `3` work/planning failure or timeout, `4` busy writer
or lost lease, `5` operator cancellation. Status preserves work-failure exit
code when the inspected run contains failures; it never implies a failed run
was repaired merely by inspecting it.

Periodic scheduling is documented below; production packaging
remains #145. This command is operational manual execution, not a scheduler.

Run credential-free real-command fixtures with:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command --test reconciliation_dispatch --locked
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
Restart claims the stored active run and resumes its existing plan and receipts.
Cancellation stops admission, cancels source waits, drains submitted owned effects,
checkpoints pending work without spending retry budget and releases the lease.
Fatal scheduled-worker errors stop the HTTP service through its shared supervisor.

Inspect and recover existing compatible state through the real operator command:

```sh
notion-knowledge-server --reconcile schedule status
notion-knowledge-server --reconcile schedule status AFTER_SEQUENCE
notion-knowledge-server --reconcile status RUN
notion-knowledge-server --reconcile retry RUN PAGE
notion-knowledge-server --reconcile schedule retry DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule abandon DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule interval SECONDS DEADLINE_UNIX_SECONDS
notion-knowledge-server --reconcile schedule configure INTERVAL ATTEMPTS BACKOFF MAX_BACKOFF DEADLINE_UNIX_SECONDS
```

Status is read-only and reports the durable deadline, active run, cycle, attempts,
classified bounded history and active-run scanned/changed/deleted/unchanged/error
counts. History pages contain at most 1000 events; `history_next` supplies the next cursor.
Run status also reports retained classified failure evidence after successful retry. Source titles, content, URLs,
credentials and upstream error strings are excluded. Run status retains existing
sanitized page-failure identifiers for targeted explicit retry.

Retryable run-level failures retain the active run and use exponential capped
backoff; exhaustion stops automatic admission. Actual failed pages and planning
failures hold the cycle after remaining independent safe pending pages are handled,
without implicit retry or spending the transient budget. Held or exhausted runs
reserve no webhook admission; unrelated events continue while failure evidence
remains available for explicit recovery. Checkpointed healthy pending runs reserve
admission until resume or explicit abandonment, without consuming inbox attempts.
Retry failed pages explicitly; `schedule retry` rejects unresolved failed work.
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
