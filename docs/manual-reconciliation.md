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
notion-knowledge-server --reconcile status operator-2026-10-12
notion-knowledge-server --reconcile resume operator-2026-10-12
notion-knowledge-server --reconcile retry operator-2026-10-12 PAGE_UUID
```

Run IDs are 1–128 ASCII letters, digits, `_` or `-`. `apply` requires a new ID;
`resume` and `status` require an existing run under the exact configured
binding. `retry` requires a failed application page in an open run and replans
that page explicitly. Unknown, extra or mixed transport/operator arguments fail
before credentials or state are used. Successful pages cannot be retried, and
resume never retries failed pages implicitly. Planning/access failures appear
separately; use a new run after repairing discovery/permissions.

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

Periodic scheduling is the second child of #243 (#350) and production packaging
remains #145. This command is operational manual execution, not a scheduler.

Run credential-free real-command fixtures with:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command --test reconciliation_dispatch --locked
```

The fixtures invoke the real argument/service/shared-composition boundary with
a fake HTTP Notion server, real SQLite and on-disk guarded LanceDB, and a counting
provider. No runtime test URL, provider selector or authentication bypass exists.
