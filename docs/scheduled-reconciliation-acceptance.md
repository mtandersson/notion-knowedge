# Reconciliation execution acceptance audit

This maps the original criteria of #350, #243 and #58, plus their implementation
children, to production boundaries and deterministic assertions. Parent closure
belongs to the parent owner after the final head's checks and merge. Packaging
#145, sync-status #59 and epic #7 retain their separate scopes.

The scheduled fixtures in `crates/server/tests/reconciliation_command.rs` start
`http::serve_composed`, exercise `/livez` over real HTTP, and run the actual
supervised worker. They use authenticated fake HTTP Notion through the trusted
`NotionClient` seam, the real SQLite journal, guarded on-disk LanceDB and a counting
embedding provider. Manual assertions invoke `Command::parse` and
`runtime::execute`, the same boundary used by the binary. Neither production
configuration nor ordinary CI selects fixture URLs/providers or requires private
credentials or model assets. Production `configured` loads offline Qwen and the
real authenticated Notion client before writable composition.

## Original #350 criteria

| Criterion | Production contract and actual assertions |
| --- | --- |
| Opt-in bounded interval; real credential/root/exclusion/model/index/state validated before effects; shared runner/authority; packaging separate | `Config`, `Settings::parse`, `WorkerSettings::index_settings`, `runtime::configured`, `IndexRuntime::compose`. `periodic_configuration_does_not_require_webhook_activation_and_validates_all_effect_inputs` checks every effect input; `missing_offline_model_rejects_periodic_startup_without_creating_state` verifies no index/SQLite creation. Scheduled/manual fixtures share the real composition. Both serving callers retain exactly the same coordinator Arc, provider, guarded table and refresh pipeline. |
| Transactional scoped schedule, deadline, active run, retry state; migration and rollback/reopen | #352 `register_schedule`, `claim_schedule`, runner completion transaction and schema 12; [durable schedule audit](reconciliation-journal.md#scoped-schedule-authority-schema-12) records SQLite authority/fencing/migration/recovery tests. Scheduled restart fixture inspects actual stored deadline/run/cycle. |
| No overlap; Busy budget-free; full authority; no duplicate active run; immutable receipts | `actual_scheduled_http_manual_periodic_and_webhook_contention_preserves_retry_budgets` holds a real scheduled source attempt, admits an actual inbox event, asserts manual Busy, Pending inbox with zero attempts and unchanged schedule; after shutdown the same real webhook worker succeeds. `actual_scheduled_http_cancellation_resumes_same_pending_plan_without_implicit_retry` compares the persisted plan and same run after reopen. Existing exact binding/receipt tests remain covered by #352/#242. |
| Restart deadline/pending resume; graceful admission stop/checkpoint/drain; bounded attempts and between-operation renewal | `actual_scheduled_http_preserves_deadline_restart_and_unchanged_next_cycle`, `actual_scheduled_http_cancellation_resumes_same_pending_plan_without_implicit_retry`, `actual_scheduled_http_shutdown_drains_live_owned_effect_before_release`. The latter proves the real directory guard is owned before cancellation, available after server exit, and the original run completes after restart. `fatal_scheduled_storage_failure_terminates_actual_http_and_both_serving_workers` verifies shared fatal supervision and released lease. Renewal happens only after `apply_next` returns. |
| Bounded failure lifecycle, status/exhaustion, no implicit failed-page retries; explicit failed/new-cycle policy/history | `actual_scheduled_http_transient_source_attempts_back_off_then_exhaust_without_spinning` checks RetryWait, next admission at or after deadline, exactly two events, same run/cycle and no subsequent attempts. `actual_scheduled_http_timeout_exhaustion_is_inspectable_and_explicit_abandon_preserves_history` checks actual status, exhaustion, explicit abandonment and preserved old failure events after a successful fresh cycle. `actual_scheduled_http_holds_failed_pages_until_explicit_manual_recovery` fails the earliest page, checks two later healthy pages still change, no implicit new embedding attempts, zero transient budget consumption, rejected premature schedule retry, a healthy webhook succeeding while the original failed record remains Held, real explicit manual retry and historical failure evidence. |
| Credential-free actual scheduled and manual fixtures; due/notdue/restart/cancel/contention/backoff/differences/orphans/retry/unchanged/dry-run | All scenarios above invoke the actual HTTP lifecycle. `actual_scheduled_http_reprocesses_stale_and_safely_deletes_authoritative_orphans` checks initial missing-page population, then stale counts `(scanned,changed,deleted,unchanged,errors)=(3,1,0,2,0)` and orphan counts `(changed,deleted,unchanged,errors)=(0,1,2,0)`. Deadline/restart fixture checks initial `(3,3,0,0)` scanned/changed/deleted/errors, no effects before due, unchanged next cycle with no body reads or embedding, and manual dry-run byte-identical state. |
| Complete operational configuration/status/recovery/counts documentation without payload logging | [Operator guide](manual-reconciliation.md#periodic-serving-and-explicit-recovery), `.env.example`, sanitized `schedule_command` status/history and run summaries. State/history are durable; no source title/body/URL/credential/upstream error is included. |
| Re-audit #243/#58 and children; leave parents open for owner | The following tables preserve every original criterion and child boundary. #353 closes only its own issue. |

## Original #243 criteria

| Criterion | Evidence |
| --- | --- |
| Real Notion/SQLite/Lance/provider, roots/exclusions, shared updates/deletes; no detection-only path | `IndexRuntime::compose` and the real scheduled/manual fixture matrix; missing, stale and orphan rows produce actual receipt-derived changes/deletions. |
| Validated apply/dry-run/resume/status/explicit retry, sanitized summaries and exits | #349 `Command::parse`, `execute`; `actual_commands_apply_differences_only_and_keep_dry_run_and_status_read_only`, `failed_pages_require_explicit_retry_and_webhook_writer_busy_does_not_consume_work`, missing-path/authority rejection, timeout/cancel assertions. Schedule recovery extends the same boundary. |
| Configured due interval; durable deadline; no overlap; bounded retries; visible failure | #350/#352 APIs plus scheduled deadline, contention, backoff/exhaustion and explicit failure-recovery fixtures above. |
| Graceful cancellation/restart; invalid model/index/state fail before effects | Manual and scheduled cancellation/drain tests, persisted same-plan restart, read-only schema/WAL/missing-path rejection, missing offline model no-creation test. |
| Full command/config/authority/resume/dry-run/recovery/count documentation | [Manual and periodic guide](manual-reconciliation.md), [runner contract](reconciliation-runner.md), [journal contract](reconciliation-journal.md). |
| Real-command and scheduled smoke; differences/orphans/restart/retry/unchanged/dry-run; no private CI assets | Combined real manual/scheduled integration file, actual HTTP serving, SQLite and Lance fixtures with counting provider. |
| Every #58 criterion and children audited; packaging consumes actual lifecycle | #58 table and child table below; #145 is unchanged and remains independently gated. |

## Original #58 criteria

| Criterion | Integrated assertions |
| --- | --- |
| Missing/stale/orphan detection | Initial scheduled cycle populates three missing pages. Stale/orphan scheduled fixture verifies actual changed/deleted counts; manual fixture verifies the same transitions. Unsafe absence/permission/race cases retain retryable state through the inherited planner/runner matrix. |
| Only differences reprocessed | Stale cycle changes one page; orphan cycle deletes one. Unchanged scheduled next cycle asserts unchanged provider and content-read counts. Manual unchanged cycle independently asserts the same. |
| Resumable run | Cancellation retains active identity, same plan and Pending work; actual restart completes that same cycle. Live-owned guard and applied-before-ack replay remain verified by runner tests. |
| Dry-run | Actual manual boundary computes planned changes without effects; snapshots of every SQLite/index file are byte-identical and content-read/provider counts remain zero. Dry-run after a completed scheduled cycle also preserves the snapshot. |
| Scanned/changed/deleted/error summary | Actual initial/stale/orphan/retry/unchanged progress assertions use durable receipt-derived counts; operator status exposes active and completed summaries, classified failures and durable schedule attempts/deadline. |

## Children and shared prerequisites

| Issue(s) | Boundary and retained verification |
| --- | --- |
| #241 | Core journal port and SQLite migration; full normalized root/exclusion/policy/generation binding; inventory/checkpoint/phase/count/deadline; atomic bounded leases/reopen/replay/failure consistency. Journal tests plus actual scheduled same-plan reopen, deadline retention and global contention now exercise these through production serving. |
| #242, children #342/#343 | Authoritative union/provenance, complete traversal and affirmative deletion authority, metadata/content differences, shared guarded refresh/removal, explicit failed retry, immutable receipts and exact distinct counts. The [original #242/#343 criterion audit](reconciliation-runner.md#original-343-and-242-acceptance-audit) maps every criterion to real-adapter tests, including inaccessible/incomplete traversal, archive/move/restore, source races, changed binding, expired fence, applied-before-ack recovery and cancelled explicit retry. These production services are now also exercised by actual commands and HTTP scheduling. |
| #243, children #349/#350 | #349 actual operator boundary and read-only compatibility/no-creation guarantees; #350 actual durable periodic serving. Every original criterion is retained above. |
| #350, children #352/#353 | #352 exact scoped durable scheduling/transaction/migration/recovery; #353 real configured dispatch, shared supervision, cancellation/drain, operational recovery and integration matrix. Storage-only tests are supplemented by actual lifecycle assertions. |
| #55 and refresh descendants #254/#255/#314/#315/#324 | Shared real authoritative canonical preparation, changed-chunk embedding, guarded SQLite/Lance commit, searchable content, idempotency and explicit retry. Existing real refresh tests plus initial/stale/unchanged manual/scheduled provider and receipt assertions retain these contracts. |
| #56 and lifecycle descendants #251/#252 | Affirmative ancestry/exclusion evidence and actual fenced delete/move/restore workflows. Existing authoritative lifecycle/runner archive/move/restore/race tests plus actual scheduled orphan deletion exercise the shared production removal path. |
| #57 and children #247/#248 | Durable bounded event retry/dead-letter policy, actual classified serving worker, operator inspection/retry, bad-page isolation and observable counts. Existing actual webhook-worker matrix remains unchanged in behavior; scheduled three-way contention verifies Busy does not spend inbox budget. Periodic run failures use the same bounded/failure-explicit lifecycle principles with durable schedule-specific evidence. |

## Reproduction

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command --test reconciliation_dispatch --test webhook_worker --locked
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test authoritative_refresh --test durable_plan --locked
nix develop .#spike --command cargo test --workspace --features notion-knowledge-server/webhook-worker --locked
```

The final PR must pass independent exact-head review and all freshly selected CI
checks before merge. The parent owner verifies that evidence and the native
parent/blocked-by/reverse-blocking graph before closing #350/#243/#58.
