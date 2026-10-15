# Guarded authoritative reconciliation

`retrieval::runner::AuthoritativeReconciliationRunner` applies the concrete
`AuthoritativeReconciliationPlanner` result through `AuthoritativePageRefresh`.
It uses the same coordinator `Arc`, guarded Lance table, SQLite state, workspace,
complete root/exclusion `webhook-v1` scope, generation and epoch as the production
webhook worker. Construction rejects incompatible planner/refresh ownership.
This application service is used by the [manual operator command](manual-reconciliation.md); the opt-in periodic HTTP caller uses the same runner and coordinator.

## Execution and recovery

Call `dry_run()` to inspect the exact real-run planner without extracting Markdown,
embedding, writing operational metadata, changing source/index state or recording
acknowledgments. A later `apply(run_id, lease, clock, provider)` plans and records
real work atomically when the run is new, or resumes its persisted pending items.
Successful runs complete automatically. Application failures remain separate from
successes and keep the run open for `retry_page`; planning failures are retained
in completed summaries and require a new inventory/run. `complete` can explicitly
finish a run with failed work retained; further attempts then require a new run.

Missing/stale/restored candidates use the shared canonical refresh pipeline.
Orphans use shared authoritative lifecycle removal. Deletion requires a complete
traversal recorded in the original run and fresh affirmative negative evidence;
absence, permissions or failed reads never authorize it. Unchanged candidates
perform full bracketed source/relation, lifecycle and local validation and record
an actual guarded no-op receipt, without embedding or changing Lance membership.
Fresh relation membership and labels require authoritative Markdown/page reads
even when the planner classified a page as unchanged. Its exact relation
projection is installed in the same SQLite transaction as the no-op receipt.

Every application proposal carries complete lifecycle/local snapshot digests and
exact metadata identity. These are comparison targets, never authority. The shared
lifecycle pipeline checks them before preparation and again inside its actual
operation-owned directory lock, alongside fresh complete source verification,
SQLite fencing and binding validation. Lance checks its freshly prepared version
inside the lock. Per-page local checks let earlier commits in this run change the
index without invalidating unrelated pages. A racing source edit/move/restore or
webhook write conflicts rather than changing the proposal's action or overwriting
newer local state. A planned deletion whose source becomes Allowed cannot commit
or be counted as deleted.

`retry_page` requires a failed page and obtains a fresh full planner result. Schema
11 persists its fresh payload-free retry proposal and Failed-to-Pending transition
in one transaction, retaining the original stable operation ID. Cancellation or
restart before preparation preserves this explicit intent. Fresh source/local/
policy checks still run inside the owned boundary. A new deletion cannot bypass
an originally incomplete traversal; finish that run and use a complete new run.

The owned commit records its exact prepared operation before effects, applies
Lance, records the actual SQLite Applied receipt/checkpoint, and only then
acknowledges the journal. A crash before journal acknowledgment leaves Pending
work and an immutable real receipt. Recovery validates current authority, source
accessibility and fencing under the owned guard, then acknowledges that historical
target. A newer source revision never rewrites or reinterprets an Applied receipt;
a subsequent new run handles its difference. A crash before checkpointing leaves
unacknowledged index effects; fresh guarded explicit retry recomputes replacement
or removal rather than fabricating an Applied receipt.

## Lease, cancellation and summaries

The caller acquires the same global `ReconciliationJournal` writer lease used by
webhook processing. Lease duration is 1–3600 seconds. Reconciliation checks expiry
between pages; owned lock acquisition and source revalidation are bounded by the
submitted lease. Source polling checks expiry every 20 ms, as in the serving
worker. A caller may renew with `renew_lease` between application attempts or
release/reacquire after expiry, then resume the same run under its new fence.
Renewal does not extend an already submitted detached operation's expiry bound.
A page-source preparation future can be cancelled normally; once submitted, the
operation owns its thread/runtime/lock and finishes any live effects even when
its observer is dropped. Cancellation leaves work Pending; resume reads the
actual receipt. Lease loss prevents stale acknowledgment or further application.

`DurableProgress` derives counts transactionally from persisted rows: `scanned`
is distinct successful inspections; planned refresh/delete remain separate;
`changed`, `deleted`, `unchanged` use actual receipt actions. Distinct planning and
application failures are available separately; `failed_pages()` deduplicates page
IDs and `run_failures` retains classified run errors. Duplicate application,
acknowledgment, retry, process reopen and resume cannot increment counters twice.
Source, index, conflict and planning permission/authentication errors remain
classified. Successful pages cannot be retried; one failed page does not stop
unrelated safe candidates.

## Original #343 and #242 acceptance audit

The following tests use fake authoritative HTTP, a counting embedding provider,
real SQLite journal/checkpoints/receipts and guarded on-disk LanceDB. The service
uses production adapters rather than an application callback double.

| Original criterion | Concrete contract and evidence |
| --- | --- |
| #242 inventory/provenance; #343 concrete plan and shared ownership | `AuthoritativeReconciliationPlanner::{plan,local_inventory}`, `production_scope`, runner construction and `AuthoritativePageRefresh::is_bound_to`; `planner_rejects_foreign_binding_unowned_index_and_refreshes_legacy_identity`, `reconciliation_generation_policy_and_expired_fence_reject_pending_plan_without_effects`, `reconciliation_binding_change_during_owned_precommit_rejects_effect_and_preserves_resume` |
| #242/#343 differences-only refresh/removal; unchanged avoids embedding/Lance mutations | Runner `apply_one`, shared `apply_lifecycle_expected`, `apply_unchanged`; `reconciliation_dry_run_real_apply_and_unchanged_resume_have_exact_receipts_and_counts`, `reconciliation_stale_archive_move_restore_and_incomplete_traversal_apply_only_safe_differences` |
| #242/#343 complete traversal plus affirmative negative authority; fail closed on access/interruption/policy/races | Planner rejects incomplete traversal deletion; expected checks plus `verify_source` run under `GuardedChunkTable::commit_page`; `planner_absence_access_failures_and_races_never_authorize_deletion`, lifecycle interruption/expiry tests, `reconciliation_stale_archive_move_restore_and_incomplete_traversal_apply_only_safe_differences`, `reconciliation_generation_policy_and_expired_fence_reject_pending_plan_without_effects`, `reconciliation_binding_change_during_owned_precommit_rejects_effect_and_preserves_resume` |
| #343 planned deletion becomes Allowed; #242 concurrent edits/webhook writes/generation | Expected action/lifecycle/metadata/local contract rejects implicit action changes; explicit persisted replan; `reconciliation_planned_delete_restoration_and_webhook_write_conflict_require_explicit_replan`, `reconciliation_inflight_webhook_and_authoritative_move_cannot_overwrite_new_local_state`, `reconciliation_bad_page_isolated_source_and_embedding_races_are_durable_explicit_failures`, authority-change test above |
| #242/#343 apply before ack, crash/restart, immutable real receipts | Durable preparation inside owned callback, actual commit then journal ack; `reconciliation_applied_before_journal_ack_reopens_with_new_fence_and_preserves_historical_target`, existing `original_event_recovers_after_index_apply_before_ack_and_changed_source`, durable receipt identity tests |
| #242/#343 distinct successes/failures, explicit retry and independent bad page | `apply` skips failed/successful pages, `retry_page` atomically stores fresh proposal; `reconciliation_index_failure_retains_separate_failed_work_until_explicit_retry`, `reconciliation_bad_page_isolated_source_and_embedding_races_are_durable_explicit_failures`, `reconciliation_owned_source_outage_is_source_failure_without_index_mutation`, `reconciliation_cancelled_explicit_retry_reopens_its_persisted_fresh_proposal` |
| #343 bounded renewal/cancellation and owned effects | Shared coordinator lock wait and 20 ms owned source lease polling; `reconciliation_cancelled_observer_retains_owned_commit_then_recovers_real_receipt`, `reconciliation_owned_source_poll_expires_lease_and_new_fence_resumes_without_effects`; `reconciliation_owned_lock_wait_expires_without_releasing_another_live_owner` and coordinator `independent_processes_serialize_aliases_after_caller_and_parent_runtime_drop`, `independent_process_cannot_bypass_active_guard_with_other_state_or_expired_lease`, `expiry_during_effect_never_acknowledges_but_keeps_guard_until_completion` |
| #242/#343 exact durable scanned/changed/deleted/errors and separate plans | `DurableProgress`, actual prepared-receipt handshake; `reconciliation_mixed_inventory_counts_missing_stale_restored_orphan_and_unchanged_once` verifies a single six-page mixed inventory with exactly three changed, one deleted and two unchanged; all runner tests assert resulting durable counts/state, plus `actual_delete_and_unchanged_receipts_count_once_and_failed_effect_requires_explicit_retry` and retry transaction/reopen tests |
| #242/#343 exact dry-run, no mutations/suppressive acknowledgments | `dry_run` delegates directly to `plan`; no operational metadata persists; `reconciliation_dry_run_real_apply_and_unchanged_resume_have_exact_receipts_and_counts` |
| #242/#343 full deterministic real-adapter matrix | Fifteen `reconciliation_*` integration scenarios above, existing planner/lifecycle guarded-source tests, real SQLite migration/reopen/rollback tests. CLI/scheduler #243, sync-status #59 and #58/#7 final parent audits remain separate. |

Run the acceptance tests with the pinned optional-feature toolchain:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test authoritative_refresh --locked
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test durable_plan --locked
nix develop .#spike --command cargo test --workspace --features notion-knowledge-server/webhook-worker --locked
```

Schema 11 is an additive atomic migration preserving schema 10 plans, receipts,
checkpoints, failure classes and legacy journal state. Retry proposals store only
IDs, digests and action classification. Tests inject migration/transaction failure
and reopen with independent coordinators; failed migration retains the prior
schema and operational rows.


Relation-aware Applied recovery first runs the shared refresh using the original
receipt identity. Missing or incompatible effects execute a separate stable-ID
repair, preserving the historical Applied receipt. Recovery then reacquires the
full source/target policy authority under the directory guard and checks the exact
current projection before acknowledging historical journal work. A graph failure
cannot acknowledge that work. The historical outcome still describes its original
operation; the repair's own receipt attests current projection completeness.
See [installed authoritative relations](authoritative-relations.md).

Explicit `begin_relation_rebuild` atomically creates the inventory run and
invalidates derived relation completeness within owned maintenance, under the
directory guard and current binding/fence. Ordinary apply/resume then plans and
commits repairs through the same refresh; failed invalidation creates neither a
run nor partial invalidation. See the [operator procedure](authoritative-relations.md#explicit-derived-relation-rebuild).
