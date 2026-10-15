# Installed authoritative relation commits

`IndexRuntime::compose` installs one `AuthoritativePageRefresh`; its clones serve
the webhook worker and the same runtime's manual/periodic reconciliation runner.
The refresh owns a `RelationProposalAdapter` and its shared current-policy owner.
Every live page operation derives stable-ID relation membership and display names
from one complete, bracketed normalized Notion source snapshot. Candidate IDs
receive fresh physical ancestry checks using the configured workspace, roots and
exclusions. Denied, excluded, inaccessible and broken references persist as
unresolved references, never as authorization. Proposal `targets()` retains
per-candidate sanitized diagnostics. No relation causes target Markdown reads,
crawling, discovery, or target-content indexing.

## Ordering and authority

Preparation and embeddings occur before the operation owns the index. The owned
thread then acquires the cross-process directory guard, validates SQLite binding,
lease and generation, reopens the actual Lance version, and revalidates full
source content/membership/names and every target observation. The returned
`RelationAuthority` stays in operation-owned state through vector replacement,
FTS/vector maintenance and the final SQLite transaction. That transaction replaces
only relation-family graph rows, writes exact versioned completeness, and records
the checkpoint, metadata identity and Applied receipt together. Unrelated link
edges survive, including source tombstones and physical scope removal.

Graph or final-ack failure rolls back the whole final SQLite transaction. The
previous valid checkpoint and graph remain committed; newer Lance rows may
already exist. The newer receipt remains unacknowledged, so existing pending-work
serving gates continue to fail closed. Restart reopens both stores, prepares from
current Lance, reuses matching vectors, reauthorizes current Notion state and
replays the same operation. Lease/binding/generation loss rejects the operation;
dropping a caller's observer never releases the owned thread or directory guard.
Owned no-op validation also polls the lease while source I/O is pending: lease
loss releases that guard even when an upstream response remains paused.
Admission and commit fencing share the existing live-reconciliation-owner rule.
Held, exhausted or failed runs retain history and explicit-retry requirements,
without reserving a live writer fence that blocks unrelated webhooks. Current
SQLite lease/binding/generation and truly active owner exclusivity remain required.
Notion has sequential observations rather than atomic source CAS: later upstream
edits need another refresh.

## No-op, immutable receipts and recovery

The unchanged direct-refresh shortcut requires exact graph/completeness,
checkpoint, metadata and canonical chunks. It rechecks those under the owned guard
with fresh source/target authority before returning `AlreadyApplied`. Valid empty
live projections are complete; empty tombstones are a distinct effect. Missing,
modified or intentionally invalidated effects and old versions/generations do not
qualify as complete.

An Applied receipt never changes. If its current effect needs repair, the refresh
creates a separate operation named `relation-repair:<original-ID hash>:<sequence>`.
The latest Pending/Failed repair retains its ID across restart; another repair
after an Applied attempt advances the sequence. An unfinished repair must still
replay if the source reverts and the previous graph becomes complete: its partial
Lance effects and unacknowledged receipt cannot be hidden by a no-op. The repair follows the full
source, lease, vector, graph and receipt lifecycle. Concurrent observers may race
and receive a conflict; retry uses durable current state. A complete Applied replay
is revalidated under the guard and causes no new graph or vector effects.
An old acknowledged delete cannot erase a later restored snapshot: that replay
requires the restored snapshot's current projection to be complete and otherwise
fails closed. A distinct authoritative hint owns any new removal.

Reconciliation's unchanged operation verifies a full relation proposal and installs
its exact graph in the same transaction as its no-op receipt, without embedding or
Lance mutation. Applied recovery runs the shared refresh first, including any
stable-ID repair, then reacquires source/target/policy authority and checks exact
projection completeness inside guarded journal acknowledgment. Repair failure
leaves journal work unsuccessful. The historical journal outcome refers to the
original immutable receipt; the repair's separate receipt establishes current
projection truth. Explicit failed-page retry retains this association across
restart.

## Compatibility and scope

Schema 15's exact projection/version contract is unchanged. Read-only and existing
startup do not migrate or repair. Current committed-snapshot, pending-receipt and
trusted transport gates remain in force. Names remain a projection-only sidecar;
canonical `IndexedMetadata`, `PageIds`, chunk and Lance schemas do not change.
This does not install `knowledge_related`, aliases, link extraction or graph boost.
The production planner compares exact relation effects against fresh normalized
source metadata and physical target evidence, including empty projections.
Target-only moves, exclusions, inaccessible/broken ancestry and re-entry classify
an affected unchanged source as stale. Planning reads no Markdown and writes no
state. Its observations remain proposals: application prepares full source
content and reacquires current source/target/policy authority under the guard.
Completed tombstones are omitted only when their exact empty projection is
complete; missing or incompatible tombstone projections schedule guarded removal
repair. A valid empty live projection remains unchanged.

## Original #383 acceptance audit

The following maps every original criterion to installed production boundaries and
behavior tests, rather than treating the preceding standalone capabilities as
installation evidence. Commands and observed verification are reported in the PR.

| Original criterion | Implementation and integrated evidence |
| --- | --- |
| One production refresh for webhook, manual and periodic | `IndexRuntime::compose` shares handler/runner ownership; `actual_scheduled_http_manual_periodic_and_webhook_contention_preserves_retry_budgets` asserts committed labels/completeness after all three callers. |
| Authorized normalized source, authoritative names, stable provenance, fresh physical targets; no expansion | `RelationProposalAdapter::prepare/revalidate` inside shared refresh; `installed_relations_replace_only_their_family_and_repair_both_noop_paths` covers normalized source to SQLite to existing navigation rename, foreign/inaccessible references and absence of indexed target content. `installed_target_authority_race_rejects_effects_and_excluded_targets_stay_unresolved` checks a target authorization race and exclusions. |
| Owned fenced graph before receipt/checkpoint/journal success | `commit_page_with_authority` and `submit_with_authority` retain authority through `finish`; `returned_commit_authority_is_held_until_final_sqlite_acknowledgment` observes Applied only when authority drops. Graph/final-ack failure tests assert the prior checkpoint and graph survive. |
| Initial/repeat/edit/rename/removal/tombstone/scope removal replace relations only | Installed relation test checks initial/repeat, rename, removal, zero-edge, tombstone, scope removal and preserved link edges; graph/final-ack restart test edits source content and labels. |
| Both no-op paths repair absent/stale/invalid effects, valid zero-edge contract | Installed relation test removes graph rows, modifies labels, deletes completeness, repairs immutable Applied receipts, races repairs and checks valid empty live/tombstone projections. Old receipt remains byte-for-byte equal. |
| Failure, cancellation, fencing, restart and truthful reconciliation acknowledgment | `relation_graph_and_final_ack_failures_preserve_checkpoint_and_replay_after_restart`; `reconciliation_recovers_missing_applied_projection_with_durable_repair_before_ack`; installed `reconciliation_cancelled_observer_retains_owned_commit_then_recovers_real_receipt`, `reconciliation_owned_source_poll_expires_lease_and_new_fence_resumes_without_effects` and binding-change tests exercise real owned refresh. `unchanged_owned_source_poll_releases_guard_after_cancelled_observer_and_lease_loss` proves guarded no-op release while upstream stays paused; the active-owner test in `index_commits` still rejects a contender, and `actual_scheduled_http_holds_failed_pages_until_explicit_manual_recovery` preserves held failures while an unrelated webhook succeeds. |
| Ordering, partial-effect recovery, completeness compatibility, retained gates | This document and `index-commits.md`; canonical server tests retain startup/read-only/transport/current-snapshot/pending-work checks. No serving capability or canonical schema expands. |

Parents #383 and #374 remain open for their own integrated acceptance audits.

## Explicit derived relation rebuild

Keep the configured Notion scope, index/vector identity and SQLite binding.
Back up local state using [the recovery runbook](local-state-recovery.md), then
use the existing operator environment and a new run ID:

```sh
notion-knowledge-server --reconcile dry-run
notion-knowledge-server --reconcile rebuild-relations relations-2026-10-15
notion-knowledge-server --reconcile status relations-2026-10-15
notion-knowledge-server --reconcile resume relations-2026-10-15
notion-knowledge-server --reconcile retry relations-2026-10-15 PAGE_UUID
```

`rebuild-relations` requires an existing compatible index/state, configured
Notion authority and a new run ID. It acquires the same global writer lease as
ordinary reconciliation. The runner's `begin_relation_rebuild` uses coordinator
owned maintenance to atomically create inventory and invalidate only the current
generation's derived completeness markers under the directory lock and current
SQLite binding/fence checks. Failure rolls back both changes. Existing graph
rows, unrelated links, checkpoints, immutable receipts, queued webhooks,
schedules and journal history survive. Neither SQLite nor Lance is deleted.

The ordinary planner and durable runner then reconstruct each affected source
through the full fenced refresh and graph-before-acknowledgment transaction.
Valid zero-edge pages also receive repaired completeness. Content vectors are
reused when unchanged. Source tombstones and physical scope removals receive
empty tombstone projections. A graph failure retains the previous graph and
checkpoint and leaves failed/pending work unsuccessful; resume recovers pending
work, while explicit `retry` replans a failed page. A crash after invalidation
reopens the committed inventory run and plans repairs; a crash before that
transaction commits has made neither change. A completed run cannot be used to
request another rebuild: use a new ID. Read-only status, dry-run and startup
never invalidate or repair. Model/vector incompatibility still requires the
separate index recovery procedure; this command rebuilds derived relations only.

Credential-free verification:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test authoritative_refresh --locked
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test reconciliation_command --locked
```

## Original #374 acceptance audit

| Original criterion | Installed boundary and joined evidence |
| --- | --- |
| Reuse relation extraction/schema; stable provenance, multiple relations, idempotence | `RelationProposalAdapter`, `RelationProjection` and the owned refresh use existing extraction/graph storage. `installed_relations_replace_only_their_family_and_repair_both_noop_paths` covers multiple targets and repeated updates; `reconciliation_converges_target_only_changes_and_rebuilds_derived_relations` covers deduplicated memberships through actual runner receipts. |
| Physical target authority without discovery/content expansion; fresh revalidation | Adapter physical checks and owned `revalidate`; the reconciliation convergence test moves a target never present in traversal through resolved/unresolved/re-entry/access changes and checks no target chunks/Markdown. `target_only_excluded_and_broken_ancestry_converge_without_source_changes` covers exclusion, broken ancestry and both re-entries. `installed_target_authority_race_rejects_effects_and_excluded_targets_stay_unresolved` retains commit-time target race proof. |
| Authoritative human names and stable property identity, normalization through navigation | Both installed relation and reconciliation convergence tests rename the normalized Notion property, apply the actual refresh/runner, and query existing navigation for the new display name with unchanged `relation:related-stable` identity; stored provenance remains `property:related-stable`. |
| Graph before receipt/checkpoint; safe partial effects, cancellation, lease loss and restart; preserve links | Operation-owned authority is retained through final SQLite acknowledgment. `relation_graph_and_final_ack_failures_preserve_checkpoint_and_replay_after_restart`, `returned_commit_authority_is_held_until_final_sqlite_acknowledgment` and the cancellation/lease/fence scenarios in the #383 audit above remain prerequisite evidence. The explicit rebuild restart test reopens inventory, injects graph failure, reopens again and explicitly retries, asserting unsuccessful receipts/journal counts until graph success and immutable historical receipts throughout. |
| Remove stale source relations and converge target-only/missing/rebuilt state without no-op suppression | Planner compares exact current version/generation/rows, including zero edges and tombstones. Installed tests cover property removal, source tombstone and scope removal. New runner tests cover target-only changes, removal, zero-edge completeness, missing rows, incompatible version, explicit rebuild and tombstone repair; existing Applied recovery test proves separate durable repair before acknowledgment. |
| Real authoritative refresh/reconciliation with typed upstream and real stores | All named integration tests use the actual typed Notion HTTP adapter, guarded on-disk LanceDB, SQLite receipts/checkpoints/journal and actual refresh/runner. `explicit_relation_rebuild_uses_shared_runtime_and_preserves_operational_history` exercises the real operator command and shared `IndexRuntime`, verifies graph/completeness/history and rejects duplicate rebuild IDs without mutation. |
| Compatibility, rebuild/commit ordering, original gates and no credentials | Schema 15 remains unchanged; the procedure above retains durable coordination and links. Existing server read-only startup/status/dry-run, current-snapshot, pending-receipt and transport tests remain in canonical checks. No new retrieval tool, alias, link extraction or graph boost is installed. |

This leaf's PR closes only #384. Parent #374 and epic #8 require their
coordinator's final audit and closure.
