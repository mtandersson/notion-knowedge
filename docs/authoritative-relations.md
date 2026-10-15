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
Planner classification of target-only changes across affected sources and explicit
whole-projection rebuild scheduling remain #384. Every individual operation this
runtime executes already owns its own truthful repair and acknowledgment.

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
