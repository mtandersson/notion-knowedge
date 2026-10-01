---
name: pick-tickets
description: Drain the eligible GitHub backlog under a persistent goal by delegating one ticket at a time through review and merge, refreshing dependencies after each outcome. Use for /goal pick-tickets; use issue-to-merge directly for a named ticket.
---

# Pick Tickets

Coordinate a continuing backlog run. Pick one eligible ticket, hand it to a
worker subagent for delivery, verify its outcome, then refresh the backlog and
repeat. A verified merge or completed split ends that assignment. A worker exit
triggers verification and, if needed, resumption. Neither completes the overall
goal. Complete only when a fresh audit proves there are no eligible tickets
and no unfinished assignments from this run.

## Invocation and goal lifecycle

The intended entry point is:

```text
/goal pick-tickets
```

Also recognize `/goal $pick-tickets` and explicit `$pick-tickets` invocations.
An explicit invocation authorizes this backlog workflow, including ticket
workers, independent reviews, focused PRs, and merges after verification.
Merely reading or editing this skill does not start a backlog run.

Identify the repository from the checkout and apply any user scope,
exclusions, ordering, and budget. Default to the repository's entire open
backlog. Inspect `get_goal` before selection. Reuse an active goal whose
objective names this skill or describes this workflow; interpret the short
skill name using these instructions without trying to replace the goal.
Respect paused goals. Do not replace an unrelated unfinished goal.

If explicitly invoked without an unfinished goal, use `create_goal` with the
following objective, substituting the repository and user constraints. Set a
token budget only when explicitly supplied:

```text
In <owner/repository>, use pick-tickets to drain the eligible backlog under
<scope, exclusions, and ordering>. Resume unfinished assignments first.
Select one open, unblocked leaf at a time and delegate its investigation,
implementation, testing, independent review, PR, and merge to a worker using
issue-to-merge. Verify merge and issue closure, or verify decomposition into
native child/dependency relationships. After each verified outcome, refresh
the backlog and select again, including newly unblocked or created children.
Complete only after a fresh full-backlog audit proves no eligible tickets
remain and this run has no unfinished work. Preserve evidence and handoffs
across turns. Follow user budgets and the goal lifecycle rules.
```

Confirm the active goal and scope. If goal tools are unavailable, explain the
limitation and provide `/goal pick-tickets` for a supporting environment.
Do not start an untracked continuing run.

## Resume before selecting

Inspect the current goal context, prior handoff, worktrees, branches, PRs, and
partial splits. Resume this run's unfinished ticket before assigning a new
one. Its own PR or children do not disqualify it from resumption.

Check the worker's actual status. If it is still running, keep that assignment
and observe it. If it exited, inspect authoritative GitHub and checkout state:
verify its claimed outcome, or hand the remaining work to a replacement worker.
An exited worker, observation timeout, or transient API error is not evidence
of delivery. Never create a duplicate worker for a live assignment.

For an open PR, resume checks, review fixes, and merge. For a partial split,
reuse existing children and repair missing relationships. Read back operations
that may have succeeded before retrying. Recheck acceptance criteria,
dependencies, and ownership. Recover ambiguous handoffs from current state;
ask only if conflicting evidence prevents identifying the assignment.

Preserve a compact handoff in the thread before yielding: repository and scope,
selected issue, worker identity/status, worktree/branch/PR, verified outcomes,
partial graph operations, live process/check handles, and next action.

## Select from the current backlog

Read repository guidance and available project documentation. Refresh the
remote default branch, all in-scope open issues (paginate), native parent,
sub-issue and blocked-by relationships, and overlapping open PRs.

Select an open leaf with no open children or unresolved blockers. Exclude work
assigned to an external owner or covered by another active PR. This run's own
worker ownership belongs to resumption, not exclusion. Follow established
ordering; absent a policy, select the oldest eligible issue. Recheck eligibility
before assigning work or mutating GitHub. Do not invent priority labels.

Run one ticket worker at a time. After a merge or verified split, refresh the
backlog instead of using a stale selection queue. This lets newly unblocked
tickets and newly created children enter the next selection.

## Delegate delivery

Create an isolated worktree and branch from the current remote default branch,
preserving unrelated user changes. Spawn a worker with the selected issue,
acceptance criteria, verified dependencies, repository guidance, worktree,
user constraints, and this assignment:

> Deliver this specific ticket using issue-to-merge. Investigate its scope,
> implement it, run relevant checks, obtain independent adversarial review,
> fix in-scope findings, open one focused PR, verify CI, merge, and verify issue
> closure. If genuinely oversized, decompose it into independently shippable
> children and verify native parent and blocked-by relationships. Report the
> evidence and any unfinished state. Do not select another backlog ticket or
> create, complete, or replace the parent goal.

The worker owns the ticket through its terminal outcome. It uses
[issue-to-merge](../issue-to-merge/SKILL.md), including a separate read-only
[adversarial-review](../adversarial-review/SKILL.md) agent before the PR and
[writing-tests](../writing-tests/SKILL.md) for behavior tests. Reserve capacity
for that reviewer. The coordinator owns selection, resumption, and overall
goal completion. If worker tools cannot perform an authorized operation, the
coordinator may carry it out using the worker's concrete, verified evidence.
If subagents are unavailable, report that limitation and continue useful local
investigation. Preserve unfinished state when delegation or the required
independent review cannot be performed; do not claim those gates passed.

Stay responsive while the worker runs. Surface consequential missing decisions
with a recommendation; continue independent investigation where possible.
Do not treat silence as approval. A technical error, unavailable credential,
or unclear requirement alone does not justify splitting a ticket.

## Verify the assignment and repeat

Inspect authoritative state after every worker exit:

- **Merged:** Verify the PR merged into the intended default branch, applicable
  checks and independent review passed, acceptance criteria were satisfied,
  and the selected issue closed. Record evidence and refresh the default branch.
- **Split:** Verify the smallest useful child set covers the parent's acceptance
  criteria, native parent links and blocked-by edges are correct and acyclic,
  external blockers are carried to affected children, and the parent remains
  open. Reuse equivalent existing issues. Body links alone are insufficient.
- **Unfinished:** Preserve the worktree, PR, partial split, and evidence. Resume
  the same assignment; never silently skip it because the worker exited.

After a verified merge or split, report brief progress and immediately select
again in the same goal. A split is a way to make work shippable; its eligible
children remain part of this run. Do not ask the user to invoke the skill again
between assignments.

## Completion and interruptions

When selection finds no candidate, audit the complete current in-scope backlog
and this run's unfinished state. Account for every remaining issue as a parent
with open children, blocked by named open prerequisites, externally owned, or
covered by another active PR. Distinguish an empty backlog from a backlog with
no currently eligible work. A failed API read or incomplete page is not an
empty-backlog result.

Only call `update_goal` with `complete` when the audit proves no eligible issue
remains and there is no unfinished assignment, live worker, open PR belonging
to this run, or partially verified decomposition. Report merged PRs, splits,
verification, and the reasons any issues remain. Do not wait indefinitely for
external owners or future tickets once this audit passes.

If progress is prevented by a tooling, permission, credential, or unresolved
user-decision blocker, preserve the goal and handoff. Follow the goal tools'
blocked audit threshold; never label unavailable backlog evidence as successful
completion. Pause only on explicit request. Budget exhaustion, yielding a
turn, or one ticket's terminal outcome does not complete the backlog goal.
