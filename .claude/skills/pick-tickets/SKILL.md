---
name: pick-tickets
description: Create a resumable one-ticket /goal when invoked, pick an open, unblocked leaf GitHub ticket, investigate its size in a subagent, then deliver it through merge or split it into linked subtickets. Use for a next-ticket backlog cycle or a recurring one-ticket goal; use issue-to-merge directly for a named ticket or epic delivery.
---

# Pick Tickets

Set up a reproducible `/goal` and drive one ticket cycle. The main driver
selects and coordinates the work; a read-only subagent investigates scope
before implementation. Finish this cycle after verified delivery or
decomposition. A subsequent invocation
resumes unfinished work or, after a verified terminal outcome, reads the
current backlog and picks again.

## Create or resume the goal

An explicit invocation of this skill is a request to create its one-ticket
goal; the user does not need to separately type `/goal`. Merely discovering,
reading, or editing the skill is not an invocation. If the skill is selected
automatically for an ordinary task, obtain an explicit goal request before
creating one, as required by the goal tools.

Before ticket selection or implementation, identify the repository and the
user's backlog scope, exclusions, ordering, and any explicit budget. When goal
tools are available, call `get_goal` to inspect existing state. Reuse a
compatible unfinished goal and resume its cycle; do not replace an unrelated
unfinished goal. If one prevents
creation, report it and ask the user which goal to continue. Respect paused
state and the goal tools' lifecycle rules.

When no unfinished goal exists, call `create_goal` with the following objective,
substituting the actual repository and user constraints. Set `token_budget`
only if the user explicitly supplied one. Do not merely describe a goal or
print a command when the goal tools are available.

```text
In <owner/repository>, use pick-tickets to resume any unfinished ticket cycle;
otherwise inspect the current backlog under <user scope, exclusions, and
ordering> and select one open leaf with no open children, unresolved blockers,
active overlapping PR, or conflicting owner. Investigate its scope in a
read-only subagent. Deliver it through one focused PR, verify required checks,
merge and issue closure; or decompose an oversized leaf into independently
shippable children and verify native parent and blocked-by relationships; or
verify that no eligible leaf exists and report why. Complete after exactly one
verified merged, split, or idle outcome. Preserve the selected issue, branch/PR,
partial split operations, evidence, and remaining work for resumption. Respect
the user's constraints and any explicit budget, and follow the goal tools'
pause and blocked rules. An open PR or exhausted budget is not completion.
```

Confirm that the goal is active before proceeding, and report its objective.
If goal tools are unavailable, return a ready-to-run `/goal <objective>` command
with the same substituted objective and explain that goal creation is
unavailable here. Do not silently run a cycle without its persistent goal.

Mark the goal complete only after its outcome is verified, report the outcome,
and return. Do not select another ticket inside this one-ticket goal. A user
or recurring runner invokes the next cycle; this skill does not install a
schedule or start an unlimited loop. Respect explicit budgets and the goal
tools' rules for unfinished, paused, or blocked work. Running out of budget,
waiting for an answer, or opening a PR does not constitute completion.

## Resume before selecting

Before fresh selection, inspect the active goal, prior handoff, and associated
issue, branch/PR, or partial decomposition for an unfinished cycle. Resume its
selected ticket and remaining work. Fresh-selection exclusions for open
children or an active PR do not disqualify this cycle's own ticket.

For an open PR, continue review, CI, and merge verification. For a partial
split, reuse the existing children, repair missing relationships, and verify
the complete graph before selecting any child. If the previous operation may
have succeeded before interruption, read back its state before retrying.
Recheck current acceptance criteria, dependencies, and ownership; resumption
does not authorize starting blocked work or taking over another owner's work.

Select a fresh ticket only after the prior cycle has a verified merged, split,
or idle outcome, or the user explicitly changes scope. If the prior handoff
cannot identify what to resume, ask the user instead of silently selecting
new work. Keep the selected issue, branch/PR, completed split operations, and
remaining work in the goal context and unfinished handoff so a later invocation
can resume them.

## Pick an eligible leaf

Read root agent guidance and available project documentation. Identify the
repository and default branch from the checkout. Apply the user's backlog
scope, exclusions, and ordering, then inspect open issues and their native
parent, sub-issue, and blocked-by relationships, along with overlapping PRs.

Select an open issue with no open children and no unresolved blockers. Exclude
work already being delivered in an active PR or by another owner. Follow
established priority conventions; absent a defined order, prefer the oldest
eligible issue and explain the choice. Do not invent a priority policy.
Recheck eligibility before editing or creating GitHub artifacts.

If none qualifies, report why (empty backlog, dependencies, or active work)
and return an idle outcome. Do not start blocked work to keep the cycle busy.

## Investigate in a subagent

Give a separate subagent the selected issue, acceptance criteria, repository
guidance, and relevant source/documentation. Ask it to investigate read-only:

- Whether the reported problem and desired behavior match the code.
- Affected boundaries, dependencies, and concrete implementation steps.
- Meaningful tests and other verification required for acceptance.
- Whether the work fits one focused, independently reviewable PR within any
  explicit execution budget, or needs independently shippable subtickets.
- Unanswered decisions, risks, and recommended next action, with evidence.

The investigator must not edit, commit, create issues, or push. The main
driver evaluates its findings and owns the decision. If subagents are
unavailable, report that limitation and investigate directly.

The main driver is allowed to ask the user questions when intent, acceptance
criteria, tradeoffs, or scope remain unclear after investigation. Recommend
an answer and explain the consequence briefly. Resolve repository facts from
the code and issues first. Continue independent investigation while waiting;
do not guess a consequential decision or treat silence as agreement.

## Deliver a focused ticket

If the scope is clear and fits one PR, use
[issue-to-merge](../issue-to-merge/SKILL.md) with this specific issue. Follow
its implementation, behavior testing, independent adversarial review,
Conventional Commit, PR, CI, and merge workflow. Preserve the investigation
findings so delivery does not repeat ticket selection.

Verify that the PR merged into the intended default branch and the selected
issue closed after its acceptance criteria were satisfied. If the work proves
larger during implementation, reassess and decompose it rather than expanding
the PR indefinitely. Preserve any partial work and report its disposition.

## Split an oversized ticket

Split based on implementation boundaries and acceptance criteria, not a fixed
line count or difficulty alone. Unclear requirements need clarification;
an unavailable credential or failing check needs resolution. Neither is by
itself evidence that subtickets are needed.

Create the smallest useful set of independently shippable child issues. Give
each a concrete outcome, acceptance criteria, scope, and verification plan.
Together they must cover the selected issue's criteria without duplicating
work. Search for existing children or equivalent issues before creating new
ones; reuse them when appropriate, including on a retried cycle.

Attach children using native GitHub sub-issue relationships. Add native
blocked-by edges from each dependent child to its prerequisites. Carry forward
external blockers to the children they affect, preserve existing valid links,
and avoid cycles. Allow independent children to remain unblocked. Body links
can explain the plan but do not substitute for native relationships.

Read back the child and dependency relationships to verify the graph. Update
the selected issue with the decomposition and leave it open as the parent
until its work is delivered. End this cycle after verified decomposition;
the next invocation can select an eligible child. If relationship APIs fail,
report the partial state and recover it before declaring this cycle complete.

## Report and return

Report the selected issue and one of these outcomes:

- **Merged:** PR link, review outcome, relevant checks, verified merge and
  issue closure.
- **Split:** Parent and child links, dependency order, verified native graph,
  and the acceptance criteria covered.
- **Idle:** No eligible leaf and the observed reason.
- **Unfinished:** Selected issue, branch/PR, existing children and graph
  operations, remaining work, and any unanswered decision or impediment needed
  to resume this cycle before fresh selection.

Only verified merged, split, or idle outcomes satisfy this cycle's objective.
For a budgeted goal, include final usage as required by the goal tools.
