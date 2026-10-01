---
name: pick-tickets
description: Pick one open, unblocked leaf GitHub ticket, investigate its size in a subagent, then deliver it through merge or split it into linked subtickets. Use for a next-ticket backlog cycle or a recurring one-ticket goal; use issue-to-merge directly for a named ticket or epic delivery.
---

# Pick Tickets

Drive one ticket cycle. The main driver selects and coordinates the work;
a read-only subagent investigates scope before implementation. Finish this
cycle after verified delivery or decomposition. A subsequent invocation
resumes unfinished work or, after a verified terminal outcome, reads the
current backlog and picks again.

## Goal boundary

When the user explicitly requests a goal, use the goal tools if available.
The objective is to inspect the eligible backlog and either deliver one leaf,
decompose one oversized leaf, or establish that no eligible leaf is available.
Use an existing compatible goal rather than creating a second unfinished one.
An ordinary skill invocation does not implicitly create a goal.

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
