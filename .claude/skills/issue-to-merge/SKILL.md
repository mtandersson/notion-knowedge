---
name: issue-to-merge
description: Deliver a named GitHub issue, epic, or ticket set through focused PRs and merge, running independent sub-issues in parallel when asked. Use pick-tickets for next-ticket backlog selection and recurring one-ticket goals.
---

# Issues to Merge

Run one issue when the user names one leaf or pick-tickets hands one off. When
the user asks to work an epic or multiple tickets, schedule its unblocked
leaves in parallel and continue with dependent leaves as their blockers merge.

## Select and scope

If the user names an issue or epic, or pick-tickets supplies a selected issue,
use it. For next-ticket backlog work without a named issue, use
[pick-tickets](../pick-tickets/SKILL.md) for selection and investigation, then
return here for delivery of that selected issue. Check each ticket's
parent, sub-issues, dependencies, the current default branch, and overlapping
open PRs.
Read `README.md` when present and the relevant documentation and source.

Keep the PR to one clear change. For a broad issue, create independently
shippable child issues, connect them through GitHub's native sub-issue and
blocked-by relationships, verify the links, and end this iteration. Search for
an existing issue before filing an unrelated finding; keep that work separate.

## Implement and review

Give each active ticket one owner, branch, worktree, and PR. Start each from
the current remote default branch. Parallelize only tickets without an unmet
blocked-by relationship; coordinate overlapping files before editing and rebase pending
branches after another ticket merges. Follow each issue's acceptance criteria
and add behavior tests where they establish a meaningful contract. Use
`$writing-tests` when writing tests.

Before the PR, ask a separate subagent to use `$adversarial-review` against
the issue, diff, relevant documentation, and verification evidence. The review
is read-only. Fix in-scope blockers and repeat the review after material
changes. File valid out-of-scope findings as separate issues.

## Verify and ship

Run the narrowest useful local checks, then any relevant build, lint, race,
and browser checks. Review the final diff. Make a Conventional Commit with a
short explanatory body; mark a breaking change in the commit message. Push
and open a PR that closes the issue. Inspect the repository CI and merge once its relevant jobs pass. Confirm the PR merged and the issue closed. If no required
checks are configured, state that and rely on the observed CI and local checks.

Report each issue, PR, review outcome, checks, and merge state. For an epic,
verify its child and dependency relationships and finish when the requested
ticket set reaches its terminal state.
