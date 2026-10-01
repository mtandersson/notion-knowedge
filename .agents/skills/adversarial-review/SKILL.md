---
name: adversarial-review
description: Independently review an in-progress change against its issue, architecture, tests, and failure modes before a PR. Use for a read-only pre-merge review or when a change needs a critical second pass.
---

# Adversarial Review

Act as an independent, read-only reviewer. Do not edit files, create GitHub
artifacts, commit, push, or merge.

Read the issue and acceptance criteria, relevant documentation, changed files,
and tests. Challenge behavior and boundary ownership, event ordering and
concurrency, persistence, validation, security, compatibility, accessibility,
and visual behavior where relevant. Check that verification reaches the
production boundary that owns the wiring; a unit test alone may not prove an
MCP transport, WebSocket, or browser requirement.

Report findings by severity with file and line references. For each finding,
state the concrete failure mode and smallest corrective action, and mark it
`blocker`, `in-scope fix`, or `out-of-scope issue`. End with `approve`,
`approve with follow-ups`, or `block`. If there are no findings, name the
acceptance criteria and risks checked.
