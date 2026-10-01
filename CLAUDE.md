# Repository agent guidance

`AGENTS.md` links to this file. Read `README.md` and `CONTRIBUTING.md` when
present before broad searches. Consult relevant documentation, source, and
tests for the area being changed. Source and tests take precedence when a
document is stale. Discover the actual project structure and toolchain; do not
assume a language, framework, directory layout, or build command.

## Implementation and checks

Keep changes focused on the requested outcome and follow existing conventions.
Check the working tree before editing and preserve unrelated user changes.
For a new project, document setup and verification commands when introducing
the toolchain.

Find supported commands in project documentation, manifests, build scripts,
and CI configuration. Run checks relevant to the change, including tests,
lint, type checking, and builds where configured. Fix failures caused by the
change; report pre-existing failures or unavailable checks with their impact.
For visual changes, inspect the rendered UI as well as its behavior.
Documentation-only or other reversible, low-impact edits do not require tests
that mirror the implementation.

## Issues and pull requests

Use GitHub issues as the unit of work for issue-driven development. Keep each
PR focused on one issue. When an issue is too broad, create independently
shippable sub-issues and use native parent and blocked-by relationships to
show the order. Check for an existing issue before creating one for an
unrelated finding; keep that finding out of the current PR.

Use existing issue labels where applicable. Do not add priority labels without
an agreed policy. Use Conventional Commits; mark breaking changes with `!` or
a `BREAKING CHANGE:` footer. Read `CONTRIBUTING.md` when present for additional
conventions. Independent tickets may run in parallel when requested, each in
its own worktree and PR. Start a blocked ticket after its dependency merges.
Confirm relevant CI checks before merging, and verify the PR and issue state
afterward. A single-ticket delivery run ends after its PR merges or the issue
has been decomposed. Follow the user's requested delivery scope for other work.

## Repository skills

The repository skills live in `.claude/skills` and are also available through
`.agents/skills` and `.codex/skills`:

- `issue-to-merge` for one ticket or parallel independent tickets through merge
- `adversarial-review` for an independent read-only review
- `writing-tests` when adding or changing behavior tests
- `grill-me` for a detailed design interview
