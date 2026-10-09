# GitHub dependency cleanup — issue #266

The dependency graph is authoritative in GitHub's **issue relationships**,
not issue Markdown. This cleanup uses the [GitHub issue dependencies API](https://docs.github.com/en/rest/issues/issue-dependencies)
to inspect and (optionally) delete only explicitly allowlisted edges.

## One-time execution for the already approved cleanup

The repository also has a specially guarded, one-shot workflow in
`.github/workflows/run-dependency-cleanup-once.yml`. This workflow
automatically runs the **previously reviewed** batch A (99 links) and batch B
(11 links) *after* its own PR is merged into protected `main` with the
deliberately specific squash commit title:

```text
chore(issues): execute reviewed dependency cleanup once (#266)
```

It is **not enabled by ordinary pushes or pull requests**: the push must
modify the one-shot workflow file on `main`, and the first line of the
commit message must contain the exact title. Its run is serialized with the
existing manual workflow, runs credential-free tests first, then read-only
preflight checks, then applies each bounded batch using
`scripts/prune-issue-dependencies.py`. After each batch it validates
critical OAuth and LanceDB dependencies; the last step explicitly verifies
that #124 still depends on #123 but not #9.

This is a single approved operational migration: it does **not** mutate
#257/#254/#255, and it leaves the preexisting manual dry-run workflow
available. If GitHub Actions stops or fails partway through, the migration
script is idempotent; inspect the Actions logs and rerun the manual
workflow for the incomplete batch rather than creating a new automatic
trigger. Do not claim #266 is completed until both batches have
actually passed the live API verification.

## Run from GitHub Actions

After the cleanup workflow is reviewed and merged, open the repository's
**Actions → Issue dependency cleanup → Run workflow** and select `main`.

1. Select **batch A** and leave **apply unchecked** to inspect the proposed
   99 redundant child-level edges.
2. Review the run output and the audit in [#266](https://github.com/mtandersson/notion-knowedge/issues/266).
3. Select batch A, **apply checked**, to remove and individually verify only
   currently-present intended edges. Deleted links are not silently replaced.
4. Optionally inspect and apply **batch B** (11 parent-level phase gates)
   **only after** batch A is fully clean and the roadmap-order policy is agreed.
5. Re-run both batches **without apply** to verify that no targeted edges
   remain. Check #124 and any issue of interest in GitHub's blocked-by UI.

The workflow is never automatically authorized to mutate dependencies on
a push or pull request. PRs only run credential-free tests; a separate,
main-only `workflow_dispatch` job receives `issues: write` permissions.

## Run locally

Using an existing GitHub CLI login with repository **Issues: write** permission:

```sh
python3 scripts/test-prune-issue-dependencies.py -v

gh auth status
python3 scripts/prune-issue-dependencies.py --batch A
python3 scripts/prune-issue-dependencies.py --batch A --apply
python3 scripts/prune-issue-dependencies.py --batch B
# Optional, only once batch A is finished and release-policy gating is agreed:
python3 scripts/prune-issue-dependencies.py --batch B --apply
```

The default is read-only. The script gets actual blocker database IDs from
`GET /issues/{number}/dependencies/blocked_by`, checks that each live edge
still matches, deletes via
`DELETE /issues/{number}/dependencies/blocked_by/{issue_id}`, and then
re-reads the issue to verify the edge disappeared. It does not fetch a
personal access token itself, echo credentials or accept arbitrary
repository names or issue ID lists.

It validates that critical OAuth relationships (#122→#123→#124 and later
grant binding/revocation) and the **#257 LanceDB dependencies** still
exist before and after applying changes. It **never** edits issue #257,
#254, #255 or their branches. If a safety invariant or GitHub API request
fails, it exits non-zero without continuing. The migration is idempotent:
after a partial failure, re-run batch A in dry-run mode to inspect the
remaining links, then continue.

**Out of scope:** batch A/B do not reverse security dependency direction
or close any tickets. Follow-up additions, such as making #9 wait for its
necessary SSRF/scope/redaction protections, require a separately reviewed
dependency change. Closing #123 because PR #264 was merged also needs a
separate acceptance-criteria review; do not force-close it to unblock #124.
