# Security scanning

The regular CI workflow runs two independent gates on pull requests targeting
`main`, pushes to `main`, and manual runs. They use `cargo-audit` and Gitleaks
from the same `flake.lock` pin as the development environment, in the smaller
`security` shell (`mkShellNoCC`). The [measured Nix policy](nix-ci.md) uses fresh
store downloads; scanner artifacts are pinned, while live advisory data is
always fetched separately. No credentials,
paid scanner account, or repository secrets are needed, including for fork PRs.

## Dependency vulnerabilities

From the repository root:

```sh
nix develop .#security --command ./scripts/check-dependencies.sh
```

Cargo audit checks the existing `Cargo.lock` against a freshly fetched
[RustSec advisory database](https://github.com/RustSec/advisory-db). It does not
update dependency versions or generate a missing lockfile. Every unsuppressed
vulnerability fails the job, including high/critical and advisories without a
CVSS score. Informational advisories (such as unmaintained crates) remain
warnings. Yanked-version checks are disabled: withdrawal from the registry is
not itself a vulnerability and this gate uses RustSec rather than a crates.io
index refresh. There is no OS/architecture filter; all supported targets are
covered. Fetch/parse/tool errors also fail CI; CI never permits stale data or
uses an offline fallback. Scanner versions are pinned, advisory data is live.

Prefer upgrading the dependency. If an advisory is demonstrated not to apply,
add only its exact `RUSTSEC-YYYY-NNNN` ID to `.cargo/audit.toml`'s `ignore` list.
Place a comment immediately above the entry stating the affected dependency,
evidence (for example, the vulnerable feature/API is unused), a tracking issue,
an owner, and a review date. Revisit the exception when the dependency or its
features change. Never suppress a severity class or all advisories for a crate.
There are currently no advisory exceptions.

## Committed secrets

From the repository root, after committing changes:

```sh
nix develop .#security --command ./scripts/check-secrets.sh
```

Gitleaks uses its full built-in ruleset, extended by `.gitleaks.toml`, and scans
all commits reachable from `HEAD`, with merge diffs against each parent. CI
checks out complete history, including the PR merge commit and its ancestors. This catches a credential introduced
and then deleted before the final PR snapshot. A shallow clone fails with an
instruction to run `git fetch --unshallow`; scanning does not silently accept
partial history. Unreachable objects, other unmerged branches, and uncommitted
changes are outside this scan's scope. To check staged changes before a commit:

```sh
nix develop .#security --command gitleaks git --staged --config .gitleaks.toml \
  --gitleaks-ignore-path .gitleaksignore --redact=100 --no-banner \
  --ignore-gitleaks-allow .
```

Every unsuppressed finding fails the job. Secret values are fully redacted in
logs. For investigation, run locally with `--verbose` and the same redaction
flags to see file, rule, commit and line metadata. Do not upload unredacted
reports or paste credential values into issue/PR comments. A real exposed
credential must be revoked/rotated; deleting its current file is insufficient.

A confirmed synthetic false positive can be suppressed with its exact
`commit:path:rule:line` fingerprint in `.gitleaksignore`. Add a comment above it
with why it cannot authenticate, the tracking issue, owner and review date.
This suppresses that historical occurrence only; a later copy or another rule
still fails. Do not exclude entire paths, commits or rules. Inline
`gitleaks:allow` comments are deliberately ignored by the gate. There are
currently no secret-scanner exceptions. Review changes to scanners, their
configuration and exception lists as changes to the security policy.

## Verify the gates

```sh
nix develop .#security --command python3 scripts/test-security-scanning.py
```

The focused offline tests invoke the production scripts and real pinned
scanners against temporary Git repositories, generated never-issued token
values, and a synthetic critical advisory database. They verify clean inputs,
deleted-secret and merge-only-secret detection and redaction, rejection of shallow history, fatal
vulnerabilities, and that exact exceptions cannot hide a separate finding.
These tests run in the dependency CI job before the live audit. Temporary
fixtures are removed automatically and do not use external credentials.

This is known-advisory and pattern-based scanning; it complements review and
does not prove the absence of unknown vulnerabilities or undetectable secrets.
Tool documentation: [cargo-audit](https://github.com/RustSec/rustsec/tree/main/cargo-audit)
and [Gitleaks](https://github.com/gitleaks/gitleaks).
