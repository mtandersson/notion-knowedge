# CI selection and verification

The CI workflow triggers on **every** pull request targeting `main`, every main
push, and manual dispatch. There are no workflow-level path filters. The cheap
**CI input selection** job runs the production selector and its contract tests.
PR inputs come from the merge-base diff between the event's base and head SHAs,
with complete history available. Renames include both old and new paths;
deletions retain their old paths. Multiple changes take the union of consumers.
An empty diff, unknown input, or new unclassified build input selects all checks.
A detection error fails the gate rather than claiming a clean skip.

## Input map

Dependency vulnerabilities and Secret scanning run on every PR regardless of
selection. The dependency scan fetches fresh RustSec advisories; secret scanning
uses complete reachable history, including the checked-out merge commit, and
redacts findings. Ordinary checks need no application credentials or secrets.

| Changed PR input | Additional jobs |
| --- | --- |
| `README.md`, `CONTRIBUTING.md`, `LICENSE`, Markdown under `docs/` or `eval/` | None |
| `AGENTS.md`, `CLAUDE.md`, `.agents/`, `.claude/`, `.codex/`, `scripts/check-agent-layout.sh` | Agent layout |
| Non-Markdown `eval/` data | Unit tests (consuming evaluation integration tests) |
| Rust `.rs` inputs and manifests under `crates/`, root `Cargo.toml`, `Cargo.lock` | Format, Clippy, Type check, Unit tests, Release build, Container smoke |
| `Dockerfile`, `.dockerignore`, `scripts/smoke-container.py` | Container smoke |
| Workflows, CI scripts/tests, security configuration/scripts, `flake.nix`, `flake.lock`, Rust toolchain files, all other inputs | All nine checks |

Rust integration tests select the whole workspace and container conservatively:
Docker's context allowlist currently admits every `.rs` file, including tests.
Build scripts are Rust inputs. Manifests and the lockfile affect all workspace
checks and the image. Toolchain/workflow changes and unknown paths require full
coverage. Main and manual dispatch always select all nine jobs.

## Stable check and failure contract

All existing nine check display names remain unchanged. Narrow PR jobs now
report intentional skips. Configure **CI gate** as the required check instead of
requiring each optional job individually. Keep **CI input selection** visible
for diagnosis; CI gate requires it to succeed and validates every selection
output and every job result. Selected checks must succeed. Unselected checks may
skip or succeed, but failures, cancellations, missing results, or malformed
selection outputs fail the gate. Both security checks must always be selected.
The gate uses `always()` and depends on all jobs, so a failed upstream job cannot
silently skip the final decision. Cancelling the entire workflow cannot produce
a successful gate; any required CI gate still needs a successful run.

On 2026-10-02, rechecking both the repository rulesets endpoint and main branch
protection endpoint returned HTTP 403: “Upgrade to GitHub Pro or make this
repository public to enable this feature.” No configured protection is claimed.
Merge delivery checks observed CI results explicitly. When the plan permits
protection, require the stable CI gate before relying on enforced blocking.

## Local verification

```sh
python3 scripts/test-ci-jobs.py
python3 scripts/ci-jobs.py select --event pull_request --base origin/main --head HEAD
python3 scripts/ci-jobs.py select --event workflow_dispatch
```

The tests exercise the production command in disposable Git repositories,
including rename/delete/mixed and unusual filenames. Gate tests invoke the same
production gate used in Actions with success, intentional skips, failures,
cancellations, unexpected skips, detection failures, and invalid outputs.

## Actual Actions evidence

Evidence is added after representative runs finish. The production baseline is
main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`, with full-coverage run
[36964071809](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809).
The comparable foundation PR run is
[36963826931](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931).
Performance/cache measurements remain in parent #151 and its follow-up children;
this child establishes selection correctness without attributing cache benefits.
