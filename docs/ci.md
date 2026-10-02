# CI selection and verification

Container smoke imports Buildx layers from main and the current PR/branch,
loads the final runtime and pinned builder, and exports a bounded mutable
scope only after the existing final-image checks pass. Fork PRs do not export;
GitHub's merge-ref cache isolation prevents PR layers from being restored on
main. See [container cache policy and commands](container.md#ci-layer-cache).
Cargo build jobs use [source-refreshing compatible snapshots](cargo-cache.md).
Nix uses measured [fresh downloads and smaller pinned CI shells](nix-ci.md);
large persistent stores did not beat the selected path in hosted probes.

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

The production baseline is
main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`, with full-coverage run
[36964071809](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809).
The comparable foundation PR run is
[36963826931](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931).
Performance/cache measurements remain in parent #151 and its follow-up children;
this child establishes selection correctness without attributing cache benefits.


On 2026-10-02, ten task-owned draft PRs targeted a temporary benchmark foundation
`fefd681fba090b54707ce4043d5887a08ada6f8e`. Its workflow differs from the
production implementation `39023a2` only by adding `benchmark/153-foundation`
to the PR base-branch trigger list. Selection logic, job conditions, check
commands, permissions, and the aggregate gate are identical. This isolates
representative changed inputs before the selector workflow merges to main;
otherwise every pre-merge test PR would itself include the workflow change
and conservatively select all jobs. The foundation is never merged to main.
Benchmark PRs and remote branches are closed/deleted after evidence capture.
Run links preserve immutable commit identities and actual job logs/results.

Every listed run passed CI input selection and both fresh security scans.
The table lists the seven optional jobs; “None” means none selected/skipped.
The failure run deliberately deleted `CLAUDE.md`: the actual Agent layout
command exited 1 and CI gate ran and rejected that failed selected result.
No fake result data or configured skip caused this failure.

| Changed inputs / event | Immutable run | Optional jobs selected | Optional jobs skipped | CI gate |
| --- | --- | --- | --- | --- |
| docs | [36965010145](https://github.com/mtandersson/notion-knowedge/actions/runs/36965010145) | None | Agent layout, Clippy, Container smoke, Format, Release build, Type check, Unit tests | success |
| agent | [36965014465](https://github.com/mtandersson/notion-knowedge/actions/runs/36965014465) | Agent layout | Clippy, Container smoke, Format, Release build, Type check, Unit tests | success |
| fixture | [36965020854](https://github.com/mtandersson/notion-knowedge/actions/runs/36965020854) | Unit tests | Agent layout, Clippy, Container smoke, Format, Release build, Type check | success |
| foundation | [36965025024](https://github.com/mtandersson/notion-knowedge/actions/runs/36965025024) | Agent layout, Clippy, Container smoke, Format, Release build, Type check, Unit tests | None | success |
| rust-tests | [36965025858](https://github.com/mtandersson/notion-knowedge/actions/runs/36965025858) | Clippy, Container smoke, Format, Release build, Type check, Unit tests | Agent layout | success |
| manifest-lock | [36965031906](https://github.com/mtandersson/notion-knowedge/actions/runs/36965031906) | Clippy, Container smoke, Format, Release build, Type check, Unit tests | Agent layout | success |
| docker | [36965036418](https://github.com/mtandersson/notion-knowedge/actions/runs/36965036418) | Container smoke | Agent layout, Clippy, Format, Release build, Type check, Unit tests | success |
| toolchain-workflow | [36965042528](https://github.com/mtandersson/notion-knowedge/actions/runs/36965042528) | Agent layout, Clippy, Container smoke, Format, Release build, Type check, Unit tests | None | success |
| rename-unknown | [36965048133](https://github.com/mtandersson/notion-knowedge/actions/runs/36965048133) | Agent layout, Clippy, Container smoke, Format, Release build, Type check, Unit tests | None | success |
| mixed | [36965053320](https://github.com/mtandersson/notion-knowedge/actions/runs/36965053320) | Agent layout, Container smoke | Clippy, Format, Release build, Type check, Unit tests | success |
| failure | [36965058690](https://github.com/mtandersson/notion-knowedge/actions/runs/36965058690) | Agent layout | Clippy, Container smoke, Format, Release build, Type check, Unit tests | failure |

The docs run changed README and deleted a Markdown ADR template. Agent changed
AGENTS. Fixture changed the evaluation JSON. Rust/tests changed production Rust
and a Rust integration test; manifest/lock changed both Cargo inputs. Docker
changed Dockerfile. Toolchain/workflow changed flake.nix and the workflow. Rename
moved README to an unknown root path, which selected full checks. Mixed changed
README, AGENTS, and Dockerfile together. Foundation was a manual dispatch and
ran all nine original checks plus selection and gate. Docs-only thus avoided
all unrelated Rust and container work while retaining advisory and secret
coverage. Full production PR and post-merge main runs are also inspected during
delivery; cache/performance comparisons belong to the later #151 children.
