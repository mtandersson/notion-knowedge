# CI selection and verification

## Required gate and optional jobs

The required **CI gate** waits only for checks that fit a 10-minute budget on
a warm cache: CI input selection, Agent layout, Format, Clippy (default
features, `--all-targets`, which also type-checks every target), Unit tests
(workspace), Dependency vulnerabilities and Secret scanning. These jobs run in
parallel.

Optional jobs still run and report on every PR that selects them, but the gate
does not wait for them: Clippy (native features), Unit tests (local-qwen), the
native test compile and its two native test suites, and the Release build. PRs
skip linking the `production-eval` example; main pushes still link it, and
Clippy (native features) checks it on every selected PR. Container smoke has
its own workflow, `.github/workflows/container.yml`. It runs on every main
push, nightly at 03:00 UTC, on manual dispatch, and on PRs that change
container inputs (`Dockerfile`, `.dockerignore`, `compose.yaml`, container
and smoke scripts, Nix inputs, `Cargo.lock` and the toolchain). Its main run
keeps publishing the GHCR BuildKit cache.

**Merge rule:** before merging a PR whose selection includes native or
container inputs, every optional job that ran for it, including Container
smoke, must be green. The gate alone is not enough for such PRs.

All Cargo jobs set `CARGO_INCREMENTAL=0` and line-table debug info for the
dev and test profiles at workflow level, so these settings are part of every
Cargo snapshot key. Each job keeps its own snapshot (`clippy-features` is
separate from `clippy`), and only main saves snapshots.

Integration tests build as one `it` binary per crate for
`notion-knowledge-retrieval` and `notion-knowledge-server`
(`tests/it/main.rs`, one module per former test file), so native jobs link
two LanceDB test binaries instead of 39. Run one former file with, for
example, `cargo test -p notion-knowledge-server --test it http::`. The
server's `allocator_bounds` test stays its own binary because its allocator
bound is process-wide.

Container smoke chooses one available Buildx index per final/builder root,
preferring the current PR/branch and falling back to trusted main,
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
| Non-Markdown `eval/` data | Unit tests (workspace and local-qwen), native tests |
| Rust `.rs` inputs and manifests under `crates/`, root `Cargo.toml`, `Cargo.lock` | Format, both Clippy jobs, all unit and native tests, Release build |
| `Dockerfile`, `.dockerignore`, `compose.yaml`, container smoke scripts | None in CI; Container smoke runs from `container.yml` |
| Workflows, CI scripts/tests, security configuration/scripts, `flake.nix`, `flake.lock`, Rust toolchain files, all other inputs | All checks |

Build scripts are Rust inputs. Manifests and the lockfile affect all workspace
checks. Toolchain/workflow changes and unknown paths require full coverage.
Main and manual dispatch always select every job. Rust source changes do not
trigger Container smoke on PRs; it runs after merge on main and nightly.

## Stable check and failure contract

Narrow PR jobs report intentional skips. Configure **CI gate** as the required
check. Keep **CI input selection** visible for diagnosis; CI gate requires it to
succeed, validates every selection output, and checks the result of every
required job. Selected required checks must succeed. Unselected ones may skip
or succeed, but failures, cancellations, missing results, or malformed
selection outputs fail the gate. Both security checks must always be selected.
The gate uses `always()` and depends on all required jobs, so a failed upstream
job cannot silently skip the final decision. Optional job results are not part
of the gate; the merge rule above covers them. Cancelling the entire workflow cannot produce
a successful gate; any required CI gate still needs a successful run.

On 2026-10-03, rechecking rulesets returned HTTP 403: “Upgrade to GitHub Pro or
make this repository public to enable this feature.” Main branch protection
returned HTTP 403: “Resource not accessible by integration.” That distinct
response proves API unavailability, not a plan restriction or absence of settings.
No configured protection is claimed.
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

## Matched whole-CI campaign

[The #174 hosted campaign](ci-performance-evidence.md) compares the historical
workflow design and current optimized CI on identical current production inputs,
with observed cold/warm sequences and matched Rust/docs PRs. Its machine ledger
and retained raw archives separate queue, setup/Nix, caches, Cargo and container
work. Warm/docs samples improved; cold and Rust PR samples were slower, including
an observed Docker compilation-cache miss after an unchanged-source PR follow-up.
These results do not establish a general Rust PR speedup. The
[causal Docker correction](docker-cache-causal-evidence.md) separately proves
repeated-PR reuse and preserves the historical cause limitation. The
[complete original acceptance audit](ci-final-audit.md) maps all #151/#155/#176
requirements without claiming a universal speedup; parent closures require
separate coordinator audits.

The [development sharing experiment](ci-dev-sharing-evidence.md) retains separate
Type check, Clippy and Unit tests jobs: measured work/storage savings did not
improve complete development PR feedback in either cold or warm samples.

## Parallel test suites

The `test` job runs workspace, `local-qwen`, and `local-lancedb` suites in
parallel with `fail-fast: false`, preserving every previous Cargo test command.
All suites use the same conservative test input selection. The final CI gate
requires the aggregated matrix result to succeed, so any failed, cancelled, or
missing suite blocks delivery. Other suites continue after a failure to retain
complete diagnostics.

Each suite has a separate `test-SUITE` compiled Cargo snapshot; dependency
downloads remain shared. Workspace tests no longer restore the optional
LanceDB/Qwen build outputs. New namespaces need an initial cold build before
warm comparisons are meaningful; splitting adds runner and cache storage cost.

The motivating [PR #323 job](https://github.com/mtandersson/notion-knowedge/actions/runs/38114458194/job/114396504011)
spent 181 seconds restoring caches, 150 seconds running workspace tests, and
33 seconds running Qwen tests before starting LanceDB tests (observed while
that step was still running). This change removes serial suite dependencies;
these baseline timings alone do not prove a hosted speedup.
