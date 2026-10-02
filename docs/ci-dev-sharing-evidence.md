# Development CI sharing experiment (#175)

Retain separate production Type check, Clippy and Unit tests jobs. Sharing reduced
summed execution and stored development outputs, but did not demonstrate faster
complete PR feedback. After input selection completed, all three development
reports finished in **138s vs 227s cold**, and **66s vs 90s warm** (parallel vs
shared). Shared warm compilation itself was faster; downstream reporting runner
queues erased that benefit. Cold sequential testing lengthened development
feedback. These are single observations, not medians, future predictions or
billing. No production workflow, gate, cache policy or toolchain is changed.

## Comparable inputs and retained coverage

Both variants start from immutable production
`ca26c51c6689b805aa0711c162aba1b8ef1da00f`, following #174's
[whole-pipeline matched campaign](ci-performance-evidence.md). Its immutable
historical main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`, PR #152 and main
runs remain preserved; their unequal inputs are not controls for this experiment.
We read unchanged #175, #155 and #151. This report closes only #175's optional
sharing experiment; #176 retains the cumulative acceptance audit, including the
unfavorable Rust/Docker result in #174. #155/#151 remain open.

[Parallel draft PR #182](https://github.com/mtandersson/notion-knowedge/pull/182)
and [shared draft PR #183](https://github.com/mtandersson/notion-knowedge/pull/183)
use the same code, test/fixture corpus, manifests/lock, flake, pinned toolchains,
Docker inputs, security scripts/config and agent check. The ledger records actual
PR head and merge-checkout production input digests, equal across all four runs.
Both full-coverage PRs modify the workflow, so production selection chooses every
canonical check. This measures comparable PR checks, not a claim that an arbitrary
Rust edit or docs-only PR has these times. #174 owns source-change/docs probes.

All nine canonical check names pass, with the existing selection and CI gate.
Both retain release build, real final-container smoke/probes, fresh live RustSec
advisories and redacted complete-history secret scans. Pins, permissions, fork
save guards, flake-selected default/format/security shells and canonical command
flags stay identical. Hosted runners report Linux/X64 `ubuntu-24.04`, image
`20260927.320.1`, Ubuntu 24.04.5. Independent runners/network/queues can vary.

Only probe changes are generated from current production: task-owned isolated
Cargo download/target namespaces and every fallback; Docker scopes excluding
production cache imports; Cargo fingerprint logging; explicit selected Nix-shell
materialization; the same Docker timing wrapper from #174. No probe is merged.
Parallel preserves current job topology. Shared runs, sequentially in one debug
target, the exact commands:

```sh
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Each command has its own named step/outcome and continues independently after a
command failure, preserving downstream coverage. Three separately named report
jobs propagate command outcomes into the unchanged canonical gate. The reusable
harness additionally requires the shared job's terminal success so setup/cache/
post-action failures cannot pass through successful command outputs. Report jobs
and their real runner queue/setup costs are included below.

Cold means unused unique namespace, confirmed target and dependency misses, not
an empty global Nix/Docker CDN. Each warm run follows its fully successful cold
run, on the same PR cache scope, with an identical added docs marker and unchanged
production inputs; all Cargo targets/downloads are exact hits and no external
crate compilation/checking remains. Successful refreshed snapshots, verified
source timestamps and incremental scratch removal use the unmodified policy.
The shared snapshot uses the supported check/debug identity under its isolated
namespace/ref; all three consume the same pinned debug target. Release remains
separate. Compatibility includes OS/arch, compiler, both flake inputs, manifests,
lock, profile/flags and policy; source snapshots refresh as documented in
[cargo-cache.md](cargo-cache.md). No compatible target or Docker compiler boundary
is broadened. Earlier actual source-refresh/invalidation/fork evidence remains
in [cargo-cache-evidence.md](cargo-cache-evidence.md).

## Hosted timings and actual work

Workflow wall is creation to final job completion, including initial queue,
selection, security, release/container and gate. Initial queue is creation to first
job start; summed job queues are individual job creation-to-start and overlap.
Summed steps are API step spans; job envelopes also contain scheduling/gaps.
Neither summation is billing. Do not add these overlapping measures together.

| State | Design | Run | Workflow wall s | Initial queue s | Sum job queues s | All steps s | Dev + reports steps s | Dev + reports envelopes s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold | parallel | [37053011474](https://github.com/mtandersson/notion-knowedge/actions/runs/37053011474) | 264.0 | 2.0 | 26.0 | 886.0 | 342.0 | 352.0 |
| cold | shared | [37053220430](https://github.com/mtandersson/notion-knowedge/actions/runs/37053220430) | 354.0 | 85.0 | 32.0 | 790.0 | 217.0 | 227.0 |
| warm | parallel | [37053557144](https://github.com/mtandersson/notion-knowedge/actions/runs/37053557144) | 107.0 | 2.0 | 42.0 | 375.0 | 162.0 | 175.0 |
| warm | shared | [37053938777](https://github.com/mtandersson/notion-knowedge/actions/runs/37053938777) | 111.0 | 2.0 | 101.0 | 276.0 | 47.0 | 56.0 |

| State | Design | Setup s | Nix s | Restore s | Commands s | Save s | Reports s | Longest dev job s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold | parallel | 22.0 | 92.0 | 21.0 | 192.0 | 14.0 | 0 | 136.0 |
| cold | shared | 9.0 | 19.0 | 6.0 | 157.0 | 24.0 | 1.0 | 218.0 |
| warm | parallel | 23.0 | 91.0 | 40.0 | 5.0 | 1.0 | 0 | 64.0 |
| warm | shared | 7.0 | 23.0 | 10.0 | 3.0 | 0.0 | 3.0 | 45.0 |

Development feedback after selection is selection completion to the final
Type check/Clippy/Unit tests report completion. It includes shared fan-out queues:
parallel/shared **138/227s cold** and **66/90s warm**. Whole-workflow cold wall
354s includes an 85s initial queue; removing that initial delay gives 269s shared
vs 262s parallel. Therefore the raw +90s wall difference is not a compiler result.
Warm wall is 111s shared vs 107s parallel; unrelated container/Nix/gate work and
runner variation limit attribution. All-stage step totals nevertheless fell
886→790s cold and 375→276s warm in these samples. Development-plus-report step
totals fell 342→217s and 162→47s. Report step overhead alone is 1s/3s cold/warm;
its full envelopes (9s/11s) and queues are separately included in the ledger.

Cargo's own finished-profile spans and actual external crate events:

| State | Design | check / Clippy / test Cargo s | External compile/check events by command |
| --- | --- | --- | --- |
| cold | parallel | 42.56 / ~73 / ~75 | 115 / 115 / 115 |
| cold | shared | ~61 / 1.53 / ~92 | 115 / 0 / 94 |
| warm | parallel | 1.18 / 0.67 / 2.24 | 0 / 0 / 0 |
| warm | shared | 0.86 / 0.17 / 0.17 | 0 / 0 / 0 |

This proves shared Clippy reused check outputs; test builds still require distinct
code-generation work, though 21 external crate events were avoided. These events
are Cargo progress records, not unique dependency counts, CPU samples or rustc
invocation traces. Cargo spans rounded to minutes/whole seconds cannot establish
subsecond precision. The JSON retains per-command fingerprint event counts/
samples, all raw logs, nested cache spans, Docker operation timings and each
job's individual API steps. Nix is materialized once per dev runner; other small
nix develop invocations still include evaluation overhead in restore/commands.
Cache restore/save envelope spans include key computation, unpacking/source
preparation and archive creation, not only HTTP transfer. Identical fingerprint
logging occurs in both variants. Raw logs remain authoritative for details.

## Storage, transfer and cache growth

Direct cold API inventories contain development targets totaling
**262,449,793 bytes parallel** (check 47,060,748; Clippy 47,057,529; tests
168,331,516) versus **190,106,261 bytes shared**. Including release and dependency
downloads, Cargo totals are **349,212,567** (five entries) versus **276,869,094**
(three). These are compressed retained bytes, not raw target size or wire traffic.
Shared saves one larger development archive, taking 24s in the cold sample vs
14s summed parallel saves; warm exact target hits save no new target archives.
Parallel jobs race to save the same download key; their benign download-save
warnings accompany successful target saves and complete cold target inventories.

The final pre-negative-probe inventory retains all entries attached to the two
PR merge refs: **24 entries, 834,526,252 bytes**, including both variants' Cargo,
Docker blobs and index generations. Content-addressed Docker blobs can share
base/default records; summing PR-associated records is not the latest image's
manifest closure or total repository usage. Old index/source generations can
accumulate until quota/LRU/unused expiry. This experiment makes no bounded-growth
or Docker Rust-repeat reuse claim; #174's unfavorable observation is preserved.
Every cache snapshot key/ref/size and raw transfer/save message is inspectable.
Both task-owned draft PRs are closed, both remote branches deleted, and all
25 cache records confined to their two PR merge refs deleted after raw evidence
preservation. The final cleanup inventory additionally includes the terminal-
failure probe's new Docker index; it does not change the pre-negative storage
comparison. No unrelated ref/cache was deleted.

## Failure authority, limits and reproduction

The first shared attempt
[37053024647](https://github.com/mtandersson/notion-knowedge/actions/runs/37053024647)
used an unsupported custom Cargo job identity. Cache validation failed, all three
reports and CI gate failed; its still-running release/container jobs were cancelled
by the corrected push. It is retained raw and excluded, not treated as cold success.
The corrected shared cold namespace is `bench175-shared-v2`.

Independent review found that the original timing probe's wrappers checked only
command success, allowing a later shared-job cache/post failure to escape. All
four timing samples had successful shared/parallel jobs, so that gap does not
change their successful timings. The reusable generator now also requires shared
terminal success; local tests execute wrapper failure/cancel/skip/missing outcomes
and canonical gate behavior. The corrected live
[terminal-failure probe 37054241039](https://github.com/mtandersson/notion-knowedge/actions/runs/37054241039)
ran all three Cargo commands successfully, then an explicit final `exit 1` made
the shared job fail. All three canonical report jobs and CI gate failed; other
checks passed. Its complete raw APIs/logs are retained as failure verification,
excluded from timing comparisons.
No weaker probe gate or injected failure enters production.

One sample per condition cannot estimate variance or forecast feedback/cost.
Shared warm savings are real observed work savings but canonical reporting was
slower in both states. Retaining production's separate jobs preserves immediate
independent failures and coverage without claiming consolidation improves PR
feedback. A future topology may improve this tradeoff, requiring new evidence.

Generate from the immutable production head using PyYAML 6.0.3; collect/summarize
need standard Python and authenticated gh, following #174's reusable harness:

```sh
python3 scripts/test-benchmark-dev-sharing.py
python3 scripts/benchmark-dev-sharing.py --design parallel --production ca26c51 --namespace YOUR_PARALLEL_NAMESPACE --base-branch main --output /tmp/dev-parallel
python3 scripts/benchmark-dev-sharing.py --design shared --production ca26c51 --namespace YOUR_SHARED_NAMESPACE --base-branch main --output /tmp/dev-shared
python3 scripts/benchmark-ci.py collect --output /tmp/dev-raw 37053011474 37053220430 37053557144 37053938777
python3 scripts/benchmark-ci.py summarize --input /tmp/dev-raw --output /tmp/dev-ledger.json 37053011474 37053220430 37053557144 37053938777
```

Create draft probe PRs from the same immutable production head, copying only each
generated harness. Verify committed production digest equality and namespace
misses. Wait for complete cold success/cache saves before pushing identical docs
markers; verify warm exact hits. Never merge the harnesses. Raw archives in
[ci-dev-sharing-raw/](ci-dev-sharing-raw/) use gzip mtime=0. Decompress with
`gzip -dc` and compare SHA256 against [the machine ledger](ci-dev-sharing-evidence.json).
API arithmetic and command evidence are recomputable offline; immutable checkout
input verification additionally needs the linked Git objects. Wrapper terminal
success is the only final generator correction relative to successful timing
prototypes and is disclosed above.

Production selection/gate tests, cache contracts, container identity tests,
agent layout and redacted history scanning accompany this report. Relevant PR
CI plus full post-merge main/manual coverage and final review are recorded in the
delivery PR/issue; the production main/manual workflow remains unchanged.
