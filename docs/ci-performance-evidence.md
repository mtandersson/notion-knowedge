# Matched hosted CI campaign (#174)

The optimized design improved this campaign's unchanged-input warm and docs-only
samples. It was slower for cold CI and both real Rust PR samples. This report
therefore establishes no general Rust PR speedup. Each cell is one observation,
not a distribution, target, promise or billing estimate. No production workflow
change or consolidation decision is made here; #175 owns sharing development
build work and #176 owns the cumulative unchanged #155/#151 acceptance audit.

## Matched inputs and controls

Both designs use production main `850c34136224513056e4bdf84615fb254807bf40`,
the same pinned default Rust/Nix and Docker compilers, source, tests, fixtures,
Cargo manifests/lock, Dockerfile/context, security scripts/configuration and
agent-layout check. The historical **workflow design** is reconstructed from
unchanged main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`; it is a control
on current code, not a rerun of that historical production revision.
[PR #152 / 36963826931](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931)
and [main / 36964071809](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809)
remain immutable foundation evidence. Their differing inputs make their timings
unsuitable as matched controls. Since that revision, #108 changes HTTP liveness,
readiness and transport tests and the Docker healthcheck; `flake.nix` adds pinned
small shells. Rust manifests/lock and security check scripts/config remain
unchanged. Both controls incorporate all current #108 behavior and smoke checks.

Both variants use fresh hosted `ubuntu-latest` Linux/X64 runners, observed image
`ubuntu-24.04` version `20260927.320.1`, Ubuntu 24.04.5. Full manuals execute all
nine canonical checks; optimized also executes input selection and CI gate.
Their overhead is included. Optimized Rust PRs intentionally skip Agent layout;
docs PRs skip all seven unrelated canonical checks. Baseline runs them all.
Equivalent relevant security/runtime/test coverage is retained, with optimized
cache-contract and cached-image-identity self-tests included as additional work.

Benchmark-only differences: isolated cache namespaces with **all** Cargo fallback
keys prefixed, isolated Docker import/export scope with no production scope import,
PR trigger support for the two temporary variant bases, identical Docker timing
wrapper, Cargo fingerprint logging, and explicit materialization of each selected
Nix shell before work. Materialization separates Nix setup from compiler spans;
it adds a second cheap `nix develop` evaluation to each design. Baseline selects
the full default shell everywhere; optimized retains default for Rust and current
format/security shells for those tools. Security scripts still fetch live RustSec
advisories and scan complete reachable `HEAD -m` history with 100% redaction and
all default rules. Pins, tests and the final-image interactions are unchanged.
No cache entries were deleted to fabricate a cold result.

The [machine ledger](ci-performance-evidence.json) contains every immutable head,
actual checkout commit (including each PR merge commit), per-file input hashes,
job IDs/URLs, runner identity, steps, nested restore/save spans, compiler/fingerprint
excerpts, cache bytes and raw archive hashes. Pair digests are asserted equal:
current inputs `2028f91c…`, Rust-edit inputs `4ffeedc2…`. The latter changes only
`crates/core/src/lib.rs`: a real public function plus a unit test distinguishing
empty/nonempty bytes. The repeat adds a docs file in the same PR, retaining that
identical Rust edit and the same selected checks; it is a new run ID, not a rerun
that overwrites evidence. Docs-only PRs start separately from their variant base.

## Whole workflow and feedback

Wall = run creation to last job completion (not API `updated_at`); execution = sum
of actual step timestamp spans across parallel jobs. Initial delay = creation to
first runner start. Job queue = sum of job-created to runner-start, excluding
skips. Dependency waits before a job is created belong to workflow wall, not that
job's queue. Step timestamps have one-second resolution; gaps/runner teardown
make summed job envelopes a separate ledger field. These are not billed minutes.

| Stage | Design | Run | Head | Wall s | Initial delay s | Job queues s | Steps summed s | Last canonical job |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold | baseline | [37048862098](https://github.com/mtandersson/notion-knowedge/actions/runs/37048862098) | `0f405b2f` | 206 | 4 | 22 | 861 | Release build |
| cold | optimized | [37048866554](https://github.com/mtandersson/notion-knowedge/actions/runs/37048866554) | `1bbd84f0` | 251 | 4 | 30 | 902 | Container smoke |
| warm | baseline | [37049315376](https://github.com/mtandersson/notion-knowedge/actions/runs/37049315376) | `0f405b2f` | 153 | 4 | 19 | 572 | Container smoke |
| warm | optimized | [37049393244](https://github.com/mtandersson/notion-knowedge/actions/runs/37049393244) | `1bbd84f0` | 101 | 4 | 28 | 351 | Container smoke |
| rust-edit | baseline | [37049495192](https://github.com/mtandersson/notion-knowedge/actions/runs/37049495192) | `26cd6468` | 153 | 3 | 21 | 542 | Container smoke |
| rust-edit | optimized | [37049499558](https://github.com/mtandersson/notion-knowedge/actions/runs/37049499558) | `46d12fc3` | 225 | 2 | 23 | 598 | Container smoke |
| rust-repeat | baseline | [37049928094](https://github.com/mtandersson/notion-knowedge/actions/runs/37049928094) | `129503cd` | 150 | 3 | 18 | 577 | Release build |
| rust-repeat | optimized | [37050012705](https://github.com/mtandersson/notion-knowedge/actions/runs/37050012705) | `5db92f16` | 245 | 3 | 62 | 513 | Container smoke |
| docs | baseline | [37049739849](https://github.com/mtandersson/notion-knowedge/actions/runs/37049739849) | `7cff3208` | 153 | 3 | 19 | 534 | Container smoke |
| docs | optimized | [37049744410](https://github.com/mtandersson/notion-knowedge/actions/runs/37049744410) | `3bef2e60` | 45 | 2 | 11 | 65 | Dependency vulnerabilities |

All runs above succeeded. Cold manuals have observed misses for both downloads
and all four compiled snapshots; optimized Docker actually compiles all 87 server
build crates. There is no hidden warm-download fallback. Warm manuals restore
exact keys on new runners. Optimized Cargo logs zero compiled/checked crates;
baseline restores stale immutable snapshots and rebuilds five workspace crates
in each Cargo job, with zero external crates. Optimized Docker marks its compiler
RUN and executable COPY `CACHED`, compiling zero crates. Baseline Docker performs
a fresh 87-crate build on every hosted runner.

The first Rust edit restores compatible optimized Cargo snapshots, compiles five
workspace crates and zero external crates, logs `ChangedFile`/dependent stale
fingerprints, saves the new source keys and passes the new unit test. The repeat
restores those exact new keys: zero compiled/checked crates and no target save.
Baseline restores its old exact immutable keys on both source runs, compiles the
five workspace crates again, and cannot refresh those keys. The repeat therefore
measures eliminated Cargo work as well as actual PR feedback.

**Docker did not reuse its compilation RUN in the optimized Rust repeat**, despite
identical committed Docker/Rust inputs: it compiled 87 crates again. Only some
other layers were `CACHED`. This observed miss and the 245s feedback are retained;
configuration or the manual warm hit cannot substitute for this PR result. The
logs prove an imported manifest, successful prior export, unchanged source-byte
digests and later recompilation, but do not establish its cause or a stable miss
rate. This remains a performance limitation for the final parent audit, not a
failed correctness check or evidence of unsafe stale reuse. The successful manual
cross-run reuse and earlier #167 correctness audit remain valid within their
stated scope. No positive claim about repeated Rust Docker reuse is made here.

[Docs baseline #179](https://github.com/mtandersson/notion-knowedge/pull/179)
ran all nine canonical jobs;
[docs optimized #180](https://github.com/mtandersson/notion-knowedge/pull/180)
completed four effective jobs (selection, dependency audit, secrets and gate),
with seven intentional skips and no pending workflow-filter check. There was
no Rust compilation or container execution in the optimized docs run.
[Rust baseline #177](https://github.com/mtandersson/notion-knowedge/pull/177)
and [Rust optimized #178](https://github.com/mtandersson/notion-knowedge/pull/178)
retain identical source edits. Drafts are benchmark artifacts, never merged.

## Setup, caches and compiler work

All columns below sum actual steps across jobs except compiler-reported build
spans. Setup includes runner setup, checkout, Nix installer and Buildx setup;
Nix is explicit shell materialization. Restore includes composite key computation,
contract tests and timestamp preparation where present; the ledger's nested
millisecond action spans isolate download/target transfer from that overhead.
Save includes baseline deferred cache post-actions or optimized explicit saves.
Cargo step includes Nix/Cargo invocation, fingerprint checking, compilation and
test execution; compiler-reported build spans exclude Nix setup and tests after
Cargo's `Finished` line, but are wall spans of parallel compiler work, not CPU.

| Stage | Design | Setup s | Nix s | Cargo restore/keys s | Cargo steps s | Cargo saves s |
| --- | --- | --- | --- | --- | --- | --- |
| cold | baseline | 56 | 213 | 4 | 394 | 18 |
| cold | optimized | 70 | 176 | 34 | 377 | 15 |
| warm | baseline | 58 | 217 | 31 | 102 | 2 |
| warm | optimized | 66 | 156 | 42 | 3 | 1 |
| rust-edit | baseline | 55 | 175 | 27 | 112 | 1 |
| rust-edit | optimized | 69 | 136 | 40 | 125 | 25 |
| rust-repeat | baseline | 62 | 219 | 27 | 111 | 1 |
| rust-repeat | optimized | 71 | 175 | 37 | 4 | 0 |
| docs | baseline | 55 | 205 | 20 | 82 | 1 |
| docs | optimized | 17 | 29 | 0 | 0 | 0 |

Compiler-reported build spans, in order Clippy / check / tests / release:

| Stage | Design | Cargo reported build wall | Compiled/checked crate events |
| --- | --- | --- | --- |
| cold | baseline | 43.77s / 54.67s / 2m 20s / 2m 25s | 120 / 120 / 120 / 87 |
| cold | optimized | 49.99s / 1m 09s / 2m 10s / 2m 07s | 120 / 120 / 120 / 87 |
| warm | baseline | 5.78s / 2.10s / 10.37s / 1m 16s | 5 / 5 / 5 / 5 |
| warm | optimized | 0.67s / 0.53s / 0.71s / 0.57s | 0 / 0 / 0 / 0 |
| rust-edit | baseline | 8.52s / 1.44s / 8.78s / 1m 23s | 5 / 5 / 5 / 5 |
| rust-edit | optimized | 2.32s / 2.14s / 36.51s / 1m 21s | 5 / 5 / 5 / 5 |
| rust-repeat | baseline | 1.54s / 7.58s / 8.36s / 1m 23s | 5 / 5 / 5 / 5 |
| rust-repeat | optimized | 0.37s / 0.53s / 1.04s / 0.58s | 0 / 0 / 0 / 0 |
| docs | baseline | 7.69s / 1.32s / 7.89s / 54.77s | 5 / 5 / 5 / 5 |
| docs | optimized | skipped / skipped / skipped / skipped | 0 / 0 / 0 / 0 |

## Docker phases

Optimized columns separate final build+load, pinned probe-builder load, real final
image smoke, and verified cache export. Build+load includes remote cache manifest/
lazy layers, base downloads, compiler work and final Docker import. The logs retain
those BuildKit phase markers; their intervals can overlap, so no fabricated additive
transfer-only time is given. Baseline uses Docker's local image store: the timing
wrapper measures its actual build calls separately from whole smoke-script time.
Its smoke residual includes required negative-version builds and interactions;
those direct subprocess calls are not isolated by the wrapper. The tiny local
probe-builder build is separate and reuses the downloaded base, not a second
server compilation. Optimized probe-builder downloads/load (30–41s) remain costly
even when the server compilation is cached.

| Stage | Design | Final build/load s | Probe builder s | Smoke s | Export s | Compile events |
| --- | --- | --- | --- | --- | --- | --- |
| cold | baseline | 139.25 | 0.16 | 6.59 | 0 | 87 |
| cold | optimized | 103.0 | 38.0 | 11.0 | 44.0 | 87 |
| warm | baseline | 128.69 | 0.21 | 13.1 | 0 | 87 |
| warm | optimized | 3.0 | 41.0 | 8.0 | 2.0 | 0 |
| rust-edit | baseline | 137.83 | 0.16 | 7.0 | 0 | 87 |
| rust-edit | optimized | 106.0 | 37.0 | 7.0 | 21.0 | 87 |
| rust-repeat | baseline | 113.65 | 0.16 | 13.18 | 0 | 87 |
| rust-repeat | optimized | 135.0 | 30.0 | 7.0 | 24.0 | 87 |
| docs | baseline | 136.48 | 0.21 | 6.31 | 0 | 87 |
| docs | optimized | skipped | skipped | skipped | skipped | skipped |

## Actual storage, growth and earlier boundary evidence

The cache API snapshot after cold runs contains five baseline Cargo entries,
**455,931,623 bytes** total. Optimized Cargo has five entries totaling
**349,210,804 bytes**, plus Docker's branch-associated index/blobs totaling
**104,213,398 bytes** (12 entries altogether, **453,424,202 bytes**).
Cargo targets are smaller after incremental scratch removal. Entries are compressed
stored bytes, not image uncompressed bytes, compiler memory or HTTP wire usage.
The JSON retains each archive size/key/ref and transfer-log excerpts.

The final snapshot retains source refresh snapshots and PR Docker entries as
well as original base entries. Docker blob keys are content addressed; summing
entries attached to a benchmark ref counts that ref's retained records, not the
latest manifest closure or a claim about all globally shared base-image bytes.
Main/default-branch blobs may be shared. Index generations and old source archives
can accumulate until shared quota/LRU/unused-cache expiry; the observed snapshots
are neither a promised fixed bound nor all repository usage. PR baseline exact
hits save no new entries. The optimized source PR's entries total 545,025,344 bytes,
including four fresh Cargo targets and Docker blobs/index generations.

The source-backed [Docker evidence](docker-cache-evidence.md) retains actual
final/probe image sizes (31,802,953 / 1,555,704,156 uncompressed bytes), warmed
runtime-layer transfer (~11.6 MB) and live source/lock/pinned-builder invalidation
run 37023454091. Those are earlier correctness observations, not new matched-image
or aggregate timing claims. [Cargo evidence](cargo-cache-evidence.md) and its JSON
retain real source refresh/reuse, four manifest/lock/flake misses, flag/compiler/
OS/profile key contracts and fork save restrictions. [Cache audit](cache-acceptance-audit.md)
maps #154's full original requirements. Production cache scopes remain main warming
and PR merge-ref isolation; this campaign changes no trust rule.

[Nix evidence](nix-cache-evidence.md) retains cold/warm persistent-versus-fresh
probes, exact-input invalidation and sizes. Warm default restore+measurement+post
was 30s versus fresh 24s; narrow format 25s versus fresh 23s; narrow security 16s
versus fresh 16s. Cold saves and 2,789,922,271 retained candidate bytes further
favored the selected fresh-download path. Selected format/security shell closures
are 1,805,510,048 / 468,932,832 NAR bytes; these differ from archive transfers.
This matched campaign includes fresh shell downloads in every run and measures
the smaller shells' actual Nix setup, while retaining all earlier raw-source
measurements instead of substituting their unequal matrix totals for this control.

## Exclusions, limits and reproducibility

First generated workflows 37048321097/37048332124 had incorrect YAML scalar types
and produced no valid measurement. First dispatched runs 37048437804/37048441675
failed the unchanged secret scanner: a SHA256 digest in tracked benchmark metadata
keyed by the secret-check filename triggered its generic rule. Optimized also
restored an unrelated legacy download fallback. Both are excluded and retained as
raw failures. Corrected variants start from clean production ancestry, put digest
metadata outside the tracked harness and namespace every fallback. There are no
scanner suppressions or skipped failing jobs.

One sample per condition cannot estimate a median, variance or future feedback.
Pairs are from the same hosted runner class/image and time window, but runners
vary and runs can overlap. Security/advisory network timing, full-history scanning,
Nix CDN/setup, extraction, probe-image load and topology/gate delays all contribute.
Byte digests prove committed production equality, not every remote-service state.
The observed Docker Rust-repeat miss specifically limits any broad reuse claim.
Cache transfer/save costs, unfavorable samples and all current check names remain
visible. No optional development-build consolidation is inferred from these data.

Generate both harnesses from a fresh checkout (generation requires PyYAML 6.0.3;
collection/summarization only use Python's standard library plus authenticated `gh`):

```sh
python3 scripts/test-benchmark-ci.py
python3 scripts/benchmark-ci.py prepare --design baseline --production 850c341 --namespace YOUR_BASELINE_NAMESPACE --base-branch YOUR_BASELINE_BRANCH --output /tmp/nk-baseline
python3 scripts/benchmark-ci.py prepare --design optimized --production 850c341 --namespace YOUR_OPTIMIZED_NAMESPACE --base-branch YOUR_OPTIMIZED_BRANCH --output /tmp/nk-optimized
```

Create task-owned branches from that same immutable production head, copy each
harness, verify tree/production input equality, publish and dispatch `ci.yml` on
each ref. Wait for full cold success and inspect misses before dispatching the
warm run. Add the same real source edit/test to two draft PRs targeting the variant
bases, wait for their full relevant success, then add an identical docs file in
those PRs for a new source-repeat run. Make separate README-only drafts from the
bases. Never merge these harnesses or delete unrelated caches. The published
benchmark heads and their actual workflow files are linked in the ledger.

```sh
python3 scripts/benchmark-ci.py collect --output /tmp/nk-ci-raw 37048862098 37048866554 37049315376 37049393244 37049495192 37049499558 37049928094 37050012705 37049739849 37049744410
python3 scripts/benchmark-ci.py summarize --input /tmp/nk-ci-raw --output /tmp/nk-ci-ledger.json 37048862098 37048866554 37049315376 37049393244 37049495192 37049499558 37049928094 37050012705 37049739849 37049744410
```

[Retained raw archives](ci-performance-raw/) include all API run/jobs snapshots and
redacted logs, gzip with deterministic timestamps. Decompress with `gzip -dc`;
compare uncompressed SHA256 to the ledger. Job/step sums can be recomputed without
network access. Summarization additionally verifies immutable Git checkout input
hashes, fetching missing commits if necessary. This closes only #174's measurement
campaign; original #155 and #151 remain open for their native dependent work and
complete acceptance audit, including the unfavorable Rust/Docker result.
