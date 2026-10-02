# Cargo cache hosted evidence (#168)

Measured 2026-10-02 on fresh GitHub `ubuntu-latest`, Linux/X64 runners in
[PR #172](https://github.com/mtandersson/notion-knowedge/pull/172).
The [machine-readable ledger](cargo-cache-evidence.json) preserves exact keys,
job links, restore/miss/save excerpts, fingerprint examples, sizes and timings.
The original shared baseline remains unchanged: main
`4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`, [PR #152 run](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931)
and [main run](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809).
This campaign starts after #167 and #108, on main `ed5864f59ba361c7083f4dbe9ef79be8f411e546`.

## Immutable sequence

| Stage | Head commit | Run | Checks | Workflow elapsed s | Summed step execution s |
| --- | --- | --- | --- | ---: | ---: |
| baseline | `a09e9c83422b50b3cef975e2dc07f67ffb0c2413` | [run 37038156724](https://github.com/mtandersson/notion-knowedge/actions/runs/37038156724) | 11 passed | 224 | 759 |
| source refresh | `f6f165d7b04750b400739d2d210a717d4761e26c` | [run 37038627298](https://github.com/mtandersson/notion-knowedge/actions/runs/37038627298) | 11 passed | 238 | 632 |
| exact reuse and boundaries | `b28a26a18232f331730e0cd39c935d98dfb2d70c` | [run 37039131335](https://github.com/mtandersson/notion-knowedge/actions/runs/37039131335) | 15 passed | 118 | 802 |

Baseline compiled snapshots were cold in the new namespace; dependency downloads
restored the existing legacy archive. Source refresh adds a real public Rust
function and a unit test that executes it, rather than changing only comments.
All four Cargo jobs restored the baseline through the strict compatible prefix,
compiled five workspace crates and **zero external crates**, and saved the new
source key. Cargo logged `ChangedFile` for `crates/core/src/lib.rs` and
`StaleDepFingerprint` for dependent workspace targets. The new unit test passed.
This separates correct cache selection from Cargo's own necessary invalidation.

The next commit changes only benchmark workflow wiring. All four production
jobs restored the refreshed source key exactly: **zero compiled/checked crates
and zero dirty fingerprints**. The function test ran again. No target save action
ran on an exact hit. Cargo's reported times were 0.67 s Clippy, 0.66 s check,
0.65 s tests and 4.01 s release; the canonical shell steps also include Nix/Cargo
startup and test execution.

Exact build keys use `nk-cargo-v1-Linux-X64-{job}-{profile}-{compatibility}-{source}`.
The shared compatibility hash in this sequence is
`eaa03e04246433a7b0a137fb66bf7deab825952a4e5e6acdcd907f5b879b2fde`.
Baseline source hash is `781824979b31c0cd821709f6d4f3b034be9487da5613502bdd83ffb8124ca9ef`;
refreshed source hash is `bb92896aef7c30ed822389cde23cfbbd3392afff00e297b556542a930e15f12c`.
Job/profile pairs are `clippy/debug`, `check/debug`, `test/debug`, `build/release`.
The ledger contains each fully expanded selected/restored/saved key, including
fallback candidates. The retained download key is
`nk-cargo-downloads-v1-Linux-X64-8a4ecda38abdb8a5091c33f0424cc7d5ca440a749b49e31886f604079c282d69`.

## Per-job cost and storage

All times are seconds. Queue is job-created to runner-start; setup is runner-start
through Nix installation. Key/Nix includes first pinned development-shell setup.
Download and target restores are separate log-timestamp spans through the next
sub-step start. Cargo and successful-save totals use Actions step timestamps
(one-second resolution); save includes snapshot preparation and any download
save attempt. Archive sizes are compressed bytes sent on save or received on
restore, expressed as MiB. Baseline jobs racing to create the same immutable
source-download key can harmlessly lose that download save race.

| Stage | Job/profile | Queue | Setup | Key/Nix | Downloads restore | Target restore | Cargo step | Save total | Target MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | Clippy | 2 | 8 | 35.17 | 1.58 | 0.34 | 42 | 3 | 44.88 |
| baseline | Type check | 2 | 10 | 38.28 | 2.30 | 0.59 | 55 | 3 | 44.88 |
| baseline | Unit tests | 3 | 8 | 36.36 | 1.45 | 0.29 | 94 | 6 | 160.53 |
| baseline | Release build | 2 | 8 | 37.51 | 1.97 | 0.32 | 146 | 4 | 72.99 |
| source refresh | Unit tests | 3 | 9 | 38.61 | 1.76 | 10.23 | 34 | 5 | 160.53 |
| source refresh | Clippy | 2 | 8 | 22.47 | 2.10 | 1.04 | 4 | 2 | 44.88 |
| source refresh | Release build | 3 | 8 | 23.80 | 2.44 | 3.11 | 82 | 4 | 72.99 |
| source refresh | Type check | 3 | 7 | 23.64 | 0.80 | 1.12 | 5 | 2 | 44.88 |
| exact reuse and boundaries | Release build | 2 | 12 | 38.56 | 1.47 | 2.25 | 6 | 1 | 72.99 |
| exact reuse and boundaries | Clippy | 3 | 8 | 24.42 | 2.29 | 2.78 | 3 | 0 | 44.88 |
| exact reuse and boundaries | Unit tests | 2 | 8 | 24.38 | 2.96 | 6.01 | 4 | 0 | 160.53 |
| exact reuse and boundaries | Type check | 2 | 9 | 23.04 | 3.13 | 2.67 | 3 | 0 | 44.88 |

The source-download archive is 10,230,450 bytes (9.76 MiB). A baseline or refreshed
set of four compiled archives totals approximately 323 MiB. Incremental scratch
is pruned; exact reruns add no entries. GitHub quota/LRU and unused-cache expiry
bound historical storage, so future source snapshots can evict old entries.
Archive transfer/extraction cost is measurable, especially for the test profile.
Pinned Nix shell initialization remains a substantial fixed cost and is separate
work. Source refresh saves compilation time even when workspace relinking remains
necessary (release: 144 s cold, 80 s changed source, 4.01 s exact reuse).

## Compatibility probes

Four additional read-only jobs in the third run changed one input each before
using the **production** restore action with `check/debug`. Every compiled
restore reported `Cache not found` for both the exact key and the strict fallback.
Dependency downloads remained reusable. Each canonical `cargo check --workspace
--all-targets --locked` succeeded from an empty target; no probe saved archives.

| Input | Actual change | Compatibility hash | Cargo reported time |
| --- | --- | --- | ---: |
| Cargo.toml | `[profile.dev] debug = 1` | `5a409bb7b22e92ecfd9b4290fe52db1b4cc69271ba05367aa474166149696cab` | 62 s |
| Cargo.lock | valid comment appended | `d83226b61a13fcba219d888f6a5f0f1e019d64badb6dc6312eed143490bd99f2` | 62 s |
| flake.nix | dev shell sets `RUSTFLAGS=-C debuginfo=1` | `b238648c79e8c222cc70dd2bd2140ae6d72b30c7acf9d2eea99e30234da3e610` | 56.62 s |
| flake.lock | JSON reformatted, same resolved inputs | `768a413b641062d4fc05544b63602e568d5613299e72e40e0bdc7fed8b5e5a70` | 43.62 s |

Manifest and shell probes change effective compilation settings. Lockfile probes
prove byte-level compatibility boundaries while preserving resolved dependencies
and compiler versions; they do **not** claim a compiler upgrade or dependency
version change. Cold fingerprint logs show absent artifacts after deliberate cache
misses, rather than a restored cache spontaneously becoming dirty. Local contract
tests separately verify every tracked manifest, both locks, OS/architecture,
job/profile, compiler identity and relevant environment-flag boundaries.

## Preserved pipeline and measurement limits

All ordinary jobs passed in all three runs: formatting, Clippy, check, tests,
release artifact package/upload, Docker final-image smoke, agent layout, fresh
RustSec audit, full-history redacted secret scan, input selection and final gate.
No repository/application credentials were supplied to Cargo. The temporary
function, test, fingerprint logging and four probe jobs are removed before merge;
the immutable commits and this ledger retain the evidence.

Docker is unchanged by this ticket. Separate Docker step timings (seconds):

| Stage | Final build/load | Probe builder/load | Runtime smoke | Verified export |
| --- | ---: | ---: | ---: | ---: |
| baseline | 11 | 33 | 7 | 2 |
| source refresh | 130 | 31 | 8 | 23 |
| exact reuse and boundaries | 5 | 46 | 16 | 2 |

Workflow elapsed includes scheduling, parallel work and the critical path;
summed step execution adds work across parallel jobs and excludes inter-step gaps.
The third run also includes four deliberately cold probe jobs, so its summed work
is not a normal warm-CI total. The source-refresh run's Docker rebuild dominates
its critical path; lower Cargo times alone do not prove a lower whole-workflow
elapsed time. These are single-run observations, not a repeated comparable
campaign; retain them for #155 alongside the immutable #151 baseline.
