# Hosted Nix setup decision (#169)

Measured on fresh GitHub-hosted `ubuntu-latest`, Linux/X64 runners on
2026-10-02, in [PR #173](https://github.com/mtandersson/notion-knowedge/pull/173).
The [machine-readable ledger](nix-cache-evidence.json) preserves job IDs, queue,
all step spans, exact keys, cache transfers/sizes, download plans, closure sizes,
actual copied-path counts and environment activation measurements. Benchmark
commits retain reproducible workflow code; temporary probes are absent from the
final production workflow. Shared immutable #151 baselines remain unchanged.

## Immutable sequence and scope

| Stage | Head commit | Run | Result | Workflow elapsed s | Summed step execution s |
| --- | --- | --- | --- | ---: | ---: |
| Initial mkShell cold candidate / fresh | `3da44f1fda030f1fa9a366d71399cf6c7e238a86` | [37043263489](https://github.com/mtandersson/notion-knowedge/actions/runs/37043263489) | All 17 jobs passed | 312 | 1069 |
| mkShellNoCC narrow cold candidate / fresh | `f39947b9e3b940f07e71daf265ef7a78cab679fb` | [37043969673](https://github.com/mtandersson/notion-knowedge/actions/runs/37043969673) | Four narrow probe jobs passed; other probes failed and remaining canonical jobs were cancelled | 191 | 938 |
| Actual cross-run restoration, fresh comparison, both input invalidations | `773c789ecbb46cbd8952221af7e567b941b3d84e` | [37044195586](https://github.com/mtandersson/notion-knowedge/actions/runs/37044195586) | All 23 jobs passed | 346 | 1079 |

Run 37043117966 used unsupported `nix develop --dry-run`, so it produced no valid
measurement. The second run's baseline/invalidation probes used shallow checkout
and could not read their immutable baseline; they are excluded. The third run
corrected checkout history and reran those boundaries successfully. Its baseline
matrix checks out the original measured flake, while the narrow matrix uses the
selected `mkShellNoCC` flake. That preserves exact candidate keys and genuine
cross-run restoration despite the later narrowing edit. No failed/cancelled
run is claimed to prove full pipeline correctness.

Workflow elapsed includes queue/parallel execution; summed steps add work across
parallel jobs and are not elapsed time or billed minutes. Probe jobs inflate
these totals, so they do not establish a whole-CI speedup. #155 owns the final
comparable cold/warm campaign and optional consolidation. Within the final probe
run, fresh and persistent jobs use the same runner class, inputs and command;
these remain individual observations, with normal hosted-run variability.

## Closure and transferred storage

| Shell / variant | Activated closure NAR bytes | Closure paths | Advertised cold package download MiB | Saved/restored archive bytes |
| --- | ---: | ---: | ---: | ---: |
| Default, unchanged | 2533937616 | 147 | 722.8 | 852871220 |
| Format, initial mkShell | 2137463432 | 79 | 592.3 | 714342465 |
| Security, initial mkShell | 800886496 | 133 | 249.6 | 358746621 |
| Format, selected mkShellNoCC | 1805510048 | 66 | 492.8 | 609942154 |
| Security, selected mkShellNoCC | 468932832 | 120 | 150.1 | 254019811 |

Download figures are Nix's advertised compressed payload for missing package
paths, confirmed by actual `copying path` logs, not HTTP wire accounting. Shell
activation additionally fetched 107.5 KiB of Bash manual data; formatting fetched
four additional paths (2.5 MiB) during activation. Nixpkgs source evaluation and
Nix installation are outside the activated shell closure and included in their
own measured spans. The whole `/nix` archive includes those store artifacts and
database files, so archive bytes must not be confused with closure NAR bytes.
The default candidate's pre-save whole-store NAR size was 2868306456 bytes.

Warm persistent jobs downloaded **zero new package paths** for activation, then
used identical closure sizes. They actually received the archive byte counts
above and logged successful restoration/extraction and exact primary-key hits;
no new archive was saved on those exact hits. Fresh jobs still copied package
paths (default 133, narrow format 55, narrow security 106 during activation).
The ledger also counts source-evaluation copies and retains full download plans.
Five task-owned candidate entries totalled 2789922271 compressed bytes before
cleanup. The three selected-shell candidate archives alone were about 1.60 GiB.

## Separate setup, restore and save costs

Seconds; queue and Nix installer are separate. Plan includes initial Nix
source/evaluation work; activation starts **after** that dry-run. Measure is the
whole script step, including plan, activation, closure inspection and version
checks. Compare restore + measure + post-action, not activation alone. Action
step timestamps have one-second resolution; plan/activation use monotonic time.

| Stage / shell / mode | Queue | Nix install | Restore | Plan | Activation | Measure | Post/save | Compared total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold initial default fresh | 2 | 3 | 0 | 13.41 | 19.28 | 34 | 0 | 34 |
| Cold initial default persistent | 2 | 5 | 0 | 16.40 | 20.42 | 38 | 15 | 53 |
| Cold narrow format fresh | 2 | 4 | 0 | 13.20 | 12.16 | 27 | 0 | 27 |
| Cold narrow format persistent | 3 | 4 | 0 | 10.99 | 15.19 | 28 | 19 | 47 |
| Cold narrow security fresh | 2 | 4 | 0 | 11.46 | 3.67 | 18 | 0 | 18 |
| Cold narrow security persistent | 3 | 4 | 1 | 11.45 | 3.70 | 17 | 10 | 28 |
| Warm default fresh comparison | 2 | 5 | 0 | 10.38 | 12.24 | 24 | 0 | 24 |
| Warm default persistent | 2 | 5 | 26 | 1.21 | 0.60 | 3 | 1 | 30 |
| Warm narrow format fresh comparison | 3 | 4 | 0 | 11.00 | 10.58 | 23 | 0 | 23 |
| Warm narrow format persistent | 2 | 4 | 22 | 0.97 | 0.53 | 2 | 1 | 25 |
| Warm narrow security fresh comparison | 4 | 5 | 0 | 11.36 | 3.96 | 16 | 0 | 16 |
| Warm narrow security persistent | 2 | 4 | 13 | 1.20 | 0.63 | 3 | 0 | 16 |

The initial mkShell format/security warm candidates also lost (30 versus 22 s,
19 versus 18 s respectively). Narrowing removes approximately 332 MB of closure
and 99.5 MiB of advertised download from each small shell. The persistent cache
saves package downloading/evaluation, but archive extraction dominates the
measured restore. **Select fresh downloads for all shells**, including the
security warm tie, avoiding cold saves and retained Nix archives. Keep the
smaller pinned format/security shells; leave ordinary Rust on default.

## Compatibility and trust probes

The rejected candidate pins cache-nix-action to
`b16f249a0248efec86dd9d4224cb75f1ffa48bcc`. Exact keys are
`nk-nix-bench-v1-OS-ARCH-SHELL-hashFiles(flake.nix,flake.lock)`, with no fallback.
Baseline combined hash is `171dd84591a31d3f4c3c2de8e66d61e344f98d3f48129c45e5d65c42ff60cf89`;
selected-shell hash is `93c91ad1769400fd0fee481c027cc2bd54540c52393d1e1cb2d0a444112ace3a`.
The third run changed one original input at a time in separate runners:

| Input change | New combined hash | Restore | Actual setup / measure |
| --- | --- | --- | --- |
| flake.nix comment | `0f078f24184395467e3bb7796795bcc8926cb7626aae4a47381338d8e580cb86` | No cache / no fallback | 24.48 s activation / 39 s measure |
| flake.lock JSON reformat | `08eadba6b1841aeff96a83297263059ce39805e547f4fa9b9b77e321207c1160` | No cache / no fallback | 13.42 s activation / 28 s measure |

Both retained pinned package identity and rebuilt the missing closure, with no
probe cache save. These prove exact byte-input invalidation, not a compiler or
dependency upgrade. GitHub scoped all benchmark entries to PR-173's merge ref;
no benchmark main warming or production cache benefit is claimed. The candidate
requested best-effort GC at 3 GiB (roots may exceed a GC target), alongside
GitHub repository quota/LRU/unused-cache expiry. With no selected persistent Nix
cache, production needs none of these additional save/trust/bounds mechanisms.

## Actual selected commands and parent acceptance

Run 37044195586 passed all ordinary checks with fresh downloads on the selected
shells: actual `cargo fmt`, all four Rust commands and Cargo snapshot saves,
security failure-contract tests, a live fresh unsuppressed RustSec audit,
complete-history redacted secrets, agent layout, full Docker final-image smoke,
release package/upload, input selection and the always-running CI gate. Tool
identity is Cargo 1.98.0, rustfmt 1.9.0, cargo-audit 0.22.2 and Gitleaks 8.30.1,
all resolved from the same flake pin. The temporary probes supplied no
application credentials and did not cache advisories or scanner results.

The [complete #154 acceptance audit](cache-acceptance-audit.md) connects the
merged Docker/Cargo leaves and this decision to the original requirements.
Final production PR, post-merge main and manual full-coverage verification links
will be recorded in the #169 delivery comment. #154 and #151 remain open; #155 retains
the comparable full campaign rather than treating these probe totals as its
result.
