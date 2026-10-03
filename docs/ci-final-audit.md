# Complete original CI acceptance audit (#186)

This report audits the unchanged full original #151 proposal and acceptance,
#155 measurement acceptance, #176 final audit and #185/#186 corrective outcome, against production main
`85abd9b90c18c232d59e6cbcb0fd0e6b2b0329e1`. It does not close #155 or #151;
the coordinator must separately audit each parent and its native terminal graph.
The immutable foundation remains main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`,
[PR #152 run 36963826931](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931)
and [main run 36964071809](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809).
These historical production inputs are not a matched timing control.

## Outcome and limits

The matched campaign demonstrates less repeated Cargo work, actual cross-run
Docker compilation-layer reuse and reduced documentation PR feedback. It does
**not** demonstrate a general Rust PR speedup: baseline/optimized workflow wall
was 206/251s cold, 153/101s warm, 153/225s Rust edit, 150/245s Rust repeat and
153/45s docs. Summed step execution was respectively 861/902s, 572/351s,
542/598s, 577/513s and 534/65s. These are one sample per condition; no variance,
median, promised percentage or billing conclusion is supported.

The original unchanged-input Rust repeat rebuilt all 87 Docker crates despite
prior successful export and an imported manifest. [The #185 causal investigation](docker-cache-causal-evidence.md)
retains that result and establishes two defects using available current index
bodies and manager traces: lazy export can lose intermediate results, and combining
overlapping cache managers can suppress existing result-bearing own records.
Production now exports builder and final roots after real smoke and chooses one
available authorized own-preferred/main-fallback source per root. BuildKit remains
the exact observed v0.33.1 digest. A real used-source producer and two consecutive
fresh PR repeats (37102506265, 37102810771, 37103058113) preserve identical binary
hashes and actually reuse compiler RUN and executable COPY, with zero compilation
across all three solves in each repeat. Source/lock-byte/pinned-builder probes
37103397187 each rebuild and pass complete smoke. The original #174 index bodies
were not retained; its unique historical cause remains unproven. No measured
Rust feedback regression, failed treatment or counterexample is erased.

Dual exports and fresh builder loading cost time; repository cache availability,
quota and races can still force valid rebuilds. These mechanisms establish
correctness and measured repeated-PR reuse within the retained controls, without
a general Rust feedback improvement, universal hit rate or performance target.
The matched #174 campaign predates this corrective production change; #185 controls
and delivery runs establish the change separately, not an invented updated
matched baseline comparison.

[Whole-pipeline evidence](ci-performance-evidence.md), [development sharing](ci-dev-sharing-evidence.md),
[Docker correctness](docker-cache-evidence.md), [Cargo correctness](cargo-cache-evidence.md)
and [Nix decision](nix-cache-evidence.md) retain immutable run/checkout identities,
raw logs, calculations, excluded failed probes, source equality controls and costs.
No experiment workflow enters production. Separate Type check, Clippy and Unit
tests remain: complete development reporting was 138/227s cold and 66/90s warm
(parallel/shared), despite shared work/storage savings. Persistent Nix was rejected
including restore/extraction/save overhead (30/24s default, 25/23s format,
16/16s security warm); selected pinned smaller shells use fresh downloads.

## Complete original implementation and acceptance matrix

Each row maps a complete original requirement; detailed sources are retained
rather than substituting configuration intent for executed behavior.

| Original #151 proposed implementation / acceptance and #155 scope | Production owner and authoritative evidence |
| --- | --- |
| Every PR triggers; cheap conservative path selection; stable names/aggregate gate; full main/manual | `.github/workflows/ci.yml`, `scripts/ci-jobs.py`; [selection mapping and actual failure](ci.md), raw/API recheck below. No workflow path filters. `always()` gate rejects failed, cancelled, missing, malformed and unexpected skipped selected results. |
| Docs-only skips compilation/container; agent/layout only selects agent; Rust workspace/build scripts/manifests/locks; tests/datasets consumers; Docker runtime/smoke; workflow/toolchain/unknown full; transitive union | [Complete path map](ci.md); eight representative hosted cases below, plus production docs/Rust matched campaigns. Rename/deletion/mixed/unknown command tests remain in `test-ci-jobs.py`. Rust tests select the workspace and Docker conservatively, non-Markdown evaluation selects consuming tests. |
| Security every PR; fresh advisories; complete redacted history; no hidden coverage suppression | Unconditional dependencies/secrets jobs, `fetch-depth: 0`, `check-secrets.sh` rejects shallow history and scans `HEAD -m`, `--redact=100`, ignores inline exemptions; `check-dependencies.sh` invokes live locked audit. Current production raw logs show fresh RustSec fetch and no leaks. Security failure tests execute in dependency job. No advisory/result cache or selected-path security gate added. |
| Repository protection availability; relevant failures block; clean skips/no pending filter | Actual selected Agent failure run 36965058690 has failed Agent and failed CI gate; every selected probe completed, optional checks intentionally skipped. Rulesets and main protection APIs rechecked 2026-10-03: both HTTP 403, with different messages below; no enforced protection claimed. |
| Explicit Buildx import/export/load, pinned real final image, smoke identity/version/revision/non-root/linkage/stdio/HTTP/shutdown, compiler stays in pinned Docker builder | Container job, own/main availability selector, pinned BuildKit and `smoke-container.py --prebuilt --builder-image`; [causal correction](docker-cache-causal-evidence.md) and [Docker evidence](docker-cache-evidence.md). Actual hosted smoke passes after restoration. Cache export follows smoke, not before. Docker bases and BuildKit retain immutable digests; action pins are immutable commits. |
| Warm cross-run Docker layers; source/dependency/toolchain invalidation; no mounts-only claim | [#185 current causal controls](docker-cache-causal-evidence.md): changed used source 37102506265 and fresh repeats 37102810771/37103058113 prove zero compilation across final/builder/final-export solves, actual compiler RUN and executable COPY reuse; independent invalidation 37103397187 compiles all 87 crates for source, lock-byte and pinned-builder variants with real smoke. Earlier 37023119555/37049393244 warm hits remain true; original 37050012705 miss remains true and uniquely unproven historically. Final main/manual delivery below verifies actual current production behavior. |
| Cargo changed-source refresh and later exact reuse; fingerprint invalidation; maintained downloads; compatibility OS/arch/toolchain/manifests/locks/profile/flags | Restore/save composite actions and `cargo-cache.py`; source refresh 37038627298 then exact 37039131335, corroborated by matched Rust edit/repeat. Five workspace crates rebuild on change, zero external; new keys saved; repeat zero compilation and no target save. Compatibility includes both flake inputs, actual compiler identity and all tracked manifests. Four live boundary jobs miss compiled snapshots; flag/compiler/OS/profile tests retain additional contract boundaries. |
| Trusted main warming, PR/fork isolation, cache storage/download/upload/growth bounds | [Cargo policy](cargo-cache.md), [Docker policy](container.md#ci-layer-cache), source-backed size/cost ledgers. Main/default caches can restore on PR; GitHub merge-ref isolation prevents PR targets poisoning main; forks cannot save. Immutable source archives and BuildKit index generations can accumulate until repository quota/LRU/unused expiry; finite logical roots do not guarantee a fixed entry count or retention. Read-only index preflight restricts lookups to runtime-authorized own/main refs; errors/absence use no remote import. It exposes no runtime token or signed URL; fork exports remain forbidden. |
| Persistent Nix candidate keyed OS/arch/flake; small pinned shells; overhead versus fresh before choosing; flake authoritative | [Nix actual cold/warm/invalidation decision](nix-cache-evidence.md); five candidate entries 2,789,922,271 bytes, cold saves 15/19/10s; fresh selected after warm comparisons, smaller format/security shell closures 1,805,510,048/468,932,832 NAR bytes. Rejected candidate exact keys had no fallback; both flake byte edits missed. No persistent production Nix cache claimed. |
| Optional development sharing only if feedback/cost/reporting/coverage improves; main retained | [Sharing evidence](ci-dev-sharing-evidence.md) retains separate jobs, command outcomes and terminal failure probe. Shared cold/warm whole walls 354/111s vs parallel 264/107s; cold 85s initial delay disclosed. Savings do not justify slower canonical development feedback in these samples. |
| Comparable cold/warm/Rust/docs before/after, equivalent relevant coverage and identical production inputs, immutable foundation | [Matched campaign](ci-performance-evidence.md) reconstructs historical workflow design on identical current code; production hashes equal within pairs. Current transport/healthcheck coverage is in both controls. Historical unequal inputs remain context. Excluded malformed/scanner/fallback probes disclosed. |
| Queue/setup/Nix/restore/save/compiler/build/smoke separation; wall versus summed execution; sizes/costs/limitations/reproduction | Both campaign ledgers retain API step spans, nested transfers, compiler/fingerprint lines and real build/smoke phases; reports define overlapping spans and one-second API precision. Docker lazy transfers cannot be fabricated into independent additive times. Cargo byte archives, Docker stored records/runtime transfers/image sizes and Nix NAR/download/archive sizes are distinct. |
| Immutable action pins, safety boundaries, no coverage reductions; documentation and independent final verification | All external workflow/composite `uses` retain 40-character commit pins. Flake and Docker base digests stay authoritative; the #185 correction pins the previously observed BuildKit digest. This report changes no production workflow/action/cache/security/flake/Docker source. [CI guide](ci.md), cache policies and source reports expose checks, keys, trust, costs and collection commands. Independent final review and current PR/main/manual delivery follow below. |

## Representative hosted path evidence

The original selection probes use the identical production selector/gate and
commands, changing only the temporary base-branch trigger (disclosed in [CI guide](ci.md)).
This audit refetched run/jobs APIs, preserving their immutable head and complete
actual check outcomes in [final ledger](ci-final-audit.json) and [raw snapshots](ci-final-audit-raw/).
Both fresh security checks and selection succeeded in every case, including the
intentional Agent failure. All eleven statuses are terminal; none pending.

| Category | Actual run | Additional canonical checks / CI gate |
| --- | --- | --- |
| Docs Markdown deletion/README | [36965010145](https://github.com/mtandersson/notion-knowedge/actions/runs/36965010145) | Seven optional skips; gate success |
| Agent guidance | [36965014465](https://github.com/mtandersson/notion-knowedge/actions/runs/36965014465) | Agent only; gate success |
| Evaluation fixture | [36965020854](https://github.com/mtandersson/notion-knowedge/actions/runs/36965020854) | Consuming Unit tests only; gate success |
| Production Rust and integration tests | [36965025858](https://github.com/mtandersson/notion-knowedge/actions/runs/36965025858) | All Rust and Container; Agent skipped; gate success |
| Cargo manifest/lock | [36965031906](https://github.com/mtandersson/notion-knowedge/actions/runs/36965031906) | All Rust and Container; Agent skipped; gate success |
| Dockerfile | [36965036418](https://github.com/mtandersson/notion-knowedge/actions/runs/36965036418) | Container only; gate success |
| Workflow/flake | [36965042528](https://github.com/mtandersson/notion-knowedge/actions/runs/36965042528) | All nine; gate success |
| Actual deleted agent compatibility link | [36965058690](https://github.com/mtandersson/notion-knowedge/actions/runs/36965058690) | Agent command failure and CI gate failure; unrelated optional jobs skipped |

Rulesets response: HTTP 403, “Upgrade to GitHub Pro or make this repository public
to enable this feature.” Main branch protection: HTTP 403, “Resource not accessible
by integration.” The second establishes API unavailability, not a proven plan
restriction or absence of settings. Delivery checks each actual CI result before
merge; CI gate failure is demonstrated, enforced branch blocking is not claimed.

## Final production evidence and reproducibility

At audited immutable production `85abd9b`, corrective delivery
[PR #189 / 37108753390](https://github.com/mtandersson/notion-knowedge/actions/runs/37108753390),
[main 37109013284](https://github.com/mtandersson/notion-knowedge/actions/runs/37109013284)
and only-after-main [manual 37109256523](https://github.com/mtandersson/notion-knowedge/actions/runs/37109256523)
all succeeded in eleven jobs. Workflow wall / summed step execution were
251/503s, 251/514s and 113/366s. The first PR/main builder-root exports lacked a reusable builder index, compiled
87 crates and cost 150s/146s respectively (whole export steps); final image builds
themselves reused compilation. These are cold builder-root observations, not
fully cold Cargo/final-image workflow controls.
The subsequent manual actually reused compiler RUN and executable COPY and
compiled zero crates across all three solves. Actual image smoke, fresh RustSec
and full-history redacted scans passed. These are correctness/delivery observations,
not additional matched performance samples. The ledger preserves sanitized raw
logs, API job identities and complete solve evidence.

This report's own future PR/main/subsequent-manual verification must be collected
after delivery; the existing #185 runs cannot replace it. Immutable delivery
commit/run/job identities and authoritative sanitized raw records will be retained
in the delivery PR/issue and artifact. A report cannot contain its own future
squash merge SHA before it is committed. Only #186 may close; the coordinator
must separately audit the original parent bodies and native terminal graphs.

Prerequisite children #153/#154/#167/#168/#169/#174/#175/#185 are CLOSED.
The current graph retains #176 children #185/#186; #186 is blocked by closed #185
and has no children. #155 retains children #174/#175/#176, and #151 retains
#153/#154/#155. Thus #176/#155/#151 remain OPEN during this leaf delivery;
the complete parent graph is not falsely described as terminal. Raw issue bodies,
body hashes, native sub-issue and dependency API snapshots are in the ledger.

On resumed collection, `gh run view --log` returned empty despite available
logs. This audit instead fetched the authoritative REST ZIP endpoint, extracted
each named text file with a filename marker, and preserved nonempty concatenated
logs. Raw API timestamps support independent arithmetic; gzip uses mtime zero.
The ledger records uncompressed archive SHA256 and compressed bytes. Existing
#174/#175 archive hashes, every matched run's workflow wall and summed step
execution, and all 25 #185 controls/index bodies were independently recomputed during this audit, without changing their evidence.

```sh
# Recheck the authoritative graph/issues and all final statuses.
gh issue view 151 --repo mtandersson/notion-knowedge
gh issue view 155 --repo mtandersson/notion-knowedge
gh run view 37109013284 --repo mtandersson/notion-knowedge
gh run view 37109256523 --repo mtandersson/notion-knowedge
gh api repos/mtandersson/notion-knowedge/actions/runs/37109256523/jobs?per_page=100
gh api repos/mtandersson/notion-knowedge/actions/runs/37109256523/logs > /tmp/manual-logs.zip
python3 -m zipfile -e /tmp/manual-logs.zip /tmp/manual-logs
gzip -dc docs/ci-final-audit-raw/37109256523-jobs.json.gz
python3 scripts/verify-ci-final-audit.py
python3 scripts/test-ci-jobs.py
python3 scripts/test-cargo-cache.py
python3 scripts/test-container-smoke.py
python3 scripts/test-benchmark-ci.py
python3 scripts/test-benchmark-dev-sharing.py
./scripts/check-agent-layout.sh
```

Campaign collection/generation commands and pinned controls remain in their
respective reports. Final delivery review, PR verification and sequential
post-merge main then manual results must be attached to this issue/PR; no
pre-merge report can truthfully contain its future squash-merge SHA. #186 delivery is complete
only after those actual results and any in-scope acceptance gap are resolved;
#176/#155/#151 remain open for the coordinator's separate complete original audits.

## Source and cost audit boundaries

The Cargo compatibility owner includes actual compiler identity, all manifests,
locks, both flake inputs, OS/arch/job/profile and compiler/linker flags; source
refresh verifies hashes before restoring timestamps and never restores source
bytes. Hosted effective manifest/flag probes and byte-only lock/flake probes are
distinguished from dependency upgrades. Local compiler/OS/profile contracts are
not relabeled hosted upgrade tests. Registry/git downloads retain lock/checksum
validation independently. Save follows successful commands/artifact upload; exact
hits create no new target key, forks cannot save, PR merge-ref archives cannot
be selected by trusted main. Neither Cargo nor Docker changes that trust boundary.

Cargo refreshed four-target sets are approximately 323 MiB plus 9.76 MiB downloads;
test restore reached 10.23s and changed release compilation remained about 80s.
#174 cold Cargo retained 455,931,623 baseline bytes vs 349,210,804 optimized, plus
104,213,398 Docker bytes; separate #175 development storage was 262,449,793 vs
190,106,261 shared bytes. #185's multi-ref failed/control campaign retained
101 entries/2,933,653,227 bytes, of which 81 Docker records were 1,238,783,480 bytes.
Its later cleanup removed only the 101 task-owned records from three task refs,
never main. These are observed compressed inventories, not a steady-state cap,
network meter or additive unique image storage. Content-addressed overlap,
versioned indices/source generations, repository quota/LRU/seven-day unused
eviction and restore/export costs preclude a fixed retention promise.

The cache-selection preflight reads index bodies (about 11.4 KiB final and
3.9 KiB builder in stable controls) and may race later eviction; BuildKit still
validates full input/platform keys. Missing/error cases incur compilation rather
than skipped verification. Lazy transfer spans overlap build/load/export; the
ledger preserves operation intervals and declared descriptor bytes, and does
not invent independent additive HTTP timings or billed minutes.

Source review covered production selector/gate, all workflow/composite actions,
Cargo key/timestamp policy, Docker allowlist/build/smoke, security scripts/tests
and configs, manifests/locks, flake shells and pinned dependencies. Scanner
exceptions remain empty. Every-PR security jobs remain unconditional: full
reachable merge history `HEAD -m`, shallow rejection, full redaction and ignored
inline allows; live advisory fetch without stale/offline fallback. Focused
security failure tests retain deleted/merge-only secrets, shallow rejection,
critical vulnerabilities and narrow-exception behavior. These are effective
policy and execution claims, not proof against unknown vulnerabilities.

Run `python3 scripts/verify-ci-final-audit.py` to reproduce archive hashes, complete
run arithmetic, immutable checkout input hashes, Docker solve/index observations,
original issue-body hashes/native snapshots, action pins and unchanged production
source. Fetch missing immutable head/merge-checkout Git objects from the machine
ledger first; the verifier deliberately does not invent evidence or mutate refs.
