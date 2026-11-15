# Product deployment acceptance: issue 455

Status: live acceptance attempted on 2026-11-15; incomplete. The corrected
release produced real embeddings and durable page commits, but a per-page
deadline stopped startup before serving search. No successful full-root sync,
query acceptance, callback delivery or production readiness is claimed.

## Latest deployed release and observed outcome

The independently reviewed HTML-span/admission correction merged through
[#647](https://github.com/mtandersson/notion-knowedge/pull/647) as
`69b0249a30135514ff74f87e6f777913f5866212`; prerequisite #646 is closed.
Exact final CI [43700990936](https://github.com/mtandersson/notion-knowedge/actions/runs/43700990936)
passed. The release feature artifact manifest records test-merge revision
`0e18addb44cfcfe52b947e7850b04fe1e380c8aa`; its source tree
`0e06b5979da647d687822650a74219257eac6bda` exactly matches the merged tree.
The downloaded executable SHA-256 was verified as
`656c0db7587b6051460f0bb76f59d163aab18f8d8955c1a69e0e4cb71000cc55`.
The reviewed versioned executable and focused service pin were activated under
the owner's explicit sync-start approval, with a SQLite-only backup and no
credential copying or source writes.

The first corrected-root durable commit contained six vectors, approximately
105 seconds after the first sync event. The first invocation later failed with
`cache streaming page timeout` after 17m21.718s. The configured automatic restart
reset the disposable index and began a second attempt. A reviewed runtime-only
`Restart=no` guard was then applied without stopping or replacing that running
process. The second invocation also hit the page deadline and stopped naturally;
the guard prevented another restart. Its retained derived cache has 13 applied
receipts/checkpoints and 143 embedded chunks; logs report 33 discovered pages
and three deterministic embedding-budget skips. No stable HTTP readiness or
search/fresh-read acceptance was reached.

Effective limits stayed at one CPU, 4 GiB memory, no swap, 128 tasks and 1,024
file descriptors. The second invocation's measured peak memory was
3,633,709,056 bytes, with no cgroup OOM event. Private monitoring continued to
report unavailable startup health honestly; persistence alone does not establish
successful recovery or product acceptance.

Profiling the first 15 completed indexing workers counted 852 Notion calls
(mean 56.8 per worker, range 24–184). Summed API-operation time was 331 seconds;
this includes shared admission/rate-limiter waiting. Optional linked-target
authorization ran during prepare and revalidation despite `NK_GRAPH=false`.
The existing configured-root history contains 2,250 inventory rows; extrapolating
observed API admission alone exceeds 11 hours, before final verification and
embedding work. The four-hour startup inventory bound is therefore unsupported
by measured capacity. A supported 24-hour inventory bound with unchanged CPU/RAM
is being staged alongside the focused correction, not claimed as measured
completion time.

[#648](https://github.com/mtandersson/notion-knowedge/issues/648) is the native
child/blocker for this measured I/O and deadline failure. Its draft
[#649](https://github.com/mtandersson/notion-knowedge/pull/649) is in progress.
No new deployment, cache reset or restart will be used as a substitute for its
production-boundary tests, independent review, exact-head CI and artifact
verification. The existing daily scheduler, usable protected search, fresh source
reads, justified resource/disk bounds and full recovery acceptance remain pending.
#455, #416, #13 and #1 remain open.

## Historical preflight and first deployment diagnostics

The sections below retain the earlier #642 candidate/preflight and failed-start
evidence. Their unactivated/local-only statements describe that earlier stage;
they do not supersede the latest deployed outcome above.

## Candidate identity

The streaming-sync candidate was independently reviewed and merged through
[#642](https://github.com/mtandersson/notion-knowedge/pull/642) as
`2ae86bab9e2a0badaf22361b0c9a9722b1785eb2`. Security dependency fixes from #644
are included. CI run
[43686243542](https://github.com/mtandersson/notion-knowedge/actions/runs/43686243542)
passed every selected check, including workspace/Qwen/LanceDB/webhook-worker
tests, Clippy, type checks, dependency and secret scans, release build, production
image indexing/retrieval smoke and final gate.

The downloaded `notion-knowledge-server-webhook-worker-Linux-X64` artifact
(ID `14151735889`) contains the release executable exported from the tested
image, rather than a native debug test executable. Its manifest records:

- Test-merge revision: `a09883e23ed17bbed3266df7ad1fabcb6f72ae73`.
- Cargo version: `0.1.0`; feature: `webhook-worker`; platform: `linux/amd64`.
- Source image: `sha256:d9320e2e32291cf04d904b5afea7fd2be4d9d5a42f2ef0abbf57af0164717eea`.
- Binary SHA-256: `878b2a7e73be5c62bd2b5da9488747da6254a92e5fee97cf93b905de9f420664`.
- Binary size: 348,922,088 bytes.

The extracted binary hash matches the manifest. Both the test-merge revision
and merged revision have tree `c1cc4c6055cf05b6e483d7b395871b12ecbfeb21`;
`git diff --quiet TEST_MERGE MERGED_REVISION` succeeds. The executable reports
`notion-knowledge 0.1.0` using the existing host's Nix loader and runtime libraries.
See [artifact verification instructions](container.md#download-the-tested-server-binary).

## Live acceptance remaining

The previous indexing and OAuth test services were stopped before this preflight;
no new full-root sync or useful current index is claimed. Prepared local operator
configuration has not been activated. Host inventory, source scope identifiers,
credentials and private monitoring data are excluded from this report.

The proposed persistent service reuses the existing integration scope, disposable
cache, local embedding assets, sync worker and daily reconciliation interval.
It keeps source operations read-only and serves bearer-protected loopback HTTP.
Initial effective limits must be measured against the actual full-root workload;
preparing bounds does not justify their sizing. The private observer must use the
existing bearer for protected health checks without logging or copying it.

Before closing #455, capture the following from the activated, exact candidate:

- Attempt the complete configured authorized root, recording available content,
  embeddings, durable page commits, honest skips and successful local search and
  fresh reads. Do not resolve template originals or repair source pages.
- Measure model loading, indexing, query and rebuild CPU/RSS/PID/FD/temp/state/index
  usage, disk growth and reserve; record effective limits and justified headroom.
- Verify private health/disk monitoring, overload rejection, recovery and preserved
  external-effect records under the actual deployed settings.
- Verify the existing periodic catch-up and authenticated Notion callbacks through
  the intended Cloudflare route without disturbing another callback consumer.
- Back up operational state before any derived reset; never replay source writes
  or uploads. Keep credentials in their existing private locations.

Actual ChatGPT installation remains separate in #109/#559. Historical quality
measurements are retained; optional ranking work does not gate this launch.

## Activation and observed startup failure

The owner approved the reviewed persistent activation on 2026-11-15. The bounded
one-way service and private bearer-aware observer were activated; the effective
limits read back as CPU 100%, memory 4 GiB, swap zero, tasks 128 and file descriptors
1,024. Persistent unit and boot dependencies were read back. Existing unrelated
services and credentials were preserved, and operational SQLite was backed up
before derived-cache recovery. No source writes or uploads occurred.

Startup repeatedly failed after approximately 50 seconds with successful Notion
reads but no embedded chunks or page commits. There was no cgroup OOM event. The
owned service was stopped after four automatic restarts to avoid repeating the
same unsuccessful recovery; no stable full-root sync or search is claimed.

A counts-only probe through production scoped page preparation found one
4,717-byte / 2,248-token chunk, exceeding the unchanged 1,024-token provider limit.
The HTML span finder incorrectly extended an opener to the last sibling's close.
The source itself contains independent complete elements; its largest genuinely
balanced container is 792 characters. This is tracked as the concrete prerequisite
[#646](https://github.com/mtandersson/notion-knowedge/issues/646), with focused draft
[#647](https://github.com/mtandersson/notion-knowedge/pull/647).

An isolated diagnostic using the current provider and that coalesced input timed
out at 330 seconds under the same 4-GiB / one-CPU envelope; peak memory was
4,126,752,768 bytes and CPU time 325.962 seconds. This diagnostic is a debug build,
not deployed release acceptance. Its failure does not justify increasing model
admission or the resource cap.

The locally corrected, independently reviewed chunking design prepares the exact
canonical source as six chunks with token counts 142, 449, 506, 493, 475 and 188.
This fits the existing context bound without truncation or source changes. These
are local preparation results, not proof of deployed embeddings, full-root sync,
resource sizing or usable search. A tested release of the correction must pass
review/CI before resuming the owned service and completing the remaining checks.
