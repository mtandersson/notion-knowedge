# Product deployment acceptance: issue 455

Status: preflight verified on 2026-11-15; live acceptance is pending.
This records the one-way cache contract in #1 and does not certify production
readiness, callback delivery, installation or cutover.

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
