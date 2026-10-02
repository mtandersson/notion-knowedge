# Docker cache verification — #167

## Hosted cross-run reuse

These runs used fresh GitHub-hosted `ubuntu-latest` runners, the same pinned
Rust/container dependencies and release command, and no upstream credentials.
Each ran the real final-image smoke and the full CI gate/security scans.

| Run / immutable head | Final build + load | Probe builder load | Final smoke | Cache export |
| --- | ---: | ---: | ---: | ---: |
| [Cold 37016396050, attempt 1](https://github.com/mtandersson/notion-knowedge/actions/runs/37016396050/attempts/1), `10d0497722d4a4554a11897fa54955263b32b4a3` | 99s | 48s | 8s | 67s |
| [Warm 37023119555](https://github.com/mtandersson/notion-knowedge/actions/runs/37023119555), `9d8cb6eab67c2e4e3a3337c6736818e93205f5aa` | 5s | 31s | 6s | 3s |

Cold job `110868267855` compiled the server in 67.79s and exported intermediate
layers to the PR-170 merge-ref cache. Warm job `110891016676` imported that cache
on a new runner and marked the complete `builder` Cargo RUN and final executable
COPY as `CACHED`. It did not compile the server. The revision label changed with
the checkout and passed the actual image/binary identity checks. Cached final
layers downloaded approximately 11.6 MB compressed; this is runtime layer
transfer, not the size of the full intermediate cache.

The cold head used a full compilation builder for the disposable permission
probe. Its repeat exposed redundant work in that probe path. The warm head
therefore splits a `probe-builder` from the same pinned Rust base, excluding
Cargo outputs; final compilation/runtime behavior is unchanged. These timings
establish cache reuse and overhead, not a controlled whole-workflow speedup.
The comparable parent campaign remains #155. They exclude job queue time and
do not add parallel job durations to produce workflow elapsed time.

## Trust and storage

The production scope is `nk-container-v1-Linux-X64-main`; PRs use a numbered
scope and GitHub additionally isolates them to their merge refs. Forks cannot
export. Exports follow successful final-image smoke. One logical scope tracks
the latest manifest. BuildKit retains versioned small index entries and
content-addressed blobs; old entries can remain until repository quota/LRU or
seven-day unused-cache expiry evicts them. Pins and context exclusions are
documented in [container.md](container.md#ci-layer-cache).

## Input invalidation

[Run 37023454091](https://github.com/mtandersson/notion-knowedge/actions/runs/37023454091),
head `3e781626730ba7c6836ddfd67176104d80b42af6`, used task-owned probes on top
of the production implementation. Job `110892168665` restored the warm baseline
and passed its final smoke, then built each variant from the original inputs
in separate disposable contexts without exporting probe layers.

| Changed input | Fresh Cargo compile | Build + load | Complete probe including final-image smoke |
| --- | ---: | ---: | ---: |
| Appended Rust source comment | 111.9s | 128.86s | 132.88s |
| Appended Cargo.lock comment | 110.9s | 112.32s | 116.29s |
| Alternate pinned Rust 1.98.0 Bookworm builder | 113.9s | 133.22s | 167.87s |

The first two prove byte-level source/dependency-input invalidation; they do not
change behavior or the resolved dependency graph. The builder probe uses
immutable digest `sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922`.
Every changed variant executed the full Cargo compile instead of reusing the
baseline RUN, then passed actual final executable linkage, version/revision,
permissions, diagnostics, stdio/HTTP/session/health, and shutdown checks.
Its probe compiler also used the alternate pinned base. The expected revision
was checkout `b4e0a750f522ac9aea8e22240057379e9dbf59a0`, Cargo version `0.1.0`.

The one-second cache metadata step reported 38 Docker entries under the PR-170
merge ref: 777,688,739 bytes total, including 38,826 bytes of versioned indices
and 777,649,913 bytes of content-addressed blobs. This includes earlier builder
experiments, not just the latest manifest. Final runtime image size was
31,802,953 bytes and probe toolchain image 1,555,704,156 bytes (uncompressed
Docker image sizes, distinct from cache/transfer bytes). The production warm
probe stage downloads approximately 562.5 MB compressed in 31s. Initial export
cost was 67s; subsequent production exports took 3s and reused existing blobs.

All eleven CI jobs, including fresh advisory and full-history redacted secret
scans, passed for production and probe runs. Temporary probe scripts, extra
Actions-read permission, and workflow steps are removed before merge; the
run/commit links retain reproducible code and immutable logs. #168/#169/#155
retain the separate Cargo/Nix and full comparative objectives.
