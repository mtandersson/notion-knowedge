# Cache objective acceptance audit (#154)

This audit covers the three implementation leaves of #154, under #151. It does
not close either parent. #155 owns the final comparable cold/warm PR campaign,
docs-only timing, and optional consolidation experiment. The immutable shared
baseline remains main `4167a4726688fd79d9e5e831ad8e9a7fc4c321a6`,
[PR #152 run 36963826931](https://github.com/mtandersson/notion-knowedge/actions/runs/36963826931)
and [main run 36964071809](https://github.com/mtandersson/notion-knowedge/actions/runs/36964071809).

| Original #154 boundary | Merged implementation / live evidence |
| --- | --- |
| Cross-run Docker persistence, explicit import/export | #167 / PR #170; [Docker evidence](docker-cache-evidence.md) identifies compilation RUN and executable COPY hits on a new hosted runner |
| Real final-image smoke and pinned builder | `smoke-container.py --prebuilt --builder-image` verifies identity, pinned bases, linkage, non-root volume writes, configuration errors, diagnostics, stdio/HTTP/session/health/shutdown after restoration; compilation remains in Docker |
| Docker invalidation | Source, Cargo.lock and alternate immutable builder probes each recompile and pass final-image smoke in run 37023454091 |
| Cargo source refresh / subsequent reuse | #168 / PR #172; [Cargo evidence](cargo-cache-evidence.md) links genuine source change, strict compatible fallback, new snapshot save and later exact hit; fingerprint logs distinguish workspace from external compilation |
| Cargo dependency/toolchain compatibility | Four live manifest/lock/flake probes miss compiled snapshots; production keys also include actual pinned compiler identity, OS/architecture, job/profile and flags; local tests cover those boundaries |
| Registry/Git downloads and trusted warming | Separate verified download cache remains; main snapshots are reusable, fork PRs only restore, successful same-repository PR saves stay merge-ref isolated |
| Cache size / cost / growth | Docker evidence distinguishes runtime transfers, stored blobs and image sizes; Cargo records compressed archive bytes and separate restore/save costs. Immutable Cargo source snapshots and BuildKit scopes remain subject to repository quota/LRU and unused-cache expiry |
| Pins and flake authority | Added Docker/cache actions use immutable SHAs; ordinary Rust commands and compiler identity come from the pinned default Nix flake; container compiler remains its pinned Docker builder |
| Security on every PR | Dependency job fetches fresh unsuppressed RustSec advisories; Secret scanning checks complete reachable history with full redaction. Neither result nor advisory database is cached |
| Main/manual/gate | Stable check names, conservative PR selection and always-running aggregate gate remain; main pushes and manual dispatch select every canonical job |
| Nix measured decision | See [Nix evidence](nix-cache-evidence.md) for actual cold/warm transfers, closure sizes, restore/save and environment setup, exact-input invalidation and selected-path verification |

The individual Docker and Cargo measurements prove correctness and persistence,
not a controlled aggregate speedup. Their run links, commits, step timing tables,
cache sizes and trust limits remain available to #155. The Nix decision must
include its cache overhead rather than inferring benefit from an installed action.
Final production and post-merge coverage links are recorded with #169 delivery.

The merged Docker/Cargo evidence and measured Nix decision cover every #154
implementation requirement; no additional cache mechanism is left unproven.
Growth is bounded by a finite key/scope per input snapshot plus GitHub's shared
repository quota, LRU and unused-cache expiry, rather than a promised fixed
number of old entries. Docker/Cargo evidence quantifies retained bytes and
transfer costs; the selected Nix path adds no persistent entries. This policy
can evict useful older caches, a documented cost rather than hidden unlimited
storage. Full clean production PR, post-merge main and manual execution remain
delivery checks for #169 and must be observed before it closes; their exact
links belong in the [#169 delivery record](https://github.com/mtandersson/notion-knowedge/issues/169).
The comparable aggregate campaign and consolidation decision in #155 are
explicitly outside this implementation audit.
