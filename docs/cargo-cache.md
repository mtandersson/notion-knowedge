# Cargo CI snapshots

The Clippy, type-check, test and release jobs share repository-local restore
and save actions. Nix remains the compiler/toolchain authority. Canonical Cargo
commands, checks, artifacts and security scanning are unchanged.

## Compatibility and refresh

Compiled snapshot keys have this shape:

```text
nk-cargo-v1-OS-ARCH-JOB-PROFILE-COMPATIBILITY_HASH-SOURCE_HASH
```

The only compiled fallback prefix ends at `COMPATIBILITY_HASH-`. Compatibility
includes every tracked Cargo manifest, Cargo.lock, flake.nix, flake.lock,
`.cargo/` configuration, cache-action/policy code, actual `rustc -vV` / `cargo -V`
from the pinned Nix shell, and compiler/profile/target/linker flag settings.
Jobs and debug/release profiles are separate; incompatible dependency,
toolchain or flag snapshots cannot match the prefix. Legacy broad `target-*`
snapshots are deliberately not restored.

The source suffix includes tracked crate inputs, evaluation fixtures, root
Rust sources and build.rs. Changed source can restore a compatible older
snapshot, build only affected code and save a new immutable key. Subsequent
runs restore that current key. Exact hits do not repeatedly attempt to overwrite
an immutable cache. Add new external build inputs to this policy when introduced.

Fresh checkouts otherwise give unchanged files newer timestamps than archived
Cargo fingerprints. A successful snapshot therefore records only input hashes
and nanosecond timestamps in `target/.nk-cache-source-times.json`. Restore resets
unchanged files to their verified saved timestamps; changed/new content is
touched to the current time even if its commit/checkout was backdated. Cached
metadata never supplies paths to write: only current tracked repository files
are processed. Corrupt metadata falls back to fresh inputs. Source contents are
never restored from the cache. Incremental scratch directories are removed
before saving; compiled outputs/dependencies/fingerprints remain.

## Downloads, trust and bounds

Registry indices, compressed crate downloads and Git dependency databases use
a separate OS/architecture/lockfile key. Their broad fallback is safe because
Cargo still resolves the committed lockfile and checks crate checksums. A
read-only legacy `cargo-OS-` fallback preserves existing download caches during
migration. No Cargo credentials/configuration files are included in these paths.

Save steps run only after successful canonical commands (and release artifact
packaging/upload). Main warms reusable trusted snapshots. Same-repository PRs
save within their GitHub merge-ref scope, which main cannot restore; fork PRs
only restore. No normal CI repository/application secrets are supplied to Cargo.
Keep secret-bearing build environments outside this cache workflow.

One snapshot is retained per job/profile/compatibility/source combination;
unchanged reruns create no new keys. Incremental scratch is omitted to reduce
archive sizes. GitHub's repository quota/LRU and seven-day unused-cache eviction
bound retained historical snapshots; source changes can still evict older keys.
Measure archive sizes and restore/save cost rather than assuming a warm cache
is cheaper for every profile. Retrieval state, models, Nix stores and Docker
layers are separate from this Cargo cache.

## Verify

```sh
python3 scripts/test-cargo-cache.py
nix develop --command python3 scripts/cargo-cache.py key --job check --profile debug --os Linux --arch X64
```

Production restore then runs `prepare`, the canonical Cargo command, and on
success `snapshot` plus explicit cache save. Key output contains hashes rather
than credential/flag values. [Hosted evidence](cargo-cache-evidence.md) records
baseline creation, a genuine source edit with compatible fallback and refreshed
save, exact reuse, dependency/flake invalidation, and per-profile storage/timings.
It distinguishes key selection from Cargo's actual fingerprint decisions.
