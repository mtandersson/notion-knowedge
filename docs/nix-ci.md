# Nix CI environments and cache decision

The pinned flake is the toolchain authority. Ordinary Rust checks use its
unchanged `default` shell; formatting uses `nix develop .#format --command
cargo fmt --all -- --check`. Both security jobs use `nix develop .#security`.
All shells resolve packages through the committed `flake.lock`.

`format` contains Cargo and rustfmt. `security` contains cargo-audit, Gitleaks,
Git and Python for the actual failure-contract tests and security scripts.
Both use `mkShellNoCC` to avoid an implicit unrelated native compiler. Cargo
and rustfmt still carry their own pinned transitive compiler dependencies;
this format shell is smaller, not a standalone tiny formatter. The default
shell, Docker compiler/base pins and all final-image smoke interactions remain
unchanged. Editing either flake input intentionally invalidates Cargo snapshot
compatibility, even if an edit affects only a CI shell.

## Measured choice

Use fresh Nix binary-cache downloads, with no persistent GitHub Nix-store
cache. [Hosted probes and their ledger](nix-cache-evidence.md) compared actual
cold saves and cross-run restores against fresh downloads on Linux/X64
`ubuntu-latest` runners. Warm cache restoration plus setup and post-action cost
was 30 s versus 24 s fresh for default, 25 s versus 23 s fresh for the selected
format shell, and 16 s versus 16 s fresh for security. Cold saves added 15–19 s
(default/format) or 10 s (security). Cache archives also occupied approximately
813/582/242 MiB respectively, competing with useful Cargo and Docker archives.
The security warm tie is not a demonstrated speedup.

These are individual hosted observations, not a statistically repeated full-CI
campaign; #155 owns that comparison. The selected fresh path is faster in the
observed default/format comparison and avoids save overhead and retained store
storage for all three. It needs no Nix cache key, cache trust permissions,
main warming or cache eviction policy. The ordinary Nix binary cache verifies
store content; installing a GitHub cache action is not assumed to help.

The rejected candidate used an immutable cache-nix-action SHA, exact
OS/architecture/shell/both-flake-input keys without fallback, a 3 GiB
best-effort garbage collection ceiling and GitHub merge-ref isolation. Separate
flake.nix/comment and flake.lock/reformat probes missed the old archives. Those
probes establish byte-key invalidation, not a compiler-version upgrade. The
candidate/probes are removed from production CI; immutable benchmark commits
retain their reproducible workflow. Only task-owned benchmark cache entries are
cleaned after recording their sizes and restoration evidence.

Security freshness is independent of shell setup: cargo-audit still fetches
fresh RustSec advisories on every PR, with no stale/offline fallback or broad
suppression. Gitleaks still scans complete reachable history with redacted
findings. Neither advisory databases nor scan results are cached. Full main and
manual coverage, stable check names and the always-running CI gate remain.

## Reproduce

On a clean runner with Nix installed:

```sh
NIX_BENCH_SHELL=default python3 scripts/benchmark-nix.py
NIX_BENCH_SHELL=format python3 scripts/benchmark-nix.py
NIX_BENCH_SHELL=security python3 scripts/benchmark-nix.py
nix develop .#format --command cargo fmt --all -- --check
nix develop .#security --command python3 scripts/test-security-scanning.py
nix develop .#security --command ./scripts/check-dependencies.sh
nix develop .#security --command ./scripts/check-secrets.sh
python3 scripts/test-cargo-cache.py
```

Use separate fresh runners for comparable cold measurements: repeating locally
reuses the existing store and `/tmp/nk-nix-benchmark-profile`. The script prints
Nix's missing-path/download plan, measures plan evaluation separately from
activation, counts actual copied paths, and reports the activated closure's NAR
bytes. Compare total environment spans plus restore/save cost, not activation
alone. Nix's advertised compressed download sizes exclude HTTP overhead;
closure NAR bytes, whole-store cache bytes and archive transfer bytes differ.
