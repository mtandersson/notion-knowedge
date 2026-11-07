# Temporary security dependency patches

## Temporary Arrow IPC / Lance encoding compression dependency patches (#562)

Exact published archives from crates.io:

| Package | Archive SHA-256 | Effective change |
| --- | --- | --- |
| arrow-ipc 58.4.0 | `29a908a11fcfb3fb2f6730f4ac15e367bc644e419155e96238f68cf3adde572b` | normalized `Cargo.toml` zstd `0.13.0` → `0.14.3` |
| lance-encoding 12.0.0 | `7a2c82ceed3511490555c211df37dd774c31e3bca0c0566aa0ae5be7b0701549` | normalized `Cargo.toml` zstd `0.13` → `0.14.3` |

Both archive checksums match the original registry lockfile checksums. All Rust,
protobuf, benchmark and upstream test sources remain byte-for-byte unchanged.
Original manifests and Arrow LICENSE/NOTICE are retained. Package lockfiles,
Cargo VCS metadata, `.gitignore` and upstream agent guidance are omitted. The
Lance package omits a license file; its Apache-2.0 LICENSE is copied from its
published VCS revision `cbeec97cb893a66ad6b3db87f25a57ab0093f358`.

As checked 2026-10-26, published Lance encoding 13.0.0 (LanceDB 0.40.0) still
requires zstd 0.13. Arrow IPC 60.0.0 uses zstd 0.14, but the installed Lance
stack requires Arrow 58; upgrading Arrow alone cannot replace that graph. The
two manifest patches select published zstd 0.14.3, zstd-safe 8.2.0 and zstd-sys
2.1.2+zstd.1.5.7 without changing Arrow/Lance APIs or storage versions. This
removes RUSTSEC-2026-0391/0398/0403/0410 and the affected zstd-safe unsoundness
warnings without advisory suppression. The Candle/ZIP backport was removed after matching registry Candle 0.11.1
shipped ZIP 9.1.3 support (#420).

Both the workspace and standalone Qwen/Lance spike patch these two consumers.
Remove each patch when a compatible published consumer accepts patched zstd;
recheck both locked graphs, persisted compression interoperability and full CI.
Do not replace the model/tokenization/vector identity or migrate stored indexes
as part of this dependency-only update.

Compression compatibility can be reproduced with:

```sh
CARGO_TARGET_DIR=/tmp/nk-compression-compat-target \
  nix develop .#spike --command python3 scripts/test-compression-compat.py
```

This pins the original compression trio (0.13.3 / 7.3.0 / 2.1.0) and patched
trio (0.14.3 / 8.2.0 / 2.1.2), running consumers in separate disposable fixture-only
processes outside the checkout, checking both reading directions for ZSTD Arrow
IPC files (nullable multilingual columns, repeated batches), Lance length-prefixed
blocks and legacy raw streaming frames. It never introduces the vulnerable
original dependency into either application lockfile. The original compressor is
used only on generated nonsecret input; this is storage interoperability evidence,
not model-quality or deployed-capacity acceptance.
