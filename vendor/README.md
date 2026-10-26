# Temporary Candle security dependency patch

Source: crates.io `candle-core` 0.11.0, upstream commit
`31f35b147389700ed2a178ee66a91c3cc25cc80d`, directory `candle-core`.
The upstream package LICENSE is retained. The normalized `Cargo.toml` ZIP
dependency changes from 8.6.0 to 9.0.3. Three filename-reading sites in
`src/npy.rs` and `src/pickle.rs` propagate ZIP 9 filename-decoding errors
instead of treating filename iteration as infallible. No other source changes.
The upstream package lockfile and local Cargo cache marker are omitted.

This resolves RUSTSEC-2026-0367, RUSTSEC-2026-0368 and
RUSTSEC-2026-0369 without suppressing vulnerability scanning. Candle's current
published 0.11.0 and upstream main still require ZIP 8.6.0. ZIP 9 makes filename reads fallible; the narrow adapters propagate those errors.
Feature CI must verify the complete Qwen path.
Model versions, tokenization and vector compatibility contracts do not change.

Remove this path patch and vendor directory when a released Candle version
requires patched ZIP. Update both Candle crates together and run local-qwen,
webhook-worker and full security CI. Track removal with issue #415 evidence.

The three filename adapters match upstream [Candle PR #4041](https://github.com/huggingface/candle/pull/4041)
at commit `70f72e9cfdbfc570846ca363ead2135394af3e9a`. That PR is open as of
2026-10-17. Its head is 67 commits ahead of the published source, so this
backport avoids unrelated kernel/tensor changes. Compatibility regressions
cover ordinary and UTF-8 names, legacy CP437 names, and malformed NPZ/PTH
archives through the public Candle API, following the upstream test cases.

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
warnings without advisory suppression. The existing Candle/ZIP patch and its
#420 release trigger are unchanged.

Both the workspace and standalone Qwen/Lance spike patch these two consumers.
Remove each patch when a compatible published consumer accepts patched zstd;
recheck both locked graphs, persisted compression interoperability and full CI.
Do not replace the model/tokenization/vector identity or migrate stored indexes
as part of this dependency-only update.
