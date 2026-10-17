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
