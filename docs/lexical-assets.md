# Offline bilingual lexical assets (#489)

The `lexical-asset` example prepares the literal dictionary and pure bounded
planner specified by [the reviewed design](design/multilingual-lexical-expansion.md).
It does not install or enable expansion in the shipping server. Native bounded
execution, index migration, runtime wiring and quality measurements are tracked
by #490–#493. Graph stays OFF; no cutover gate has passed from this work.

## Preparation

Obtain the exact public FreeDict/WikDict eng-swe 2025.11.23 source archive from
the [official release directory](https://download.freedict.org/dictionaries/eng-swe/2025.11.23/).
Keep it outside the repository. The importer performs no network requests and
accepts only the archive and member hashes in the
[pinned design manifest](design/multilingual-lexical-assets-v1.json).

From a clean, committed checkout and the pinned toolchain:

```sh
nix develop .#spike
cargo run -p notion-knowledge-retrieval --example lexical-asset --locked -- \
  /absolute/path/freedict-eng-swe-2025.11.23.src.tar.xz \
  /absolute/path/new-lexical.bundle
```

The output path must not exist. The JSON output gives the bundle SHA256, byte
size, relation count and importer identity digest. Save it with the bundle for
future trusted index binding; it is not caller-selected search policy. Keep
original source attribution: FreeDict/WikDict, Karl Bartel, Wiktionary via DBnary,
CC BY-SA 3.0 Unported. The bundle embeds the exact original COPYING, source URL,
edition and transformation attribution. No dictionary archive or source data is
embedded in the repository.

Import verifies the complete archive SHA256 and SHA512 before decompression.
Decoded input is capped at 64 MiB with a 64 MiB decoder memory limit; compressed
input is capped at 16 MiB. Only regular, unique, nontraversing archive paths are
accepted. Nothing is extracted; only the declared TEI and COPYING are retained.
XML has a depth cap of 32 and 250,000 entry cap. Only the pinned external DTD
declaration is ignored; other declarations and unknown entities fail, and no
external resource is resolved. Direct forms and Swedish literal translation
quotes become relations; examples, definitions and usage do not.

The format stores sorted, deduplicated bidirectional literal relations with the
first public entry ordinal for each relation. It does not infer synonyms through
shared translations. NFC, Unicode lowercase, NFC and Unicode word boundaries
normalize lookup; punctuation/operators, overlong words and phrases beyond four
words are excluded. Unsupported morphology remains unsupported.

## Identity and immutable loading

The canonical bundle embeds planner/schema version, importer source revision
and source digest, Cargo.lock digest, compiler version and Unicode tables, exact
normalization/segmentation versions, input/member hashes and attribution. The
source digest identifies development changes too. Prepare release assets from
the committed build that will consume them. A different importer identity
requires a newly prepared asset and compatible cache rebuild/resync (#577); the
loader never relabels a bundle as compatible.

`Dictionary::load(path, trusted_sha256)` opens a regular local file without
following symlinks on Unix, reads at most 128 MiB and verifies the trusted digest,
canonical metadata, original license, relation bounds/order and current identity.
It validates record/string allocation accounting before allocating entries:
encoded input, record storage and conservative string allocation overhead must
fit within 256 MiB. The resulting dictionary owns its data. Replacing the path
cannot change an already loaded handle. Other platforms currently reject file
loading; the pure planner remains portable.

The owning adapter must identify exact stable IDs through its existing validated
identifier contract and request bypass explicitly. Quoted whole-query phrases
also bypass expansion. Other queries inspect at most 128 word positions and the
first eight nonoverlapping longest matching units, including ambiguous units.
Units with more than four targets add no translations. Sorted round-robin
selection emits at most eight distinct literal phrases. Bounded/ambiguous/
unmatched counters contain no query text. Generated phrases and plans must
never be printed in diagnostics or logs.

## Verification

```sh
cargo test -p notion-knowledge-retrieval --lib lexical_dictionary --locked
NK_LEXICAL_TEST_ARCHIVE=/absolute/path/freedict-eng-swe-2025.11.23.src.tar.xz \
  cargo test -p notion-knowledge-retrieval --lib pinned_public_asset --locked -- --ignored
cargo clippy -p notion-knowledge-retrieval --all-targets --locked -- -D warnings
```

The explicit public-asset test imports twice, compares complete bytes/digests,
loads the actual bundle and exercises corruption, incompatible identity,
no-overwrite and immutable loaded-data behavior. It needs no private corpus,
Notion credential or API. Normal CI runs independent planner/archive/XML/bundle
boundary tests without downloading the dictionary. These checks do not measure
production retrieval quality or establish parent acceptance.
