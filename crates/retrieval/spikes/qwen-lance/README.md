# Local Qwen / LanceDB feasibility experiment (#212)

Experimental, isolated Cargo workspace owned by the retrieval adapter. It uses
`core::EmbeddingProvider`, `embed_batch` output validation and schema-v1
`EmbeddingMetadata`. It does not wire the production server or implement all
production provider/table criteria in #39/#37. No Notion credentials are used.

## Reproduce

From the repository root on Linux x86_64 (CPU):

```sh
nix develop .#spike
./crates/retrieval/spikes/qwen-lance/download-assets.sh /tmp/qwen-assets
cargo build --locked --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2
exe=crates/retrieval/spikes/qwen-lance/target/debug/qwen-lance-spike
/usr/bin/env time -v "$exe" create /tmp/qwen-assets /tmp/qwen-index
/usr/bin/env time -v "$exe" query /tmp/qwen-assets /tmp/qwen-index
```

Use an unused index directory for `create`; existing indexes are never overwritten.
`create` and `query` are separate executable processes: query reloads the actual
model and opens the on-disk Lance table. The two synthetic bilingual texts and
single Swedish question are in `src/main.rs`; backup storage must rank first.
The score is `1 - cosine distance`, and every returned score must be finite.
There is no vector index: querying explicitly bypasses ANN for exact scan.
Model downloads are explicit, public, credential-free, and outside Git. Runtime
requires all three local assets and checks their SHA256 against the immutable
upstream revision before loading. Never edit assets during execution.

## Vector identity and limitations

Qwen/Qwen3-Embedding-0.6B revision
`97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`, 1024 float32 dimensions, CPU F32
inference, Candle 0.11.0 / fastembed 7.1.0. Documents use raw text; the query has
Qwen's `Instruct: ...\nQuery: ...` retrieval template. Tokenization uses left
batch padding, no extra EOS and no truncation; pooling uses the last real token
followed by L2 normalization. The executable only exposes its tiny fixtures;
production #39 must enforce input/resource limits and model-specific validation.
The async provider currently performs synchronous CPU work while polled; model
execution is not cancellable once started. Production needs bounded worker
execution and cancellation semantics. Owned safetensor bytes avoid unsafe mmap
lifetime assumptions but increase transient memory.

LanceDB 0.39.0 / Arrow 58.4.0 schema: UTF8 `chunk_id`, UTF8 `text`, UTF8 `chunk_record` (serialized canonical schema-v1 IndexedChunk),
FixedSizeList<Float32,1024> `vector`. Schema metadata contains the complete core
embedding identity; a schema-v1 `embedding.json` sidecar duplicates it for cheap
preflight before loading the model. Both identities and the actual vector
column type/dimension are validated before vector search. The fixture chunk IDs
and source metadata are synthetic; the complete canonical IndexedChunk JSON is
persisted in `chunk_record`. Production #37 retains real page/chunk provenance,
fingerprints, timestamps, filters and schema migration requirements. Useful
reusable seams are the real core provider boundary, identity check, local model
loader and Arrow/Lance exact-scan path, rather than this fixture table itself.

The dedicated Nix shell adds protobuf (`protoc` required by Lance encoding),
pkg-config and OpenSSL to the pinned Rust tooling, plus curl and GNU time.
CPU kernels use Rust/Candle; ONNX is configured for dynamic loading solely to
avoid fastembed's unused default ONNX download/link path. The Qwen code path
never initializes ONNX and requires no ONNX shared library. No GPU or remote
inference is involved. The spike shell pins upstream Rust 1.98.1 with its bundled
LLVM 22.1.8 through the locked rust-overlay input, including matching Clippy and
rustfmt. The distro Rust 1.98.1 / LLVM 21.1.8 pairing fails AVX512 code generation
required by Lance 12.0.0. The `remote` compile-time feature works around LanceDB
0.39.0's unconditional `Error::Http` references; the executable still connects
only to the supplied local directory. See the report for the compiler finding
and remaining unsuppressed dependency warning.

Check both compiler optimization paths and the model-free specifications:

```sh
rustc --edition=2024 crates/retrieval/spikes/qwen-lance/compiler-reproducer.rs -o /tmp/nk-avx512
rustc --edition=2024 -C opt-level=3 crates/retrieval/spikes/qwen-lance/compiler-reproducer.rs -o /tmp/nk-avx512-opt
cargo test --locked --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2
cargo clippy --locked --all-targets --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2 -- -D warnings
cargo fmt --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -- --check
python3 crates/retrieval/spikes/qwen-lance/check-failures.py "$exe"
```

Run these in `nix develop .#spike`; the reproducer compiles but never executes
AVX512 instructions. The failure checks require a built binary and use
disposable invalid fixtures; they do not require model assets.

Compile resources and model runtime resources must be reported separately. Models/indexes are disposable local state.

## Evidence checkpoint

Investigation and observed execution results are recorded in
[the spike report](../../../../docs/local-vector-spike.md). The experiment
alone does not close production #37/#39 or the end-to-end parent #211.

## Explicit semantic MCP configuration (#214)

`serve-stdio ASSETS INDEX` and `serve-http ASSETS INDEX` inject the experimental
semantic adapter into the existing shared MCP handler. HTTP reuses the server
library's transport wiring; both transports return the same contract and errors.
The default production server still has no retrieval adapter. See
[semantic MCP reproduction and actual-response evidence](../../../../docs/semantic-mcp-spike.md).
Neither serving mode reads Notion or requires a token. The already-derived index
and raw returned source excerpts remain local outside Git.
