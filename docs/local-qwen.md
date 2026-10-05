# Local Qwen embedding provider

`retrieval::qwen::QwenProvider` implements the production core
`EmbeddingProvider` port behind the explicit `local-qwen` Cargo feature. It
loads Qwen3-Embedding-0.6B from caller-provided local assets, without Notion,
OpenAI, Hugging Face or any other credentials. Loading and inference never
download anything. The server bootstrap is not wired to this provider yet;
index integration and server composition remain separate work.

## Assets and identity

Download these public files once from the pinned revision (requires network,
not credentials), then operate offline:

```sh
mkdir -p /your/local/qwen-assets
for asset in config.json tokenizer.json model.safetensors; do
  curl --fail --location --output "/your/local/qwen-assets/$asset" \
    "https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/resolve/97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3/$asset"
done
```

The provider verifies SHA-256 hashes of the exact owned bytes before loading:

The exact checksums are the `ASSETS` constants in the
[provider source](../crates/retrieval/src/qwen.rs).

Weights, tokenizer and config are pinned together. Assets are about 1.2 GB on
disk; loading converts weights to Float32. The immutable metadata identifies
the provider, weights revision and exact runtime/preprocessing versions. It
also records 1024-dimensional
Float32 vectors, last-token pooling, left padding and L2 normalization. Inputs
are embedded exactly as supplied, without hidden query instructions or
truncation. Retrieval callers should format retrieval instructions themselves.

`persist_metadata(path)` exclusively creates and synchronizes a JSON sidecar;
it refuses to overwrite any existing file. Deserialize it as
`core::embedding::EmbeddingMetadata` and call `ensure_compatible` before reusing
an index. Same-dimensional revision changes also require an explicit rebuild.
A storage adapter must bind this identity to its own index transaction; this
helper does not implement cross-file atomicity or index persistence. On an I/O
failure, a partial sidecar may remain and must not be accepted as a valid record.

The production identity deliberately differs from the isolated spike's
`spike-fastembed-candle-cpu-f32` identity. Do not attach production metadata to
old spike vectors or claim an automatic migration; rebuild under the selected
provider. The spike binaries and their prototype indexes remain experimental.

## Configuration and execution

Construct `QwenConfig::cpu(assets)`, change its public fields as needed, and call
`QwenProvider::load(config).await` inside a Tokio runtime. Call the shared core
`embedding::embed_batch(&provider, inputs)` boundary when indexing.

- `batch_size`: 1..=8 texts per forward pass; default 1. A call may contain up
  to 32 texts and is split in input order, retaining duplicates.
- `device`: explicit `QwenDevice::Cpu`; this build supports CPU only. CUDA and
  Metal requests fail with `UnsupportedModel`, rather than silently falling
  back. Accelerator implementation and verification are future work.
- `max_tokens`: 1..=4096 per text; default 512. Oversized text is rejected,
  never truncated. No input may exceed 32 KiB, and a call may contain at most
  256 KiB total. Each padded runtime batch is limited to 4096 tokens combined.
  These bounds constrain work; they are not a measured memory guarantee.

Empty calls return no vectors without inference. Blank strings and over-budget
inputs return sanitized typed errors. All input token and padded-batch budgets
are checked before any inference; a failed call returns no partial vectors.
Results are validated for exact cardinality, dimension and finite components.

Each instance admits at most one active call; concurrent calls receive
`ResourceExhausted` immediately. CPU work runs on Tokio's blocking pool. Dropping
the future stops waiting, but an already-started forward pass continues holding
its admission permit until completion. There is no unbounded per-instance work
queue. Loading is also blocking work; the composition root owns how many model
instances it loads. Raw runtime messages and input text are never returned or
logged by the adapter.

## Checks and real-model smoke

Use the pinned matched compiler in `.#spike` for this optional feature; the
ordinary bootstrap remains model-free. This is the same upstream Rust/LLVM
compatibility boundary verified by the original spike; it does not require
LanceDB to use the production provider.

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-qwen --locked
nix develop .#spike --command cargo clippy -p notion-knowledge-retrieval --all-targets --features local-qwen --locked -- -D warnings
nix develop .#spike --command cargo run -p notion-knowledge-retrieval --features local-qwen --locked --example qwen-smoke -- /your/local/qwen-assets /tmp/new-qwen-identity.json 2
```

The smoke uses only four public example sentences: Swedish and English versions
of a sleeping cat, an unrelated database-backup sentence and a duplicate of the
Swedish sentence. It checks four finite 1024-dimensional vectors in order,
identical duplicate vectors, higher Swedish/English similarity than the
unrelated sentence, and persisted identity compatibility. It prints timing and
similarity metrics, never source text or vectors. Choose a new metadata path
each run. It is an opt-in real-model check, not a general retrieval quality or
latency acceptance threshold.

Permanent CI runs provider tests and Clippy with this feature and the matched
Nix shell, without model downloads or credentials. Model-free tests specify
batch splitting/order, duplicates, limits before inference, bounded admission,
invalid-output rejection, missing/tampered assets and metadata persistence.

Verified on the Linux `north` development host on 2026-10-05, using this
production adapter and unoptimized debug build: model load 32,923 ms, four
sentences with batch size 2 in 11,857 ms; related cosine 0.808172 and unrelated
cosine 0.224139. Duplicate vectors and on-disk identity round trip passed.
These are one warm-file-cache development observations, not peak-memory,
cold-cache, production latency or general quality guarantees. The prior spike
reported sampled RSS around 2.5–2.8 GiB; it did not measure this adapter's peak.
