# Configured backend evaluation (#452)

This protocol evaluates the production guarded retrieval implementation on the
unchanged fictional corpora. Measurements are pending; the adapter and passing
protocol tests alone do not establish retrieval quality or complete #12.

`production-eval` commits canonical chunks through `IndexCommitCoordinator` and
`GuardedChunkTable`, using the pinned real Qwen provider, then queries the actual
Lance vector index, production lexical indexes and reciprocal-rank fusion.
Optional graph runs use the committed relation projection and the existing
graph reranker. No relevance judgments enter indexing or ranking. The harness
alone combines ranked IDs with judgments after retrieval.

## Corpus and configuration

The personal-knowledge corpus contains 12 fictional pages, 14 fixed chunks and
24 Swedish/English queries. Exact-identifier evaluation uses its separate,
unchanged eight-page/eight-query corpus. Each corpus gets its own directory;
vector, FTS, hybrid and paired graph runs reuse that corpus's committed snapshot.

Fixture page IDs map deterministically to UUID-shaped IDs from the first 128
bits of their SHA-256 digest. Chunk IDs map to full SHA-256 digests. The manifest
records the reverse mapping, corpus identity, configured root/workspace,
model identity and configuration. These IDs represent local fictional pages.

| Setting | Evaluation configuration | Shipping configuration |
| --- | --- | --- |
| Vector implementation | Guarded Lance IVF_FLAT, Cosine | Same implementation and metric in `--index ensure/rebuild` |
| Partitions | Explicitly 1 | Automatic partition selection |
| Training sample rate | 8 | 256 |
| Maximum training iterations | 20 | 50 |
| Query probes | 1 | 1 in the installed retrieval composition |
| Lexical retrieval | Production body/title/identifier indexes | Same implementation |
| Hybrid | Default RRF: constant 60, weights 1/1, candidate limit 100 | Same default fusion |

The smaller training configuration accommodates these tiny corpora. Its scores
do not establish deployed multi-partition ANN recall, concurrency or capacity.
The adapter does not change shipping defaults.

The provider is `Qwen/Qwen3-Embedding-0.6B`, revision
`97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`, using local verified assets and
1024-dimensional CPU vectors. See [the Qwen baseline](QWEN_BASELINE.md) for the
provider identity and asset manifest. Production preparation embeds **raw,
unchanged chunk text**. Titles, status, dates and links remain canonical metadata;
they are not prepended to embedding text. Queries use their supplied text.
The earlier exact-vector baseline prepended metadata, so differences from that
report cannot be attributed solely to ANN or fusion.

The adapter rejects changed corpus/model/configuration, missing manifests or
indexes, and changes to pinned index bytes, inventory version or operational
state. It checks root filtering, authoritative fixture authorization, source
expansion, source identity and edit timestamps. Reads must leave committed
index/state bytes unchanged. Its authority is a fixed local fixture, not live
Notion or OAuth evidence.

## Reproduce

Provision the public pinned Qwen assets externally using the repository's
documented model setup. Use a new state directory for each corpus. The commands
below assume paths without spaces; adapter commands are parsed without a shell.

```sh
nix develop .#spike --command cargo build --release --locked \
  -p notion-knowledge-retrieval --example production-eval \
  --features local-qwen,local-lancedb --target-dir target/production-eval

NK_EVAL_ASSETS=/absolute/path/to/verified-qwen-assets
NK_EVAL_RUN_DIR=$(mktemp -d)
NK_EVAL_BINARY="$PWD/target/production-eval/release/examples/production-eval"

python3 scripts/retrieval-eval.py \
  --adapter "$NK_EVAL_BINARY $NK_EVAL_ASSETS $NK_EVAL_RUN_DIR/personal" \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector,fts,hybrid --top-k 10 --timeout 3600 \
  --output "$NK_EVAL_RUN_DIR/personal-ranking.json"

python3 scripts/retrieval-eval.py \
  --adapter "$NK_EVAL_BINARY $NK_EVAL_ASSETS $NK_EVAL_RUN_DIR/exact" \
  --dataset eval/retrieval/exact-identifiers-v1.json \
  --modes fts,hybrid --top-k 3 --timeout 3600 \
  --output "$NK_EVAL_RUN_DIR/exact-ranking.json"
python3 scripts/exact-identifier-gate.py \
  --dataset eval/retrieval/exact-identifiers-v1.json \
  --report "$NK_EVAL_RUN_DIR/exact-ranking.json"

python3 scripts/graph-boost-eval.py \
  --adapter "$NK_EVAL_BINARY $NK_EVAL_ASSETS $NK_EVAL_RUN_DIR/personal" \
  --top-k 10 --timeout 3600 --output "$NK_EVAL_RUN_DIR/graph.json"
```

Repeat the harness commands against the same directories and compare ranking
JSON byte-for-byte. Keep variable timings separate from deterministic rankings.
Record the source commit, executable build profile, host/CPU/thread settings,
dependency versions, corpus file digests, model manifest, index configuration
and immutable snapshot identity with each published measurement.

The exact-identifier gate retains its existing 75% top-1/top-3 floors, language
coverage and identifier-family checks. A failing floor requires investigation;
changing the threshold is not evidence that the configured backend passed.
The graph protocol compares complete candidate sets, original baseline scores
and matched paths at candidate limit 20, one hop and weight 0.15. Graph boost
stays OFF unless its established gain and negative-control review justifies
activation. A neutral or harmful comparison is a valid measured outcome.

## CI and acceptance boundary

CI runs model-free protocol, metric and gate tests. The guarded adapter boundary
test uses explicitly synthetic embeddings to exercise real persistent storage,
lexical known-answer retrieval, immutable reuse and failure cases. Those vectors
do not certify semantic or hybrid quality. Full real-Qwen measurements remain
opt-in and must be published separately before integrated quality acceptance.

These fictional fixtures cannot prove private-workspace recall, live ChatGPT
behavior or deployed service limits. Those requirements remain with #109/#110
and the production operations acceptance work.
