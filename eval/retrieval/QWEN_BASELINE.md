# Qwen vector baseline (#98)

The 2026-10-18 baseline uses real, pinned `Qwen/Qwen3-Embedding-0.6B` CPU
embeddings: revision `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`, 1024 dimensions,
Float32, fastembed 7.1.0 / Candle 0.11.0 / tokenizers 0.23.2, last-token pooling,
left padding and L2 normalization. Batch size is 1; maximum sequence length is
512 tokens. The provider verifies the three pinned asset hashes before loading.
Asset digest roles are `config` (`config.json`), `vocabulary`
(`tokenizer.json`) and `weights` (`model.safetensors`). The complete identity
and hashes are in the [measurement artifact](results/qwen-0.6b-2026-10-18-measurements.json).

The `qwen-eval` adapter embeds every fixed chunk and ranks by exhaustive cosine
similarity, breaking ties by chunk ID. Documents include title, status, update
date and outgoing fixture link IDs, followed by unchanged chunk text. Queries
use exact supplied text with no instruction prefix. This is the current
exact-text provider policy, not an assessment of every possible Qwen prompt.
The harness sends no relevance judgments to the adapter. There is no ANN,
lexical search, graph expansion or source filtering in this model baseline.

The corpus is 14 chunks from 12 fictional pages, with 24 queries paired across
Swedish and English (12 intents). Chunk boundaries, judgments and the fixed
`as_of` date remain unchanged. Scores describe this small hand-authored fixture,
not private workspace or general retrieval quality. A relevant chunk means any
grade 1–3; MRR can reward background before the direct answer. nDCG uses the
harness's exponential graded gain. See the [metric definitions](README.md#ranking-metrics-96).

## Measured retrieval

The [ranked-ID artifact](results/qwen-0.6b-2026-10-18-ranking.json) records every
query, rank, grade and metric. All 24 queries have a grade-3 chunk within top 10.
A second optimized run over the unchanged inputs produced byte-identical ranking
and metric JSON (`cmp` succeeded); variable timings remain separate.

| Cutoff | Macro Recall | Macro nDCG |
| --- | ---: | ---: |
| 1 | 0.4931 | 0.5417 |
| 3 | 0.9375 | 0.8098 |
| 5 | 0.9583 | 0.8040 |
| 10 | 1.0000 | 0.8272 |

Macro MRR is **0.8056**. Group means include all queries, including failures:

| Group | Queries | MRR | Recall@3 | nDCG@3 |
| --- | ---: | ---: | ---: | ---: |
| Semantic | 6 | 0.8056 | 0.9167 | 0.7850 |
| Exact name | 6 | 0.7500 | 1.0000 | 0.8259 |
| Date/status | 6 | 1.0000 | 0.8333 | 0.8824 |
| Cross link | 6 | 0.6667 | 1.0000 | 0.7458 |
| English | 12 | 0.8194 | 0.9167 | 0.8086 |
| Swedish | 12 | 0.7917 | 0.9583 | 0.8109 |

Weak classes and observed mistakes:

- **Linked-source questions:** weakest MRR (0.6667). Energy questions rank the
  review ahead of the directly answering decision; study questions rank the
  referring routine ahead of the requested reading queue in both languages.
  Embedding outgoing link IDs is not graph traversal.
- **Exact identity with archived near-matches:** both bike queries put the
  archived service record before the current record. The Swedish project-name
  query puts a related review ahead of the named project. Exact-name Recall@3
  therefore hides wrong first results; this supports retaining lexical and
  status-aware regression evaluation rather than declaring vector search enough.
- **Watering semantics:** the English query ranks travel preparation and a
  gardening plan before watering instructions; the Swedish query also favors
  the broader plan. Top-one semantics still needs evaluation beyond this fixture.
- **Multiple relevant chunks:** date/status MRR is perfect but Recall@3 is
  0.8333; finding one relevant source does not retrieve every judged source.
  Paired translations and six-query category groups are too small for a general
  language or category superiority claim.

## Measured latency and memory

The optimized release build used Rust 1.98.1 on Linux 6.18.51 x86_64, an AMD
Ryzen 7 7800X3D (8 cores / 16 logical CPUs), about 30.5 GiB host RAM. This was a
shared host without CPU isolation, cache eviction or concurrency/load sweep.
One run provides observations, not confidence intervals or a throughput SLA.

| Measurement | Observed |
| --- | ---: |
| Model load, including asset checks | 2859.70 ms |
| Embed all 14 corpus chunks | 6707.06 ms |
| Query embedding, min / median / max (24) | 249.28 / 292.89 / 329.54 ms |
| Query embedding, mean | 291.27 ms |
| Exact search over 14 vectors, median / max | 0.0121 / 0.0149 ms |
| Whole adapter peak resident memory | 3,519,744 KiB (3.36 GiB) |

Timings use monotonic wall time. The Linux wrapper records child `ru_maxrss`
for the complete adapter, including model loading and inference; this is not
idle model memory or whole-server RSS. Per-query samples remain in the artifact.
No indexed storage, network, MCP authorization or HTTP overhead is measured.
These timings and exhaustive scores do not establish production LanceDB ANN
recall, hybrid quality, end-to-end latency or multi-user capacity.

## Reproduce

Provision the exact assets locally as described in [local Qwen setup](../../docs/local-qwen.md).
No Notion credentials, model download during evaluation or private content are
required. From the repository root:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-qwen --example qwen-eval
nix develop .#spike --command cargo build --release -p notion-knowledge-retrieval \
  --features local-qwen --example qwen-eval
python3 scripts/retrieval-eval.py \
  --adapter 'python3 scripts/qwen-eval-measure.py target/release/examples/qwen-eval /path/to/assets /tmp/qwen-measurements.json' \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector --top-k 10 --timeout 3600 --output /tmp/qwen-ranking.json
python3 scripts/test-retrieval-eval.py
python3 scripts/test-retrieval-metrics.py
```

Use a new measurements path on each run; the adapter refuses to overwrite one.
The harness's normal deadline remains 60 seconds; an explicit CPU-run deadline
may be up to 3600 seconds. Unsupported modes and malformed responses fail rather
than fabricate zero scores. Model rankings/metrics should reproduce independently
of variable wall time and memory; keep the measurement sidecar separate.
