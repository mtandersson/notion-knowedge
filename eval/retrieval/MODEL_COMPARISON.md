# Multilingual model comparison (#99)

**Recommendation: retain the current Qwen production default.** E5-small is a
promising lower-memory evaluation candidate, but this small fixture shows worse
coverage and graded ranking despite slightly higher first-hit MRR. It does not
justify changing defaults. Broaden the corpus and measure configured production
retrieval under #452 before considering a switch. No production provider,
configuration default or index identity changes in this comparison.

## Protocol and provenance

The 2026-10-18 comparison uses the same unchanged 12-page, 14-chunk corpus,
24 Swedish/English queries, judgments, as-of date and top-k 10 as the
[Qwen baseline](QWEN_BASELINE.md). Both examples share one judgment-free input,
document formatter, query iteration, exact cosine ranking, chunk-ID tie breaking
and measurement protocol. They use CPU Float32, batch size 1 and a 512-token
bound, rejecting overlength inputs rather than silently truncating.

| Pipeline | Pinned revision | Dimensions | Pooling / input policy |
| --- | --- | ---: | --- |
| Qwen/Qwen3-Embedding-0.6B | `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3` | 1024 | Last token, L2, current exact text without query prefix |
| intfloat/multilingual-e5-small | `614241f622f53c4eeff9890bdc4f31cfecc418b3` | 384 | Masked mean, L2, `passage: ` documents / `query: ` queries |

This compares those model/preprocessing pipelines, not architecture alone. E5
follows the [pinned upstream model card](https://huggingface.co/intfloat/multilingual-e5-small/blob/614241f622f53c4eeff9890bdc4f31cfecc418b3/README.md)
and [Candle BERT implementation](https://github.com/huggingface/candle/blob/31f35b147389700ed2a178ee66a91c3cc25cc80d/candle-transformers/src/models/bert.rs).
Candle is 0.11.0 and the explicit tokenizer is 0.23.2; Qwen additionally uses
fastembed 7.1.0. Inference loads only local assets, verified by SHA-256, with no
remote code or credential access. E5 is an opt-in evaluation example, not an
installed server embedding provider.

The [comparison artifact](results/multilingual-comparison-2026-10-18.json)
records complete model identities, config/vocabulary/weights hashes, source,
environment and every timing sample. Digest roles map to `config.json`,
`tokenizer.json` and `model.safetensors`. [E5 rankings](results/e5-small-2026-10-18-ranking.json)
and [Qwen rankings](results/qwen-0.6b-2026-10-18-ranking.json) retain all query IDs,
returned chunks, grades and metrics. Two runs per model used counterbalanced
order (E5 then Qwen, Qwen then E5). Each model's repeated ranked-ID/metric JSON
was byte-identical. The shared-code Qwen control also exactly reproduced #98's
original ranking artifact.

## Quality observations

These are equal-query macro means over fictional hand-authored fixtures, not
private-workspace or general multilingual quality. Recall counts all grade 1–3
judgments; nDCG uses exponential graded gain. See [metric definitions](README.md#ranking-metrics-96).

| Metric | Qwen | E5-small |
| --- | ---: | ---: |
| Recall@1 | 0.4931 | 0.4722 |
| Recall@3 | 0.9375 | 0.8333 |
| Recall@5 | 0.9583 | 0.9514 |
| Recall@10 | 1.0000 | 0.9861 |
| nDCG@1 | 0.5417 | 0.6012 |
| nDCG@3 | 0.8098 | 0.7357 |
| nDCG@5 | 0.8040 | 0.7805 |
| nDCG@10 | 0.8272 | 0.7989 |
| MRR | 0.8056 | 0.8194 |

Both models find a grade-3 chunk within top 10 for all 24 queries. E5's higher
MRR does not imply complete coverage: it misses some other judged-relevant
chunks within top 10 and has lower Recall@3 and graded nDCG@3.

| Group (six queries each) | Qwen nDCG@3 | E5 nDCG@3 |
| --- | ---: | ---: |
| Semantic | 0.7850 | 0.6806 |
| Exact name | 0.8259 | 0.9117 |
| Date/status | 0.8824 | 0.6895 |
| Cross link | 0.7458 | 0.6608 |

E5 improves the watering questions in both languages and the Swedish exact bike
lookup. It also retrieves the linked energy decision earlier. Regressions include
the Swedish energy-purpose query and English linked-garden query (no judged hit
in E5's top three), and active-project/status queries in both languages. The
latter can put archived or merely related pages ahead of current projects.
Neither model resolves all linked-source and current-versus-archived cases.
These paired questions and six-query groups are too small for significance or
a general language-quality claim.

## Latency and memory observations

Both release binaries ran on Linux 6.18.51 x86_64 with Rust 1.98.1, an AMD Ryzen
7 7800X3D (8 cores /16 logical CPUs), about 30.5 GiB host RAM. Other build work
ran on this shared host; there was no CPU isolation, cache eviction or load
sweep. Below are the first and repeat observations, not confidence intervals,
throughput capacity or a production speedup claim.

| Measurement | Qwen first / repeat | E5 first / repeat |
| --- | ---: | ---: |
| Model load including asset checks (ms) | 3414.11 / 2370.14 | 4614.77 / 1126.66 |
| Embed 14 corpus chunks (ms) | 17285.52 / 10103.95 | 1504.78 / 791.18 |
| Query embedding median (ms, 24 each run) | 717.22 / 346.08 | 66.97 / 38.29 |
| Query embedding mean (ms) | 710.19 / 469.77 | 71.74 / 38.01 |
| Exact search median (ms, 14 vectors) | 0.0135 / 0.0130 | 0.0077 / 0.0065 |
| Whole-adapter peak RSS (KiB) | 3519532 / 3518212 | 939980 / 939948 |

E5 used about 0.90 GiB peak RSS versus Qwen's 3.36 GiB in both observations,
and query embedding was faster in both. First-load observations also show why
a small number of wall-time samples should not be summarized as universal model
latency. The Linux wrapper records child `ru_maxrss` including loading and
inference. This is neither idle model memory nor full-server RSS. ANN, BM25,
hybrid, HTTP, authorization, network and concurrent-user overhead are excluded.

## Reproduce

Provision the pinned Qwen assets using [local Qwen setup](../../docs/local-qwen.md).
For E5, use a fresh directory and fetch the three public files; the adapter
checks the recorded hashes before loading them:

```sh
eval_e5_assets=$(mktemp -d)
for asset in config.json tokenizer.json model.safetensors; do
  curl --fail --location --proto '=https' --proto-redir '=https' \
    "https://huggingface.co/intfloat/multilingual-e5-small/resolve/614241f622f53c4eeff9890bdc4f31cfecc418b3/$asset" \
    --output "$eval_e5_assets/$asset"
done
nix develop .#spike --command cargo test --release -p notion-knowledge-retrieval \
  --features local-qwen,eval-e5 --example qwen-eval --example e5-eval --locked
nix develop .#spike --command cargo build --release -p notion-knowledge-retrieval \
  --features local-qwen,eval-e5 --example qwen-eval --example e5-eval --locked
python3 scripts/retrieval-eval.py \
  --adapter "python3 scripts/qwen-eval-measure.py target/release/examples/e5-eval $eval_e5_assets /tmp/e5-measurements.json" \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector --top-k 10 --timeout 3600 --output /tmp/e5-ranking.json
```

Use the same harness invocation for `qwen-eval` with its assets and new output
paths. Repeat in reverse order if comparing timings. Model-free CI tests cover
judgment isolation, ranking/ties, masked pooling and length rejection; they do
not fabricate model quality. Full real-model benchmarks run explicitly outside CI.

## Required migration boundary

Any future production switch requires an explicitly implemented provider,
reviewed model/preprocessing identity and a new, fully re-embedded index. Equal
dimensions alone never establish compatibility. Existing `EmbeddingMetadata`
comparison rejects a changed provider/model/version/dimension; do not overwrite
Qwen metadata or mix E5 vectors into its table. Provision a separately identified
derived index, re-embed the authoritative corpus and validate scope, provenance
and real configured search quality before a deliberate operator cutover. This
benchmark does not implement that production migration or authorize the switch.

Preserve operational OAuth grants, audit/idempotency receipts and pending refresh
work throughout such a migration. Deleting operational state or weakening
compatibility checks is not an index-version migration. Retain Qwen now; evaluate
E5 further if its lower resource footprint is useful and the observed coverage
regressions can be addressed on a broader, reviewed corpus.
