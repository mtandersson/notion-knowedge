# Multilingual model comparison (#99)

The opt-in `e5-eval` example compares pinned `intfloat/multilingual-e5-small`
(revision `614241f622f53c4eeff9890bdc4f31cfecc418b3`, 384 dimensions) with the
[Qwen baseline](QWEN_BASELINE.md). It is evaluation code, not a selectable
production embedding provider. Production defaults and index metadata remain
unchanged.

Both examples share one judgment-free input, document formatter, complete fixed
chunk corpus, query iteration, exact cosine ranking, tie breaking and timing
protocol. E5 uses its documented `passage: ` and `query: ` prefixes, masked-mean
pooling and L2 normalization; Qwen retains the current exact-text policy with
last-token pooling and no prefix. This compares these two model/preprocessing
pipelines, not model architecture alone. Both use CPU Float32, batch size 1 and
a 512-token bound, rejecting overlength inputs instead of silently truncating.
All inference assets are local and pinned by SHA-256. No relevance judgments are
passed to either adapter.

Upstream methodology: [pinned E5 model card](https://huggingface.co/intfloat/multilingual-e5-small/blob/614241f622f53c4eeff9890bdc4f31cfecc418b3/README.md),
[Candle BERT implementation](https://github.com/huggingface/candle/blob/0.11.0/candle-transformers/src/models/bert.rs).

Run focused model-free tests and build optimized examples:

```sh
nix develop .#spike --command cargo test --release -p notion-knowledge-retrieval \
  --features local-qwen,eval-e5 --example qwen-eval --example e5-eval
nix develop .#spike --command cargo build --release -p notion-knowledge-retrieval \
  --features local-qwen,eval-e5 --example qwen-eval --example e5-eval
```

The existing harness and Linux peak-memory wrapper accept either executable.
Use the same unchanged dataset, top-k 10 and new measurement paths for each run.
Measured artifacts, environment and an evidence-based recommendation are pending
the actual model runs; synthetic tests do not supply quality scores.

A future production switch requires an explicitly implemented provider and a
new, fully re-embedded index identity. Equal dimensions alone never establish
compatibility. Do not overwrite the Qwen metadata, reuse its vectors with E5,
or delete operational OAuth grants, audit/idempotency receipts or pending
refresh work as a model migration shortcut. The current shipping provider
configuration does not install E5; this benchmark grants no implicit switch.
