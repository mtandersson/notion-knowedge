# Qwen vector baseline (#98)

The `qwen-eval` example implements the existing harness adapter contract with
real, pinned `Qwen/Qwen3-Embedding-0.6B` CPU embeddings. It accepts the harness's
judgment-free request and supports only `vector`. It embeds every fixed chunk,
then uses exhaustive cosine ranking over normalized vectors with chunk-ID tie
breaking. This measures model retrieval before ANN approximation; it does not
measure production LanceDB recall, authorization, HTTP latency, or hybrid search.

Documents include title, status, update date and outgoing fixture link IDs,
followed by the unchanged chunk text. Queries use their exact supplied text,
with no instruction prefix. The provider checks pinned model asset hashes and
rejects oversized inputs rather than silently truncating. Batch size is 1 and
maximum sequence length is 512 tokens. Model identity and per-query timings are
written separately from the harness's stable ranked-ID/metric report; the
measurement file must not already exist.

Build and run from the repository root with locally provisioned model assets:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-qwen --example qwen-eval
nix develop .#spike --command cargo build -p notion-knowledge-retrieval \
  --features local-qwen --example qwen-eval
python3 scripts/retrieval-eval.py \
  --adapter 'python3 scripts/qwen-eval-measure.py target/debug/examples/qwen-eval /path/to/assets /tmp/qwen-measurements.json' \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector --top-k 10 --timeout 300 --output /tmp/qwen-ranking.json
```

On Linux, the wrapper records whole-process peak resident memory using child
`ru_maxrss` (KiB), including model loading and inference. Timings use monotonic
wall time, with model load, corpus embedding and each query measured separately.

The corpus is 14 chunks from 12 fictional pages, with 24 queries paired across
Swedish and English. These measurements describe this small fixture only.
Measured scores, memory, environment and weak query classes will be recorded
with the baseline artifact after the model run completes.
