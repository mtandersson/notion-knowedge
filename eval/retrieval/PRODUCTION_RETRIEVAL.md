# Configured backend evaluation (#452)

This protocol evaluates the production guarded retrieval implementation on the
unchanged fictional corpora. The measured backend fails the existing broad
retrieval-quality floors; exact-identifier gates pass and graph boost remains
OFF. These results do not complete #12 or establish primary-connector cutover.

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

## Measured results, 2026-10-18

Release adapter commit `62f1cd085f4c1e2fa3eb2d5606899b2697f99f11`
ran on Linux x86_64, AMD Ryzen 7 7800X3D, using Nix `.#spike` and
Rust 1.98.1. Batch size is 1, CPU float32, maximum 512 tokens, last-token
pooling and L2 normalization. Excess tokens are rejected rather than truncated.
The [provenance artifact](results/production-qwen-2026-10-18-provenance.json)
records binary/source/lock/corpus/asset digests, canonical mappings, full model
metadata, index inventory versions, operational/index byte fingerprints and
repeat-report digests. Both complete retrieval reports and the graph report
were **byte-identical on repeat**, reusing their original persistent indexes.

The first initialized personal-corpus run rejected an adapter assertion that
mistook production heading-section expansion for anchor-only expansion. The
corrected adapter validates complete section text, contributing chunk IDs and
provenance; the retained vectors, chunks, graph and index were unchanged. The
boundary suite now includes the unchanged multi-chunk personal corpus.

The [complete personal-corpus ranking report](results/production-qwen-2026-10-18-personal-ranking.json)
contains all 24 queries, graded ranks and Recall/nDCG at 1, 3, 5 and 10.
Macro means count every query, including no-hit cases:

| Actual path | Recall@5 | MRR | nDCG@5 | Relevant / grade-3 hit at 10 |
| --- | ---: | ---: | ---: | ---: |
| Vector | 0.9306 | 0.6958 | 0.7003 | 24/24 / 24/24 |
| FTS | 0.7361 | 0.7060 | 0.6297 | 21/24 / 20/24 |
| Hybrid | 0.9306 | 0.7928 | 0.7605 | 24/24 / 24/24 |

Language cohorts and all query categories are in the
[cohort artifact](results/production-qwen-2026-10-18-cohorts.json):

| Path / language (12 queries each) | Recall@5 | MRR | nDCG@5 |
| --- | ---: | ---: | ---: |
| Vector / English | 0.9167 | 0.6903 | 0.6952 |
| Vector / Swedish | 0.9444 | 0.7014 | 0.7053 |
| FTS / English | 0.7639 | 0.6968 | 0.6184 |
| FTS / Swedish | 0.7083 | 0.7153 | 0.6409 |
| Hybrid / English | 0.8889 | 0.7801 | 0.7285 |
| Hybrid / Swedish | 0.9722 | 0.8056 | 0.7925 |

Both existing acceptance policies fail, as recorded without changing any
threshold in the [numeric policy review](results/production-qwen-2026-10-18-quality-policies.json).
[ChatGPT smoke G1](../../docs/chatgpt-cutover-acceptance.md) requires hybrid
Recall@5, nDCG@5 and MRR each at least 0.80: recall passes, the other two fail.
[Primary-cutover gate 1](../../docs/cutover-gates.md) requires FTS **and** hybrid
Recall@5 at least 0.95, MRR at least 0.90 and nDCG@5 at least 0.90: all six
comparisons fail. Its per-language Recall@5 floor of 0.90 also fails for both
FTS cohorts and hybrid English (0.8889 is not rounded up to a pass). The
separate live-workspace/pilot requirements are unmeasured, not passed.

### Exact identifiers

The [real-Qwen exact-identifier report](results/production-qwen-2026-10-18-exact-ranking.json)
passes the unchanged `exact-identifier-gate.py` floors:

| Path | Correct at 1 | Correct at 3 | English / Swedish at 3 | Gate |
| --- | ---: | ---: | ---: | --- |
| FTS | 8/8 | 8/8 | 4/4 / 4/4 | PASS |
| Hybrid | 7/8 | 8/8 | 4/4 / 4/4 | PASS |

The broad personal corpus's six exact-name queries also have their grade-3
source within top 3 for FTS and hybrid. This does not imply perfect top-1
precision: its hybrid exact-name MRR is 0.7222 versus FTS 1.0.

### Weak-case investigation

The [production score diagnostics](results/production-qwen-2026-10-18-weak-case-scores.json)
rerun selected failing queries against the same pinned indexes. These are
actual backend scores and fusion path provenance, not alternative ranks:

- `semantic-watering-en`: vector ranks the travel list first and the watering
  chunk fourth. FTS also ranks travel first and misses both relevant chunks
  at 5; hybrid has no relevant chunk at 5 and its first relevant hit is ninth.
  Lexical support reinforces irrelevant English results for an English query
  whose intended source is Swedish. This is a measured cross-language weakness.
- `exact-reading-en`: FTS returns the correct reading queue first, while vector
  places it fifth behind the archived Lighthouse record and other pages. Equal
  RRF weights promote the archived record to hybrid rank 1 (0.03227) over the
  queue (0.03178). Metadata-prefix baseline scores cannot diagnose this as an
  ANN-only regression because this production path embeds raw text.
- `name-en` in the identifier corpus: FTS ranks the exact name first and the
  near-name second; vector reverses them. Their fused scores tie exactly at
  `0.032522473484277725`. The production canonical-ID tie-break puts the
  near-name first. The deterministic fixture mapping is recorded; the adapter
  does not replace it with judgment-aware or evaluation-ID ordering.
- `status-active-en`: a completed review leads hybrid, and the archived
  project record remains a high vector candidate. Business status and dates
  are preserved canonical metadata, but a natural-language question does not
  automatically become an explicit status/date filter. Fictional `status:
  archived` is a business field, not a physically deleted Notion page.
- `link-study-sv`: FTS retrieves only the referring study routine, which cannot
  answer the linked-source question. Hybrid retrieves the reading queue third.
  More generally, cross-link hybrid MRR is 0.5972; graph traversal is not a
  substitute for relevance or source authorization.

These results identify model/preprocessing, lexical cross-language coverage and
fusion/tie behavior requiring reviewed quality remediation. This measurement
does not tune queries, change judgments, alter RRF weights, lower floors or
change the production model. #12/#1 and cutover stay open for the unmet gates.
The native quality-remediation follow-up is
[#461](https://github.com/mtandersson/notion-knowedge/issues/461), blocked on
this measurement delivery; this PR does not implement its ranking changes.

### Real graph comparison

The [measured graph report](results/production-qwen-2026-10-18-graph.json) uses
the original six linked and six negative-control cases, one hop, weight 0.15
and candidate limit 20. Both runs acknowledge identical complete candidate
sets, original baseline scores/paths and the same immutable index signature.
All graph edges are the fixture's actual committed links.

The decision is **`keep_disabled`**. Two target ranks improve and linked MRR
increases from 0.5972 to 0.6071, but linked Recall@5 drops from 1.0 to 0.8333
and nDCG@5 drops from 0.6278 to 0.5875. Excluded guards move upward for both
`date-review` translations; unjudged content newly enters top 3 in
`link-garden-en`, `semantic-energy-en` and both `date-review` cases. No new
excluded guard enters top 3, and negative-control aggregate metrics remain
unchanged, but those facts do not erase the established safety regressions.
Production graph boost remains OFF.

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
