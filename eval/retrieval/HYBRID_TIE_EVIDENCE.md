# Hybrid tie evidence (#467)

The production RRF ranker now resolves exact fused-score ties using contributing
lexical rank before canonical source IDs. It preserves numeric fusion scoring,
weights, candidate depth, source checks and index/model identities. Raw scores
from different paths are still never compared. Zero weights remain invalid.
This is a deterministic secondary ordering rule, not exact-match promotion:
a strictly stronger fused score wins even when lexical ordering disagrees.

## Measured complete corpora

The real pinned Qwen production adapter was rebuilt from ranking commit
`66813ed`, using the unchanged #452 corpora, judgments, CPU provider and tiny
corpus IVF configuration. Each corpus used a new guarded committed snapshot;
vector, FTS and hybrid reused that snapshot, then repeated the complete report.
The [provenance](results/production-qwen-2026-10-20-tie-provenance.json)
records binary, lockfile, corpus and asset digests, canonical mappings and
immutable snapshots. Production embeddings still use raw chunk text; no new
embedding/index compatibility or operational-state migration is needed.

| Personal corpus path | Recall@5 | MRR | nDCG@5 |
| --- | ---: | ---: | ---: |
| Vector | 0.93056 | 0.69583 | 0.70025 |
| FTS | 0.73611 | 0.70602 | 0.62968 |
| Hybrid | 0.93056 | 0.79282 | 0.76145 |

Vector and FTS metrics equal the #452 baseline. Hybrid nDCG@5 changes from
0.7604743243 to 0.7614487576; its Recall@5 and MRR stay unchanged.
On the separate eight-query exact-identifier corpus, FTS remains 8/8 correct
at rank 1 and top 3. Hybrid improves from 7/8 to 8/8 at rank 1, with 8/8 at
3 retained. The previously tied `name-en` case now follows the stronger lexical
rank instead of incidental canonical ID ordering. No query or corpus identity
is inspected by the ranker.

Complete ranked IDs, grades and per-query metrics are in the
[personal report](results/production-qwen-2026-10-20-tie-personal-ranking.json)
and [exact report](results/production-qwen-2026-10-20-tie-exact-ranking.json).
All language/category breakdowns are in the
[cohort report](results/production-qwen-2026-10-20-tie-cohorts.json).
The [original policy verdicts](results/production-qwen-2026-10-20-tie-policies.json)
remain **FAIL**: the hybrid smoke requires MRR and nDCG@5 at least 0.80, and
primary cutover requires both FTS and hybrid Recall@5/MRR/nDCG@5 at least
0.95/0.90/0.90, plus the existing cohort floors. This small improvement does
not meet those requirements. #461, #12 and #1 remain open. Lexical candidate
coverage is tracked separately in #468. Graph boost remains OFF.

## Reproduction

Follow [the production evaluation protocol](PRODUCTION_RETRIEVAL.md#reproduce)
with the ranking commit above and new corpus state directories. Use all three
modes and top-k 10 for both complete corpora. Repeat against the same respective
state directories and compare reports byte-for-byte; the provenance artifact
records those comparisons. Run `scripts/exact-identifier-gate.py` on the exact
report with its unchanged default floors. No private Notion data, hosted
provider, real Notion write or live ChatGPT evidence is involved.
