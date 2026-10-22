# Complete lexical candidate coverage (#471)

The unchanged personal corpus has eight of 24 queries whose relevant sources
are absent from every current production lexical candidate. Even an ideal
reranker of those candidates can reach only **0.77778 macro Recall@5**, below
the original primary-cutover 0.95 floor. Increasing the output limit or changing
title/body ordering cannot recover absent candidates. This is a measured
coverage limitation, not a production ranking improvement.

The configured FTS pipeline uses Swedish stemming and Swedish stop-word removal
on title/body columns, plus raw exact identifier indexes. English question words
can therefore contribute lexical matches. Query language is not a translation
step. Canonical status/link metadata is retained but natural-language questions
do not automatically become status filters or authorized link traversal.

## Actual complete-candidate evidence

The existing `production-eval` adapter retrieved FTS at top-k **100**, exceeding
the entire personal corpus (14 chunks) and exact corpus (8 chunks). Production
lexical per-field candidate limits therefore exceed the number of indexed
rows; no source can be hidden by top-k truncation in these corpora. The adapter
reused the guarded immutable snapshots from #467, with unchanged pinned Qwen,
fixture authority and root filtering. It verifies index/state fingerprints,
provenance, source expansion and denied-root behavior on every invocation.
Judgments were stripped from adapter requests and used only afterward.

The [personal candidate report](results/lexical-coverage-2026-10-20-personal-candidates.json)
and [exact report](results/lexical-coverage-2026-10-20-exact-candidates.json)
contain every actual ranked FTS candidate, including zero-hit queries. The
[personal analysis](results/lexical-coverage-2026-10-20-personal-analysis.json)
records every missing judged source and each query's oracle Recall@5 ceiling.
This ceiling assumes ideal ordering of available relevant candidates; it is
**not** an achieved ranking or a synthetic replacement report.

| Cohort | Queries | Queries missing relevant candidates | Oracle Recall@5 ceiling |
| --- | ---: | ---: | ---: |
| All personal | 24 | 8 | 0.77778 |
| English | 12 | 3 | 0.84722 |
| Swedish | 12 | 5 | 0.70833 |
| Semantic | 6 | 5 | 0.41667 |
| Exact name | 6 | 0 | 1.00000 |
| Date/status | 6 | 2 | 0.86111 |
| Cross-link | 6 | 1 | 0.83333 |
| Exact-identifier corpus | 8 | 0 | 1.00000 |

Missing-candidate cases are both memory queries, both watering queries, both
active-status queries, `semantic-energy-sv` and `link-study-sv`. In particular,
the Swedish energy query has no lexical candidates at all; an English watering
question has no relevant Swedish watering/review source in its candidate set.
The complete exact-identifier corpus retains all intended candidates and 8/8
FTS top-1/top-3 coverage. Language/category means include every original query.

[Provenance](results/lexical-coverage-2026-10-20-provenance.json) pins corpus,
assets, binary, source, lockfile, complete manifest/mapping and immutable
snapshots. Both complete candidate reports repeated byte-for-byte against their
same respective snapshots. No source/ranking defaults or judgments changed.

## Limits and remaining work

This measurement does not pass either original quality policy. Actual personal
FTS Recall@5/MRR/nDCG@5 remain 0.73611/0.70602/0.62968, below primary-cutover
0.95/0.90/0.90. Current complete candidate coverage itself also fails the
per-language/category Recall@5 floors of 0.90. Hybrid smoke and primary policy
verdicts remain FAIL as recorded in #467's full reports; candidate analysis
cannot establish hybrid quality, live ChatGPT or private-workspace acceptance.
Graph boost remains OFF. #468, #461, #12 and #1 remain open.

#472 separately requires independent review of a generic versioned multilingual
expansion design, including linguistic variation, translation/paraphrase,
linked-source questions, metadata intent, bounds and provenance. #473 then
implements and measures the reviewed design, including safe rebuild/recovery
that preserves operational grants and ledgers. No fixture-derived synonym list
or semantic results relabeled as FTS can satisfy those contracts.

## Reproduce

Use the build/assets/snapshot protocol in [production evaluation](PRODUCTION_RETRIEVAL.md#reproduce).
For each unchanged corpus, run the existing harness with `--modes fts --top-k
100` and repeat against the same snapshot. Then analyze the reports:

```sh
python3 scripts/lexical-coverage.py \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --report /absolute/path/personal-candidates.json \
  --output /absolute/path/personal-coverage.json
python3 scripts/test-lexical-coverage.py
```

The analyzer validates report identity, exact query coverage, source IDs,
provenance labels, grades and independently recomputed per-query metrics before
calculating missing candidates. It labels reports whose top-k is smaller than
the corpus as `bounded_observation_not_a_ceiling`. That observation does not
prove unreturned sources are absent. Report JSON alone does not attest how an
adapter ran; retain the pinned adapter/snapshot provenance with decision evidence.

The proposed [versioned lexical expansion contract](../../docs/design/multilingual-lexical-expansion.md)
addresses #472. It requires independent review before #473 implementation or
new decision measurements and does not establish quality acceptance.
