# Complete installed lexical measurement — 2026-10-29

**Historical result: the original personal-corpus numeric targets FAIL.**

**Current launch policy, revised by the owner on 2026-10-30:** numeric ranking
floors and long quality pilots are post-launch improvement goals. These score
shortfalls no longer block production. The historical measurements and failed
verdicts below remain unchanged; they are not retroactively declared passes.
See the [current launch policy](../../../../docs/cutover-gates.md).

A real authorized functional smoke, correct attribution, exact lookups,
OAuth/scope enforcement, safe writes and working sync/cache recovery remain
required. This offline run does not prove those live checks. Graph and bilingual
expansion remain optional and default OFF. Further ranking/language mechanisms
are post-launch backlog work driven by production feedback.

## Execution and retained evidence

[Hosted measurement run 40954599938](https://github.com/mtandersson/notion-knowedge/actions/runs/40954599938)
built and ran the actual release Qwen/production adapter at source
`65dd30c52e7486902c39632dad8c650b49f7f27e`. The pinned public dictionary was
prepared with that same build and compiler. Qwen assets were verified by the
production provider. The vector results match the retained production baseline;
no model, corpus, query, judgment, metric definition or gate was changed.

Both unchanged complete fictional corpora ran through vector, FTS and hybrid at
top-k 10. Each complete run repeated against its original persisted snapshot.
Ranking reports and all three native adapter responses were byte-identical on
repeat, including scores, match evidence and index identities. The FTS candidate
diagnostic separately requested top-k 100 over all 14/8 corpus chunks.

- Full ranks, including zero-hit cases: [personal](personal-ranking-1.json) and [exact](exact-ranking-1.json), with corresponding `-2.json` repeats.
- Raw shipping-adapter evidence: `personal-adapter-1/`, `personal-adapter-2/`, `exact-adapter-1/`, `exact-adapter-2/`; each includes input/source snapshots, manifest, pinned snapshot and vector/FTS/hybrid responses.
- [Provenance](provenance.json): executable/source/lock/compiler/model-asset/dictionary identities and repeat digests. Native adapter responses additionally retain complete asset/storage/plan bindings and bounded candidate/top-k descriptors.
- `personal-state/` and `exact-state/`: manifest, pinned snapshot and timing observations. Binary index/state files are retained in the hosted artifact, rather than checked into source.
- [Original corpus snapshot](personal-knowledge-v1.json) and [exact-identifier snapshot](exact-identifiers-v1.json), unchanged byte-for-byte.
- [Both original policy verdicts](quality-policies.json), [exact regression](exact-regression-gate.json), complete cohorts, candidate lists and baseline comparisons. [Run state](run-state.json) distinguishes complete collection from passing quality.

The hosted job fixes Rayon and OpenMP thread counts at 2 and disables tokenizer
parallelism. These are hosted offline measurements, not deployment capacity evidence.

## Complete quality results

| Corpus | Path | Recall@5 | MRR | nDCG@5 |
| --- | --- | ---: | ---: | ---: |
| personal | vector | 0.930556 | 0.695833 | 0.700250 |
| personal | fts | 0.805556 | 0.684722 | 0.675852 |
| personal | hybrid | 0.923611 | 0.740278 | 0.744948 |
| exact | vector | 1.000000 | 0.875000 | 0.907732 |
| exact | fts | 1.000000 | 1.000000 | 1.000000 |
| exact | hybrid | 1.000000 | 1.000000 | 1.000000 |

On the personal corpus, FTS Recall@5 improves from 0.736111 to 0.805556;
FTS MRR decreases from 0.7060 to 0.684722. Hybrid Recall@5 decreases from
0.930556 to 0.923611, MRR from 0.792824 to 0.740278, and nDCG@5 from
0.760474 to 0.744948. Preserve the [failing historical baseline](../production-qwen-2026-10-18-personal-ranking.json);
candidate coverage improvement is not ranking acceptance.

| Personal language | Path | Recall@5 | MRR | nDCG@5 |
| --- | --- | ---: | ---: | ---: |
| en | fts | 0.805556 | 0.695833 | 0.662974 |
| sv | fts | 0.805556 | 0.673611 | 0.688730 |
| en | hybrid | 0.888889 | 0.779167 | 0.737241 |
| sv | hybrid | 0.958333 | 0.701389 | 0.752655 |

| Personal category | FTS Recall@5 | FTS MRR | Hybrid Recall@5 | Hybrid MRR |
| --- | ---: | ---: | ---: | ---: |
| cross_link | 0.666667 | 0.391667 | 1.000000 | 0.472222 |
| date_status | 0.888889 | 0.763889 | 0.861111 | 1.000000 |
| exact_name | 1.000000 | 1.000000 | 1.000000 | 0.722222 |
| semantic | 0.666667 | 0.583333 | 0.833333 | 0.766667 |

Full vector/FTS/hybrid language/category Recall/MRR/nDCG are in
[personal cohorts](personal-cohorts.json) and [exact cohorts](exact-cohorts.json).

## Historical proposed policies and remaining limitations

G1 requires personal-corpus hybrid Recall@5, MRR and nDCG@5 each at least 0.80,
plus critical exact-name hits within top 3. Recall and exact-name checks pass;
MRR and nDCG fail. Cutover requires FTS AND hybrid Recall@5 >= 0.95, MRR and
nDCG@5 >= 0.90, and language/category Recall@5 >= 0.90: the personal corpus
fails these floors. Scores are compared without rounding upward. Live pilot
and other cutover requirements remain unmeasured.

The unchanged exact regression passes: FTS and hybrid each return the correct
source at top 1 and top 3 for 8/8 cases, with 4/4 in each language. Its existing
75% top-1/top-3, language and family checks pass, and there are zero critical
exact-ID misses at top 3. This separate success does not erase the broad failures.

## Complete candidate diagnostic

Personal-corpus candidate availability improves from eight queries missing
relevant candidates to six. Its observed oracle Recall@5 ceiling improves from
0.777778 to 0.854167; exact identifiers remain 1.0. Retained original #471
analyses and hashes are in the `*-baseline-coverage.json` and
`*-coverage-comparison.json` files. All ranked candidates and missing judged IDs
are retained in `*-candidates.json` and `*-coverage.json`.

This covers the complete tiny corpus under the **bounded installed plan**.
It does not establish complete dictionary/language coverage. The unchanged
planner/native resource and candidate limits remain explicit in each adapter
response; no partial native failure or semantic-to-FTS substitute is accepted.

The missing-candidate cases span semantic paraphrases, active-status questions
and a linked-reading-list question. Missing lexical relations, ambiguity,
morphology or query semantics cannot be diagnosed from wording alone. Actual
ordering also regresses despite improved availability, so a candidate-only
correction is insufficient to establish the original gates.

Post-launch follow-up contracts are natively parented by #473. They are not
launch blockers and must not start merely because #493 merges. Production
feedback and an owner decision should justify further work; independent
contract review is still required before implementation:
- [#602](https://github.com/mtandersson/notion-knowedge/issues/602): generic diagnosis/correction contract for expansion-induced hybrid ordering regressions.
- [#603](https://github.com/mtandersson/notion-knowedge/issues/603): bounded evidence-led diagnosis and generic correction of residual bilingual candidate omissions.
- [#604](https://github.com/mtandersson/notion-knowedge/issues/604): generic support/correction contracts for metadata and linked-source question classes.

No follow-up may consume runtime judgments, change the corpora/gates, add
fixture answer maps, enable graph to hide failures, or revive retained cache
generations/process handoff. Split implementation only after reviewed contracts.

## Reproduce

Use the [hosted workflow](../../../../.github/workflows/lexical-quality.yml) and
[measurement collector](../../../../scripts/measure-lexical-quality.py). The workflow
builds the pinned release examples once, provisions public assets before
offline search, runs both complete corpora twice, preserves full native
responses and compares candidate coverage to #471. No credentials are needed.
The workflow is now manual-only. Its successful exit establishes completed
collection; the policy JSON records failure against the historical proposed
targets, not failure of the revised launch policy. Repeating requires fresh output directories
and matching compiler/source assets; no existing cache is relabeled.

The retained measurement predates the owner-approved policy relaxation. A
subsequent historical-policy helper correction checks exact-name top-3 for both
FTS and hybrid, matching the old written rule; both modes pass that check here,
so the retained verdicts are unchanged. No ranking binary or corpus changed.

The published provenance renders model assets as explicit file/digest entries
to avoid a generic API-key false positive on the public tokenizer checksum.
Digest values equal the raw hosted artifact and the provider's pinned assets.
`original_artifact_provenance_sha256` retains the original provenance-byte
identity. Other captured evidence, including repeated native responses, is
byte-preserved. A single reviewed historical scanner fingerprint covers the
old occurrence; no path or scanner rule is disabled.
