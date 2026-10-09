# Personal-knowledge retrieval dataset

The [hosted Notion baseline](HOSTED_NOTION.md) records search availability and
the protocol for comparable hosted measurements; unavailable cases are not
retrieval failures or zero scores.

`personal-knowledge-v1.json` is a self-contained, version-controlled evaluation
input: 12 fictional pages, 14 explicit chunks and 24 queries (12 intents paired
in Swedish and English). It models everyday patterns: finding a project by
purpose, remembering reading advice, exact titles, current versus archived
records, dated next actions, and following a project or routine to a linked page.
All content is hand-authored and fictional. It is not sampled from a private
Notion workspace, and does not claim to measure a particular person's workload.

## Format version 1

The top-level `schema_version` identifies the format; `dataset_id` identifies the
content revision. `as_of` fixes the interpretation of current/date questions.
`provenance` describes how the fixture content was authored.

Each page has a stable `id`, `title`, `language` (`sv` or `en`), `status`
(`active`, `complete`, `archived`), ISO calendar `updated_at`, explicit outgoing
page-ID `links`, and `chunks`. Each chunk has a stable `id` and indexable `text`.
Titles, statuses, timestamps and links are indexable metadata. Chunk boundaries
are supplied deliberately; use them unchanged when comparing retrieval modes.
The identifiers use a `fixture:` namespace and are never Notion UUIDs.

Each query has a stable `id`, `language`, `category` (`semantic`, `exact_name`,
`date_status`, `cross_link`), input `text`, an `intent` explaining the retrieval
need, `relevant` judgments and `excluded_source_ids` naming plausible negatives.
Each judgment records a chunk `source_id`, an integer `grade` and a `rationale`:

- 3: directly answers the request or is a necessary requested source.
- 2: useful supporting context, less direct than the primary source.
- 1: related background, useful but insufficient to answer the request.

Unlisted chunks and explicit exclusions have grade 0. Positive judgments are
unordered; order in the JSON does not establish the expected ranking. For binary
recall/MRR, grades 1–3 count as relevant. For graded nDCG use the recorded grades;
the metrics implementation must document its gain convention. There is no
retrieval harness or metric calculation in this dataset change.

When a query asks to identify a page by title or project status, every chunk on
that page receives grade 3 because the indexable page metadata satisfies that
request. Questions asking for particular content judge chunks by their text.

Chunk IDs are the evaluation unit. The containing page provides the expected
page ID without a separate duplicated judgment. A future harness should load all
chunks and page metadata into the same fixed corpus for every retrieval mode,
return these chunk IDs, and retain query IDs in machine-readable results.
For page-level evaluation, deduplicate judgments by containing page and use the
maximum chunk grade; do not silently compare page IDs with chunk judgments.

Cross-link questions require an actual fixture edge and a relevant target chunk;
the referring page alone often cannot answer the question. Similar titles and
archived records provide false-positive cases. Paired translations intentionally
share judgments so language comparisons use the same information need. Both
cross-language retrieval directions are included. Fixed dates and fictional
statuses avoid dependence on today's date or a live Notion workspace.

## Run the offline retrieval harness (#95)

The harness takes this **fixed fictional dataset** and calls a configured
subprocess adapter separately for `vector`, `fts`, and `hybrid`. The
adapter must index/search the **same corpus, model revision and configuration**
for every mode; the harness does **not** implement any backend, generate fake
semantic embeddings, or substitute a synthetic ranker for production retrieval.

From the repository root, after implementing an adapter bridge to the chosen
backend:

```sh
python3 scripts/retrieval-eval.py \
  --adapter "./path/to/your-retrieval-adapter" \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector,fts,hybrid --top-k 10 \
  --output /tmp/notion-retrieval-eval.json
python3 scripts/test-retrieval-eval.py
```

The adapter command is executed **without a shell**, once per requested mode.
It receives one JSON object on stdin containing:

```json
{
  "schema_version": 1,
  "dataset_id": "personal-knowledge-fixtures-v1",
  "as_of": "2026-03-02",
  "mode": "vector",
  "top_k": 10,
  "pages": ["full page records from the dataset"],
  "queries": [{"id": "semantic-energy-en", "text": "...",
               "language": "en", "category": "semantic"}]
}
```

It returns **only** a JSON object on stdout with the same schema version,
dataset identity and requested mode, plus an entry for **every query**, even
when no matches exist:

```json
{
  "schema_version": 1,
  "dataset_id": "personal-knowledge-fixtures-v1",
  "mode": "vector",
  "results": [
    {
      "query_id": "semantic-energy-en",
      "ranked_source_ids": [
        "fixture:chunk:project-lighthouse:overview"
      ]
    }
  ]
}
```

The example is illustrative: actual adapter output **must** be produced by
the selected retrieval implementation, not derived from relevance judgments.
IDs are **chunk IDs** in decreasing score order; ties must be deterministically
ordered by the backend. The adapter must map its `semantic` search to
`vector`, lexical/BM25 to `fts`, and configured fusion to
`hybrid`. It must return no more than `top_k` items per query,
include each query exactly once and emit no unknown or duplicate chunk IDs.
The harness checks these invariants; an unsupported mode or missing index is
an error rather than a fabricated zero-score result. Ensure the full corpus
is indexed before answering any query. Do not insert or delete documents
between mode runs.

Output JSON records, in stable dataset order, the ranked IDs and matching
graded judgments for each query and mode; its human-readable stderr summary
counts queries with any relevant hit and grade-3 hit at K, plus the ranking
metrics below. The report contains no source document text or query wording. Runs from a fixed dataset/adapter/model should produce
byte-identical JSON; a nondeterministic adapter fails this comparison and must
be investigated rather than sorted/re-ranked by the harness.

Nonzero adapter status, timeout, invalid UTF-8/JSON, mismatched mode or
dataset, omitted/duplicate query, unknown chunk, repeated rank or excess
rank count fail with exit code 2. Exit code 0 means *the evaluation ran*,
not that quality is acceptable. Adapter stdout/stderr on failure are
intentionally not copied into logs because these may contain credentials or
private data. Run only trusted local adapters with approved fixture data;
the harness does not authorize or sanitize arbitrary external adapter code.
The credential-free tests use a deterministic **contract stub**, not a real
embedding model or full LanceDB evaluation. A complete model/index-specific
report requires an actual adapter command; no production score is claimed.

## Ranking metrics (#96)

Run the exact-valued, credential-free metric tests with:

```sh
python3 scripts/test-retrieval-metrics.py
python3 scripts/test-retrieval-eval.py
```

Every JSON `results[]` row now includes a `metrics` object and every
`summary.<mode>.metrics` holds the **equal-weight macro mean across all
queries**, including no-hit queries. The human-readable stderr summary includes
those mean values. No document contents are added to the JSON output.
Metric keys are `recall_at_1`, `recall_at_3`, `recall_at_5`,
`recall_at_10`, `ndcg_at_1`, `ndcg_at_3`, `ndcg_at_5`,
`ndcg_at_10`, and `mrr`.

- **Recall@K** = number of unique judged-relevant **chunk IDs** in the first
  K results divided by all graded-relevant chunk IDs in the query's complete
  ground truth. Any grade 1–3 is binary relevant; unjudged chunks count as 0.
  A partially relevant answer counts as relevant for recall.
- **MRR** = reciprocal rank of the first grade 1–3 chunk within the requested
  `top_k` (0 if none). The summary reports mean reciprocal rank across all
  queries. Unlike nDCG, MRR does not distinguish grade 1 from grade 3.
- **nDCG@K** uses graded exponential gain `2^grade - 1` and the
  `log2(rank + 1)` positional discount, with rank starting at 1.
  Divide observed DCG@K by the ideal DCG@K of **all** judged relevant chunks
  ordered by descending grade. The metric is 0 for a zero-gain ranking;
  ideal gain is never zero on this dataset's nonempty judgments.

All metrics evaluate **chunks**, not pages. A result whose page matches but
whose chunk is not judged gets grade 0; page-level evaluation would need a
separately documented deduplication and ground truth conversion.
Only cutoffs **at or below `--top-k`** are emitted. For example `--top-k 3`
reports Recall@1/3 and nDCG@1/3, not a misleading Recall@5/10 from a truncated
response. Set `--top-k 10` or higher to report all four advertised cutoffs.
A short result list is treated as exhausted, not padded with synthetic hits.
No-hit queries contribute zero to macro averages rather than being excluded.

These metrics are deterministic calculations on the adapter's real ranked IDs,
**not evidence of production search quality**. Current contract tests exercise
a synthetic mode-aware stub and known hand-calculated metric values. The
harness still needs a trusted real Qwen/LanceDB adapter and fixed
model/index for meaningful quality results.

## Stale-index and authoritative read evaluation (#101)

The versioned [freshness scenario matrix](freshness-v1.json) contains **seven
synthetic cases** for an indexed page last edited at
`2026-10-07T11:00:00Z`. It intentionally supplies a second, newer
authoritative timestamp without modifying the index. No actual Notion API
credentials or live documents are used.

Run the cases against the **real MCP `knowledge_get` handler**, a
mock `SourceExpansion` index and a read-only `NotionRead` backend:

```sh
nix develop --command cargo test -p notion-knowledge-mcp \
  --test get_adapter versioned_freshness_scenarios --locked
```

The cases assert this lifecycle: `indexed` and omitted freshness
preserve the old indexed text and **do not** imply that a source comparison has
occurred; `fresh` with unchanged source returns authoritative whole-page text,
both timestamps and `index_stale: false`; `fresh` after an edit
returns newer source content with `index_stale: true` and preserves
the old indexed timestamp. An unavailable source, archived page or concurrent
edit returns a structured failure with no partial or stale fallback. The test
reads the index **again after every scenario** and checks it is byte-for-byte
identical; no fresh result is silently written back or advertised as an
automatic reindex. It also asserts the read-only Notion call counts and that
plain indexed reads never invoke the authoritative backend.

The scenario file is separate from `personal-knowledge-v1.json` because
the ranking fixture's relevance grades are not a notion of wall-clock
freshness. This is a repeatable **behavioral regression**, not a latency/SLA
measurement or evidence that production webhook synchronization meets its
freshness target. Actual freshness after writes requires the production
reconciliation/index writer and its separate integration tests.

## Validate and maintain

Run the integrity and coverage checks with the repository toolchain:

```sh
nix develop --command cargo test -p notion-knowledge-retrieval --test evaluation_dataset --locked
```

These tests run in the normal workspace CI. They check unique/resolvable source
IDs, links, nonconflicting grades and distractors, both languages in every
category, translation parity and representative link/status ground truth. They
validate dataset integrity, not retrieval accuracy. A reviewer must still check
that judgment rationales are supported by the fixture text.

Keep IDs stable across wording corrections. When changing meaning, corpus,
chunk boundaries or ground truth, create a new content revision and update
`dataset_id`; bump `schema_version` only for incompatible format changes.
Review new cases against their source content and include realistic negatives.
Do not paste private notes, credentials, personal identifiers or live workspace
URLs into fixtures. New real-world patterns should be rewritten into fictional
content and documented in provenance before being committed. The small balanced
set is a baseline for regression comparisons, not evidence of general retrieval
quality; broaden domains and independently review judgments before using scores
for deployment decisions.

## Exact identifier and lexical regression gate (#100)

[`exact-identifiers-v1.json`](exact-identifiers-v1.json) is a **separate**
versioned, entirely fictional ten-page / ten-query near-neighbor corpus.
It exercises raw complete page/chunk IDs (including colons and hyphens),
mixed letters and digits, Swedish titles and names, accents, an English
query over a Swedish title, case variants and similar adjacent IDs. Every
query identifies **one direct-answer chunk** (grade 3) and an explicit
plausible distractor, avoiding a convenient positive-only fixture.

A credential-free integration test drives the **real** embedded LanceDB
FTS indices, the real vector index and the production hybrid RRF service.
It reports (on failure) which query IDs lost their direct answer. The
lexical and hybrid modes each must reach **at least 8/10 direct answers
in the top three**, and full stable page/chunk ID lookups must return the
exact target **at rank one in FTS**. A query with no hits counts as a
miss; the two modes never average away one another's failures.

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --test exact_identifier_regression --locked
```

The test intentionally uses a deterministic, **nonsemantic** embedding
fixture and a lexical-favoring RRF configuration (semantic weight 0.1,
lexical weight 1.0). It checks that BM25 evidence is not lost when real
vector and fusion paths run; it is **not** evidence of real Qwen relevance
or default-production hybrid scores. The dataset can also be passed to
`scripts/retrieval-eval.py` with a trusted real adapter to compare FTS
and hybrid scores, using the same chunk-level judgments. The regression
test is the automatic executable gate, not a substitute for a full
production-adapter evaluation.

### Tokenization and case expectations

The two raw stable-ID indices store the **entire** page/chunk ID as one
token and do not lowercase, stem, strip stopwords or fold accents.
Complete identifier probes therefore use the exact ID bytes (including
case); they are not prefix or fuzzy lookups. Title and text FTS use the
Swedish-first simple tokenizer, with stemming, Swedish stopword removal
and no ASCII folding. The dataset has a lowercase `zx9q42` probe against
uppercase document text and contrasts `Bergström` with `Bergstrom`:
these are explicit regression cases for the configured backend, not a
promise of generic case-insensitive identifier lookup or accent folding.
Punctuation in arbitrary body-text codes is backend-tokenizer-dependent;
the strict rank-one check is intentionally only for the raw stable IDs.
See [FTS index](../../docs/fts-index.md) and
[lexical search](../../docs/lexical-search.md).

Changing chunk text, ground truth or tokenizer expectations requires a
new `dataset_id` content revision and review of negative judgments.
