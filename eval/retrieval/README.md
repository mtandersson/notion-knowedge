# Personal-knowledge retrieval dataset

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
