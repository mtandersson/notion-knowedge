# Hosted Notion baseline

`hosted-notion-2026-10-01.json` records the baseline availability assessment for
all 24 queries in `personal-knowledge-v1.json`, with unchanged text and IDs.
It contains **no measured rankings or retrieval scores**. Every query is
unavailable, not a failed search or a measured empty result.

## Observation and limitations

At 2026-10-01T21:40:13Z the callable Notion connector exposed
`mcp__codex_apps__notion_search`. Its description requires `get_tool_access({})`
before any content search, then selects `ai_search` when access is available
and keyword `search` only when discovery reports AI search unavailable. The
callable inventory exposed neither `get_tool_access` nor `ai_search`. There was
no discovery result in context. Missing discovery is not a denial: no content
search was called, and plan and AI search access remain unknown.

The connector does not expose a deployed tool/backend version. The artifact
records a null version with this explanation rather than borrowing a package or
Notion REST API version. This is an environment observation, not a claim that
Notion search itself is unavailable or plan-gated. Recheck access on a later run.

The dataset is fictional, with no verified mapping to hosted Notion pages.
Corpus presence was not checked. Queries against an unrelated workspace cannot
measure fixture relevance, even if they return zero hits or similarly titled
pages. No private result text, identifiers, URLs or credentials are recorded.

## Reproduce or collect a measured revision

1. Read the dataset revision and enumerate its query IDs/text without dropping
   difficult cases. Inspect the current callable search tool instructions and
   perform required access discovery. Record time, selected surface, exposed
   version (or explicitly unknown), plan/access evidence and scope. An explicit
   plan denial is `plan_gated`; missing tooling is `unavailable`. Never infer a
   denial from an absent tool or documentation alone.
2. Before searching, obtain authorization to use an isolated hosted fixture
   corpus if none exists. Verify all 12 pages, their text/chunks, titles,
   statuses and links against the dataset. Keep the private hosted UUID-to-fixture
   mapping outside Git. Confirm search can discover this corpus after indexing;
   do not score queries while indexing or visibility remains unverified. Record
   any unavoidable metadata differences, including Notion edit timestamps versus
   the dataset's fixed dates. Material differences make affected cases
   `noncomparable`; preserve their query rows and explain why.
3. Execute each query once with unchanged text on the selected surface, default
   relevance ordering and a fixed top-10 cutoff. For a keyword-only surface,
   preserve the original text and record the actual shorter query plus an
   explanation of equivalent intent; do not silently shorten or tune queries
   after seeing results. Record all parameters, errors, truncation and scope.
   Use separate revisions for UI workspace search, AI search and API title
   search; they are distinct surfaces. Record ordered fixture page IDs only.
   Unmapped/private results must not be exported or silently removed to improve
   ranks: use anonymous nonrelevant placeholders only in an authorized mixed
   corpus study, and document that separate protocol.
4. Measure only executed, verified comparable rows. A verified empty search is
   an empty ranking; an unavailable/noncomparable row has null ranking and
   metrics, never an empty array or zero score. Report coverage alongside scores.
   Page-level judgments deduplicate fixture chunk judgments by containing page
   using maximum grade, as in the dataset README. A hosted page ranking cannot
   be compared directly to chunk ranks. Fix metric definitions and gain
   convention in the comparison harness before calculating recall/MRR/nDCG;
   this record does not implement that harness.
5. Save a new dated artifact instead of replacing this observation. Retain
   unavailable and plan-gated cases with observed reasons. Review ground truth,
   corpus equivalence, privacy and evidence independently before interpreting
   differences as retrieval quality.

## Official surface context

Checked on 2026-10-01: Notion's [workspace search documentation](https://www.notion.com/help/search)
describes content search, default best-match ordering and an AI search option
when the workspace has Notion AI. Its [REST search endpoint](https://developers.notion.com/reference/post-search)
searches titles of shared pages/data sources. The REST endpoint is therefore
not a substitute measurement of hosted content or AI search. Documentation
establishes surface distinctions, not this connection's entitlements.

## Validation and CI

```sh
nix develop --command cargo test -p notion-knowledge-retrieval --test hosted_baseline --locked
```

The offline tests enforce dataset/query alignment, explicit observation context
and absence of fabricated measurements in this unavailable snapshot. Ordinary
CI does not connect to Notion, require a baseline score, or fail because hosted
access is unavailable. These checks validate the record, not hosted quality.
