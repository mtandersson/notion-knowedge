# One selected Notion page to local vectors

Experimental follow-up to the [local vector spike](local-vector-spike.md).
`create-notion` consumes the existing authoritative Notion reader, canonical
chunker and fingerprints. It completes fresh root discovery with exclusions,
then reads content for exactly the selected allowed page. Links and other
allowed pages do not expand the embedded corpus. Unreadable, incomplete or
unsupported content fails before model execution or index creation.

Use one explicitly selected disposable non-sensitive page and a root containing
it. Selecting the page itself as root bounds discovery to its physical subtree;
only that page's own content is extracted. Excluded page IDs are optional,
repeatable final arguments. Set `NK_NOTION_AUTH=integration` and `NOTION_TOKEN`
through a local secret environment. The server does not load `.env` files.
Never supply tokens in command arguments or commit source output.

In the matched pinned `nix develop .#spike` environment:

```sh
cargo run --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -- \
  create-notion /absolute/model-assets /absolute/new-index \
  ROOT_PAGE_ID SELECTED_PAGE_ID TRUSTED_WORKSPACE_ID [EXCLUDED_PAGE_ID ...]
cargo run --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -- \
  query-page /absolute/model-assets /absolute/new-index 'One fixed question'
```

Workspace identity is caller-supplied trusted provenance, not independently
verified authorization. Page IDs accept the same normalized IDs/URLs as the
existing adapter. Query output contains source text and actual citation fields:
page/chunk IDs, title, URL, heading path, edit timestamp and content hash. Store
that output only locally in an ignored/private directory. Ordinary CI does not
use credentials or model assets. Maximum 32 chunks bounds this experimental
one-page path; it is not the production ingestion limit.

This reuses the canonical source authorization, read and chunk contracts. The
CPU provider and exact-vector table remain experimental adapters. #37 retains
production schema/migration requirements; #39 retains runtime/provider lifecycle
and resource requirements. This proof does not close either production ticket.

## Evidence

The approved page is its own explicit root. The authenticated integration
identity provides workspace provenance. Fresh discovery and canonical extraction
returned one allowed selected document and eleven chunks. Real Qwen execution
persisted all eleven rows with the pinned 1024-dimensional vector identity.
A separate process reopened that index and queried it successfully.

The predeclared successful question was:
“Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?”
The expected introductory passage naming U.S. Central Command and 1988–1991
ranked first. Independent assertions compared the returned title, authoritative
URL and edit timestamp with authenticated source metadata; checked the exact
selected page ID, stable `nk-chunk-v1` identity, present empty introduction heading
path `[]`, and finite similarity; and recomputed the `nk-content-v1` hash from
returned canonical text. Both returned results belonged to the selected page.
Actual source text, page IDs, URLs, metadata responses and model/index files
remain local; no live source snapshot is committed.

An earlier predeclared promotion-year question,
“Vilket år blev H. Norman Schwarzkopf Jr. fyrstjärnig general?”, **failed** its
expected-passage assertion: the introduction ranked first and the explicit
promotion section was not returned in the two results. A mention of 1988 in the
introduction did not prove the expected promotion passage. This failure is retained
separately from the successful organization question; this tiny experiment proves
execution and one cited retrieval, not retrieval quality or an evaluation gate.
Two questions were used for this step; the whole-spike five-question bound remains.

Measured timings on the local CPU, in the executable's unoptimized development
profile (`debug = 0` removes symbols, not optimization):

| Stage | Observed elapsed time |
| --- | ---: |
| Create: model loading and asset validation | 35.343 s |
| Create: embedding all eleven canonical chunks | 642.846 s |
| Successful query: fresh-process model loading/validation | 35.296 s |
| Successful query: query embedding | 11.647 s |
| Successful query: exact vector scan | 0.009 s |

These instrumented stages exclude Nix/Cargo startup and Notion discovery/read
latency. The create process was observed actively using multiple CPU cores with
roughly 2.5–2.8 GiB resident memory; these sampled observations are not a measured
process peak. The long debug-profile embedding cost is an experimental limitation,
not a production performance recommendation.

The strict HTTP tests prove that a discovered but unselected page is not
extracted, excluded/outside IDs are rejected before content reads, links are not
followed, current response metadata is preserved, and unsupported content fails.
The full Notion suite, selected boundary tests, Notion/spike Clippy, spike type
checking and a fresh audit of both tracked lockfiles passed. The existing `paste`
unmaintained warning remains visible. Live credentials are not used by these tests.

The live API exposed two stale normalization assumptions: its authoritative URL
uses the exact `app.notion.com` hostname, and API version `2026-03-11` omits legacy
`archived`. Readers now accept that exact HTTPS host and combine `in_trash`,
optional page `is_archived`, and legacy `archived`: any true state is inactive,
any supplied nonboolean fails, and an object with none of these fields fails.
Current blocks legitimately provide `in_trash` alone. The
[official SDK migration notes](https://github.com/makenotion/notion-sdk-js)
document the replacement of `archived` by `in_trash`. Strict selected-page and
normalization tests use the current response shape and reject hostname spoofing.
No broader URL host allowance or discovery permission was added.
