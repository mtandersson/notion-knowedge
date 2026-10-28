# Versioned multilingual lexical expansion (#472)

Status: proposed contract for independent review. No runtime feature, decision
measurement, quality acceptance or cutover approval is established by this PR.
Implementation belongs to #473 after this design is independently approved.
Parents #468/#461/#12/#1 remain open. Graph boost stays OFF.

## Evidence and problem boundary

The [complete production candidate diagnostic](../../eval/retrieval/LEXICAL_CANDIDATE_COVERAGE.md)
finds missing relevant candidates in 8/24 unchanged personal queries. Its oracle
Recall@5 ceiling is 0.7777777777777778 overall, 0.8472222222222222 English and
0.7083333333333334 Swedish. This is not achieved ranking. The exact corpus has
no missing candidates. Increasing top-k or reranking existing candidates cannot
repair the measured absence.

The following is a diagnostic inventory, never an input to expansion or ranking.
Each row corresponds to every missing-candidate query in the pinned analysis;
source IDs and judgments remain exclusively in the offline evaluator.

| Missing query | Linguistic/candidate class | What this proposal can and cannot supply |
| --- | --- | --- |
| `semantic-memory-en` | English query, Swedish advice; translation plus paraphrase | Literal dictionary equivalents can bridge languages; no guarantee of remembering/retrieval-practice inference. |
| `semantic-memory-sv` | Swedish query, English reading queue; multiword variation and translation | Recorded multiword equivalents may bridge the terms; arbitrary paraphrase remains unsupported. |
| `semantic-watering-en` | English question, Swedish watering/review text; translation and compounds | Dictionary entries may supply lexical equivalents; no inference from being away to a self-watering recommendation. |
| `semantic-watering-sv` | Swedish inflection/compound and related review phrasing | Existing Swedish stemming remains; missing compound forms or conceptual equivalence are not invented. |
| `semantic-energy-sv` | Swedish purpose question, English project description | Translation can add literal electricity/energy terms when present in the asset; matching purpose to a project is not guaranteed. |
| `status-active-en` | English status/date intent; relevant Swedish page chunks omit the requested property words | Translation alone cannot implement metadata intent or propagate page status into arbitrary text matches. |
| `status-active-sv` | Swedish status/date intent plus English content and missing lexical status words | Same limitation; no undocumented conversion of dates/status into filters or inferred answer sets. |
| `link-study-sv` | Follow a referring page's link to an English reading list | Translation may coincidentally match the target; expansion does not traverse links or count referring-page hits as target hits. |

Existing Swedish-first title/body FTS and exact raw ID indexes are implemented
in `crates/retrieval/src/chunks.rs`. `GuardedChunkTable` and the index commit
coordinator own authorized, consistent snapshots and fenced derived writes.
The new planner must compose within those boundaries, not query a second
unfiltered store or repair indexes from a search call.

## Decision: literal bilingual query expansion

Version 1 adds an explicitly enabled **query-side** English/Swedish dictionary
branch alongside the existing raw FTS request. It does not translate documents,
generate summaries, embed expansion terms, use nearest neighbors, invoke a
language model, or perform graph traversal. Every new candidate must match
original indexed title/body text through native FTS under the same predicate.
It remains lexical retrieval, including when composed into hybrid search.

The asset is the public FreeDict/WikDict English–Swedish **2025.11.23** source
release, not a dictionary assembled from the evaluation questions. Exact
archive, member and license hashes are recorded in
[the proposed asset manifest](multilingual-lexical-assets-v1.json). The archive
and upstream SHA512 sidecar were fetched and compared before this proposal;
no retrieval measurement was collected. The TEI header declares CC BY-SA 3.0
Unported. Preserve the original COPYING, attribution, source URL, edition and
transformation provenance in the separately distributed local data bundle.
No dictionary bytes or downloaded archive are embedded in this documentation PR.

The [official release directory](https://download.freedict.org/dictionaries/eng-swe/2025.11.23/)
and [FreeDict source distribution documentation](https://freedict.org/downloads/#sources)
are the source of the release. Its directional entries become a deterministic
bidirectional *literal relation*: inversion is lookup, not a claim that senses
are equivalent. Do not combine unrelated words merely because they share a
translation, recursively expand translations, select a sense using relevance
judgments, or maintain fixture-specific synonym exceptions.

### Offline asset preparation

The implementation must pin the importer version/source commit and dependency
lockfile, plus Unicode normalization/tokenization versions. This proposal uses
NFC, Unicode lowercase without accent folding, and Unicode word boundaries;
`unicode-normalization` 0.1.25 and `unicode-segmentation` 1.13.3 are already
locked. Pin the compiler's Unicode tables as part of the importer identity too.
Do not assume this normalization is the same as Lance's Swedish stemmer.

Read only the declared regular TEI and COPYING archive members, without extracting
archive paths. Reject duplicate members, links, traversal, oversize members and
unexpected hashes. Do not resolve DTDs, external entities, XInclude or network
resources. The pinned TEI contains an external `freedict-P5.dtd` declaration;
ignore that exact declaration without loading it, and reject internal entity
subsets or other declarations. Parse only bounded literal XML text.

Extract direct entry `form/orth` headwords and Swedish `cit type="trans"`
`quote` values, including nested senses. Definitions, examples, pronunciations,
links and usage text do not become translations. Normalize and deduplicate
pairs, preserving original entry ordinals as public provenance. Keep only
nonempty sequences of 1–4 Unicode words, each at most 64 scalars and containing
letters/combining marks (no query operators or punctuation). Do not split a
quoted translation into unrelated synonyms. Add the reverse edge for accepted
pairs, sort by normalized source/target bytes and emit canonical versioned data.

Version 1 does not add a new lemmatizer, speculative Swedish compound splitter,
fuzzy spelling repair or language detector. Dictionary forms that do not match
normalized query units remain unmatched. Existing backend Swedish stemming
still applies to actual FTS queries. Unsupported morphology is a documented
remaining coverage risk, not a reason to consult evaluation answers.

## Request planning, authorization and ranking

1. Validate the existing query/filter/limit contract and acquire the existing
   guarded source/index snapshot. Validate enabled expansion identity before
   returning any raw, lexical or hybrid result. No search-time asset download.
2. Preserve the original request unchanged for all four existing FTS fields.
   A whole-query quoted phrase or exact page/chunk identifier bypasses expansion
   entirely; preserve literal phrase post-filtering and ID precedence. Do not
   case-normalize or translate stable identifiers.
3. For other queries, normalize only the dictionary-lookup copy. Enumerate
   longest matching 1–4-word units left to right, without overlapping accepted
   units. Lookup is language-agnostic over the two directional relations; do not
   send queries to a hosted detector/translator. A unit with more than four
   distinct targets is ambiguous and contributes no translations in v1.
4. Keep at most eight matched units. Take accepted translations in deterministic
   round-robin unit order, targets sorted by normalized UTF-8 bytes, deduplicate,
   and keep at most eight generated phrases total. Never choose targets from
   source/page IDs, corpus frequency, model scores, judgments or query IDs.
5. Search each generated phrase in title and body only. Use a typed literal
   term/phrase query, or an audited literal serializer with parser round-trip
   tests; never interpolate raw dictionary text into FTS syntax. No generated
   query may widen the caller's page/root/metadata predicate. Each backend call
   applies the same authorized predicate **before** scoring and top-k.
6. Merge by `(page_id, chunk_id)` from the same snapshot. Preserve existing
   score bands: exact ID above title above body, with native nonnegative BM25
   mapped as `band + s/(1+s)`. A translated title/body match uses its actual
   native score and the same field band, not a fabricated semantic score. For
   equal scores prefer raw evidence, then fewer generated queries/earlier
   planner ordinal, then page and chunk ID. Do not sum scores merely because a
   polysemous term matched several translations. Raw-only defaults and hybrid
   RRF/tie policy otherwise remain unchanged.
7. Return only original canonical text/snippets and citations. If an expansion
   candidate is returned, provenance records `lexical_dictionary_v1`, manifest
   digest, field and planner ordinal. Preserve raw and derived match evidence
   when a hit has both. Do not expose another page's terms, private query text,
   dictionary search history or generated strings in logs/metrics by default.

This preserves stable-ID ordering and raw-only phrase behavior. It does not
promise all old natural-language rankings remain unchanged: expansion is
intended to alter those candidate sets. #473 must verify exact names and both
complete fixed corpora rather than using that intent as evidence of quality.
Current `matched_paths` distinguishes lexical/vector paths; detailed lexical
provenance needs a versioned additive internal result contract, not replacement
of source provenance or labeling dictionary evidence as vector/graph evidence.

## Hard bounds and default/failure behavior

| Boundary | Version 1 maximum and behavior |
| --- | --- |
| Input | Existing 4096-character query, 1–100 result limit and current scoped filters remain. |
| Asset download/import | 16 MiB compressed archive; 64 MiB total decoded bytes; XML depth 32; 250,000 entries; 1,000,000 normalized edges. Reject the entire artifact if a bound is exceeded. |
| Ready asset | 128 MiB canonical payload, at most 256 MiB runtime resident storage; importer rejects a bundle whose selected representation cannot enforce these limits. |
| Query lookup | At most 128 word positions examined; first eight nonoverlapping matched units, four targets per unit, eight final phrases. Overlong queries still use the existing raw path; report bounded expansion, never complete lexical coverage. |
| Backend work | Four raw field requests plus at most 16 expanded field requests. Each uses `min(4 * requested_limit, 400)` candidates. At most 8,000 candidate rows before deduplication. |
| In-flight memory | At most 8,000 bounded candidate records; source text projection/snippet at most 2,000 scalars per record. Admission must reserve a 96 MiB per-request candidate budget before materialization, including all strings/provenance and container overhead; whichever byte/count bound is reached first applies. Backend projections must bound text and metadata before constructing result vectors. Overflow fails the complete request. |
| Time/concurrency | Share the existing tool admission and request deadline/cancellation. No detached expansion tasks, retries or unbounded queues. Per-request additional expansion CPU budget 50 ms; expiry fails the request rather than returning an unlabeled partial branch. |
| Provenance | At most 20 match records per returned hit, deduplicated and sorted; counters for bounded/ambiguous/unsupported units without query text. |

Default is disabled: existing raw FTS behavior and stored index contracts remain
usable. Enabling expansion is trusted operator configuration, never tool input.
There is no per-caller request field that selects an asset, workspace or policy.
With expansion enabled, missing/corrupt/incompatible assets or index binding,
resource/time failure, interrupted rebuild or incomplete branch execution fail
closed for lexical/hybrid search. Vector-only behavior may remain independently
available under its existing compatible contract; do not silently substitute it
for requested lexical/hybrid search. Unknown dictionary terms or languages are
valid zero-expansion plans with explicit provenance, not asset failures. Planned
unit/phrase caps are deterministic complete bounded plans; a runtime timeout or
missing backend branch is an incomplete execution and cannot return partial hits.

## Identity, explicit transition and recovery

Separate three identities rather than relabeling vectors:

- Embedding/vector identity: existing model revision, tokenizer, dimension and
  canonical embedding text contract, unchanged by query-only expansion.
- Lexical storage identity: schema, source snapshot, four field/index names,
  exact Swedish tokenization settings, backend/version and raw ID semantics.
- Retrieval plan identity: planner version, source/lockfile, Unicode behavior,
  verified input/derived dictionary hashes, bounds, phrase/ID bypass and score/
  tie semantics. Hash canonical metadata; never use an unversioned local path.

The enabled runtime must require an explicit binding of all three plus the
trusted configured workspace/root policy. Physical cache identity is an
implementation detail, not a requirement to preserve a prior generation.
A missing binding is incompatible, not permission to stamp an old table as new.
Persist the binding in trusted derived-index metadata checked by guarded opens
and commit preparation; propagate plan identity into evaluation provenance.
Assets must be regular immutable local files with verified digests, opened once
and retained through a request; atomic path replacement cannot mix generations.

The service is one process and its local retrieval state is disposable. Initial
enablement and later dictionary/planner/model changes use the supported cache
reset and authoritative Notion resync path (#577). Rebuild compatible lexical
structures from authorized canonical content; preserve source text, hashes,
citations and correct embedding identity. Never relabel incompatible vectors.
Re-embed from Notion when the configured vector identity changes.

Coordinate admission and writers inside the process while rebuilding. Reopen
and validate the configured vector, lexical and plan identities before allowing
search. Missing or incompatible metadata is a reason to rebuild derived data,
not a requirement to preserve the old generation. A failed or interrupted rebuild
can reset its partial output and resync again. No sibling candidate, verified old
index backup, durable phase journal, process handoff or rollback protocol is
required. Existing code may still enforce physical bindings until #577/#578
replace that recovery path; this design does not authorize bypassing those guards
manually.

Requests must use one coherent verified configuration. Asset files are verified
local inputs; a request cannot mix dictionary or plan identities after path
replacement. Keep ordinary process-local writer coordination and authorization.

Reset only derived retrieval and its invalidated bookkeeping. Preserve grant
storage, credentials, external mutation/idempotency records and audit. Preserve
admitted refresh work or account for it through authoritative resync without
replaying Notion writes or attachments. Operational SQLite contains a mixture of
derived projections and admitted work; whole-file deletion is not the recovery
API. Retaining every historical cache receipt is not a prerequisite to rebuilding.
See [cache reset boundaries](../cache-reset-and-resync.md).

## Implementation and decision protocol (#473)

Independent design approval must precede implementation and new decision
measurements. After merge, #473 must implement this contract, including the 2026-10-28
single-process reset/resync correction, before collecting evidence; it cannot quietly add
query expansion branches after observing failed judgments.

Use independent behavior examples for normalization, phrase/ID bypass, literal
query safety, ambiguity/budget limits, English/Swedish lookup and out-of-scope
sources. Verify default/off mode, corrupted assets, version mismatch, cache
loss followed by authoritative resync, coherent request configuration and
preservation of credentials and external-effect safety records. The direct matching path must expose original citations, with no semantic
fallback or unauthorized candidates admitted before truncation.

Then rerun the real configured production vector/FTS/hybrid adapter on **both
unchanged complete corpora**, retaining all ranked IDs, zero-hit queries, full
metrics, language/category breakdowns, input/asset/plan identities, snapshots
and byte-repeat evidence. Keep independent judgments out of all ranking inputs.
Repeat the complete-candidate diagnostic and identify truncated observations.
Retain the failing baseline and exact-identifier gates.

Both existing policies remain binding: G1 in
[ChatGPT cutover acceptance](../chatgpt-cutover-acceptance.md) requires hybrid
Recall@5/MRR/nDCG@5 at least 0.80; the separate primary
[cutover gate](../cutover-gates.md) requires **FTS and hybrid** Recall@5 at least
0.95 and MRR/nDCG@5 at least 0.90, plus language/category Recall@5 floors of 0.90 and the
exact-ID requirements retained by #461. Neither this design nor improvement against baseline
passes a failed floor. Do not remove questions, revise judgments, lower targets
or enable graph boost. Pilot/live acceptance remains separately required.

If translation cannot repair morphology, semantic paraphrase, status metadata
or linked-source intent, #468/#461/#12/#1 remain open. Decompose further generic
linguistic, metadata or authorized navigation improvements into focused children
with reviewed contracts; do not expand this dictionary into an answer map.
