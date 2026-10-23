# Bounded literal lexical execution (#490)

This component executes the [reviewed dictionary plan](design/multilingual-lexical-expansion.md)
against original indexed title/body text. It is an internal adapter primitive;
the shipping server does not install it yet. Explicit index binding/migration
and trusted runtime installation remain #491/#492, followed by the complete
unchanged-corpus measurements in #493. No quality gate or parent acceptance is
established here. Graph remains OFF.

## Execution and compatibility

`expanded_lexical_search` holds one candidate reservation and one pinned table
snapshot. Its guarded variant additionally holds the directory/authority fence
through execution and rechecks authority before returning. Existing tool
admission must remain held by the owning installed adapter. The operation races
the caller's cancellation future and absolute deadline; it starts no detached
application task and returns no partial results on branch failure.

All four raw FTS fields execute unchanged queries. Whole-query quoted phrases,
syntactically valid Notion page identifiers/references, and versioned chunk IDs
bypass dictionary lookup even when missing or excluded. Exact raw identifier
matches additionally support adapter fixture/legacy IDs. Identifier recognition
does not normalize the original FTS request. Other queries use the bounded
asset planner, then at most eight literal phrases in title and body only.
Generated branches use native typed `PhraseQuery`, never generated FTS syntax.
All branches apply the same source/root/metadata predicate before native top-k.

Native phrase queries require token positions. The existing default FTS profile
continues to disable positions. `FtsIndexConfig::lexical_expansion_v1()` enables
positions for title/body, keeping raw ID tokenization and the existing Swedish
language/stemming/stopword/accent settings. An incompatible index causes the
complete request to fail. Search never rebuilds it. The explicit sibling-index
rebuild, identity checks, activation and recovery in #491 must establish this
profile before installation; setting a boolean is not an index migration.

## Candidate bounds

`CandidateBudget` is a shared, operator-owned, zero-queue pool. Each permit
reserves a 96 MiB candidate envelope before backend work. Keep the reservation
through merging; dropping the future releases it. The pool supports 1–16
concurrent reservations; runtime composition must choose a capacity consistent
with its installed tool and process limits, and share it across search modes.

Each branch returns at most `min(4 * result_limit, 400)` rows. The complete
request allows at most 8,000 candidate rows. The envelope reserves 24 MiB for
bounded backend scratch and accounts projected Arrow storage, owned result
strings, parsed heading containers, merge keys and provenance before decoding
result vectors. Accounting is conservative and cumulative across branches;
exhaustion fails the complete request.

Backend projections return at most 2,000 Unicode scalars of original text.
Metadata is projected with Unicode prefixes and separate overflow flags:

| Field | Maximum scalars |
| --- | ---: |
| page/chunk/root/block ID | 128 each |
| canonical URL | 2,048 |
| title | 1,024 |
| serialized heading path | 4,096 |
| edit timestamp | 64 |

Overflow rejects the request; truncated metadata is never returned as a valid
citation. Whole-query raw phrase checks evaluate complete original fields in
the backend, so a phrase beyond the snippet can still match. The returned
snippet retains the existing URL-redaction step.

Typed property/date filtering uses a bounded, source-scoped metadata scan from
the same snapshot. It projects IDs/timestamps and at most 4,096 scalars of
property JSON, detects overflow, and allows at most 8,000 scanned rows plus one
overflow sentinel. Resource exhaustion rejects the request instead of limiting
an incomplete eligible set. The resulting stable-ID predicate narrows every
raw and generated branch before ranking/top-k. Missing properties retain their
existing nonmatch behavior.

The additional planner/decoding/filtering/merge work has a 50 ms budget.
Synchronous segments use elapsed monotonic time as a conservative upper bound
on CPU; backend awaits are excluded. Exceeding the bound fails the request.
There are no retries or queues within this component.

## Scores and evidence

Native nonnegative BM25 maps to `band + score/(1+score)`, retaining ID/title/body
bands 4/2/1. Merging keeps the maximum score and original canonical source;
matching several translations never adds scores. Equal scores prefer raw
evidence, fewer distinct generated query ordinals, earlier ordinal, then stable
page and chunk IDs. Existing hybrid RRF remains unchanged.

`BoundedLexicalResult` adds sorted, deduplicated internal match records to each
hit, capped at 20: raw field evidence or `lexical_dictionary_v1` with verified
asset digest, field and planner ordinal. It retains raw evidence when both
paths match. Original source text, URL, IDs and citation metadata are preserved.
No private query/generated strings enter evidence, logs or counters. The asset
loader now retains its verified bundle digest for this provenance.

## Verification

Use the pinned Nix spike shell:

```sh
cargo test -p notion-knowledge-retrieval --features local-lancedb --locked
cargo clippy -p notion-knowledge-retrieval --features local-lancedb --all-targets --locked -- -D warnings
```

Independent native fixtures cover bilingual literal matches, phrase adjacency,
source/root and typed metadata filtering before limits, full-source phrase checks
beyond snippets, ID/quote bypass, original citation/provenance, score/tie policy,
resource admission, incompatible profiles and cancellation/deadline failures.
A later translated branch failure discards earlier successful raw candidates.
These are behavior specifications, not production quality measurements.
